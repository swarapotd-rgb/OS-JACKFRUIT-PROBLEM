/*
 * engine.c - Supervised Multi-Container Runtime (User Space)
 */

#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <pthread.h>
#include <sched.h>
#include <signal.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/mount.h>
#include <sys/resource.h>
#include <sys/select.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/un.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include "monitor_ioctl.h"

#define STACK_SIZE (1024 * 1024)
#define CONTAINER_ID_LEN 32
#define CONTROL_PATH "/tmp/mini_runtime.sock"
#define LOG_DIR "logs"
#define CONTROL_MESSAGE_LEN 4096
#define CHILD_COMMAND_LEN 256
#define LOG_CHUNK_SIZE 4096
#define LOG_BUFFER_CAPACITY 64
#define DEFAULT_SOFT_LIMIT (40UL << 20)
#define DEFAULT_HARD_LIMIT (64UL << 20)

typedef enum { CMD_SUPERVISOR = 0, CMD_START, CMD_RUN, CMD_PS, CMD_LOGS, CMD_STOP } command_kind_t;
typedef enum { CONTAINER_STARTING = 0, CONTAINER_RUNNING, CONTAINER_STOPPED, CONTAINER_KILLED, CONTAINER_EXITED } container_state_t;

typedef struct {
    char container_id[CONTAINER_ID_LEN];
    size_t length;
    char data[LOG_CHUNK_SIZE];
} log_item_t;

typedef struct {
    log_item_t items[LOG_BUFFER_CAPACITY];
    size_t head;
    size_t tail;
    size_t count;
    int shutting_down;
    pthread_mutex_t mutex;
    pthread_cond_t not_empty;
    pthread_cond_t not_full;
} bounded_buffer_t;

typedef struct {
    command_kind_t kind;
    char container_id[CONTAINER_ID_LEN];
    char rootfs[PATH_MAX];
    char command[CHILD_COMMAND_LEN];
    unsigned long soft_limit_bytes;
    unsigned long hard_limit_bytes;
    int nice_value;
} control_request_t;

typedef struct {
    int status;
    char message[CONTROL_MESSAGE_LEN];
} control_response_t;

typedef struct {
    char id[CONTAINER_ID_LEN];
    char rootfs[PATH_MAX];
    char command[CHILD_COMMAND_LEN];
    int nice_value;
    int log_write_fd;
} child_config_t;

typedef struct container_record {
    char id[CONTAINER_ID_LEN];
    char rootfs[PATH_MAX];
    pid_t host_pid;
    time_t started_at;
    container_state_t state;
    unsigned long soft_limit_bytes;
    unsigned long hard_limit_bytes;
    int exit_code;
    int exit_signal;
    int exited;
    int stop_requested;
    int producer_joined;
    int log_read_fd;
    pthread_t producer_thread;
    char log_path[PATH_MAX];
    char final_reason[64];
    struct container_record *next;
} container_record_t;

typedef struct {
    int server_fd;
    int monitor_fd;
    int should_stop;
    pthread_t logger_thread;
    bounded_buffer_t log_buffer;
    pthread_mutex_t metadata_lock;
    pthread_cond_t metadata_cond;
    container_record_t *containers;
} supervisor_ctx_t;

typedef struct {
    supervisor_ctx_t *ctx;
    char container_id[CONTAINER_ID_LEN];
    int read_fd;
} producer_arg_t;

static volatile sig_atomic_t g_stop = 0;
static volatile sig_atomic_t g_sigchld = 0;

static void usage(const char *prog)
{
    fprintf(stderr,
            "Usage:\n"
            "  %s supervisor <base-rootfs>\n"
            "  %s start <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n"
            "  %s run <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n"
            "  %s ps\n"
            "  %s logs <id>\n"
            "  %s stop <id>\n",
            prog, prog, prog, prog, prog, prog);
}

static int parse_mib_flag(const char *flag,
                          const char *value,
                          unsigned long *target_bytes)
{
    char *end = NULL;
    unsigned long mib;

    errno = 0;
    mib = strtoul(value, &end, 10);
    if (errno != 0 || end == value || *end != '\0') {
        fprintf(stderr, "Invalid value for %s: %s\n", flag, value);
        return -1;
    }

    if (mib > ULONG_MAX / (1UL << 20)) {
        fprintf(stderr, "Value for %s is too large: %s\n", flag, value);
        return -1;
    }

    *target_bytes = mib * (1UL << 20);
    return 0;
}

static int parse_optional_flags(control_request_t *req,
                                int argc,
                                char *argv[],
                                int start_index)
{
    int i;

    for (i = start_index; i < argc; i += 2) {
        char *end = NULL;
        long nice_value;

        if (i + 1 >= argc) {
            fprintf(stderr, "Missing value for option: %s\n", argv[i]);
            return -1;
        }

        if (strcmp(argv[i], "--soft-mib") == 0) {
            if (parse_mib_flag("--soft-mib", argv[i + 1], &req->soft_limit_bytes) != 0)
                return -1;
            continue;
        }

        if (strcmp(argv[i], "--hard-mib") == 0) {
            if (parse_mib_flag("--hard-mib", argv[i + 1], &req->hard_limit_bytes) != 0)
                return -1;
            continue;
        }

        if (strcmp(argv[i], "--nice") == 0) {
            errno = 0;
            nice_value = strtol(argv[i + 1], &end, 10);
            if (errno != 0 || end == argv[i + 1] || *end != '\0' ||
                nice_value < -20 || nice_value > 19) {
                fprintf(stderr,
                        "Invalid value for --nice (expected -20..19): %s\n",
                        argv[i + 1]);
                return -1;
            }
            req->nice_value = (int)nice_value;
            continue;
        }

        fprintf(stderr, "Unknown option: %s\n", argv[i]);
        return -1;
    }

    if (req->soft_limit_bytes > req->hard_limit_bytes) {
        fprintf(stderr, "Invalid limits: soft limit cannot exceed hard limit\n");
        return -1;
    }

    return 0;
}

static const char *state_to_string(container_state_t state)
{
    switch (state) {
    case CONTAINER_STARTING:
        return "starting";
    case CONTAINER_RUNNING:
        return "running";
    case CONTAINER_STOPPED:
        return "stopped";
    case CONTAINER_KILLED:
        return "killed";
    case CONTAINER_EXITED:
        return "exited";
    default:
        return "unknown";
    }
}

static int bounded_buffer_init(bounded_buffer_t *buffer)
{
    int rc;

    memset(buffer, 0, sizeof(*buffer));

    rc = pthread_mutex_init(&buffer->mutex, NULL);
    if (rc != 0)
        return rc;

    rc = pthread_cond_init(&buffer->not_empty, NULL);
    if (rc != 0) {
        pthread_mutex_destroy(&buffer->mutex);
        return rc;
    }

    rc = pthread_cond_init(&buffer->not_full, NULL);
    if (rc != 0) {
        pthread_cond_destroy(&buffer->not_empty);
        pthread_mutex_destroy(&buffer->mutex);
        return rc;
    }

    return 0;
}

static void bounded_buffer_destroy(bounded_buffer_t *buffer)
{
    pthread_cond_destroy(&buffer->not_full);
    pthread_cond_destroy(&buffer->not_empty);
    pthread_mutex_destroy(&buffer->mutex);
}

static void bounded_buffer_begin_shutdown(bounded_buffer_t *buffer)
{
    pthread_mutex_lock(&buffer->mutex);
    buffer->shutting_down = 1;
    pthread_cond_broadcast(&buffer->not_empty);
    pthread_cond_broadcast(&buffer->not_full);
    pthread_mutex_unlock(&buffer->mutex);
}

/*
 * TODO:
 * Implement producer-side insertion into the bounded buffer.
 *
 * Requirements:
 *   - block or fail according to your chosen policy when the buffer is full
 *   - wake consumers correctly
 *   - stop cleanly if shutdown begins
 */
int bounded_buffer_push(bounded_buffer_t *buffer, const log_item_t *item)
{
    pthread_mutex_lock(&buffer->mutex);
    while (buffer->count == LOG_BUFFER_CAPACITY && !buffer->shutting_down)
        pthread_cond_wait(&buffer->not_full, &buffer->mutex);
    if (buffer->shutting_down) {
        pthread_mutex_unlock(&buffer->mutex);
        return -1;
    }
    buffer->items[buffer->tail] = *item;
    buffer->tail = (buffer->tail + 1) % LOG_BUFFER_CAPACITY;
    buffer->count++;
    pthread_cond_signal(&buffer->not_empty);
    pthread_mutex_unlock(&buffer->mutex);
    return 0;
}

/*
 * TODO:
 * Implement consumer-side removal from the bounded buffer.
 *
 * Requirements:
 *   - wait correctly while the buffer is empty
 *   - return a useful status when shutdown is in progress
 *   - avoid races with producers and shutdown
 */
int bounded_buffer_pop(bounded_buffer_t *buffer, log_item_t *item)
{
    pthread_mutex_lock(&buffer->mutex);
    while (buffer->count == 0 && !buffer->shutting_down)
        pthread_cond_wait(&buffer->not_empty, &buffer->mutex);
    if (buffer->count == 0 && buffer->shutting_down) {
        pthread_mutex_unlock(&buffer->mutex);
        return -1;
    }
    *item = buffer->items[buffer->head];
    buffer->head = (buffer->head + 1) % LOG_BUFFER_CAPACITY;
    buffer->count--;
    pthread_cond_signal(&buffer->not_full);
    pthread_mutex_unlock(&buffer->mutex);
    return 0;
}

/*
 * TODO:
 * Implement the logging consumer thread.
 *
 * Suggested responsibilities:
 *   - remove log chunks from the bounded buffer
 *   - route each chunk to the correct per-container log file
 *   - exit cleanly when shutdown begins and pending work is drained
 */
static void appendf(char *dst, size_t cap, const char *fmt, ...)
{
    size_t used = strlen(dst);
    va_list ap;
    if (used >= cap)
        return;
    va_start(ap, fmt);
    vsnprintf(dst + used, cap - used, fmt, ap);
    va_end(ap);
}

static int ensure_log_dir(void)
{
    if (mkdir(LOG_DIR, 0755) == 0 || errno == EEXIST)
        return 0;
    return -1;
}

static ssize_t read_full(int fd, void *buf, size_t len)
{
    size_t off = 0;
    while (off < len) {
        ssize_t rc = read(fd, (char *)buf + off, len - off);
        if (rc == 0)
            return (ssize_t)off;
        if (rc < 0) {
            if (errno == EINTR)
                continue;
            return -1;
        }
        off += (size_t)rc;
    }
    return (ssize_t)off;
}

static ssize_t write_full(int fd, const void *buf, size_t len)
{
    size_t off = 0;
    while (off < len) {
        ssize_t rc = write(fd, (const char *)buf + off, len - off);
        if (rc < 0) {
            if (errno == EINTR)
                continue;
            return -1;
        }
        off += (size_t)rc;
    }
    return (ssize_t)off;
}

static container_record_t *find_container_locked(supervisor_ctx_t *ctx, const char *id)
{
    container_record_t *cur = ctx->containers;
    while (cur) {
        if (strncmp(cur->id, id, CONTAINER_ID_LEN) == 0)
            return cur;
        cur = cur->next;
    }
    return NULL;
}

static int rootfs_in_use_locked(supervisor_ctx_t *ctx, const char *rootfs)
{
    container_record_t *cur = ctx->containers;
    while (cur) {
        if (!cur->exited && strcmp(cur->rootfs, rootfs) == 0)
            return 1;
        cur = cur->next;
    }
    return 0;
}

void *logging_thread(void *arg)
{
    supervisor_ctx_t *ctx = (supervisor_ctx_t *)arg;
    log_item_t item;
    while (bounded_buffer_pop(&ctx->log_buffer, &item) == 0) {
        char path[PATH_MAX];
        int fd;
        snprintf(path, sizeof(path), "%s/%s.log", LOG_DIR, item.container_id);
        fd = open(path, O_CREAT | O_WRONLY | O_APPEND, 0644);
        if (fd >= 0) {
            (void)write_full(fd, item.data, item.length);
            close(fd);
        }
    }
    return NULL;
}

static void *producer_thread(void *arg)
{
    producer_arg_t *parg = (producer_arg_t *)arg;
    char buf[LOG_CHUNK_SIZE];
    for (;;) {
        ssize_t n = read(parg->read_fd, buf, sizeof(buf));
        if (n <= 0)
            break;
        log_item_t item;
        memset(&item, 0, sizeof(item));
        strncpy(item.container_id, parg->container_id, sizeof(item.container_id) - 1);
        item.length = (size_t)n;
        memcpy(item.data, buf, (size_t)n);
        if (bounded_buffer_push(&parg->ctx->log_buffer, &item) != 0)
            break;
    }
    close(parg->read_fd);
    free(parg);
    return NULL;
}

/*
 * TODO:
 * Implement the clone child entrypoint.
 *
 * Required outcomes:
 *   - isolated PID / UTS / mount context
 *   - chroot or pivot_root into rootfs
 *   - working /proc inside container
 *   - stdout / stderr redirected to the supervisor logging path
 *   - configured command executed inside the container
 */
int child_fn(void *arg)
{
    child_config_t *cfg = (child_config_t *)arg;
    if (sethostname(cfg->id, strlen(cfg->id)) < 0)
        perror("sethostname");
    if (setpriority(PRIO_PROCESS, 0, cfg->nice_value) < 0)
        perror("setpriority");

    if (mount(NULL, "/", NULL, MS_REC | MS_PRIVATE, NULL) < 0)
        perror("mount-private");

    if (chdir(cfg->rootfs) < 0 || chroot(".") < 0 || chdir("/") < 0) {
        perror("chroot");
        return 1;
    }

    mkdir("/proc", 0555);
    if (mount("proc", "/proc", "proc", 0, NULL) < 0)
        perror("mount-proc");

    if (dup2(cfg->log_write_fd, STDOUT_FILENO) < 0 || dup2(cfg->log_write_fd, STDERR_FILENO) < 0) {
        perror("dup2");
        return 1;
    }
    close(cfg->log_write_fd);

    execl(cfg->command, cfg->command, (char *)NULL);
    perror("execl");
    return 127;
}

int register_with_monitor(int monitor_fd,
                          const char *container_id,
                          pid_t host_pid,
                          unsigned long soft_limit_bytes,
                          unsigned long hard_limit_bytes)
{
    struct monitor_request req;

    memset(&req, 0, sizeof(req));
    req.pid = host_pid;
    req.soft_limit_bytes = soft_limit_bytes;
    req.hard_limit_bytes = hard_limit_bytes;
    strncpy(req.container_id, container_id, sizeof(req.container_id) - 1);

    if (ioctl(monitor_fd, MONITOR_REGISTER, &req) < 0)
        return -1;

    return 0;
}

int unregister_from_monitor(int monitor_fd, const char *container_id, pid_t host_pid)
{
    struct monitor_request req;

    memset(&req, 0, sizeof(req));
    req.pid = host_pid;
    strncpy(req.container_id, container_id, sizeof(req.container_id) - 1);

    if (ioctl(monitor_fd, MONITOR_UNREGISTER, &req) < 0)
        return -1;

    return 0;
}

/*
 * TODO:
 * Implement the long-running supervisor process.
 *
 * Suggested responsibilities:
 *   - create and bind the control-plane IPC endpoint
 *   - initialize shared metadata and the bounded buffer
 *   - start the logging thread
 *   - accept control requests and update container state
 *   - reap children and respond to signals
 */
static void on_sigint_term(int signo)
{
    (void)signo;
    g_stop = 1;
}

static void on_sigchld(int signo)
{
    (void)signo;
    g_sigchld = 1;
}

static int install_handlers(void)
{
    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = on_sigchld;
    sa.sa_flags = SA_RESTART | SA_NOCLDSTOP;
    if (sigaction(SIGCHLD, &sa, NULL) < 0)
        return -1;
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = on_sigint_term;
    sa.sa_flags = SA_RESTART;
    if (sigaction(SIGINT, &sa, NULL) < 0)
        return -1;
    if (sigaction(SIGTERM, &sa, NULL) < 0)
        return -1;
    return 0;
}

static void reap_children(supervisor_ctx_t *ctx)
{
    int status;
    pid_t pid;
    while ((pid = waitpid(-1, &status, WNOHANG)) > 0) {
        container_record_t *cur;
        pthread_mutex_lock(&ctx->metadata_lock);
        cur = ctx->containers;
        while (cur && cur->host_pid != pid)
            cur = cur->next;
        if (cur) {
            cur->exited = 1;
            if (WIFEXITED(status)) {
                cur->exit_code = WEXITSTATUS(status);
                cur->state = (cur->stop_requested) ? CONTAINER_STOPPED : CONTAINER_EXITED;
                snprintf(cur->final_reason, sizeof(cur->final_reason), "%s",
                         cur->stop_requested ? "stopped" : "exited");
            } else if (WIFSIGNALED(status)) {
                cur->exit_signal = WTERMSIG(status);
                if (cur->stop_requested) {
                    cur->state = CONTAINER_STOPPED;
                    snprintf(cur->final_reason, sizeof(cur->final_reason), "stopped");
                } else if (cur->exit_signal == SIGKILL) {
                    cur->state = CONTAINER_KILLED;
                    snprintf(cur->final_reason, sizeof(cur->final_reason), "hard_limit_killed");
                } else {
                    cur->state = CONTAINER_KILLED;
                    snprintf(cur->final_reason, sizeof(cur->final_reason), "killed_signal_%d", cur->exit_signal);
                }
            }
            if (!cur->producer_joined) {
                pthread_join(cur->producer_thread, NULL);
                cur->producer_joined = 1;
            }
            if (ctx->monitor_fd >= 0)
                (void)unregister_from_monitor(ctx->monitor_fd, cur->id, cur->host_pid);
            pthread_cond_broadcast(&ctx->metadata_cond);
        }
        pthread_mutex_unlock(&ctx->metadata_lock);
    }
}

static int start_container(supervisor_ctx_t *ctx, const control_request_t *req, control_response_t *resp)
{
    int pipefd[2];
    void *stack = NULL;
    child_config_t *child_cfg = NULL;
    container_record_t *rec = NULL;
    producer_arg_t *parg = NULL;
    pid_t child_pid;

    pthread_mutex_lock(&ctx->metadata_lock);
    if (find_container_locked(ctx, req->container_id)) {
        pthread_mutex_unlock(&ctx->metadata_lock);
        resp->status = 1;
        snprintf(resp->message, sizeof(resp->message), "container id already exists: %s", req->container_id);
        return 1;
    }
    if (rootfs_in_use_locked(ctx, req->rootfs)) {
        pthread_mutex_unlock(&ctx->metadata_lock);
        resp->status = 1;
        snprintf(resp->message, sizeof(resp->message), "rootfs already in use by another running container");
        return 1;
    }
    pthread_mutex_unlock(&ctx->metadata_lock);

    if (pipe(pipefd) < 0) {
        resp->status = 1;
        snprintf(resp->message, sizeof(resp->message), "pipe failed: %s", strerror(errno));
        return 1;
    }

    child_cfg = calloc(1, sizeof(*child_cfg));
    rec = calloc(1, sizeof(*rec));
    stack = malloc(STACK_SIZE);
    if (!child_cfg || !rec || !stack) {
        close(pipefd[0]); close(pipefd[1]);
        free(child_cfg); free(rec); free(stack);
        resp->status = 1;
        snprintf(resp->message, sizeof(resp->message), "allocation failed");
        return 1;
    }

    strncpy(child_cfg->id, req->container_id, sizeof(child_cfg->id) - 1);
    strncpy(child_cfg->rootfs, req->rootfs, sizeof(child_cfg->rootfs) - 1);
    strncpy(child_cfg->command, req->command, sizeof(child_cfg->command) - 1);
    child_cfg->nice_value = req->nice_value;
    child_cfg->log_write_fd = pipefd[1];

    child_pid = clone(child_fn, (char *)stack + STACK_SIZE,
                      CLONE_NEWUTS | CLONE_NEWNS | CLONE_NEWPID | SIGCHLD, child_cfg);
    if (child_pid < 0) {
        close(pipefd[0]); close(pipefd[1]);
        free(child_cfg); free(rec); free(stack);
        resp->status = 1;
        snprintf(resp->message, sizeof(resp->message), "clone failed: %s", strerror(errno));
        return 1;
    }
    close(pipefd[1]);

    memset(rec, 0, sizeof(*rec));
    strncpy(rec->id, req->container_id, sizeof(rec->id) - 1);
    strncpy(rec->rootfs, req->rootfs, sizeof(rec->rootfs) - 1);
    rec->host_pid = child_pid;
    rec->started_at = time(NULL);
    rec->state = CONTAINER_RUNNING;
    rec->soft_limit_bytes = req->soft_limit_bytes;
    rec->hard_limit_bytes = req->hard_limit_bytes;
    rec->log_read_fd = pipefd[0];
    snprintf(rec->log_path, sizeof(rec->log_path), "%s/%s.log", LOG_DIR, rec->id);
    snprintf(rec->final_reason, sizeof(rec->final_reason), "running");

    parg = calloc(1, sizeof(*parg));
    if (!parg) {
        kill(child_pid, SIGKILL);
        close(pipefd[0]);
        free(child_cfg); free(rec); free(stack);
        resp->status = 1;
        snprintf(resp->message, sizeof(resp->message), "producer alloc failed");
        return 1;
    }
    parg->ctx = ctx;
    parg->read_fd = pipefd[0];
    strncpy(parg->container_id, rec->id, sizeof(parg->container_id) - 1);
    if (pthread_create(&rec->producer_thread, NULL, producer_thread, parg) != 0) {
        kill(child_pid, SIGKILL);
        close(pipefd[0]);
        free(parg); free(child_cfg); free(rec); free(stack);
        resp->status = 1;
        snprintf(resp->message, sizeof(resp->message), "pthread_create failed");
        return 1;
    }

    pthread_mutex_lock(&ctx->metadata_lock);
    rec->next = ctx->containers;
    ctx->containers = rec;
    pthread_mutex_unlock(&ctx->metadata_lock);

    if (ctx->monitor_fd >= 0)
        (void)register_with_monitor(ctx->monitor_fd, rec->id, rec->host_pid, rec->soft_limit_bytes, rec->hard_limit_bytes);

    free(child_cfg);
    free(stack);
    resp->status = 0;
    snprintf(resp->message, sizeof(resp->message), "started id=%s pid=%d", rec->id, rec->host_pid);
    return 0;
}

static int handle_request(supervisor_ctx_t *ctx, const control_request_t *req, control_response_t *resp)
{
    container_record_t *rec;
    memset(resp, 0, sizeof(*resp));

    if (req->kind == CMD_START || req->kind == CMD_RUN) {
        if (start_container(ctx, req, resp) != 0)
            return 0;
        if (req->kind == CMD_RUN) {
            pthread_mutex_lock(&ctx->metadata_lock);
            rec = find_container_locked(ctx, req->container_id);
            while (rec && !rec->exited)
                pthread_cond_wait(&ctx->metadata_cond, &ctx->metadata_lock);
            if (!rec) {
                pthread_mutex_unlock(&ctx->metadata_lock);
                resp->status = 1;
                snprintf(resp->message, sizeof(resp->message), "container vanished");
                return 0;
            }
            if (rec->exit_signal != 0) {
                resp->status = 128 + rec->exit_signal;
                snprintf(resp->message, sizeof(resp->message), "run finished: signal=%d reason=%s", rec->exit_signal, rec->final_reason);
            } else {
                resp->status = rec->exit_code;
                snprintf(resp->message, sizeof(resp->message), "run finished: exit_code=%d reason=%s", rec->exit_code, rec->final_reason);
            }
            pthread_mutex_unlock(&ctx->metadata_lock);
        }
        return 0;
    }

    if (req->kind == CMD_PS) {
        pthread_mutex_lock(&ctx->metadata_lock);
        appendf(resp->message, sizeof(resp->message), "ID\tPID\tSTATE\tSOFT_MIB\tHARD_MIB\tREASON\n");
        rec = ctx->containers;
        while (rec) {
            appendf(resp->message, sizeof(resp->message), "%s\t%d\t%s\t%lu\t%lu\t%s\n",
                    rec->id, rec->host_pid, state_to_string(rec->state),
                    rec->soft_limit_bytes >> 20, rec->hard_limit_bytes >> 20, rec->final_reason);
            rec = rec->next;
        }
        pthread_mutex_unlock(&ctx->metadata_lock);
        resp->status = 0;
        return 0;
    }

    if (req->kind == CMD_LOGS) {
        char path[PATH_MAX];
        int fd;
        ssize_t n;
        snprintf(path, sizeof(path), "%s/%s.log", LOG_DIR, req->container_id);
        fd = open(path, O_RDONLY);
        if (fd < 0) {
            resp->status = 1;
            snprintf(resp->message, sizeof(resp->message), "cannot open log file: %s", path);
            return 0;
        }
        n = read(fd, resp->message, sizeof(resp->message) - 1);
        close(fd);
        if (n < 0) {
            resp->status = 1;
            snprintf(resp->message, sizeof(resp->message), "log read failed");
        } else {
            resp->status = 0;
            resp->message[n] = '\0';
        }
        return 0;
    }

    if (req->kind == CMD_STOP) {
        pthread_mutex_lock(&ctx->metadata_lock);
        rec = find_container_locked(ctx, req->container_id);
        if (!rec || rec->exited) {
            pthread_mutex_unlock(&ctx->metadata_lock);
            resp->status = 1;
            snprintf(resp->message, sizeof(resp->message), "container not running: %s", req->container_id);
            return 0;
        }
        rec->stop_requested = 1;
        pthread_mutex_unlock(&ctx->metadata_lock);
        kill(rec->host_pid, SIGTERM);
        resp->status = 0;
        snprintf(resp->message, sizeof(resp->message), "stop requested for %s", req->container_id);
        return 0;
    }

    resp->status = 1;
    snprintf(resp->message, sizeof(resp->message), "unsupported command");
    return 0;
}

static void shutdown_supervisor(supervisor_ctx_t *ctx)
{
    container_record_t *cur;
    if (ctx->server_fd >= 0) {
        close(ctx->server_fd);
        unlink(CONTROL_PATH);
    }

    pthread_mutex_lock(&ctx->metadata_lock);
    cur = ctx->containers;
    while (cur) {
        if (!cur->exited) {
            cur->stop_requested = 1;
            kill(cur->host_pid, SIGTERM);
        }
        cur = cur->next;
    }
    pthread_mutex_unlock(&ctx->metadata_lock);

    sleep(1);
    reap_children(ctx);

    bounded_buffer_begin_shutdown(&ctx->log_buffer);
    pthread_join(ctx->logger_thread, NULL);

    pthread_mutex_lock(&ctx->metadata_lock);
    cur = ctx->containers;
    while (cur) {
        container_record_t *next = cur->next;
        if (!cur->producer_joined)
            pthread_join(cur->producer_thread, NULL);
        free(cur);
        cur = next;
    }
    ctx->containers = NULL;
    pthread_mutex_unlock(&ctx->metadata_lock);
}

static int run_supervisor(const char *rootfs)
{
    supervisor_ctx_t ctx;
    int rc, client_fd;
    struct sockaddr_un addr;

    memset(&ctx, 0, sizeof(ctx));
    ctx.server_fd = -1;
    ctx.monitor_fd = -1;
    (void)rootfs;

    rc = pthread_mutex_init(&ctx.metadata_lock, NULL);
    if (rc != 0) {
        errno = rc;
        perror("pthread_mutex_init");
        return 1;
    }

    rc = bounded_buffer_init(&ctx.log_buffer);
    if (rc != 0) {
        errno = rc;
        perror("bounded_buffer_init");
        pthread_mutex_destroy(&ctx.metadata_lock);
        return 1;
    }

    rc = pthread_cond_init(&ctx.metadata_cond, NULL);
    if (rc != 0) {
        errno = rc;
        perror("pthread_cond_init");
        bounded_buffer_destroy(&ctx.log_buffer);
        pthread_mutex_destroy(&ctx.metadata_lock);
        return 1;
    }
    if (ensure_log_dir() < 0)
        perror("mkdir logs");

    ctx.monitor_fd = open("/dev/container_monitor", O_RDWR);
    if (ctx.monitor_fd < 0)
        perror("open /dev/container_monitor");

    if (install_handlers() < 0) {
        perror("sigaction");
        return 1;
    }
    if (pthread_create(&ctx.logger_thread, NULL, logging_thread, &ctx) != 0) {
        perror("pthread_create logger");
        return 1;
    }

    ctx.server_fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (ctx.server_fd < 0) {
        perror("socket");
        shutdown_supervisor(&ctx);
        return 1;
    }
    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, CONTROL_PATH, sizeof(addr.sun_path) - 1);
    unlink(CONTROL_PATH);
    if (bind(ctx.server_fd, (struct sockaddr *)&addr, sizeof(addr)) < 0 || listen(ctx.server_fd, 16) < 0) {
        perror("bind/listen");
        shutdown_supervisor(&ctx);
        return 1;
    }

    while (!g_stop && !ctx.should_stop) {
        fd_set rfds;
        struct timeval tv;
        int saved_errno = 0;
        FD_ZERO(&rfds);
        FD_SET(ctx.server_fd, &rfds);
        tv.tv_sec = 1;
        tv.tv_usec = 0;
        rc = select(ctx.server_fd + 1, &rfds, NULL, NULL, &tv);
        if (rc < 0) {
            saved_errno = errno;
            if (saved_errno == EINTR) {
                if (g_sigchld) {
                    g_sigchld = 0;
                    reap_children(&ctx);
                }
                continue;
            }
            errno = saved_errno;
            perror("select");
            break;
        }
        if (g_sigchld) {
            g_sigchld = 0;
            reap_children(&ctx);
        }
        if (rc == 0)
            continue;
        client_fd = accept(ctx.server_fd, NULL, NULL);
        if (client_fd >= 0) {
            control_request_t req;
            control_response_t resp;
            memset(&req, 0, sizeof(req));
            if (read_full(client_fd, &req, sizeof(req)) == (ssize_t)sizeof(req)) {
                handle_request(&ctx, &req, &resp);
                (void)write_full(client_fd, &resp, sizeof(resp));
            }
            close(client_fd);
        }
    }

    shutdown_supervisor(&ctx);
    if (ctx.monitor_fd >= 0)
        close(ctx.monitor_fd);
    bounded_buffer_destroy(&ctx.log_buffer);
    pthread_cond_destroy(&ctx.metadata_cond);
    pthread_mutex_destroy(&ctx.metadata_lock);
    return 0;
}

/*
 * TODO:
 * Implement the client-side control request path.
 *
 * The CLI commands should use a second IPC mechanism distinct from the
 * logging pipe. A UNIX domain socket is the most direct option, but a
 * FIFO or shared memory design is also acceptable if justified.
 */
static int send_control_request(const control_request_t *req)
{
    int fd;
    struct sockaddr_un addr;
    control_response_t resp;
    fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (fd < 0) {
        perror("socket");
        return 1;
    }
    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, CONTROL_PATH, sizeof(addr.sun_path) - 1);
    if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("connect supervisor");
        close(fd);
        return 1;
    }
    if (write_full(fd, req, sizeof(*req)) != (ssize_t)sizeof(*req)) {
        perror("write request");
        close(fd);
        return 1;
    }
    if (read_full(fd, &resp, sizeof(resp)) != (ssize_t)sizeof(resp)) {
        perror("read response");
        close(fd);
        return 1;
    }
    close(fd);
    printf("%s\n", resp.message);
    return resp.status;
}

static int cmd_start(int argc, char *argv[])
{
    control_request_t req;

    if (argc < 5) {
        fprintf(stderr,
                "Usage: %s start <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n",
                argv[0]);
        return 1;
    }

    memset(&req, 0, sizeof(req));
    req.kind = CMD_START;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);
    strncpy(req.rootfs, argv[3], sizeof(req.rootfs) - 1);
    strncpy(req.command, argv[4], sizeof(req.command) - 1);
    req.soft_limit_bytes = DEFAULT_SOFT_LIMIT;
    req.hard_limit_bytes = DEFAULT_HARD_LIMIT;

    if (parse_optional_flags(&req, argc, argv, 5) != 0)
        return 1;

    return send_control_request(&req);
}

static int cmd_run(int argc, char *argv[])
{
    control_request_t req;

    if (argc < 5) {
        fprintf(stderr,
                "Usage: %s run <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n",
                argv[0]);
        return 1;
    }

    memset(&req, 0, sizeof(req));
    req.kind = CMD_RUN;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);
    strncpy(req.rootfs, argv[3], sizeof(req.rootfs) - 1);
    strncpy(req.command, argv[4], sizeof(req.command) - 1);
    req.soft_limit_bytes = DEFAULT_SOFT_LIMIT;
    req.hard_limit_bytes = DEFAULT_HARD_LIMIT;

    if (parse_optional_flags(&req, argc, argv, 5) != 0)
        return 1;

    return send_control_request(&req);
}

static int cmd_ps(void)
{
    control_request_t req;

    memset(&req, 0, sizeof(req));
    req.kind = CMD_PS;

    return send_control_request(&req);
}

static int cmd_logs(int argc, char *argv[])
{
    control_request_t req;

    if (argc < 3) {
        fprintf(stderr, "Usage: %s logs <id>\n", argv[0]);
        return 1;
    }

    memset(&req, 0, sizeof(req));
    req.kind = CMD_LOGS;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);

    return send_control_request(&req);
}

static int cmd_stop(int argc, char *argv[])
{
    control_request_t req;

    if (argc < 3) {
        fprintf(stderr, "Usage: %s stop <id>\n", argv[0]);
        return 1;
    }

    memset(&req, 0, sizeof(req));
    req.kind = CMD_STOP;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);

    return send_control_request(&req);
}

int main(int argc, char *argv[])
{
    if (argc < 2) {
        usage(argv[0]);
        return 1;
    }

    if (strcmp(argv[1], "supervisor") == 0) {
        if (argc < 3) {
            fprintf(stderr, "Usage: %s supervisor <base-rootfs>\n", argv[0]);
            return 1;
        }
        return run_supervisor(argv[2]);
    }

    if (strcmp(argv[1], "start") == 0)
        return cmd_start(argc, argv);

    if (strcmp(argv[1], "run") == 0)
        return cmd_run(argc, argv);

    if (strcmp(argv[1], "ps") == 0)
        return cmd_ps();

    if (strcmp(argv[1], "logs") == 0)
        return cmd_logs(argc, argv);

    if (strcmp(argv[1], "stop") == 0)
        return cmd_stop(argc, argv);

    usage(argv[0]);
    return 1;
}
