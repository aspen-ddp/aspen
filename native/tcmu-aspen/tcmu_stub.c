/* Stub definitions of every tcmu function handler_aspen.c calls, for test_handler only. */
#define _GNU_SOURCE
#include "tcmu_stub.h"

#include <errno.h>
#include <pthread.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "tcmu-runner.h"
#include "tcmur_device.h"
#include "libtcmu.h"

#define MAX_COMPLETIONS 256

static pthread_mutex_t stub_lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t stub_cond = PTHREAD_COND_INITIALIZER;
static struct {
	void *cmd;
	int rc;
	int conn_lost_before;
} completions[MAX_COMPLETIONS];
static int ncompletions;
static int conn_lost;
static int conn_lost_ignored;
static int conn_lost_to_ignore;
static char last_err[4096];

static void deadline_after(struct timespec *ts, int timeout_ms)
{
	clock_gettime(CLOCK_REALTIME, ts);
	ts->tv_sec += timeout_ms / 1000;
	ts->tv_nsec += (long)(timeout_ms % 1000) * 1000000L;
	if (ts->tv_nsec >= 1000000000L) {
		ts->tv_sec++;
		ts->tv_nsec -= 1000000000L;
	}
}

void stub_dev_init(struct tcmu_device *dev, const char *cfgstring, uint64_t num_lbas,
		   uint32_t block_size)
{
	memset(dev, 0, sizeof(*dev));
	dev->cfgstring = strdup(cfgstring);
	dev->num_lbas = num_lbas;
	dev->block_size = block_size;
}

void stub_dev_free(struct tcmu_device *dev)
{
	free(dev->cfgstring);
	dev->cfgstring = NULL;
}

int stub_wait_complete(void *cmd, int *rc, int timeout_ms)
{
	int lost;

	return stub_wait_complete_lost(cmd, rc, &lost, timeout_ms);
}

int stub_wait_complete_lost(void *cmd, int *rc, int *conn_lost_before, int timeout_ms)
{
	struct timespec deadline;
	int ret = -1;

	deadline_after(&deadline, timeout_ms);
	pthread_mutex_lock(&stub_lock);
	for (;;) {
		for (int i = 0; i < ncompletions; i++) {
			if (completions[i].cmd == cmd) {
				*rc = completions[i].rc;
				*conn_lost_before = completions[i].conn_lost_before;
				completions[i] = completions[--ncompletions];
				ret = 0;
				goto out;
			}
		}
		if (pthread_cond_timedwait(&stub_cond, &stub_lock, &deadline) == ETIMEDOUT)
			break;
	}
out:
	pthread_mutex_unlock(&stub_lock);
	return ret;
}

int stub_unconsumed_completions(void)
{
	int n;

	pthread_mutex_lock(&stub_lock);
	n = ncompletions;
	pthread_mutex_unlock(&stub_lock);
	return n;
}

int stub_conn_lost_accepted(void)
{
	int n;

	pthread_mutex_lock(&stub_lock);
	n = conn_lost - conn_lost_ignored;
	pthread_mutex_unlock(&stub_lock);
	return n;
}

void stub_ignore_conn_lost(int n)
{
	pthread_mutex_lock(&stub_lock);
	conn_lost_to_ignore = n;
	pthread_mutex_unlock(&stub_lock);
}

void stub_last_err(char *buf, size_t len)
{
	pthread_mutex_lock(&stub_lock);
	snprintf(buf, len, "%s", last_err);
	pthread_mutex_unlock(&stub_lock);
}

int stub_conn_lost_count(void)
{
	int n;

	pthread_mutex_lock(&stub_lock);
	n = conn_lost;
	pthread_mutex_unlock(&stub_lock);
	return n;
}

void stub_reset_conn_lost(void)
{
	pthread_mutex_lock(&stub_lock);
	conn_lost = 0;
	conn_lost_ignored = 0;
	conn_lost_to_ignore = 0;
	pthread_mutex_unlock(&stub_lock);
}

int stub_wait_conn_lost(int n, int timeout_ms)
{
	struct timespec deadline;
	int ret = 0;

	deadline_after(&deadline, timeout_ms);
	pthread_mutex_lock(&stub_lock);
	while (conn_lost < n) {
		if (pthread_cond_timedwait(&stub_cond, &stub_lock, &deadline) == ETIMEDOUT) {
			ret = conn_lost >= n ? 0 : -1;
			break;
		}
	}
	pthread_mutex_unlock(&stub_lock);
	return ret;
}

/* ---- tcmu-runner exports ---- */

int tcmur_register_handler(struct tcmur_handler *handler)
{
	(void)handler;
	return 0;
}

void tcmur_cmd_complete(struct tcmu_device *dev, void *data, int rc)
{
	(void)dev;
	pthread_mutex_lock(&stub_lock);
	if (ncompletions == MAX_COMPLETIONS) {
		fprintf(stderr, "tcmu_stub: too many unconsumed completions\n");
		abort();
	}
	completions[ncompletions].cmd = data;
	completions[ncompletions].rc = rc;
	completions[ncompletions].conn_lost_before = conn_lost;
	ncompletions++;
	pthread_cond_broadcast(&stub_cond);
	pthread_mutex_unlock(&stub_lock);
}

void tcmu_notify_conn_lost(struct tcmu_device *dev)
{
	(void)dev;
	pthread_mutex_lock(&stub_lock);
	conn_lost++;
	if (conn_lost_to_ignore > 0) {
		conn_lost_to_ignore--;
		conn_lost_ignored++;
	}
	pthread_cond_broadcast(&stub_cond);
	pthread_mutex_unlock(&stub_lock);
}

void tcmur_dev_set_private(struct tcmu_device *dev, void *private)
{
	dev->runner_private = private;
}

void *tcmur_dev_get_private(struct tcmu_device *dev)
{
	return dev->runner_private;
}

/* ---- libtcmu exports ---- */

char *tcmu_dev_get_cfgstring(struct tcmu_device *dev)
{
	return dev->cfgstring;
}

uint64_t tcmu_dev_get_num_lbas(struct tcmu_device *dev)
{
	return dev->num_lbas;
}

uint32_t tcmu_dev_get_block_size(struct tcmu_device *dev)
{
	return dev->block_size;
}

void tcmu_dev_set_write_cache_enabled(struct tcmu_device *dev, bool enabled)
{
	dev->write_cache = enabled;
}

void tcmu_dev_set_unmap_enabled(struct tcmu_device *dev, bool enabled)
{
	dev->unmap_enabled = enabled;
}

void tcmu_dev_set_opt_unmap_gran(struct tcmu_device *dev, uint32_t len, bool split)
{
	dev->opt_unmap_gran = len;
	dev->opt_unmap_split = split;
}

void tcmu_dev_set_unmap_gran_align(struct tcmu_device *dev, uint32_t len)
{
	dev->unmap_gran_align = len;
}

static void log_message(const char *level, const char *funcname, int linenr, const char *fmt,
			va_list ap)
{
	if (getenv("ASPEN_TEST_LOG") == NULL)
		return;
	fprintf(stderr, "%s %s:%d: ", level, funcname, linenr);
	vfprintf(stderr, fmt, ap);
}

void tcmu_err_message(struct tcmu_device *dev, const char *funcname, int linenr, const char *fmt, ...)
{
	va_list ap;

	(void)dev;
	va_start(ap, fmt);
	pthread_mutex_lock(&stub_lock);
	vsnprintf(last_err, sizeof(last_err), fmt, ap);
	pthread_mutex_unlock(&stub_lock);
	va_end(ap);
	va_start(ap, fmt);
	log_message("ERR", funcname, linenr, fmt, ap);
	va_end(ap);
}

void tcmu_dbg_message(struct tcmu_device *dev, const char *funcname, int linenr, const char *fmt, ...)
{
	va_list ap;

	(void)dev;
	va_start(ap, fmt);
	log_message("DBG", funcname, linenr, fmt, ap);
	va_end(ap);
}
