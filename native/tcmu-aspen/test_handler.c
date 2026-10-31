/* Tests for handler_aspen.c. The test plays the JVM daemon on a Unix socket; tcmu is stubbed
 * by tcmu_stub.c. Every wait is bounded at 5 s so a bug fails the test instead of hanging it. */
#define _GNU_SOURCE
#include <errno.h>
#include <poll.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <time.h>
#include <unistd.h>

#include "tcmu-runner.h"
#include "tcmur_device.h"
#include "libtcmu.h"

#include "aspen_proto.h"
#include "tcmu_stub.h"

struct tcmur_handler *aspen_handler_for_test(void);

#define WAIT_MS 5000
#define QUIET_MS 50
#define NUM_LBAS 2048u
#define BLOCK_SIZE 512u
#define BLOCK_OBJECT_SIZE 65536u

#define CHECK(cond) \
	do { \
		if (!(cond)) { \
			fprintf(stderr, "test_handler: FAILED %s line %d: %s\n", current_test, __LINE__, \
				#cond); \
			exit(1); \
		} \
	} while (0)

static const char *current_test = "";
static struct tcmur_handler *handler;
static char tmpdir[64];
static char sock_path[108];

/* ---------------------------------------------------------------- fake daemon */

static uint32_t get_u32(const uint8_t *p)
{
	return ((uint32_t)p[0] << 24) | ((uint32_t)p[1] << 16) | ((uint32_t)p[2] << 8) | p[3];
}

static uint64_t get_u64(const uint8_t *p)
{
	return ((uint64_t)get_u32(p) << 32) | get_u32(p + 4);
}

static void put_u32(uint8_t *p, uint32_t v)
{
	p[0] = (uint8_t)(v >> 24);
	p[1] = (uint8_t)(v >> 16);
	p[2] = (uint8_t)(v >> 8);
	p[3] = (uint8_t)v;
}

static void put_u64(uint8_t *p, uint64_t v)
{
	put_u32(p, (uint32_t)(v >> 32));
	put_u32(p + 4, (uint32_t)v);
}

static long now_ms(void)
{
	struct timespec ts;

	clock_gettime(CLOCK_MONOTONIC, &ts);
	return ts.tv_sec * 1000L + ts.tv_nsec / 1000000L;
}

/* Reads exactly n bytes, waiting at most WAIT_MS overall. Returns 0, or -1 on EOF/error/timeout. */
static int d_read_full(int fd, void *buf, size_t n)
{
	long deadline = now_ms() + WAIT_MS;
	uint8_t *p = buf;

	while (n > 0) {
		struct pollfd pfd = {.fd = fd, .events = POLLIN};
		long left = deadline - now_ms();
		ssize_t r;

		if (left <= 0 || poll(&pfd, 1, (int)left) <= 0)
			return -1;
		r = read(fd, p, n);
		if (r < 0 && errno == EINTR)
			continue;
		if (r <= 0)
			return -1;
		p += r;
		n -= (size_t)r;
	}
	return 0;
}

static int listen_socket(void)
{
	struct sockaddr_un addr = {.sun_family = AF_UNIX};
	int fd = socket(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0);

	CHECK(fd >= 0);
	unlink(sock_path);
	strcpy(addr.sun_path, sock_path);
	CHECK(bind(fd, (struct sockaddr *)&addr, sizeof(addr)) == 0);
	CHECK(listen(fd, 4) == 0);
	return fd;
}

static int d_accept(int lfd)
{
	struct pollfd pfd = {.fd = lfd, .events = POLLIN};
	int fd;

	CHECK(poll(&pfd, 1, WAIT_MS) == 1);
	fd = accept4(lfd, NULL, NULL, SOCK_CLOEXEC);
	CHECK(fd >= 0);
	return fd;
}

struct req {
	uint8_t op;
	uint32_t flags;
	uint64_t id;
	uint64_t offset;
	uint32_t length;
	uint32_t payload_len;
	uint8_t *payload;
};

static void d_read_req(int fd, struct req *r)
{
	uint8_t h[ASPEN_REQ_HDR_SIZE];

	CHECK(d_read_full(fd, h, sizeof(h)) == 0);
	CHECK(h[0] == 0xA5 && h[1] == 0xD1 && h[2] == 1);
	r->op = h[3];
	r->flags = get_u32(h + 4);
	r->id = get_u64(h + 8);
	r->offset = get_u64(h + 16);
	r->length = get_u32(h + 24);
	r->payload_len = get_u32(h + 28);
	r->payload = malloc(r->payload_len + 1);
	CHECK(r->payload != NULL);
	CHECK(d_read_full(fd, r->payload, r->payload_len) == 0);
	r->payload[r->payload_len] = 0;
}

static void d_expect_req(int fd, struct req *r, uint8_t op)
{
	d_read_req(fd, r);
	CHECK(r->op == op);
}

static void req_free(struct req *r)
{
	free(r->payload);
	r->payload = NULL;
}

static void d_reply(int fd, uint8_t op, uint32_t status, uint64_t id, const void *payload,
		    uint32_t len)
{
	uint8_t h[ASPEN_RESP_HDR_SIZE] = {0xA5, 0xD2, 1, op};

	put_u32(h + 4, status);
	put_u64(h + 8, id);
	put_u32(h + 16, len);
	CHECK(write(fd, h, sizeof(h)) == (ssize_t)sizeof(h));
	if (len > 0)
		CHECK(write(fd, payload, len) == (ssize_t)len);
}

static void d_reply_err(int fd, const struct req *r, uint32_t status)
{
	static const char msg[] = "scripted failure";

	d_reply(fd, r->op, status, r->id, msg, sizeof(msg) - 1);
}

/* Asserts nothing arrives within QUIET_MS. */
static void d_expect_quiet(int fd)
{
	struct pollfd pfd = {.fd = fd, .events = POLLIN};

	CHECK(poll(&pfd, 1, QUIET_MS) == 0);
}

/* Asserts the handler closed its end. */
static void d_expect_eof(int fd)
{
	uint8_t b;

	CHECK(d_read_full(fd, &b, 1) == -1);
}

/* ---------------------------------------------------------------- handler calls on a thread */

enum call_kind { CALL_OPEN, CALL_CLOSE, CALL_RECONFIG };

struct call {
	pthread_t thread;
	enum call_kind kind;
	struct tcmu_device *dev;
	bool reopen;
	struct tcmulib_cfg_info cfg;
	int ret;
};

static void *call_main(void *arg)
{
	struct call *c = arg;

	switch (c->kind) {
	case CALL_OPEN:
		c->ret = handler->open(c->dev, c->reopen);
		break;
	case CALL_CLOSE:
		handler->close(c->dev);
		c->ret = 0;
		break;
	case CALL_RECONFIG:
		c->ret = handler->reconfig(c->dev, &c->cfg);
		break;
	}
	return NULL;
}

static void call_start(struct call *c)
{
	CHECK(pthread_create(&c->thread, NULL, call_main, c) == 0);
}

static int call_join(struct call *c)
{
	struct timespec deadline;

	clock_gettime(CLOCK_REALTIME, &deadline);
	deadline.tv_sec += WAIT_MS / 1000;
	CHECK(pthread_timedjoin_np(c->thread, NULL, &deadline) == 0);
	return c->ret;
}

/* ---------------------------------------------------------------- commands */

struct test_cmd {
	struct tcmur_cmd rcmd;
	struct tcmulib_cmd lcmd;
	uint8_t cdb[16];
};

static struct tcmur_cmd *make_cmd(struct test_cmd *t, uint8_t opcode, uint8_t byte1)
{
	memset(t, 0, sizeof(*t));
	t->cdb[0] = opcode;
	t->cdb[1] = byte1;
	t->lcmd.cdb = t->cdb;
	t->rcmd.lib_cmd = &t->lcmd;
	return &t->rcmd;
}

static int wait_complete(struct tcmur_cmd *cmd)
{
	int rc = -12345;

	CHECK(stub_wait_complete(cmd, &rc, WAIT_MS) == 0);
	return rc;
}

/* ---------------------------------------------------------------- fixtures */

struct fixture {
	struct tcmu_device dev;
	int lfd;
	int dfd;
};

static void fixture_init(struct fixture *f)
{
	char cfg[160];

	snprintf(cfg, sizeof(cfg), "aspen/disk1@%s", sock_path);
	stub_dev_init(&f->dev, cfg, NUM_LBAS, BLOCK_SIZE);
	stub_reset_conn_lost();
	f->lfd = listen_socket();
	f->dfd = -1;
}

/* Runs open, answering OPEN and LOCK with Ok. Returns the daemon's end of the connection. */
static int do_open(struct fixture *f, bool reopen)
{
	struct call c = {.kind = CALL_OPEN, .dev = &f->dev, .reopen = reopen};
	uint8_t geom[8];
	struct req r;
	int fd;

	call_start(&c);
	fd = d_accept(f->lfd);
	d_expect_req(fd, &r, ASPEN_OP_OPEN);
	put_u32(geom, BLOCK_OBJECT_SIZE);
	put_u32(geom + 4, BLOCK_SIZE);
	d_reply(fd, r.op, ASPEN_ST_OK, r.id, geom, sizeof(geom));
	req_free(&r);
	d_expect_req(fd, &r, ASPEN_OP_LOCK);
	d_reply(fd, r.op, ASPEN_ST_OK, r.id, NULL, 0);
	req_free(&r);
	CHECK(call_join(&c) == 0);
	f->dfd = fd;
	return fd;
}

/* Runs close, answering UNLOCK with Ok. */
static void do_close(struct fixture *f)
{
	struct call c = {.kind = CALL_CLOSE, .dev = &f->dev};
	struct req r;

	call_start(&c);
	d_expect_req(f->dfd, &r, ASPEN_OP_UNLOCK);
	d_reply(f->dfd, r.op, ASPEN_ST_OK, r.id, NULL, 0);
	req_free(&r);
	CHECK(call_join(&c) == 0);
	d_expect_eof(f->dfd);
	CHECK(tcmur_dev_get_private(&f->dev) == NULL);
}

static void fixture_done(struct fixture *f)
{
	CHECK(stub_unconsumed_completions() == 0);
	if (f->dfd >= 0)
		close(f->dfd);
	close(f->lfd);
	stub_dev_free(&f->dev);
}

/* ---------------------------------------------------------------- tests */

static void test_check_config(void)
{
	char *reason = NULL;

	CHECK(handler->check_config("aspen/disk1", &reason));
	CHECK(handler->check_config("aspen/disk1@/tmp/x.sock", &reason));
	CHECK(!handler->check_config("aspen/", &reason));
	CHECK(reason != NULL);
	free(reason);
	reason = NULL;
	CHECK(!handler->check_config("file/disk1", &reason));
	CHECK(reason != NULL);
	free(reason);
}

static void test_open(void)
{
	struct fixture f;
	struct call c;
	uint8_t geom[8];
	struct req r;
	int fd;

	fixture_init(&f);
	c = (struct call){.kind = CALL_OPEN, .dev = &f.dev};
	call_start(&c);
	fd = d_accept(f.lfd);

	d_expect_req(fd, &r, ASPEN_OP_OPEN);
	CHECK(strcmp((char *)r.payload, "disk1") == 0);
	CHECK(r.payload_len == 5);
	CHECK(r.offset == (uint64_t)NUM_LBAS * BLOCK_SIZE);
	CHECK(r.length == BLOCK_SIZE);
	put_u32(geom, BLOCK_OBJECT_SIZE);
	put_u32(geom + 4, BLOCK_SIZE);
	d_reply(fd, r.op, ASPEN_ST_OK, r.id, geom, sizeof(geom));
	req_free(&r);

	d_expect_req(fd, &r, ASPEN_OP_LOCK);
	CHECK(r.payload_len == 0);
	d_reply(fd, r.op, ASPEN_ST_OK, r.id, NULL, 0);
	req_free(&r);

	CHECK(call_join(&c) == 0);
	CHECK(f.dev.write_cache);
	CHECK(f.dev.unmap_enabled);
	CHECK(f.dev.opt_unmap_gran == BLOCK_OBJECT_SIZE / BLOCK_SIZE);
	CHECK(!f.dev.opt_unmap_split);
	CHECK(f.dev.unmap_gran_align == 0);
	CHECK(tcmur_dev_get_private(&f.dev) != NULL);

	f.dfd = fd;
	do_close(&f);
	fixture_done(&f);
}

/* One failing open: OPEN (or LOCK when fail_lock) is answered with status. */
static int open_failing(uint32_t status, bool fail_lock)
{
	struct fixture f;
	struct call c;
	uint8_t geom[8];
	struct req r;
	int fd, ret;

	fixture_init(&f);
	c = (struct call){.kind = CALL_OPEN, .dev = &f.dev};
	call_start(&c);
	fd = d_accept(f.lfd);
	d_expect_req(fd, &r, ASPEN_OP_OPEN);
	if (fail_lock) {
		put_u32(geom, BLOCK_OBJECT_SIZE);
		put_u32(geom + 4, BLOCK_SIZE);
		d_reply(fd, r.op, ASPEN_ST_OK, r.id, geom, sizeof(geom));
		req_free(&r);
		d_expect_req(fd, &r, ASPEN_OP_LOCK);
	}
	d_reply_err(fd, &r, status);
	req_free(&r);
	ret = call_join(&c);
	d_expect_eof(fd);
	CHECK(tcmur_dev_get_private(&f.dev) == NULL);
	CHECK(stub_conn_lost_count() == 0);
	close(fd);
	fixture_done(&f);
	return ret;
}

static void test_open_failures(void)
{
	struct tcmu_device dev;
	char cfg[160];

	CHECK(open_failing(ASPEN_ST_NOT_FOUND, false) == -ENOENT);
	CHECK(open_failing(ASPEN_ST_SIZE_MISMATCH, false) == -EINVAL);
	CHECK(open_failing(ASPEN_ST_INTERNAL, false) == -EIO);
	CHECK(open_failing(ASPEN_ST_FENCED, true) == -EIO);

	/* Nothing listening. */
	snprintf(cfg, sizeof(cfg), "aspen/disk1@%s/missing.sock", tmpdir);
	stub_dev_init(&dev, cfg, NUM_LBAS, BLOCK_SIZE);
	CHECK(handler->open(&dev, false) == -EIO);
	CHECK(tcmur_dev_get_private(&dev) == NULL);
	stub_dev_free(&dev);

	/* Bad cfgstring. */
	stub_dev_init(&dev, "aspen/", NUM_LBAS, BLOCK_SIZE);
	CHECK(handler->open(&dev, false) == -EINVAL);
	stub_dev_free(&dev);
}

/* Sends a one-iovec write with the given CDB and returns the flags the daemon saw. */
static uint32_t write_flags(struct fixture *f, uint8_t opcode, uint8_t byte1)
{
	struct test_cmd t;
	struct tcmur_cmd *cmd = make_cmd(&t, opcode, byte1);
	uint8_t data[BLOCK_SIZE];
	struct iovec iov = {.iov_base = data, .iov_len = sizeof(data)};
	struct req r;
	uint32_t flags;

	memset(data, 0x5A, sizeof(data));
	CHECK(handler->write(&f->dev, cmd, &iov, 1, sizeof(data), 0) == TCMU_STS_OK);
	d_expect_req(f->dfd, &r, ASPEN_OP_WRITE);
	flags = r.flags;
	d_reply(f->dfd, r.op, ASPEN_ST_OK, r.id, NULL, 0);
	req_free(&r);
	CHECK(wait_complete(cmd) == TCMU_STS_OK);
	return flags;
}

static void test_fua(void)
{
	struct fixture f;

	fixture_init(&f);
	do_open(&f, false);
	CHECK(write_flags(&f, 0x8A, 0x08) == ASPEN_FLAG_FUA); /* WRITE_16 + FUA */
	CHECK(write_flags(&f, 0x2A, 0x08) == ASPEN_FLAG_FUA); /* WRITE_10 + FUA */
	CHECK(write_flags(&f, 0xAA, 0x08) == ASPEN_FLAG_FUA); /* WRITE_12 + FUA */
	CHECK(write_flags(&f, 0x2A, 0x00) == 0);              /* WRITE_10 */
	CHECK(write_flags(&f, 0x8A, 0x00) == 0);              /* WRITE_16 */
	CHECK(write_flags(&f, 0x04, 0x08) == 0);              /* FORMAT UNIT */
	do_close(&f);
	fixture_done(&f);
}

static void test_write_iovecs(void)
{
	struct fixture f;
	struct test_cmd t;
	struct tcmur_cmd *cmd;
	uint8_t a[100], b[1000], c[948];
	struct iovec iov[3] = {{a, sizeof(a)}, {b, sizeof(b)}, {c, sizeof(c)}};
	const size_t len = sizeof(a) + sizeof(b) + sizeof(c);
	struct req r;

	for (size_t i = 0; i < sizeof(a); i++)
		a[i] = (uint8_t)i;
	for (size_t i = 0; i < sizeof(b); i++)
		b[i] = (uint8_t)(i + sizeof(a));
	for (size_t i = 0; i < sizeof(c); i++)
		c[i] = (uint8_t)(i + sizeof(a) + sizeof(b));

	fixture_init(&f);
	do_open(&f, false);
	cmd = make_cmd(&t, 0x2A, 0);
	CHECK(handler->write(&f.dev, cmd, iov, 3, len, 8192) == TCMU_STS_OK);
	d_expect_req(f.dfd, &r, ASPEN_OP_WRITE);
	CHECK(r.offset == 8192);
	CHECK(r.length == len);
	CHECK(r.payload_len == len);
	for (size_t i = 0; i < len; i++)
		CHECK(r.payload[i] == (uint8_t)i);
	d_reply(f.dfd, r.op, ASPEN_ST_OK, r.id, NULL, 0);
	req_free(&r);
	CHECK(wait_complete(cmd) == TCMU_STS_OK);
	do_close(&f);
	fixture_done(&f);
}

static void test_read(void)
{
	struct fixture f;
	struct test_cmd t;
	struct tcmur_cmd *cmd;
	uint8_t a[7], b[500], c[517];
	struct iovec iov[3] = {{a, sizeof(a)}, {b, sizeof(b)}, {c, sizeof(c)}};
	const size_t len = sizeof(a) + sizeof(b) + sizeof(c);
	uint8_t data[sizeof(a) + sizeof(b) + sizeof(c)];
	struct req r;

	for (size_t i = 0; i < len; i++)
		data[i] = (uint8_t)(i * 7 + 3);

	fixture_init(&f);
	do_open(&f, false);
	cmd = make_cmd(&t, 0x28, 0);
	CHECK(handler->read(&f.dev, cmd, iov, 3, len, 4096) == TCMU_STS_OK);
	d_expect_req(f.dfd, &r, ASPEN_OP_READ);
	CHECK(r.offset == 4096);
	CHECK(r.length == len);
	CHECK(r.payload_len == 0);
	d_reply(f.dfd, r.op, ASPEN_ST_OK, r.id, data, (uint32_t)len);
	req_free(&r);
	CHECK(wait_complete(cmd) == TCMU_STS_OK);
	CHECK(memcmp(a, data, sizeof(a)) == 0);
	CHECK(memcmp(b, data + sizeof(a), sizeof(b)) == 0);
	CHECK(memcmp(c, data + sizeof(a) + sizeof(b), sizeof(c)) == 0);
	do_close(&f);
	fixture_done(&f);
}

static void test_out_of_order(void)
{
	enum { N = 3 };
	struct fixture f;
	struct test_cmd t[N];
	struct tcmur_cmd *cmd[N];
	uint8_t buf[N][BLOCK_SIZE];
	uint8_t data[BLOCK_SIZE];
	struct iovec iov[N];
	struct req r[N];
	const int order[N] = {2, 0, 1};

	fixture_init(&f);
	do_open(&f, false);
	for (int i = 0; i < N; i++) {
		cmd[i] = make_cmd(&t[i], 0x28, 0);
		iov[i] = (struct iovec){buf[i], BLOCK_SIZE};
		CHECK(handler->read(&f.dev, cmd[i], &iov[i], 1, BLOCK_SIZE,
				    (off_t)i * BLOCK_SIZE) == TCMU_STS_OK);
	}
	for (int i = 0; i < N; i++) {
		d_expect_req(f.dfd, &r[i], ASPEN_OP_READ);
		CHECK(r[i].offset == (uint64_t)i * BLOCK_SIZE);
	}
	for (int k = 0; k < N; k++) {
		int i = order[k];

		memset(data, 0x10 + i, sizeof(data));
		d_reply(f.dfd, ASPEN_OP_READ, ASPEN_ST_OK, r[i].id, data, sizeof(data));
		CHECK(wait_complete(cmd[i]) == TCMU_STS_OK);
		CHECK(memcmp(buf[i], data, sizeof(data)) == 0);
		req_free(&r[i]);
	}
	do_close(&f);
	fixture_done(&f);
}

/* Issues op through the matching callout, answers with status and returns the completion rc. */
static int io_with_status(struct fixture *f, uint8_t op, uint32_t status)
{
	struct test_cmd t;
	struct tcmur_cmd *cmd = make_cmd(&t, op == ASPEN_OP_READ ? 0x28 : 0x2A, 0);
	uint8_t data[BLOCK_SIZE] = {0};
	struct iovec iov = {data, sizeof(data)};
	struct req r;

	switch (op) {
	case ASPEN_OP_READ:
		CHECK(handler->read(&f->dev, cmd, &iov, 1, sizeof(data), 0) == TCMU_STS_OK);
		break;
	case ASPEN_OP_WRITE:
		CHECK(handler->write(&f->dev, cmd, &iov, 1, sizeof(data), 0) == TCMU_STS_OK);
		break;
	case ASPEN_OP_FLUSH:
		CHECK(handler->flush(&f->dev, cmd) == TCMU_STS_OK);
		break;
	case ASPEN_OP_UNMAP:
		CHECK(handler->unmap(&f->dev, cmd, 0, BLOCK_SIZE) == TCMU_STS_OK);
		break;
	}
	d_expect_req(f->dfd, &r, op);
	if (status == ASPEN_ST_OK) {
		d_reply(f->dfd, op, status, r.id, data, op == ASPEN_OP_READ ? sizeof(data) : 0);
	} else {
		d_reply_err(f->dfd, &r, status);
	}
	req_free(&r);
	return wait_complete(cmd);
}

static void test_status_mapping(void)
{
	static const struct {
		uint32_t status;
		int read_rc;
		int other_rc;
	} table[] = {
		{ASPEN_ST_OK, TCMU_STS_OK, TCMU_STS_OK},
		{ASPEN_ST_INVALID_REQUEST, TCMU_STS_INVALID_CDB, TCMU_STS_INVALID_CDB},
		{ASPEN_ST_OUT_OF_RANGE, TCMU_STS_RANGE, TCMU_STS_RANGE},
		{ASPEN_ST_FENCED, TCMU_STS_FENCED, TCMU_STS_FENCED},
		{ASPEN_ST_NOT_OWNER, TCMU_STS_FENCED, TCMU_STS_FENCED},
		{ASPEN_ST_DISK_FAILED, TCMU_STS_RD_ERR, TCMU_STS_WR_ERR},
		{ASPEN_ST_NOT_FOUND, TCMU_STS_HW_ERR, TCMU_STS_HW_ERR},
		{ASPEN_ST_SIZE_MISMATCH, TCMU_STS_HW_ERR, TCMU_STS_HW_ERR},
		{ASPEN_ST_PROTOCOL_ERROR, TCMU_STS_HW_ERR, TCMU_STS_HW_ERR},
		{ASPEN_ST_INTERNAL, TCMU_STS_HW_ERR, TCMU_STS_HW_ERR},
		{1000, TCMU_STS_HW_ERR, TCMU_STS_HW_ERR},
	};
	static const uint8_t others[] = {ASPEN_OP_WRITE, ASPEN_OP_FLUSH, ASPEN_OP_UNMAP};
	struct fixture f;

	fixture_init(&f);
	do_open(&f, false);
	for (size_t i = 0; i < sizeof(table) / sizeof(table[0]); i++) {
		CHECK(io_with_status(&f, ASPEN_OP_READ, table[i].status) == table[i].read_rc);
		for (size_t k = 0; k < sizeof(others); k++)
			CHECK(io_with_status(&f, others[k], table[i].status) == table[i].other_rc);
	}
	CHECK(stub_conn_lost_count() == 0);
	do_close(&f);
	fixture_done(&f);
}

static void test_flush(void)
{
	struct fixture f;
	struct test_cmd t;
	struct tcmur_cmd *cmd;
	struct req r;

	fixture_init(&f);
	do_open(&f, false);
	cmd = make_cmd(&t, 0x35, 0);
	CHECK(handler->flush(&f.dev, cmd) == TCMU_STS_OK);
	d_expect_req(f.dfd, &r, ASPEN_OP_FLUSH);
	CHECK(r.payload_len == 0);
	d_reply(f.dfd, r.op, ASPEN_ST_OK, r.id, NULL, 0);
	req_free(&r);
	CHECK(wait_complete(cmd) == TCMU_STS_OK);
	do_close(&f);
	fixture_done(&f);
}

static void test_unmap(void)
{
	struct fixture f;
	struct test_cmd t;
	struct tcmur_cmd *cmd;
	struct req r;

	fixture_init(&f);
	do_open(&f, false);
	cmd = make_cmd(&t, 0x42, 0);
	CHECK(handler->unmap(&f.dev, cmd, 0x123456789000ULL, 0x40000ULL) == TCMU_STS_OK);
	d_expect_req(f.dfd, &r, ASPEN_OP_UNMAP);
	CHECK(r.payload_len == 16);
	CHECK(get_u64(r.payload) == 0x123456789000ULL);
	CHECK(get_u64(r.payload + 8) == 0x40000ULL);
	d_reply(f.dfd, r.op, ASPEN_ST_OK, r.id, NULL, 0);
	req_free(&r);
	CHECK(wait_complete(cmd) == TCMU_STS_OK);
	do_close(&f);
	fixture_done(&f);
}

static int reconfig_size(struct fixture *f, uint64_t size, uint32_t status)
{
	struct call c = {.kind = CALL_RECONFIG, .dev = &f->dev};
	struct req r;

	c.cfg.type = TCMULIB_CFG_DEV_SIZE;
	c.cfg.data.dev_size = size;
	call_start(&c);
	d_expect_req(f->dfd, &r, ASPEN_OP_RESIZE);
	CHECK(r.offset == size);
	CHECK(r.payload_len == 0);
	if (status == ASPEN_ST_OK)
		d_reply(f->dfd, r.op, status, r.id, NULL, 0);
	else
		d_reply_err(f->dfd, &r, status);
	req_free(&r);
	return call_join(&c);
}

static void test_reconfig(void)
{
	struct fixture f;
	struct tcmulib_cfg_info cfg = {.type = TCMULIB_CFG_WRITE_CACHE};

	fixture_init(&f);
	do_open(&f, false);
	CHECK(reconfig_size(&f, 4ULL << 20, ASPEN_ST_OK) == 0);
	CHECK(reconfig_size(&f, 1ULL << 20, ASPEN_ST_INVALID_REQUEST) == -EINVAL);
	CHECK(reconfig_size(&f, 8ULL << 20, ASPEN_ST_DISK_FAILED) == -EINVAL);
	CHECK(handler->reconfig(&f.dev, &cfg) == -EOPNOTSUPP);
	cfg.type = TCMULIB_CFG_DEV_CFGSTR;
	CHECK(handler->reconfig(&f.dev, &cfg) == -EOPNOTSUPP);
	d_expect_quiet(f.dfd);
	do_close(&f);
	fixture_done(&f);
}

/* After a loss: a new write is refused with BUSY, close sends nothing, and a reopen works. */
static void after_loss(struct fixture *f)
{
	struct test_cmd t;
	struct tcmur_cmd *cmd = make_cmd(&t, 0x2A, 0);
	uint8_t data[BLOCK_SIZE] = {0};
	struct iovec iov = {data, sizeof(data)};
	struct call c = {.kind = CALL_CLOSE, .dev = &f->dev};

	CHECK(handler->write(&f->dev, cmd, &iov, 1, sizeof(data), 0) == TCMU_STS_BUSY);
	CHECK(handler->flush(&f->dev, cmd) == TCMU_STS_BUSY);
	CHECK(handler->read(&f->dev, cmd, &iov, 1, sizeof(data), 0) == TCMU_STS_BUSY);
	CHECK(handler->unmap(&f->dev, cmd, 0, BLOCK_SIZE) == TCMU_STS_BUSY);

	/* The runner closes the device and reopens it. */
	call_start(&c);
	CHECK(call_join(&c) == 0);
	CHECK(tcmur_dev_get_private(&f->dev) == NULL);
	close(f->dfd);
	f->dfd = -1;
	CHECK(stub_conn_lost_count() == 1);

	do_open(f, true);
	CHECK(write_flags(f, 0x2A, 0) == 0);
	do_close(f);
	CHECK(stub_conn_lost_count() == 1);
}

static void test_disconnect(void)
{
	struct fixture f;
	struct test_cmd t[2];
	struct tcmur_cmd *w, *rd;
	uint8_t wdata[BLOCK_SIZE] = {0}, rdata[BLOCK_SIZE];
	struct iovec wiov = {wdata, sizeof(wdata)}, riov = {rdata, sizeof(rdata)};
	struct req r;

	fixture_init(&f);
	do_open(&f, false);
	w = make_cmd(&t[0], 0x2A, 0);
	rd = make_cmd(&t[1], 0x28, 0);
	CHECK(handler->write(&f.dev, w, &wiov, 1, sizeof(wdata), 0) == TCMU_STS_OK);
	CHECK(handler->read(&f.dev, rd, &riov, 1, sizeof(rdata), 0) == TCMU_STS_OK);
	d_expect_req(f.dfd, &r, ASPEN_OP_WRITE);
	req_free(&r);
	d_expect_req(f.dfd, &r, ASPEN_OP_READ);
	req_free(&r);

	shutdown(f.dfd, SHUT_RDWR);
	CHECK(wait_complete(w) == TCMU_STS_BUSY);
	CHECK(wait_complete(rd) == TCMU_STS_BUSY);
	CHECK(stub_wait_conn_lost(1, WAIT_MS) == 0);
	after_loss(&f);
	fixture_done(&f);
}

/* A reply whose payload is larger than the request allows is a protocol error. */
static void test_oversized_reply(void)
{
	struct fixture f;
	struct test_cmd t;
	struct tcmur_cmd *cmd;
	uint8_t buf[2 * BLOCK_SIZE];
	uint8_t big[2 * BLOCK_SIZE] = {0};
	struct iovec iov = {buf, sizeof(buf)}; /* room for the excess: only the length check stops it */
	struct req r;

	fixture_init(&f);
	do_open(&f, false);
	cmd = make_cmd(&t, 0x28, 0);
	CHECK(handler->read(&f.dev, cmd, &iov, 1, BLOCK_SIZE, 0) == TCMU_STS_OK);
	d_expect_req(f.dfd, &r, ASPEN_OP_READ);
	d_reply(f.dfd, r.op, ASPEN_ST_OK, r.id, big, sizeof(big));
	req_free(&r);
	CHECK(wait_complete(cmd) == TCMU_STS_BUSY);
	CHECK(stub_wait_conn_lost(1, WAIT_MS) == 0);
	after_loss(&f);
	fixture_done(&f);
}

/* A reply for an id that is not pending is a protocol error. */
static void test_unknown_id(void)
{
	struct fixture f;
	struct test_cmd t;
	struct tcmur_cmd *cmd;
	uint8_t buf[BLOCK_SIZE];
	struct iovec iov = {buf, sizeof(buf)};
	struct req r;

	fixture_init(&f);
	do_open(&f, false);
	cmd = make_cmd(&t, 0x28, 0);
	CHECK(handler->read(&f.dev, cmd, &iov, 1, sizeof(buf), 0) == TCMU_STS_OK);
	d_expect_req(f.dfd, &r, ASPEN_OP_READ);
	d_reply(f.dfd, ASPEN_OP_FLUSH, ASPEN_ST_OK, r.id + 1000, NULL, 0);
	req_free(&r);
	CHECK(wait_complete(cmd) == TCMU_STS_BUSY);
	CHECK(stub_wait_conn_lost(1, WAIT_MS) == 0);
	after_loss(&f);
	fixture_done(&f);
}

static void test_close(void)
{
	struct fixture f;
	struct call c;
	struct req r;

	fixture_init(&f);
	do_open(&f, false);
	c = (struct call){.kind = CALL_CLOSE, .dev = &f.dev};
	call_start(&c);
	d_expect_req(f.dfd, &r, ASPEN_OP_UNLOCK);
	CHECK(r.payload_len == 0);
	d_reply(f.dfd, r.op, ASPEN_ST_OK, r.id, NULL, 0);
	req_free(&r);
	CHECK(call_join(&c) == 0);
	d_expect_eof(f.dfd);
	CHECK(tcmur_dev_get_private(&f.dev) == NULL);
	CHECK(stub_conn_lost_count() == 0);
	fixture_done(&f);
}

/* The daemon goes away while close waits for the UNLOCK reply: close still returns, quietly. */
static void test_close_without_reply(void)
{
	struct fixture f;
	struct call c;
	struct req r;

	fixture_init(&f);
	do_open(&f, false);
	c = (struct call){.kind = CALL_CLOSE, .dev = &f.dev};
	call_start(&c);
	d_expect_req(f.dfd, &r, ASPEN_OP_UNLOCK);
	req_free(&r);
	shutdown(f.dfd, SHUT_RDWR);
	CHECK(call_join(&c) == 0);
	CHECK(tcmur_dev_get_private(&f.dev) == NULL);
	CHECK(stub_conn_lost_count() == 0);
	fixture_done(&f);
}

static void test_handler_registration(void)
{
	CHECK(strcmp(handler->name, "Aspen virtual disk") == 0);
	CHECK(strcmp(handler->subtype, "aspen") == 0);
	CHECK(strcmp(handler->cfg_desc, "<disk>[@<socket path>]") == 0);
	CHECK(handler->nr_threads == 0);
	CHECK(handler->handle_cmd == NULL);
}

#define RUN(fn) \
	do { \
		current_test = #fn; \
		fn(); \
		printf("test_handler: %s ok\n", #fn); \
	} while (0)

int main(void)
{
	strcpy(tmpdir, "/tmp/aspen-th-XXXXXX");
	CHECK(mkdtemp(tmpdir) != NULL);
	snprintf(sock_path, sizeof(sock_path), "%s/vdisk.sock", tmpdir);
	handler = aspen_handler_for_test();

	RUN(test_handler_registration);
	RUN(test_check_config);
	RUN(test_open);
	RUN(test_open_failures);
	RUN(test_fua);
	RUN(test_write_iovecs);
	RUN(test_read);
	RUN(test_out_of_order);
	RUN(test_status_mapping);
	RUN(test_flush);
	RUN(test_unmap);
	RUN(test_reconfig);
	RUN(test_disconnect);
	RUN(test_oversized_reply);
	RUN(test_unknown_id);
	RUN(test_close);
	RUN(test_close_without_reply);

	unlink(sock_path);
	rmdir(tmpdir);
	printf("test_handler: all passed\n");
	return 0;
}
