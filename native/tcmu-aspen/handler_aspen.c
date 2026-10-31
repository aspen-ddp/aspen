/* tcmu-runner handler that forwards SCSI block I/O to the aspen vdisk bridge daemon over a Unix
 * socket (wire format: aspen_proto.h). The only SCSI logic here is reading the FUA bit.
 *
 * Per device: one socket, one reader thread, and a table of pending requests keyed by request id.
 * I/O callouts queue a request and return TCMU_STS_OK; the reader completes the command with
 * tcmur_cmd_complete when the reply arrives. On connection loss every pending command completes
 * with TCMU_STS_BUSY, the runner is told with tcmu_notify_conn_lost, and new I/O is refused with
 * TCMU_STS_BUSY until the runner closes and reopens the device. tcmu-runner drops a notification
 * while the device is already in recovery (for example when the connection is lost while the
 * recovery that opened it is still finishing) or has no enabled TPG port, so every refused
 * request notifies again unless close is running; the runner ignores the repeats it does not need.
 *
 * UNMAP: the unmap granularity alignment is one block object, like the optimal granularity, and
 * split_unmaps is off. A plain UNMAP therefore reaches aspen_unmap as whole ranges. A WRITE SAME
 * with the UNMAP bit (Linux's WRITE ZEROES) reaches aspen_unmap only when its start and length are
 * block-object aligned; otherwise tcmu-runner emulates it by writing zeros through aspen_write,
 * which is correct but does not release block objects. The alignment must not be 0: tcmu-runner
 * 1.5.4 divides by it in handle_unmap_in_writesame. */
#define _GNU_SOURCE
#include <errno.h>
#include <limits.h>
#include <pthread.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/uio.h>
#include <sys/un.h>
#include <unistd.h>

#include "tcmu-runner.h"
#include "tcmur_device.h"
#include "libtcmu.h"

#include "aspen_proto.h"

#define MAX_PAYLOAD (64u << 20)
#define MAX_ERROR_PAYLOAD 1024u
#define OPEN_REPLY_SIZE 8u

struct pending {
	struct pending *next;
	uint64_t id;
	uint8_t op;

	/* Asynchronous I/O: the command to complete, and for READ where the data goes. */
	struct tcmur_cmd *cmd;
	struct iovec *iov;
	size_t iov_cnt;
	uint32_t len;

	/* Synchronous request (cmd == NULL): the waiter sleeps on aspen_dev.cond until done. */
	bool done;
	bool lost;
	uint32_t status;
};

struct aspen_dev {
	struct tcmu_device *dev;
	int fd;
	pthread_t reader;
	pthread_mutex_t send_lock; /* serializes whole requests on fd */
	pthread_mutex_t lock;      /* everything below */
	pthread_cond_t cond;
	struct pending *pending;
	uint64_t next_id;
	bool lost;    /* the reader has stopped; refuse new requests */
	bool closing; /* close is running; don't report the loss */
};

/* ---------------------------------------------------------------- socket I/O */

static int read_full(int fd, void *buf, size_t len)
{
	uint8_t *p = buf;

	while (len > 0) {
		ssize_t n = read(fd, p, len);

		if (n < 0 && errno == EINTR)
			continue;
		if (n <= 0)
			return -1;
		p += n;
		len -= (size_t)n;
	}
	return 0;
}

static int skip_bytes(int fd, uint32_t len)
{
	uint8_t buf[MAX_ERROR_PAYLOAD];

	while (len > 0) {
		uint32_t n = len < sizeof(buf) ? len : (uint32_t)sizeof(buf);

		if (read_full(fd, buf, n))
			return -1;
		len -= n;
	}
	return 0;
}

/* Reads exactly len bytes into the iovecs. */
static int read_into_iov(int fd, const struct iovec *iov, size_t cnt, size_t len)
{
	for (size_t i = 0; i < cnt && len > 0; i++) {
		size_t n = iov[i].iov_len < len ? iov[i].iov_len : len;

		if (read_full(fd, iov[i].iov_base, n))
			return -1;
		len -= n;
	}
	return len == 0 ? 0 : -1;
}

/* Sends every byte of the iovecs, which it consumes. */
static int send_all(int fd, struct iovec *iov, size_t cnt)
{
	while (cnt > 0) {
		struct msghdr msg = {.msg_iov = iov, .msg_iovlen = cnt < IOV_MAX ? cnt : IOV_MAX};
		ssize_t n;

		if (iov->iov_len == 0) {
			iov++;
			cnt--;
			continue;
		}
		n = sendmsg(fd, &msg, MSG_NOSIGNAL);
		if (n < 0 && errno == EINTR)
			continue;
		if (n < 0)
			return -1;
		while (n > 0) {
			size_t step = (size_t)n < iov->iov_len ? (size_t)n : iov->iov_len;

			iov->iov_base = (uint8_t *)iov->iov_base + step;
			iov->iov_len -= step;
			n -= (ssize_t)step;
			if (iov->iov_len == 0) {
				iov++;
				cnt--;
			}
		}
	}
	return 0;
}

/* Sends the header and the first h->payload_len bytes of data, without copying the data. */
static int send_req(struct aspen_dev *ad, const struct aspen_req_hdr *h, const struct iovec *data,
		    size_t data_cnt)
{
	uint8_t hdr[ASPEN_REQ_HDR_SIZE];
	struct iovec local[8];
	struct iovec *iov = local;
	size_t n = 0;
	size_t left = h->payload_len;
	int ret;

	if (data_cnt + 1 > sizeof(local) / sizeof(local[0])) {
		iov = malloc((data_cnt + 1) * sizeof(*iov));
		if (iov == NULL)
			return -1;
	}
	aspen_encode_req(hdr, h);
	iov[n++] = (struct iovec){.iov_base = hdr, .iov_len = sizeof(hdr)};
	for (size_t i = 0; i < data_cnt && left > 0; i++) {
		size_t l = data[i].iov_len < left ? data[i].iov_len : left;

		iov[n++] = (struct iovec){.iov_base = data[i].iov_base, .iov_len = l};
		left -= l;
	}

	if (left > 0) {
		ret = -1;
	} else {
		pthread_mutex_lock(&ad->send_lock);
		ret = send_all(ad->fd, iov, n);
		pthread_mutex_unlock(&ad->send_lock);
	}
	if (iov != local)
		free(iov);
	return ret;
}

static int recv_resp_hdr(int fd, struct aspen_resp_hdr *h)
{
	uint8_t buf[ASPEN_RESP_HDR_SIZE];

	if (read_full(fd, buf, sizeof(buf)))
		return -1;
	return aspen_decode_resp(buf, h);
}

/* ---------------------------------------------------------------- status mapping */

static int tcmu_status(uint8_t op, uint32_t status)
{
	switch (status) {
	case ASPEN_ST_OK:
		return TCMU_STS_OK;
	case ASPEN_ST_INVALID_REQUEST:
		return TCMU_STS_INVALID_CDB;
	case ASPEN_ST_OUT_OF_RANGE:
		return TCMU_STS_RANGE;
	case ASPEN_ST_FENCED:
	case ASPEN_ST_NOT_OWNER:
		return TCMU_STS_FENCED;
	case ASPEN_ST_DISK_FAILED:
		return op == ASPEN_OP_READ ? TCMU_STS_RD_ERR : TCMU_STS_WR_ERR;
	default:
		return TCMU_STS_HW_ERR;
	}
}

/* ---------------------------------------------------------------- pending table */

static struct pending *take_pending(struct aspen_dev *ad, uint64_t id)
{
	struct pending **pp;
	struct pending *p = NULL;

	pthread_mutex_lock(&ad->lock);
	for (pp = &ad->pending; *pp != NULL; pp = &(*pp)->next) {
		if ((*pp)->id == id) {
			p = *pp;
			*pp = p->next;
			break;
		}
	}
	pthread_mutex_unlock(&ad->lock);
	return p;
}

/* Adds p to the table and returns its new id, or 0 if the connection is already lost.
 * Once added, p may be completed (and an async p freed) by the reader at any time.
 * A refused request re-sends the loss notification unless close is running: tcmu-runner may
 * have dropped the earlier one, and without it the device would stay BUSY forever. */
static uint64_t add_pending(struct aspen_dev *ad, struct pending *p)
{
	uint64_t id = 0;
	bool notify = false;

	pthread_mutex_lock(&ad->lock);
	if (!ad->lost) {
		id = p->id = ad->next_id++;
		p->next = ad->pending;
		ad->pending = p;
	} else {
		notify = !ad->closing;
	}
	pthread_mutex_unlock(&ad->lock);
	if (notify)
		tcmu_notify_conn_lost(ad->dev);
	return id;
}

/* Completes a request taken out of the table. Called without ad->lock held. */
static void finish(struct aspen_dev *ad, struct pending *p, bool lost, uint32_t status)
{
	if (p->cmd != NULL) {
		int rc = lost ? TCMU_STS_BUSY : tcmu_status(p->op, status);

		tcmur_cmd_complete(ad->dev, p->cmd, rc);
		free(p);
		return;
	}
	pthread_mutex_lock(&ad->lock);
	p->lost = lost;
	p->status = status;
	p->done = true;
	pthread_cond_broadcast(&ad->cond);
	pthread_mutex_unlock(&ad->lock);
}

/* Fails every pending request, plus taken (a request the reader already took out of the table,
 * or NULL). The runner is notified before any command completes, as target.c asks. */
static void connection_lost(struct aspen_dev *ad, struct pending *taken)
{
	struct pending *p, *next, *async = NULL;
	bool notify;

	pthread_mutex_lock(&ad->lock);
	ad->lost = true;
	notify = !ad->closing;
	for (p = ad->pending; p != NULL; p = next) {
		next = p->next;
		if (p->cmd != NULL) {
			p->next = async;
			async = p;
		} else {
			/* Lives on the waiter's stack: don't touch it after unlocking. */
			p->lost = true;
			p->done = true;
		}
	}
	ad->pending = NULL;
	pthread_cond_broadcast(&ad->cond);
	pthread_mutex_unlock(&ad->lock);

	if (notify)
		tcmu_notify_conn_lost(ad->dev);
	if (taken != NULL)
		finish(ad, taken, true, 0);
	for (p = async; p != NULL; p = next) {
		next = p->next;
		finish(ad, p, true, 0);
	}
}

/* ---------------------------------------------------------------- reader thread */

/* The payload length a reply to p may carry: exact for Ok, at most the message size otherwise. */
static bool payload_len_ok(const struct pending *p, const struct aspen_resp_hdr *h)
{
	if (h->status != ASPEN_ST_OK)
		return h->payload_len <= MAX_ERROR_PAYLOAD;
	return h->payload_len == (p->op == ASPEN_OP_READ ? p->len : 0);
}

static void *reader_main(void *arg)
{
	struct aspen_dev *ad = arg;
	struct aspen_resp_hdr h;
	struct pending *p = NULL;
	int ret;

	while (recv_resp_hdr(ad->fd, &h) == 0) {
		p = take_pending(ad, h.id);
		if (p == NULL) {
			tcmu_dev_err(ad->dev, "reply for unknown request %llu\n",
				     (unsigned long long)h.id);
			break;
		}
		if (h.op != p->op || !payload_len_ok(p, &h)) {
			tcmu_dev_err(ad->dev, "bad reply to request %llu: op %u, payload %u bytes\n",
				     (unsigned long long)h.id, h.op, h.payload_len);
			break;
		}
		if (h.status == ASPEN_ST_OK && p->op == ASPEN_OP_READ)
			ret = read_into_iov(ad->fd, p->iov, p->iov_cnt, h.payload_len);
		else
			ret = skip_bytes(ad->fd, h.payload_len);
		if (ret)
			break;
		finish(ad, p, false, h.status);
		p = NULL;
	}
	connection_lost(ad, p);
	return NULL;
}

/* ---------------------------------------------------------------- requests */

/* Queues an I/O request for cmd. Returns TCMU_STS_OK when queued (the reader completes it),
 * otherwise the status to complete the command with now. */
static int queue_io(struct tcmu_device *dev, struct tcmur_cmd *cmd, struct aspen_req_hdr *h,
		    struct iovec *iov, size_t iov_cnt)
{
	struct aspen_dev *ad = tcmur_dev_get_private(dev);
	struct pending *p;

	/* Between close and a successful reopen the runner can still send I/O. */
	if (ad == NULL)
		return TCMU_STS_BUSY;
	p = calloc(1, sizeof(*p));
	if (p == NULL)
		return TCMU_STS_NO_RESOURCE;
	p->op = h->op;
	p->cmd = cmd;
	if (h->op == ASPEN_OP_READ) {
		p->iov = iov;
		p->iov_cnt = iov_cnt;
		p->len = h->length;
	}
	h->id = add_pending(ad, p);
	if (h->id == 0) {
		free(p);
		return TCMU_STS_BUSY;
	}
	if (send_req(ad, h, h->op == ASPEN_OP_READ ? NULL : iov, h->op == ASPEN_OP_READ ? 0 : iov_cnt))
		/* The stream may hold a partial request: drop it, so the reader completes p BUSY. */
		shutdown(ad->fd, SHUT_RDWR);
	return TCMU_STS_OK;
}

/* Sends a request with no payload through the reader and waits for its reply (or for the
 * connection to be lost). Returns the reply status, or -1 if the connection was lost. */
static int64_t sync_request(struct aspen_dev *ad, uint8_t op, uint64_t offset)
{
	struct pending p = {.op = op};
	struct aspen_req_hdr h = {.op = op, .offset = offset};

	h.id = add_pending(ad, &p);
	if (h.id == 0)
		return -1;
	if (send_req(ad, &h, NULL, 0))
		shutdown(ad->fd, SHUT_RDWR);

	pthread_mutex_lock(&ad->lock);
	while (!p.done)
		pthread_cond_wait(&ad->cond, &ad->lock);
	pthread_mutex_unlock(&ad->lock);
	return p.lost ? -1 : (int64_t)p.status;
}

/* Sends a request and reads its reply directly; used by open before the reader starts.
 * Returns the reply status, or -1 if the connection failed. An Ok reply must carry exactly
 * out_len bytes, which are copied to out. An error reply's message (UTF-8, at most
 * MAX_ERROR_PAYLOAD bytes) is read in full and left NUL-terminated in errmsg, with control
 * characters replaced by '?'; errmsg is empty otherwise. */
static int64_t exchange(struct aspen_dev *ad, struct aspen_req_hdr *h, const char *payload,
			uint8_t *out, uint32_t out_len, char errmsg[MAX_ERROR_PAYLOAD + 1])
{
	struct iovec iov = {.iov_base = (void *)payload, .iov_len = h->payload_len};
	struct aspen_resp_hdr r;

	errmsg[0] = '\0';
	h->id = ad->next_id++;
	if (send_req(ad, h, &iov, 1) || recv_resp_hdr(ad->fd, &r))
		return -1;
	if (r.id != h->id || r.op != h->op)
		return -1;
	if (r.status == ASPEN_ST_OK) {
		if (r.payload_len != out_len || read_full(ad->fd, out, out_len))
			return -1;
	} else {
		if (r.payload_len > MAX_ERROR_PAYLOAD || read_full(ad->fd, errmsg, r.payload_len))
			return -1;
		errmsg[r.payload_len] = '\0';
		for (uint32_t i = 0; i < r.payload_len; i++) {
			unsigned char c = (unsigned char)errmsg[i];

			if (c < 0x20 || c == 0x7f)
				errmsg[i] = '?';
		}
	}
	return r.status;
}

/* ---------------------------------------------------------------- device lifetime */

static void free_dev(struct aspen_dev *ad)
{
	if (ad->fd >= 0)
		close(ad->fd);
	pthread_cond_destroy(&ad->cond);
	pthread_mutex_destroy(&ad->lock);
	pthread_mutex_destroy(&ad->send_lock);
	free(ad);
}

static int connect_socket(struct tcmu_device *dev, const char *path)
{
	struct sockaddr_un addr = {.sun_family = AF_UNIX};
	int fd;

	if (strlen(path) >= sizeof(addr.sun_path)) {
		tcmu_dev_err(dev, "socket path too long: %s\n", path);
		return -1;
	}
	strcpy(addr.sun_path, path);
	fd = socket(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0);
	if (fd < 0)
		return -1;
	if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) != 0) {
		tcmu_dev_err(dev, "cannot connect to %s: %s\n", path, strerror(errno));
		close(fd);
		return -1;
	}
	return fd;
}

static uint32_t get_u32(const uint8_t *p)
{
	return ((uint32_t)p[0] << 24) | ((uint32_t)p[1] << 16) | ((uint32_t)p[2] << 8) | p[3];
}

static bool aspen_check_config(const char *cfgstring, char **reason)
{
	char *disk, *socket_path;

	if (aspen_parse_cfgstring(cfgstring, &disk, &socket_path, reason))
		return false;
	free(disk);
	free(socket_path);
	return true;
}

static int aspen_open(struct tcmu_device *dev, bool reopen)
{
	uint32_t block_size = tcmu_dev_get_block_size(dev);
	struct aspen_req_hdr h;
	char *disk, *socket_path, *reason;
	struct aspen_dev *ad;
	uint8_t geom[OPEN_REPLY_SIZE];
	char errmsg[MAX_ERROR_PAYLOAD + 1];
	int64_t st;
	int ret = -EIO;

	(void)reopen;
	if (aspen_parse_cfgstring(tcmu_dev_get_cfgstring(dev), &disk, &socket_path, &reason)) {
		tcmu_dev_err(dev, "%s\n", reason != NULL ? reason : "out of memory");
		free(reason);
		return -EINVAL;
	}

	ad = calloc(1, sizeof(*ad));
	if (ad == NULL)
		goto out;
	ad->dev = dev;
	ad->next_id = 1;
	pthread_mutex_init(&ad->send_lock, NULL);
	pthread_mutex_init(&ad->lock, NULL);
	pthread_cond_init(&ad->cond, NULL);
	ad->fd = connect_socket(dev, socket_path);
	if (ad->fd < 0)
		goto fail;

	h = (struct aspen_req_hdr){.op = ASPEN_OP_OPEN,
				   .offset = tcmu_dev_get_num_lbas(dev) * block_size,
				   .length = block_size,
				   .payload_len = (uint32_t)strlen(disk)};
	st = exchange(ad, &h, disk, geom, sizeof(geom), errmsg);
	if (st != ASPEN_ST_OK) {
		tcmu_dev_err(dev, "OPEN %s failed with status %lld: %s\n", disk, (long long)st,
			     st < 0 ? "connection failed" : errmsg);
		ret = st == ASPEN_ST_NOT_FOUND ? -ENOENT : st == ASPEN_ST_SIZE_MISMATCH ? -EINVAL : -EIO;
		goto fail;
	}

	if (get_u32(geom + 4) != block_size) {
		tcmu_dev_err(dev, "%s has %u byte blocks, tcmu uses %u\n", disk, get_u32(geom + 4),
			     block_size);
		ret = -EINVAL;
		goto fail;
	}

	if (get_u32(geom) < block_size || get_u32(geom) % block_size != 0) {
		tcmu_dev_err(dev, "%s has a %u byte block object, not a multiple of %u byte blocks\n",
			     disk, get_u32(geom), block_size);
		ret = -EINVAL;
		goto fail;
	}

	tcmu_dev_set_write_cache_enabled(dev, true);
	tcmu_dev_set_unmap_enabled(dev, true);
	tcmu_dev_set_opt_unmap_gran(dev, get_u32(geom) / block_size, false);
	/* Never 0: see the UNMAP note at the top of the file. */
	tcmu_dev_set_unmap_gran_align(dev, get_u32(geom) / block_size);

	h = (struct aspen_req_hdr){.op = ASPEN_OP_LOCK};
	st = exchange(ad, &h, NULL, NULL, 0, errmsg);
	if (st != ASPEN_ST_OK) {
		tcmu_dev_err(dev, "LOCK %s failed with status %lld: %s\n", disk, (long long)st,
			     st < 0 ? "connection failed" : errmsg);
		goto fail;
	}

	if (pthread_create(&ad->reader, NULL, reader_main, ad) != 0)
		goto fail;
	tcmur_dev_set_private(dev, ad);
	tcmu_dev_dbg(dev, "opened %s on %s\n", disk, socket_path);
	ret = 0;
	goto out;

fail:
	free_dev(ad);
out:
	free(disk);
	free(socket_path);
	return ret;
}

static void aspen_close(struct tcmu_device *dev)
{
	struct aspen_dev *ad = tcmur_dev_get_private(dev);
	int64_t st;

	if (ad == NULL)
		return;
	pthread_mutex_lock(&ad->lock);
	ad->closing = true;
	pthread_mutex_unlock(&ad->lock);

	st = sync_request(ad, ASPEN_OP_UNLOCK, 0);
	if (st > 0)
		tcmu_dev_err(dev, "UNLOCK failed with status %lld\n", (long long)st);

	shutdown(ad->fd, SHUT_RDWR);
	pthread_join(ad->reader, NULL);
	tcmur_dev_set_private(dev, NULL);
	free_dev(ad);
}

static int aspen_reconfig(struct tcmu_device *dev, struct tcmulib_cfg_info *cfg)
{
	struct aspen_dev *ad = tcmur_dev_get_private(dev);
	int64_t st;

	if (cfg->type != TCMULIB_CFG_DEV_SIZE)
		return -EOPNOTSUPP;
	if (ad == NULL)
		return -EBUSY; /* closed, and not yet reopened */
	st = sync_request(ad, ASPEN_OP_RESIZE, cfg->data.dev_size);
	if (st != ASPEN_ST_OK) {
		tcmu_dev_err(dev, "RESIZE to %llu failed with status %lld\n",
			     (unsigned long long)cfg->data.dev_size, (long long)st);
		return -EINVAL;
	}
	return 0;
}

/* ---------------------------------------------------------------- I/O callouts */

static int aspen_read(struct tcmu_device *dev, struct tcmur_cmd *cmd, struct iovec *iov,
		      size_t iov_cnt, size_t len, off_t off)
{
	struct aspen_req_hdr h = {.op = ASPEN_OP_READ, .offset = (uint64_t)off,
				  .length = (uint32_t)len};

	if (len > MAX_PAYLOAD)
		return TCMU_STS_INVALID_CDB;
	return queue_io(dev, cmd, &h, iov, iov_cnt);
}

static bool fua_requested(const uint8_t *cdb)
{
	bool write_cmd = cdb[0] == 0x2A || cdb[0] == 0xAA || cdb[0] == 0x8A; /* WRITE_10/12/16 */

	return write_cmd && (cdb[1] & 0x08);
}

static int aspen_write(struct tcmu_device *dev, struct tcmur_cmd *cmd, struct iovec *iov,
		       size_t iov_cnt, size_t len, off_t off)
{
	struct aspen_req_hdr h = {.op = ASPEN_OP_WRITE,
				  .flags = fua_requested(cmd->lib_cmd->cdb) ? ASPEN_FLAG_FUA : 0,
				  .offset = (uint64_t)off,
				  .length = (uint32_t)len,
				  .payload_len = (uint32_t)len};

	if (len > MAX_PAYLOAD)
		return TCMU_STS_INVALID_CDB;
	return queue_io(dev, cmd, &h, iov, iov_cnt);
}

static int aspen_flush(struct tcmu_device *dev, struct tcmur_cmd *cmd)
{
	struct aspen_req_hdr h = {.op = ASPEN_OP_FLUSH};

	return queue_io(dev, cmd, &h, NULL, 0);
}

static void put_u64(uint8_t *p, uint64_t v)
{
	for (int i = 7; i >= 0; i--) {
		p[i] = (uint8_t)v;
		v >>= 8;
	}
}

static int aspen_unmap(struct tcmu_device *dev, struct tcmur_cmd *cmd, uint64_t off, uint64_t len)
{
	uint8_t range[16];
	struct iovec iov = {.iov_base = range, .iov_len = sizeof(range)};
	struct aspen_req_hdr h = {.op = ASPEN_OP_UNMAP, .payload_len = sizeof(range)};

	put_u64(range, off);
	put_u64(range + 8, len);
	/* send_req finishes with range before queue_io returns. */
	return queue_io(dev, cmd, &h, &iov, 1);
}

static struct tcmur_handler aspen_handler = {
	.name = "Aspen virtual disk",
	.subtype = "aspen",
	.cfg_desc = "<disk>[@<socket path>]",
	.nr_threads = 0,
	.check_config = aspen_check_config,
	.open = aspen_open,
	.close = aspen_close,
	.read = aspen_read,
	.write = aspen_write,
	.flush = aspen_flush,
	.unmap = aspen_unmap,
	.reconfig = aspen_reconfig,
};

int handler_init(void)
{
	return tcmur_register_handler(&aspen_handler);
}

#ifdef ASPEN_TEST
struct tcmur_handler *aspen_handler_for_test(void)
{
	return &aspen_handler;
}
#endif
