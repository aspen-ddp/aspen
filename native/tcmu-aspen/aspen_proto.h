/* Framing for the aspen vdisk bridge protocol (see the JVM codec, vdisk/bridge/Protocol.scala).
 * All integers on the wire are big-endian. No tcmu headers are used here. */
#ifndef ASPEN_PROTO_H
#define ASPEN_PROTO_H

#include <stdint.h>

#define ASPEN_REQ_HDR_SIZE 32
#define ASPEN_RESP_HDR_SIZE 24
#define ASPEN_DEFAULT_SOCKET "/run/aspen/vdisk.sock"

enum aspen_op {
	ASPEN_OP_OPEN = 1,
	ASPEN_OP_READ = 2,
	ASPEN_OP_WRITE = 3,
	ASPEN_OP_FLUSH = 4,
	ASPEN_OP_UNMAP = 5,
	ASPEN_OP_RESIZE = 6,
	ASPEN_OP_LOCK = 7,
	ASPEN_OP_UNLOCK = 8,
	ASPEN_OP_LOCK_STATE = 9
};

enum aspen_status {
	ASPEN_ST_OK = 0,
	ASPEN_ST_INVALID_REQUEST = 1,
	ASPEN_ST_OUT_OF_RANGE = 2,
	ASPEN_ST_FENCED = 3,
	ASPEN_ST_NOT_OWNER = 4,
	ASPEN_ST_DISK_FAILED = 5,
	ASPEN_ST_NOT_FOUND = 6,
	ASPEN_ST_SIZE_MISMATCH = 7,
	ASPEN_ST_PROTOCOL_ERROR = 8,
	ASPEN_ST_INTERNAL = 9
};

#define ASPEN_FLAG_FUA 1u

struct aspen_req_hdr {
	uint8_t op;
	uint32_t flags;
	uint64_t id;
	uint64_t offset;
	uint32_t length;
	uint32_t payload_len;
};

struct aspen_resp_hdr {
	uint8_t op;
	uint32_t status;
	uint64_t id;
	uint32_t payload_len;
};

void aspen_encode_req(uint8_t out[ASPEN_REQ_HDR_SIZE], const struct aspen_req_hdr *h);

/* Returns 0, or -EPROTO on bad magic or version. */
int aspen_decode_resp(const uint8_t in[ASPEN_RESP_HDR_SIZE], struct aspen_resp_hdr *h);

/* cfg is tcmu's full cfgstring ("aspen/<disk>[@<socket>]", possibly followed by tcmu-runner's own
 * ";option=value" suffixes, which are ignored: ours end at the first ';'). Returns 0 with malloc'd *disk and
 * *socket_path, or -EINVAL with *reason set to a malloc'd message. The caller frees the strings. */
int aspen_parse_cfgstring(const char *cfg, char **disk, char **socket_path, char **reason);

#endif
