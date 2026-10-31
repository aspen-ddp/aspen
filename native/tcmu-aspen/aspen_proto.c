#include "aspen_proto.h"

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define REQ_MAGIC 0xA5D1u
#define RESP_MAGIC 0xA5D2u
#define VERSION 1u
#define CFG_PREFIX "aspen/"

static void put_u16(uint8_t *p, uint16_t v)
{
	p[0] = (uint8_t)(v >> 8);
	p[1] = (uint8_t)v;
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

static uint16_t get_u16(const uint8_t *p)
{
	return (uint16_t)(((uint16_t)p[0] << 8) | p[1]);
}

static uint32_t get_u32(const uint8_t *p)
{
	return ((uint32_t)p[0] << 24) | ((uint32_t)p[1] << 16) | ((uint32_t)p[2] << 8) | p[3];
}

static uint64_t get_u64(const uint8_t *p)
{
	return ((uint64_t)get_u32(p) << 32) | get_u32(p + 4);
}

void aspen_encode_req(uint8_t out[ASPEN_REQ_HDR_SIZE], const struct aspen_req_hdr *h)
{
	put_u16(out, REQ_MAGIC);
	out[2] = VERSION;
	out[3] = h->op;
	put_u32(out + 4, h->flags);
	put_u64(out + 8, h->id);
	put_u64(out + 16, h->offset);
	put_u32(out + 24, h->length);
	put_u32(out + 28, h->payload_len);
}

int aspen_decode_resp(const uint8_t in[ASPEN_RESP_HDR_SIZE], struct aspen_resp_hdr *h)
{
	if (get_u16(in) != RESP_MAGIC || in[2] != VERSION)
		return -EPROTO;
	h->op = in[3];
	h->status = get_u32(in + 4);
	h->id = get_u64(in + 8);
	h->payload_len = get_u32(in + 16);
	return 0;
}

static int fail(char **reason, const char *msg)
{
	*reason = strdup(msg);
	return -EINVAL;
}

int aspen_parse_cfgstring(const char *cfg, char **disk, char **socket_path, char **reason)
{
	const size_t prefix_len = strlen(CFG_PREFIX);
	const char *name;
	const char *at;
	const char *sock;
	size_t name_len;

	*disk = NULL;
	*socket_path = NULL;
	*reason = NULL;

	if (strncmp(cfg, CFG_PREFIX, prefix_len) != 0)
		return fail(reason, "cfgstring must look like aspen/<disk>[@<socket path>]");

	name = cfg + prefix_len;
	at = strchr(name, '@');
	name_len = at ? (size_t)(at - name) : strlen(name);
	if (name_len == 0)
		return fail(reason, "cfgstring has an empty disk name");

	if (at) {
		sock = at + 1;
		if (*sock == '\0')
			return fail(reason, "cfgstring has an empty socket path after '@'");
	} else {
		sock = ASPEN_DEFAULT_SOCKET;
	}

	*disk = strndup(name, name_len);
	*socket_path = strdup(sock);
	if (*disk == NULL || *socket_path == NULL) {
		free(*disk);
		free(*socket_path);
		*disk = NULL;
		*socket_path = NULL;
		return fail(reason, "out of memory");
	}
	return 0;
}
