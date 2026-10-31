#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "aspen_proto.h"

#define CHECK(cond) \
	do { \
		if (!(cond)) { \
			fprintf(stderr, "test_proto: FAILED line %d: %s\n", __LINE__, #cond); \
			exit(1); \
		} \
	} while (0)

static const uint8_t golden_write[36] = {
	0xA5, 0xD1, 0x01, 0x03, 0x00, 0x00, 0x00, 0x01, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
	0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x10, 0x00, 0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x04,
	0xDE, 0xAD, 0xBE, 0xEF};

static const uint8_t golden_read_reply[26] = {
	0xA5, 0xD2, 0x01, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x07,
	0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00, 0x01, 0x02};

static void test_encode(void)
{
	struct aspen_req_hdr h = {.op = ASPEN_OP_WRITE,
				  .flags = ASPEN_FLAG_FUA,
				  .id = 0x0102030405060708ULL,
				  .offset = 4096,
				  .length = 4,
				  .payload_len = 4};
	uint8_t out[ASPEN_REQ_HDR_SIZE];

	aspen_encode_req(out, &h);
	CHECK(memcmp(out, golden_write, ASPEN_REQ_HDR_SIZE) == 0);
}

static void test_decode(void)
{
	struct aspen_resp_hdr h;
	uint8_t bad[ASPEN_RESP_HDR_SIZE];

	CHECK(aspen_decode_resp(golden_read_reply, &h) == 0);
	CHECK(h.op == ASPEN_OP_READ);
	CHECK(h.status == ASPEN_ST_OK);
	CHECK(h.id == 7);
	CHECK(h.payload_len == 2);

	memcpy(bad, golden_read_reply, sizeof(bad));
	bad[1] = 0xD1; /* request magic is not a response magic */
	CHECK(aspen_decode_resp(bad, &h) == -EPROTO);

	memcpy(bad, golden_read_reply, sizeof(bad));
	bad[2] = 2;
	CHECK(aspen_decode_resp(bad, &h) == -EPROTO);
}

static void expect_ok(const char *cfg, const char *disk, const char *sock)
{
	char *d = NULL, *s = NULL, *r = NULL;

	CHECK(aspen_parse_cfgstring(cfg, &d, &s, &r) == 0);
	CHECK(d != NULL && s != NULL);
	CHECK(strcmp(d, disk) == 0);
	CHECK(strcmp(s, sock) == 0);
	free(d);
	free(s);
	free(r);
}

static void expect_invalid(const char *cfg)
{
	char *d = NULL, *s = NULL, *r = NULL;

	CHECK(aspen_parse_cfgstring(cfg, &d, &s, &r) == -EINVAL);
	CHECK(r != NULL && r[0] != '\0');
	free(d);
	free(s);
	free(r);
}

static void test_cfgstring(void)
{
	expect_ok("aspen/disk1@/run/x.sock", "disk1", "/run/x.sock");
	expect_ok("aspen/disk1", "disk1", ASPEN_DEFAULT_SOCKET);
	expect_invalid("aspen/");
	expect_invalid("aspen/@/x");
	expect_invalid("aspen/d@");
	expect_invalid("other/d");
}

int main(void)
{
	test_encode();
	test_decode();
	test_cfgstring();
	printf("test_proto: all passed\n");
	return 0;
}
