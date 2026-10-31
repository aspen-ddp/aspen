/* Test-only stand-ins for the tcmu-runner and libtcmu functions handler_aspen.c calls.
 * libtcmu keeps struct tcmu_device opaque, so the stub defines its own. */
#ifndef TCMU_STUB_H
#define TCMU_STUB_H

#include <stdbool.h>
#include <stdint.h>

struct tcmu_device {
	char *cfgstring;
	uint64_t num_lbas;
	uint32_t block_size;
	void *runner_private;

	/* What the handler set through the tcmu_dev_set_* calls. */
	bool write_cache;
	bool unmap_enabled;
	uint32_t opt_unmap_gran;
	bool opt_unmap_split;
	uint32_t unmap_gran_align;
};

void stub_dev_init(struct tcmu_device *dev, const char *cfgstring, uint64_t num_lbas,
		   uint32_t block_size);
void stub_dev_free(struct tcmu_device *dev);

/* Waits up to timeout_ms for cmd to be completed and consumes that completion.
 * Returns 0 with *rc set, or -1 on timeout. */
int stub_wait_complete(void *cmd, int *rc, int timeout_ms);

/* Completions recorded and not yet consumed by stub_wait_complete. */
int stub_unconsumed_completions(void);

int stub_conn_lost_count(void);
void stub_reset_conn_lost(void);

/* Waits up to timeout_ms for the conn_lost count to reach n. Returns 0, or -1 on timeout. */
int stub_wait_conn_lost(int n, int timeout_ms);

#endif
