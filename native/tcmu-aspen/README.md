# tcmu-aspen

`handler_aspen.so` is a tcmu-runner (1.5.4) handler plugin. LIO hands SCSI commands to tcmu-runner,
which calls this plugin. The plugin forwards block I/O over a Unix socket to the aspen vdisk bridge
daemon (`src/main/scala/org/aspen_ddp/aspen/vdisk/bridge/`). The wire format is defined in
`aspen_proto.h` and in the JVM codec `Protocol.scala`.

## Build

The handler compiles against the tcmu-runner headers in the `submodules/tcmu-runner` submodule
(tag `v1.5.4`):

    git submodule update --init submodules/tcmu-runner
    make -C native/tcmu-aspen

This builds `handler_aspen.so` and `test_proto`. To use a different tcmu-runner source tree, set
`TCMU_SRC=/path/to/tcmu-runner`.

## Test

    make -C native/tcmu-aspen check

`test_proto` checks the framing code. `test_handler` compiles `handler_aspen.c` with
`-DASPEN_TEST` against stub tcmu functions (`tcmu_stub.c`). The test plays the daemon on a Unix
socket in a temporary directory under `/tmp`. Set `ASPEN_TEST_LOG=1` to see the handler's log lines.

    make -C native/tcmu-aspen symcheck

This checks that every symbol `handler_aspen.so` needs, apart from glibc's, is exported by
`/usr/bin/tcmu-runner` or `libtcmu.so.2`. Override the paths with `TCMU_RUNNER=` and `LIBTCMU=`.

## Install

    sudo make -C native/tcmu-aspen install

This copies `handler_aspen.so` to `/usr/lib/x86_64-linux-gnu/tcmu-runner/`. Prefix the path with
`DESTDIR=` to stage the install elsewhere. Restart tcmu-runner afterwards so it loads the handler.

## Configuration string

The backstore is created with subtype `aspen` and the cfgstring

    <disk>[@<socket path>]

tcmu passes it to the handler as `aspen/<disk>[@<socket path>]`. `<disk>` is the vdisk's name in
the aspen disk catalog, and it may not contain `@`. The socket path defaults to
`/run/aspen/vdisk.sock`, and it must fit in a Unix socket address (at most 107 bytes). For example:

    targetcli /backstores/user:aspen create name=vd1 size=10G cfgstring=vd1@/run/aspen/vdisk.sock \
        control=hw_block_size=4096

The size and block size given to tcmu must match the disk's, or open fails with `-EINVAL`; the
kernel's `hw_block_size` defaults to 512 while `create-disk` defaults to 4096. A disk the daemon
does not know fails with `-ENOENT`. For a failed OPEN or LOCK the handler logs the daemon's error
message with the status. Add `hw_max_sectors=<n>` to the control string (comma separated) for
transfers above the default 128 sectors (512 KiB with 4 KiB blocks).

tcmu-runner may append its own options after a `;` (for example `;tcmur_cmd_time_out=30`); the
handler ignores everything from the first `;`.

## Behavior

- **open** sends OPEN and then LOCK. It enables the write cache and UNMAP, with the optimal unmap
  granularity and the unmap granularity alignment both set to one block object. The alignment
  must not be 0: tcmu-runner 1.5.4 divides by it when handling WRITE SAME with the UNMAP bit,
  which Linux sends for every WRITE ZEROES. With `split_unmaps` off, a plain UNMAP reaches the
  handler as whole ranges. An aligned WRITE SAME + UNMAP reaches the handler's unmap; an unaligned
  one falls back to tcmu-runner's WRITE SAME emulation, which writes zeros through write (correct,
  but no block objects are released).
- **READ, WRITE, FLUSH and UNMAP** are queued asynchronously and completed when the daemon replies.
  WRITE carries FUA when the CDB of WRITE(10), WRITE(12) or WRITE(16) has it set. Write data is sent
  without being copied. Known limitation: tcmu-runner 1.5.4's MODE SENSE reports DPOFUA=0, so Linux
  initiators never send FUA; `sd` turns each FUA write into a WRITE followed by SYNCHRONIZE CACHE
  (a full FLUSH). The FUA path is therefore unused with Linux initiators, which matters for the
  part-4 latency measurements.
- **Resize** (`reconfig` with a new device size) sends RESIZE. The daemon re-reads the disk's
  config, so a RESIZE to a size already committed (by `aspen grow-disk`, or by an earlier RESIZE
  whose reply was lost) succeeds.
- **close** sends UNLOCK and waits for the reply.
- **If the connection is lost,** every outstanding command completes with BUSY, so the initiator
  retries it. tcmu-runner is notified, and it closes and reopens the device. Until the reopen, new
  commands complete with BUSY and each one notifies tcmu-runner again (unless close is running),
  because tcmu-runner silently drops a notification while the device is still in recovery or has
  no enabled TPG port; it de-duplicates the repeats.
