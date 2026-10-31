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

    targetcli /backstores/user:aspen create name=vd1 size=10G cfgstring=vd1@/run/aspen/vdisk.sock

The size given to tcmu must match the disk's size, or open fails with `-EINVAL`. A disk the daemon
does not know fails with `-ENOENT`.

## Behavior

- **open** sends OPEN and then LOCK. It enables the write cache and UNMAP, with the optimal unmap
  granularity set to one block object.
- **READ, WRITE, FLUSH and UNMAP** are queued asynchronously and completed when the daemon replies.
  WRITE carries FUA when the CDB of WRITE(10), WRITE(12) or WRITE(16) has it set. Write data is sent
  without being copied.
- **Resize** (`reconfig` with a new device size) sends RESIZE.
- **close** sends UNLOCK and waits for the reply.
- **If the connection is lost,** every outstanding command completes with BUSY, so the initiator
  retries it. tcmu-runner is notified, and it closes and reopens the device. Until the reopen, new
  commands complete with BUSY.
