# Design: native TCMU frontend for vdisks (replacing tcmu-runner)

Branch: `tcmu_native`.

Related: `docs/superpowers/specs/2026-10-30-iscsi-tcmu-target-design.md` (the original vdisk/iSCSI design),
`src/main/scala/org/aspen_ddp/aspen/vdisk/DESIGN.md` (how the engine, bridge and plugin work today),
`docs/vdisk-tcmu-setup.md` (operator guide).

## Intent

Today a SCSI command takes this path: the kernel (LIO + `target_core_user`) → the TCMU shared-memory ring →
tcmu-runner → `handler_aspen.so` → a Unix socket → `vdisk-server` (JVM) → `LockableBlockDevice`. The extra process
and the socket hop add latency. They also add runtime complexity: a second root daemon, a C plugin, a private wire
protocol kept in step on both sides, and connection-loss and reopen behavior split between two processes.

This project removes all of that. `vdisk-server` drives the kernel's TCMU interface directly from the JVM, using
Java's Foreign Function & Memory API (final in JDK 22; the project runs on JDK 25). The SCSI emulation that
tcmu-runner did for us moves into Scala. `LockableBlockDevice` was designed as the seam for exactly this frontend,
so the engine (`VirtualDisk`, `DiskHandle`, `DiskCatalog`) should need few or no changes.

Use the tcmu-runner source in `submodules/tcmu-runner` (v1.5.3-10, effectively 1.5.4) as the reference for correct
kernel interaction and SCSI behavior. When this design is silent on a protocol detail, do what tcmu-runner does.

**Success** is correct operation: an initiator can log in, create and use a filesystem, survive a `vdisk-server`
restart and a grow, and never read wrong data. There is no latency target for this project.

## Goals

- `vdisk-server`, running as root, serves Aspen vdisks to LIO through TCMU with no tcmu-runner process.
- Operators keep using targetcli for all setup: `user:aspen` backstores, iSCSI targets, LUNs, ACLs, portals,
  `saveconfig`/restore.
- `vdisk-server` takes over tcmu-runner's D-Bus service so targetcli still sees the `user:aspen` backstore type.
- SCSI emulation covering everything tcmu-runner provided for our handler (list below), with FUA advertised, so
  Linux initiators send real FUA writes instead of WRITE + SYNCHRONIZE CACHE.
- Detailed error handling and logging, so a bug in the ring or SCSI code can be found and fixed quickly from the
  logs alone.
- A crash or restart of `vdisk-server` looks to the initiator like a stall, and serving resumes on restart.

## Non-goals

- ALUA, multipath failover and implicit lock transfer. Those remain part 3. Keep the `LockableBlockDevice`
  acquire/release/lockState seam intact and unused beyond what is described here.
- Persistent reservations, EXTENDED COPY / RECEIVE COPY RESULTS (XCOPY), and zoned or optical device types.
- Serving any tcmu handler other than `aspen`. Running alongside tcmu-runner on the same host: both want the same
  D-Bus name, and `vdisk-server` must refuse to start if the name is already owned.
- Backward compatibility with the old cfgstring `aspen/<disk>@<socket>` or the bridge protocol. Persisted-data
  compatibility is not required in this project.
- Non-Linux hosts.
- Performance tuning or benchmarks beyond not being slower by design (data copied once each way, no intermediate
  serialization).

## Anti-goals (failure even if it "works")

- **Wrong data.** A misparsed ring entry, iovec, LBA or transfer length must never read or write the wrong bytes.
  Every value read from shared memory is validated before use.
- **Crashing or corrupting the JVM.** A bad offset from the ring must never become an out-of-bounds native access.
  All ring access goes through `MemorySegment`s whose size is exactly the mapped size, so bounds violations throw
  instead of faulting. No raw-address arithmetic outside one small, well-tested layer.
- **Losing or double-completing a command.** Every command taken off the ring is completed exactly once, including
  on every error path. An uncompleted command leaves the initiator hanging until kernel timeouts fire; a double
  completion corrupts kernel state.
- **Taking a whole device down for one bad command.** See error handling.
- **Silent failure.** Any unexpected condition is logged with enough context to diagnose it without reproducing it.

## Constraints and fixed decisions

### Placement and project rules

- All new code lives in `org.aspen_ddp.aspen.vdisk` or a subpackage of it (for example `vdisk.tcmu`,
  `vdisk.tcmu.scsi`, `vdisk.tcmu.dbus`). The only exception is CLI wiring in `cmdline/Main.scala`.
- Scala 3 quiet syntax per `CLAUDE.md`; os-lib for filesystem work (configfs and sysfs are files), java.nio only
  where os-lib has no equivalent; scribe for logging, like the rest of the codebase.
- Linux only. `vdisk-server` runs as root.
- The FFM API needs native access enabled (`--enable-native-access=ALL-UNNAMED`, or the module equivalent) in
  every launcher that runs `vdisk-server`, and in the sbt test JVM if tests make real downcalls.

### Removed

- `vdisk/bridge/` (`Protocol`, `FrameDecoder`, `BridgeSession`, `BridgeServer`) and their tests.
- `native/tcmu-aspen/` in its entirety, including the C tests and `e2e/` (replaced, see Testing).
- The `vdisk-server --socket` option and the socket-directory checks.
- Keep the `submodules/tcmu-runner` submodule: it is the reference source.

### Kernel interface (what replaces tcmu-runner)

The builder takes exact layouts and sequences from tcmu-runner's `libtcmu.c`, `libtcmu_config.c`, `configfs.c`,
`target_core_user_local.h` and `tcmur_device.c`. The pieces are:

- **Generic netlink**, family `TCM-USER`, multicast group `config`: receive `TCMU_CMD_ADDED_DEVICE`,
  `TCMU_CMD_REMOVED_DEVICE` and `TCMU_CMD_RECONFIG_DEVICE`. Send `TCMU_CMD_SET_FEATURES` announcing reply support,
  and answer every event with the matching `*_DONE` command and `TCMU_ATTR_CMD_STATUS`, as tcmu-runner does. The
  kernel blocks the targetcli operation until it gets the reply, so a failure to open a disk shows up as a failed
  targetcli command, and the reason goes to our log.
- **UIO**: the device `/dev/uioN`, with its name and map size read from `/sys/class/uio/uioN/`. The UIO name
  encodes `tcm-user/<hba>/<device>/<subtype>/<cfgstring>`; handle only subtype `aspen`, and log and ignore the
  rest. Map the ring with `mmap`, block on `read()` of the fd for kernel notifications, and `write()` to it to
  notify the kernel of completions.
- **configfs** (`/sys/kernel/config/target/core/user_*/<device>/`): read the device size, `hw_block_size` and
  `hw_max_sectors`. Use the `action/` files (`block_dev`, `reset_ring`) for ring recovery the way tcmu-runner does.
- **The ring**: the `tcmu_mailbox` header, the command ring (`CMD`, `PAD`, and `TMR` entries if the kernel offers
  them) and the data area. Requests carry an iovec list of offsets into the data area. Completions go into the
  entry at the current `cmd_tail` with the command's `cmd_id` (see `tcmulib_command_complete`), so commands may
  complete out of order.
- **Native calls**: whatever libc functions are needed (`socket`/`bind`/`sendmsg`/`recvmsg` for netlink, `open`,
  `mmap`, `munmap`, `read`, `write`, `poll` or `eventfd` as needed), through FFM downcalls. `errno` is captured on
  every call (`Linker.Option.captureCallState`) and reported in errors.

### cfgstring and disk binding

- The cfgstring is `aspen/<disk>`. As with tcmu-runner, anything from the first `;` belongs to tcmu-runner-style
  options and is ignored. The old `@<socket>` suffix is gone. `CheckConfig` rejects it with a message saying so.
- **Device added** = today's OPEN then LOCK: `DiskHandle.open`, check geometry, then `acquire`. The device begins
  serving only after both succeed, and only then does the netlink reply report success.
- **Geometry (fixes the grow-then-reopen dead end in `Todo.txt`):** the kernel's logical block size must equal the
  disk's. The kernel's size may be **less than or equal to** the Aspen size: a disk grown with `aspen grow-disk`
  but not yet resized in targetcli still opens. The frontend serves the kernel's size (READ CAPACITY, range checks)
  until a reconfig changes it.
- **Grow on add (decided):** a kernel size larger than the Aspen size is treated as a grow, the same as a targetcli
  resize. After `acquire`, the add calls `DiskHandle.grow` to the kernel size, and reports success only once that
  grow has committed. It logs at warn with both sizes, so an accidental grow (a targetcli size typo) is easy to
  spot; grow cannot be undone. Only size gets this treatment: a block size mismatch or an unknown disk still fails
  the add.
- **Device removed** = `release` (flush, then close), then unmap and close the UIO device.
- **Reconfig, size** = `DiskHandle.grow` to the new size, committed before replying. Then serve the new size and
  raise the CAPACITY DATA HAS CHANGED unit attention (as `TCMUR_UA_DEV_SIZE_CHANGED` does). Reconfig of the write
  cache setting or the config string is refused with an error status and a log line. The write cache is always
  reported enabled.

### SCSI emulation

Implement in Scala, matching tcmu-runner's `tcmur_cmd_handler.c` and `scsi.c` behavior for a handler like ours:

- READ and WRITE (6/10/12/16), with FUA honored on writes (`cdb[1] & 0x08` on WRITE 10/12/16 → `fua = true`)
- SYNCHRONIZE CACHE (10/16) → `flush`
- UNMAP → `unmap`
- WRITE SAME (10/16), including the UNMAP bit
- WRITE AND VERIFY (10/16), as write followed by flush, as tcmu-runner does
- COMPARE AND WRITE: read, compare, write. Serialize it against other writes to the overlapping range, and return a
  MISCOMPARE with the offset on mismatch
- FORMAT UNIT, as tcmu-runner does it
- INQUIRY, standard and VPD pages: supported pages, unit serial, device identification, block limits (unmap
  granularity and alignment = one block object; max transfer from `hw_max_sectors`), and logical block provisioning
- READ CAPACITY (10) and SERVICE ACTION IN (16) / READ CAPACITY (16), with LBPME set
- MODE SENSE / MODE SELECT (6/10): the caching page with WCE=1, and the control page. The mode parameter header's
  DPOFUA bit is set
- TEST UNIT READY, START STOP UNIT, REQUEST SENSE, REPORT SUPPORTED OPERATION CODES (MAINTENANCE IN) if tcmu-runner
  does it
- Unit attentions (capacity changed)
- TMR entries from the kernel: honor them as tcmu-runner does (`tcmu_notify_lock_lost` aside), and at minimum
  acknowledge them correctly
- Anything else: CHECK CONDITION, ILLEGAL REQUEST, INVALID COMMAND OPERATION CODE

**Error → SCSI status.** Map each `VDiskError` the way `tcmu_status` in `handler_aspen.c` and tcmu-runner's
sense helpers do today:

| Error | SCSI result |
|-------|-------------|
| `InvalidRequest`, `InvalidGeometry` | ILLEGAL REQUEST, INVALID FIELD IN CDB |
| `OutOfRange` | ILLEGAL REQUEST, LBA OUT OF RANGE |
| `Fenced`, `NotOwner` | what tcmu-runner returns for `TCMU_STS_FENCED` |
| `DiskFailed` | MEDIUM ERROR, unrecovered read error / write error |
| anything unexpected | HARDWARE ERROR |

Take the exact sense keys, ASC/ASCQ values and descriptor-vs-fixed format from tcmu-runner.

### D-Bus service (targetcli compatibility)

- Use dbus-java: core plus the native Unix-socket transport, on the system bus.
- Own `org.kernel.TCMUService1` and export the `org.freedesktop.DBus.ObjectManager` at `/org/kernel/TCMUService1`,
  plus one object `/org/kernel/TCMUService1/aspen` implementing interface `org.kernel.TCMUService1`. That interface
  has a `ConfigDesc` property (the cfgstring help targetcli prints), `CheckConfig(s) → (b, s)`, and
  `ChangeMedium(s, t, s) → (i, s)`, which returns "not supported". Copy the shape from tcmu-runner's `main.c` and
  `tcmu-handler.xml`, and check it against targetcli's `ui_backstore.py`.
- `CheckConfig` validates `aspen/<disk>` and that the disk exists in the catalog. It returns a clear message
  otherwise.
- Ship and document a D-Bus policy file allowing root to own the name. Distribution tcmu-runner packages install
  one that may already allow it.
- If the name is already owned (tcmu-runner is running), `vdisk-server` exits at startup with a message saying so.

### Startup and restart

- Startup order: check that it is running as root, start the Aspen client, open the netlink socket and join the
  multicast group, send SET_FEATURES, take the D-Bus name, then scan `/sys/class/uio` for existing `aspen` devices
  and add each one. The add for an existing device resets its ring first, so commands left by a previous process
  are failed back to the kernel for retry, as tcmu-runner does on open.
- Crash-only: no clean shutdown path. On a crash the kernel keeps the devices, the initiator stalls, and unflushed
  writes are lost, as before. The restart takes ownership again through `acquire`.

### Error handling and logging (decided)

- **An unexpected condition while handling a command:** fail that command with appropriate sense data, log the
  ring state, and keep serving the device. Examples: an iovec outside the data area, a transfer length that
  disagrees with the CDB, an unknown entry opcode, or an exception in emulation code.
- **Ring state** in a log means at least: device name, UIO minor and disk name; the mailbox fields (version, flags,
  `cmdr_off`, `cmdr_size`, `cmd_head`, `cmd_tail`); the offending entry's header (`len_op`, `cmd_id`, `kflags`,
  `uflags`); its CDB in hex; its iovec list; and the number of commands in flight.
- **When the ring itself is inconsistent** (a mailbox head or tail out of range, or an entry length that walks off
  the ring), no command can be trusted. This is the builder's call within the decision above: log the ring state,
  complete what can be completed, reset the ring through configfs as tcmu-runner does, and keep serving. Never kill
  the process.
- **Netlink, D-Bus and per-device setup failures** are logged with the errno and the operation, and reported back to
  the kernel or targetcli as an error status. They never exit the daemon after startup.
- **A per-command trace** at debug/trace level: opcode, LBA, length, FUA, cmd_id, latency and result.
- **Rate limiting (designer's call):** the full ring-state dump is logged for the first occurrence of each distinct
  failure kind per device, then repeated occurrences are counted and summarized periodically. That keeps a stuck
  initiator retrying a bad command from flooding the log, while losing no information about new failures.

## Testing

Follow `CLAUDE.local.md`: correctness first, no flaky tests, every test as fast as possible.

- **Fake kernel.** Abstract the UIO device and netlink behind small interfaces, so tests can play the kernel on an
  ordinary `Arena`-allocated `MemorySegment` laid out exactly like the real ring. The fake writes entries and
  iovecs, advances `cmd_head`, notifies, then reads completions and sense. It also covers PAD wraparound, a full
  ring, out-of-order completion, and every malformed-entry case in the error-handling section, each asserting that
  the command is failed exactly once and the device keeps serving.
- **SCSI emulation** against a fake `LockableBlockDevice`: each supported CDB, including bad fields, out-of-range
  LBAs, FUA propagation, WRITE SAME/UNMAP, COMPARE AND WRITE miscompare, the unit attention after a grow, and the
  error → sense table. Use golden byte vectors for INQUIRY/VPD/MODE SENSE/READ CAPACITY responses, checked against
  tcmu-runner's output for the same settings.
- **Netlink messages**: encode and decode with golden byte vectors.
- **D-Bus**: unit-test the `CheckConfig`/`ConfigDesc` logic directly. Real system-bus wiring is covered by the
  root end-to-end run.
- **Integration**: fake kernel → real frontend → real `DiskHandle` on the in-process Aspen test system, replacing
  `BridgeIntegrationSuite`. Cover read-your-writes, FLUSH/FUA durability, UNMAP, grow (including the
  grown-before-targetcli case) and fencing.
- **Root end-to-end** (manual, not run by `sbt test`): a script and doc covering targetcli create (via our D-Bus
  service), iSCSI login, mkfs.ext4, mount, fio verify, `vdisk-server` restart under I/O, grow followed by targetcli
  resize, and checking dmesg. Update `docs/vdisk-tcmu-setup.md` to match.

## Documentation

- Rewrite `vdisk/DESIGN.md` for the new stack (sections 1, 2, 7–12).
- Update `docs/vdisk-tcmu-setup.md`: no tcmu-runner, the D-Bus policy file, running as root, the new cfgstring.
- Update the VDisks bullet in `CLAUDE.md`.
- Update the `Todo.txt` follow-ups: remove the items this project resolves (DPOFUA=0, grow-then-reopen, the
  resize/reopen race) and add anything new.

## Left to the builder

- Package, class and file names within `vdisk.*`. CLI flag names (`cmdline` is unstable).
- Threading: for example a platform thread per device blocking on the UIO fd (FFM downcalls pin virtual threads),
  plus one netlink thread. How completions get back to the ring, provided ring writes are correctly ordered and
  synchronized.
- Which optional kernel mailbox capabilities to use (TMR notification, KEEP_BUF and so on), based on what kernel
  7.x offers and what tcmu-runner does.
- The dbus-java version and its exact artifacts.
- The rate-limit window and the summary format.
- The order of work. A sensible order: FFM/libc layer and ring codec with the fake kernel, then SCSI emulation,
  then netlink and configfs device lifecycle, then D-Bus, then CLI and removal of the bridge and plugin, then
  docs and the e2e script.
