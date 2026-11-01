# Native TCMU Frontend Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace tcmu-runner, `handler_aspen.so` and the Unix-socket bridge with a JVM frontend in `vdisk-server`
that drives the kernel's TCMU ring, netlink and configfs directly, emulates SCSI, and publishes tcmu-runner's
D-Bus service for targetcli.

**Architecture:** A small FFM layer (`vdisk.tcmu.sys`) wraps the libc calls. Over it sit:
- a pure ring codec over a `MemorySegment` (`vdisk.tcmu.ring`)
- a pure SCSI layer that turns CDBs into `LockableBlockDevice` calls and results into status and sense
  (`vdisk.tcmu.scsi`)
- a per-device serving loop (`TcmuDevice`)
- a device lifecycle manager driven by netlink events and a sysfs scan (`TcmuService`)
- the D-Bus service (`vdisk.tcmu.dbus`)

Kernel-facing pieces sit behind small traits, so nearly everything is tested without root against a fake kernel
that plays the kernel's side of the ring on an `Arena`-allocated segment.

**Tech Stack:** Scala 3.7.3 (all code, including the SCSI layer; Java only as the FFM API), JDK 25 FFM (`java.lang.foreign`), dbus-java 5.x (core plus native Unix-socket
transport), scribe, os-lib, ScalaTest.

**Spec:** `docs/superpowers/specs/2026-10-31-native-tcmu-design.md`. Read it first. For any kernel or SCSI detail
this plan doesn't pin, do what `submodules/tcmu-runner` does.

## Global Constraints

- Branch `tcmu_native`. Commit on it after each task.
- All new code is in package `org.aspen_ddp.aspen.vdisk` or a subpackage of it. Only CLI wiring goes in
  `cmdline/Main.scala`. Tests mirror that under `src/test/scala/org/aspen_ddp/aspen/vdisk/tcmu/`.
- Scala 3 quiet syntax; indented if/then/else. Filesystem access uses os-lib (configfs and sysfs are files). Use
  java.nio only where os-lib has no equivalent. Logging uses scribe.
- Linux only. `vdisk-server` runs as root.
- Native access is enabled with `--enable-native-access=ALL-UNNAMED`, in `.jvmopts` (the sbt test JVM) and in the
  `aspen` launcher's java command.
- Every ring value read from shared memory is validated before use. Native memory is touched only through
  `MemorySegment`s bounded to the exact mapped size and owned by an `Arena`. A closed arena makes access throw;
  it must never fault.
- Every command taken off the ring is completed exactly once, on every path.
- An unexpected condition in one command fails that command with sense data, logs the ring state, and the device
  keeps serving. Nothing after startup exits the process.
- Struct layouts come from `/usr/include/linux/target_core_user.h`, which is newer than the submodule's
  `target_core_user_local.h`: it has TMR, `read_len` and the capability flags. All multi-byte ring and netlink
  fields are host byte order (`ByteOrder.nativeOrder()`).
- Test priorities: correctness, then no flakiness, then speed. No sleeps to wait for state: use latches, promises
  and polling with a deadline. A full `sbt test` must finish within 70 s under normal load.

## Review Focus

1. **A transfer length or LBA near 2^64**: e.g. READ 16 with LBA `0xFFFFFFFFFFFFFFF0` and 32 blocks. Expect
   ILLEGAL REQUEST / LBA OUT OF RANGE, never arithmetic overflow into a valid range. Test:
   `ScsiCommandsSuite` "LBA plus length overflow is out of range" (Task 5).
2. **READ 6 / WRITE 6 transfer length 0 means 256 blocks**, while READ/WRITE 10/12/16 with length 0 complete GOOD
   with no device call. Tests: `CdbSuite` "6-byte transfer length 0 is 256 blocks" (Task 3) and
   `ScsiCommandsSuite` "zero-length READ 10 completes without a device call" (Task 5).
3. **Device removal while commands are in flight**: completions arriving after the ring is unmapped must be
   dropped, with a log line. They must not write unmapped memory or throw out of a Future callback. Test:
   `TcmuDeviceSuite` "completion after stop is dropped" (Task 6).
4. **Ring wraparound with a PAD while earlier commands are still outstanding and complete out of order.** Test:
   `RingSuite` "PAD at wrap with out-of-order completions" (Task 2).
5. **UNMAP parameter lists that lie**: the descriptor-data length disagrees with the allocation length, or a
   descriptor runs past the end of the disk. Expect ILLEGAL REQUEST (parameter list length, or LBA out of range),
   with no partial unmap. Test: `ScsiCommandsSuite` "UNMAP with inconsistent lengths is rejected whole" (Task 5).

---

## File Structure

```
src/main/scala/org/aspen_ddp/aspen/vdisk/tcmu/
  sys/Libc.scala            FFM downcalls, errno capture, SysError, Linux constants
  ring/RingLayout.scala     Byte offsets of mailbox / entry / rsp / tmr fields (one place)
  ring/Ring.scala           Ring: consume entries, validate, complete, gather/scatter, dump
  ring/RingItem.scala       RingItem ADT, Iovec, RingCorrupt
  scsi/ScsiResult.scala     ScsiResult, Sense, ScsiStatus constants, VDiskError → result mapping
  scsi/Cdb.scala            Opcode constants, CDB length, LBA/length/flag decoding
  scsi/Emulation.scala      INQUIRY/VPD, READ CAPACITY, MODE SENSE/SELECT, TUR, REQUEST SENSE, START STOP, RSOC
  scsi/ScsiCommands.scala   Dispatch: data commands → LockableBlockDevice, unit attention, CAW serialization
  DeviceSettings.scala      Per-device SCSI-visible settings (size, block size, max sectors, ids)
  KernelChannel.scala       trait KernelChannel + UioChannel (UIO fd await/kick)
  FailureLog.scala          Rate-limited ring-state logging
  TcmuDevice.scala          One device's serving loop
  Netlink.scala             Generic-netlink codec (pure) + TcmuNetlink socket
  Configfs.scala            configfs/sysfs access: device attrs, info Size, actions, UIO scan, UIO name parse
  Cfgstring.scala           aspen/<disk> parsing and validation
  TcmuService.scala         Device lifecycle: add / remove / reconfig / startup scan
  dbus/TcmuDbusService.scala  org.kernel.TCMUService1 objects, CheckConfig logic
src/test/scala/org/aspen_ddp/aspen/vdisk/tcmu/
  FakeKernel.scala, FakeBlockDevice.scala (test helpers)
  LibcSuite, RingSuite, CdbSuite, SenseSuite, EmulationSuite, ScsiCommandsSuite, TcmuDeviceSuite, NetlinkSuite,
  ConfigfsSuite, CfgstringSuite, TcmuServiceSuite, TcmuDbusServiceSuite, TcmuIntegrationSuite
native/tcmu-aspen/  → deleted (e2e scripts move to scripts/vdisk-e2e/)
src/main/scala/org/aspen_ddp/aspen/vdisk/bridge/ and its tests → deleted
deploy/dbus/org.kernel.TCMUService1.conf D-Bus policy file
```

---

### Task 1: FFM libc layer

**Files:**
- Create: `src/main/scala/org/aspen_ddp/aspen/vdisk/tcmu/sys/Libc.scala`
- Modify: `.jvmopts` (add `--enable-native-access=ALL-UNNAMED`), `aspen:119` (add the same flag before `-cp`)
- Test: `src/test/scala/org/aspen_ddp/aspen/vdisk/tcmu/LibcSuite.scala`

**Interfaces:**
- Produces:
  - `final case class SysError(call: String, errno: Int, message: String) extends Exception(s"$call failed: errno $errno ($message)")`.
    `message` comes from `strerror`.
  - `object Linux`, the constants: `O_RDONLY = 0`, `O_RDWR = 2`, `O_CLOEXEC = 0x80000`, `PROT_READ = 1`,
    `PROT_WRITE = 2`, `MAP_SHARED = 1`, `AF_NETLINK = 16`, `SOCK_RAW = 3`, `SOCK_CLOEXEC = 0x80000`,
    `NETLINK_GENERIC = 16`, `SOL_NETLINK = 270`, `NETLINK_ADD_MEMBERSHIP = 1`, `EINTR = 4`, `ENOENT = 2`,
    `EAGAIN = 11`, `EINVAL = 22`, `EBUSY = 16`, `ENODEV = 19`.
  - `object Libc`:
    - `open(path: String, flags: Int): Int`
    - `close(fd: Int): Unit`
    - `read(fd: Int, buf: MemorySegment): Long`
    - `write(fd: Int, buf: MemorySegment): Long`
    - `mmapShared(fd: Int, size: Long, arena: Arena): MemorySegment`: `PROT_READ|PROT_WRITE`, `MAP_SHARED`. The
      result is `reinterpret(size, arena, munmap cleanup)`.
    - `socket(domain: Int, tpe: Int, protocol: Int): Int`
    - `bind(fd: Int, addr: MemorySegment): Unit`
    - `setsockoptInt(fd: Int, level: Int, name: Int, value: Int): Unit`
    - `send(fd: Int, buf: MemorySegment): Long`
    - `recv(fd: Int, buf: MemorySegment): Long`
    - `geteuid(): Int`
    - `pipe(): (Int, Int)` (test helper only)
  - Every call taking a fd or a path throws `SysError` on `-1`. `read`, `write`, `send` and `recv` retry on `EINTR`.

- [ ] **Step 1: Write the failing tests** in `LibcSuite` (plain `AnyFunSuite with Matchers`):
  - "open of a missing file reports ENOENT": `intercept[SysError](Libc.open("/nonexistent/x", Linux.O_RDONLY))`
    has `errno == Linux.ENOENT` and `call == "open"`, and the message contains "No such file".
  - "pipe round trip": write 4 bytes `[1,2,3,4]` to the write end, read 4 from the read end, and they're equal.
  - "mmapShared maps a file and the closed arena makes access throw": create a 4096-byte temp file with byte 7 at
    offset 100, map it, check `seg.get(JAVA_BYTE, 100) == 7`, write 9 at 200, then `arena.close()`. Re-read the
    file through os-lib: offset 200 is 9. `intercept[IllegalStateException](seg.get(JAVA_BYTE, 0))`.
  - "geteuid returns a non-negative id".
- [ ] **Step 2: Run** `sbt 'testOnly *LibcSuite'`. Expected: compile failure, because `Libc` is not defined.
- [ ] **Step 3: Implement `Libc` and `Linux`.** Use `Linker.nativeLinker()` with
  `Linker.Option.captureCallState("errno")`. Read errno from the captured state segment. Cache each
  `MethodHandle` in a `lazy val`. Use a confined arena per call for path strings and the state segment.
- [ ] **Step 4: Run** `sbt 'testOnly *LibcSuite'`. Expected: 4 tests pass, with no "restricted method" warning.
- [ ] **Step 5: Commit** `vdisk.tcmu: FFM libc layer`.

### Task 2: Ring codec

**Files:**
- Create: `vdisk/tcmu/ring/RingLayout.scala`, `ring/RingItem.scala`, `ring/Ring.scala`
- Create (test helper): `src/test/scala/.../vdisk/tcmu/FakeKernel.scala`
- Test: `RingSuite.scala`

**Interfaces:**
- Consumes: nothing from Task 1. `Ring` works on any `MemorySegment`.
- Produces:
  - `RingLayout` offsets, all from the kernel header:
    - mailbox: `version@0 u16`, `flags@2 u16`, `cmdrOff@4 u32`, `cmdrSize@8 u32`, `cmdHead@12 u32`,
      `cmdTail@64 u32`
    - entry header: `lenOp@0 u32`, `cmdId@4 u16`, `kflags@6 u8`, `uflags@7 u8`
    - req: `iovCnt@8`, `iovBidiCnt@12`, `iovDifCnt@16`, `cdbOff@20 u64`, `iov@44`, each iovec 16 bytes
      (`base u64`, `len u64`)
    - rsp: `scsiStatus@8 u8`, `readLen@12 u32`, `sense@16`, 96 bytes
    - tmr: `tmrType@8 u8`, `cmdCnt@12 u32`, `cmdIds@32 u16[]`
    - `OpPad = 0`, `OpCmd = 1`, `OpTmr = 2`, `OpMask = 7`
    - `FlagOooc = 1`, `FlagReadLen = 2`, `FlagTmr = 4`, `FlagKeepBuf = 8`
    - `MailboxVersion = 2`, `SenseSize = 96`
  - `final case class Iovec(offset: Long, length: Long)`
  - `sealed trait RingItem`:
    - `Command(cmdId: Int, cdb: Array[Byte], iovecs: Vector[Iovec])`
    - `Tmr(tmrType: Int, cmdIds: Vector[Int])`
    - `Malformed(cmdId: Int, reason: String, dump: String)`: the header is sane, so the entry can still be
      completed by id
  - `final case class RingCorrupt(reason: String, dump: String) extends Exception(reason)`: the mailbox or an
    entry length is unusable
  - `final class Ring private (map: MemorySegment)`:
    - `def nextItems(): List[RingItem]`: acquire-load `cmd_head`, walk the private consume tail to it, skip PAD,
      and decode CMD/TMR. Throws `RingCorrupt`.
    - `def complete(cmdId: Int, status: Int, sense: Array[Byte], readLen: Option[Int]): Boolean`: synchronized.
      Returns `false`, writing nothing, if `cmdId` is not in the outstanding set: a double completion, or a command
      from before a `resync`. Otherwise it skips non-CMD entries (PAD and TMR, as `tcmulib_command_complete` does)
      from the mailbox `cmd_tail`, writes `cmdId`/status/sense/`read_len` into that entry (`read_len` only when
      `FlagReadLen` is set), and release-stores the new `cmd_tail`.
    - `def resync(): Either[String, Unit]`: after the kernel resets the ring, revalidates the mailbox, sets the
      consume tail to the mailbox `cmd_tail`, and clears the outstanding set.
    - `def gather(iovecs: Vector[Iovec], length: Int): Array[Byte]`
    - `def scatter(iovecs: Vector[Iovec], data: Array[Byte]): Int`: returns the bytes written
    - `def dataLength(iovecs: Vector[Iovec]): Long`
    - `def inFlight: Int`
    - `def mailboxDump: String`
  - `object Ring`: `def open(map: MemorySegment): Either[String, Ring]`. Starts the consume tail at the mailbox
    `cmd_tail`, as tcmu-runner does.

Validation rules. Each failure carries a `reason` naming the field and the value.
- **open:** `Left` unless `version == 2`, OOOC is set, `cmdrOff + cmdrSize <= map.byteSize`, and `cmdrSize` is a
  nonzero multiple of 8.
- **`RingCorrupt`:** head or tail ≥ `cmdrSize`, or not 8-aligned; an entry length `< 8`, not 8-aligned, or running
  past `cmdrSize` from its offset; an unknown op.
- **`Malformed` (header valid):**
  - `iov_bidi_cnt` or `iov_dif_cnt` nonzero
  - `44 + 16 * iovCnt > len`
  - a `cdb_off` whose CDB (length from the group code) doesn't fit inside the map
  - an iovec not entirely inside the data area `[cmdrOff + cmdrSize, map.byteSize)`
  - a TMR `cmdCnt` that doesn't fit in the entry
- Use `ValueLayout.JAVA_INT` / `JAVA_SHORT` / `JAVA_LONG_UNALIGNED` (the req fields at 20+ aren't 8-aligned),
  with native order. Use `VarHandle` acquire/release access modes for `cmd_head` and `cmd_tail`.

`FakeKernel` (test helper):
- `FakeKernel(cmdrSize: Int = 4096, dataSize: Int = 64 * 1024, flags: Int = FlagOooc | FlagReadLen | FlagTmr)`
  allocates `Arena.ofShared()` memory and writes the mailbox (`cmdrOff = 128`)
- `submit(cdb: Array[Byte], data: Array[Byte] = Array.empty, readLen: Int = 0, iovSplit: Int = Int.MaxValue): Int`
  returns the cmdId, inserts a PAD when the entry would cross the ring end, and splits data into iovecs of
  `iovSplit` bytes
- `submitRaw(bytes: Array[Byte])` writes a hand-built entry
- `submitTmr(tpe: Int, ids: Seq[Int])`
- `completions(): List[FakeCompletion(cmdId, status, sense, readLen, data)]` walks from the kernel's last-seen tail
  to `cmd_tail`
- `segment: MemorySegment`

The fake reclaims data-area space only after completion, as the kernel does.

- [ ] **Step 1: Write the failing tests** in `RingSuite`:
  - "open rejects version 1, a missing OOOC flag, and a command ring past the map": three `Left`s whose messages
    contain "version", "OOOC" and "cmdr".
  - "a command round trips": submit a READ 10 CDB with `readLen = 4096`. `nextItems()` returns one `Command`
    with the same CDB bytes and iovecs summing to 4096. `scatter` a pattern, then `complete(id, 0, Array.empty,
    Some(4096))`. The fake sees status 0, `readLen` 4096 and the pattern.
  - "write data is gathered across split iovecs": `iovSplit = 1000`, 4096 bytes, gather equals the input.
  - "PAD at wrap with out-of-order completions": fill the ring so the third entry needs a PAD. Complete ids in the
    order 3, 1, 2. The fake sees three completions with ids {1,2,3}, and `cmd_tail` equals `cmd_head`.
  - "TMR entries decode": `submitTmr(5, Seq(7, 9))` gives `Tmr(5, Vector(7, 9))`.
  - "an iovec outside the data area is Malformed and completable": `submitRaw` with iovec offset 0. The result is
    `Malformed(id, reason, dump)` with "iovec" in the reason and the cmdId in the dump. Completing it with status
    2 works.
  - "bidi and dif counts are Malformed".
  - "an entry length running past the ring is RingCorrupt": the dump contains "cmd_head" and "cmd_tail".
  - "a head out of range is RingCorrupt".
  - "a second completion of the same id returns false and writes nothing".
  - "resync drops outstanding ids": consume 2 commands, simulate a kernel reset (the fake rewrites head and tail to
    0), `resync()`, then `complete` of an old id returns false and a new command round trips.
- [ ] **Step 2: Run** `sbt 'testOnly *RingSuite'`. Expected: compile failure.
- [ ] **Step 3: Implement `RingLayout`, `RingItem` and `Ring`** with the validation rules above. `mailboxDump`
  prints version, flags, `cmdr_off`, `cmdr_size`, `cmd_head`, `cmd_tail`, the private consume tail and
  `inFlight`. An entry dump adds `len_op`, `cmd_id`, `kflags`, `uflags`, the CDB in hex, and the iovec list.
- [ ] **Step 4: Run** `sbt 'testOnly *RingSuite'`. Expected: all pass.
- [ ] **Step 5: Commit** `vdisk.tcmu: TCMU ring codec and fake kernel`.

### Task 3: CDB decoding, sense and error mapping

**Files:**
- Create: `vdisk/tcmu/scsi/Cdb.scala`, `scsi/ScsiResult.scala`
- Test: `CdbSuite.scala`, `SenseSuite.scala`

**Interfaces:**
- Produces:
  - `object Op`: opcode constants for every command in the spec's list, named as in tcmu-runner's `scsi_defs.h`
    (`Read6 = 0x08`, `Read10 = 0x28`, `Read12 = 0xA8`, `Read16 = 0x88`, `Write6 = 0x0A`, `Write10 = 0x2A`,
    `Write12 = 0xAA`, `Write16 = 0x8A`, `SyncCache10 = 0x35`, `SyncCache16 = 0x91`, `Unmap = 0x42`,
    `WriteSame10 = 0x41`, `WriteSame16 = 0x93`, `WriteVerify10 = 0x2E`, `WriteVerify16 = 0x8E`,
    `CompareAndWrite = 0x89`, `FormatUnit = 0x04`, `Inquiry = 0x12`, `ReadCapacity10 = 0x25`,
    `ServiceActionIn16 = 0x9E`, `ModeSense6 = 0x1A`, `ModeSense10 = 0x5A`, `ModeSelect6 = 0x15`,
    `ModeSelect10 = 0x55`, `TestUnitReady = 0x00`, `RequestSense = 0x03`, `StartStop = 0x1B`,
    `MaintenanceIn = 0xA3`)
  - `object Cdb`:
    - `length(firstByte: Byte): Option[Int]`: groups 0 → 6, 1/2 → 10, 4 → 16, 5 → 12, else None
    - `lba(cdb: Array[Byte]): Long`
    - `transferLength(cdb: Array[Byte]): Long`: 6-byte 0 → 256, others as encoded
    - `fua(cdb: Array[Byte]): Boolean`: `cdb(1) & 0x08`, for WRITE 10/12/16 and WRITE AND VERIFY only
  - `final case class ScsiResult(status: Int, sense: Array[Byte] = Array.empty, readLen: Option[Int] = None)`,
    with `ScsiResult.Good` and `ScsiResult.good(readLen: Int)`
  - `object ScsiStatus`: `Good = 0x00`, `CheckCondition = 0x02`, `Busy = 0x08`, `TaskSetFull = 0x28`
  - `object Sense`:
    - `fixed(key: Int, ascAscq: Int, info: Option[Long] = None): Array[Byte]`: an 18-byte fixed-format sense,
      byte 0 `0x70`, byte 7 = 10, laid out as tcmu-runner's `tcmu_sense_set_data`. `info` sets the VALID bit and
      bytes 3–6, for MISCOMPARE.
    - `check(key: Int, ascAscq: Int): ScsiResult`
    - the result constants, with values from `libtcmu.c:tcmu_sts_to_scsi`:
      `LbaOutOfRange` (5, 0x2100), `InvalidFieldInCdb` (5, 0x2400), `InvalidFieldInParamList` (5, 0x2600),
      `ParamListLength` (5, 0x1A00), `InvalidOpcode` (5, 0x2000), `HardwareError` (4, 0x4400),
      `ReadError` (3, 0x1100), `WriteError` (3, 0x0C00), `Fenced` (2, 0x040B), `CapacityChanged` (6, 0x2A09),
      `NotSuppSaveParams` (5, 0x3900), `Miscompare(offset: Long)` (0x0E, 0x1D00 with info)
  - `object ErrorMap`: `def result(err: Throwable, isWrite: Boolean): ScsiResult`
    - `InvalidRequest`/`InvalidGeometry` → `InvalidFieldInCdb`
    - `OutOfRange` → `LbaOutOfRange`
    - `Fenced`/`NotOwner` → `Fenced`
    - `DiskFailed` → `WriteError` if `isWrite`, else `ReadError`
    - anything else → `HardwareError`

- [ ] **Step 1: Write the failing tests:**
  - `CdbSuite`:
    - "group codes give CDB lengths": 0x28 → 10, 0x88 → 16, 0xA8 → 12, 0x08 → 6, 0xC0 → None
    - "6-byte transfer length 0 is 256 blocks"
    - "READ 16 LBA and length decode big-endian": LBA `0x0102030405060708`, length `0x0A0B0C0D`
    - "FUA bit only on writes": WRITE 10 with `cdb(1) = 0x08` → true; READ 10 with the same → false
    - "READ 6 uses the 21-bit LBA": `cdb(1) = 0xFF` gives LBA bits 16–20 only
  - `SenseSuite`:
    - "fixed sense layout": `Sense.fixed(5, 0x2100)` is 18 bytes, `(0)=0x70, (2)=5, (7)=10, (12)=0x21, (13)=0`
    - "miscompare carries the offset": `Miscompare(1234)` has the valid bit set and bytes 3–6 = 1234 big-endian
    - "error map": one assertion per row of the table above, including `DiskFailed` for read vs write and an
      arbitrary `RuntimeException` → HARDWARE ERROR 0x44
- [ ] **Step 2: Run** `sbt 'testOnly *CdbSuite *SenseSuite'`. Expected: compile failure.
- [ ] **Step 3: Implement `Cdb`, `Op`, `ScsiResult`, `ScsiStatus`, `Sense` and `ErrorMap`.**
- [ ] **Step 4: Run** the same command. Expected: all pass.
- [ ] **Step 5: Commit** `vdisk.tcmu: CDB decoding and sense data`.

### Task 4: Non-data SCSI emulation

**Files:**
- Create: `vdisk/tcmu/DeviceSettings.scala`, `scsi/Emulation.scala`
- Test: `EmulationSuite.scala`

**Interfaces:**
- Consumes: `ScsiResult`, `Sense`, `Op`, `Cdb` (Task 3).
- Produces:
  - `final case class DeviceSettings(size: Long, blockSize: Int, maxSectors: Int, blockObjectSize: Int, deviceName: String, wwn: String)`
    - `def lbaCount: Long = size / blockSize`
    - `deviceName` is the configfs device name. `wwn` is the unit serial: configfs `wwn/vpd_unit_serial` if
      present, otherwise the disk name (Task 9 chooses).
  - `object Emulation`: `def handle(cdb: Array[Byte], settings: DeviceSettings, dataOut: () => Array[Byte]): Option[(ScsiResult, Array[Byte])]`.
    `dataOut` is the MODE SELECT parameter list. Returns `None` for opcodes it doesn't own. The returned bytes are
    truncated to the allocation length from the CDB, and `readLen` is set to the bytes returned.
  - It handles: INQUIRY standard and VPD 0x00, 0x80, 0x83, 0xB0, 0xB2; READ CAPACITY 10 (`0xFFFFFFFF` when the
    LBA count exceeds 32 bits); SERVICE ACTION IN 16 with service action 0x10 (READ CAPACITY 16, LBPME = 1);
    MODE SENSE 6/10 (pages 0x08 caching with WCE = 1, 0x0A control, 0x3F all; header byte DPOFUA `0x10` set);
    MODE SELECT 6/10; TEST UNIT READY; REQUEST SENSE (no sense); START STOP UNIT (GOOD); MAINTENANCE IN /
    REPORT SUPPORTED OPERATION CODES if tcmu-runner implements it (`tcmur_cmd_handler.c`).
  - MODE SELECT accepts only a parameter list identical to what MODE SENSE reports for the changeable fields,
    as `tcmur_cmd_handler.c:handle_mode_select` does. The SP bit gives `NotSuppSaveParams`.
  - Block limits VPD 0xB0: max transfer = `maxSectors`; optimal unmap granularity and unmap granularity alignment
    = `blockObjectSize / blockSize`, with the UGAVALID bit set; max unmap LBA count and max unmap descriptor count
    as in tcmu-runner (`VPD_MAX_UNMAP_LBA_COUNT`, `VPD_MAX_UNMAP_BLOCK_DESC_COUNT`); max write same length as
    tcmu-runner.
  - Logical block provisioning VPD 0xB2: LBPU = 1, LBPWS = 1, LBPWS10 = 1, LBPRZ = 1. Unmapped bytes read as zero.

- [ ] **Step 1: Write the failing tests** in `EmulationSuite`, using `DeviceSettings(8 MiB, 4096, 1024, 16384, "vd1", "a5d1…")`:
  - "standard INQUIRY": peripheral type 0, version byte 6 (SPC-4), vendor "LIO-ORG ", a product, and truncation to
    allocation length 36
  - "VPD 0x00 lists 0x00 0x80 0x83 0xB0 0xB2"
  - "VPD 0x80 returns the wwn"
  - "VPD 0x83 has an NAA or T10 designator built as tcmu-runner builds it"
  - "VPD 0xB0 unmap granularity is one block object": optimal granularity 4, alignment 4 with UGAVALID,
    max transfer 1024
  - "VPD 0xB2 advertises UNMAP and WRITE SAME provisioning"
  - "READ CAPACITY 10 gives last LBA and block size": 2047, 4096
  - "READ CAPACITY 16 sets LBPME"
  - "MODE SENSE 6 caching page has WCE and the header has DPOFUA": `(2) & 0x10 != 0`, and page 0x08 byte 2 bit 2
    set
  - "MODE SENSE 10 all pages contains caching and control"
  - "MODE SELECT with SP set is rejected" (`NotSuppSaveParams`)
  - "unknown VPD page is INVALID FIELD IN CDB"
  - "TEST UNIT READY is GOOD"
  - "a data opcode returns None"
  - Golden vectors: for INQUIRY, VPD 0x83, VPD 0xB0 and MODE SENSE 6 all-pages, pin the full response bytes.
    Check every field by hand against the tcmu-runner function that builds it, and name that function in a comment
    beside each vector.
- [ ] **Step 2: Run** `sbt 'testOnly *EmulationSuite'`. Expected: compile failure.
- [ ] **Step 3: Implement `DeviceSettings` and `Emulation`**, following tcmu-runner's `scsi.c` / `tcmur_cmd_handler.c`
  byte for byte, apart from the WCE/DPOFUA/provisioning values above.
- [ ] **Step 4: Run** the same command. Expected: all pass.
- [ ] **Step 5: Commit** `vdisk.tcmu: SCSI emulation for inquiry, capacity and mode pages`.

### Task 5: Data commands and dispatch

**Files:**
- Create: `vdisk/tcmu/scsi/ScsiCommands.scala`
- Create (test helper): `src/test/scala/.../vdisk/tcmu/FakeBlockDevice.scala`, modeled on `BridgeServerSuite`'s
  `FakeDevice`: records calls, completes automatically or by hand
- Test: `ScsiCommandsSuite.scala`

**Interfaces:**
- Consumes: `LockableBlockDevice` (`vdisk/BlockDevice.scala`), `DeviceSettings`, `Emulation`, `Cdb`, `Sense`,
  `ErrorMap`.
- Produces:
  - `final class ScsiCommands(device: LockableBlockDevice, initial: DeviceSettings)(using ExecutionContext)`:
    - `def handle(cdb: Array[Byte], dataOut: () => Array[Byte], dataInCapacity: Long): Future[(ScsiResult, Array[Byte])]`.
      `dataOut` gathers the write payload lazily. The returned bytes are scattered by the caller, and the
      `readLen` in the result equals their length. It never fails: every exception becomes a `ScsiResult`
      through `ErrorMap`, and anything thrown synchronously is caught too.
    - `def settings: DeviceSettings`
    - `def resized(newSize: Long): Unit`: updates the settings and arms a CAPACITY DATA HAS CHANGED unit
      attention. The next command other than INQUIRY, REPORT LUNS or REQUEST SENSE gets it once, as tcmu-runner
      does with `TCMUR_UA_DEV_SIZE_CHANGED`.
  - The behavior:
    - Ranges are checked against `settings.size`, not the device's geometry. A kernel size smaller than the Aspen
      size is legal.
    - LBA + length uses overflow-safe arithmetic (`Math.addExact` or `lba > lbaCount - len`).
    - READ: `device.read(lba * bs, len * bs)`.
    - WRITE: `device.write(..., fua = Cdb.fua(cdb))`.
    - A transfer length of 0 on a 10/12/16-byte READ or WRITE gives GOOD with no device call.
    - The payload length must equal `len * bs`, else `InvalidFieldInCdb` and a log line.
    - SYNCHRONIZE CACHE → `flush()`.
    - UNMAP: parse the parameter list (header lengths, 16-byte descriptors), validate every descriptor first, then
      issue one `unmap` with all the ranges. A length mismatch gives `ParamListLength`, out of range gives
      `LbaOutOfRange`, and nothing is unmapped.
    - WRITE SAME 10/16 with the UNMAP bit → `unmap`. Without it, the one-block payload repeated, written in chunks
      of at most `maxSectors`. A NUMBER OF BLOCKS of 0 → `InvalidFieldInCdb`, as tcmu-runner does.
    - WRITE AND VERIFY → write, then `flush`.
    - COMPARE AND WRITE (NUMBER OF BLOCKS ≤ 1, as tcmu-runner): read, compare the first half of the payload, and
      on a mismatch give `Miscompare(offset of first differing byte)`; otherwise write the second half. Use a
      per-instance range lock, so overlapping WRITE, WRITE SAME, UNMAP and CAW wait for an in-flight CAW and a CAW
      waits for overlapping in-flight writes.
    - FORMAT UNIT → GOOD (tcmu-runner's behavior for a handler without format support; confirm in
      `tcmur_cmd_handler.c`).
    - Unknown opcodes → `InvalidOpcode`.

- [ ] **Step 1: Write the failing tests** in `ScsiCommandsSuite` (AsyncFunSuite). Settings are 8 MiB / 4096
  block size / 1024 max sectors / 16384 block object size.
  - "READ 10 maps to device.read": LBA 2, length 3 → `read(8192, 12288)`; returned bytes equal the fake's data,
    and `readLen` is 12288
  - "WRITE 16 with FUA passes fua = true"
  - "zero-length READ 10 completes without a device call"
  - "READ 6 length 0 reads 256 blocks"
  - "LBA plus length overflow is out of range": READ 16 with LBA `0xFFFFFFFFFFFFFFF0` and 32 blocks →
    `LbaOutOfRange`, no device call
  - "range checked against the kernel size, not the Aspen size": the fake geometry is 16 MiB and the settings 8 MiB;
    a READ at LBA 2048 → `LbaOutOfRange`
  - "payload length mismatch is INVALID FIELD IN CDB"
  - "SYNCHRONIZE CACHE flushes"
  - "UNMAP sends every descriptor in one call"
  - "UNMAP with inconsistent lengths is rejected whole": two cases (header length mismatch; second descriptor past
    the end). Both give the expected sense with no `unmap` call.
  - "WRITE SAME with UNMAP bit unmaps"
  - "WRITE SAME without UNMAP writes the repeated block": 3 blocks of pattern P → the written data is P×3
  - "WRITE AND VERIFY writes then flushes"
  - "COMPARE AND WRITE writes on match"
  - "COMPARE AND WRITE miscompare reports the offset and writes nothing": the first difference at byte 100 →
    `Miscompare(100)`
  - "COMPARE AND WRITE serializes against an overlapping write": hold the CAW's read in the fake, issue a WRITE to
    the same block, release, and assert the call order is read, CAW write, WRITE
  - "device failures map through ErrorMap": `Fenced` → NOT READY 0x040B; `DiskFailed` on read → 0x1100 and on
    write → 0x0C00
  - "a synchronous throw from the device becomes HARDWARE ERROR"
  - "capacity change unit attention is reported once": after `resized(16 MiB)`, TEST UNIT READY gets
    `CapacityChanged`, the next gets GOOD, and READ CAPACITY gives 4095
  - "INQUIRY does not consume the unit attention"
  - "unknown opcode is INVALID OPCODE"
- [ ] **Step 2: Run** `sbt 'testOnly *ScsiCommandsSuite'`. Expected: compile failure.
- [ ] **Step 3: Implement `ScsiCommands`.** Dispatch emulated opcodes to `Emulation.handle`, after the
  unit-attention check.
- [ ] **Step 4: Run** the same command. Expected: all pass.
- [ ] **Step 5: Commit** `vdisk.tcmu: SCSI data commands over LockableBlockDevice`.

### Task 6: Device serving loop

**Files:**
- Create: `vdisk/tcmu/KernelChannel.scala`, `FailureLog.scala`, `TcmuDevice.scala`
- Modify: `FakeKernel.scala`. Add `channel: KernelChannel`: `await()` blocks on a semaphore released by
  `submit*`, and `kick()` counts.
- Test: `TcmuDeviceSuite.scala`

**Interfaces:**
- Consumes: `Ring`, `RingItem`, `RingCorrupt` (Task 2); `ScsiCommands`, `ScsiResult`, `Sense` (Tasks 3 and 5);
  `Libc` (Task 1) for `UioChannel`.
- Produces:
  - `trait KernelChannel`:
    - `def await(): Unit`: blocks until the kernel signals, and throws `InterruptedException` after `close`
    - `def kick(): Unit`
    - `def close(): Unit`
  - `final class UioChannel(fd: Int) extends KernelChannel`: `read` / `write` 4 bytes on the UIO fd.
    `close` closes the fd and wakes any blocked reader.
  - `trait RingControl`: `def resetRing(): Unit`. The real one is in Task 8; the tests count calls.
  - `final class FailureLog(device: String, window: FiniteDuration = 60.seconds, clock: () => Long = System.nanoTime)`:
    - `def report(kind: String, message: String, dump: => String): Unit`: logs `error` with the dump the first
      time a `kind` is seen, then counts repeats. The next report after `window` logs `warn` "N more <kind> since
      <first>" (no dump) and resets the count.
    - `def counts: Map[String, Long]`
  - `final class TcmuDevice(name: String, ring: Ring, channel: KernelChannel, control: RingControl, commands: ScsiCommands, failures: FailureLog)`:
    - `def start(): Unit`: one platform thread, named `tcmu-<name>`, loops: `await`, `nextItems`, dispatch
    - `def stop(): Unit`: stops the loop, waits for the thread, and marks the device stopped. Later completions are
      dropped and logged at `warn`.
    - `def processedCount: Long`
  - Per item:
    - **`Command`:** `commands.handle(cdb, () => ring.gather(...), ring.dataLength(...))`. On completion: if the
      returned data is longer than the iovec capacity, log it through `FailureLog` and complete with
      `HardwareError`; otherwise `scatter` and `complete`. Then `kick()`. Completion runs on the Future's thread,
      and `Ring.complete` is synchronized.
    - **`Malformed`:** `failures.report("malformed-entry", reason, dump)`, complete with `HardwareError`, kick.
    - **`Tmr`:** log at `info`, take no action (acknowledged by consumption, as tcmu-runner does without a
      `tmr_notify` handler).
    - **`RingCorrupt`:** `failures.report("ring-corrupt", ...)`, then `control.resetRing()` and `ring.resync()`.
      The kernel retries the commands that were outstanding. Their later device completions get `false` from
      `complete`, which is logged at `warn`. A `resync` `Left` is reported and retried on the next notification.
    - **`complete` returning `false`** at any other time is reported as `"stale-completion"`.
    - **Exception in dispatch:** if the cmdId is known, complete it with `HardwareError` and report it.
  - A per-command trace at `debug`: opcode, LBA, length, FUA, cmd_id, latency in µs, status and sense key. Use
    scribe's `logger.debug` with a lazy message.

- [ ] **Step 1: Write the failing tests** in `TcmuDeviceSuite`, using `FakeKernel` and `FakeBlockDevice`:
  - "a read completes with data and a kick"
  - "out-of-order device completions are all delivered once": hold 3 READs, release them in order 3, 1, 2, and
    expect exactly three completions with distinct ids
  - "malformed entry fails with HARDWARE ERROR and the device keeps serving": a bad iovec, then a good READ; the
    first gets status 2 / key 4 and the second GOOD, and `failures.counts("malformed-entry") == 1`
  - "repeated failures are rate limited": 5 malformed entries → one dump logged (capture scribe output through a
    test handler, as other suites do) and `counts == 5`; advancing the injected clock past the window and
    reporting once more gives the summary line
  - "ring corruption resets the ring and serving resumes": corrupt `cmd_head`, check `resetRing` is called once,
    then the fake re-initializes the mailbox and a new READ completes
  - "returned data larger than the iovecs is HARDWARE ERROR"
  - "completion after stop is dropped": hold a READ, `stop()`, close the fake's arena, release the READ, and
    expect no exception to escape (the Future's `onComplete` throws nothing) and a warn logged
  - "stop wakes a blocked loop": `stop()` returns within 1 s with nothing submitted
- [ ] **Step 2: Run** `sbt 'testOnly *TcmuDeviceSuite'`. Expected: compile failure.
- [ ] **Step 3: Implement `KernelChannel`, `UioChannel`, `RingControl`, `FailureLog` and `TcmuDevice`.**
  Completions after `stop` check a `@volatile stopped` flag under the same lock as `Ring.complete`. That makes the
  arena-closed case unreachable, and the test proves it.
- [ ] **Step 4: Run** the same command. Expected: all pass.
- [ ] **Step 5: Commit** `vdisk.tcmu: per-device ring serving loop with failure logging`.

### Task 7: Generic netlink

**Files:**
- Create: `vdisk/tcmu/Netlink.scala`
- Test: `NetlinkSuite.scala`

**Interfaces:**
- Consumes: `Libc`, `Linux` (Task 1).
- Produces:
  - `object TcmuGenl`: commands `AddedDevice = 1`, `RemovedDevice = 2`, `ReconfigDevice = 3`,
    `AddedDeviceDone = 4`, `RemovedDeviceDone = 5`, `ReconfigDeviceDone = 6`, `SetFeatures = 7`; attributes
    `Device = 1` (string), `Minor = 2` (u32), `DevCfg = 4` (string), `DevSize = 5` (u64), `WriteCache = 6` (u8),
    `CmdStatus = 7` (s32), `DeviceId = 8` (u32), `SuppKernCmdReply = 9` (u8). Family `"TCM-USER"`, group
    `"config"`, version 2.
  - `sealed trait TcmuEvent`:
    - `Added(device: String, minor: Int, deviceId: Int)`
    - `Removed(device: String, minor: Int, deviceId: Int)`
    - `Reconfig(device: String, minor: Int, deviceId: Int, change: ReconfigChange)`, where
      `enum ReconfigChange { case Size(bytes: Long); case Config(cfg: String); case WriteCache(on: Boolean); case Unknown }`
  - `object GenlCodec` (pure, `ByteBuffer` in native order):
    - `getFamily(name: String, seq: Int): Array[Byte]`
    - `parseFamily(msg: Array[Byte]): Either[String, (Int, Map[String, Int])]` (family id, group name → id)
    - `setFeatures(familyId: Int, seq: Int): Array[Byte]`
    - `reply(familyId: Int, seq: Int, cmd: Int, status: Int, deviceId: Int): Array[Byte]`
    - `parseEvents(buf: Array[Byte], familyId: Int): List[Either[String, TcmuEvent]]`, which handles several
      messages per datagram, `NLMSG_ERROR` and `NLMSG_DONE`. A message without `Minor`/`Device`, or a v2 message
      without `DeviceId`, gives `Left`, as `libtcmu.c:handle_netlink` drops it.
    - attributes padded to 4 (`NLA_ALIGN`)
  - `final class TcmuNetlink private (fd: Int, familyId: Int)`:
    - `def receive(): List[Either[String, TcmuEvent]]`: blocks
    - `def reply(event: TcmuEvent, status: Int): Unit`: synchronized, so replies may come from any thread
    - `def close(): Unit`
  - `object TcmuNetlink`: `def open(): TcmuNetlink`. Socket `AF_NETLINK`/`SOCK_RAW|SOCK_CLOEXEC`/`NETLINK_GENERIC`,
    bind, resolve the family and group with CTRL_CMD_GETFAMILY, `NETLINK_ADD_MEMBERSHIP`, send `SET_FEATURES`
    with `SuppKernCmdReply = 1` and `NLM_F_ACK`, and check the ack. Each failure is a `SysError` or an
    `IllegalStateException` naming the step.

- [ ] **Step 1: Write the failing tests** in `NetlinkSuite`. The golden bytes are written by hand from the netlink
  wire format (16-byte `nlmsghdr`, 4-byte `genlmsghdr`, NLA headers):
  - "GETFAMILY request bytes": the exact bytes for `getFamily("TCM-USER", 1)` (`nlmsg_type = GENL_ID_CTRL = 0x10`,
    `cmd = CTRL_CMD_GETFAMILY = 3`, attr `CTRL_ATTR_FAMILY_NAME = 2` with a NUL-terminated name padded to 4)
  - "family reply parses id and groups": a hand-built `CTRL_CMD_NEWFAMILY` with `CTRL_ATTR_FAMILY_ID = 1` and
    nested `CTRL_ATTR_MCAST_GROUPS = 7` containing "config" → id 5 gives `Right((familyId, Map("config" -> 5)))`
  - "added event parses", "reconfig size parses", "reconfig write cache parses"
  - "missing minor is Left", "v2 without device id is Left"
  - "two messages in one buffer parse"
  - "reply bytes carry status and device id": exact bytes for `reply(0x1a, 9, 4, -22, 3)`
  - "a truncated attribute is Left, not an exception"
- [ ] **Step 2: Run** `sbt 'testOnly *NetlinkSuite'`. Expected: compile failure.
- [ ] **Step 3: Implement `TcmuGenl`, `GenlCodec` and `TcmuNetlink`.** `TcmuNetlink.open` is exercised only by the
  root e2e run (Task 12).
- [ ] **Step 4: Run** the same command. Expected: all pass.
- [ ] **Step 5: Commit** `vdisk.tcmu: generic netlink codec and socket`.

### Task 8: configfs, sysfs and cfgstring

**Files:**
- Create: `vdisk/tcmu/Configfs.scala`, `vdisk/tcmu/Cfgstring.scala`
- Test: `ConfigfsSuite.scala`, `CfgstringSuite.scala`

**Interfaces:**
- Consumes: `RingControl` (Task 6).
- Produces:
  - `final case class UioName(hba: String, device: String, subtype: String, cfgstring: String)`.
    `object UioName`: `parse(s: String): Either[String, UioName]`, for the format
    `tcm-user/<hba>/<device>/<subtype>/<cfgstring>` (`libtcmu.c` around line 443). The cfgstring may contain `/`.
  - `final class Configfs(core: os.Path = os.root / "sys" / "kernel" / "config" / "target" / "core", uioClass: os.Path = os.root / "sys" / "class" / "uio", dev: os.Path = os.root / "dev")`:
    - `def hwBlockSize(hba: String, device: String): Int`: `attrib/hw_block_size`
    - `def hwMaxSectors(hba: String, device: String): Int`: `attrib/hw_max_sectors`
    - `def size(hba: String, device: String): Long`: the `Size:` field of `info`, as `tcmu_cfgfs_dev_get_info_u64`
      parses it
    - `def unitSerial(hba: String, device: String): Option[String]`: `wwn/vpd_unit_serial`
    - `def action(hba: String, device: String, name: String, value: Int): Unit`: writes `action/<name>`
    - `def control(hba: String, device: String): RingControl`: `resetRing` does `block_dev 1`, `reset_ring 1`,
      `block_dev 0`, as `libtcmu.c:585-617`
    - `def uioName(minor: Int): Either[String, UioName]`: `<uioClass>/uio<minor>/name`
    - `def mapSize(minor: Int): Long`: `<uioClass>/uio<minor>/maps/map0/size`, hex
    - `def uioDevicePath(minor: Int): os.Path`
    - `def scanUio(): List[(Int, UioName)]`: every `uioN` whose name parses with prefix `tcm-user`
    - Every read failure throws an exception naming the full path and what was expected.
  - `object Cfgstring`:
    - `def parse(cfg: String): Either[String, String]`: the disk name. It accepts `aspen/<disk>` and drops
      anything from the first `;`.
    - Errors: "cfgstring must start with aspen/" for a wrong handler prefix; an empty disk; and for an `@`
      present, "the @<socket> form was removed with tcmu-runner; use aspen/<disk>".
    - `val ConfigDesc: String`: the help targetcli prints, e.g. "aspen/<disk>: <disk> is the name of an Aspen
      virtual disk (aspen list-disks)".

- [ ] **Step 1: Write the failing tests:**
  - `ConfigfsSuite` builds a fake tree under `os.temp.dir()`:
    - "reads block size, max sectors and info Size": the `info` content is copied from a real kernel's format, e.g.
      `"Status: ACTIVATED  Max Data Area: 8MB  ... Size: 8388608\n"` (take the real format from the kernel source
      `target_core_user.c:tcmu_show_configfs_dev_params`)
    - "missing info Size names the path"
    - "resetRing writes block_dev, reset_ring, block_dev in order": `Configfs` writes through
      `protected def writeFile(path: os.Path, value: String): Unit`, and a test subclass records the
      `(path, value)` list, which must be `[(…/action/block_dev,"1"), (…/action/reset_ring,"1"), (…/action/block_dev,"0")]`.
      Also check that `block_dev 0` is written even when the `reset_ring` write throws.
    - "scanUio returns only tcm-user devices", "uio name with slashes in cfgstring parses", "mapSize parses hex"
  - `CfgstringSuite`: "aspen/vd1" → `Right("vd1")`, "aspen/vd1;foo" → `Right("vd1")`, "file/x" → Left,
    "aspen/" → Left, "aspen/vd1@/run/x" → Left containing "removed"
- [ ] **Step 2: Run** `sbt 'testOnly *ConfigfsSuite *CfgstringSuite'`. Expected: compile failure.
- [ ] **Step 3: Implement `Configfs`, `UioName` and `Cfgstring`.**
- [ ] **Step 4: Run** the same command. Expected: all pass.
- [ ] **Step 5: Commit** `vdisk.tcmu: configfs and sysfs access, cfgstring parsing`.

### Task 9: Device lifecycle service

**Files:**
- Create: `vdisk/tcmu/TcmuService.scala`
- Test: `TcmuServiceSuite.scala`

**Interfaces:**
- Consumes: everything above; `LockableBlockDevice`; `DiskHandle.open(client, name, config): Future[DiskHandle]`.
- Produces:
  - `trait TcmuKernel`, the seam the tests fake:
    - `def configfs: Configfs`
    - `def openRing(minor: Int, mapSize: Long): (MemorySegment, KernelChannel, () => Unit)`. The real one opens
      `/dev/uioN` with `O_RDWR|O_CLOEXEC`, calls `mmapShared` in a shared arena, and wraps the fd in
      `UioChannel`; the closer stops and unmaps.
  - `final class TcmuService(kernel: TcmuKernel, openDisk: String => Future[LockableBlockDevice], config: TcmuServiceConfig = TcmuServiceConfig())(using ExecutionContext)`:
    - `def added(minor: Int, reopen: Boolean): Future[Unit]`
    - `def removed(minor: Int): Future[Unit]`
    - `def reconfig(minor: Int, change: ReconfigChange): Future[Unit]`
    - `def startupScan(): Future[Unit]`: calls `added(minor, reopen = true)` for every `aspen` UIO device. Each
      failure is logged and does not stop the others.
    - `def onEvent(event: TcmuEvent): Future[Int]`: the status to reply, `0` or `-errno`
    - `def devices: Map[Int, String]`: minor → disk name
  - `final case class TcmuServiceConfig(acquireTimeout: FiniteDuration = 60.seconds)`. A device add that hasn't
    acquired within this time replies `-ETIMEDOUT` and is torn down. The kernel blocks targetcli meanwhile, and the
    timeout bounds it.
  - The `added` sequence, each failure logged with the device, minor, disk and step:
    1. UIO name → subtype `aspen`, else `-ENODEV`
    2. `Cfgstring.parse`, else `-EINVAL`
    3. Read size, block size and max sectors from configfs
    4. `openDisk(name)`: `DiskNotFound` gives `-ENOENT`
    5. Geometry: the block size must equal the disk's (`-EINVAL`)
    6. `acquire()`
    6a. If the kernel size is larger than the Aspen size: log at warn "growing Aspen disk <name> from Y to X to
        match the targetcli size", then `device.grow(kernelSize)`. `InvalidGeometry`/`InvalidRequest` gives
        `-EINVAL`, and any other failure `-EIO`. Serving starts only after the grow commits.
    7. If `reopen`, `control.resetRing()`
    8. `openRing`, `Ring.open` (`Left` → `-EINVAL`)
    9. Build `DeviceSettings` (`blockObjectSize` from the disk geometry, `wwn` from `unitSerial` or the disk name),
       `ScsiCommands`, `TcmuDevice`, `start()`
    10. Record the device

    On any failure after step 4: `device.close()` (crash-only), and unmap if mapped.
  - `removed`: `TcmuDevice.stop()`, `release()`, then unmap. Reply `0` even if `release` fails, logged: the kernel
    is removing the device regardless.
  - `reconfig` `Size(n)`: `device.grow(n)`, then `ScsiCommands.resized(n)` → `0`. A shrink is `-EINVAL`, as is
    `Config` or `WriteCache` (logged "not supported"). `Unknown` is `-EINVAL`. A reconfig for an unknown minor is
    `-ENODEV`.
  - A second `added` for a minor already present → `-EEXIST`.

- [ ] **Step 1: Write the failing tests** in `TcmuServiceSuite`, with a fake `TcmuKernel` (a temp-dir `Configfs`
  plus `FakeKernel` rings) and `FakeBlockDevice`s:
  - "added opens, acquires and serves": after `added`, a READ submitted on the fake ring completes, and the fake
    device saw `acquire` before `read`
  - "added with reopen resets the ring before serving"
  - "unknown disk is -ENOENT and nothing is left open"
  - "kernel size larger than the Aspen size grows the disk before serving": the fake device saw `acquire`, then
    `grow(16 MiB)`, then the first `read`; a warn line names both sizes; READ CAPACITY reports 16 MiB
  - "a failed grow on add is -EIO and the device is closed"
  - "kernel size smaller than the Aspen size serves the kernel size": READ CAPACITY reports the kernel size
  - "block size mismatch is -EINVAL"
  - "the old @socket cfgstring is -EINVAL"
  - "acquire that never completes times out": `acquireTimeout = 200.millis` → `-ETIMEDOUT`, and the device is
    closed
  - "removed stops serving and releases"
  - "reconfig size grows then raises the unit attention"
  - "reconfig shrink, write cache and config are -EINVAL"
  - "duplicate added is -EEXIST"
  - "startup scan adds every aspen device and survives one failure": two devices, one with an unknown disk; the
    other serves
- [ ] **Step 2: Run** `sbt 'testOnly *TcmuServiceSuite'`. Expected: compile failure.
- [ ] **Step 3: Implement `TcmuKernel` (trait plus `LinuxTcmuKernel`), `TcmuServiceConfig` and `TcmuService`.**
- [ ] **Step 4: Run** the same command. Expected: all pass.
- [ ] **Step 5: Commit** `vdisk.tcmu: device lifecycle from netlink events and startup scan`.

### Task 10: D-Bus service

**Files:**
- Modify: `build.sbt`. Add `"com.github.hypfvieh" % "dbus-java-core" % <latest 5.x>` and
  `"com.github.hypfvieh" % "dbus-java-transport-native-unixsocket" % <same>`. Pick a release at least two weeks
  old and check that it resolves.
- Create: `vdisk/tcmu/dbus/TcmuDbusService.scala`, `deploy/dbus/org.kernel.TCMUService1.conf`
- Test: `TcmuDbusServiceSuite.scala`

**Interfaces:**
- Consumes: `Cfgstring` (Task 8). For the disk existence check, `DiskCatalog.load(client, name): Future[DiskInfo]`
  (fails with `DiskNotFound`).
- Produces:
  - `final class HandlerLogic(diskExists: String => Future[Boolean])`:
    - `def checkConfig(cfg: String, timeout: FiniteDuration = 10.seconds): (Boolean, String)`.
      `(true, "success")` when valid. Otherwise `(false, <reason>)`: a parse error, "disk <x> not found", or
      "could not reach Aspen: <msg>" on a timeout or failure.
    - `def changeMedium(name: String, size: Long, cfg: String): (Int, String)` = `(-EOPNOTSUPP i.e. -95, "ChangeMedium is not supported by vdisk-server")`
    - `val configDesc: String = Cfgstring.ConfigDesc`
  - `final class TcmuDbusService private (conn: DBusConnection)`, with `def close(): Unit`
  - `object TcmuDbusService`: `def start(logic: HandlerLogic): TcmuDbusService`
    - Connect to the system bus and request the name `org.kernel.TCMUService1` without queueing/replacement. If the
      name is owned, throw `IllegalStateException("org.kernel.TCMUService1 is already owned on the system bus (is tcmu-runner running?)")`.
    - Export the ObjectManager at `/org/kernel/TCMUService1` and the handler object at
      `/org/kernel/TCMUService1/aspen`, with interface `org.kernel.TCMUService1`, methods `CheckConfig(s) → (bs)`
      and `ChangeMedium(sts) → (is)`, and a read-only property `ConfigDesc` (s). Match `main.c` and
      `tcmu-handler.xml`. `GetManagedObjects` must return the handler object with that interface's properties,
      because targetcli reads `ConfigDesc` from it (`ui_backstore.py:612`).
  - The policy file allows `user="root"` to own `org.kernel.TCMUService1` and anyone to send to it, copied from
    tcmu-runner's packaged policy.

- [ ] **Step 1: Write the failing tests** in `TcmuDbusServiceSuite` (`HandlerLogic` only):
  - "valid existing disk": `(true, "success")`
  - "parse error is reported"
  - "missing disk is reported"
  - "a failed lookup reports Aspen unreachable"
  - "a lookup that never completes times out with a message" (`timeout = 100.millis`)
  - "ChangeMedium is refused"
- [ ] **Step 2: Run** `sbt 'testOnly *TcmuDbusServiceSuite'`. Expected: compile failure.
- [ ] **Step 3: Implement `HandlerLogic`, `TcmuDbusService` and the policy file.** Real bus wiring is checked in
  Task 12's e2e run.
- [ ] **Step 4: Run** the same command. Expected: all pass, and `sbt compile` shows no new warnings.
- [ ] **Step 5: Commit** `vdisk.tcmu: org.kernel.TCMUService1 D-Bus service for targetcli`.

### Task 11: Integration against a real DiskHandle

**Files:**
- Create: `src/test/scala/.../vdisk/tcmu/TcmuIntegrationSuite.scala`
- Delete: `src/test/scala/.../vdisk/bridge/BridgeIntegrationSuite.scala`. The bridge itself is deleted in Task 12;
  delete only this suite's file here.

**Interfaces:**
- Consumes: `TcmuService` with a fake `TcmuKernel` (reuse Task 9's), and `DiskHandle.open(client, _, Held)` as
  `openDisk`, on `IntegrationTestSuite`. Copy the pool settings and the `createDisk`/`pattern` helpers from
  `BridgeIntegrationSuite`: IDA `RandomAccessReedSolomon(5, 3, 4, 64)`, 8 MiB disk, 4096 block size,
  `VDiskConfig(coalesceWindow = 10.seconds)`.

- [ ] **Step 1: Write the tests** (`atest`):
  - "write then read through the ring": a WRITE 10 of 64 KiB, then a READ 10, gives the same bytes
  - "FUA write is durable after takeover": a WRITE 16 with FUA, then close the handle crash-only, open a new
    `DiskHandle`, `acquire`, and read: the data is present
  - "SYNCHRONIZE CACHE makes earlier writes durable", tested the same way
  - "UNMAP reads back zeros"
  - "grow before targetcli resize still opens": `DiskCatalog.grow` to 16 MiB, with configfs reporting 8 MiB;
    `added` succeeds and READ CAPACITY reports 8 MiB. Then reconfig `Size(16 MiB)` gives a unit attention, then
    READ CAPACITY reports 16 MiB.
  - "add with a larger targetcli size grows the Aspen disk": configfs reports 16 MiB for an 8 MiB disk. After
    `added`, `DiskCatalog.load` reports 16 MiB, and a write at 12 MiB reads back.
  - "a fenced owner returns NOT READY 0x040B": a second handle acquires; a WRITE then FLUSH on the first returns
    the fenced sense
- [ ] **Step 2: Run** `sbt 'testOnly *TcmuIntegrationSuite'`. Expected: all pass. A failure is a bug in Tasks 2–9:
  fix it there with a unit test that pins it.
- [ ] **Step 3: Commit** `vdisk.tcmu: integration tests through the ring to DiskHandle`.

### Task 12: CLI wiring, removal of the bridge and plugin, e2e script

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/cmdline/Main.scala`
  - `:458-459`: remove the `--socket` option and the `vdiskSocket` config field
  - `:868`: the description becomes "Serves virtual disks to the Linux kernel's TCMU target (requires root)"
  - `:1012` and `run_vdisk_server` at `:1437`
- Delete: `src/main/scala/org/aspen_ddp/aspen/vdisk/bridge/`, `src/test/scala/.../vdisk/bridge/`, `native/tcmu-aspen/`
- Create: `scripts/vdisk-e2e/` (setup, teardown and the fio job, ported from `native/tcmu-aspen/e2e/`). Add steps
  for a `vdisk-server` restart under I/O, and for grow then `targetcli` resize.

**Interfaces:**
- Consumes: `TcmuService`, `LinuxTcmuKernel`, `TcmuNetlink`, `TcmuDbusService`, `HandlerLogic`, `DiskHandle.open`,
  `DiskCatalog.load`.
- Produces: `run_vdisk_server(session: ClientSession, onetworkCacheDir: Option[os.Path] = None): Int`. In order:
  1. `Libc.geteuid() != 0` → print "Error: vdisk-server must run as root" and return 1
  2. `configureLogging()`
  3. `createAspenClient`, then `network.startIoThread`
  4. `TcmuNetlink.open()`
  5. `TcmuDbusService.start(...)`
  6. `TcmuService.startupScan()`: wait for it and log the results
  7. Start one platform thread looping `netlink.receive()`. Each event goes to `service.onEvent`, and its Future's
     completion sends `netlink.reply`, so a slow acquire doesn't block other events (`TcmuNetlink.reply` is
     synchronized). A `Left` from parsing is logged and dropped. A thrown exception is logged and the loop
     continues.
  8. Print "Serving virtual disks through TCMU" and `Thread.currentThread.join()`

  A failure in steps 4 or 5 prints `Error: <message>` and returns 1. This is the only point where the process exits
  on its own.

- [ ] **Step 1: Make the changes above.** Then `grep -rn "bridge\|BridgeServer\|tcmu-aspen\|vdisk.sock" src native docs CLAUDE.md`
  must show only documentation that Task 13 rewrites.
- [ ] **Step 2: Run** `sbt compile` and then the full `sbt test` with a 70 s timeout. Expected: compiles with no new
  warnings, and all tests pass.
- [ ] **Step 3: Commit** `vdisk-server: serve through native TCMU; remove the tcmu-runner bridge and plugin`.

### Task 13: Documentation

**Files:**
- Modify:
  - `src/main/scala/org/aspen_ddp/aspen/vdisk/DESIGN.md`: rewrite sections 1, 2, 7–12 for the new stack. Replace
    the protocol and plugin sections with: the ring, the netlink lifecycle, SCSI emulation, D-Bus, failure
    logging, and a "Where to look" table for the new files.
  - `docs/vdisk-tcmu-setup.md`: no tcmu-runner (stop and disable it), install the policy file, run as root, the
    `cfgstring=aspen/<disk>` form, restart behavior, grow, and FUA now advertised.
  - `CLAUDE.md`: the VDisks bullet. `vdisk-server` drives TCMU directly through FFM (`vdisk.tcmu`), owns
    `org.kernel.TCMUService1` on D-Bus for targetcli, and runs as root. Remove the `handler_aspen.so` sentence.
  - `Todo.txt`: remove DPOFUA=0, grow-then-reopen, and the resize/reopen race. Rewrite the e2e item to point at
    `scripts/vdisk-e2e/`.
  - `docs/superpowers/specs/2026-10-30-iscsi-tcmu-target-design.md`: one line at the top pointing to the new
    design for the frontend.

- [ ] **Step 1: Make the edits.** Every file path and class name mentioned must exist: check with `ls` / `grep`.
- [ ] **Step 2: Commit** `docs: native TCMU frontend`.
