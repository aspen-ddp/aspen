# tcmu-runner Bridge Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Serve Aspen virtual disks to LIO through tcmu-runner. This needs a C handler plugin, a framed Unix-domain-socket protocol, and a JVM daemon (`vdisk-server`) that serves any number of disks.

**Architecture:** A frontend-agnostic `LockableBlockDevice` (a `BlockDevice` plus lock acquire, release and state) is the seam a future foreign-memory TCMU frontend would also use. `DiskHandle` implements it over `VirtualDisk`. The bridge is one frontend:
- **JVM side:** a Netty epoll domain-socket server, with one `BridgeSession` per connection. Each connection serves exactly one disk.
- **C side:** a thin tcmu-runner handler (`handler_aspen.so`). Its framing code (`aspen_proto.c`) has no tcmu dependency, so both it and the handler glue are unit-tested with `make check` against stub tcmu functions.
- **Golden byte vectors:** the same vectors are asserted in Scala and in C, which pins the wire format on both sides.

**Tech Stack:**
- **JVM:** Scala 3.7.3, Netty 4.2.19 epoll (`netty-transport-native-epoll` is already a dependency), ScalaTest 3.2.18, JDK 25 (`SocketChannel` with `StandardProtocolFamily.UNIX` for the test client).
- **C:** C11 with gcc and make, built against tcmu-runner v1.5.4 headers (the version installed on the test host).

**Spec:** `docs/superpowers/specs/2026-10-30-iscsi-tcmu-target-design.md` (sections "Frontend interface", "Operations", "Failover lifecycle", "Testing"). Part 2 of 4. Part 1, the engine, is done. Part 3 is ALUA failover and part 4 is the cell-size measurements.

**Scope boundary with part 3:** the protocol and `LockableBlockDevice` include LOCK, UNLOCK and LOCK_STATE now. In this part the plugin sends LOCK itself right after OPEN (single path). Part 3 moves this to tcmu's `lock`/`unlock`/`get_lock_state` callbacks and configures ALUA.

## Global Constraints

- The C plugin stays thin. It parses the cfgstring, maintains the socket, frames commands and completions, and copies data between TCMU iovecs and the socket. No caching, and no SCSI logic beyond what tcmu-runner requires (spec, "Frontend interface").
- Errors map to SCSI status in the frontend, not on the Aspen side. The JVM reports semantic statuses and the C plugin translates them to `TCMU_STS_*`.
- Payloads never pass through a serialization library. WRITE payload bytes are copied once in the JVM (`ByteBuf` -> `Array[Byte]`, which `BlockDevice.write` requires). READ replies wrap the returned array without copying.
- Many requests can be outstanding on one connection, and completions return in completion order, not arrival order.
- Aspen holds the authoritative size. OPEN fails if the size tcmu reports disagrees. RESIZE commits the grow before it is acknowledged.
- WCE=1 (volatile write cache reported) and FUA must be honored. tcmu-runner 1.5.4's generic write path ignores the FUA bit (verified in `tcmur_cmd_handler.c:handle_write`), so the plugin reads it from the CDB itself.
- Crash-only. A dropped connection closes the device without flushing. Only UNLOCK flushes.
- The daemon does not time out requests itself.
- Tests: correctness > reliability > speed, no flaky tests, and 70 s is enough to detect hangs. Use small disks: the integration suites use the same IDA and pool slice size as `VirtualDiskSuite`.
- Unix socket paths must stay under 108 bytes. Tests create sockets under `os.temp.dir(prefix = "vdb")`.
- Commit directly on `main`. Don't push.

## Wire format (pinned; used by Tasks 2, 3, 5 and 6)

All integers are big-endian.

**Request:** a 32-byte header followed by `payloadLength` bytes.

| off | size | field |
|---|---|---|
| 0 | u16 | magic `0xA5D1` |
| 2 | u8 | version `1` |
| 3 | u8 | op |
| 4 | u32 | flags (bit 0 = FUA, WRITE only) |
| 8 | u64 | requestId (chosen by the client, echoed in the reply) |
| 16 | u64 | offset (bytes) |
| 24 | u32 | length (bytes) |
| 28 | u32 | payloadLength (at most `MaxPayload` = 64 MiB) |

**Response:** a 24-byte header followed by `payloadLength` bytes.

| off | size | field |
|---|---|---|
| 0 | u16 | magic `0xA5D2` |
| 2 | u8 | version `1` |
| 3 | u8 | op (echoed) |
| 4 | u32 | status |
| 8 | u64 | requestId |
| 16 | u32 | payloadLength |
| 20 | u32 | reserved, always 0 |

**Ops:**

| op | name | request | OK reply payload |
|---|---|---|---|
| 1 | OPEN | payload = disk name (UTF-8); offset = size tcmu reports; length = tcmu block size | u32 blockObjectSize, u32 logicalBlockSize |
| 2 | READ | offset, length | `length` bytes |
| 3 | WRITE | offset; length == payloadLength; payload = data; flags FUA | empty |
| 4 | FLUSH | nothing | empty |
| 5 | UNMAP | payload = n × (u64 offset, u64 length), with n >= 1 | empty |
| 6 | RESIZE | offset = new size in bytes | empty |
| 7 | LOCK | nothing | empty |
| 8 | UNLOCK | nothing | empty |
| 9 | LOCK_STATE | nothing | u8: 0 Unlocked, 1 Locked, 2 Fenced, 3 Failed |

**Statuses:** 0 Ok, 1 InvalidRequest, 2 OutOfRange, 3 Fenced, 4 NotOwner, 5 DiskFailed, 6 NotFound, 7 SizeMismatch, 8 ProtocolError, 9 Internal. A non-Ok reply's payload is a UTF-8 message of at most 1024 bytes.

A bad magic, a bad version or a `payloadLength` over `MaxPayload` is a framing violation: the JVM closes the connection. An unknown op is answered with ProtocolError.

**Golden vectors:**
- A WRITE request with FUA, id `0x0102030405060708`, offset 4096 and payload `DE AD BE EF` (36 bytes):
  `A5 D1 01 03 00 00 00 01 01 02 03 04 05 06 07 08 00 00 00 00 00 00 10 00 00 00 00 04 00 00 00 04 DE AD BE EF`
- A READ reply with status Ok, id 7 and payload `01 02` (26 bytes):
  `A5 D2 01 02 00 00 00 00 00 00 00 00 00 00 00 07 00 00 00 02 00 00 00 00 01 02`

**cfgstring:** tcmu passes `aspen/<disk>[@<socket path>]`. The socket defaults to `/run/aspen/vdisk.sock`. Disk names may not contain `@`.

## Review Focus

1. **FUA writes:** tcmu's generic write path drops the FUA bit. A FUA WRITE_10/12/16 must reach the JVM with flag 1, and every other write-callback user (FORMAT UNIT, WRITE SAME emulation) with flag 0. Test: Task 6 `test_handler` "fua bit".
2. **Daemon restart under a mounted filesystem:** outstanding and new commands must complete BUSY, so the initiator retries, and the plugin must report the connection lost (once) so tcmu-runner reopens. They must not complete with a hard error that remounts ext4 read-only. Test: Task 6 `test_handler` "disconnect".
3. **Large and fragmented frames:** a 4 MiB WRITE, and frames split at every byte boundary, decode exactly once. Tests: Task 2 "byte at a time" and Task 3 "4 MiB write round trip".
4. **cfgstring and name edge cases:** a missing socket path uses the default, and an empty disk name is rejected. A disk named with `@` can't be created, because its cfgstring would be ambiguous. Tests: Task 1 DiskCatalog test and Task 5 cfgstring tests.
5. **Socket path left over from an earlier daemon:** a stale socket file from a crashed daemon is replaced, but starting on a path where a live daemon is listening fails instead of stealing it. Test: Task 3 "stale socket" and "live socket".

---

### Task 1: `LockableBlockDevice` and `DiskHandle`

**Files:**
- Create: `src/main/scala/org/aspen_ddp/aspen/vdisk/DiskHandle.scala`
- Modify: `src/main/scala/org/aspen_ddp/aspen/vdisk/BlockDevice.scala` (add the trait), `VDiskError.scala` (add `NotOwner`), `DiskCatalog.scala` (validate the name in `create`)
- Test: `src/test/scala/org/aspen_ddp/aspen/vdisk/DiskHandleSuite.scala`, plus one test in `DiskCatalogSuite.scala`

**Interfaces:**
- Consumes: `VirtualDisk.acquire(client, name, config): Future[VirtualDisk]`, `VirtualDisk.state: DiskState` (`Serving | Fenced | Failed(reason)`), `VirtualDisk.close()`, `DiskCatalog.load(client, name): Future[DiskInfo]` (fails `DiskNotFound`), `FenceState.fromArray`, `DiskInfo.fencePointer`.
- Produces:
  ```scala
  // BlockDevice.scala
  enum LockState:
    case Unlocked, Locked, Fenced
    case Failed(reason: String)
  trait LockableBlockDevice extends BlockDevice:
    /** Takes ownership (fencing any other owner). Idempotent while Locked; concurrent calls share one takeover */
    def acquire(): Future[Unit]
    /** Locked: flush, then close. Any other state: close. Ends Unlocked */
    def release(): Future[Unit]
    def lockState: LockState
  // VDiskError.scala
  case object NotOwner extends VDiskError("this handle does not own the disk")
  // DiskHandle.scala
  final class DiskHandle private (...) extends LockableBlockDevice
  object DiskHandle:
    def open(client: AspenClient, name: String, config: VDiskConfig = VDiskConfig()): Future[DiskHandle]
  ```

Behavior:
- `open` loads the disk but does not take ownership.
- `geometry` is the owned disk's geometry when there is one, and otherwise the loaded one.
- I/O and `grow` delegate to the owned `VirtualDisk` while `Locked`. They fail with `NotOwner` when `Unlocked`, and with the disk's stop error (`Fenced` / `DiskFailed`) when it has stopped.
- `grow(newSize)` with `newSize == geometry.size` succeeds without a transaction (tcmu may repeat a RESIZE).
- `lockState` maps from the owned disk's `state`: no disk is `Unlocked`, `Serving` is `Locked`, `Fenced` is `Fenced`, and `Failed(r)` is `Failed(r)`.
- `acquire` from `Fenced` or `Failed` closes the old disk and takes over again.
- `release` during an in-flight `acquire` waits for it, then releases.
- `close()` is crash-only: it closes the owned disk without flushing, and the state becomes `Unlocked`.

- [ ] **Step 1: Write failing tests.** `DiskHandleSuite extends IntegrationTestSuite` uses the same `testIda`, `testPoolMaxSliceSize`, disk size (65536), the `Held` config, and a teardown that closes every handle and disk, all as in `VirtualDiskSuite`. Tests:
  - "open does not take ownership": after `VirtualDisk.acquire(client, "d")` and `DiskHandle.open(client, "d")`, the disk's write and flush still succeed. The handle's `lockState == LockState.Unlocked`, and `handle.read(0, 4096)` fails with `NotOwner`.
  - "acquire enables I/O": after `acquire()`, `lockState == Locked`, and write then read returns the data.
  - "acquire while locked keeps the same owner": write with `Held`, call `acquire()` again, and the read still returns the unflushed data. The fence epoch, read from `info.fencePointer` with `FenceState.fromArray`, went up by exactly 1 in total.
  - "concurrent acquires share one takeover": `Future.sequence(List(h.acquire(), h.acquire()))` succeeds, and the fence epoch went up by exactly 1.
  - "a second handle fences the first": h1 and h2 both acquire. h1's write followed by flush fails with `Fenced`, and `h1.lockState == Fenced`. After `h1.acquire()`, `h1.lockState == Locked` and `h2.lockState == Fenced`.
  - "release flushes": h1 acquires, writes with `Held` and releases (`lockState == Unlocked`). h2 acquires and reads the data.
  - "close does not flush": h1 acquires, writes with `Held`, flushes write A, writes B and calls `close()`. h2 reads A, and `lockState` on h1 is `Unlocked`.
  - "grow to the current size is a no-op; larger grows": `grow(65536)` succeeds with the size unchanged. `grow(131072)` makes `geometry.size == 131072`. `grow(32768)` fails with `InvalidGeometry`.
  - "open of a missing disk fails DiskNotFound".
  - In `DiskCatalogSuite`: "create rejects a name containing '@' or an empty name". `DiskCatalog.create(client, "a@b", ...)` and `create(client, "", ...)` fail with `InvalidRequest`.
- [ ] **Step 2: Run them.** `sbt 'testOnly *DiskHandleSuite *DiskCatalogSuite'`. Expected: they fail to compile (`DiskHandle` not found).
- [ ] **Step 3: Implement** the interfaces above. Serialize state changes on the handle's monitor, and never hold it across a Future (part 1's rule). Add the name check to the start of `DiskCatalog.create`.
- [ ] **Step 4: Run the suites, then `sbt 'testOnly *vdisk*'`.** Expected: all pass. Run `DiskHandleSuite` 20 times in one sbt session with no failures.
- [ ] **Step 5: Commit** with the message "vdisk: LockableBlockDevice seam and DiskHandle with acquire/release/lock state".

### Task 2: Wire-protocol codec (JVM)

**Files:**
- Create: `src/main/scala/org/aspen_ddp/aspen/vdisk/bridge/Protocol.scala`, `src/main/scala/org/aspen_ddp/aspen/vdisk/bridge/FrameDecoder.scala`
- Test: `src/test/scala/org/aspen_ddp/aspen/vdisk/bridge/ProtocolSuite.scala` (AnyFunSuite with `EmbeddedChannel`, in the style of `StreamFrameDecoderSuite`)

**Interfaces:**
- Produces:
  ```scala
  object Protocol:
    val RequestMagic = 0xA5D1; val ResponseMagic = 0xA5D2; val Version = 1
    val RequestHeaderSize = 32; val ResponseHeaderSize = 24; val MaxPayload: Int = 64 * 1024 * 1024
    val FlagFua = 1
    object Op:     // Open=1 Read=2 Write=3 Flush=4 Unmap=5 Resize=6 Lock=7 Unlock=8 LockState=9
    object Status: // Ok=0 InvalidRequest=1 OutOfRange=2 Fenced=3 NotOwner=4 DiskFailed=5 NotFound=6
                   // SizeMismatch=7 ProtocolError=8 Internal=9
    /** Header plus payload as one composite buffer; the payload is not copied. Takes ownership of `payload` */
    def encodeResponse(op: Int, status: Int, requestId: Long, payload: ByteBuf): ByteBuf
    def errorPayload(msg: String): ByteBuf   // UTF-8, truncated to 1024 bytes
  /** `payload` is a retained slice of the inbound buffer; whoever handles the request releases it */
  final case class Request(op: Int, flags: Int, requestId: Long, offset: Long, length: Int, payload: ByteBuf)
  final class FrameViolation(msg: String) extends io.netty.handler.codec.CorruptedFrameException(msg)
  final class FrameDecoder extends io.netty.handler.codec.ByteToMessageDecoder
  ```
- Values: the opcodes, statuses and golden vectors are in "Wire format" above.

- [ ] **Step 1: Write failing tests:**
  - "decodes the golden WRITE request": the 36 golden bytes produce exactly one `Request(3, 1, 0x0102030405060708L, 4096, 4, <DE AD BE EF>)`.
  - "byte at a time": the same bytes written one byte per `writeInbound` produce exactly one equal request.
  - "two requests in one buffer decode to two".
  - "a 4 MiB payload split in 3 chunks decodes intact": compare the payload bytes.
  - "bad magic", "bad version" and "payload over MaxPayload": `writeInbound` throws a `DecoderException` whose cause is a `FrameViolation`. The oversize case throws as soon as the 32 header bytes arrive, without waiting for the payload.
  - "encodes the golden READ reply": `encodeResponse(2, 0, 7, <01 02>)` equals the 26 golden bytes.
  - "errorPayload truncates to 1024 bytes".
  - Every test releases its decoded payloads, and `EmbeddedChannel.finishAndReleaseAll()` reports no leaks.
- [ ] **Step 2: Run them.** `sbt 'testOnly *ProtocolSuite'`. Expected: they fail to compile.
- [ ] **Step 3: Implement** with Netty `ByteBuf` big-endian accessors. Emit `in.readRetainedSlice(payloadLength)` for the payload.
- [ ] **Step 4: Run them.** Expected: all pass.
- [ ] **Step 5: Commit** with the message "vdisk bridge: frame protocol codec".

### Task 3: `BridgeSession` and `BridgeServer`

**Files:**
- Create: `src/main/scala/org/aspen_ddp/aspen/vdisk/bridge/BridgeSession.scala`, `src/main/scala/org/aspen_ddp/aspen/vdisk/bridge/BridgeServer.scala`
- Test: `src/test/scala/org/aspen_ddp/aspen/vdisk/bridge/TestBridgeClient.scala` (a blocking `SocketChannel` with `StandardProtocolFamily.UNIX`, its own encoder written from the wire table and the constants in `Protocol`, plus a reader thread that completes a `Promise` per requestId and records arrival order), `BridgeServerSuite.scala` (AnyFunSuite against a `FakeDevice`), and `BridgeIntegrationSuite.scala` (an `IntegrationTestSuite` with the `VirtualDiskSuite` IDA settings).

**Interfaces:**
- Consumes: `LockableBlockDevice`, `DiskHandle.open` (Task 1), and `Protocol`, `Request` and `FrameDecoder` (Task 2).
- Produces:
  ```scala
  final class BridgeServer(socketPath: os.Path, openDisk: String => Future[LockableBlockDevice]):
    /** Binds the socket. A leftover socket file nobody listens on is replaced. If a connect to it succeeds, fails
      * with IllegalStateException("socket in use: <path>") */
    def start(): Unit
    /** Closes the listener and every session; their devices are closed crash-only */
    def stop(): Unit
  ```
  The server uses `MultiThreadIoEventLoopGroup` with `EpollIoHandler.newFactory()` and `EpollServerDomainSocketChannel`, and installs a pipeline of `FrameDecoder` then `BridgeSession` per connection.

Session behavior:
- The first request must be OPEN. A request of any other op before OPEN, or a second OPEN, gets ProtocolError.
- OPEN's result:
  - The device on success. If its `geometry.size != offset` or `logicalBlockSize != length`, the reply is SizeMismatch and the device is closed.
  - `DiskNotFound` replies NotFound.
- Error mapping, from `VDiskError` to status:
  - `InvalidRequest` and `InvalidGeometry` → InvalidRequest
  - `OutOfRange` → OutOfRange
  - `Fenced` → Fenced
  - `NotOwner` → NotOwner
  - `DiskFailed` → DiskFailed
  - `DiskNotFound` → NotFound
  - anything else → Internal (logged)
- Request validation:
  - A WRITE whose `length != payload.readableBytes` gets InvalidRequest.
  - An UNMAP payload that is empty or not a multiple of 16 bytes gets InvalidRequest.
  - Any other op that carries a payload gets InvalidRequest. The exception is OPEN, whose payload is the disk name.
- Completion: each reply is written from the request's future with `ExecutionContext.parasitic` through `ctx.writeAndFlush`.
- Connection end:
  - `channelInactive` calls `close()` on the device: a crash-only close, not `release`.
  - `exceptionCaught` with a `FrameViolation` logs the error and closes the channel.

- [ ] **Step 1: Write the failing `BridgeServerSuite` tests.** `FakeDevice` returns a caller-controlled `Promise` per call and records the calls and their arguments.
  - "open then lock then I/O": the fake's `read` returns its bytes, and a write reaches the fake with the exact bytes and `fua = true` when the flag is set.
  - "open reply carries blockObjectSize and logicalBlockSize".
  - "size mismatch closes the device and replies SizeMismatch"; "open of a missing disk replies NotFound".
  - "requests before OPEN and a second OPEN reply ProtocolError"; "an unknown op replies ProtocolError".
  - "completions return in completion order": two READs where the second's promise completes first. The client sees id 2 before id 1, each with its own data.
  - "error mapping": a table test over each `VDiskError` case to the expected status, with the message in the payload.
  - "WRITE length mismatch, bad UNMAP payloads and stray payloads reply InvalidRequest"; "UNMAP passes every range in order".
  - "LOCK, UNLOCK and LOCK_STATE call acquire, release and lockState": the LOCK_STATE byte matches each of the 4 states.
  - "disconnect closes the device without release"; "UNLOCK then disconnect calls release once and close once".
  - "bad magic closes the connection and the server accepts the next one": the client reads EOF, then a fresh connection works.
  - "200 outstanding writes are all answered".
  - "stale socket": a socket file left by a server that was stopped without unlinking is replaced. "live socket": a second `start()` on the same path while the first is running throws `IllegalStateException`.
- [ ] **Step 2: Write the failing `BridgeIntegrationSuite` tests** against real `DiskHandle`s, through `BridgeServer(path, DiskHandle.open(client, _, Held))`:
  - "4 MiB write round trip": the disk is 8 MiB and the write is at offset 0. Read it back.
  - "FUA data survives a dropped connection": OPEN, LOCK, WRITE with FUA, then close the socket abruptly. A new connection (OPEN, LOCK, READ) returns the data.
  - "FLUSH makes earlier writes survive a dropped connection".
  - "RESIZE commits the grow": after RESIZE to double the size, `DiskCatalog.load` reports the new size.
- [ ] **Step 3: Run them.** `sbt 'testOnly *vdisk.bridge*'`. Expected: they fail to compile.
- [ ] **Step 4: Implement** `BridgeSession` and `BridgeServer` as above.
- [ ] **Step 5: Run `sbt 'testOnly *vdisk*'`, then run both bridge suites 20 times in one sbt session.** Expected: everything passes every time.
- [ ] **Step 6: Commit** with the message "vdisk bridge: Unix-socket server and per-connection session".

### Task 4: `vdisk-server` CLI command

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/cmdline/Main.scala` (Args field `vdiskSocket: os.Path`, defaulting to `os.Path("/run/aspen/vdisk.sock")`), `UsageGuide.md`, `CLAUDE.md` (the vdisk bullet)
- Test: `src/test/scala/org/aspen_ddp/aspen/cmdline/MainSuite.scala`

**Interfaces:**
- Consumes: `BridgeServer` and `DiskHandle.open`.
- Produces: `aspen vdisk-server [--socket <path>] [--network-cache-dir <dir>] <client-config args>`. It mirrors `amoebafs`:
  - It calls `configureLogging()`, then `createAspenClient(session, onetworkCacheDir)`, then `network.startIoThread(client)`.
  - It creates the socket's parent directory if it is missing, starts `BridgeServer(socket, DiskHandle.open(client, _))`, and then calls `Thread.currentThread.join()`.

- [ ] **Step 1: Write failing MainSuite tests**, in the style of the existing `amoebafs` argument tests:
  - "vdisk-server parses --socket and --network-cache-dir".
  - "vdisk-server defaults the socket to /run/aspen/vdisk.sock".
- [ ] **Step 2: Run them.** `sbt 'testOnly *MainSuite'`. Expected: FAIL.
- [ ] **Step 3: Implement** the command and `run_vdisk_server`.
  - **UsageGuide.md:** a "Serving virtual disks" section covering the daemon command and its flags, with a pointer to `docs/vdisk-tcmu-setup.md` (Task 8).
  - **CLAUDE.md:** extend the vdisk bullet with one line: "`vdisk-server` serves disks to tcmu-runner's `handler_aspen.so` (`native/tcmu-aspen`) over a Unix socket; `LockableBlockDevice` is the frontend seam".
- [ ] **Step 4: Run them.** Expected: PASS.
- [ ] **Step 5: Commit** with the message "cmdline: vdisk-server daemon".

### Task 5: C framing module and its tests

**Files:**
- Create: `native/tcmu-aspen/aspen_proto.h`, `native/tcmu-aspen/aspen_proto.c`, `native/tcmu-aspen/test_proto.c`, `native/tcmu-aspen/Makefile`, `native/tcmu-aspen/.gitignore` (build outputs)

**Interfaces:**
- Produces (no tcmu headers are included):
  ```c
  #define ASPEN_REQ_HDR_SIZE 32
  #define ASPEN_RESP_HDR_SIZE 24
  #define ASPEN_DEFAULT_SOCKET "/run/aspen/vdisk.sock"
  enum aspen_op { ASPEN_OP_OPEN = 1, /* ... */ ASPEN_OP_LOCK_STATE = 9 };
  enum aspen_status { ASPEN_ST_OK = 0, /* ... */ ASPEN_ST_INTERNAL = 9 };
  #define ASPEN_FLAG_FUA 1u
  struct aspen_req_hdr  { uint8_t op; uint32_t flags; uint64_t id; uint64_t offset; uint32_t length; uint32_t payload_len; };
  struct aspen_resp_hdr { uint8_t op; uint32_t status; uint64_t id; uint32_t payload_len; };
  void aspen_encode_req(uint8_t out[ASPEN_REQ_HDR_SIZE], const struct aspen_req_hdr *h);
  /* 0, or -EPROTO on bad magic or version */
  int aspen_decode_resp(const uint8_t in[ASPEN_RESP_HDR_SIZE], struct aspen_resp_hdr *h);
  /* cfg is tcmu's full cfgstring ("aspen/<disk>[@<socket>]"). 0 with malloc'd *disk and *socket_path, or
   * -EINVAL with *reason set to a malloc'd message */
  int aspen_parse_cfgstring(const char *cfg, char **disk, char **socket_path, char **reason);
  ```
- Makefile:
  - Targets: `all` (`handler_aspen.so`, added in Task 6), `check` (builds and runs the tests), `clean`.
  - Flags: `CFLAGS ?= -O2 -g -Wall -Wextra -Werror -std=gnu11 -fPIC -pthread`.

- [ ] **Step 1: Write `test_proto.c`**, a plain `main` with an assert macro that prints the failing line and exits 1. Cases:
  - Encoding the golden WRITE header gives the first 32 golden bytes.
  - Decoding the golden READ reply header gives op 2, status 0, id 7 and payload_len 2.
  - A bad magic and a bad version each give `-EPROTO`.
  - cfgstrings:
    - `"aspen/disk1@/run/x.sock"` → `disk1`, `/run/x.sock`
    - `"aspen/disk1"` → `disk1`, `ASPEN_DEFAULT_SOCKET`
    - `"aspen/"`, `"aspen/@/x"` and `"aspen/d@"` → `-EINVAL` with a reason
    - `"other/d"` → `-EINVAL`
- [ ] **Step 2: Run `make -C native/tcmu-aspen check`.** Expected: it fails to link (`aspen_encode_req` is undefined).
- [ ] **Step 3: Implement `aspen_proto.c`.** Use explicit shifts for big-endian, with no `htobe64` portability shims.
- [ ] **Step 4: Run `make -C native/tcmu-aspen check`.** Expected: it prints `test_proto: all passed` and exits 0.
- [ ] **Step 5: Commit** with the message "tcmu-aspen: C frame protocol module".

### Task 6: tcmu-runner handler `handler_aspen.so`

**Files:**
- Create: `native/tcmu-aspen/handler_aspen.c`, `native/tcmu-aspen/test_handler.c`, `native/tcmu-aspen/tcmu_stub.c` (stub definitions of every tcmu function the handler calls, for `test_handler` only), `native/tcmu-aspen/README.md`
- Add a submodule: `git submodule add https://github.com/open-iscsi/tcmu-runner.git submodules/tcmu-runner`, then `git -C submodules/tcmu-runner checkout v1.5.4`.
- Modify: `native/tcmu-aspen/Makefile`. Add `TCMU_SRC ?= ../../submodules/tcmu-runner` and `INCLUDES = -I$(TCMU_SRC) -I$(TCMU_SRC)/ccan`. These two include paths are enough: a stub handler compiled with them was verified to build. Also add the targets `handler_aspen.so`, `test_handler` (part of `check`), `symcheck` and `install` (`install -m 0755 handler_aspen.so $(DESTDIR)/usr/lib/x86_64-linux-gnu/tcmu-runner/`, which the user runs with sudo).

**Interfaces:**
- Consumes: `aspen_proto.h` (Task 5) and the tcmu-runner 1.5.4 handler API:
  - `struct tcmur_handler`, `tcmur_register_handler`, `tcmur_cmd_complete(dev, cmd, rc)`, `tcmu_notify_conn_lost(dev)`.
  - Per-device state uses `tcmur_dev_set_private` / `tcmur_dev_get_private`. The runner itself owns `tcmu_dev_get_private`.
  - `tcmu_dev_get_cfgstring`, `tcmu_dev_get_num_lbas`, `tcmu_dev_get_block_size`, `tcmu_dev_set_write_cache_enabled`, `tcmu_dev_set_unmap_enabled`, `tcmu_dev_set_opt_unmap_gran`, `tcmu_dev_set_unmap_gran_align`, `tcmu_memcpy_into_iovec`.
  - `cmd->lib_cmd->cdb`.
- Produces:
  - **Handler:** `handler_init()`, which registers `{ .name = "Aspen virtual disk", .subtype = "aspen", .cfg_desc = "<disk>[@<socket path>]", .nr_threads = 0, check_config, open, close, read, write, flush, unmap, reconfig }`.
  - **Test seam:** the handler is compiled into `test_handler` together with `tcmu_stub.c`. `handler_aspen.c` exposes `struct tcmur_handler *aspen_handler_for_test(void)` under `#ifdef ASPEN_TEST`.

Pinned behavior:
- **open** (also used when `reopen` is true):
  - Parse the cfgstring, connect, and send OPEN with `num_lbas * block_size` and `block_size`. Then:
    - `tcmu_dev_set_write_cache_enabled(dev, true)`
    - `tcmu_dev_set_unmap_enabled(dev, true)`
    - `tcmu_dev_set_opt_unmap_gran(dev, blockObjectSize / block_size, false)`
    - `tcmu_dev_set_unmap_gran_align(dev, 0)`
  - Then send LOCK, and start one reader thread per device.
  - Return codes: NotFound → `-ENOENT`, SizeMismatch → `-EINVAL`, any other failure → `-EIO`.
- **I/O callouts** return `TCMU_STS_OK` once the request is queued, and complete it later with `tcmur_cmd_complete`.
- **write:** FUA = `cdb[0]` ∈ {`0x2A`, `0xAA`, `0x8A`} (WRITE_10/12/16) and `cdb[1] & 0x08`. The header and the data iovecs are sent with one `writev` loop under the per-device send mutex, without copying the data.
- **read:** the reader thread copies the reply payload into the command's iovecs (`tcmu_memcpy_into_iovec` from one receive buffer, or `readv` straight into them; builder's choice).
- **unmap:** one call sends one range `(off, len)`, in bytes.
- **reconfig:** `TCMULIB_CFG_DEV_SIZE` sends RESIZE with `cfg->data.dev_size` and waits for the reply. Ok → 0, anything else → `-EINVAL`. Other cfg types → `-EOPNOTSUPP`.
- **close:** send UNLOCK and wait for the reply (or for the connection to be lost), then shut the socket down, join the reader and free.
- **Connection loss** (EOF or error on the reader):
  - Complete every pending I/O with `TCMU_STS_BUSY`, and wake every synchronous waiter with an error.
  - Call `tcmu_notify_conn_lost(dev)` exactly once.
  - Until a reopen, every new I/O callout returns `TCMU_STS_BUSY` synchronously.
- **Status mapping:**
  - Ok → `TCMU_STS_OK`
  - InvalidRequest → `TCMU_STS_INVALID_CDB`
  - OutOfRange → `TCMU_STS_RANGE`
  - Fenced and NotOwner → `TCMU_STS_FENCED`
  - DiskFailed → `TCMU_STS_RD_ERR` for READ, `TCMU_STS_WR_ERR` otherwise
  - anything else → `TCMU_STS_HW_ERR`
- **Pending table:** request ids come from a per-device counter. Pending commands live in a table keyed by id, protected by a mutex.

- [ ] **Step 1: Write `test_handler.c`.**
  - **Fake daemon:** it plays the daemon on a listening Unix socket in a `mkdtemp` directory, decodes requests with `aspen_proto`, and answers from a per-test script.
  - **Stubs:** `tcmu_stub.c` records each `tcmur_cmd_complete` (cmd and rc), counts `tcmu_notify_conn_lost` calls, and serves the cfgstring, LBA count and block size from a fake device struct.
  - **Cases:**
    - "open sends OPEN then LOCK and sets write cache and unmap granularity".
    - "fua bit": a WRITE_16 CDB with `0x08` set sends flag 1, a WRITE_10 without it sends 0, and a FORMAT UNIT CDB (`0x04`) through `write` sends 0.
    - "write payload arrives intact across 3 iovecs".
    - "read reply fills the iovecs and completes OK".
    - "out-of-order replies complete the right commands".
    - "status mapping": a table test.
    - "unmap sends one 16-byte range".
    - "reconfig DEV_SIZE sends RESIZE; error status gives -EINVAL".
    - "disconnect": with 2 commands pending, the fake daemon closes. Both complete BUSY, conn_lost is called exactly once, and the next write returns `TCMU_STS_BUSY`.
    - "close sends UNLOCK".
  - Every wait in the test is bounded at 5 s, so a bug fails instead of hanging.
- [ ] **Step 2: Run `make -C native/tcmu-aspen check`.** Expected: `test_handler` fails to build.
- [ ] **Step 3: Implement `handler_aspen.c`** as pinned above.
- [ ] **Step 4: Run `make -C native/tcmu-aspen check` 20 times in a loop.** Expected: all passed every time.
- [ ] **Step 5: Add `make symcheck`** (`nm -D --undefined-only handler_aspen.so`). Every symbol not provided by glibc or libpthread must be in tcmu-runner's dynamic exports (`nm -D /usr/bin/tcmu-runner`) or in `libtcmu.so.2`'s. Otherwise it fails and lists the missing symbols. Run `make -C native/tcmu-aspen all symcheck`. Expected: it builds with `-Werror`, and symcheck passes.
- [ ] **Step 6: Write `README.md`** covering build (submodule init, `make`), `make check`, install, and the cfgstring format.
- [ ] **Step 7: Commit** with the message "tcmu-aspen: tcmu-runner handler plugin" (including the submodule).

### Task 7: Partial UNMAP reads back zeros (engine)

tcmu-runner 1.5.4 always reports LBPRZ = 1 in READ CAPACITY(16) and VPD B2 when unmap is enabled (`scsi.c`). Linux then sends WRITE ZEROES as WRITE SAME(16) with the UNMAP bit, and tcmu's emulation turns that into an `unmap` call (`tcmur_cmd_handler.c:755`). If the partial pieces of an UNMAP are ignored, a zero-out can leave stale data behind. ext4's lazy inode-table init and journal zeroing rely on zero-out. The spec's UNMAP bullet was updated to match.

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/vdisk/VirtualDisk.scala` (`unmap`)
- Test: `src/test/scala/org/aspen_ddp/aspen/vdisk/VirtualDiskSuite.scala`

**Interfaces:** `BlockDevice.unmap` keeps its signature. Its doc comment gains: "Unmapped bytes read back as zero."

Behavior:
- Whole blocks are deleted as now.
- Each partial piece of a range (from `DiskGeometry.segments` minus the fully covered blocks) is zeroed with the same path a non-FUA `write` uses.
- No write is made for a piece whose block has no pointer, no dirty data and no in-flight commit. It already reads as zero.
- The UNMAP completes once the deletions and zero writes are accepted, under the same rules as today's deletions with respect to FUA and FLUSH.

- [ ] **Step 1: Write failing tests** in `VirtualDiskSuite`:
  - "partial unmap reads back zeros": write the pattern over block 0 (16384 bytes) and unmap `[4096, 8192)`. The read of `[0, 16384)` is the pattern with zeros in `[4096, 8192)`.
  - "partial unmap survives reacquire after flush": the same, then flush, close, reacquire and read.
  - "unmap of a whole block plus partial neighbours": write blocks 0 to 2 and unmap `[8192, 40960)`. Block 1 is deleted (`blockPointer(1) == None` after flush), and the edges of blocks 0 and 2 read as zeros.
  - "partial unmap of an untouched block makes no transaction": on a fresh disk, unmap `[4096, 8192)` and flush. `stats.mapTxCount == 0` and `stats.rangedTxCount == 0`.
- [ ] **Step 2: Run them.** `sbt 'testOnly *VirtualDiskSuite -- -z unmap'`. Expected: the first three FAIL.
- [ ] **Step 3: Implement** the behavior above in `VirtualDisk.unmap`.
- [ ] **Step 4: Run `sbt 'testOnly *vdisk*'`, then `VirtualDiskSuite` 20 times in one sbt session.** Expected: everything passes every time.
- [ ] **Step 5: Commit** with the message "vdisk: partial UNMAP zeroes the covered bytes so unmapped ranges read as zero".

### Task 8: End-to-end setup guide and scripts (run by the user)

The end-to-end path needs root: installing the handler, restarting tcmu-runner, targetcli and iSCSI login. The implementer writes and lint-checks the guide and scripts but does not run them. The user runs them, for example with `! sudo native/tcmu-aspen/e2e/setup.sh`, and reports the results.

**Files:**
- Create: `docs/vdisk-tcmu-setup.md`, `native/tcmu-aspen/e2e/setup.sh`, `native/tcmu-aspen/e2e/teardown.sh`, `native/tcmu-aspen/e2e/smoke.fio`

Content:
- **Prerequisites:** `target_core_user` and `iscsi_target_mod` loaded, and tcmu-runner 1.5.4 active. The pool is created with `create-pool --backend fsif --max-object-size 5120000` over RandomAccessReedSolomon 16/10/12 (spec, "Storage configuration").
- **Steps:**
  1. `aspen create-disk <name> <size> --pool <p>`
  2. `aspen vdisk-server --socket /run/aspen/vdisk.sock`
  3. `sudo make -C native/tcmu-aspen install && sudo systemctl restart tcmu-runner`
  4. Create the backstore in targetcli: `/backstores/user:aspen create name=<name> size=<size> cfgstring=<name>@/run/aspen/vdisk.sock`
  5. Create the iSCSI target, LUN, ACL and portal on 127.0.0.1.
  6. `iscsiadm -m discovery -t st -p 127.0.0.1` and log in, then `mkfs.ext4`, mount, run fio and a kernel build.
- **`setup.sh` and `teardown.sh`:** take `DISK`, `SIZE` and `IQN` variables, and are idempotent (`set -euo pipefail`). Teardown logs out, deletes the target and backstore, and stops nothing else.
- **`smoke.fio`:** a 4 KiB random read/write job with `verify=crc32c` on a file in the mounted fs, a 1 MiB sequential write job, and `fsync=32` on the write jobs.
- **Expected observations,** from the spec's sanity bounds: individual writes well under 100 ms, a FLUSH within a couple of seconds, and no stalls. Restarting `vdisk-server` while mounted pauses I/O and then resumes it, with no ext4 errors in `dmesg`. Unflushed data may be lost.

- [ ] **Step 1: Write the doc and scripts.** Run `bash -n` on each script and `fio --parse-only native/tcmu-aspen/e2e/smoke.fio` (if fio is installed). Expected: no errors.
- [ ] **Step 2: Commit** with the message "docs: vdisk tcmu end-to-end setup".
- [ ] **Step 3: Hand off to the user** with the exact `!` commands to run. Record their results (pass/fail and any `dmesg` errors) in the plan's ledger before the final review.
