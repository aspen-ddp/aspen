# Store Transfer over TLS/TCP Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Move store-transfer bytes off the message-oriented network layer and onto one blocking, mutual-TLS TCP connection per transfer attempt.

**Architecture:** A new `StoreTransferManager` (behind a `StoreTransferService` trait) owns a TLS server socket on the host's `store-transfer-port`. It runs one thread per inbound or outbound transfer, streaming `jar` subprocess output and input over the socket. `Host` keeps all metadata decisions: it starts and retransmits `StartStoreTransfer`, promotes the staged copy, and repoints the pool. It calls the service in place of the old `StoreTransferFactory`. Finally, `StoreTransferData` is removed from the message set.

**Tech Stack:** Scala 3, `javax.net.ssl` (`SSLServerSocket`/`SSLSocket` from `TLSContext.create`), os-lib (`os.spawn`, `os.remove.all`), `jar` CLI, ScalaTest.

**Spec:** `docs/superpowers/specs/2026-10-22-store-transfer-tcp-design.md`

## Global Constraints

- Header: 33 bytes = `StoreId.toBytes` (17) followed by the TransferUUID as two big-endian longs (MSB, then LSB).
- Trailer: 17 bytes = `StoreId.toBytes`. It is written only if `jar --create` exited 0.
- `CHUNK_SIZE` defaults to 1 MiB (`1024 * 1024`).
- I/O timeout defaults to 90 s: the receiver uses the socket read timeout, and the sender uses a write-progress watchdog that closes the socket.
- Archive command: `jar --create --no-manifest .` with cwd set to the source store directory. Extract command: `jar --extract` with cwd set to `<destinationDirectory>/<storeId.directoryName>`. (Verified by hand: plain `jar --create` with no inputs exits 1, so the current code never produced an archive. `--no-manifest` keeps a `META-INF/` directory out of the store.)
- `jar --extract` exits 0 on garbage input and on input truncated at an entry boundary. The trailer is the completeness check, so it must never be skipped or loosened.
- Peers are trusted if their chain is signed by the system CA (`AspenTrustManager`). Servers require client auth. Clients do no endpoint or hostname identification.
- The message keeps the name `StartStoreTransfer`. Its existing `transferUUID` field is the per-attempt UUID.
- Project style: Scala 3 quiet syntax, indented if/then/else, os-lib for filesystem work, comment density in line with `Host.scala`.
- Run tests with `sbt 'testOnly *SuiteName'` from the repo root.

## Review Focus

1. **Stream truncated exactly at a jar entry boundary, or garbage bytes followed by a valid trailer.** Truncation must fail on the missing trailer. Garbage with a trailer is accepted by `jar` and yields an empty directory, which is acceptable because only the sender can produce a trailer. Pinned by `truncated stream without trailer fails and cleans up` (Task 1) and `failed jar create sends no trailer` (Task 2).
2. **Retransmitted `StartStoreTransfer` arriving after the send already finished.** It must not start a second archive. Pinned by `same or older timestamp is ignored after completion` (Task 2).
3. **A second connection presenting a UUID that already connected.** It must be rejected and must not disturb the transfer in progress. Pinned by `reused UUID is rejected while first transfer proceeds` (Task 1).
4. **Peer that stops reading or writing mid-stream.** Each side must give up after the I/O timeout. Pinned by `receiver times out on stalled sender` (Task 1) and `sender times out on stalled receiver` (Task 2).
5. **Retry after a failed transfer in.** The retry must use a new UUID and timestamp, and it must stop retransmitting the old message. Pinned by `failed transfer in restarts with a fresh UUID` (Task 3).

---

### Task 1: `StoreTransferService` trait and the receiving half of `StoreTransferManager`

**Files:**
- Modify (rewrite): `src/main/scala/org/aspen_ddp/aspen/server/transfer/StoreTransfer.scala`
- Create: `src/main/scala/org/aspen_ddp/aspen/server/transfer/StoreTransferManager.scala`
- Test: `src/test/scala/org/aspen_ddp/aspen/server/transfer/StoreTransferManagerSuite.scala`

Leave `TransferringIn.scala`, `TransferringOut.scala` and `StoreTransferFactory` in place for now. Move the factory's trait and object to the bottom of `StoreTransfer.scala` unchanged, because `Host` still compiles against them until Task 3. Point `TransferringIn` and `TransferringOut` at the new constants in `object StoreTransfer`.

**Interfaces:**
- Produces:
  ```scala
  object StoreTransfer:
    val TransferOutMarkerFile: String = "transferring-out"   // was TransferringOut.MarkerFile
    val StagingDirectory: String = "transferring-in"         // was TransferringIn.TransferDirectory

  trait StoreTransferService:
    def beginTransferIn(storeId: StoreId, transferUUID: UUID, destinationDirectory: os.Path): Future[Unit]
    def beginTransferOut(storeId: StoreId, transferUUID: UUID, sourceDirectory: os.Path,
                         destHost: String, destPort: Int, timestamp: HLCTimestamp): Unit
    def isTransferringOut(storeId: StoreId): Boolean
    def transferringOutStores: Set[StoreId]

  object StoreTransferService:
    /** For hosts built without a transfer port (TestNetwork, harness defaults). beginTransferIn
     *  returns a Future that never completes; beginTransferOut logs a warning and does nothing. */
    object Disabled extends StoreTransferService

  class StoreTransferManager(port: Int,
                             tls: TlsCredentials,
                             chunkSize: Int = StoreTransferManager.DefaultChunkSize,
                             ioTimeout: FiniteDuration = StoreTransferManager.DefaultIoTimeout)
      extends StoreTransferService with Logging:
    def boundPort: Int        // actual port (tests pass 0)
    def close(): Unit         // tests only: closes the server socket and aborts live transfers

  object StoreTransferManager:
    val DefaultChunkSize: Int = 1024 * 1024
    val DefaultIoTimeout: FiniteDuration = 90.seconds
    val HeaderSize: Int = 33
    val TrailerSize: Int = 17
    def encodeHeader(storeId: StoreId, transferUUID: UUID): Array[Byte]
  ```
  `TlsCredentials` lives in `common/network/implementations/quic/QuicTls.scala`.

- [ ] **Step 1: Write the failing tests**

Suite helpers:
- Credentials come from `TestTLS.ca`, as in `QuicTlsSuite.credsFrom`.
- `rawSender(port, creds)` returns a connected client `SSLSocket` built from `TLSContext.create(...)`.
- `mkTree(dir)` writes `a` (3 MiB random), `sub/b` ("hello") and an empty `sub/empty/`.
- `jarBytes(dir)` returns the stdout of `jar --create --no-manifest .`.
- Each manager is built with `port = 0` and `ioTimeout = 1.second`, and closed in `afterEach`.

Tests and their assertions:
- `beginTransferIn completes when header, archive and trailer arrive`: the future succeeds within 10 s, and `dest / storeId.directoryName` contains `a` (byte-equal), `sub/b`, `sub/empty/`, and no `META-INF`.
- `existing destination store directory is replaced`: pre-create `dest/<dirName>/stale`; after success, `stale` is gone.
- `truncated stream without trailer fails and cleans up`: send the header and the full archive, then close. The future fails, and `dest/<dirName>` does not exist.
- `wrong trailer fails`: the trailer is a different `StoreId`. The future fails and the directory is removed.
- `extractor failure fails the transfer`: send the header, the archive cut at half its length, and the correct trailer. The future fails, because jar exits 1 on a mid-entry cut.
- `unknown StoreId is dropped`: a header for an unregistered store gets a closed connection (`read()` returns -1 or throws `SSLException`/`SocketException`), and a separately registered transfer still succeeds afterwards.
- `stale UUID is rejected and the current one succeeds`: register `(sid, u2)`. A connection with `(sid, u1)` is closed, then a connection with `(sid, u2)` completes the future.
- `reused UUID is rejected while first transfer proceeds`: the first connection sends the header and half the archive. A second connection with the same header is closed. The first then finishes and the future succeeds.
- `receiver times out on stalled sender`: send the header and 1 KiB, then hold the socket open. The future fails within 5 s and the directory is removed.
- `re-registering a store fails the previous future`: `beginTransferIn(sid, u1, …)` then `beginTransferIn(sid, u2, …)`. The first future fails.

- [ ] **Step 2: Run tests to verify they fail**

Run: `sbt 'testOnly *StoreTransferManagerSuite'`
Expected: compilation failure (`StoreTransferManager` not found).

- [ ] **Step 3: Implement `StoreTransfer.scala` and the receive side of `StoreTransferManager.scala`**

- Server socket: create with `TLSContext.create(tls.caCertificate, tls.certificate, tls.privateKey).getServerSocketFactory`, set `setNeedClientAuth(true)`, and bind to `port`. A daemon accept thread hands each socket to a new daemon thread named `store-transfer-in-<remote>`.
- Registrations: `Map[StoreId, Registration(transferUUID, destinationDirectory, promise, var activeSocket: Option[SSLSocket], var extractor: Option[os.SubProcess])]`, guarded by `synchronized`.
- Connection thread: call `setSoTimeout(ioTimeout.toMillis)`, then read exactly 33 bytes. The header must match a registration with no `activeSocket`; otherwise log at warn and close. On a match, record the socket. Delete and recreate `<dest>/<dirName>`, then spawn `jar --extract` there.
- Hold-back read loop. The algorithm, since the tests don't determine it:
  ```
  buf = new Array[Byte](chunkSize + TrailerSize); held = 0
  loop:
    n = in.read(buf, held, buf.length - held); if n == -1 then break
    held += n
    if held > TrailerSize then
      extractor.stdin.write(buf, 0, held - TrailerSize)
      System.arraycopy(buf, held - TrailerSize, buf, 0, TrailerSize); held = TrailerSize
  at EOF: success path only if held == TrailerSize && buf.take(17) sameElements storeId.toBytes
  ```
- Success: close stdin, `waitFor()`, exit 0 completes the promise, then remove the registration.
- Any failure (exception, timeout, bad trailer, non-zero exit): destroy the extractor, run `os.remove.all` on the store directory, fail the promise with a message naming the store and the cause, and remove the registration.
- `beginTransferIn` on a store that is already registered fails the old promise and aborts its socket and extractor first.
- Leave the `beginTransferOut`, `isTransferringOut` and `transferringOutStores` bodies as `???` until Task 2.

- [ ] **Step 4: Run tests to verify they pass**

Run: `sbt 'testOnly *StoreTransferManagerSuite'`
Expected: all Task 1 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/server/transfer src/test/scala/org/aspen_ddp/aspen/server/transfer
git commit -m "StoreTransferManager: TLS listener and transfer-in over blocking TCP"
```

---

### Task 2: Sending half of `StoreTransferManager`

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/server/transfer/StoreTransferManager.scala`
- Test: `src/test/scala/org/aspen_ddp/aspen/server/transfer/StoreTransferManagerSuite.scala`

**Interfaces:**
- Consumes: Task 1's `StoreTransferManager`, `encodeHeader`, constants.
- Produces: working `beginTransferOut`, `isTransferringOut` and `transferringOutStores` (signatures in Task 1).

- [ ] **Step 1: Write the failing tests**

- `round trip between two managers`: build managers A and B. B runs `beginTransferIn(sid, u, destB)`. A runs `beginTransferOut(sid, u, srcTree, "127.0.0.1", B.boundPort, HLCTimestamp.now)`. B's future succeeds, the trees match, and `A.isTransferringOut(sid)` becomes false within 5 s.
- `isTransferringOut is true while sending`: send to a raw TLS server (a test helper that accepts, records headers, and never reads). `isTransferringOut(sid)` is true and `transferringOutStores == Set(sid)`.
- `sender times out on stalled receiver`: the source has 32 MiB of random data and the server never reads. `isTransferringOut(sid)` becomes false within 15 s.
- `newer timestamp supersedes the active attempt`: against the recording server, call `beginTransferOut(sid, u1, ts1)` and then `beginTransferOut(sid, u2, ts2 > ts1)`. The server sees headers for `u1` and then `u2`, and the first server-side socket reaches EOF or error.
- `same or older timestamp is ignored after completion`: complete a round trip with `ts1`. Then call `beginTransferOut(sid, u1, ts1)` and `beginTransferOut(sid, u0, olderTs)` at a recording server. The server records no connections within 2 s.
- `same timestamp is ignored while active`: the recording server sees exactly one header.
- `failed jar create sends no trailer`: the source directory does not exist, or contains an unreadable file (`chmod 000`), so `jar` exits non-zero. The receiver's future fails.
- `unreachable destination`: target a port with no listener. `isTransferringOut` returns to false and no exception escapes `beginTransferOut`.

- [ ] **Step 2: Run tests to verify they fail**

Run: `sbt 'testOnly *StoreTransferManagerSuite'`
Expected: Task 2 tests FAIL with `NotImplementedError`.

- [ ] **Step 3: Implement the send side**

- State:
  - `active: Map[StoreId, Outbound(timestamp, socket, process, thread)]`
  - `lastTimestamp: Map[StoreId, HLCTimestamp]`, which is kept after an attempt ends.
- `beginTransferOut` ignores the call if `lastTimestamp(sid) >= timestamp`. Otherwise it records the new timestamp, aborts any active attempt for the store (destroy the process, close the socket), and starts a daemon thread named `store-transfer-out-<sid>`.
- Thread:
  1. Connect with the client `SSLSocketFactory` and call `startHandshake()`.
  2. Write the header.
  3. Spawn `jar --create --no-manifest .` (cwd = sourceDirectory).
  4. Copy stdout to the socket in `chunkSize` reads.
  5. At EOF, `waitFor()`. Write the trailer only if the exit code is 0.
  6. Flush and close.
  - Every outcome is logged (info on success, warn on failure). The `finally` block destroys the process and removes the store's `active` entry only if it is still this attempt's entry.
- Watchdog: one daemon `ScheduledExecutorService` per manager ticks every `ioTimeout / 4`. It closes any outbound socket whose last completed write, or whose connect start if nothing has been written yet, is older than `ioTimeout`. Each attempt keeps an `@volatile var lastProgress: Long` (nanoTime), updated after every `write`.

- [ ] **Step 4: Run tests to verify they pass**

Run: `sbt 'testOnly *StoreTransferManagerSuite'`
Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/server/transfer src/test/scala/org/aspen_ddp/aspen/server/transfer
git commit -m "StoreTransferManager: transfer-out with send watchdog and timestamp supersession"
```

---

### Task 3: Wire `Host` and `Main` to the transfer service; remove the old transfer classes

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/server/Host.scala` (constructor ~125; state 161-165; retransmit task 176-183; `tryLoadStore` marker check ~364; `startStoreTransferIn` 598-664; `startStoreTransferOut` 666-741; delete `transferDataReceived` 743-748; abandoned-transfer guard ~1136-1140; marker removal ~1211; store listings ~1470-1478; dispatch ~1646)
- Modify: `src/main/scala/org/aspen_ddp/aspen/cmdline/Main.scala` (~1250-1286)
- Delete: `src/main/scala/org/aspen_ddp/aspen/server/transfer/TransferringIn.scala`, `TransferringOut.scala`; remove `StoreTransferFactory`, `StoreTransferIn` and `StoreTransferOut` from `StoreTransfer.scala`
- Modify tests: `src/test/scala/org/aspen_ddp/aspen/server/HostTestHarness.scala` (138-207), `src/test/scala/org/aspen_ddp/aspen/server/HostDeviceDiscoverySuite.scala` (`StagedTransferFactory` 21-62, uses at 754, 833)
- Test: `src/test/scala/org/aspen_ddp/aspen/server/HostDeviceDiscoverySuite.scala`

**Interfaces:**
- Consumes: `StoreTransferService`, `StoreTransferService.Disabled`, `StoreTransfer.TransferOutMarkerFile`, `StoreTransfer.StagingDirectory`, `StoreTransferManager(port, tls)`.
- Produces: the `Host` constructor parameter `val storeTransferService: StoreTransferService = StoreTransferService.Disabled`, which replaces `storeTransferFactory`. The harness parameter `transferService: StoreTransferService = StoreTransferService.Disabled` replaces `transferFactory`.

- [ ] **Step 1: Convert the test seam and write the failing tests**

In `HostDeviceDiscoverySuite`, `StagedTransferFactory` becomes `StagedTransferService extends StoreTransferService`:
- It records `beginTransferIn(storeId, uuid, dir)` calls in order, as `transfersIn: Vector[(StoreId, UUID, os.Path)]`, each with its own `Promise[Unit]`.
- `finishTransferIn(storeId)` completes the latest promise for that store. `failTransferIn(storeId)` fails it.
- `beginTransferOut` throws `UnsupportedOperationException`, as before.
- `finishTransferIn` must also create `<dir>/<storeId.directoryName>` containing the marker file, standing in for what extraction would have produced, so that Host's promotion step has something to move.

Update the two existing call sites (754, 833) to `transferService = transfers`. They keep their current assertions.

New tests:
- `transfer in stages under the transferring-in directory`: `beginTransferIn` receives `devicePath / "transferring-in"`.
- `successful transfer in promotes the staged store and drops the marker`: after `finishTransferIn`, `devicePath/<dirName>` exists, `transferring-out` is absent from it, and `devicePath/transferring-in/<dirName>` is gone.
- `StartStoreTransfer carries the UUID registered with the service`: the captured `StartStoreTransfer.transferUUID` equals the UUID passed to `beginTransferIn`.
- `failed transfer in restarts with a fresh UUID`: after `failTransferIn`, a second `beginTransferIn` arrives with a different UUID. The newest captured `StartStoreTransfer` has that UUID and a timestamp later than the first.
- `retransmit stops once the transfer in completes`: after `finishTransferIn`, no further `StartStoreTransfer` for that store is sent when the retransmit task runs. Expose the retransmit step to tests as `private[aspen] def retransmitPendingStartTransfers(force: Boolean = false): Unit`. With `force = true` it ignores the 30 s age check, so the test does not have to wait.

- [ ] **Step 2: Run tests to verify they fail**

Run: `sbt 'testOnly *HostDeviceDiscoverySuite'`
Expected: compilation failure (`StoreTransferService` parameter not on `Host`).

- [ ] **Step 3: Change `Host`**

- State:
  - `transferringInUUIDs` and `transferringInStoreIds` become `transferringIn: Map[StoreId, UUID]`.
  - `transferringOut` is deleted. Every read of it becomes `storeTransferService.isTransferringOut(id)` or `storeTransferService.transferringOutStores`.
  - `pendingStartTransfers` stays.
- `startStoreTransferIn`:
  - Skip if `transferringIn` contains the store.
  - If `devicePath/<dirName>` already exists, log an error and return. This is the guard `TransferringIn`'s constructor used to apply.
  - Mint the UUID and the timestamp, then call `beginTransferIn(storeId, uuid, os.Path(devicePath) / StoreTransfer.StagingDirectory)`.
  - Record the pending `StartStoreTransfer` (with that UUID and timestamp) and send it.
  - On completion, always remove both the `transferringIn` and `pendingStartTransfers` entries for the store.
    - Success: remove `TransferOutMarkerFile` from the staged copy if it is present, `os.move` the staged copy to `devicePath/<dirName>`, then run the existing `updateStateForTransferredStore` / load / discard logic unchanged.
    - Failure: recurse as today.
- `startStoreTransferOut`: keep the validation for-comprehension, and add `toHostState <- client.getHostState(toDevice.hostId)`. In the `yield`:
  - Replace `transferringOut.contains` checks with `storeTransferService.isTransferringOut`.
  - After `closeStore`, write the marker file to `sourceDs.devicePath/<dirName>/transferring-out`, then call `beginTransferOut(m.storeId, m.transferUUID, os.Path(sourceDs.devicePath) / m.storeId.directoryName, toHostState.address, toHostState.storeTransferPort, m.timestamp)`.
  - Delete the trailing supersession `match`, because the service now applies the timestamp rule.
  - Errors in the for-comprehension (including a missing `HostState`) are logged and dropped as they are today.
  - The spec's "immediate scan" is already satisfied, because this path reads pool and device state fresh and closes the store itself. Add nothing beyond a one-line comment saying so.
- Delete `transferDataReceived` and the `StoreTransferData` dispatch case.
- Replace `TransferringOut.MarkerFile` with `StoreTransfer.TransferOutMarkerFile` everywhere.

- [ ] **Step 4: Change `Main` and the harness**

- `Main` (~1250-1286): build `new StoreTransferManager(hostCfg.storeTransferPort, tls)` and pass it as `storeTransferService`.
- `HostTestHarness`: rename the parameter, thread it through to `RecordingHost`, and default it to `StoreTransferService.Disabled`.
- Delete `TransferringIn.scala`, `TransferringOut.scala` and the factory, in/out traits from `StoreTransfer.scala`.

- [ ] **Step 5: Run tests to verify they pass**

Run: `sbt 'testOnly *HostDeviceDiscoverySuite *HostManagerSuite *FailedStorageDeviceSuite *DeviceCheckNudgeSuite *TombstoneGuardSuite *StoreTransferManagerSuite'`
Expected: all PASS.

- [ ] **Step 6: Commit**

```bash
git add -A src/main/scala/org/aspen_ddp/aspen/server src/main/scala/org/aspen_ddp/aspen/cmdline/Main.scala src/test/scala/org/aspen_ddp/aspen/server
git commit -m "Host: transfer stores through StoreTransferService over TLS/TCP"
```

---

### Task 4: Remove `StoreTransferData`

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/common/network/Message.scala:43-48`
- Modify: `src/main/protobuf/codec.proto` (message at ~389; oneof field 22 at ~441)
- Modify: `src/main/scala/org/aspen_ddp/aspen/common/network/Codec.scala:922-935`
- Modify: `src/main/scala/org/aspen_ddp/aspen/common/network/implementations/quic/QuicMessageCodec.scala` (116, 144-145, 252-256)
- Modify: `src/main/scala/org/aspen_ddp/aspen/common/network/implementations/zmqnet/ProtobufMessageCodec.scala` (117, 250-252)
- Modify tests: `CodecRoundTripSuite.scala:473`, `quic/QuicTlsSuite.scala:122,199`, `quic/QuicNetFlushSuite.scala:54`, `quic/StreamFrameDecoderSuite.scala:114`, `quic/QuicMessageCodecSuite.scala:84,152-189`

**Interfaces:**
- Consumes: nothing new.
- Produces: none. After this task, `grep -rn StoreTransferData src` returns nothing.

- [ ] **Step 1: Delete the message and its codec paths**

In `codec.proto`, delete `message StoreTransferData` and its oneof entry, and add `reserved 22;` to the oneof's enclosing message so the field number is never reused.

- [ ] **Step 2: Retarget the tests that used it as a convenient one-section message**

Replace each use with `OpportunisticRebuild`, which has the same single out-of-band `data` section. Keep each test's payload sizes and assertions (section handling, flush and large-message behaviour, round-trip equality). Where a QUIC test needs host-addressed routing rather than store-addressed routing, address the peer the way that suite's other store-routed messages do. Delete the `CodecRoundTripSuite` `StoreTransferData round-trip` test outright.

- [ ] **Step 3: Run the full test suite**

Run: `sbt test`
Expected: all PASS, and `grep -rn StoreTransferData src` prints nothing.

- [ ] **Step 4: Commit**

```bash
git add -A src
git commit -m "Remove StoreTransferData; store bytes no longer travel as messages"
```
