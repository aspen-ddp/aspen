# Device I/O isolation — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A hung storage device stalls only its own stores. The host keeps serving the other devices' stores, keeps
heartbeating, and keeps managing its other devices.

**Architecture:** Each device path gets a `DeviceIo`: one management lane (a single FIFO thread) for control-issued
disk jobs, a data pool for backend I/O, and an FSIF serial pool. Every task those threads run passes through a
`DeviceIoObserver`. The control thread keeps every decision and does no disk I/O. A control sequence that needs the
disk follows four steps: decide, mark the copy busy, submit one job, then apply the result in a control event after
rechecking that the device and copy are still current. Backends, rebuild staging and the three store-thread sites move
onto the device's threads. Global locks become per device. An enforcement suite ratchets the remaining disk calls in
control code down to zero.

**Tech Stack:** Scala 3, ScalaTest `AnyFunSuite`, sbt, os-lib, RocksJava 8.11.3.

**Spec:** `docs/superpowers/specs/2026-11-23-device-io-isolation-design.md`. Section numbers (§) below refer to it.

## Global Constraints

- Work on the current branch (`device-threads`). No worktree. Never use `git stash`.
- The control thread performs no device I/O (§2). The only exception is code that runs before `threading.start()`
  and posts the same jobs. New control code never calls `os.*` filesystem operations, `DurableFiles`, `FileStat`,
  `DeviceMarkers`, `CrashRecoveryLog.saveStoreState`/`loadValidStoreState`, or `RebuildState` I/O directly.
- Job bodies live in `*Disk` objects (§9 enforcement). Their methods take plain values (paths, ids, configs), return
  plain results, and touch no control or store state. Call sites read `io.submit(kind, store)(XDisk.op(args))`.
- A job touches exactly one device. No control step joins Futures across devices (§2).
- Every apply step rechecks before acting: `ctx.devices.isLoaded(sds)` (or that the path's `DeviceIo` is still the one
  submitted to), and that the copy record or attempt id is the one the job was for. On a mismatch it clears the busy
  mark and does nothing else. On `DeviceIo.Cancelled` it clears the mark and does nothing.
- Never treat a busy copy as absent (§7a). A rule that needs a busy copy's disk waits.
- No timeouts on device I/O, and no fail-stop or lost-store declaration caused by slowness (spec anti-goals).
- Single-owner model stays (`HostThreading` rules 1, 3 and 4). No new locks shared across devices, and no lock that a
  non-device thread can take is ever held across device I/O (§5).
- Scala 3 quiet syntax, indented if/then/else. Every existence test goes through `server.FileStat`, and
  `FileStatUsageSuite` keeps passing. Use os-lib for file work.
- Test priorities: correctness, then reliability (no flaky tests), then speed. New control tests are deterministic
  (`LockstepThreading`, `ControlFixture`). Threaded tests assert outcomes only and wait on conditions, never sleeps.
- Full suite: run `sbt Test/compile` first, then `sbt test` with an 80 s Bash timeout. A task is done only when the
  full suite passes.
- When a subsystem's behaviour changes, update its module CLAUDE.md in the same task.
- Defaults: data pool threads `--device-io-threads` = 4; FSIF serial threads per device = 1; RocksDB
  `setMaxBackgroundJobs(2)` per database with at most 1 flush; read bound `--device-read-bound` = 4096 outstanding
  reads per device (§6, §8). The builder may change the thread defaults only on measurement evidence (Task 17).

## Decisions

- **P1. Where `DeviceIo`s live.** `HostThreading` gains `def newDeviceIo(path: os.Path): DeviceIo`, so lockstep,
  single-thread and production each supply their own. `DeviceControl` gains `val deviceIos: DeviceIos`, a
  control-owned registry from path to `DeviceIo`. `LocalStorageDeviceState` gains `val io: DeviceIo`. The host-root
  lane is `deviceIos.hostRoot`, a `DeviceIo` for `ctx.rootDir`.
- **P2. Lockstep jobs run on the FIFO.** `LockstepDeviceIo.submit` enqueues the job on the threading's FIFO, so its
  result arrives as a later event. `dataEc` and `serialEc` enqueue on the same FIFO. Test hooks:
  `hold(path)`/`release(path)` stop and resume a path's jobs (held jobs stay in a side queue in order).
  `held(path): List[JobKind]` lists them.
- **P3. Enforcement is a ratchet.** `DeviceIoUsageSuite` starts with a per-file baseline of today's violation counts.
  Each conversion task lowers its files' counts. Task 16 asserts the baseline is empty. A count that rises fails the
  suite.
- **P4. Busy copies.** `Activity.DiskJob(kind: JobKind, prior: Activity)` and `ActivityKind.DiskJob`. `decide`
  returns `Nil` for a copy in `DiskJob`, so no action fires for it, `ForgetGoneCopy` included. Device-level steps skip a device whose
  periodic job of that kind is outstanding (coalescing, §2).
- **P5. Read bound location.** `Backend.readsOverBound: Boolean` (default `false`, thread-safe). FSIF and RocksDB
  delegate to their `DeviceIo.readsOverBound`. `Frontend` checks it only when a `NetworkRead` arrives, before it
  creates any state (§6).

## Review Focus

1. A device unloaded, shut down or re-adopted while one of its jobs is in flight. The apply step must change nothing
   for the stale device and leave the new one consistent. Owners: Tasks 9-15. Each adds a "job completes after
   unload" lockstep test.
2. Two devices, one held, and a store with a copy on each (a transfer or rebuild from the hung device to the healthy
   one). The healthy copy must come Active; the hung copy's release waits and never runs early. Owner: Task 16.
3. The startup scan runs with a device that hangs on its first stat. The host must still start, heartbeat, and load
   the other devices. Owner: Task 10 (lockstep hold before `start`), Task 16 (threaded).
4. A cancelled transfer-in whose removal job is queued behind a hung job. It must not queue a second removal, and
   cancel-before-start must leave no staging state recorded as removed. Owner: Task 15.
5. A long hang under client read retries. Memory must stay bounded: reads beyond the bound get no answer, and reads
   below it are answered once the device returns. Owner: Task 8 (unit) and Task 16 (threaded backlog bound).

---

### Task 1: `DeviceIo` and its threaded implementation

**Files:**
- Create: `server/host/DeviceIo.scala` (trait, `JobKind`, `JobHandle`, `Cancelled`, `DeviceIoObserver`)
- Create: `server/host/ThreadedDeviceIo.scala`
- Test: `src/test/scala/org/aspen_ddp/aspen/server/host/ThreadedDeviceIoSuite.scala`

Paths are relative to `src/main/scala/org/aspen_ddp/aspen/` unless they start with `src/`.

**Interfaces:**
- Produces:
  - `enum JobKind`: `Scan, ReadConfig, Markers, CopyLayout, Move, Retire, Release, Delete, Sweep, SaveFile, CrlOpen,
    CrlCreate, BackendOpen, BackendCreate, Usage, Staging, ReconcileInputs, Other(name: String)`
  - `trait JobHandle[T] { def future: Future[T]; def cancel(): Boolean }`. `cancel` returns true only if the job had
    not started. The future then fails with `DeviceIo.Cancelled`.
  - `trait DeviceIo`:
    `def path: os.Path; def label: String; def setDeviceId(id: StorageDeviceId): Unit;`
    `def submit[T](kind: JobKind, store: Option[StoreId])(job: => T): JobHandle[T];`
    `def dataEc: ExecutionContext; def serialEc: ExecutionContext; def observer: DeviceIoObserver;`
    `def beginRead(): Unit; def endRead(): Unit; def readsOverBound: Boolean; def close(): Unit`
  - `object DeviceIo { object Cancelled extends Exception; final case class Config(dataThreads: Int = 4,
    serialThreads: Int = 1, readBound: Int = 4096) }`
  - `final class DeviceIoObserver`: `started(kind: String, store: Option[StoreId], bytes: Long): Long` (returns a
    token); `finished(token: Long, ok: Boolean): Unit`; `inFlight: Map[String, (Int, Long)]` (count and oldest start,
    nanoTime); `completed: Map[String, Long]`. It is thread-safe.
  - `class ThreadedDeviceIo(path: os.Path, cfg: DeviceIo.Config, gate: () => Unit = () => ())`. `gate` runs before
    every task on every thread of this `DeviceIo`. It is the fault seam for hang tests: a test passes a latch await.
- Futures returned by `submit` complete on the lane thread. Callers name `ctx.threading.control` in their callbacks,
  as today. Threads are daemon threads named `dev-<label>-mgmt`, `dev-<label>-io-N` and `dev-<label>-serial-N`. The
  label is the path basename, plus `-<id>` once `setDeviceId` has been called (it renames live threads).
  `dataEc`/`serialEc` tasks are observed under kind names `data` and `serial`.

- [ ] **Step 1: Write failing tests** in `ThreadedDeviceIoSuite`:
  - `"jobs run in submission order on one lane thread"`: submit 100 jobs appending to a list; the list is `0 until 100`
    and every job saw the same `Thread.currentThread`, whose name starts with `dev-`.
  - `"cancel before start skips the job"`: block the lane with a latch job, then submit job B and cancel it.
    `cancel()` is true, B's future fails with `DeviceIo.Cancelled`, and B's body never runs after the latch opens.
    Cancelling a running job returns false.
  - `"a hung DeviceIo does not delay another"`: A has `gate = () => latch.await()`; B is ungated. 50 jobs and 50
    `dataEc` tasks on B complete while A's first job is blocked.
  - `"observer sees start and finish"`: while a job is blocked, `inFlight("Scan")._1 == 1`; after release,
    `completed("Scan") == 1` and `inFlight` has no `Scan`.
  - `"read bound"`: with `readBound = 2`, after two `beginRead()`, `readsOverBound` is true; after one `endRead()`, it
    is false.
- [ ] **Step 2:** `sbt 'testOnly *ThreadedDeviceIoSuite'`. Expected: compile failure (types missing).
- [ ] **Step 3:** Implement `DeviceIo.scala` and `ThreadedDeviceIo.scala`. The lane is a single-thread executor over
  a `LinkedBlockingQueue` of cancellable entries (an `AtomicInteger` state per entry: queued, running, cancelled).
  `close()` shuts the executors down without awaiting them.
- [ ] **Step 4:** Run the suite. Expected: PASS.
- [ ] **Step 5:** Commit `Add DeviceIo: per-device lane, data and serial pools, observer`.

### Task 2: Threading support, the registry, and the enforcement ratchet

**Files:**
- Modify: `server/host/HostThreading.scala` (add `newDeviceIo`; rewrite rule 2 of the scaladoc to "the control
  thread performs no device I/O")
- Modify: `server/host/StoreThreadPool.scala`, `server/host/SingleThreadThreading.scala` (return a
  `ThreadedDeviceIo` built from a `DeviceIo.Config` constructor parameter), `server/host/LockstepThreading.scala`
- Create: `server/host/LockstepDeviceIo.scala`, `server/host/DeviceIos.scala`
- Modify: `server/host/DeviceControl.scala` (`val deviceIos: DeviceIos`), `server/host/LocalDevices.scala`
  (`LocalStorageDeviceState` gains `val io: DeviceIo`), `server/host/DeviceDiscovery.scala:805` (pass
  `ctx.deviceIos.forPath(storageDevicePath)`), `src/test/.../server/host/ControlFixture.scala` (helpers)
- Create: `src/test/scala/org/aspen_ddp/aspen/server/DeviceIoUsageSuite.scala`
- Test: `src/test/.../server/host/LockstepThreadingSuite.scala`

**Interfaces:**
- Produces:
  - `HostThreading.newDeviceIo(path: os.Path): DeviceIo`
  - `class DeviceIos(threading: HostThreading, rootDir: os.Path)`: `forPath(path: os.Path): DeviceIo` (creates on
    first sight, else reuses, §7); `get(path): Option[DeviceIo]`; `forget(path: os.Path): Unit` (closes it, only once
    its observer shows nothing in flight; otherwise it keeps the entry); `hostRoot: DeviceIo`; `all: Map[os.Path,
    DeviceIo]`. It is control-owned.
  - `LockstepThreading`: `hold(path: os.Path): Unit`, `release(path: os.Path): Unit`, `held(path: os.Path):
    List[JobKind]`
  - `ControlFixture`: `holdDevice(host: Host, path: os.Path)`, `releaseDevice(host, path)`, `heldJobs(host, path)`
  - `DeviceIoUsageSuite` with a `baseline: Map[String, Int]` (relative file path to the allowed count of forbidden
    calls), built from today's counts.

- [ ] **Step 1: Write failing tests.**
  - `LockstepThreadingSuite`: `"device jobs run on the FIFO in order"`. Submit two jobs on one path. Neither runs
    before `drain()`; after it, both ran in order and their futures completed.
  - `"a held path keeps its jobs until released"`. After `hold(p)`, submit a job and `drain()`. It has not run, and
    `held(p) == List(JobKind.Scan)`. Another path's job did run. After `release(p)` and `drain()`, it has run.
  - `DeviceIoUsageSuite`: `"no device I/O in control code beyond the baseline"`. It scans `server/host`,
    `server/rebuild` and `server/transfer` with the forbidden regex
    `\bos\.(remove|move|list|read|write|makeDir|copy|walk|size|stat|exists|isDir|isFile|perms)\b|DurableFiles\.|FileStat\.|DeviceMarkers\.|saveStoreState|loadValidStoreState|RebuildState\.(load|save|remove)|getFreeSpace|getTotalSpace|getUsableSpace`.
    It skips comment lines as `FileStatUsageSuite` does. Allowed files are any `*Disk.scala`, `DeviceIo.scala`,
    `ThreadedDeviceIo.scala` and `LockstepDeviceIo.scala`. Per file, the count must be `<= baseline(file)`. A second
    test, `"baseline has no slack"`, requires the count to equal the baseline, so each task must lower it as it
    converts.
- [ ] **Step 2:** Run both suites. Expected: compile failure.
- [ ] **Step 3: Implement.** `LockstepDeviceIo` enqueues on the `LockstepThreading` queue, or on its held side queue.
  Its `dataEc`/`serialEc` enqueue too. `Host.shutdown` closes every `DeviceIo` in `deviceIos.all` without waiting.
  Build the baseline by running the scan once and pasting the counts into the
  suite as a literal map. Thread `deviceIos` through `Host` → `DeviceControl`.
- [ ] **Step 4:** Run the full suite. Expected: PASS. Nothing uses the lanes yet.
- [ ] **Step 5:** Update `server/host/CLAUDE.md` "Host threading model" (DeviceIo, DeviceIos, the lockstep hold
  hooks, the ratchet). Commit `Add DeviceIos registry, lockstep device lanes and the device I/O usage ratchet`.

### Task 3: `CopyDecision` busy activity

**Files:**
- Modify: `server/host/CopyRegistry.scala` (`Activity.DiskJob(kind: JobKind, prior: Activity)`), `server/host/CopyDecision.scala`
  (`ActivityKind.DiskJob`; `decide` yields `Nil` for it), `server/host/HostCopies.scala` (`diskJob(sds, storeId,
  kind): Boolean` claim, `diskJobEnded(sds, storeId): Unit` fact). The new activity state is legal from `Idle`,
  `Served` and `AwaitingRetry`, and returns to the activity it came from.
- Test: `src/test/.../server/host/CopyDecisionSuite.scala`, `CopyRegistrySuite.scala`

**Interfaces:**
- Produces: `HostCopies.diskJob(sds: LocalStorageDeviceState, storeId: StoreId, kind: JobKind): Boolean` (false if
  the copy is already busy or the transition is illegal) and `HostCopies.diskJobEnded(sds, storeId): Unit`.
  `ActivityKind.DiskJob`.

- [ ] **Step 1: Write failing tests.** `CopyDecisionSuite`: extend the enumeration over every input so that each
  `CopyState` with `activity = ActivityKind.DiskJob` gives `decide(...) == Nil`. `CopyRegistrySuite`:
  `"a disk job returns the copy to its prior activity"` (Served → DiskJob → Served) and `"a second disk job on a busy
  copy is refused"`.
- [ ] **Step 2:** Run both. Expected: FAIL.
- [ ] **Step 3:** Implement. `ActivityKind.of` maps it. `diskJobEnded` restores `prior`.
- [ ] **Step 4:** Run the full suite. Expected: PASS.
- [ ] **Step 5:** Commit `Copy activity DiskJob: decide holds off a copy with a disk job in flight`.

### Task 4: Backend opens on the device lane; FSIF on per-device pools

**Files:**
- Modify: `server/store/backend/BackendOpener.scala`: `create(storeCfg, storePath, creatingRoot, io: DeviceIo)` and
  `open(storeCfg, storePath, onFatal, io: DeviceIo)`. Both always `io.submit(BackendCreate|BackendOpen, …)`. Remove
  `FsifOpenEc`. `openFsifBackend` passes `io.dataEc` and `Some(io.serialEc)`.
- Modify: `server/store/backend/fsif/FsifBackend.scala`: production no longer uses `sharedIo`/`sharedSerial`; keep
  them `private[aspen]` for tests. Compaction's inline `dataFile.truncate` and `close()`'s channel closes run on `io`.
- Modify: `server/host/StoreLoader.scala` `startOpen` and `openBackend` (pass `sds.io`; remove the inline-completion
  branch, since every open now posts `BackendOpened`), `server/host/StoreRebuilds.scala` `createNewStore`,
  `server/rebuild/StoreRebuild.scala` / `RebuildingStore.scala` (the staging opener gets the destination's
  `DeviceIo`)
- Test: `src/test/.../server/store/backend/BackendOpenerSuite.scala` (create if absent), existing FSIF suites

**Interfaces:**
- Consumes: `DeviceIo` (Task 1), `LocalStorageDeviceState.io` (Task 2).
- Produces: the new `BackendOpener` signatures above. Every test opener (`Host.Seams`, test backends) is updated to
  take `io`.

- [ ] **Step 1: Write failing tests.** `BackendOpenerSuite`: `"an FSIF open runs on the device lane and its I/O on
  the device data pool"`. Open via a `ThreadedDeviceIo`. The open's thread name starts with `dev-` and ends `-mgmt`.
  A read's I/O thread name contains `-io-`. `"a RocksDB open never completes inline"`: the returned future is not
  completed when `open` returns while the lane is blocked by a latch job.
- [ ] **Step 2:** Run. Expected: FAIL.
- [ ] **Step 3:** Implement the signature changes and follow the compiler through every caller.
- [ ] **Step 4:** Run the full suite. Expected: PASS. Fix lockstep tests that assumed an inline RocksDB open by
  draining, never by weakening an assertion.
- [ ] **Step 5:** Lower the baseline for the touched files. Update `server/store/CLAUDE.md` (backend executors) and
  `server/host/CLAUDE.md` (backend opens paragraph). Commit `Backend opens and FSIF I/O on the device's own threads`.

### Task 5: RocksDB foreground on the device data pool; background slots sized per database

**Files:**
- Modify: `server/store/backend/rocksdb/RocksDBBackend.scala`: the constructor takes `io: DeviceIo` instead of
  `executionContext`. `openDatabase` sets `setMaxBackgroundJobs(2)` (one flush at most). `threadAllocation(openDbs)`
  returns `(compaction = openDbs * 1, flush = openDbs * 1)`, with a minimum of 1 each. Remove `maxBackgroundThreads`
  from `ResourceConfig` and its CLI/config plumbing (`cmdline/Main.scala`, host config yaml).
- Modify: `server/store/backend/rocksdb/BufferedConsistentRocksDB.scala`: its `ec` is `io.dataEc`.
- Modify: `server/store/backend/BackendOpener.scala` (pass `io`).
- Test: `src/test/.../server/store/backend/rocksdb/RocksDBResourcesSuite.scala` (or the existing suite that covers
  `threadAllocation`)

**Interfaces:**
- Produces: `RocksDBBackend(dbPath: os.Path, storeId: StoreId, io: DeviceIo, ...)` and
  `RocksDBBackend.threadAllocation(openDatabases: Int): (Int, Int)`.

- [ ] **Step 1: Write failing tests.** `"background pools cover every open database"`:
  `threadAllocation(0) == (1, 1)`, `threadAllocation(7) == (7, 7)`, `threadAllocation(40) == (40, 40)` (no cap).
  `"RocksDB writes run on the device data pool"`: a `put`'s `db.write` runs on a thread whose name contains `-io-`
  (capture it through a test subclass overriding `writeBatch`).
- [ ] **Step 2:** Run. Expected: FAIL.
- [ ] **Step 3:** Implement. Update the existing assertions on `threadAllocation` to the new rule.
- [ ] **Step 4:** Run the full suite. Expected: PASS.
- [ ] **Step 5:** Update `server/store/CLAUDE.md` (RocksDB threads, why the pools are sized this way, the shared
  WriteBufferManager note from §4). Commit `RocksDB on device threads; background slots cover every database`.

### Task 6: Rebuild staging I/O on the destination device

**Files:**
- Modify: `server/store/backend/Backend.scala`: `rebuildWrite(...): Future[Unit]`, `rebuildFlush(): Future[Unit]`.
  Update the implementations in `MapBackend`, `FsifBackend` (no `Await`: chain on `serial` and `io`) and
  `RocksDBBackend`/`BufferedConsistentRocksDB` (run on `io.dataEc`, no monitor held across I/O).
- Create: `server/rebuild/RebuildDisk.scala` holding the bodies of `RebuildState.save/load/removeAll`,
  `completeStaging` and `cleanupStaging`.
- Modify: `server/rebuild/RebuildingStore.scala`: it takes `io: DeviceIo` (the destination's). Checkpoints, complete
  and cleanup become `io.submit(JobKind.Staging, Some(storeId))(RebuildDisk.…)`. Remove `given client.clientContext`
  for disk work. Client reads stay on the client context.
- Test: `src/test/.../server/rebuild/` existing rebuild suites, plus `RebuildingStoreIoSuite.scala`

**Interfaces:**
- Produces: `Backend.rebuildWrite(...): Future[Unit]` and `Backend.rebuildFlush(): Future[Unit]`;
  `RebuildDisk.save(dir: os.Path, state: RebuildState): Unit`, `RebuildDisk.load(dir: os.Path):
  Option[RebuildState]`, `RebuildDisk.complete(staging: os.Path, ...): UUID`, `RebuildDisk.cleanup(staging:
  os.Path): Unit` (exact parameters follow today's methods).

- [ ] **Step 1: Write failing test.** `RebuildingStoreIoSuite`: `"a rebuild onto a held device never blocks the
  client context"`. Run in lockstep with the destination held. The rebuild's first checkpoint is listed in
  `held(path)`. A client transaction on another store commits meanwhile, and after release the rebuild completes.
  `grep -n "Await" server/rebuild server/store/backend` returns nothing on rebuild paths (assert in the test via
  source scan, like the usage suite).
- [ ] **Step 2:** Run. Expected: FAIL.
- [ ] **Step 3:** Implement.
- [ ] **Step 4:** Run the full suite. Expected: PASS.
- [ ] **Step 5:** Lower the baselines. Update `server/rebuild/CLAUDE.md`. Commit `Rebuild staging I/O on the
  destination device's threads, no Await`.

### Task 7: Store-thread sites

**Files:**
- Create: `server/host/StoreDisk.scala` (`removeSaveFile(file: os.Path): Unit`, `saveState(file: os.Path, state:
  …): Unit`)
- Modify: `server/host/StoreRouter.scala:276-280` (`crlLoaded`: submit `SaveFile` on the store's device's `io`, post
  the result back with `router.onStore`), `server/host/StoreLoader.scala:905-912` (transfer-out close's
  `saveStoreState`: same pattern)
- Modify: `server/store/backend/fsif/FsifBackend.scala:357-361` `estimateSize`: track the data, journal and file
  sizes in memory, updated where the backend grows or truncates them; no stat.
- Test: `src/test/.../server/host/StoreRouterSuite.scala`, an FSIF suite

**Interfaces:**
- Consumes: `StoreRouter.onStore` (existing), `LocalStorageDeviceState.io`.

- [ ] **Step 1: Write failing tests.** `StoreRouterSuite`: `"crlLoaded removes the save file on the device lane"`.
  With the device held, the save file still exists after drain and `held(path) == List(SaveFile)`. After release it
  is gone, and a failed removal still reaches `handleSaveFileRemovalFailed`. FSIF: `"estimateSize matches the files
  after writes and compaction"`: compare the result with `os.size` of the three files.
- [ ] **Step 2:** Run. Expected: FAIL.
- [ ] **Step 3:** Implement.
- [ ] **Step 4:** Run the full suite. Expected: PASS.
- [ ] **Step 5:** Lower the baselines. Update `server/host/CLAUDE.md` (store-thread rules). Commit `Store threads do
  no device I/O`.

### Task 8: Read bound

**Files:**
- Modify: `server/store/backend/Backend.scala` (`def readsOverBound: Boolean = false`), `FsifBackend.scala`,
  `RocksDBBackend.scala` (call `io.beginRead()` when a backend read is submitted and `io.endRead()` when it finishes
  or fails; delegate `readsOverBound`)
- Modify: `server/store/Frontend.scala`: at the entry of a network read (where `NetworkRead` is created from a
  `Read` message), return without creating state if `backend.readsOverBound`. Log once per transition into and out
  of the over-bound state, per store.
- Modify: `cmdline/Main.scala` (`--device-io-threads`, `--device-read-bound`, feeding `DeviceIo.Config`)
- Test: `src/test/.../server/store/FrontendReadBoundSuite.scala`

**Interfaces:**
- Consumes: `DeviceIo.beginRead/endRead/readsOverBound` (Task 1).

- [ ] **Step 1: Write failing tests.** `"a network read over the bound is dropped with no state"`. Use a test backend
  with `readsOverBound = true`: send a `Read` message. No answer is sent, and the Frontend's pending-read and parked
  maps are unchanged. `"internal reads ignore the bound"`: a `TransactionRead` and a `SnapshotRead` proceed.
  `"reads under the bound are answered"`.
- [ ] **Step 2:** Run. Expected: FAIL.
- [ ] **Step 3:** Implement.
- [ ] **Step 4:** Run the full suite. Expected: PASS.
- [ ] **Step 5:** Update `server/store/CLAUDE.md`. Commit `Per-device read bound: a hung device's stores drop new
  client reads`.

### Tasks 9-15: Control-thread conversions

Every conversion task follows the same pattern and the same test shape.

- **Pattern:** move each disk-touching body into the task's `*Disk` object as a pure function. At the call site:
  1. Decide.
  2. Mark the copy with `copyRegistry.diskJob` (or the device's coalescing flag).
  3. Run `sds.io.submit(kind, Some(storeId))(XDisk.op(args))`.
  4. Apply in `onComplete` (control context): recheck (Global Constraints), then clear the mark, then act.

  A sequence that today does several disk steps and then decides on the outcome becomes one job that returns a
  result value (an ADT in the `*Disk` object) for the apply step to match on.
- **Tests per task**, in `src/test/.../server/host/<Area>IoSuite.scala`, all lockstep with `ControlFixture`:
  1. `"<sequence> waits on a held device and others proceed"`: two devices, one held. The sequence's job is in
     `heldJobs`, and the same sequence on the other device completes.
  2. `"<sequence> completing after unload changes nothing"`: hold, trigger, unload the device (or replace the copy),
     release, drain. Control state equals the post-unload state.
  3. One test per decision the sequence makes on its disk result (each branch of the result ADT), migrated from the
     existing tests for that sequence where they exist.
- Every task ends by lowering its files' baselines to the new counts, updating `server/host/CLAUDE.md` for the
  sequences it changed, running the full suite, and committing.

### Task 9: Host-root lane and per-path discovery

**Files:** `server/host/DeviceDiscovery.scala` (`checkForNewDevices`, `discoverDevices`, `tryLoadDevice`,
`loadStorageDeviceConfig` reads, the `failed`/`shutdown.state`/`owning-host` marker reads, the sweeps of the
transfer staging dir, `creating/` and `crl.creating`, `retireDuplicates`). Create `server/host/DiscoveryDisk.scala`.
`server/Host.scala` (the constructor-time first scan posts the same jobs).

**Interfaces:** `DiscoveryDisk.listDevicePaths(storageDevicesDir: os.Path): Seq[os.Path]` (runs on
`deviceIos.hostRoot`). `DiscoveryDisk.probe(path: os.Path): ProbeResult` (one job per path: isDir, config, markers),
where `enum ProbeResult { case NotADevice; case Unreadable(err: Throwable); case Device(cfg: StorageDeviceConfig,
markers: DeviceMarkers.State) }`. `DiscoveryDisk.sweepAtLoad(devicePath: os.Path): Unit`. A path with a probe
outstanding is skipped by later scans (coalescing). A path missing from a listing, with no device loaded from it, is
passed to `deviceIos.forget` (§7). A forgotten path that is still busy stays registered and is retried at the next scan.

- [ ] Write the per-task tests (pattern above). Add `"startup scan with one path held starts the host and loads the
  other device"` (Review Focus 3): hold path A before `start`, and device B loads and serves.
- [ ] Run them and see them fail. Implement. Run the full suite. Lower the baselines. Update the docs. Commit `Device
  discovery: host-root lane and one probe job per path`.

### Task 10: Device CRL open and recovery; per-path `CrlLock`

**Files:** `server/host/DeviceDiscovery.scala` (`openDeviceCrl` incl. the lost-CRL path, `holdsOwnState`,
`loadOwnedDevice`). Create `server/host/DeviceCrlDisk.scala`. `server/crl/simple/CrlLock.scala` (`held` becomes a
`ConcurrentHashMap[AnyRef, Object]` of per-file monitors; acquire and release synchronize on their file's monitor
only).

**Interfaces:** `DeviceCrlDisk.open(devicePath: os.Path, cfg: StorageDeviceConfig, holdsStores: Boolean, factory:
CrashRecoveryLogFactory): CrlOpenResult`, where `enum CrlOpenResult { case Opened(crl: CrashRecoveryLog); case
Lost(movedTo: os.Path, crl: CrashRecoveryLog); case Failed(err: Throwable) }`. The whole lost-CRL path (move aside,
create, reopen) is one job.

- [ ] Write the per-task tests, plus `CrlLockSuite`: `"a lock held on one CRL never delays acquire on another"`
  (threaded: acquire A from a thread blocked inside a test hook before `tryLock`; acquire B completes).
- [ ] Run and see them fail. Implement. Run the full suite. Lower the baselines. Update `server/crl/CLAUDE.md`
  (where the open runs; `CrlLock` per file) and the Todo.txt note about CRL recovery on the control thread. Commit
  `Device CRL opens on the device lane; CrlLock per file`.

### Task 11: Store loads

**Files:** `server/host/StoreLoader.scala` (`tryLoadStore` config read, `handleLoad` reads: `crl_save.log` via
`loadValidStoreState`, `incarnation`, `acceptor-since`, `inherited-votes`, `readArrivedAt`/`writeArrivedAt`; the
`FileStat.existence` checks at :281, :344 and :420; `closeUnloaded`). Create `server/host/LoadDisk.scala`.

**Interfaces:** `LoadDisk.readStoreConfig(path: os.Path): Try[StoreConfig]`, `LoadDisk.readLoadInputs(storePath:
os.Path, saveFile: os.Path): LoadInputs` (one job for every read `handleLoad` does), `LoadDisk.writeArrivedAt(...)`.
`handleLoad` splits into `handleLoad` (submits) and `applyLoadInputs(sds, storeId, copy, inputs)` (today's
decisions, on control). The copy's activity is `Opening` throughout, which already makes it busy. Do not add
`DiskJob` here.

- [ ] Write the per-task tests. Run and see them fail. Implement. Run the full suite. Lower the baselines. Update
  the docs. Commit `Store loads read their inputs on the device lane`.

### Task 12: Copies, moves and registry disk checks

**Files:** `server/host/StoreCopies.scala` (`retire`, `retirePresent`, `retireLost`, `moveToPendingDelete`,
`release`, `ensureDirectory`, `delete`, `sweepDeleting`), `server/host/CopyMoves.scala` (`move`, `place`,
`moveUnattached`, `forceUnattached`, `resync`, `relocate`), `server/host/HostCopies.scala` (`rescan`, `reread`,
`readAtLoad`, `existence`, `verifyAgainstDisk`), `server/host/LocalDevices.scala` (`discardStoreDirectory`).
Create `server/host/CopyDisk.scala`.

**Interfaces:** `CopyDisk.retire(...)`, `CopyDisk.delete(devicePath: os.Path, storePath: os.Path, reason: String):
Boolean`, `CopyDisk.move(...)`, `CopyDisk.list(devicePath: os.Path): CopyLayout.Listing`. The callers mark
`DiskJob(Retire|Move|Delete|Release)` through `HostCopies.diskJob`. `verifyAgainstDisk` (test-only verification)
runs as a job, and the test seam awaits it via drain.

- [ ] Write the per-task tests. Run and see them fail. Implement. Run the full suite. Lower the baselines. Update
  the docs. Commit `Copy moves, retirement and deletion on the device lane`.

### Task 13: Reconcile inputs and periodic device work

**Files:** `server/host/DeviceReconciler.scala` (gather a device's disk inputs: copy stats, `CopyLayout.list`,
phase-param reads, in one `ReconcileInputs` job; then run `decide` per store on control),
`server/host/HostServices.scala:123-141` (usage: one coalesced `Usage` job per device per tick; publish per device),
`server/host/LocalDevices.scala` (`totalSize`/`currentUsage` become values set by the usage apply step). Create
`server/host/ReconcileDisk.scala`.

**Interfaces:** `ReconcileDisk.gather(devicePath: os.Path, storeIds: Set[StoreId]): ReconcileDiskInputs`.
`DeviceReconciler` keeps `outstanding: Set[(os.Path, JobKind)]` for coalescing. `LocalStorageDeviceState.usage:
Option[(Long, Long)]` replaces the two `def`s.

- [ ] Write the per-task tests, plus `"usage ticks never queue a second job for a held device"`: hold, fire three
  ticks. `heldJobs` has one `Usage`, and the other device's usage updated three times.
- [ ] Run and see them fail. Implement. Run the full suite. Lower the baselines. Update the docs. Commit `Reconcile
  inputs and usage polled per device, coalesced`.

### Task 14: Rebuild bookkeeping

**Files:** `server/host/StoreRebuilds.scala` (`sweepUnlistedStaging`, `dropStagingCheckpoint`, `moveToLost`,
`moveClosedToLost`, `retryPendingLost`, `crlSaveFileLost`, `discardLostStore`, `bringRebuiltLive`,
`retireForeignCopy`, `createNewStore`'s non-opener disk work), `server/rebuild/RebuildingStore.scala` (the
constructor's `RebuildState.load` and `cleanupStaging` move out of the constructor: `StoreRebuilds` loads the state
with one job and constructs the `RebuildingStore` in the apply step). Create `server/host/RebuildBookkeepingDisk.scala`.

- [ ] Write the per-task tests. Run and see them fail. Implement. Run the full suite. Lower the baselines. Update
  `server/rebuild/CLAUDE.md` and `server/host/CLAUDE.md`. Commit `Rebuild bookkeeping on the device lane`.

### Task 15: Transfers, shutdown, adoption, markers

**Files:** `server/host/StoreTransfers.scala` (`removeStaging`, `bringArrivedLive`, the arrived-copy checks in
`startIn`/`retireEarlierTenancy`, `discardArrivedTransferIn`, `dropAbandonedTransferIn`),
`server/host/StoreTransfersOut.scala` (`extractUnloaded`, `extractFromDirectory`, `moveToOutbound`),
`server/host/DeviceShutdown.scala` (marker writes at :90, :118 and :246), `server/host/DeviceDiscovery.scala`
(`bringIntoService`, `moveToLostUnattached`), `server/transfer/StoreTransferManager.scala` (`directoryLock` becomes
one lock per device root, `Map[os.Path, Object]` keyed by the staging dir's device; `removeOwnedDirectory` and the
control-called `cancelTransferIn`/supersede `abort` never take it on the caller's thread and return a `Future[Unit]`
run as a `Delete` job on the device's lane). Create `server/host/TransferDisk.scala` and
`server/host/DeviceStateDisk.scala`.

**Interfaces:** `StoreTransferManager.cancelTransferIn(...): Future[Unit]` (was `Unit`). Callers store the
`JobHandle` of a queued removal per transfer attempt. A superseding cancel calls `handle.cancel()` before submitting
anything new (Review Focus 4).

- [ ] Write the per-task tests, plus `"a cancelled transfer-in behind a hung job queues one removal"`: hold the
  device, start a transfer-in, cancel it twice. `heldJobs` contains exactly one `Delete`. After release, the staging
  dir is gone and no attempt is recorded.
- [ ] Run and see them fail. Implement. Run the full suite. Lower the baselines. Update the docs (`server/host`,
  `server/transfer` scaladoc). Commit `Transfers, shutdown markers and adoption on the device lane; per-device
  transfer lock`.

### Task 16: Hang tests and the empty baseline

**Files:**
- Modify: `src/test/scala/org/aspen_ddp/aspen/server/DeviceIoUsageSuite.scala` (the baseline becomes `Map.empty`)
- Create: `src/test/.../server/host/HungDeviceSuite.scala` (lockstep, `ControlFixture`) and
  `src/test/.../server/host/ThreadedHungDeviceSuite.scala` (an `IntegrationTestSuite` on `StoreThreadPool`, with a
  `ThreadedDeviceIo` gate seam on one device, passed through a `HostThreading` test subclass overriding
  `newDeviceIo`)

**Interfaces:** Consumes everything above.

- [ ] **Step 1: Write the tests.**
  - Lockstep `"a held device stalls only its own stores"`: two devices, A held. Device B: a scan, a store load, a
    transfer in and a rebuild onto B all complete. A's jobs are only listed in `heldJobs`.
  - Lockstep `"a store with copies on both devices"` (Review Focus 2): a rebuild of store S onto B while S's old copy
    is on held device A. The new copy's entry becomes Active and is served. The old copy's release is in
    `heldJobs(A)` and not run. After release it runs and the single-owner invariant holds
    (`verifyCopyRegistry`).
  - Threaded `"a hung device stalls only its own stores"`: gate device A shut (latch). On B, 50 transactions commit
    and reads are answered. Heartbeats continue: the host's heartbeat count rises over the test (use the existing
    heartbeat seam or the gossip counter). A thread dump of the host's non-`dev-<A>` threads shows none in
    `WAITING` on A's latch. A hung read flood (10× the bound against A's store) leaves `readsOverBound` true and the
    process heap below the start + 64 MiB (Review Focus 5). Open the latch, and A's stores answer again.
- [ ] **Step 2:** Run them. Expected: PASS. A failure is a real isolation gap: fix it in the owning area. Then set
  the baseline to empty and see the usage suite pass.
- [ ] **Step 3:** Run the full suite twice under load (`stress-ng --cpu 0` or a parallel `sbt test` loop, per the
  flake-hunting memory) to check for flakes.
- [ ] **Step 4:** Commit `Hung-device isolation tests; no device I/O left on the control thread`.

### Task 17: Measurement and final docs

**Files:** `docs/host-threading-measurement.md` (new section with the numbers), `server/CLAUDE.md`,
`server/host/CLAUDE.md`, `server/store/CLAUDE.md`, `Todo.txt` (mark project 1 done, with notes for projects 2 and 3:
the observer's in-flight table, coalescing flags, read-bound transitions), and the spec's status line.

- [ ] **Step 1:** Check out `main` at the commit before Task 4 (`git worktree add --force` to a scratch path; remove
  it with `--force` afterwards). Run the measurement doc's benchmarks (randrw and seqwrite; single device and two
  devices; FSIF and RocksDB; three runs each) and record the baseline.
- [ ] **Step 2:** Run the same on `device-threads` HEAD.
- [ ] **Step 3:** Compare. If a result is outside the run-to-run spread, try `--device-io-threads` 2, 8 and
  CPUs/2. If no default fixes it, stop and report to the user with both tables (§8). Do not change the design.
- [ ] **Step 4:** Write the numbers and the chosen defaults into the measurement doc. Update the docs listed above.
  Commit `Device I/O isolation: measurements, defaults and docs`.
