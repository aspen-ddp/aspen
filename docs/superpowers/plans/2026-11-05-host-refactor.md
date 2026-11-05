# Host Refactor Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or
> superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Split `server.Host` into single-concern classes, run device management on one control thread with no
locks, and run stores on pluggable store threads, while the regression suites keep today's ordering through a
lockstep mode.

**Architecture:** Device management moves into `server.host` classes wired by a `DeviceControl` object. All of them
run on a single-threaded control `ExecutionContext` that `HostThreading` supplies. The hot path goes through a
concurrent `StoreRouter` straight to each store's `StoreExecutor`. `HostThreading` has three implementations:
`LockstepThreading` (tests: one FIFO queue, drained by `TestNetwork.handleEvents()`), `SingleThreadThreading` (an
interim step and a debugging aid: everything on one thread, as today) and `StoreThreadPool` (the default: lazy, at
most one thread per CPU, least-loaded placement).

**Tech Stack:** Scala 3, sbt, ScalaTest (`AsyncFunSuite` through `IntegrationTestSuite`, `AnyFunSuite` for unit tests),
os-lib.

**Spec:** `docs/superpowers/specs/2026-11-05-host-refactor-design.md` (read it first; section numbers below refer to it).

**One deviation from the spec's order of work.** The spec's step 1 says to rewrite the Host suites as each class is
extracted. This plan extracts first and keeps the suites unchanged, because an unchanged suite passing is the proof
that extraction preserved behaviour. It then rewrites them (Phase 3) after the control context exists and before the
store threads arrive, so the rewritten suites guard the threading change.

## Global Constraints

- All work on branch `host-refactor`. Commit after every task. Every commit message ends with
  `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.
- Scala 3 quiet syntax; braces only where they clearly help. Indented `if/then/else`, no one-line `if a then b`.
- os-lib for filesystem work; `java.nio` only where os-lib has no equivalent.
- Persisted-data compatibility is not required.
- Test priorities: correctness, then reliability (no flaky tests, ever), then speed.
- Full-suite command: `sbt test`. Under normal load, treat no progress for 70 s as a hang. Single suite:
  `sbt 'testOnly *SuiteName'`; single test: `sbt 'testOnly *SuiteName -- -z "substring"'`.
- **Acceptance bar for every task:** the full suite passes. From Task 9 on, the integration suites (everything outside
  the suites being rewritten in Phase 3) must pass **unchanged**. A changed expected ordering is a bug in the new code.
- New production classes live in package `org.aspen_ddp.aspen.server.host`
  (`src/main/scala/org/aspen_ddp/aspen/server/host/`). `Host` stays at `server.Host`.
- Store threads are named `aspen-store-N` and are daemon threads. The control thread is named `aspen-host-control`
  and is non-daemon.
- Default store-thread cap: `Runtime.getRuntime.availableProcessors()`.
- Strategy selection is in code only (Host constructor parameter). Add no config-file option.
- Do not change Paxos, the CRL, the transaction protocol or store behaviour.

## Review Focus

These are failure modes the spec implies but no task's ordinary tests cover. Each one has a pinning test in the task
named.
1. **A store closed while traffic is in flight for it.** Messages and I/O completions that arrive after the close must
   get today's offline handling and never reach a closed store. Pinned in Task 15
   (`close racing traffic gets offline handling`).
2. **A message reaching a store during CRL recovery.** It must be held until `frontend.recover()`, as today, and never
   answered from unrecovered state. Pinned in Task 15 (`messages during CRL recovery are held until recover`).
3. **A Future callback in control code with no context named.** It must not compile, or must still land on the
   control thread. Pinned in Task 10 (`foreign-thread completion runs its callback on the control thread`), plus the
   grep check in Task 10 Step 6.
4. **An exception thrown inside a store task or control task.** The thread must log it and keep running, as today's
   event loop does (`Host.scala:1211-1233`). Pinned in Task 9 (`a throwing task does not stop the queue`) and Task 14
   (`a throwing task does not kill its store thread`).
5. **A host with more stores than threads, or stores loaded and unloaded repeatedly.** Placement must stay balanced
   and freed slots must be reused. Pinned in Task 14 (`release frees a slot for the next store`).

---

## Method → class map (used by Tasks 2–8)

Line numbers are in `src/main/scala/org/aspen_ddp/aspen/server/Host.scala` at commit `89e7e42d`. Each listed member
moves out of `Host`, together with any private helpers only it uses.

| Target (file under `server/host/`) | Members moving from Host |
|---|---|
| `LocalDevices.scala` | `storageDevices` (`:324`), `Host.LocalStorageDeviceState` (`:243`), `getDevicePath` (`:1237`), `discardStoreDirectory` (`:1501`) |
| `DeviceMetadata.scala` (trait + `ClientDeviceMetadata`) | `lookupStorageDeviceState` `:2714`, `readTransferSourceState` `:2721`, `updateStateForTransferredStore` `:1266`, `markRebuiltStoreActive` `:2296`, `completeDeviceShutdown` `:2755`, `adoptDevice` `:2608`, `abortLostTransfer` `:2039`, `readHostStateForEndpoint` `:1483` |
| `ControlScheduler.scala` | the delays behind `scheduleShutdownWorkTimeout` `:2951`, `scheduleEndpointRetry` `:1486`, `scheduleTransferInRetry` `:1541`, `scheduleDeviceRecheck` `:3366` |
| `BackendOpener.scala` | `openBackend` `:1178`, `openFsifBackend` `:1196`, `fsifOpenEc` `:1156` |
| `StoreRouter.scala` | `stores` `:346`, `offlineStores` `:349`; `handleEvent` cases `IOCompletion`, `TransactionMessage`, `ClientReq`, `Repair`, `RepairDelete`, `HeartbeatEvent`; `containsStore`, `getStoreIds`, `hasTransactions`, `logTransactionStatus` |
| `StoreLoader.scala` | `tryLoadStore` `:1004`, `serveOpened` `:1040`, `startFsifOpen` `:1051`, `fsifOpened` `:1084`, `isFsif` `:1158`, `retryStoresAwaitingSpace` `:1164`, `backendHasFailed` `:2426`, `closeStore` `:3634`; `handleEvent` cases `LoadStore`, `StoreCrlLoaded`, `UnloadFailedStore`, `ShutdownStore`, `FsifOpened`, `LoadStoreById`; state `storesAwaitingSpace`, `fsifOpening`, `spaceRetryCapScans`; `Host.SpaceWait`, `Host.StoreFailedOnClose`, `Host.FsifOpenEc` |
| `DeviceDiscovery.scala` | `checkForNewDevices` `:596`, `warnOnce` `:619`, `logDeviceDirWarning` `:625`, `resolveShutdownMarker` `:638`, `shutdownMarkerResolved` `:672`, `adoptionClass` `:763`, `tryAdoptDevice` `:787`, `offlineStoresIn` `:825`, `uncleanStores` `:834`, `warnUnclean` `:837`, `bringIntoService` `:870`, `tryLoadDevice` `:911`, `loadOwnedDevice` `:941`; state `storageDevicesDir`, `loadedDeviceDirs`, `reportedDeviceDirs`, `pendingMarkerResolutions`; `Host.AdoptOutcome`, `Host.AdoptionClass` |
| `StoreTransfers.scala` | `retransmitPendingStartTransfers` `:432`, `readdressPendingTransfer` `:456`, `discardArrivedTransferIn` `:1524`, `dropAbandonedTransferIn` `:1603`, `clearTransferredInMarker` `:1633`, `recordTransferIn` `:1651`, `startStoreTransferIn` `:1729`, `startStoreTransferOut` `:1890`, `returnLostTransferToRebuild` `:2022`, `closeStoreForTransferOut` `:2726`, `reinstateAbandonedTransferOut` `:3279`, `abandonTransferIn` `:3314`; state `abortingLostTransfers`, `recordingTransfersIn`, `transferringIn`, `abandoningTransferIns`, `pendingStartTransfers`, `transferInRetries`, `pendingStartTask`; `Host.PendingTransfer`, `Host.TransferInRetry`, `TransferInRetryDelay`, `TransferOutRecheckDelay`, `Host.TransferOutcome` |
| `StoreRebuilds.scala` | `createNewStore` `:2058`, `startStoreRebuild` `:2148`, `keepRebuiltStoreOffline` `:2368`, `markRebuildRequired` `:2387`, `crlSaveFileLost` `:2400`, `retireUnservedStore` `:2410`, `failedOnClose` `:2434`, `backendFailed` `:2458`, `requestStoreRebuild` `:2504`, `discardLostStore` `:2547`; `handleEvent` case `BackendFailed`; state `rebuildingStores`, `rebuildsSettled`, `pendingRebuildRequests`, `creatingStores`; `Host.RebuildOutcome` |
| `DeviceShutdown.scala` | `shutDownDevice` `:2802`, `stopDeviceWork` `:2875`, `awaitShutdownWork` `:2934`, `closeStoresForShutdown` `:2970`, `storesToClose` `:2992`, `closeThenUnload` `:3009`, `unloadDevice` `:3024`, `closeStoreForShutdown` `:2732`; state `shuttingDownDevices`; `Host.ShutdownOutcome` |
| `DeviceReconciler.scala` | `reconcileDeviceState` `:3048`, `runDeviceRecheck` `:3372`, `checkStorageDevice` `:3392`, `startDeviceCheck` `:3467`; `handleEvent` cases `CheckAllDevices`, `ScanDevices`, `HostMsg(CheckStorageDevice)`; state `activeDeviceChecks`, `scheduledDeviceRechecks`, `deferredDeviceChecks` |
| `HostServices.scala` | `initializeTaskExecutor` `:495`, `initializeSystemTaskRunner` `:545`, `startUsageTracking` `:558`, `getTaskExecutor`, `getServiceExecutor`, `reconcileEndpoint` `:1441`, `gossipStoreOnline` `:2577`; state `taskExecutorPromise`, `serviceExecutorPromise`, `systemTaskRunnerPromise`, `poolUsageManager`, `deviceUsageManager`, `usageUpdateTask`, `gossipRetransmitter`, `gossipRetransmitTask`; `handleEvent` cases `InitializeTaskExecutor`, `SystemTaskHeartbeatEvent`, `HostMsg(ServiceMessage | ExecuteSystemTask)` |

`createNewStore` is placed with rebuilds because it shares the `creatingStores` bookkeeping that `abandonTransferIn`
and `abortLostTransfer` consult, and it opens a store the way a rebuild places one.

---

## Phase 1: Extract classes, with no threading change (Tasks 1–8)

**Rules for all of Phase 1:**
- **Behaviour is unchanged.** Every existing suite passes unmodified; no test file changes in Phase 1.
- **Locking is unchanged for now.** Each extracted class receives `lock: AnyRef` (the `Host` instance). Every
  `synchronized` block it inherits becomes `lock.synchronized`. Phase 2 deletes them.
- **The execution context is unchanged for now.** Each extracted class keeps `given ExecutionContext = ec`, using
  Host's `ec`, until Task 10.
- **Event posting is unchanged for now.** Classes that put events use a `post: Host.Event => Unit` function that Host
  supplies (`events.put`). `Host.Event` and its cases become `private[server]` so that `server.host` can construct
  them.
- **Seams stay overridable for now.** The suites override `protected def`s on Host through `RecordingHost`
  (`src/test/scala/org/aspen_ddp/aspen/server/HostTestHarness.scala:182`) and `FsifTestHost`. Until Task 11, every
  overridden `protected def` stays on Host as a one-line delegate. The seam object Host passes to the new classes is
  an adapter that calls those Host methods, so the overrides still take effect. The full list of overridden methods:
  `updateStateForTransferredStore`, `tryLoadStore`, `scheduleShutdownWorkTimeout`, `scheduleEndpointRetry`,
  `readTransferSourceState`, `readHostStateForEndpoint`, `markRebuiltStoreActive`, `lookupStorageDeviceState`,
  `logDeviceDirWarning`, `completeDeviceShutdown`, `closeStoreForTransferOut`, `closeStoreForShutdown`, `adoptDevice`,
  `abortLostTransfer`, `openFsifBackend`, `gossipStoreOnline`, `fsifOpenEc`.
- **The test hooks stay for now.** Every `testingOnly*` hook on Host stays, delegating into the class that now owns
  the state.
- **Not-yet-extracted methods are reached through Host.** A class may call a method still on Host through a
  `private[server]` widening. Task 8 removes every such call.

### Task 1: Wiring, seams and `LocalDevices`

**Files:**
- Create: `server/host/DeviceControl.scala`, `server/host/LocalDevices.scala`, `server/host/DeviceMetadata.scala`,
  `server/host/ControlScheduler.scala`, `server/host/BackendOpener.scala`
- Modify: `server/Host.scala`

**Interfaces:**
- Produces:
  - `class LocalDevices`, with:
    - `var devices: Map[StorageDeviceId, LocalStorageDeviceState]`
    - `def devicePath(id: StorageDeviceId): Option[os.Path]`
    - `def discardStoreDirectory(...)`, with the signature it has today
  - `LocalStorageDeviceState` moves to `server.host`, unchanged.
  - `trait DeviceMetadata`, with exactly the eight methods in the map, each with today's signature (minus `protected`).
    - `class ClientDeviceMetadata(client: AspenClient, hostId: HostId, …)` holds today's bodies.
    - Where a body reads Host state, that state becomes a parameter of the method.
  - `trait ControlScheduler`, with `def schedule(delay: Duration)(fn: => Unit): BackgroundTaskManager.ScheduledTask`.
    - `class BackgroundControlScheduler(bg: BackgroundTaskManager, post: (() => Unit) => Unit)` runs `fn` by posting
      it, never directly on a timer thread.
  - `trait BackendOpener`, with:
    - `def open(storeCfg: StoreConfig, storePath: os.Path, create: Boolean, onFatal: Throwable => Unit): Backend`
    - `def fsifOpenEc: ExecutionContext`
    - `class DefaultBackendOpener` holds today's `openBackend`/`openFsifBackend` bodies.
  - `final class DeviceControl`, holding:
    - `metadata: DeviceMetadata`, `scheduler: ControlScheduler`, `backends: BackendOpener`, `devices: LocalDevices`
    - `client`, `hostId`, `aspenSystemId`, `rootDir`, `net`, `backgroundTasks`, `lock: AnyRef`,
      `post: Host.Event => Unit`, `given ec`
    - later tasks add one `lazy val` per component (`loader`, `router`, `discovery`, `transfers`, `rebuilds`,
      `shutdown`, `reconciler`, `services`). Each component takes `ctx: DeviceControl` as its only constructor
      parameter, which resolves the cycles between them.

- [ ] **Step 1:** Create the five files with the interfaces above, moving the bodies from Host.
- [ ] **Step 2:** In Host, build `DeviceControl` with adapter seams. For example,
      `new DeviceMetadata { def lookupStorageDeviceState(id) = Host.this.lookupStorageDeviceState(id); … }`. Keep each
      `protected def` as a delegate to `ClientDeviceMetadata` / `DefaultBackendOpener`, so the test overrides still win.
- [ ] **Step 3:** Run `sbt 'testOnly *HostDeviceDiscoverySuite *HostFsifLoadSuite *DeviceShutdownSuite
      *DeviceAdoptionSuite'`. Expected: all pass.
- [ ] **Step 4:** Run `sbt test`. Expected: all pass.
- [ ] **Step 5:** Commit: `host: extract DeviceControl wiring, metadata/scheduler/backend seams and LocalDevices`.

### Task 2: `StoreRouter` and `StoreLoader`

**Files:** Create `server/host/StoreRouter.scala` and `server/host/StoreLoader.scala`. Modify `server/Host.scala`.

**Interfaces:**
- Produces:
  - `class StoreRouter(ctx: DeviceControl)`, with:
    - `def deliverTx(msg: TxMessage): Unit`
    - `def deliverClient(msg: ClientRequest): Unit`
    - `def deliverCompletion(op: Completion): Unit`
    - `def repair(storeId: StoreId, os: ClientObjectState, completion: Promise[Unit]): Unit`
    - `def repairDelete(storeId: StoreId, objectId: ObjectId, storePointer: Array[Byte], completion: Promise[Unit]): Unit`
    - `def heartbeat(): Unit`
    - `def get(storeId: StoreId): Option[Store]`
    - `def isOffline(storeId: StoreId): Boolean`
    - `def storeIds: List[StoreId]`
    - `def hasTransactions: Boolean`
    - `def logTransactionStatus(log: String => Unit): Unit`
    - `private[host] def install(store: Store): Unit`, which also clears the store's offline mark
    - `private[host] def remove(storeId: StoreId): Unit`, which also sets its offline mark
  - `class StoreLoader(ctx: DeviceControl)`, with:
    - `def tryLoadStore(...)`, with today's signature
    - `def handleLoad(e: Host.LoadStore)`, `def handleCrlLoaded(e)`, `def handleUnloadFailed(e)`, `def handleClose(e)`,
      `def handleFsifOpened(e)`, `def handleLoadById(e)`
    - `def retryStoresAwaitingSpace(): Unit`
    - `def closeStore(storeId: StoreId): Future[Unit]`
- [ ] **Step 1:** Move the members listed in the map. `handleEvent`'s matching cases call the new methods.
      `IOHandler` calls `post(IOCompletion(op))` as today.
- [ ] **Step 2:** Run `sbt 'testOnly *HostFsifLoadSuite *HostCrlSaveLoadSuite *FsifIntegrationSuite'`, then `sbt test`.
      Expected: all pass.
- [ ] **Step 3:** Commit: `host: extract StoreRouter and StoreLoader`.

### Task 3: `DeviceDiscovery`

**Files:** Create `server/host/DeviceDiscovery.scala`. Modify `server/Host.scala`.

**Interfaces:**
- Produces `class DeviceDiscovery(ctx: DeviceControl)`, with:
  - `def checkForNewDevices(): List[StorageDeviceId]`
  - `def handleMarkerResolved(e: Host.ShutdownMarkerResolved): Unit`
  - `def warnOnce(path: os.Path, msg: String): Unit`
  - `private[host]` accessors `loadedDeviceDirs`, `reportedDeviceDirs` and `pendingMarkerResolutions`, used by Host's
    hooks until Task 11
  - `logDeviceDirWarning` becomes `ctx.warn: String => Unit`, adapted to Host's `protected def` in this phase
- [ ] **Step 1:** Move the members listed in the map.
- [ ] **Step 2:** Run `sbt 'testOnly *HostDeviceDiscoverySuite *DeviceAdoptionSuite'`, then `sbt test`. Expected: all
      pass.
- [ ] **Step 3:** Commit: `host: extract DeviceDiscovery`.

### Task 4: `StoreTransfers`

**Files:** Create `server/host/StoreTransfers.scala`. Modify `server/Host.scala`.

**Interfaces:**
- Produces `class StoreTransfers(ctx: DeviceControl)`, with:
  - `def startOut(m: StartStoreTransfer): Future[Unit]`
  - `def startIn(...)`, `def abandonIn(...)`, `def reinstateAbandonedOut(...)`, with today's signatures
  - `def retransmitPendingStarts(): Unit`
  - `private[host]` accessors `transferringIn`, `transferInRetries` and `pendingStartTransfers`
  - `def inFlightFor(deviceId: StorageDeviceId)`: the work `stopDeviceWork` collects for that device (pending
    starts, recording, ins, retries)
  - `TransferInRetry` is no longer run by a hook. A retry is scheduled through `ctx.scheduler.schedule(
    TransferInRetryDelay)`. For now, `testingOnlyRetryTransferIns` runs the recorded `run` closures, as today.
- [ ] **Step 1:** Move the members listed in the map.
- [ ] **Step 2:** Run `sbt 'testOnly *HostDeviceDiscoverySuite *AbortStoreTransferSuite *SetRebalanceDurableTaskSuite
      *MigratePoolToSetSuite'`, then `sbt test`. Expected: all pass.
- [ ] **Step 3:** Commit: `host: extract StoreTransfers`.

### Task 5: `StoreRebuilds`

**Files:** Create `server/host/StoreRebuilds.scala`. Modify `server/Host.scala`.

**Interfaces:**
- Produces `class StoreRebuilds(ctx: DeviceControl)`, with:
  - `def createNewStore(...)`, `def startRebuild(...)`, `def requestRebuild(...)`, `def markRebuildRequired(...)`,
    `def crlSaveFileLost(...)`, `def retireUnserved(...)`, `def failedOnClose(store: Store): Unit`, with today's
    signatures
  - `def handleBackendFailed(e: Host.BackendFailed): Unit`
  - `def isRebuilding(storeId: StoreId): Boolean`
  - `def isCreating(storeId: StoreId): Boolean`
  - `def settledFor(deviceId: StorageDeviceId): List[Future[Unit]]`, used by shutdown
- [ ] **Step 1:** Move the members listed in the map.
- [ ] **Step 2:** Run `sbt 'testOnly *StoreRebuildTriggerSuite *RebuildingStoreSuite *FailedStorageDeviceSuite'`, then
      `sbt test`. Expected: all pass.
- [ ] **Step 3:** Commit: `host: extract StoreRebuilds`.

### Task 6: `DeviceShutdown`

**Files:** Create `server/host/DeviceShutdown.scala`. Modify `server/Host.scala`.

**Interfaces:**
- Produces `class DeviceShutdown(ctx: DeviceControl)`, with:
  - `def shutDownDevice(local: LocalStorageDeviceState, remote: StorageDeviceState): Unit`, with today's parameters
  - `def isShuttingDown(id: StorageDeviceId): Boolean`
  - `def unloadDevice(...)`
- `stopDeviceWork` gathers each component's in-flight work through `ctx.transfers.inFlightFor` and
  `ctx.rebuilds.settledFor`.
- [ ] **Step 1:** Move the members listed in the map.
- [ ] **Step 2:** Run `sbt 'testOnly *DeviceShutdownSuite *DeviceAdoptionSuite'`, then `sbt test`. Expected: all pass.
- [ ] **Step 3:** Commit: `host: extract DeviceShutdown`.

### Task 7: `DeviceReconciler` and `HostServices`

**Files:** Create `server/host/DeviceReconciler.scala` and `server/host/HostServices.scala`. Modify
`server/Host.scala`.

**Interfaces:**
- Produces:
  - `class DeviceReconciler(ctx: DeviceControl)`, with:
    - `def checkStorageDevice(id: StorageDeviceId): Unit`
    - `def checkAll(): Unit` (the `CheckAllDevices` case)
    - `def scan(): Unit` (the `ScanDevices` case)
    - `def handleCheckMessage(m: CheckStorageDevice): Unit`
    - `private[host]` accessors `activeDeviceChecks`, `scheduledDeviceRechecks` and `deferredDeviceChecks`
    - rechecks are scheduled through `ctx.scheduler.schedule(TransferOutRecheckDelay)`
  - `class HostServices(ctx: DeviceControl)`, with:
    - `def initializeTaskExecutor(): Unit`
    - `def getTaskExecutor(): Future[TaskExecutor]`
    - `def getServiceExecutor(): Future[DurableServiceExecutor]`
    - `def deliver(m: ServiceMessage | ExecuteSystemTask): Unit`
    - `def systemTaskHeartbeat(): Unit`
    - `def reconcileEndpoint(...)`, with today's signature
    - `def gossipStoreOnline(storeId: StoreId): Unit`
    - `def shutdown(): Unit`, which cancels its periodic tasks and shuts down the executors
- [ ] **Step 1:** Move the members listed in the map.
- [ ] **Step 2:** Run `sbt 'testOnly *HostEndpointSuite *HostSystemTaskSuite *HostManagerSuite'`, then `sbt test`.
      Expected: all pass.
- [ ] **Step 3:** Commit: `host: extract DeviceReconciler and HostServices`.

### Task 8: Clean up what is left of Host

**Files:** Modify `server/Host.scala` and the `server/host/*` files.

- [ ] **Step 1:** Remove every call from `server.host` back into `Host` that Phase 1 allowed through
      `private[server]`. The components reach each other only through `ctx`.
- [ ] **Step 2:** Run `grep -n "host\.\|Host\.this" src/main/scala/org/aspen_ddp/aspen/server/host/*.scala`. Expected:
      only references to `Host.Event` case classes and the `Host` companion's constants.
- [ ] **Step 3:** Run `wc -l src/main/scala/org/aspen_ddp/aspen/server/Host.scala`. Expected: under 700, made up of
      construction, the public entry points, `handleEvent` dispatch, and the temporary delegates and hooks. Task 11
      shrinks it further.
- [ ] **Step 4:** Run `sbt test`. Expected: all pass.
- [ ] **Step 5:** Commit: `host: components reach each other only through DeviceControl`.

---

## Phase 2: Threading interface and the control context (Tasks 9–10)

### Task 9: `HostThreading`, `LockstepThreading` and `SingleThreadThreading`; the events queue goes

**Files:**
- Create: `server/host/HostThreading.scala`, `server/host/LockstepThreading.scala`,
  `server/host/SingleThreadThreading.scala`
- Test: `src/test/scala/org/aspen_ddp/aspen/server/host/LockstepThreadingSuite.scala`,
  `.../server/host/SingleThreadThreadingSuite.scala`
- Modify: `server/Host.scala` (constructor parameter `threading: HostThreading`; delete `events`, `threadPool`,
  `start()`'s loop and `IOHandler`), `server/SimpleDriverRecoveryMixin.scala`, `cmdline/Main.scala`,
  `src/test/scala/org/aspen_ddp/aspen/TestNetwork.scala`, `HostTestHarness.scala`, `HostFsifLoadSuite.scala`,
  `HostCrlSaveLoadSuite.scala`, `FsifIntegrationSuite.scala` (constructor argument only)

**Interfaces:**
- Produces:

```scala
/** How a host runs its stores and its device-management control thread. See the threading-model scaladoc in
 *  HostThreading.scala and CLAUDE.md "Host threading model". */
trait HostThreading:
  /** Where every device-management Future callback runs. Single-threaded. */
  def control: ExecutionContext
  /** Where control events are posted (production: the control thread; lockstep: the FIFO). */
  def controlEvents: Executor
  /** Entry hop for work arriving from a foreign thread: network, I/O completion, timers, CRL callbacks.
   *  Production: runs inline. Lockstep: enqueues on the FIFO. */
  def dispatch: Executor
  /** Binds a store being loaded to an executor. Called on the control thread. */
  def assign(storeId: StoreId, deviceId: StorageDeviceId): StoreExecutor
  /** Called on the control thread once a store's close has completed. */
  def release(storeId: StoreId): Unit
  def shutdown(): Unit

/** A single-threaded FIFO executor for one or more stores. */
trait StoreExecutor extends ExecutionContext
```

- `LockstepThreading(ec: ExecutionContext)`:
  - `control = ec`
  - `controlEvents` and `dispatch` enqueue on one FIFO
  - `assign` returns an inline executor that runs the task on the caller's thread
  - `def drain(): Unit` runs queued tasks until the queue is empty
  - `def hasQueued: Boolean`
- `SingleThreadThreading()`:
  - one non-daemon thread named `aspen-host-control` drains a FIFO queue
  - `control`, `controlEvents` and `dispatch` all enqueue on it
  - `assign` returns an executor that runs inline, because the caller is already on that thread
  - this reproduces today's production behaviour
- In both, a task that throws is logged (by the task's class name only, as `Host.scala:1233` does) and the loop
  continues.
- Mapping of today's `events.put` sites:
  - `TransactionMessage`, `ClientReq`, `Repair`, `RepairDelete`, `IOCompletion`, `HeartbeatEvent`, `StoreCrlLoaded`
    and `UnloadFailedStore` go through `threading.dispatch`.
  - Every other `Event` goes through `threading.controlEvents`.
  - Each site still enqueues exactly one item, at the same point in the code as today.
- `Host.testingOnlyHandleEvents()` and `hasEvents` delegate to `LockstepThreading.drain` / `hasQueued`, and throw if the
  threading is not lockstep.
- `TestNetwork` and every test Host pass `new LockstepThreading(executionContext)`. `Main.scala` passes
  `new SingleThreadThreading()`.

- [ ] **Step 1: Write the failing tests.**
  - `LockstepThreadingSuite`:
    - `tasks run in posting order across dispatch and controlEvents`: post A by dispatch, B by controlEvents, C by
      dispatch; `drain()`; the recorded order is `List("A","B","C")`.
    - `nothing runs until drain`: post a task; it has not run; after `drain()` it has run.
    - `tasks posted while draining run in the same drain`: a task posts another; one `drain()` runs both.
    - `assign returns an inline executor`: `assign(...).execute(r)` runs `r` before `execute` returns, on the calling
      thread.
    - `a throwing task does not stop the queue`: post a throwing task then a recording task; `drain()` runs the
      second.
  - `SingleThreadThreadingSuite`:
    - `all work runs on one thread named aspen-host-control`: post 1,000 tasks from 8 threads through
      `dispatch`/`controlEvents`/`control`; every recorded `Thread.currentThread.getName` equals
      `"aspen-host-control"`; await completion with a `CountDownLatch`.
    - `per-poster FIFO order`: each of the 8 posters records a sequence number; each poster's sequence appears in
      increasing order.
    - `a throwing task does not stop the queue`: the same check as the lockstep suite.
- [ ] **Step 2:** Run `sbt 'testOnly *LockstepThreadingSuite *SingleThreadThreadingSuite'`. Expected: FAIL (classes do
      not exist).
- [ ] **Step 3:** Implement the two classes and wire `Host` as described above.
- [ ] **Step 4:** Run the two suites. Expected: PASS.
- [ ] **Step 5:** Run `sbt test`. Expected: all pass, with no test file changed except the constructor arguments.
- [ ] **Step 6:** Commit: `host: HostThreading with lockstep and single-thread implementations replace the event queue`.

### Task 10: Run device management on the control context and remove its locks

**Files:**
- Modify: every `server/host/*.scala` component, `server/Host.scala`, `server/repair/RepairTarget.scala` (no change
  if `Host` keeps implementing it through the view)
- Create: `server/host/HostStoreView.scala`
- Test: `src/test/scala/org/aspen_ddp/aspen/server/host/ControlContextSuite.scala`
- Modify: `CLAUDE.md` (add the "Host threading model" section)

**Interfaces:**
- Produces:
  - `final case class HostStoreView`, with:
    - `served: Set[StoreId]`, `offline: Set[StoreId]`, `repairable: Set[StoreId]`
    - `devicePaths: Map[StorageDeviceId, os.Path]`
  - `DeviceControl` gains:
    - `@volatile private var view: HostStoreView`
    - `def currentView: HostStoreView`
    - `private[host] def publishView(): Unit`, called after every change to the router's maps, `devices`,
      `rebuildingStores` or the transferring-out set. It recomputes `repairable` exactly as `repairableStoreIds` does
      today.
  - `Host.containsStore`, `getStoreIds`, `getDevicePath`, `repairableStoreIds` and `isRepairable` read
    `ctx.currentView`.
  - `DeviceControl`'s `given ExecutionContext` becomes `threading.control`. No other `given ExecutionContext` or
    `implicit` context may exist in `server.host`. Work deliberately run elsewhere (`backends.fsifOpenEc`; client
    transactions that take a context) names it explicitly, with a one-line comment saying where it runs and how its
    result comes back.
  - `lock` is removed from `DeviceControl`.
- Documentation (spec section 2):
  - The scaladoc at the top of `HostThreading.scala` states rules 1–4 verbatim from the spec.
  - Each component's class scaladoc opens with: "Runs only on the control thread. See HostThreading.scala."
  - `CLAUDE.md` gets a "Host threading model" section after "Transaction Design", covering: store threads; the control
    thread and its four rules; `LockstepThreading` for tests; and the rule that "callbacks name the control context".
    Keep it to about 8 lines in the file's existing style.

- [ ] **Step 1: Write the failing tests** in `ControlContextSuite` (an `AnyFunSuite`, using `SingleThreadThreading`).
  - `foreign-thread completion runs its callback on the control thread`: complete a `Promise` from a fresh thread;
    its `onComplete(using threading.control)` records `"aspen-host-control"`.
  - `view is published to other threads`: on the control thread, change the router's served set and call
    `publishView()`; a separate thread sees the new `currentView.served` after a latch.
- [ ] **Step 2:** Run `sbt 'testOnly *ControlContextSuite'`. Expected: FAIL.
- [ ] **Step 3:** Replace the contexts, delete every `lock.synchronized` in `server/host/*` and every remaining
      `synchronized` in `Host`, add `HostStoreView`, and write the documentation.
- [ ] **Step 4:** Run `grep -rn "synchronized" src/main/scala/org/aspen_ddp/aspen/server/host/
      src/main/scala/org/aspen_ddp/aspen/server/Host.scala`. Expected: no output.
- [ ] **Step 5:** Run `grep -rn "given ExecutionContext\|implicit.*ExecutionContext\|ExecutionContext.global\|Implicits.global"
      src/main/scala/org/aspen_ddp/aspen/server/host/`. Expected: only `DeviceControl`'s
      `given ExecutionContext = threading.control`.
- [ ] **Step 6:** Run `sbt 'testOnly *ControlContextSuite'`, then `sbt test`. Expected: all pass, with no test file
      changed.
- [ ] **Step 7:** Commit: `host: device management runs on the control context without locks`.

---

## Phase 3: Rewrite the Host suites against the new classes (Tasks 11–13)

**Coverage rule (spec section 7):** every behaviour a current suite asserts is still asserted. A test may be dropped
only when it checks a hook rather than host behaviour, and each one dropped is named in the commit message.

### Task 11: `ControlFixture`; remove Host's test seams and hooks

**Files:**
- Create: `src/test/scala/org/aspen_ddp/aspen/server/host/ControlFixture.scala`,
  `src/test/scala/org/aspen_ddp/aspen/server/host/ManualControlScheduler.scala`
- Modify: `server/Host.scala` (add constructor parameter `seams: Host.Seams = Host.Seams.default`. The adapters from
  Phase 1 use a seam when it is given and fall back to the `protected def` otherwise, so `RecordingHost` keeps working
  until Task 13 deletes it), `TestNetwork.scala`

**Interfaces:**
- Produces:
  - `final case class Host.Seams`, with:
    - `metadata: Option[DeviceMetadata]`, `scheduler: Option[ControlScheduler]`, `backends: Option[BackendOpener]`
    - `warn: Option[String => Unit]`
    - `closeForTransferOut: Option[StoreId => Future[Unit]]`, `closeForShutdown: Option[StoreId => Future[Unit]]`
    - `gossipStoreOnline: Option[StoreId => Unit]`
    - `None` means the production implementation.
  - `def Host.control: DeviceControl` is `private[aspen]`, for tests to reach components.
  - `class ManualControlScheduler extends ControlScheduler`, with:
    - `def pending: List[Duration]`
    - `def runAll(): Unit`, which runs every scheduled task posted to `controlEvents`, as production does
    - `def runWhere(p: Duration => Boolean): Unit`
    - This replaces `testingOnlyRetryTransferIns` and `testingOnlyRunDeviceRechecks` (run the matching delay), and
      the `scheduleShutdownWorkTimeout`/`scheduleEndpointRetry` overrides.
  - `trait ControlFixture` (mixes into `IntegrationTestSuite`, like `HostTestHarness`, which it replaces):
    - today's helpers `newHostDir`, `writeDevice`, `deviceState` and `completeRebuilds`, moved as they are
    - `newHost(...)` returns a `Host` built with `LockstepThreading` and `Seams` that record the calls the old
      `RecordingHost` recorded (the same counters and promises, moved into a `RecordingSeams` class)
    - Component state is read through `host.control.discovery.loadedDeviceDirs` and the like (the `private[host]`
      accessors from Tasks 3–7; widen them to `private[aspen]`).
    - Hooks that ran an event synchronously (`testingOnlyCheckAllDevices`, `testingOnlyScanDevices`,
      `testingOnlyHandleHostMessage`, `testingOnlyHandleMarkerResolutions`) become direct calls
      (`host.control.reconciler.checkAll()` and the like). This works because a component can now be called without
      draining unrelated queued work.
    - `testingOnlyQueuedMarkerResolutions` becomes a count of what `discovery.pendingMarkerResolutions` holds.
- [ ] **Step 1:** Write `ControlFixture`, `RecordingSeams` and `ManualControlScheduler`. Leave the old hooks in place.
- [ ] **Step 2:** Move `HostEndpointSuite`, `HostRepairTargetSuite`, `RepairServiceSuite`,
      `FailedStorageDeviceSuite`, `AbortStoreTransferSuite`, `SetRebalanceDurableTaskSuite`, `MigratePoolToSetSuite`
      and `RebuildingStoreSuite` onto `ControlFixture`. Each has 13 or fewer hook uses.
- [ ] **Step 3:** Run `sbt 'testOnly *HostEndpointSuite *HostRepairTargetSuite *RepairServiceSuite
      *FailedStorageDeviceSuite *AbortStoreTransferSuite *SetRebalanceDurableTaskSuite *MigratePoolToSetSuite
      *RebuildingStoreSuite'`. Expected: all pass, with the same test count as before.
- [ ] **Step 4:** Run `sbt test`. Expected: all pass.
- [ ] **Step 5:** Commit: `host tests: ControlFixture and Host.Seams; first suites moved off Host hooks`.

### Task 12: Rewrite `HostDeviceDiscoverySuite`

**Files:** Modify `src/test/scala/org/aspen_ddp/aspen/server/HostDeviceDiscoverySuite.scala` (173 hook uses). Move it
to `src/test/scala/org/aspen_ddp/aspen/server/host/` if its tests now target `DeviceDiscovery` and
`DeviceReconciler`.

- [ ] **Step 1:** Record the test count: `sbt 'testOnly *HostDeviceDiscoverySuite'`.
- [ ] **Step 2:** Replace each hook with its `ControlFixture` / component equivalent.
- [ ] **Step 3:** Run the suite. Expected: all pass with the same count, or fewer only by tests named in the commit
      message under the coverage rule.
- [ ] **Step 4:** Commit: `host tests: HostDeviceDiscoverySuite targets DeviceDiscovery and DeviceReconciler`.

### Task 13: Rewrite the remaining suites and delete the old harness

**Files:** Modify `HostFsifLoadSuite.scala` (81 hook uses; its `openFsifBackend`, `fsifOpenEc`,
`lookupStorageDeviceState` and `gossipStoreOnline` overrides become `Seams`), `DeviceShutdownSuite.scala`,
`DeviceAdoptionSuite.scala`, `StoreRebuildTriggerSuite.scala`, `HostCrlSaveLoadSuite.scala`, `FsifIntegrationSuite.scala`
(`FsifTestHost` becomes a seam configuration), `TestNetwork.scala`. Delete `HostTestHarness.scala`. In
`server/Host.scala`, delete every delegate `protected def` listed in the Phase 1 rules, the adapters' fallbacks to
them, and every `testingOnly*` hook except `testingOnlyHandleEvents`.

- [ ] **Step 1:** Record each suite's test count.
- [ ] **Step 2:** Rewrite each suite against `ControlFixture`.
- [ ] **Step 3:** Run `grep -rn "testingOnly" src/test src/main | grep -v testingOnlyHandleEvents`. Expected: no Host
      hooks remain. Hooks on other classes that already existed before this work may stay.
- [ ] **Step 4:** Run `sbt test`. Expected: all pass, with test counts unchanged except tests named in commit
      messages.
- [ ] **Step 5:** Commit: `host tests: remaining suites target the new classes; HostTestHarness removed`.

---

## Phase 4: Store threads (Tasks 14–15)

### Task 14: `StoreThreadPool`, the default strategy

**Files:**
- Create: `server/host/StoreThreadPool.scala`
- Test: `src/test/scala/org/aspen_ddp/aspen/server/host/StoreThreadPoolSuite.scala`

**Interfaces:**
- Produces `class StoreThreadPool(maxThreads: Int = Runtime.getRuntime.availableProcessors()) extends HostThreading`:
  - `control` and `controlEvents` are one non-daemon `aspen-host-control` thread, reusing the queue/loop class from
    `SingleThreadThreading`, which is extracted as `SerialLoop(name: String, daemon: Boolean)`.
  - `dispatch` is an inline executor.
  - `assign` returns the `SerialLoop` with the fewest assigned stores, ties going to the lowest index. A new loop
    (`aspen-store-N`, daemon) is created only when every existing loop has at least one store and fewer than
    `maxThreads` exist.
  - `release` decrements the count of that store's loop. Loops are never destroyed before `shutdown()`.
  - `private[host] def threadCount: Int` and `private[host] def assignedCounts: Vector[Int]` are exposed for tests.
- [ ] **Step 1: Write the failing tests** (`AnyFunSuite`).
  - `threads are created lazily`: `maxThreads = 4`; assign 2 stores; `threadCount == 2`.
  - `never more threads than maxThreads`: `maxThreads = 4`; assign 10; `threadCount == 4`;
    `assignedCounts.sorted == Vector(2, 2, 3, 3)`.
  - `least-loaded placement`: `maxThreads = 2`; assign s1, s2, s3; release s1; assign s4; s4 lands on s1's loop;
    `assignedCounts == Vector(2, 2)`.
  - `release frees a slot for the next store`: `maxThreads = 1`; assign and release the same store 100 times;
    `threadCount == 1` and `assignedCounts == Vector(0)`.
  - `store threads are daemon threads named aspen-store-N`: execute a task that records
    `Thread.currentThread.getName` and `isDaemon`; expect `"aspen-store-0"`, `true`.
  - `tasks of one store never overlap and run in posting order`:
    - `maxThreads = 4`, 16 stores, 8 poster threads each posting 2,000 tasks spread over the stores.
    - Each task checks an `AtomicBoolean` "in use" flag per store: it fails the test if the flag is already set,
      sets it, records the poster's sequence number, then clears it.
    - Assert no overlap was seen and that each (poster, store) sequence is increasing.
    - Wait with a `CountDownLatch`.
  - `a throwing task does not kill its store thread`: a throwing task, then a recording task; the second runs.
- [ ] **Step 2:** Run `sbt 'testOnly *StoreThreadPoolSuite'`. Expected: FAIL.
- [ ] **Step 3:** Implement `SerialLoop` and `StoreThreadPool`.
- [ ] **Step 4:** Run the suite 20 times under load to rule out flakiness:
      `for i in $(seq 20); do sbt -batch 'testOnly *StoreThreadPoolSuite' || break; done`. Expected: 20 passes.
- [ ] **Step 5:** Commit: `host: StoreThreadPool default strategy (lazy, least-loaded, daemon store threads)`.

### Task 15: Route the hot path to store executors; install/close protocol; `Main` uses the default

**Files:**
- Modify: `server/host/StoreRouter.scala`, `server/host/StoreLoader.scala`, `server/Host.scala`,
  `server/SimpleDriverRecoveryMixin.scala`, `cmdline/Main.scala`
- Test: `src/test/scala/org/aspen_ddp/aspen/server/host/StoreRouterSuite.scala`,
  `src/test/scala/org/aspen_ddp/aspen/server/host/ThreadedHostSuite.scala`
- Modify: `TestNetwork.scala` (optional constructor parameter `threading: ExecutionContext => HostThreading =
  new LockstepThreading(_)`, used only by `ThreadedHostSuite`)

**Interfaces:**
- `StoreRouter`:
  - `stores` becomes a `java.util.concurrent.ConcurrentHashMap[StoreId, StoreRouter.Entry]`, where
    `Entry(store: Store, executor: StoreExecutor)`; `offline` becomes a `ConcurrentHashMap.newKeySet[StoreId]`.
    Only the control thread writes either.
  - New method `def post(storeId: StoreId)(task: Store => Unit): Unit`. It runs `threading.dispatch.execute { lookup;
    entry.executor.execute { if the store is still installed then task(store) } }`. Every `deliver*`, `repair*`,
    completion, `StoreCrlLoaded` and `UnloadFailedStore` path uses it.
  - The unknown-store and offline replies happen inside the dispatch step, so in lockstep they still happen when the
    item is drained.
  - `heartbeat()` runs one `dispatch` step that posts `store.heartbeat()` to each entry's executor.
  - Each backend gets its own `CompletionHandler` that calls `router.deliverCompletion`. The host-wide `IOHandler` is
    gone.
- `StoreLoader`, following spec section 5:
  - **Install:** call `threading.assign(storeId, deviceId)`. On the control thread, construct the `Store`, set its
    completion handler and, when there is CRL state, call `frontend.awaitRecovery()`. All of this happens before the
    store is published, when no other thread can reach it. Then call `router.install`, the same step today's
    `LoadStore` case performs. Ownership passes to the executor at that moment: the `ConcurrentHashMap` put is the
    happens-before edge, and from then on only the executor touches the store. This keeps today's guarantee that no
    message finds the store missing between construction and registration. In lockstep it is all inline, so the order
    is today's. Note this refinement of spec section 5 in the commit message.
  - **Close:** call `router.remove`, which marks the store offline, then post the existing `ShutdownStore` sequence to
    the executor. On completion, call `threading.release(storeId)` on the control context.
- `SimpleDriverRecoveryMixin.handleRecoveryEvent` posts a per-store task through `router.post`; it never walks
  `frontend.transactions` from another thread.
- `Main.scala` passes `new StoreThreadPool()`.
- [ ] **Step 1: Write the failing tests** in `StoreRouterSuite` (an `AnyFunSuite` with `StoreThreadPool(maxThreads =
      2)`, `MapBackend` stores, and a recording `Messenger`).
  - `close racing traffic gets offline handling`:
    - Install store s0. From 4 threads, send 5,000 `Read`s to s0 while the control thread closes it after the
      first 1,000 arrive.
    - Every `Read` either gets a normal `ReadResponse` or is dropped as offline. None reaches a closed `Store`: put a
      check in a test `Frontend` hook, or assert that no exception is logged after the close completes.
    - No `StoreNotFound` reply is sent, because the store is offline, not unknown.
  - `messages during CRL recovery are held until recover`:
    - Load a store whose `crl_save.log` holds one transaction.
    - Deliver a `TxAccept` for it before `crl.loadStore` completes, using a CRL stub with a gate `Promise`.
    - Assert no `TxAcceptResponse` is sent until the gate opens, and that one is sent after.
  - `unknown store gets TxUnknownStore from the calling thread`: deliver a `TxPrepare` for an unregistered store; the
    recorded reply's thread is the caller's.
- [ ] **Step 2: Write the failing test** `ThreadedHostSuite`, an `IntegrationTestSuite` whose `TestNetwork` uses
      `new StoreThreadPool(maxThreads = 3)`, overriding the fixture setup.
  - `concurrent transactions commit on store threads`: run 200 single-object overwrite transactions on 20 objects
    from `Future.traverse`, then read every object back. The final contents equal the last committed write of each
    object.
  - Assertions are on outcomes only; there are no timing assertions.
  - `TestNetwork.handleEvents()` must be a no-op for non-lockstep threading. Make `Host.testingOnlyHandleEvents()`
    return quietly instead of throwing when the threading is not lockstep, and say so in its scaladoc.
- [ ] **Step 3:** Run `sbt 'testOnly *StoreRouterSuite *ThreadedHostSuite'`. Expected: FAIL.
- [ ] **Step 4:** Implement the interfaces above.
- [ ] **Step 5:** Run both suites 20 times under load:
      `for i in $(seq 20); do sbt -batch 'testOnly *StoreRouterSuite *ThreadedHostSuite' || break; done`. Expected:
      20 passes.
- [ ] **Step 6:** Run `sbt test`. Expected: all pass. The integration suites are unchanged.
- [ ] **Step 7:** Commit: `host: stores run on their own executors; Main uses StoreThreadPool`.

---

## Phase 5: Store-side cleanup, documentation and measurement (Task 16)

### Task 16: Remove redundant store locks, finish the docs, write the measurement procedure

**Files:**
- Modify: `server/store/Store.scala`, `server/store/Frontend.scala` (only if it has locks), `CLAUDE.md`, `Todo.txt`
- Create: `docs/host-threading-measurement.md`

- [ ] **Step 1:** In `Store`, remove `synchronized` from methods now called only on the store's executor:
      `driveTransaction`, `repair`, `repairDelete`, `receiveTransactionMessage`, and `heartbeat`'s block around
      `checkCollisionWait`.
      - Keep it on `hasTransactions` and `logTransactionStatus`. Make `transactionDrivers` a `@volatile` immutable
        `Map`, so those two may read it from `TestNetwork`'s checker thread and from `Host.hasTransactions`. Add a
        comment saying that is why.
      - Keep any other block that a `TransactionDriver` timer thread can reach, with a comment.
- [ ] **Step 2:** Run `sbt test`, then run `ThreadedHostSuite` and `StoreRouterSuite` 20 times as in Task 15 Step 5.
      Expected: all pass.
- [ ] **Step 3:** Finish `CLAUDE.md`'s "Host threading model" section: `StoreThreadPool` is the default (lazy, at most
      one thread per CPU, least-loaded, daemon `aspen-store-N`); the strategy is chosen in code; `HostStoreView`.
      Add one line to "Key Architectural Components" pointing at `server.host`.
- [ ] **Step 4:** Update `Todo.txt`:
      - In the "Store commit delay" entry, note that the event loop is split (`host-refactor`) and that the next step
        is the measurement in `docs/host-threading-measurement.md`.
      - Add a new entry: "`HostObjectCache` is one `synchronized` cache shared by every store; with stores on parallel
        threads it is the likely next contention point (see `docs/superpowers/specs/2026-11-05-host-refactor-design.md`)".
- [ ] **Step 5:** Write `docs/host-threading-measurement.md`. It gives the before/after procedure on the same hardware
      as the `Todo.txt` runs (16-store RS 16/10/12 pool, one host, all 16 stores on one disk):
      - Build `main` (before) and `host-refactor` (after).
      - Run the `scripts/vdisk-e2e/` fio randrw and seqwrite jobs used for the `Todo.txt` numbers.
      - Record BackendTiming's "store commit (FsifBackend)" and "fsif commit" averages over the same 60 s window.
      - Take 3 thread dumps 10 s apart and record per-thread CPU for `aspen-host-control` and each `aspen-store-N`.
      - There is no numeric target. If the commit figure does not improve, add the dumps' findings to `Todo.txt` as the
        next investigation.
- [ ] **Step 6:** Commit: `store: drop locks made redundant by store executors; document host threading and
      measurement`.

---

## Self-review notes

- Spec sections 1–8 map to Tasks 9/14 (threading), 10 (control context and documentation), 14 (default strategy),
  15 (router, lifecycle), 9/15 (lockstep, including the dispatch hop and heartbeat), 1–8 and 11–13 (the split and
  coverage), and 16 (store locks, follow-ups, measurement). The `@volatile` backend flags (spec section 5) need no
  change: `FsifBackend.fatal` and `frozen` are already `@volatile`.
- Names used across tasks: `DeviceControl`, `ctx`, `HostThreading.{control, controlEvents, dispatch, assign,
  release, shutdown}`, `StoreExecutor`, `LockstepThreading.drain`, `SerialLoop`, `StoreRouter.post`, `HostStoreView`,
  `publishView`, `Host.Seams`, `ManualControlScheduler`, `ControlFixture`.
