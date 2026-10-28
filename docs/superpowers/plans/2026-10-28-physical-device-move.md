# Physical Device Move Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let an operator shut a storage device down on its host, physically move the disk, and have another host adopt it with no data movement, plus a way to bring a shut-down device back on its original host.

**Architecture:** A device-level `DeviceStatus` (`Operational`/`ShuttingDown`/`Shutdown`/`Failed`) and a store-level `Offline` status drive everything. Clients only write metadata (`shutdownStorageDevice`, `restartStorageDevice`). The owning `Host` reacts in its existing device reconcile: it closes stores the way transfer-out does, writes `shutdown.state`, and marks the device `Shutdown`. Every host decides whether to load a device from two small files on the device (`owning-host`, `shutdown.state`) before opening any store, and adopts a properly shut-down foreign device in one transaction. A cheap 10-second directory scan, which skips directories it has already loaded, finds disks as they arrive.

**Tech Stack:** Scala 3, sbt, ScalaPB (`src/main/protobuf/codec.proto`), os-lib, ScalaTest (`IntegrationTestSuite`, `HostTestHarness`).

**Spec:** `docs/superpowers/specs/2026-10-28-physical-device-move-design.md`

## Global Constraints

- Only `Active` stores change status: `Active → Offline` at shutdown request, `Offline → Active` at restart/adoption. `Initializing`, `Rebuilding`, `TransferringIn`, `TransferringOut` entries (and their `transferDevice`/`transferId`) are never rewritten by this feature.
- A device owned by another host without a valid `shutdown.state` is never loaded and never claimed. The auto-claim in `reconcileDeviceState` is removed.
- No store of a device is opened before the load decision is made.
- The new directory scan runs every 10 seconds. The hourly `CheckAllDevices` reconcile stays.
- Problems with a device directory are logged once per process per directory, in memory only, and rechecked silently on every scan.
- `shutdown-device` and `restart-device` are idempotent and always resend the nudge.
- Backward compatibility with persisted data is not required.
- Scala 3 quiet syntax. Indented if/then/else. os-lib for filesystem operations.
- Tests: correct, never flaky, as fast as possible. Drive hosts through `testingOnly*` hooks, never timers. Full test runs use a 50 s timeout.

## Builder decisions taken in this plan (spec "left to the builder")

- **Per-store marker:** reuse `StoreTransfer.TransferOutMarkerFile` (`transferring-out`), with empty content for a shutdown. `reconcileDeviceState`'s abandoned-transfer-out pass only fires for `Active` entries, so it cannot misfire against `Offline` stores. Restart and adoption remove the marker explicitly before loading, and that pass is the crash backstop.
- **`shutdown.state`:** plain text, the HostId UUID of the host that shut the device down.
- **Decide before load without the network:** a new device-local file `owning-host` (HostId UUID text) records which host owns the device. It is written at device creation and atomically rewritten at adoption. When `shutdown.state` is absent, it lets the load decision stay synchronous and network-free, which matters because a host may need its own devices loaded before it can read any metadata. Only a device carrying `shutdown.state` needs a `StorageDeviceState` lookup before loading.
- **Tombstone:** sets `status = Failed` and keeps zeroing both ids (so nothing addresses a dead host). `isFailed` reads only the status.
- **Rebuild on shutdown:** `StoreRebuild.cancel()` trips `RebuildingStore`'s existing abort latch. The checkpoint stays in staging, exactly as a crash leaves it.
- **Load table refinement:** a device whose `shutdown.state` names this host and whose status is `ShuttingDown` is loaded, not ignored. That is a crash between writing the marker and committing `Shutdown`, and the normal reconcile finishes the shutdown. The spec's "ignore" applies to `Shutdown`.

## Review Focus

1. A second `shutdown-device` (or the hourly check) arriving while the host's shutdown is still closing stores should not start a second shutdown. One shutdown runs per device (Task 7 test).
2. A host process that crashes mid-shutdown should finish the shutdown after restart, whether or not `shutdown.state` was already written (Task 8 test).
3. `restart-device` issued while the host is still shutting the device down should cancel the shutdown and bring the stores back, not leave the device `Shutdown` (Task 7 test).
4. A store still `Initializing` when shutdown is requested should not be created while the device is shutting down. It should be created after restart/adoption (Task 7 test).
5. A disk pulled after `shutdown.state` was written but before `Shutdown` committed, then plugged into another host, should be refused there and logged once (Task 8 test).

---

### Task 1: Device status and Offline store status

**Files:**
- Modify: `src/main/protobuf/codec.proto:700-726`
- Modify: `src/main/scala/org/aspen_ddp/aspen/common/metadata/StorageDeviceState.scala`
- Modify: `src/main/scala/org/aspen_ddp/aspen/common/network/Codec.scala:1381-1437`
- Modify: `src/main/scala/org/aspen_ddp/aspen/common/metadata/management/FailedStorageDeviceDurableTask.scala:206`
- Modify: `src/main/scala/org/aspen_ddp/aspen/cmdline/Main.scala:2331` (`formatDeviceState`)
- Test: `src/test/scala/org/aspen_ddp/aspen/common/network/CodecRoundTripSuite.scala`, `src/test/scala/org/aspen_ddp/aspen/common/metadata/management/FailedStorageDeviceSuite.scala`, `src/test/scala/org/aspen_ddp/aspen/cmdline/MainSuite.scala`

**Interfaces:**
- Produces:
  - `enum StorageDeviceState.DeviceStatus: case Operational, ShuttingDown, Shutdown, Failed`
  - `StorageDeviceState.StoreStatus.Offline`
  - `StorageDeviceState(..., storageDeviceSet, status: DeviceStatus = DeviceStatus.Operational)`: new last field with a default
  - `def isFailed: Boolean` (= `status == Failed`), `def isOperational: Boolean`
  - `def withActiveStoresOffline: StorageDeviceState` (every `Active` entry → `Offline`, all else untouched)
  - `def withOfflineStoresActive: StorageDeviceState` (every `Offline` entry → `Active`)
  - proto: `enum StorageDeviceStatus { STORAGE_DEVICE_STATUS_OPERATIONAL = 0; ..._SHUTTING_DOWN = 1; ..._SHUTDOWN = 2; ..._FAILED = 3; }`, `StorageDeviceState.status = 7`, `STORAGE_DEVICE_STORE_STATUS_OFFLINE = 5`

- [ ] **Step 1: Write failing tests**
  - `CodecRoundTripSuite`: `"StorageDeviceState round-trips every DeviceStatus and the Offline store status"`. For each `DeviceStatus`, encode/decode a state with one `Offline` store and assert equality.
  - `StorageDeviceState` unit tests (add to `CodecRoundTripSuite` or a new `StorageDeviceStateSuite` in `common/metadata`):
    - `withActiveStoresOffline` on `{a: Active, b: Rebuilding, c: TransferringOut(dev, id), d: TransferringIn, e: Initializing}` changes only `a`, and preserves `c`'s `transferDevice`/`transferId`.
    - `withOfflineStoresActive` is its inverse on `a` and leaves the others unchanged.
    - `isFailed` is true only for `status = Failed`. In particular, a state with zeroed ids but `Operational` status is not failed.
  - `FailedStorageDeviceSuite`: after `failStorageDevice` and the tombstone step, the tombstoned state has `status == DeviceStatus.Failed`.
  - `MainSuite`: `formatDeviceState` output contains `"  Status:     ShuttingDown"` for a `ShuttingDown` device, and store lines render `Offline`.

- [ ] **Step 2: Run them to make sure they fail**

Run: `sbt 'testOnly *CodecRoundTripSuite *StorageDeviceStateSuite *FailedStorageDeviceSuite *MainSuite'`
Expected: compile failure (no `DeviceStatus`, no `Offline`).

- [ ] **Step 3: Implement**

Add the proto entries and the codec mapping (`Unrecognized` throws `EncodingError`, as for store status). The tombstone `copy` also sets `status = DeviceStatus.Failed`. In `formatDeviceState`, add the `Status:` line directly after `Storage Device:`. Fix any exhaustive `match` on `StoreStatus` that now warns or fails to compile. Update the comments that describe failure as "zeroed ids" (`StorageDeviceState.isFailed`, `AspenClient.failStorageDevice`) to say the status is authoritative.

- [ ] **Step 4: Run the tests and make sure they pass**

Run: same as Step 2. Expected: PASS. Then `sbt test` (50 s timeout): PASS.

- [ ] **Step 5: Commit**

```bash
git commit -am "Add StorageDeviceState.DeviceStatus and StoreStatus.Offline"
```

---

### Task 2: Never choose a non-Operational device as a destination

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/client/AspenClient.scala` (error class, `createPool` ~349-356, `transferStore` ~402)
- Modify: `src/main/scala/org/aspen_ddp/aspen/client/internal/BaseAspenClient.scala:529` (`stageDeviceUpdate`)
- Modify: `src/main/scala/org/aspen_ddp/aspen/common/metadata/StorageDeviceSetState.scala:178,201`
- Modify: `src/main/scala/org/aspen_ddp/aspen/common/rebalancing/SetRebalanceDurableTask.scala:244` (`usable`)
- Modify: `src/main/scala/org/aspen_ddp/aspen/common/metadata/management/FailedStorageDeviceDurableTask.scala:447`
- Test: `src/test/scala/org/aspen_ddp/aspen/client/TombstoneGuardSuite.scala` (extend), `src/test/scala/org/aspen_ddp/aspen/common/metadata/StorageDeviceSetIntegrationSuite.scala`, `src/test/scala/org/aspen_ddp/aspen/common/rebalancing/SetRebalanceDurableTaskSuite.scala`

**Interfaces:**
- Consumes: `DeviceStatus`, `isOperational` (Task 1)
- Produces: `class AspenClient.DeviceNotOperational(deviceId: StorageDeviceId) extends Exception(s"Storage device ${deviceId.uuid} is not operational")`

- [ ] **Step 1: Write failing tests** (a test helper that rewrites a device's state in one transaction is useful here. Put it in `IntegrationTestSuite` or a shared test trait as `setDeviceState(id)(f: StorageDeviceState => StorageDeviceState): Future[Unit]`, since Tasks 3, 7 and 8 reuse it)
  - `"transferStore onto a ShuttingDown device fails with DeviceNotOperational"`: also check that a `Shutdown` destination fails the same way, and that the source entry is unchanged.
  - `"selectDeviceForStore skips a Shutdown device"`: two-device set, one `Shutdown`. Selection always returns the other one, and fails with `AllocationError` when only the shut-down device has room.
  - `"selectDeviceForRebuild skips a ShuttingDown device"`: same shape.
  - `"rebalance does not treat a Shutdown device as usable"`: `usable` returns false. A planned transfer to it is re-targeted or left pending, never started.
  - `"createPool avoids a Shutdown member device"`: a set of width+1 devices with one `Shutdown`. The pool's stores never name it. With exactly width devices, one of them `Shutdown`, creation fails with `DeviceNotOperational`.

- [ ] **Step 2: Run to verify failure**

Run: `sbt 'testOnly *TombstoneGuardSuite *StorageDeviceSetIntegrationSuite *SetRebalanceDurableTaskSuite'`
Expected: the new tests FAIL.

- [ ] **Step 3: Implement**
  - `transferStore`: right after the `isFailed` check, `if !dstState.isOperational then throw DeviceNotOperational(destinationId)`.
  - `StorageDeviceSetState`: both `freeSpaceLookup` lambdas fail (`Future.failed(DeviceNotOperational(id))`) for a non-operational device. `selectDeviceWithSpace` already skips a candidate whose lookup fails.
  - `SetRebalanceDurableTask.usable`: add `&& device.isOperational`.
  - `createPool`: if any selected device is not operational, re-run selection once with those devices excluded. To do that, add `exclude: Set[StorageDeviceId] = Set.empty` to the public `selectDevicesForPool` and pass it to `selectDevices`, whose soft exclusion prefers the others, re-read the states, and fail with `DeviceNotOperational` if a non-operational device is still selected. `stageDeviceUpdate` gets the same check after its `isFailed` check, for the race.
  - `FailedStorageDeviceDurableTask:447`: also throw (and so retry selection) when the destination is not operational.

- [ ] **Step 4: Run tests to verify pass.** Same command, then `sbt test`. Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git commit -am "Never place, rebuild, or transfer stores onto a non-Operational device"
```

---

### Task 3: Client `shutdownStorageDevice` / `restartStorageDevice` and CLI commands

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/client/AspenClient.scala` (next to `abortStoreTransfer`)
- Modify: `src/main/scala/org/aspen_ddp/aspen/cmdline/Main.scala` (`Command` list ~758, dispatch ~902, new `shutdown_device` / `restart_device` modelled on `fail_storage_device`, args reuse `failStorageDeviceArgs`)
- Create: `src/test/scala/org/aspen_ddp/aspen/client/DeviceShutdownClientSuite.scala`
- Test: `src/test/scala/org/aspen_ddp/aspen/cmdline/MainSuite.scala`

**Interfaces:**
- Consumes: `withActiveStoresOffline`, `withOfflineStoresActive`, `DeviceStatus` (Task 1), `setDeviceState` test helper (Task 2)
- Produces:
  - `def shutdownStorageDevice(deviceId: StorageDeviceId): Future[Unit]`
  - `def restartStorageDevice(deviceId: StorageDeviceId): Future[Unit]`
  - CLI: `shutdown-device <client-config> <device-uuid>`, `restart-device <client-config> <device-uuid>`

- [ ] **Step 1: Write failing tests** in `DeviceShutdownClientSuite` (`IntegrationTestSuite`, nudges captured as in `AbortStoreTransferSuite.takeNudges`). Use `net.createSecondDevice()` and give it entries with `setDeviceState`:
  - `"shutdownStorageDevice marks only Active stores Offline and the device ShuttingDown"`: entries `{Active, Rebuilding, TransferringOut(d,id), Initializing}` → `{Offline, Rebuilding, TransferringOut(d,id), Initializing}`, `status == ShuttingDown`. Exactly one `CheckStorageDevice(dev.hostId, client.clientId, deviceId)` nudge.
  - `"shutdownStorageDevice on a ShuttingDown or Shutdown device writes nothing and still nudges"`: the state is unchanged (`after == before`) and one nudge is sent.
  - `"shutdownStorageDevice on a Failed device fails with DeviceFailed"`: no nudge.
  - `"restartStorageDevice returns Offline stores to Active and the device to Operational"`: from `Shutdown`, and also from `ShuttingDown`. One nudge.
  - `"restartStorageDevice on an Operational device writes nothing and still nudges"`.
  - `"restartStorageDevice on a Failed device fails with DeviceFailed"`.
  - `MainSuite`: `shutdown-device` and `restart-device` parse a device UUID into `cfg.deviceId`.

- [ ] **Step 2: Run to verify failure**

Run: `sbt 'testOnly *DeviceShutdownClientSuite *MainSuite'`. Expected: compile failure.

- [ ] **Step 3: Implement**

Both methods follow `abortStoreTransfer`'s shape: `transactUntilSuccessfulWithRecovery(StopRetrying.unlessCollision)`, a `KeyRevision` on `StorageDeviceState.StateKey`, and the nudge built inside the transaction and sent after commit with `sendBestEffortHostMessage`. When there is nothing to write, the empty transaction commits immediately and the nudge is still sent. A `Failed` device → `StopRetrying(DeviceFailed(deviceId))`. Each CLI command prints the device's new status and tells the operator to watch `show-device`, and to re-run the command if it does not progress. For shutdown it adds: the disk may be removed once the status reads `Shutdown`.

- [ ] **Step 4: Run tests to verify pass.** Same command, then `sbt test`. Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git commit -am "Add shutdownStorageDevice/restartStorageDevice and their CLI commands"
```

---

### Task 4: Device-local ownership files

**Files:**
- Create: `src/main/scala/org/aspen_ddp/aspen/server/DeviceMarkers.scala`
- Modify: `src/main/scala/org/aspen_ddp/aspen/server/StorageDeviceManager.scala:~104` (write `owning-host` after the config file)
- Modify: `src/main/scala/org/aspen_ddp/aspen/cmdline/Main.scala:1495` (bootstrap device)
- Modify: `src/test/scala/org/aspen_ddp/aspen/server/HostTestHarness.scala:125` (`writeDeviceConfig` gains `owner: Option[HostId] = Some(HostId.BootstrapHostId)`)
- Create: `src/test/scala/org/aspen_ddp/aspen/server/DeviceMarkersSuite.scala`
- Test: `src/test/scala/org/aspen_ddp/aspen/server/StorageDeviceManagerSuite.scala`

**Interfaces:**
- Produces, in `object DeviceMarkers`:
  - `val OwningHostFile = "owning-host"`, `val ShutdownStateFile = "shutdown.state"`
  - `def readOwningHost(devicePath: os.Path): Option[HostId]` (None if absent; throws on malformed content)
  - `def writeOwningHost(devicePath: os.Path, hostId: HostId): Unit` (write a temp file in `devicePath`, then `os.move(..., replaceExisting = true, atomicMove = true)`)
  - `def readShutdownState(devicePath: os.Path): Option[HostId]`, `def writeShutdownState(devicePath: os.Path, hostId: HostId): Unit` (same atomic write), `def removeShutdownState(devicePath: os.Path): Unit`

- [ ] **Step 1: Write failing tests**
  - `DeviceMarkersSuite`: round-trip for both files, absent → `None`, malformed content → throws, overwrite replaces, no temp file left behind after a write.
  - `StorageDeviceManagerSuite`: after `createStorageDevice`, `readOwningHost(devDir) == Some(hostConfig.hostId)`.

- [ ] **Step 2: Run to verify failure:** `sbt 'testOnly *DeviceMarkersSuite *StorageDeviceManagerSuite'`. Expected: compile failure.

- [ ] **Step 3: Implement** the object and the two production writers. The bootstrap writer uses the bootstrap host's HostId. Correct the stale scaladoc in `createStorageDevice` ("Host validates only aspenSystemId, never hostId").

- [ ] **Step 4: Run tests to verify pass**, then `sbt test`. Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git commit -am "Record the owning host and shutdown state in files on the device"
```

---

### Task 5: Cancelling a running rebuild or transfer-out

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/server/rebuild/StoreRebuild.scala`, `.../rebuild/RebuildingStore.scala`
- Modify: `src/main/scala/org/aspen_ddp/aspen/server/transfer/StoreTransfer.scala`, `.../transfer/StoreTransferManager.scala`
- Modify: test doubles that implement these traits (`HostTestHarness.scala` `StagedTransferService`/`StagedSendService`, recording rebuild factories in `StoreRebuildTriggerSuite` etc.). Compile errors list them.
- Test: `src/test/scala/org/aspen_ddp/aspen/server/rebuild/RebuildingStoreSuite.scala` (or the existing RebuildingStore suite), `src/test/scala/org/aspen_ddp/aspen/server/transfer/StoreTransferManagerSuite.scala` (existing manager suite)

**Interfaces:**
- Produces:
  - `trait StoreRebuild: def cancel(): Unit`. Afterwards `complete` fails with `RebuildingStore.RebuildCancelled(storeId)`, the backend is closed, and staging and its checkpoint are left in place.
  - `trait StoreTransferService: def cancelTransferOut(storeId: StoreId): Unit`. Aborts the active send for the store, if any, after which `isTransferringOut(storeId)` is false. `Disabled` does nothing.

- [ ] **Step 1: Write failing tests**
  - `"cancel stops a rebuild and leaves its checkpoint for resumption"`: cancel mid-walk. `complete` fails with `RebuildCancelled`, `RebuildState.load(stagingPath)` is `Some`, the final path does not exist, and a new `RebuildingStore` on the same paths resumes and completes.
  - `"cancelTransferOut aborts an active send"`: afterwards `isTransferringOut` is false and the receiver's attempt fails.

- [ ] **Step 2: Run to verify failure.** Expected: compile failure.

- [ ] **Step 3: Implement.** `RebuildingStore.cancel` sets `abortCause` (under the instance lock) to `RebuildCancelled`, so it is reported the same way as the existing fatal latch. `cancelTransferOut` uses `abortOutbound` on `active.get(storeId)`.

- [ ] **Step 4: Run tests to verify pass**, then `sbt test`.

- [ ] **Step 5: Commit**

```bash
git commit -am "Allow running store rebuilds and transfers out to be cancelled"
```

---

### Task 6: Fast device scan, ownership check before load, remove auto-claim

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/server/Host.scala` (constructor, `checkForNewDevices` 354, `tryLoadDevice` 367, `reconcileDeviceState` 1491, `storageDevices` comment 182, events, `handleEvent`)
- Modify: `src/main/scala/org/aspen_ddp/aspen/cmdline/Main.scala:1366` (only if the new parameter needs passing. The default is fine)
- Test: `src/test/scala/org/aspen_ddp/aspen/server/HostDeviceDiscoverySuite.scala`, `HostTestHarness.scala`

**Interfaces:**
- Consumes: `DeviceMarkers` (Task 4)
- Produces:
  - `Host(..., deviceScanPeriod: Duration = Duration(10, SECONDS))`, the last constructor parameter. It schedules `ScanDevices()` via `backgroundTasks.schedulePeriodic`. The handler calls `checkForNewDevices()`.
  - `private var loadedDeviceDirs: Map[os.Path, StorageDeviceId]`. `checkForNewDevices` skips listed paths without reading them, and drops entries whose path is no longer listed.
  - `private var reportedDeviceDirs: Set[os.Path]` and `private def warnOnce(path: os.Path, msg: => String): Unit`. Cleared for a path once it loads successfully.
  - `private[aspen] def testingOnlyScanDevices(): Unit = handleEvent(ScanDevices())`
  - Load decision in `tryLoadDevice`, after the config and system id checks and before listing stores:
    - `shutdown.state` present → do not load. Hand off to `resolveShutdownMarker(path, cfg, markerHost)`, which in this task only records the path as not loaded (Task 8 implements it).
    - otherwise `readOwningHost == Some(hostId)` → load as today.
    - otherwise (other host, absent, or malformed) → `warnOnce`. Not loaded and never claimed.
  - `reconcileDeviceState`: the `remote.hostId != hostId` branch logs an error once ("device owned by host X is loaded here. Not claiming it") and does nothing else. `updateHostId` and RecordingHost's `hostIdClaims` override are removed. Task 8 brings the transaction back as `adoptDevice`.

- [ ] **Step 1: Write failing tests** in `HostDeviceDiscoverySuite`
  - `"the device scan skips directories that are already loaded"`: load a device, then corrupt its config file on disk. `testingOnlyScanDevices()` neither warns nor touches it (`storeLoadAttempts` unchanged), and the device stays loaded.
  - `"a device whose directory disappears is checked again when it reappears"`: load, move the directory out, scan, move it back, scan. The path is back in the skip set, and nothing is double-loaded (the existing duplicate guard).
  - `"a device without an owning-host file is not loaded"`: `writeDeviceConfig(..., owner = None)`. Not loaded. A second scan still does not load it, and the warning is logged once (assert via the test logger or a counter seam: expose `testingOnlyReportedDeviceDirs: Set[os.Path]`).
  - `"a device owned by another host is not loaded or claimed"`: replaces the existing test `"a device owned by another host is claimed with updateHostId"` (line 642). No load, and no transaction touches the device state.
  - `"a device carrying shutdown.state is not loaded by the scan"` (Task 8 extends this).
  - `"the scan runs on the configured period"`: a `newHost` built with a recording `BackgroundTaskManager` registers a periodic task at `Duration(10, SECONDS)`. If the harness has no such seam, assert on the constructor default instead.

- [ ] **Step 2: Run to verify failure:** `sbt 'testOnly *HostDeviceDiscoverySuite'`.

- [ ] **Step 3: Implement** as specified in Interfaces. Update the `storageDevices` comment: entries are now removed when a device shuts down (Task 7). Search the docs (`Architecture.md`, `Operation.md`, `UsageGuide.md`, `README.md`) for wording saying a new device needs a host restart, or saying moved devices are claimed automatically, and fix it.

- [ ] **Step 4: Run tests to verify pass:** the suite, then `sbt test`. Some existing tests write devices with the default owner (Task 4 harness default) and should still pass. Fix any that relied on auto-claim.

- [ ] **Step 5: Commit**

```bash
git commit -am "Scan for devices every 10s, check ownership before loading, stop auto-claiming"
```

---

### Task 7: Owning host shuts a device down

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/server/Host.scala` (`reconcileDeviceState`, `startStoreTransferOut` 1044, the TransferringIn pass ~1630, `startStoreRebuild` failure handling, new methods)
- Modify: `src/test/scala/org/aspen_ddp/aspen/server/HostTestHarness.scala` (RecordingHost seams)
- Create: `src/test/scala/org/aspen_ddp/aspen/server/DeviceShutdownSuite.scala`

**Interfaces:**
- Consumes: Tasks 1, 4, 5, `setDeviceState` helper (Task 2)
- Produces:
  - `private var shuttingDownDevices: Set[StorageDeviceId]`: one shutdown per device at a time.
  - `private def shutDownDevice(local: LocalStorageDeviceState, remote: StorageDeviceState): Unit`: called from `reconcileDeviceState` when `remote.status == ShuttingDown`, instead of the normal passes (no `createNewStore`, no transfer-in start, no rebuild start).
  - `protected def closeStoreForShutdown(storeId: StoreId): Future[Unit]`: default `closeStore(storeId)`. RecordingHost records it.
  - `private[aspen] enum ShutdownOutcome: case Completed, StoresReactivated, Cancelled`
  - `protected def completeDeviceShutdown(storageDeviceId: StorageDeviceId): Future[ShutdownOutcome]`: one transaction on the device state:
    - status not `ShuttingDown` → write nothing, `Cancelled`
    - any `Active` entry → `withActiveStoresOffline` (status unchanged), `StoresReactivated`
    - otherwise status → `Shutdown`, `Completed`
  - `private def unloadDevice(storageDeviceId: StorageDeviceId): Unit`: removes it from `storageDevices` and `loadedDeviceDirs`. Its stores stay in `offlineStores`.

`shutDownDevice` sequence (single-flight on `shuttingDownDevices`; every step is idempotent):
1. Cancel the work running on this device: `rebuildingStores` entries for this device's `Rebuilding` stores → `cancel()`, and await `complete` (failure expected). Transfers in onto this device (entries in `pendingStartTransfers` / `transferInRetries` / `transferringIn` whose store is `TransferringIn` on this device) → forget them and call `storeTransferService.cancelTransferIn`. This device's `TransferringOut` stores that are `isTransferringOut` → `cancelTransferOut`. A rebuild that fails with `RebuildCancelled` is not logged as an error and does not trigger `recheckOtherDevices` for this device.
2. For each store in `local.loadedStores` still in `stores`: `closeStoreForShutdown`, then write `TransferOutMarkerFile` with empty content, then gossip `StoreOffline(storeId, HLCTimestamp.now)`.
3. `DeviceMarkers.writeShutdownState(local.devicePath, hostId)`.
4. `completeDeviceShutdown`:
   - `Completed` → `unloadDevice`.
   - `StoresReactivated` → release the guard and `checkStorageDevice` again, which closes the newly offline stores.
   - `Cancelled` (restart arrived) → `removeShutdownState`, release the guard, and `checkStorageDevice`. The normal reconcile's abandoned-transfer-out pass brings the `Active`-again stores back.
   - Failure → release the guard and leave the device to the next check.

Also in this task:
- `startStoreTransferOut`: refuse (`err`) when the source device state is not operational.
- The reconcile's TransferringIn pass: skip `startStoreTransferIn` when `fromDevice` is not operational. The transfer stays pending, and the next check retries it with a fresh read of the source's owner.

- [ ] **Step 1: Write failing tests** in `DeviceShutdownSuite` (`IntegrationTestSuite with HostTestHarness`). Use a device written with `writeDevice`, registered with `net.createDevice(id)`, and set up with `setDeviceState`:
  - `"a ShuttingDown device has its stores closed, marked and announced, then is Shutdown"`: two loaded stores. Assert `closeStoreForShutdown` was called for both, each store dir has `transferring-out`, the device root has `shutdown.state` naming `HostId.BootstrapHostId`, the device state reads `Shutdown`, two `StoreOffline` gossips were sent, `loadedDevices` no longer contains the device, and the device is still in the bootstrap `HostState.storageDevices`.
  - `"shutdown cancels a running rebuild and does not restart it"`: recording rebuild factory whose rebuild completes with failure on `cancel()`. Assert `cancel` was called, no second `createRebuild` happens, and the entry still reads `Rebuilding`.
  - `"shutdown cancels transfers in and out of the device and leaves their entries alone"`: `StagedTransferService.cancelledTransfersIn` contains the store, `cancelTransferOut` is recorded, and both entries are unchanged.
  - `"a store that becomes Active during shutdown is taken offline before Shutdown commits"`: arm `completeDeviceShutdown` (or flip the entry with `setDeviceState` between steps 3 and 4). The final state has no `Active` entry and status `Shutdown`.
  - `"repeated checks during a shutdown do not start a second one"` (Review Focus 1): hold `closeStoreForShutdown`'s future open, run `testingOnlyCheckAllDevices()` twice, and assert each store was closed exactly once.
  - `"restart during shutdown cancels it"` (Review Focus 3): hold the close open, `client.restartStorageDevice`, then release. Status `Operational`, `shutdown.state` removed, and the device still loaded.
  - `"an Initializing store is not created while the device is ShuttingDown"` (Review Focus 4).
  - `"StartStoreTransfer for a store on a ShuttingDown device is ignored"`: `testingOnlyHandleHostMessage(StartStoreTransfer(...))`. No `beginTransferOut` is recorded.
  - `"a transfer in from a Shutdown source is not started"`.

- [ ] **Step 2: Run to verify failure:** `sbt 'testOnly *DeviceShutdownSuite'`.

- [ ] **Step 3: Implement** as specified above.

- [ ] **Step 4: Run tests to verify pass**: the suite plus `*HostDeviceDiscoverySuite *StoreRebuildTriggerSuite`, then `sbt test`. Re-run `DeviceShutdownSuite` 20 times (`for i in $(seq 20); do sbt -batch 'testOnly *DeviceShutdownSuite' || break; done`) to rule out flakiness.

- [ ] **Step 5: Commit**

```bash
git commit -am "Shut down a ShuttingDown device's stores and mark it Shutdown"
```

---

### Task 8: Restart on the original host and adoption by a new host

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/server/Host.scala` (`resolveShutdownMarker`, new event, `adoptDevice`)
- Modify: `src/test/scala/org/aspen_ddp/aspen/server/HostTestHarness.scala`
- Create: `src/test/scala/org/aspen_ddp/aspen/server/DeviceAdoptionSuite.scala`
- Modify: `CLAUDE.md` (one bullet under Host and StorageDevice Management describing the shutdown/move procedure and the `owning-host`/`shutdown.state` files), `Todo.txt` (remove the completed item)

**Interfaces:**
- Consumes: Tasks 1, 4, 6, 7
- Produces:
  - `private var pendingMarkerResolutions: Set[os.Path]`: one lookup in flight per path.
  - `resolveShutdownMarker(path, cfg, markerHost)` issues `lookupStorageDeviceState(cfg.storageDeviceId)`. On completion it enqueues `ShutdownMarkerResolved(path: os.Path, cfg: StorageDeviceConfig, markerHost: HostId, state: Try[StorageDeviceState])`. The handler applies this table:

| marker names | device state | action |
|---|---|---|
| this host | `Shutdown` | not loaded (quietly) |
| this host | `ShuttingDown` | load the device. The reconcile finishes the shutdown |
| this host | `Operational`, owner this host | restart: remove `transferring-out` from stores whose entry is `Active` and that are not `isTransferringOut`, `removeShutdownState`, then load normally |
| another host | `Shutdown`, owner = marker host | adopt (below) |
| another host | `Operational`, owner this host | adoption committed earlier: finish the post-commit steps |
| any | `Failed`, or anything else | `warnOnce`, not loaded |
| lookup failed | n/a | `warnOnce`. Retried on the next scan |

  - `protected def adoptDevice(storageDeviceId: StorageDeviceId, previousHostId: HostId): Future[Boolean]`: rebuilt from the removed `updateHostId`. In one transaction (`transactUntilSuccessfulWithRecovery(StopRetrying.unlessCollision)`), revision-guarded on the device, every affected pool's `ConfigKey`, and both HostStates, it applies the following. It returns false and writes nothing if the device is no longer `Shutdown` and owned by `previousHostId`.
    - device: `withOfflineStoresActive`, `status = Operational`, `hostId = this.hostId`
    - each pool of the device's stores: entry `hostId = this.hostId`
    - previous `HostState`: remove the device id. Skipped if the previous host's pointer or state cannot be found.
    - this host's `HostState`: add the device id
  - Pre-adoption validation (before calling `adoptDevice`): every store whose entry is `Offline` has `<dir>/transferring-out` and its backend's CRL save file. Use the same path `ShutdownStore` writes through `store.backend.crlSaveFile`, and compute it from the store directory without opening the backend, adding a static helper on the backend if needed. On any failure, `warnOnce` and do not adopt.
  - Post-commit steps, in order: `writeOwningHost(path, hostId)`, remove `transferring-out` from the stores now `Active` (except those `isTransferringOut`), `removeShutdownState`, `tryLoadDevice(path)`, then for each store loaded, gossip `StoreMoved(storeId, hostId, <pool ConfigKey timestamp>, HLCTimestamp.now)` (the same values `recordTransferIn` uses for a completed transfer in).

- [ ] **Step 1: Write failing tests** in `DeviceAdoptionSuite`. The "old host" is a second host created with `client.createHost`. Its `HostState` lists the device, and the device state names it with status `Shutdown`. The device dir on the `RecordingHost`'s root has `owning-host` = old host and `shutdown.state` = old host, plus store dirs with `transferring-out` and CRL save files:
  - `"a properly shut down device from another host is adopted"`: device `Operational`, `hostId == BootstrapHostId`, `Offline → Active` (a `Rebuilding` entry unchanged), old `HostState` lacks and new `HostState` has the device id, each pool entry's `hostId == BootstrapHostId`, `owning-host` rewritten, `shutdown.state` removed, `transferring-out` removed from the formerly `Offline` stores only, stores offered to `tryLoadStore`, and one `StoreMoved` per loaded store.
  - `"adoption resumes a Rebuilding store"`: after adoption the recording rebuild factory receives `createRebuild` for it.
  - `"a device missing a store's CRL file is not adopted"`: no transaction, logged once across two scans, and still not adopted. Once the file is added it is adopted on the next scan.
  - `"a device whose shutdown did not finish is refused by another host"` (Review Focus 5): marker names the old host but status `ShuttingDown`. Not loaded and logged once.
  - `"a crash between the adoption commit and the load is finished on the next scan"`: state already `Operational`/this host, `shutdown.state` still names the old host. One scan completes the post-commit steps, with no second transaction.
  - `"adoption proceeds when the previous host's state is missing"`.
  - `"a Shutdown device back on its own host is not loaded until restarted"`: marker names this host and the device is `Shutdown`, so it is not loaded. After `client.restartStorageDevice` and `testingOnlyHandleHostMessage(CheckStorageDevice(...))` it loads, markers are removed, and the stores are offered to `tryLoadStore`.
  - `"a host restarted mid-shutdown finishes it"` (Review Focus 2): construct a new host over a device with marker naming itself and status `ShuttingDown`. It loads the device, and after `testingOnlyCheckAllDevices()` the device reads `Shutdown`.

- [ ] **Step 2: Run to verify failure:** `sbt 'testOnly *DeviceAdoptionSuite'`.

- [ ] **Step 3: Implement** as specified above, and update `CLAUDE.md` / `Todo.txt`.

- [ ] **Step 4: Run tests to verify pass:** the suite, then `sbt test` (50 s timeout), then the suite 20 times as in Task 7.

- [ ] **Step 5: Commit**

```bash
git commit -am "Restart a shut-down device in place or adopt it on a new host"
```
