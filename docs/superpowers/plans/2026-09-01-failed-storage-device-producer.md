# Failed Storage Device — Producer Half Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** An operator can declare a storage device dead with one CLI command; a durable task tombstones it and moves every store that lived on it onto a live device, marked `Rebuilding`.

**Architecture:** A device is *tombstoned* by zeroing its `hostId` and `storageDeviceId`. `FailedStorageDeviceDurableTask` does that in one transaction (also removing the device from its enclosing set and its owning host), then drains the tombstone one store at a time — each drain selects a replacement device from the *pool's* set, adds a `Rebuilding` entry there, removes the store from the tombstone, and repoints the pool, all in one transaction. Three existing write paths grow a guard that refuses a tombstoned destination, and `reconcileDeviceState` grows a short-circuit that keeps a still-mounting tombstoned device from wedging the device-check machinery.

**Tech Stack:** Scala 3, sbt, ScalaTest (`AnyFunSuite` for unit suites, `IntegrationTestSuite`/`AsyncFunSuite` over `TestNetwork` for integration), scopt for the CLI, os-lib for filesystem work.

**Spec:** `docs/superpowers/specs/2026-09-01-failed-store-rebuild-design.md` (sections 1, 2, 3, plus the "Producer" and "Guards" testing sections)

## Global Constraints

- Scala 3 "quiet mode" syntax. Braces only where they clearly enhance readability.
- Indented `if/then/else`. Avoid single-line `if a == 1 then value` unless it clearly reads better.
- `cmdline` is a work-in-progress; its interfaces are unstable and may be changed freely.
- Run one test by substring: `sbt 'testOnly *SuiteName -- -z "test substring"'`.
- Every commit message ends with the trailer `Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>`.
- A `DurableTask`'s `processNext` MUST never throw and MUST never return a failed `Future`.
  `ignoreExtraCallsWhileRunning` clears its single-flight flag from `Future.foreach`, which does
  not run on failure; either would wedge the wrapper permanently.
- This plan has **no dependency** on the TKVL plan (`2026-09-01-tkvl-foreach-fixes.md`) and may
  land before or after it.

---

### Task 1: Tombstone constants and the `isFailed` predicate

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/common/metadata/fixed_ids.scala`
- Modify: `src/main/scala/org/aspen_ddp/aspen/common/metadata/StorageDeviceState.scala:44-62`
- Test: `src/test/scala/org/aspen_ddp/aspen/common/metadata/FailedDeviceTombstoneSuite.scala` (create)

**Interfaces:**
- Consumes: nothing.
- Produces:
  - `org.aspen_ddp.aspen.common.metadata.fixed_ids.FailedHostId: HostId`
  - `org.aspen_ddp.aspen.common.metadata.fixed_ids.FailedStorageDeviceId: StorageDeviceId`
  - `StorageDeviceState.isFailed: Boolean` (instance method, no arguments)

- [ ] **Step 1: Write the failing test**

Create `src/test/scala/org/aspen_ddp/aspen/common/metadata/FailedDeviceTombstoneSuite.scala`:

```scala
package org.aspen_ddp.aspen.common.metadata

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.util.UUID

class FailedDeviceTombstoneSuite extends AnyFunSuite with Matchers:

  private val liveHost = HostId(UUID.randomUUID())
  private val liveDevice = StorageDeviceId(UUID.randomUUID())
  private val setId = StorageDeviceSetId(UUID.randomUUID())

  private def state(hostId: HostId, deviceId: StorageDeviceId): StorageDeviceState =
    StorageDeviceState(deviceId, hostId, 0L, 1024L, Map.empty, setId)

  test("the tombstone constants are the zero UUID"):
    fixed_ids.FailedHostId should be(HostId(new UUID(0, 0)))
    fixed_ids.FailedStorageDeviceId should be(StorageDeviceId(new UUID(0, 0)))

  test("a healthy device does not read as failed"):
    state(liveHost, liveDevice).isFailed should be(false)

  test("a fully tombstoned device reads as failed"):
    state(fixed_ids.FailedHostId, fixed_ids.FailedStorageDeviceId).isFailed should be(true)

  // Both half-written cases are failed, not healthy: the predicate is `||`, so a tombstone
  // that was only partly applied still refuses every destination check downstream.
  test("a device with only the host id zeroed reads as failed"):
    state(fixed_ids.FailedHostId, liveDevice).isFailed should be(true)

  test("a device with only the device id zeroed reads as failed"):
    state(liveHost, fixed_ids.FailedStorageDeviceId).isFailed should be(true)
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `sbt 'testOnly *FailedDeviceTombstoneSuite'`
Expected: compilation failure — `value FailedHostId is not a member of object fixed_ids`.

- [ ] **Step 3: Add the constants**

In `src/main/scala/org/aspen_ddp/aspen/common/metadata/fixed_ids.scala`, append to the `fixed_ids`
object, after `BootstrapStorageDeviceSetId`:

```scala
  /** A tombstoned storage device: an operator has declared it dead. Both ids are zeroed so a
   *  half-applied tombstone still reads as failed. Slot 0 is free for both types --
   *  BootstrapStorageDeviceId is UUID(0, 2) and BootstrapHostId is UUID(0, 3). */
  val FailedHostId: HostId = new HostId(new UUID(0, 0))
  val FailedStorageDeviceId: StorageDeviceId = new StorageDeviceId(new UUID(0, 0))
```

- [ ] **Step 4: Add the predicate**

In `src/main/scala/org/aspen_ddp/aspen/common/metadata/StorageDeviceState.scala`, add to the
`StorageDeviceState` case class body, immediately after `def encode()`:

```scala
  /** True once an operator has declared this device dead. A tombstoned device still exists and
   *  is still readable -- it holds the list of stores awaiting reconstruction -- but it is no
   *  longer a member of any set or host, and is never a legal destination for a store.
   *
   *  `||` rather than `&&`: a half-written tombstone must read as failed, not as healthy. */
  def isFailed: Boolean =
    hostId == fixed_ids.FailedHostId || storageDeviceId == fixed_ids.FailedStorageDeviceId
```

- [ ] **Step 5: Run the test to verify it passes**

Run: `sbt 'testOnly *FailedDeviceTombstoneSuite'`
Expected: PASS, 5 tests.

- [ ] **Step 6: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/common/metadata/fixed_ids.scala \
        src/main/scala/org/aspen_ddp/aspen/common/metadata/StorageDeviceState.scala \
        src/test/scala/org/aspen_ddp/aspen/common/metadata/FailedDeviceTombstoneSuite.scala
git commit -m "$(cat <<'EOF'
feat: add the failed-device tombstone constants and isFailed predicate

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
EOF
)"
```

---

### Task 2: Stop `reconcileDeviceState` wedging on a tombstoned device

A tombstoned device whose directory still mounts — a dead controller rather than a dead platter —
makes `reconcileDeviceState` see `remote.hostId != hostId` and call `updateHostId`, which reads
`getHostPointer(FailedHostId)`. That read fails inside `transactUntilSuccessful`, which retries
forever, and the device's entry in `activeDeviceChecks` is never released — every later check of
that device is silently dropped for the life of the process.

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/server/StoreManager.scala:570` (visibility only)
- Modify: `src/main/scala/org/aspen_ddp/aspen/server/StoreManager.scala:710-761` (the short-circuit)
- Test: `src/test/scala/org/aspen_ddp/aspen/server/StoreManagerDeviceDiscoverySuite.scala` (add a
  recording override to `RecordingStoreManager` and two tests)

**Interfaces:**
- Consumes: `StorageDeviceState.isFailed` (Task 1).
- Produces: `StoreManager.updateHostId(storageDeviceId: StorageDeviceId): Future[Unit]` is now
  `protected` rather than `private`, so test subclasses can record it. No other signature changes.

- [ ] **Step 1: Add the recording seam to `RecordingStoreManager`**

In `src/test/scala/org/aspen_ddp/aspen/server/StoreManagerDeviceDiscoverySuite.scala`, add to the
`RecordingStoreManager` class body (immediately after the `lookupStorageDeviceState` override):

```scala
  /** Storage device ids passed to updateHostId, in call order.
   *
   *  Recorded rather than executed: the real updateHostId transacts against the host object,
   *  and the case under test is precisely the one where that host does not exist. Lazy for the
   *  same initialization-order reason as storeLoadAttempts.
   */
  lazy val hostIdClaims: mutable.ListBuffer[StorageDeviceId] =
    mutable.ListBuffer[StorageDeviceId]()

  override protected def updateHostId(storageDeviceId: StorageDeviceId): Future[Unit] =
    synchronized:
      hostIdClaims += storageDeviceId
    Future.unit
```

- [ ] **Step 2: Write the failing tests**

Add to `StoreManagerDeviceDiscoverySuite`, after the existing device-discovery tests:

```scala
  /** A tombstoned StorageDeviceState for a device that is still mounted here. Both ids are
   *  zeroed, which is what an operator's fail-storage-device leaves behind. */
  private def tombstonedState(stores: Map[StoreId, StorageDeviceState.StoreEntry] = Map()):
      StorageDeviceState =
    StorageDeviceState(fixed_ids.FailedStorageDeviceId, fixed_ids.FailedHostId,
                       0L, 1024L, stores, deviceSetId)

  atest("a device owned by another host is claimed with updateHostId"):
    val hostRoot = newHostDir()
    writeDevice(hostRoot, "dev0", deviceA)
    val mgr = newManager(hostRoot)

    val armed = mgr.armLookup(deviceA)
    mgr.testingOnlyCheckAllDevices()
    armed.success(StorageDeviceState(deviceA, HostId(UUID.randomUUID()),
                                     0L, 1024L, Map(), deviceSetId))

    yieldUntil(mgr.hostIdClaims.nonEmpty).map: _ =>
      mgr.hostIdClaims.toList should be(List(deviceA))

  atest("a tombstoned device is not claimed and nothing is reconciled"):
    val hostRoot = newHostDir()
    writeDevice(hostRoot, "dev0", deviceA)
    val mgr = newManager(hostRoot)

    val before = mgr.storeLoadAttempts.size

    val armed = mgr.armLookup(deviceA)
    mgr.testingOnlyCheckAllDevices()
    // An Initializing store would normally drive createNewStore; on a tombstone nothing runs.
    armed.success(tombstonedState(
      Map(storeId -> StorageDeviceState.StoreEntry(
        StorageDeviceState.StoreStatus.Initializing, None))))

    yieldUntil(!mgr.testingOnlyActiveDeviceChecks.contains(deviceA)).map: _ =>
      mgr.hostIdClaims.toList should be(Nil)
      mgr.storeLoadAttempts.size should be(before)
      // The guard entry is released, so later checks of this device still run. That release
      // is the actual regression: the old code held it for the life of the process.
      mgr.testingOnlyActiveDeviceChecks should not contain deviceA
```

Add `fixed_ids` to the existing metadata import at the top of the file:

```scala
import org.aspen_ddp.aspen.common.metadata.{HostId, StorageDeviceId, StorageDeviceSetId, StorageDeviceState, fixed_ids}
```

- [ ] **Step 3: Run the tests to verify they fail**

Run: `sbt 'testOnly *StoreManagerDeviceDiscoverySuite -- -z "tombstoned device is not claimed"'`
Expected: compilation failure — `method updateHostId ... cannot be accessed` / `method updateHostId
overrides nothing`, because it is still `private`.

- [ ] **Step 4: Widen `updateHostId` and add the short-circuit**

In `src/main/scala/org/aspen_ddp/aspen/server/StoreManager.scala`, change line 570 from:

```scala
  private def updateHostId(storageDeviceId: StorageDeviceId): Future[Unit] =
```

to:

```scala
  // protected so a test subclass can record the claim instead of transacting against a host
  // object. Nothing outside StoreManager calls it.
  protected def updateHostId(storageDeviceId: StorageDeviceId): Future[Unit] =
```

Then change the head of `reconcileDeviceState` (line 719-721) from:

```scala
  private def reconcileDeviceState(local: LocalStorageDeviceState,
                                   remote: StorageDeviceState): Unit =
    if remote.hostId != hostId then
```

to:

```scala
  private def reconcileDeviceState(local: LocalStorageDeviceState,
                                   remote: StorageDeviceState): Unit =
    if remote.isFailed then
      // An operator has declared this device dead. Its directory can still mount -- a dead
      // controller rather than a dead platter -- and without this the zeroed host id reads as a
      // migration: updateHostId would call getHostPointer on a host that does not exist, fail
      // inside transactUntilSuccessful, and retry forever while holding this device's entry in
      // activeDeviceChecks. Every later check of the device would then be dropped silently.
      logger.info(s"Ignoring failed storage device ${local.storageDeviceId}")
    else if remote.hostId != hostId then
```

Update the scaladoc immediately above it (line 710-718) to mention the new first clause: after the
existing sentence `If the tree records a different host, ...`, add:

```
   *  A device the tree records as failed is ignored outright: it is neither claimed nor
   *  reconciled, and its stores are being rebuilt elsewhere.
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `sbt 'testOnly *StoreManagerDeviceDiscoverySuite'`
Expected: PASS, including both new tests and every pre-existing test in the suite.

- [ ] **Step 6: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/server/StoreManager.scala \
        src/test/scala/org/aspen_ddp/aspen/server/StoreManagerDeviceDiscoverySuite.scala
git commit -m "$(cat <<'EOF'
fix: ignore failed storage devices in reconcileDeviceState

A tombstoned device whose directory still mounts read as a host migration,
so updateHostId retried a nonexistent host forever while holding the
device's activeDeviceChecks entry.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
EOF
)"
```

---

### Task 3: Refuse a tombstoned destination in `transferStore`

This one guard also covers `MigratePoolToSetDurableTask` and the rebalancer: both reach a device
only through `transferStore`.

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/client/AspenClient.scala:22-26` (new exception)
- Modify: `src/main/scala/org/aspen_ddp/aspen/client/AspenClient.scala:244-320` (`transferStore`)
- Test: `src/test/scala/org/aspen_ddp/aspen/client/TombstoneGuardSuite.scala` (create)

**Interfaces:**
- Consumes: `StorageDeviceState.isFailed` (Task 1), `fixed_ids.FailedHostId`,
  `fixed_ids.FailedStorageDeviceId`.
- Produces: `AspenClient.DeviceFailed(deviceId: StorageDeviceId) extends Exception`. Task 4 and
  Task 9 both throw it.

- [ ] **Step 1: Write the failing test**

Create `src/test/scala/org/aspen_ddp/aspen/client/TombstoneGuardSuite.scala`:

```scala
package org.aspen_ddp.aspen.client

import org.aspen_ddp.aspen.IntegrationTestSuite
import org.aspen_ddp.aspen.common.metadata.{StorageDeviceId, StorageDeviceState, fixed_ids}
import org.aspen_ddp.aspen.common.objects.Insert
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.transaction.KeyValueUpdate.KeyRevision

import scala.concurrent.{ExecutionContext, Future}

/** Every code path that writes a StoreEntry into a StorageDeviceState must refuse a tombstoned
 *  destination: a store placed on a dead device is silently lost. */
class TombstoneGuardSuite extends IntegrationTestSuite:

  /** Zero the host and device ids of `deviceId`'s state in place, leaving everything else --
   *  including its store map and its set membership -- untouched.
   *
   *  This is only the third write of FailedStorageDeviceDurableTask's step 1. The set and host
   *  removals are deliberately skipped: the guards under test read the device object alone, and
   *  leaving the device in its set is the harder case, because selection can still offer it.
   */
  private def tombstone(deviceId: StorageDeviceId): Future[Unit] =
    given ExecutionContext = executionContext
    client.transactUntilSuccessful: tx =>
      for
        ptr <- client.getStorageDevicePointer(deviceId)
        kvos <- client.read(ptr)
      yield
        val state = StorageDeviceState(kvos)
        val tombstoned = state.copy(hostId = fixed_ids.FailedHostId,
                                    storageDeviceId = fixed_ids.FailedStorageDeviceId)
        val reqs = List(KeyRevision(StorageDeviceState.StateKey,
          kvos.contents(StorageDeviceState.StateKey).revision))
        val ops = List(Insert(StorageDeviceState.StateKey, tombstoned.encode()))
        tx.update(ptr, None, None, reqs, ops)

  atest("transferStore refuses a tombstoned destination"):
    given ExecutionContext = executionContext
    val storeId = StoreId(PoolId.BootstrapPoolId, 0.toByte)
    for
      _ <- net.createSecondDevice()
      _ <- waitForTransactionsToComplete()
      _ <- tombstone(net.secondDeviceId)
      _ <- waitForTransactionsToComplete()
      result <- recoverToSucceededIf[AspenClient.DeviceFailed](
                  client.transferStore(storeId, net.secondDeviceId))
    yield result

  atest("transferStore still works against a live destination"):
    given ExecutionContext = executionContext
    val storeId = StoreId(PoolId.BootstrapPoolId, 0.toByte)
    for
      _ <- net.createSecondDevice()
      _ <- waitForTransactionsToComplete()
      _ <- client.transferStore(storeId, net.secondDeviceId)
      _ <- waitForTransactionsToComplete()
      dst <- client.getStorageDeviceState(net.secondDeviceId)
    yield
      dst.stores(storeId).status should be(StorageDeviceState.StoreStatus.TransferringIn)
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `sbt 'testOnly *TombstoneGuardSuite -- -z "transferStore refuses"'`
Expected: compilation failure — `type DeviceFailed is not a member of object AspenClient`.

- [ ] **Step 3: Add the exception**

In `src/main/scala/org/aspen_ddp/aspen/client/AspenClient.scala`, add to the `AspenClient` object
alongside the existing exceptions:

```scala
  /** A store was about to be written onto a device an operator has declared dead. Terminal:
   *  retrying cannot help, since a tombstone is one-way. */
  class DeviceFailed(deviceId: StorageDeviceId)
    extends Exception(s"Storage device ${deviceId.uuid} has been declared failed")
```

- [ ] **Step 4: Add the guard**

In `transferStore`, add `DeviceFailed` to `onFail` so the retry loop stops on it:

```scala
    def onFail(err: Throwable): Future[Unit] = err match
      case e: NoSuchElementException => throw StopRetrying(e)
      case e: InvalidDestination => throw StopRetrying(e)
      case e: StoreNotActive => throw StopRetrying(e)
      case e: DeviceFailed => throw StopRetrying(e)
```

and, in the `yield` block, immediately after the `if sourceId == destinationId then throw
InvalidDestination()` line:

```scala
        // A store on a tombstoned device is silently lost: nothing reconciles it, because
        // reconcileDeviceState ignores failed devices outright.
        if dstState.isFailed then
          throw DeviceFailed(destinationId)
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `sbt 'testOnly *TombstoneGuardSuite'`
Expected: PASS, 2 tests.

- [ ] **Step 6: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/client/AspenClient.scala \
        src/test/scala/org/aspen_ddp/aspen/client/TombstoneGuardSuite.scala
git commit -m "$(cat <<'EOF'
feat: refuse a tombstoned destination in transferStore

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
EOF
)"
```

---

### Task 4: Refuse a tombstoned device at pool creation

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/client/internal/BaseAspenClient.scala:380-393`
  (`stageDeviceUpdate`)
- Test: `src/test/scala/org/aspen_ddp/aspen/client/TombstoneGuardSuite.scala` (add one test)

**Interfaces:**
- Consumes: `AspenClient.DeviceFailed` (Task 3), `StorageDeviceState.isFailed` (Task 1).
- Produces: nothing new.

- [ ] **Step 1: Write the failing test**

Add to `src/test/scala/org/aspen_ddp/aspen/client/TombstoneGuardSuite.scala`:

```scala
  atest("createNewStoragePool refuses to place a store on a tombstoned device"):
    given ExecutionContext = executionContext
    // A dedicated level-0 set holding exactly one device, which is then tombstoned, so
    // selection has nowhere else to put the pool's stores.
    for
      setId <- client.createStorageDeviceSet("doomed-set", level = 0, parent = None)
      _ <- waitForTransactionsToComplete()
      _ <- net.createSecondDevice()
      _ <- waitForTransactionsToComplete()
      _ <- client.moveDeviceToSet(net.secondDeviceId, setId)
      _ <- waitForTransactionsToComplete()
      _ <- tombstone(net.secondDeviceId)
      _ <- waitForTransactionsToComplete()
      result <- recoverToSucceededIf[AspenClient.DeviceFailed](
                  client.createNewStoragePool(
                    "doomed-pool",
                    Replication(1, 1),
                    None,
                    RocksDBConfig(),
                    setId,
                    1_000_000L))
    yield result
```

Add the imports it needs at the top of the file:

```scala
import org.aspen_ddp.aspen.common.ida.Replication
import org.aspen_ddp.aspen.server.store.backend.RocksDBConfig
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `sbt 'testOnly *TombstoneGuardSuite -- -z "createNewStoragePool refuses"'`
Expected: FAIL — the pool is created and no `DeviceFailed` is thrown.

- [ ] **Step 3: Add the guard**

In `src/main/scala/org/aspen_ddp/aspen/client/internal/BaseAspenClient.scala`, in
`stageDeviceUpdate`, insert before the `val updates = ...` line:

```scala
        // A tombstoned device is never a legal destination. This throws inside the transaction
        // preparation, so nothing is staged and transactUntilSuccessful's recovery hook can
        // stop the retry loop.
        if du.state.isFailed then
          throw AspenClient.DeviceFailed(du.storageDeviceId)
```

`createStoragePool`'s prepare runs under `transactUntilSuccessful`, which retries forever on any
failure. Locate the `transactUntilSuccessful` call that wraps this `fStaged` (it opens the
`createStoragePool` override) and change it to `transactUntilSuccessfulWithRecovery(onFail)`,
adding immediately above it:

```scala
    def onFail(err: Throwable): Future[Unit] = err match
      case e: AspenClient.DeviceFailed => throw StopRetrying(e)
```

Verify `StopRetrying` and `AspenClient` are already imported in the file; add whichever is missing.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `sbt 'testOnly *TombstoneGuardSuite'`
Expected: PASS, 3 tests.

- [ ] **Step 5: Run the pool-creation regression suites**

Run: `sbt 'testOnly *StorageDeviceSetIntegrationSuite *StorageDeviceCreationSuite'`
Expected: PASS, unchanged.

- [ ] **Step 6: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/client/internal/BaseAspenClient.scala \
        src/test/scala/org/aspen_ddp/aspen/client/TombstoneGuardSuite.scala
git commit -m "$(cat <<'EOF'
feat: refuse a tombstoned device when creating a storage pool

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
EOF
)"
```

---

### Task 5: Let `selectDeviceForRebuild` recurse into level-1+ sets

`selectRebuildDevice` currently passes a throwing stub as its recursion `lookup`, so a pool
assigned to a level-1+ set cannot be rebuilt at all. The fix is to thread a real `lookup` through.

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/common/metadata/StorageDeviceSetState.scala:142-177`
  (`selectDeviceForRebuild`) and `:349-366` (`selectRebuildDevice`)
- Test: `src/test/scala/org/aspen_ddp/aspen/common/metadata/StorageDeviceSetSelectionSuite.scala`
  (replace the level-guard test, update two call sites)

**Interfaces:**
- Consumes: nothing from earlier tasks.
- Produces: `selectRebuildDevice` gains a fourth parameter:
  ```scala
  private[metadata] def selectRebuildDevice(
      requiredSize: Long,
      failedDevice: StorageDeviceId,
      poolDevices: Set[StorageDeviceId],
      lookup: StorageDeviceSetId => Future[StorageDeviceSetState],
      freeSpaceLookup: StorageDeviceId => Future[Long],
      rng: Random
  )(using ec: ExecutionContext): Future[StorageDeviceId]
  ```
  `selectDeviceForRebuild(poolId, failedIndex, client, rng)` keeps its public signature and is
  what Task 9 calls.

- [ ] **Step 1: Update the existing tests to the new signature and add recursion coverage**

In `src/test/scala/org/aspen_ddp/aspen/common/metadata/StorageDeviceSetSelectionSuite.scala`,
**replace** this test:

```scala
  test("rebuild: level != 0 fails with AllocationError"):
    val upperSet = upper(1, List(leaf(List(dev()))))
    val lookup = fixedLookup(Map.empty[StorageDeviceId, Long].withDefaultValue(9999L))
    assertThrows[AllocationError]:
      Await.result(
        upperSet.selectRebuildDevice(0L, dev(), Set.empty, lookup, new Random(8)), timeout)
```

with:

```scala
  test("rebuild: a level-1 set recurses into its member sets"):
    val target = dev()
    val leafSet = leaf(List(target))
    val upperSet = upper(1, List(leafSet))
    val free = fixedLookup(Map(target -> 9999L))
    val chosen = Await.result(
      upperSet.selectRebuildDevice(
        500L, dev(), Set.empty, lookupFor(leafSet), free, new Random(8)), timeout)
    chosen should be(target)

  test("rebuild: a level-1 set still hard-excludes the failed device"):
    val failed = dev()
    val alive = dev()
    val leafSet = leaf(List(failed, alive))
    val upperSet = upper(1, List(leafSet))
    val free = fixedLookup(Map(failed -> 9999L, alive -> 9999L))
    val chosen = Await.result(
      upperSet.selectRebuildDevice(
        500L, failed, Set(failed), lookupFor(leafSet), free, new Random(11)), timeout)
    chosen should be(alive)

  test("rebuild: a level-1 set with no room anywhere fails with AllocationError"):
    val only = dev()
    val leafSet = leaf(List(only))
    val upperSet = upper(1, List(leafSet))
    val free = fixedLookup(Map(only -> 10L))
    assertThrows[AllocationError]:
      Await.result(
        upperSet.selectRebuildDevice(
          500L, dev(), Set.empty, lookupFor(leafSet), free, new Random(12)), timeout)
```

Then update the two remaining `selectRebuildDevice` call sites in the suite to pass `noLookup` in
the new fourth position. `rebuild: empty member devices fails with AllocationError` becomes:

```scala
      Await.result(
        set.selectRebuildDevice(0L, dev(), Set.empty, noLookup, lookup, new Random(9)), timeout)
```

and both calls in `rebuild: identical seed produces identical selection` become:

```scala
    val a = Await.result(
      set.selectRebuildDevice(500L, failed, Set(failed), noLookup, lookup, new Random(123)), timeout)
    val b = Await.result(
      set.selectRebuildDevice(500L, failed, Set(failed), noLookup, lookup, new Random(123)), timeout)
```

Search the whole suite for any other `selectRebuildDevice(` call and give it `noLookup` in the same
position — a level-0 set never recurses, so `noLookup` is correct for every level-0 case.

- [ ] **Step 2: Run the suite to verify it fails**

Run: `sbt 'testOnly *StorageDeviceSetSelectionSuite'`
Expected: compilation failure — too many arguments for `selectRebuildDevice`.

- [ ] **Step 3: Thread the lookup through**

In `src/main/scala/org/aspen_ddp/aspen/common/metadata/StorageDeviceSetState.scala`, replace
`selectRebuildDevice` (lines 342-366) with:

```scala
  /** Select a device to host a store rebuilt from scratch (its data was lost).
   *
   *  The failed device is hard-excluded; devices already hosting a store in the pool
   *  (`poolDevices`) are soft-excluded. Level-1+ sets recurse through `lookup`, so a pool
   *  assigned to a tiered set can be rebuilt within that tier.
   */
  private[metadata] def selectRebuildDevice(
      requiredSize: Long,
      failedDevice: StorageDeviceId,
      poolDevices: Set[StorageDeviceId],
      lookup: StorageDeviceSetId => Future[StorageDeviceSetState],
      freeSpaceLookup: StorageDeviceId => Future[Long],
      rng: Random
  )(using ec: ExecutionContext): Future[StorageDeviceId] =
    selectDeviceWithSpace(
      requiredSize,
      hardExclude = Set(failedDevice),
      softExclude = poolDevices,
      lookup = lookup,
      freeSpaceLookup = freeSpaceLookup,
      rng = rng)
```

Then in `selectDeviceForRebuild` (lines 142-177) remove the level guard and pass the client's
lookup. Replace the whole method with:

```scala
  /** Select a device to host a store that must be rebuilt from scratch because its
   *  data was lost. The store currently at `failedIndex` in pool `poolId` is the one
   *  being rebuilt; its current size (the pool's per-store usage record, or 0 if absent)
   *  is the amount of free space a candidate device must have.
   *
   *  Valid at any level: a level-1+ set recurses into its member sets. The pool object is read
   *  directly via its pointer so the per-store size key and the config come from a single read.
   *  See `selectRebuildDevice` for the selection policy (failed device hard-excluded, other pool
   *  devices soft-excluded, free space required). All failures are
   *  `Future.failed(AllocationError(...))`.
   *
   *  Callers must select BEFORE repointing the pool: the failed device is derived from
   *  `poolState.stores(failedIndex)`, so a pool already repointed elsewhere would hard-exclude
   *  the wrong device.
   */
  def selectDeviceForRebuild(
      poolId: PoolId,
      failedIndex: Byte,
      client: AspenClient,
      rng: Random = new Random()
  ): Future[StorageDeviceId] =
    given ExecutionContext = client.clientContext
    if level == 0 && memberDevices.isEmpty then
      Future.failed(AllocationError(
        s"StorageDeviceSet ${setId.uuid} (level 0) has no member devices"))
    else
      for
        poolPtr <- client.getStoragePoolPointer(poolId)
        poolKvos <- client.read(poolPtr)
        poolState = StoragePoolState(poolKvos)
        device <-
          if failedIndex < 0 || failedIndex >= poolState.stores.length then
            Future.failed(AllocationError(
              s"failedIndex $failedIndex out of range for pool ${poolId.uuid} (${poolState.stores.length} stores)"))
          else
            val requiredSize = poolKvos.contents
              .get(StoragePoolState.getStoreUsageKey(failedIndex))
              .map(vs => byte2long(vs.value.bytes))
              .getOrElse(0L)
            val failedDevice = poolState.stores(failedIndex).storageDeviceId
            val poolDevices = poolState.stores.map(_.storageDeviceId).toSet
            selectRebuildDevice(
              requiredSize,
              failedDevice,
              poolDevices,
              client.getStorageDeviceSetState,
              id => client.getStorageDeviceState(id).map(s => s.totalSize - s.currentUsage),
              rng)
      yield device
```

- [ ] **Step 4: Run the suite to verify it passes**

Run: `sbt 'testOnly *StorageDeviceSetSelectionSuite'`
Expected: PASS. Every pre-existing case in the suite must pass unchanged; only the level-guard case
was replaced.

- [ ] **Step 5: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/common/metadata/StorageDeviceSetState.scala \
        src/test/scala/org/aspen_ddp/aspen/common/metadata/StorageDeviceSetSelectionSuite.scala
git commit -m "$(cat <<'EOF'
feat: let selectDeviceForRebuild recurse into tiered device sets

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
EOF
)"
```

---

### Task 6: Lift `prepUpdateBootstrapConfig` into a shared helper

`StoreManager.updateStateForTransferredStore` regenerates the radicle's bootstrap config whenever a
bootstrap-pool store changes device. The drain in Task 9 makes exactly that change from the client
side, so the logic has to become reachable from both. It currently reads `aspenSystemId` from a
`StoreManager` constructor parameter, which a client-side caller does not have — but the radicle
already stores it under `Radicle.SystemIdKey`, so the helper reads it from there and needs nothing
but an `AspenClient`.

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/common/metadata/BootstrapConfig.scala` (add the helper)
- Modify: `src/main/scala/org/aspen_ddp/aspen/server/StoreManager.scala:366-408` (call it)
- Test: `src/test/scala/org/aspen_ddp/aspen/common/metadata/BootstrapConfigSuite.scala` — no, this
  is a pure-unit suite; create instead
  `src/test/scala/org/aspen_ddp/aspen/common/metadata/BootstrapConfigRadicleUpdateSuite.scala`

**Interfaces:**
- Consumes: nothing from earlier tasks.
- Produces:
  ```scala
  object BootstrapConfig:
    def prepRadicleUpdate(client: AspenClient,
                          storeId: StoreId,
                          poolCfg: StoragePoolState,
                          newHostId: HostId)
                         (using tx: Transaction): Future[Unit]
  ```
  A no-op returning `Future.unit` unless `storeId.poolId == PoolId.BootstrapPoolId`. Task 9 calls it.

- [ ] **Step 1: Write the failing test**

Create `src/test/scala/org/aspen_ddp/aspen/common/metadata/BootstrapConfigRadicleUpdateSuite.scala`:

```scala
package org.aspen_ddp.aspen.common.metadata

import org.aspen_ddp.aspen.IntegrationTestSuite
import org.aspen_ddp.aspen.client.Transaction
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

class BootstrapConfigRadicleUpdateSuite extends IntegrationTestSuite:

  atest("prepRadicleUpdate rewrites the radicle config to name the new host"):
    given ExecutionContext = executionContext
    val storeId = StoreId(PoolId.BootstrapPoolId, 0.toByte)
    for
      newHostId <- client.createHost("second-host", "127.0.0.1", 5100, 5101, 5102)
      _ <- waitForTransactionsToComplete()
      poolCfg <- client.getStoragePoolState(PoolId.BootstrapPoolId)
      _ <- client.transactUntilSuccessful: tx =>
             given Transaction = tx
             BootstrapConfig.prepRadicleUpdate(client, storeId, poolCfg, newHostId)
      _ <- waitForTransactionsToComplete()
      yaml <- client.getBootstrapConfig()
    yield
      val cfg = BootstrapConfig.parseBootstrapConfig(yaml)
      // Store 0 has moved to the new host; the other two are untouched.
      val hostOfStore0 = cfg.hosts.find(_.stores.contains(storeId)).map(_.hostId)
      hostOfStore0 should be(Some(newHostId))
      cfg.hosts.map(_.stores.length).sum should be(3)

  atest("prepRadicleUpdate is a no-op for a non-bootstrap pool"):
    given ExecutionContext = executionContext
    val storeId = StoreId(PoolId(UUID.randomUUID()), 0.toByte)
    for
      before <- client.getBootstrapConfig()
      poolCfg <- client.getStoragePoolState(PoolId.BootstrapPoolId)
      staged <- {
        given Transaction = client.newTransaction()
        BootstrapConfig.prepRadicleUpdate(client, storeId, poolCfg, HostId.BootstrapHostId)
      }
      after <- client.getBootstrapConfig()
    yield
      staged should be(())
      after should be(before)
```

Note the second test deliberately never commits its transaction: the assertion is that nothing was
staged, and Aspen has no empty transactions.

- [ ] **Step 2: Run the test to verify it fails**

Run: `sbt 'testOnly *BootstrapConfigRadicleUpdateSuite'`
Expected: compilation failure — `value prepRadicleUpdate is not a member of object BootstrapConfig`.

- [ ] **Step 3: Write the helper**

In `src/main/scala/org/aspen_ddp/aspen/common/metadata/BootstrapConfig.scala`, add to the
`BootstrapConfig` object, after `generateBootstrapConfig`:

```scala
  /** Stage a regeneration of the radicle's bootstrap config inside the caller's transaction,
   *  for a bootstrap-pool store that is moving to `newHostId`. A no-op -- `Future.unit`, nothing
   *  staged -- for any other pool.
   *
   *  Every host bootstraps by reading this config, so a bootstrap-pool store that changes device
   *  without it leaves every host in the system pointing at where the store used to be.
   *
   *  `aspenSystemId` comes from the radicle's own SystemIdKey rather than from a parameter, so
   *  this needs nothing but an AspenClient. StoreManager has the value in a constructor
   *  parameter; the client-side callers do not.
   */
  def prepRadicleUpdate(client: AspenClient,
                        storeId: StoreId,
                        poolCfg: StoragePoolState,
                        newHostId: HostId)
                       (using tx: Transaction): Future[Unit] =
    given ExecutionContext = client.clientContext

    if storeId.poolId != PoolId.BootstrapPoolId then
      Future.unit
    else
      for
        newHost <- client.getHostState(newHostId)
        poolHosts <- Future.sequence(poolCfg.stores.zipWithIndex.toList.map: (e, index) =>
                       client.getHostState(e.hostId).map(host =>
                         (StoreId(storeId.poolId, index.toByte), host)))
        radicleKvos <- client.read(client.radicle)
      yield
        val hostsMap = poolHosts.map((_, host) => host.hostId -> host).toMap +
          (newHost.hostId -> newHost)
        val hostsList = hostsMap.valuesIterator.toList
        val storeMap = poolHosts.map: (sid, host) =>
          if sid == storeId then
            (sid, newHost.hostId)
          else
            (sid, host.hostId)

        val aspenSystemId = byte2uuid(radicleKvos.contents(Radicle.SystemIdKey).value.bytes)

        val yaml = generateBootstrapConfig(aspenSystemId, poolCfg.ida, hostsList, storeMap)

        val reqs = List(KeyRevision(Radicle.BootstrapConfigKey,
          radicleKvos.contents(Radicle.BootstrapConfigKey).revision))
        val ops = List(Insert(Radicle.BootstrapConfigKey, yaml.getBytes(StandardCharsets.UTF_8)))
        tx.update(client.radicle, None, None, reqs, ops)
```

Add the imports it needs to the top of `BootstrapConfig.scala`:

```scala
import org.aspen_ddp.aspen.client.{AspenClient, Transaction}
import org.aspen_ddp.aspen.common.Radicle
import org.aspen_ddp.aspen.common.objects.Insert
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.transaction.KeyValueUpdate.KeyRevision
import org.aspen_ddp.aspen.common.util.byte2uuid

import java.nio.charset.StandardCharsets
import scala.concurrent.{ExecutionContext, Future}
```

Skip any that are already present.

- [ ] **Step 4: Run the test to verify it passes**

Run: `sbt 'testOnly *BootstrapConfigRadicleUpdateSuite'`
Expected: PASS, 2 tests.

- [ ] **Step 5: Delete the nested copy in `StoreManager`**

In `src/main/scala/org/aspen_ddp/aspen/server/StoreManager.scala`, delete the nested
`def prepUpdateBootstrapConfig` (lines 371-397) entirely and change its one call site (line 408)
from:

```scala
        _ <- prepUpdateBootstrapConfig(poolCfg, toDev)
```

to:

```scala
        _ <- BootstrapConfig.prepRadicleUpdate(client, storeId, poolCfg, toDev.hostId)(using tx)
```

`updateStateForTransferredStore` already has `tx` in scope from
`client.transactUntilSuccessful: tx =>`; pass it explicitly rather than introducing a `given`, so
the surrounding for-comprehension is untouched. Confirm `BootstrapConfig` is imported in
`StoreManager.scala` (it is — `generateBootstrapConfig` was called there). Remove any imports the
deletion orphans (`StandardCharsets` and `Radicle` may still be used elsewhere in the file — check
before removing).

- [ ] **Step 6: Run the transfer regression suites**

Run: `sbt 'testOnly *StoreManagerDeviceDiscoverySuite *MigratePoolToSetSuite *StorageDeviceSetIntegrationSuite'`
Expected: PASS, unchanged.

- [ ] **Step 7: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/common/metadata/BootstrapConfig.scala \
        src/main/scala/org/aspen_ddp/aspen/server/StoreManager.scala \
        src/test/scala/org/aspen_ddp/aspen/common/metadata/BootstrapConfigRadicleUpdateSuite.scala
git commit -m "$(cat <<'EOF'
refactor: lift the bootstrap-config regeneration into a shared helper

The failed-device drain performs the same bootstrap-pool store move that
StoreManager's transfer completion does, so both need this. The system id
now comes from the radicle's SystemIdKey rather than a StoreManager field.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
EOF
)"
```

---

### Task 7: The task skeleton, its registration, and `failStorageDevice`

The task does nothing yet beyond finishing immediately; this task lands the factory, the type
registration, the client entry point, and the enrollment test. Steps 1 and 2 of the task itself
arrive in Tasks 8 and 9.

**Files:**
- Create: `src/main/scala/org/aspen_ddp/aspen/common/metadata/management/FailedStorageDeviceDurableTask.scala`
- Modify: `src/main/scala/org/aspen_ddp/aspen/common/TypeFactories.scala`
- Modify: `src/main/scala/org/aspen_ddp/aspen/client/AspenClient.scala` (declaration)
- Modify: `src/main/scala/org/aspen_ddp/aspen/client/internal/BaseAspenClient.scala` (implementation)
- Test: `src/test/scala/org/aspen_ddp/aspen/common/metadata/management/FailedStorageDeviceSuite.scala` (create)

**Interfaces:**
- Consumes: `StorageDeviceState.isFailed`, `fixed_ids.FailedHostId`,
  `fixed_ids.FailedStorageDeviceId` (Task 1).
- Produces:
  ```scala
  object FailedStorageDeviceDurableTask extends DurableTaskFactory:
    val typeUUID: UUID                               // 7b1f4d62-0c58-4a97-8e35-6a2d91cf4b08
    private[aspen] val DeviceIdKey: Key              // Key(Array[Byte](0))
    private[aspen] val SetIdKey: Key                 // Key(Array[Byte](1))
    val DefaultPollPeriod: Duration                  // 2 MINUTES
    @volatile var pollPeriod: Duration
    def createTask(client, pointer, revision, state, taskExecutor): DurableTask
    def prepareSystemTask(client: AspenClient,
                          deviceId: StorageDeviceId,
                          setId: StorageDeviceSetId)(using tx: Transaction): Future[Unit]

  class FailedStorageDeviceDurableTask(taskPointer: DurableTaskPointer,
                                       client: AspenClient,
                                       deviceId: StorageDeviceId,
                                       setId: StorageDeviceSetId,
                                       pollPeriod: Duration) extends DurableTask
  ```
  and on `AspenClient`:
  ```scala
  def failStorageDevice(deviceId: StorageDeviceId): Future[Unit]
  class AspenClient.DeviceAlreadyFailed(deviceId: StorageDeviceId) extends Exception
  ```
  Tasks 8, 9 and 10 all build on these exact names.

- [ ] **Step 1: Write the failing test**

Create `src/test/scala/org/aspen_ddp/aspen/common/metadata/management/FailedStorageDeviceSuite.scala`:

```scala
package org.aspen_ddp.aspen.common.metadata.management

import org.aspen_ddp.aspen.IntegrationTestSuite
import org.aspen_ddp.aspen.client.AspenClient
import org.aspen_ddp.aspen.client.tkvl.{KVObjectRootManager, TieredKeyValueList}
import org.aspen_ddp.aspen.common.{Radicle, TypeFactories}
import org.aspen_ddp.aspen.common.metadata.{StorageDeviceId, StorageDeviceSetId, StorageDeviceState}
import org.aspen_ddp.aspen.common.objects.{Key, KeyValueObjectPointer}
import org.aspen_ddp.aspen.compute.{DurableTaskFactory, DurableTaskPointer, ServiceEntry}
import org.aspen_ddp.aspen.compute.systemtask.{SystemTaskExecutorService, SystemTaskServiceState}

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.concurrent.duration.{Duration, MILLISECONDS}

class FailedStorageDeviceSuite extends IntegrationTestSuite:

  atest("FailedStorageDeviceDurableTask is registered in the client type registry"):
    given ExecutionContext = executionContext
    val factory = client.typeRegistry.getType[DurableTaskFactory](
      FailedStorageDeviceDurableTask.typeUUID)
    factory should be(Some(FailedStorageDeviceDurableTask))
    TypeFactories.factories should contain(FailedStorageDeviceDurableTask)

  /** The task state objects currently enrolled with the system task executor. */
  protected def enrolledTasks(): Future[List[(java.util.UUID, KeyValueObjectPointer)]] =
    given ExecutionContext = executionContext
    val tkvl = TieredKeyValueList(client,
      KVObjectRootManager(client, Radicle.ServicesTreeKey, Radicle.pointer))
    for
      vs <- tkvl.get(Key(SystemTaskExecutorService.ServiceUUID))
      statePtr = ServiceEntry.decode(vs.get.value.bytes).statePointer
      enrolled <- SystemTaskServiceState.scan(client, statePtr)
    yield enrolled

  atest("failStorageDevice enrolls exactly one task"):
    given ExecutionContext = executionContext
    for
      _ <- net.createSecondDevice()
      _ <- waitForTransactionsToComplete()
      _ <- client.failStorageDevice(net.secondDeviceId)
      _ <- waitForTransactionsToComplete()
      enrolled <- enrolledTasks()
    yield
      enrolled.size should be(1)

  atest("failStorageDevice refuses a device that is already tombstoned"):
    given ExecutionContext = executionContext
    for
      _ <- net.createSecondDevice()
      _ <- waitForTransactionsToComplete()
      _ <- client.failStorageDevice(net.secondDeviceId)
      _ <- waitForTransactionsToComplete()
      // Drive step 1 so the device actually carries the tombstone.
      task <- taskForEnrolled(net.secondDeviceId, StorageDeviceSetId.BootstrapStorageDeviceSetId)
      _ <- awaitTombstone(net.secondDeviceId, Duration(30000, MILLISECONDS))
      result <- recoverToSucceededIf[AspenClient.DeviceAlreadyFailed](
                  client.failStorageDevice(net.secondDeviceId))
    yield result

  atest("failStorageDevice fails with NoSuchElementException for an unknown device"):
    given ExecutionContext = executionContext
    recoverToSucceededIf[NoSuchElementException](
      client.failStorageDevice(StorageDeviceId(java.util.UUID.randomUUID())))
```

The third test references two helpers that Task 8 adds. For this task, **omit that third test
entirely** — add it in Task 8, where `taskForEnrolled` and `awaitTombstone` are defined. Write only
the first, second and fourth tests now.

- [ ] **Step 2: Run the test to verify it fails**

Run: `sbt 'testOnly *FailedStorageDeviceSuite'`
Expected: compilation failure — `not found: object FailedStorageDeviceDurableTask`.

- [ ] **Step 3: Write the task skeleton**

Create
`src/main/scala/org/aspen_ddp/aspen/common/metadata/management/FailedStorageDeviceDurableTask.scala`:

```scala
package org.aspen_ddp.aspen.common.metadata.management

import org.aspen_ddp.aspen.client.{AspenClient, KeyValueObjectState, Transaction}
import org.aspen_ddp.aspen.common.metadata.{StorageDeviceId, StorageDeviceSetId}
import org.aspen_ddp.aspen.common.objects.{Key, ObjectRevision}
import org.aspen_ddp.aspen.common.util.BackgroundTaskManager.{NoTask, ScheduledTask}
import org.aspen_ddp.aspen.common.util.{byte2uuid, ignoreExtraCallsWhileRunning, uuid2byte}
import org.aspen_ddp.aspen.compute.{DurableTask, DurableTaskFactory, DurableTaskPointer, TaskExecutor, TaskStopped}
import scribe.Logging

import java.util.UUID
import scala.concurrent.duration.{Duration, MINUTES}
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success}

object FailedStorageDeviceDurableTask extends DurableTaskFactory:

  val typeUUID: UUID = UUID.fromString("7b1f4d62-0c58-4a97-8e35-6a2d91cf4b08")

  /** The failed device. Below DurableTask.ReservedFromKeyId and distinct from
   *  SimpleTaskExecutor.TaskTypeKey, which is where prepareSystemDurableTask writes the type
   *  UUID. */
  private[aspen] val DeviceIdKey: Key = Key(Array[Byte](0))

  /** The set that contained the device, captured at enrollment because step 1 destroys the
   *  device's own record of it. */
  private[aspen] val SetIdKey: Key = Key(Array[Byte](1))

  val DefaultPollPeriod: Duration = Duration(2, MINUTES)

  /** Overridable poll period (test seam; mirrors MigratePoolToSetDurableTask.pollPeriod). */
  @volatile var pollPeriod: Duration = DefaultPollPeriod

  def createTask(client: AspenClient,
                 pointer: DurableTaskPointer,
                 revision: ObjectRevision,
                 state: Map[Key, KeyValueObjectState.ValueState],
                 taskExecutor: TaskExecutor): DurableTask =
    val deviceId = StorageDeviceId(byte2uuid(state(DeviceIdKey).value.bytes))
    val setId = StorageDeviceSetId(byte2uuid(state(SetIdKey).value.bytes))
    new FailedStorageDeviceDurableTask(pointer, client, deviceId, setId, pollPeriod)

  /** Stage enrollment of a failure task for `deviceId` inside the caller's transaction. */
  def prepareSystemTask(client: AspenClient,
                        deviceId: StorageDeviceId,
                        setId: StorageDeviceSetId)(using tx: Transaction): Future[Unit] =
    client.prepareSystemDurableTask(typeUUID, Map(
      DeviceIdKey -> uuid2byte(deviceId.uuid),
      SetIdKey -> uuid2byte(setId.uuid)))


/** Tombstones a failed storage device and drains its stores onto live devices, one at a time.
 *
 *  Two steps, each a single transaction and each idempotent by re-reading inside that
 *  transaction. Step 1 removes the device from its set and its host and zeroes both ids; step 2
 *  moves one store per pass onto a device selected from that store's pool's set, marked
 *  Rebuilding. The task completes when the tombstone's store map is empty.
 *
 *  Progress needs no record of its own: the tombstone's own store map is the work list, and every
 *  transaction that moves a store also removes it from that map. Crash recovery is therefore
 *  free.
 *
 *  Nothing here reconstructs data. A Rebuilding entry is a message to the StoreManager on the
 *  destination host, which does the actual walk. The two halves never talk and either may crash
 *  and restart independently.
 */
class FailedStorageDeviceDurableTask(
    val taskPointer: DurableTaskPointer,
    client: AspenClient,
    deviceId: StorageDeviceId,
    setId: StorageDeviceSetId,
    pollPeriod: Duration
) extends DurableTask with Logging:

  private given ExecutionContext = client.clientContext

  private val promise = Promise[Option[AnyRef]]()
  def completed: Future[Option[AnyRef]] = promise.future

  private var pollTask: ScheduledTask = NoTask

  /** Single-flight guard: a tick arriving while a pass is still running is dropped.
   *
   *  processNext MUST never return a failed Future, nor throw: ignoreExtraCallsWhileRunning
   *  clears its flag from `Future.foreach`, which does not run on failure, and a synchronous
   *  throw escapes before the flag is ever cleared. Either would wedge the wrapper permanently. */
  private val run: () => Unit = ignoreExtraCallsWhileRunning(processNext())

  // Begin driving immediately.
  run()

  override protected def onStop(): Unit = synchronized:
    pollTask.cancel()
    if !promise.isCompleted then promise.failure(new TaskStopped)

  private def scheduleRecheck(): Unit = synchronized:
    if !isStopped then
      pollTask.cancel()
      pollTask = client.backgroundTaskManager.schedule(pollPeriod):
        run()

  private def finishOk(): Unit = synchronized:
    if !promise.isCompleted then promise.success(None)

  private def processNext(): Future[Unit] =
    if isStopped then
      synchronized { if !promise.isCompleted then promise.failure(new TaskStopped) }
      Future.unit
    else
      // try/catch as well as recover: a synchronous throw out of drive() would leave the
      // single-flight flag set forever.
      val pass =
        try drive()
        catch
          case err: Throwable => Future.failed(err)

      // transformWith to a constant Future.unit rather than recover: a `recover` body that
      // itself threw would fail the returned future and wedge the flag just the same.
      pass.transformWith: outcome =>
        try
          outcome match
            case Failure(err) =>
              logger.warn(s"Failed device ${deviceId.uuid}: transient error, will retry: $err")
              scheduleRecheck()

            case Success(_) => ()
        catch
          case _: Throwable => () // nothing left to report with; the next tick is the recovery

        Future.unit

  /** One pass. Filled in by the tombstone step (Task 8) and the drain step (Task 9). */
  private def drive(): Future[Unit] =
    Future.successful(finishOk())
```

- [ ] **Step 4: Register the factory**

In `src/main/scala/org/aspen_ddp/aspen/common/TypeFactories.scala`, add the new factory to the list
and import it:

```scala
  val factories: List[RegisteredTypeFactory] =
    List(UpdateAllocationGroupUsageTask, MigratePoolToSetDurableTask, FailedStorageDeviceDurableTask)
```

- [ ] **Step 5: Declare `failStorageDevice` on `AspenClient`**

In `src/main/scala/org/aspen_ddp/aspen/client/AspenClient.scala`, add to the `AspenClient` object,
next to `DeviceFailed`:

```scala
  /** fail-storage-device was invoked against a device that already carries a tombstone.
   *  Terminal: the tombstone is one-way, and a recovered device is re-introduced under a new id. */
  class DeviceAlreadyFailed(deviceId: StorageDeviceId)
    extends Exception(s"Storage device ${deviceId.uuid} has already been declared failed")
```

and declare the method on the trait, next to `migratePoolToSet`:

```scala
  /** Declare `deviceId` dead and begin reconstructing every store that lived on it.
   *
   *  One transaction enrolls a FailedStorageDeviceDurableTask, which then tombstones the device
   *  -- removing it from its set and its owning host and zeroing both ids -- and moves its stores
   *  one at a time onto live devices of each store's pool's set, marked Rebuilding. Returns as
   *  soon as the enrollment commits; progress is observable via `show-device`.
   *
   *  The tombstone is one-way. A recovered device is re-introduced as a new device with a new id.
   *
   *  Fails with NoSuchElementException if the device does not exist, or DeviceAlreadyFailed if it
   *  is already tombstoned. Two simultaneous calls both enroll, which is harmless: the task's
   *  steps are idempotent and racing drains lose on their KeyRevision requirements and retry.
   */
  def failStorageDevice(deviceId: StorageDeviceId): Future[Unit]
```

- [ ] **Step 6: Implement it in `BaseAspenClient`**

In `src/main/scala/org/aspen_ddp/aspen/client/internal/BaseAspenClient.scala`, add after
`migratePoolToSet`:

```scala
  override def failStorageDevice(deviceId: StorageDeviceId): Future[Unit] =
    given ExecutionContext = clientContext

    def onFail(err: Throwable): Future[Unit] = err match
      case e: NoSuchElementException => throw StopRetrying(e)
      case e: ReadError => throw StopRetrying(e)
      case e: AspenClient.DeviceAlreadyFailed => throw StopRetrying(e)

    transactUntilSuccessfulWithRecovery(onFail): tx =>
      given Transaction = tx

      for
        devPtr <- getStorageDevicePointer(deviceId)
        devKvos <- read(devPtr, "fail storage device")
        state = StorageDeviceState(devKvos)
        _ <-
          if state.isFailed then
            throw AspenClient.DeviceAlreadyFailed(deviceId)
          else
            // The set is captured now because step 1 zeroes the device's own record of it.
            FailedStorageDeviceDurableTask.prepareSystemTask(
              this, deviceId, state.storageDeviceSet)
      yield ()
```

Add `FailedStorageDeviceDurableTask` to the file's `common.metadata.management` import.

- [ ] **Step 7: Run the tests to verify they pass**

Run: `sbt 'testOnly *FailedStorageDeviceSuite'`
Expected: PASS, 3 tests.

- [ ] **Step 8: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/common/metadata/management/FailedStorageDeviceDurableTask.scala \
        src/main/scala/org/aspen_ddp/aspen/common/TypeFactories.scala \
        src/main/scala/org/aspen_ddp/aspen/client/AspenClient.scala \
        src/main/scala/org/aspen_ddp/aspen/client/internal/BaseAspenClient.scala \
        src/test/scala/org/aspen_ddp/aspen/common/metadata/management/FailedStorageDeviceSuite.scala
git commit -m "$(cat <<'EOF'
feat: add failStorageDevice and the FailedStorageDeviceDurableTask skeleton

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
EOF
)"
```

---

### Task 8: Step 1 — tombstone the device

One transaction: remove the device from its enclosing set's `memberDevices`, remove it from its
owning `HostState.storageDevices`, and rewrite the device state with both ids zeroed. All three
re-read inside the transaction and are self-healing if the removal has already happened, which
makes the step idempotent and concurrent invocations harmless.

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/common/metadata/management/FailedStorageDeviceDurableTask.scala`
- Test: `src/test/scala/org/aspen_ddp/aspen/common/metadata/management/FailedStorageDeviceSuite.scala`

**Interfaces:**
- Consumes: the skeleton from Task 7; `StorageDeviceState.isFailed`, `fixed_ids.*` (Task 1);
  `HostState.removeStorageDevice`; `StorageDeviceSetState.memberDevices`.
- Produces: `drive()` now performs step 1 and finishes; Task 9 replaces the finish with the drain.
  Test helpers `taskForEnrolled(deviceId, setId)` and `awaitTombstone(deviceId, timeout)` in
  `FailedStorageDeviceSuite`, used by Tasks 8 and 9.

- [ ] **Step 1: Write the failing tests**

Add to `src/test/scala/org/aspen_ddp/aspen/common/metadata/management/FailedStorageDeviceSuite.scala`:

```scala
  // ---- Driving the task -----------------------------------------------------------

  private val fastPoll = Duration(50, MILLISECONDS)

  /** Build a task instance over the state object failStorageDevice enrolled. */
  protected def taskForEnrolled(deviceId: StorageDeviceId,
                                setId: StorageDeviceSetId): Future[FailedStorageDeviceDurableTask] =
    given ExecutionContext = executionContext
    enrolledTasks().map: enrolled =>
      enrolled.size should be(1)
      new FailedStorageDeviceDurableTask(
        DurableTaskPointer(enrolled.head._2), client, deviceId, setId, fastPoll)

  /** Poll until `deviceId` reads as tombstoned, or fail on timeout. A bare wait on the task's
   *  completion would hang the suite rather than fail if the loop stopped making progress. */
  protected def awaitTombstone(deviceId: StorageDeviceId, timeout: Duration): Future[Unit] =
    given ExecutionContext = executionContext
    val deadline = System.nanoTime() + timeout.toNanos
    def loop(): Future[Unit] =
      client.getStorageDeviceState(deviceId).flatMap: state =>
        if state.isFailed then
          Future.unit
        else if System.nanoTime() > deadline then
          Future.failed(new AssertionError(s"device ${deviceId.uuid} was not tombstoned"))
        else
          val p = Promise[Unit]()
          client.backgroundTaskManager.schedule(fastPoll)(p.success(()))
          p.future.flatMap(_ => loop())
    loop()

  atest("step 1 zeroes both ids and removes the device from its set and host"):
    given ExecutionContext = executionContext
    val setId = StorageDeviceSetId.BootstrapStorageDeviceSetId
    for
      _ <- net.createSecondDevice()
      _ <- waitForTransactionsToComplete()
      _ <- client.failStorageDevice(net.secondDeviceId)
      _ <- waitForTransactionsToComplete()

      _ <- taskForEnrolled(net.secondDeviceId, setId)
      _ <- awaitTombstone(net.secondDeviceId, Duration(30000, MILLISECONDS))

      state <- client.getStorageDeviceState(net.secondDeviceId)
      set <- client.getStorageDeviceSetState(setId)
      host <- client.getHostState(net.bootstrapHost.hostId)
    yield
      state.hostId should be(fixed_ids.FailedHostId)
      state.storageDeviceId should be(fixed_ids.FailedStorageDeviceId)
      state.isFailed should be(true)
      set.memberDevices should not contain net.secondDeviceId
      host.storageDevices should not contain net.secondDeviceId

  atest("step 1 against an already-tombstoned device changes nothing"):
    given ExecutionContext = executionContext
    val setId = StorageDeviceSetId.BootstrapStorageDeviceSetId
    for
      _ <- net.createSecondDevice()
      _ <- waitForTransactionsToComplete()
      _ <- client.failStorageDevice(net.secondDeviceId)
      _ <- waitForTransactionsToComplete()

      _ <- taskForEnrolled(net.secondDeviceId, setId)
      _ <- awaitTombstone(net.secondDeviceId, Duration(30000, MILLISECONDS))
      _ <- waitForTransactionsToComplete()
      first <- client.getStorageDeviceState(net.secondDeviceId)

      // A second instance over the same enrolled state re-runs step 1 from scratch.
      second <- taskForEnrolled(net.secondDeviceId, setId)
      _ <- awaitTombstone(net.secondDeviceId, Duration(30000, MILLISECONDS))
      _ <- waitForTransactionsToComplete()
      after <- client.getStorageDeviceState(net.secondDeviceId)
      set <- client.getStorageDeviceSetState(setId)
    yield
      after should be(first)
      set.memberDevices should not contain net.secondDeviceId
```

Also add the third test from Task 7's listing (`failStorageDevice refuses a device that is already
tombstoned`), which needs these two helpers.

Add the imports the new code needs:

```scala
import org.aspen_ddp.aspen.common.metadata.fixed_ids
import org.aspen_ddp.aspen.common.DataBuffer
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `sbt 'testOnly *FailedStorageDeviceSuite -- -z "step 1 zeroes"'`
Expected: FAIL with `AssertionError: device ... was not tombstoned` — the skeleton's `drive()`
finishes without writing anything.

- [ ] **Step 3: Implement step 1**

In `FailedStorageDeviceDurableTask.scala`, replace the placeholder `drive()` with:

```scala
  private def drive(): Future[Unit] =
    client.getStorageDeviceState(deviceId).flatMap: state =>
      if state.isFailed then
        // Step 1 is already done. The drain (step 2) goes here in the next task.
        Future.successful(finishOk())
      else
        tombstone()
```

and add below it:

```scala
  /** Step 1. One transaction: the device leaves its set, leaves its owning host, and has both
   *  ids zeroed.
   *
   *  Everything is re-read inside the transaction and every removal is self-healing, so a
   *  partially-applied step, a concurrent second invocation, and a plain retry all converge on
   *  the same final state. Re-drives rather than finishing: the drain has work to do.
   */
  private def tombstone(): Future[Unit] =
    val done = client.transactUntilSuccessful: tx =>
      for
        devPtr <- client.getStorageDevicePointer(deviceId)
        devKvos <- client.read(devPtr, "fail storage device")
        state = StorageDeviceState(devKvos)
        setPtr <- client.getStorageDeviceSetPointer(setId)
        setDos <- client.read(setPtr, "fail storage device")
        hostPtr <- client.getHostPointer(state.hostId)
        hostKvos <- client.read(hostPtr, "fail storage device")
      yield
        // Set: drop the device (self-healing if absent).
        val set = StorageDeviceSetState(setDos)
        val newSet = set.copy(memberDevices = set.memberDevices.filter(_ != deviceId))
        tx.overwrite(setPtr, setDos.revision, DataBuffer(newSet.toBytes))

        // Host: drop the device (self-healing if absent).
        val host = HostState(hostKvos)
        val newHost = host.removeStorageDevice(deviceId)
        tx.update(hostPtr, None, None,
          List(KeyRevision(HostState.StateKey, hostKvos.contents(HostState.StateKey).revision)),
          List(Insert(HostState.StateKey, newHost.encode())))

        // Device: zero both ids. Everything else -- the store map above all -- is preserved:
        // it is the drain's work list.
        val tombstoned = state.copy(hostId = fixed_ids.FailedHostId,
                                    storageDeviceId = fixed_ids.FailedStorageDeviceId)
        tx.update(devPtr, None, None,
          List(KeyRevision(StorageDeviceState.StateKey,
            devKvos.contents(StorageDeviceState.StateKey).revision)),
          List(Insert(StorageDeviceState.StateKey, tombstoned.encode())))

    done.transformWith:
      case Success(_) =>
        // scheduleRecheck, not run(): run()'s single-flight flag is still held by this pass, so
        // a direct call would be dropped.
        scheduleRecheck()
        Future.unit

      case Failure(err) =>
        logger.warn(s"Failed device ${deviceId.uuid}: tombstone transaction failed: $err")
        scheduleRecheck()
        Future.unit
```

`getStorageDevicePointer` looks the device up by the id in the tree, which the tombstone does not
change — only the id *inside* the state object is zeroed. So the device stays reachable under its
original id forever, which is what makes the drain and `show-device` work.

Add the imports the new code needs to `FailedStorageDeviceDurableTask.scala`:

```scala
import org.aspen_ddp.aspen.common.DataBuffer
import org.aspen_ddp.aspen.common.metadata.{HostState, StorageDeviceSetState, StorageDeviceState, fixed_ids}
import org.aspen_ddp.aspen.common.objects.Insert
import org.aspen_ddp.aspen.common.transaction.KeyValueUpdate.KeyRevision
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `sbt 'testOnly *FailedStorageDeviceSuite'`
Expected: PASS, 6 tests.

- [ ] **Step 5: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/common/metadata/management/FailedStorageDeviceDurableTask.scala \
        src/test/scala/org/aspen_ddp/aspen/common/metadata/management/FailedStorageDeviceSuite.scala
git commit -m "$(cat <<'EOF'
feat: tombstone the device in FailedStorageDeviceDurableTask step 1

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
EOF
)"
```

---

### Task 9: Step 2 — drain the tombstone

One store per pass, one transaction per store: select a replacement from the store's pool's set,
add a `Rebuilding` entry there, remove the store from the tombstone, repoint the pool, regenerate
the bootstrap config if this is a bootstrap-pool store, and nudge the destination host. The task
completes when the tombstone's store map is empty.

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/common/metadata/management/FailedStorageDeviceDurableTask.scala`
- Test: `src/test/scala/org/aspen_ddp/aspen/common/metadata/management/FailedStorageDeviceSuite.scala`

**Interfaces:**
- Consumes: `selectDeviceForRebuild` (Task 5), `BootstrapConfig.prepRadicleUpdate` (Task 6),
  `AspenClient.DeviceFailed` (Task 3), the skeleton and `tombstone()` (Tasks 7-8).
- Produces: nothing new outside the file.

- [ ] **Step 1: Write the failing tests**

Add to `src/test/scala/org/aspen_ddp/aspen/common/metadata/management/FailedStorageDeviceSuite.scala`:

```scala
  /** Await the task's completion under a deadline. A bare `task.completed` would hang the whole
   *  suite rather than fail if the loop ever stopped making progress. */
  private def awaitCompletion(task: FailedStorageDeviceDurableTask,
                              timeout: Duration): Future[Unit] =
    given ExecutionContext = executionContext
    val expired = Promise[Unit]()
    client.backgroundTaskManager.schedule(timeout):
      expired.tryFailure(new AssertionError(s"task did not complete within $timeout"))
    Future.firstCompletedOf(List(task.completed.map(_ => ()), expired.future))

  atest("the drain moves every store off the tombstone and completes"):
    given ExecutionContext = executionContext
    val setId = StorageDeviceSetId.BootstrapStorageDeviceSetId
    val failedId = StorageDeviceId.BootstrapStorageDeviceId
    for
      // A live destination in the same set. The bootstrap device carries all three stores.
      _ <- net.createSecondDevice()
      _ <- waitForTransactionsToComplete()

      _ <- client.failStorageDevice(failedId)
      _ <- waitForTransactionsToComplete()

      task <- taskForEnrolled(failedId, setId)
      _ <- awaitCompletion(task, Duration(30000, MILLISECONDS))
      _ <- waitForTransactionsToComplete()

      tombstone <- client.getStorageDeviceState(failedId)
      destination <- client.getStorageDeviceState(net.secondDeviceId)
      poolState <- client.getStoragePoolState(PoolId.BootstrapPoolId)
    yield
      tombstone.isFailed should be(true)
      tombstone.stores shouldBe empty

      destination.stores.size should be(3)
      destination.stores.values.foreach: entry =>
        entry.status should be(StorageDeviceState.StoreStatus.Rebuilding)
        entry.transferDevice should be(None)

      // The pool is repointed at the start of the rebuild, not at its end.
      poolState.stores.size should be(3)
      poolState.stores.foreach: entry =>
        entry.storageDeviceId should be(net.secondDeviceId)

  atest("the drain regenerates the bootstrap config for a bootstrap-pool store"):
    given ExecutionContext = executionContext
    val setId = StorageDeviceSetId.BootstrapStorageDeviceSetId
    val failedId = StorageDeviceId.BootstrapStorageDeviceId
    for
      _ <- net.createSecondDevice()
      _ <- waitForTransactionsToComplete()
      before <- client.getBootstrapConfig()

      _ <- client.failStorageDevice(failedId)
      _ <- waitForTransactionsToComplete()
      task <- taskForEnrolled(failedId, setId)
      _ <- awaitCompletion(task, Duration(30000, MILLISECONDS))
      _ <- waitForTransactionsToComplete()

      after <- client.getBootstrapConfig()
    yield
      // The second device is on the same host, so the YAML's host list is unchanged -- but it
      // is rewritten, and it still parses and still names all three stores. The assertion that
      // matters is that the write happened at all: a bootstrap-pool store changing device
      // without it leaves every host bootstrapping against stale state.
      after should not be before
      val cfg = BootstrapConfig.parseBootstrapConfig(after)
      cfg.hosts.map(_.stores.length).sum should be(3)

  atest("a resumed task picks up mid-drain"):
    given ExecutionContext = executionContext
    val setId = StorageDeviceSetId.BootstrapStorageDeviceSetId
    val failedId = StorageDeviceId.BootstrapStorageDeviceId
    for
      _ <- net.createSecondDevice()
      _ <- waitForTransactionsToComplete()
      _ <- client.failStorageDevice(failedId)
      _ <- waitForTransactionsToComplete()

      // Stop the first instance once it has moved at least one store, simulating a crash.
      first <- taskForEnrolled(failedId, setId)
      _ <- awaitStoresRemaining(failedId, atMost = 2, Duration(30000, MILLISECONDS))
      _ = first.stop()

      // A fresh instance over the same enrolled state finishes the job.
      second <- taskForEnrolled(failedId, setId)
      _ <- awaitCompletion(second, Duration(30000, MILLISECONDS))
      _ <- waitForTransactionsToComplete()

      tombstone <- client.getStorageDeviceState(failedId)
      destination <- client.getStorageDeviceState(net.secondDeviceId)
    yield
      tombstone.stores shouldBe empty
      destination.stores.size should be(3)

  /** Poll until the tombstone holds at most `atMost` stores, or fail on timeout. */
  private def awaitStoresRemaining(deviceId: StorageDeviceId,
                                   atMost: Int,
                                   timeout: Duration): Future[Unit] =
    given ExecutionContext = executionContext
    val deadline = System.nanoTime() + timeout.toNanos
    def loop(): Future[Unit] =
      client.getStorageDeviceState(deviceId).flatMap: state =>
        if state.stores.size <= atMost then
          Future.unit
        else if System.nanoTime() > deadline then
          Future.failed(new AssertionError(
            s"device ${deviceId.uuid} still holds ${state.stores.size} stores"))
        else
          val p = Promise[Unit]()
          client.backgroundTaskManager.schedule(fastPoll)(p.success(()))
          p.future.flatMap(_ => loop())
    loop()
```

Add the imports the new code needs:

```scala
import org.aspen_ddp.aspen.common.metadata.BootstrapConfig
import org.aspen_ddp.aspen.common.pool.PoolId
```

`DurableTask.stop()` is the public stop hook — confirm the exact name against
`src/main/scala/org/aspen_ddp/aspen/compute/DurableTask.scala` and use whatever it is; `onStop()`
is the protected override, not the caller-facing entry point.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `sbt 'testOnly *FailedStorageDeviceSuite -- -z "drain moves every store"'`
Expected: FAIL — the tombstone still holds all three stores and the destination holds none, because
`drive()` finishes as soon as the tombstone exists.

- [ ] **Step 3: Implement the drain**

In `FailedStorageDeviceDurableTask.scala`, change `drive()` to hand off to the drain instead of
finishing:

```scala
  private def drive(): Future[Unit] =
    client.getStorageDeviceState(deviceId).flatMap: state =>
      if state.isFailed then
        drain(state)
      else
        tombstone()
```

and add:

```scala
  /** The tombstone's own store map is the work list. Sorted so passes are deterministic and a
   *  resumed task picks the same next store a crashed one would have. */
  private def nextStore(state: StorageDeviceState): Option[StoreId] =
    state.stores.keys.toList.sortBy(sid => (sid.poolId.uuid, sid.poolIndex)).headOption

  /** Step 2. One store per pass.
   *
   *  Selection happens BEFORE the pool is repointed, and against the pool's own set rather than
   *  the failed device's former set. The former is required: selectDeviceForRebuild derives the
   *  failed device from `poolState.stores(poolIndex)`, so a pool already repointed would
   *  hard-exclude the wrong device. The latter means a store lost during a pool migration is
   *  rebuilt directly into the migration's target rather than into the set the pool is leaving.
   */
  private def drain(state: StorageDeviceState): Future[Unit] = nextStore(state) match
    case None =>
      Future.successful(finishOk())

    case Some(storeId) =>
      val moved = for
        poolState <- client.getStoragePoolState(storeId.poolId)
        set <- client.getStorageDeviceSetState(poolState.storageDeviceSet)
        destinationId <- set.selectDeviceForRebuild(storeId.poolId, storeId.poolIndex, client)
        nudge <- moveStore(storeId, destinationId)
      yield nudge

      moved.transformWith:
        case Success(nudge) =>
          // A best-effort wake-up so the destination host starts promptly rather than waiting
          // out Main.CheckStorageDevicesPeriod. The poll is the guarantee; this is the
          // optimization.
          client.sendBestEffortHostMessage(nudge)
          scheduleRecheck()
          Future.unit

        case Failure(err) =>
          // AllocationError (no device with room), a lost transaction race, a read failure: all
          // transient from this task's point of view. The store stays on the tombstone and the
          // next poll retries, so capacity appearing later is enough to recover.
          logger.warn(s"Failed device ${deviceId.uuid}: could not place $storeId: $err")
          scheduleRecheck()
          Future.unit

  /** The one transaction that moves a single store off the tombstone. Returns the nudge to send
   *  once it has committed -- built inside so a retried attempt cannot double-send, and sent by
   *  the caller rather than from tx.result so the returned Future actually waits for it. */
  private def moveStore(storeId: StoreId,
                        destinationId: StorageDeviceId): Future[CheckStorageDevice] =

    def onFail(err: Throwable): Future[Unit] = err match
      case e: NoSuchElementException => throw StopRetrying(e)
      case e: AspenClient.DeviceFailed => throw StopRetrying(e)
      case e: StoreAlreadyMoved => throw StopRetrying(e)

    client.transactUntilSuccessfulWithRecovery(onFail): tx =>
      given Transaction = tx

      for
        devPtr <- client.getStorageDevicePointer(deviceId)
        devKvos <- client.read(devPtr, "failed device drain")
        tombstoneState = StorageDeviceState(devKvos)
        dstPtr <- client.getStorageDevicePointer(destinationId)
        dstKvos <- client.read(dstPtr, "failed device drain")
        dstState = StorageDeviceState(dstKvos)
        poolPtr <- client.getStoragePoolPointer(storeId.poolId)
        poolKvos <- client.read(poolPtr, "failed device drain")
        poolCfg = StoragePoolState(poolKvos)
        _ <-
          if !tombstoneState.stores.contains(storeId) then
            // Another pass, or a concurrent fail-storage-device, already moved it.
            throw new StoreAlreadyMoved(storeId)
          else if dstState.isFailed then
            // Structurally unreachable once step 1 has removed the tombstone from its set, but
            // the pool state driving selection can be stale.
            throw AspenClient.DeviceFailed(destinationId)
          else
            BootstrapConfig.prepRadicleUpdate(client, storeId, poolCfg, dstState.hostId)
      yield
        // Destination: gains a Rebuilding entry. This is the entire message to the consumer.
        val newDst = dstState.setStoreEntry(
          storeId, StorageDeviceState.StoreStatus.Rebuilding, None)
        tx.update(dstPtr, None, None,
          List(KeyRevision(StorageDeviceState.StateKey,
            dstKvos.contents(StorageDeviceState.StateKey).revision)),
          List(Insert(StorageDeviceState.StateKey, newDst.encode())))

        // Tombstone: loses the store. This is also the progress record -- an empty store map is
        // what completes the task.
        val newTombstone = tombstoneState.removeStore(storeId)
        tx.update(devPtr, None, None,
          List(KeyRevision(StorageDeviceState.StateKey,
            devKvos.contents(StorageDeviceState.StateKey).revision)),
          List(Insert(StorageDeviceState.StateKey, newTombstone.encode())))

        // Pool: repointed now, at the start of the rebuild rather than at its end. Reads of a
        // rebuilding store fail until it is reconstructed, but that is equally true of a store
        // on a dead device, and the pool must stop naming the dead device before anything can
        // route around it. The rebalancer already excludes non-Active stores from movement and
        // from the write-threshold count.
        poolCfg.stores(storeId.poolIndex) =
          StoragePoolState.StoreEntry(dstState.hostId, destinationId)
        tx.update(poolPtr, None, None,
          List(KeyRevision(StoragePoolState.ConfigKey,
            poolKvos.contents(StoragePoolState.ConfigKey).revision)),
          List(Insert(StoragePoolState.ConfigKey, poolCfg.encode())))

        CheckStorageDevice(dstState.hostId, client.clientId, destinationId)
```

Add the signal class to the companion object, next to the keys:

```scala
  /** The store this pass chose is no longer on the tombstone -- another pass, or a concurrent
   *  invocation, moved it. Not an error: the pass is abandoned and the next poll picks up
   *  whatever is left. */
  class StoreAlreadyMoved(storeId: StoreId)
      extends Throwable(s"store $storeId is no longer recorded on the failed device")
```

and import it into the class body alongside the other companion imports:

```scala
  import FailedStorageDeviceDurableTask.StoreAlreadyMoved
```

Add the remaining imports to the file:

```scala
import org.aspen_ddp.aspen.client.StopRetrying
import org.aspen_ddp.aspen.common.metadata.{BootstrapConfig, StoragePoolState}
import org.aspen_ddp.aspen.common.network.CheckStorageDevice
import org.aspen_ddp.aspen.common.store.StoreId
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `sbt 'testOnly *FailedStorageDeviceSuite'`
Expected: PASS, 9 tests.

- [ ] **Step 5: Run the neighbouring suites**

Run: `sbt 'testOnly *MigratePoolToSetSuite *StorageDeviceSetIntegrationSuite *TombstoneGuardSuite *StoreManagerDeviceDiscoverySuite'`
Expected: PASS, unchanged.

- [ ] **Step 6: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/common/metadata/management/FailedStorageDeviceDurableTask.scala \
        src/test/scala/org/aspen_ddp/aspen/common/metadata/management/FailedStorageDeviceSuite.scala
git commit -m "$(cat <<'EOF'
feat: drain a tombstoned device's stores onto live devices

Each store moves in one transaction that marks it Rebuilding on the
destination, drops it from the tombstone, and repoints the pool.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
EOF
)"
```

---

### Task 10: The `fail-storage-device` CLI command

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/cmdline/Main.scala` (parser entry near line 428,
  dispatch near line 741, implementation near `move_device_to_set` at line 1504)

**Interfaces:**
- Consumes: `AspenClient.failStorageDevice`, `AspenClient.DeviceAlreadyFailed` (Task 7).
- Produces: nothing consumed elsewhere.

- [ ] **Step 1: Add the parser entry**

In `src/main/scala/org/aspen_ddp/aspen/cmdline/Main.scala`, add immediately after the
`move-device-to-set` command block (which ends at line 447):

```scala
      cmd("fail-storage-device").text("Declares a storage device dead and rebuilds its stores elsewhere").
        action((_, c) => c.copy(mode = "fail-storage-device")).
        children(
          arg[File]("<bootstrap-config-file>").text("Bootstrap Configuration File").
            action((x, c) => c.copy(bootstrapConfigFile = x)).
            validate(x => if (x.exists()) success else failure(s"Config file does not exist: $x")),

          arg[String]("<device-uuid>").text("UUID of the storage device to declare failed").
            action((x, c) => c.copy(deviceId = x)).
            validate { x =>
              try
                UUID.fromString(x)
                success
              catch
                case _: Throwable => failure("Storage device id must be a valid UUID")
            },
        )
```

- [ ] **Step 2: Add the dispatch entry**

Add immediately after the `move-device-to-set` dispatch line (line 741):

```scala
            case "fail-storage-device" => fail_storage_device(bootstrapConfigPath, cfg.deviceId)
```

- [ ] **Step 3: Add the implementation**

Add immediately after `move_device_to_set` (which ends at line 1537):

```scala
  def fail_storage_device(bootstrapConfigFile: os.Path,
                          deviceIdStr: String): Int = {

    configureLogging()

    val (client, network, _) = createAmoebaClient(bootstrapConfigFile)

    network.startIoThread(client)

    given ExecutionContext = client.clientContext

    val deviceId = StorageDeviceId(UUID.fromString(deviceIdStr))

    val f = client.failStorageDevice(deviceId)

    // getStorageDevicePointer throws NoSuchElementException for an unknown id;
    // failStorageDevice throws DeviceAlreadyFailed when the device already carries a tombstone.
    def reportError(cause: Throwable): Unit = cause match
      case _: NoSuchElementException =>
        println(s"Error: storage device '$deviceIdStr' not found")
      case _: AspenClient.DeviceAlreadyFailed =>
        println(s"Error: storage device '$deviceIdStr' has already been declared failed")
      case e =>
        println(s"Error failing storage device: ${e.getMessage}")

    awaitAndReport(f):
      case Success(_) =>
        println(s"Storage device '$deviceIdStr' declared failed.")
        println("Its stores will be rebuilt onto live devices; watch progress with:")
        println(s"  show-device $deviceIdStr")
      case Failure(err) => reportError(err)
  }
```

Confirm `AspenClient` and `StorageDeviceId` are already imported in `Main.scala` (they are, for
`move_device_to_set` and `transfer_store`); add whichever is missing.

- [ ] **Step 4: Verify the command parses and reports cleanly**

Run: `sbt 'runMain org.aspen_ddp.aspen.cmdline.Main --help'`
Expected: the help output lists `fail-storage-device` with its two arguments.

Run: `sbt compile`
Expected: no errors.

- [ ] **Step 5: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/cmdline/Main.scala
git commit -m "$(cat <<'EOF'
feat: add the fail-storage-device CLI command

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
EOF
)"
```

---

### Task 11: Full-suite regression pass

**Files:**
- No source changes expected. Fix anything this pass turns up.

**Interfaces:**
- Consumes: everything above.
- Produces: nothing.

- [ ] **Step 1: Run the full suite**

Run: `sbt test`
Expected: PASS.

- [ ] **Step 2: Fix and re-run**

If anything fails, fix it and re-run `sbt test` until green. Commit each fix separately with a
message naming what broke:

```bash
git commit -m "$(cat <<'EOF'
fix: <what broke and why>

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
EOF
)"
```

- [ ] **Step 3: Update TODO.txt**

Strike the producer-half bullets from `TODO.txt` — the `fail-storage-device` command, the
`FailedStorageDeviceDurableTask`, and the "Add guard code to StoreTransfer and Pool allocation
logic" bullet — leaving the consumer-half bullets (the `aspen.server.rebuild` package, the
StoreManager polling clause, `rebuild-state.yaml`, and checkpointing) for the next plan.

```bash
git add TODO.txt
git commit -m "$(cat <<'EOF'
docs: strike the completed producer-half items from TODO.txt

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
EOF
)"
```
