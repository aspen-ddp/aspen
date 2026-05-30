# Storage Device Usage Manager Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a `StorageDeviceUsageManager` that updates `StorageDeviceState` objects when `currentUsage` or `totalSize` change by more than 0.5%, integrated into `StoreManager`'s periodic usage tracking task.

**Architecture:** A new `StorageDeviceUsageManager` class in `server/usage/` follows the same pattern as `StoragePoolUsageManager` — cached state, 0.5% threshold check, transactional update. Unlike the pool manager, it has no downstream durable tasks. The existing periodic task in `StoreManager.startUsageTracking` is extended to iterate over local storage devices in addition to stores.

**Tech Stack:** Scala 3, ScalaTest (AsyncFunSuite), Aspen client transaction API

---

## File Structure

- **Create:** `src/main/scala/org/aspen_ddp/aspen/server/usage/StorageDeviceUsageManager.scala` — the new manager class
- **Modify:** `src/main/scala/org/aspen_ddp/aspen/server/StoreManager.scala` — add device usage manager instance and extend periodic task
- **Create:** `src/test/scala/org/aspen_ddp/aspen/server/usage/StorageDeviceUsageManagerSuite.scala` — integration tests

---

### Task 1: Write StorageDeviceUsageManager with tests for no-op behavior

**Files:**
- Create: `src/test/scala/org/aspen_ddp/aspen/server/usage/StorageDeviceUsageManagerSuite.scala`
- Create: `src/main/scala/org/aspen_ddp/aspen/server/usage/StorageDeviceUsageManager.scala`

- [ ] **Step 1: Write the test file with two initial tests**

The test harness provides `client`, `net.storageDeviceId` (the bootstrap device), and a `readDeviceState()` helper. The first test verifies a first report updates the state. The second test verifies a repeated identical report is a no-op.

```scala
package org.aspen_ddp.aspen.server.usage

import org.aspen_ddp.aspen.IntegrationTestSuite
import org.aspen_ddp.aspen.common.metadata.{StorageDeviceId, StorageDeviceState}

import scala.concurrent.{ExecutionContext, Future}

class StorageDeviceUsageManagerSuite extends IntegrationTestSuite:

  private def readDeviceState(): Future[StorageDeviceState] =
    given ExecutionContext = executionContext
    for
      devPtr <- client.getStorageDevicePointer(net.storageDeviceId)
      devKvos <- client.read(devPtr)
    yield
      StorageDeviceState(devKvos)

  atest("First report updates device state"):
    given ExecutionContext = executionContext
    val mgr = new StorageDeviceUsageManager(client)

    for
      state0 <- readDeviceState()
      _ = state0.currentUsage should be(0L)
      _ = state0.totalSize should be(0L)

      _ <- mgr.updateDeviceUsage(net.storageDeviceId, 5000L, 100000L)
      _ <- waitForTransactionsToComplete()

      state1 <- readDeviceState()
    yield
      state1.currentUsage should be(5000L)
      state1.totalSize should be(100000L)

  atest("No update when values are unchanged"):
    given ExecutionContext = executionContext
    val mgr = new StorageDeviceUsageManager(client)

    for
      _ <- mgr.updateDeviceUsage(net.storageDeviceId, 5000L, 100000L)
      _ <- waitForTransactionsToComplete()

      devPtr <- client.getStorageDevicePointer(net.storageDeviceId)
      kvos1 <- client.read(devPtr)
      rev1 = kvos1.contents(StorageDeviceState.StateKey).revision

      _ <- mgr.updateDeviceUsage(net.storageDeviceId, 5000L, 100000L)
      _ <- waitForTransactionsToComplete()

      kvos2 <- client.read(devPtr)
    yield
      kvos2.contents(StorageDeviceState.StateKey).revision should be(rev1)
```

- [ ] **Step 2: Write the minimal StorageDeviceUsageManager implementation**

```scala
package org.aspen_ddp.aspen.server.usage

import org.aspen_ddp.aspen.client.{AspenClient, FatalReadError, StopRetrying}
import org.aspen_ddp.aspen.common.metadata.{StorageDeviceId, StorageDeviceState}
import org.aspen_ddp.aspen.common.objects.{Insert, KeyValueObjectPointer}
import org.aspen_ddp.aspen.common.transaction.KeyValueUpdate.KeyRevision
import org.apache.logging.log4j.scala.Logging

import scala.concurrent.{ExecutionContext, Future}

class StorageDeviceUsageManager(client: AspenClient) extends Logging:

  given ExecutionContext = client.clientContext

  private var cachedPointers: Map[StorageDeviceId, KeyValueObjectPointer] = Map()
  private var cachedUsage: Map[StorageDeviceId, Long] = Map()
  private var cachedTotalSize: Map[StorageDeviceId, Long] = Map()

  private def exceedsThreshold(newValue: Long, oldValue: Long): Boolean =
    math.abs(newValue - oldValue).toDouble / math.max(oldValue, 1) > 0.005

  private def getDevicePointer(storageDeviceId: StorageDeviceId): Future[KeyValueObjectPointer] =
    val cached = synchronized(cachedPointers.get(storageDeviceId))
    cached match
      case Some(ptr) => Future.successful(ptr)
      case None =>
        client.getStorageDevicePointer(storageDeviceId).map: ptr =>
          synchronized(cachedPointers += storageDeviceId -> ptr)
          ptr

  def updateDeviceUsage(storageDeviceId: StorageDeviceId, currentUsage: Long, totalSize: Long): Future[Unit] =
    val (lastUsage, lastTotalSize) = synchronized:
      (cachedUsage.getOrElse(storageDeviceId, 0L), cachedTotalSize.getOrElse(storageDeviceId, 0L))

    if !exceedsThreshold(currentUsage, lastUsage) && !exceedsThreshold(totalSize, lastTotalSize) then
      Future.unit
    else
      def onFail(err: Throwable): Future[Unit] = err match
        case _: FatalReadError => throw StopRetrying(err)
        case _ => Future.unit

      client.transactUntilSuccessfulWithRecovery(onFail): tx =>
        for
          devPtr <- getDevicePointer(storageDeviceId)
          devKvos <- client.read(devPtr)
          state = StorageDeviceState(devKvos)
          updatedState = state.copy(currentUsage = currentUsage, totalSize = totalSize)
          requirements = List(KeyRevision(StorageDeviceState.StateKey, devKvos.contents(StorageDeviceState.StateKey).revision))
          operations = List(Insert(StorageDeviceState.StateKey, updatedState.encode()))
        yield
          tx.update(devPtr, None, None, requirements, operations)

      .map: _ =>
        synchronized:
          cachedUsage += storageDeviceId -> currentUsage
          cachedTotalSize += storageDeviceId -> totalSize
```

- [ ] **Step 3: Run the tests**

Run: `sbt 'testOnly *StorageDeviceUsageManagerSuite'`
Expected: Both tests PASS

- [ ] **Step 4: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/server/usage/StorageDeviceUsageManager.scala src/test/scala/org/aspen_ddp/aspen/server/usage/StorageDeviceUsageManagerSuite.scala
git commit -m "Add StorageDeviceUsageManager with initial tests"
```

---

### Task 2: Add threshold behavior tests

**Files:**
- Modify: `src/test/scala/org/aspen_ddp/aspen/server/usage/StorageDeviceUsageManagerSuite.scala`

- [ ] **Step 1: Add test for significant currentUsage change**

Append to `StorageDeviceUsageManagerSuite`:

```scala
  atest("Update when currentUsage changes significantly"):
    given ExecutionContext = executionContext
    val mgr = new StorageDeviceUsageManager(client)

    for
      _ <- mgr.updateDeviceUsage(net.storageDeviceId, 10000L, 100000L)
      _ <- waitForTransactionsToComplete()

      devPtr <- client.getStorageDevicePointer(net.storageDeviceId)
      kvos1 <- client.read(devPtr)
      rev1 = kvos1.contents(StorageDeviceState.StateKey).revision

      _ <- mgr.updateDeviceUsage(net.storageDeviceId, 10200L, 100000L)
      _ <- waitForTransactionsToComplete()

      kvos2 <- client.read(devPtr)
      state2 = StorageDeviceState(kvos2)
    yield
      kvos2.contents(StorageDeviceState.StateKey).revision should not be rev1
      state2.currentUsage should be(10200L)
      state2.totalSize should be(100000L)

  atest("Update when totalSize changes significantly"):
    given ExecutionContext = executionContext
    val mgr = new StorageDeviceUsageManager(client)

    for
      _ <- mgr.updateDeviceUsage(net.storageDeviceId, 10000L, 100000L)
      _ <- waitForTransactionsToComplete()

      devPtr <- client.getStorageDevicePointer(net.storageDeviceId)
      kvos1 <- client.read(devPtr)
      rev1 = kvos1.contents(StorageDeviceState.StateKey).revision

      _ <- mgr.updateDeviceUsage(net.storageDeviceId, 10000L, 101000L)
      _ <- waitForTransactionsToComplete()

      kvos2 <- client.read(devPtr)
      state2 = StorageDeviceState(kvos2)
    yield
      kvos2.contents(StorageDeviceState.StateKey).revision should not be rev1
      state2.currentUsage should be(10000L)
      state2.totalSize should be(101000L)

  atest("No update when both changes are below threshold"):
    given ExecutionContext = executionContext
    val mgr = new StorageDeviceUsageManager(client)

    for
      _ <- mgr.updateDeviceUsage(net.storageDeviceId, 10000L, 100000L)
      _ <- waitForTransactionsToComplete()

      devPtr <- client.getStorageDevicePointer(net.storageDeviceId)
      kvos1 <- client.read(devPtr)
      rev1 = kvos1.contents(StorageDeviceState.StateKey).revision

      _ <- mgr.updateDeviceUsage(net.storageDeviceId, 10040L, 100400L)
      _ <- waitForTransactionsToComplete()

      kvos2 <- client.read(devPtr)
    yield
      kvos2.contents(StorageDeviceState.StateKey).revision should be(rev1)
```

- [ ] **Step 2: Run the tests**

Run: `sbt 'testOnly *StorageDeviceUsageManagerSuite'`
Expected: All 5 tests PASS

- [ ] **Step 3: Commit**

```bash
git add src/test/scala/org/aspen_ddp/aspen/server/usage/StorageDeviceUsageManagerSuite.scala
git commit -m "Add threshold behavior tests for StorageDeviceUsageManager"
```

---

### Task 3: Integrate into StoreManager

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/server/StoreManager.scala`

- [ ] **Step 1: Add the import**

Add to the imports in `StoreManager.scala`:

```scala
import org.aspen_ddp.aspen.server.usage.StorageDeviceUsageManager
```

(The `StoragePoolUsageManager` import already exists on a nearby line.)

- [ ] **Step 2: Add the deviceUsageManager field**

Add the field declaration next to the existing `poolUsageManager` field (around the line `private val poolUsageManager = new StoragePoolUsageManager(client)`):

```scala
  private val deviceUsageManager = new StorageDeviceUsageManager(client)
```

- [ ] **Step 3: Extend the periodic task in startUsageTracking**

Replace the existing `startUsageTracking` method body. The current implementation is:

```scala
  private def startUsageTracking(executor: TaskExecutor): Unit =
    poolUsageManager.setTaskExecutor(executor)
    usageUpdateTask = Some(backgroundTasks.schedulePeriodic(Duration(20, SECONDS)):
      synchronized:
        stores.valuesIterator.foreach: store =>
          poolUsageManager.updateStoreSize(store.storeId, store.estimateSize())
    )
```

Replace with:

```scala
  private def startUsageTracking(executor: TaskExecutor): Unit =
    poolUsageManager.setTaskExecutor(executor)
    usageUpdateTask = Some(backgroundTasks.schedulePeriodic(Duration(20, SECONDS)):
      synchronized:
        stores.valuesIterator.foreach: store =>
          poolUsageManager.updateStoreSize(store.storeId, store.estimateSize())
        storageDevices.valuesIterator.foreach: sds =>
          deviceUsageManager.updateDeviceUsage(sds.storageDeviceId, sds.currentUsage, sds.totalSize)
    )
```

- [ ] **Step 4: Run the full test suite to verify no regressions**

Run: `sbt 'testOnly *StoragePoolUsageManagerSuite *StorageDeviceUsageManagerSuite'`
Expected: All tests PASS

- [ ] **Step 5: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/server/StoreManager.scala
git commit -m "Integrate StorageDeviceUsageManager into StoreManager periodic usage tracking"
```
