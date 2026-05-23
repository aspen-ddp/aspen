# Storage Pool Usage Tracking Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Track per-store and per-pool usage metrics, updating StoragePoolState objects when sizes change significantly, and optionally triggering durable tasks to propagate usage to allocation groups.

**Architecture:** StoragePoolState gets three new fields (currentUsage, maximumStoreSize, allocationGroups). A new StoragePoolUsageManager class manages threshold-based updates to pool KV objects. StoreManager initializes a TaskExecutor from the HostState KV object and runs a periodic task that feeds store sizes into the usage manager. An UpdateAllocationGroupUsageTask (placeholder) is prepared when pool-level usage changes and allocation groups exist.

**Tech Stack:** Scala 3, Protocol Buffers, ScalaTest (IntegrationTestSuite)

---

## File Structure

| File | Action | Responsibility |
|------|--------|---------------|
| `src/main/protobuf/codec.proto` | Modify | Add 3 fields to StoragePoolState message |
| `src/main/scala/org/aspen_ddp/aspen/common/metadata/StoragePoolState.scala` | Modify | Add 3 fields + companion method |
| `src/main/scala/org/aspen_ddp/aspen/common/network/Codec.scala` | Modify | Encode/decode new StoragePoolState fields |
| `src/main/scala/org/aspen_ddp/aspen/common/util/package.scala` | Modify | Add long2byte/byte2long helpers |
| `src/main/scala/org/aspen_ddp/aspen/server/usage/UpdateAllocationGroupUsageTask.scala` | Create | Placeholder durable task for allocation group propagation |
| `src/main/scala/org/aspen_ddp/aspen/client/internal/StaticTypeRegistry.scala` | Modify | Register UpdateAllocationGroupUsageTask |
| `src/main/scala/org/aspen_ddp/aspen/server/usage/StoragePoolUsageManager.scala` | Create | Core usage tracking logic |
| `src/main/scala/org/aspen_ddp/aspen/server/StoreManager.scala` | Modify | TaskExecutor init + periodic usage task |
| `src/test/scala/org/aspen_ddp/aspen/server/usage/StoragePoolUsageManagerSuite.scala` | Create | Integration tests |

---

### Task 1: Add Long Byte Conversion Utilities

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/common/util/package.scala`

- [ ] **Step 1: Add long2byte and byte2long to the util package**

In `src/main/scala/org/aspen_ddp/aspen/common/util/package.scala`, add after the `byte2int` function (line 39):

```scala
  def long2byte(l: Long): Array[Byte] = {
    val arr = new Array[Byte](8)
    val bb = ByteBuffer.wrap(arr)
    bb.putLong(l)
    arr
  }
  def byte2long(arr: Array[Byte]): Long = ByteBuffer.wrap(arr).getLong()
```

- [ ] **Step 2: Verify compilation**

Run: `sbt compile`
Expected: BUILD SUCCESSFUL

- [ ] **Step 3: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/common/util/package.scala
git commit -m "Add long2byte and byte2long utilities"
```

---

### Task 2: Extend StoragePoolState Protobuf and Scala Model

**Files:**
- Modify: `src/main/protobuf/codec.proto:469-476`
- Modify: `src/main/scala/org/aspen_ddp/aspen/common/metadata/StoragePoolState.scala`
- Modify: `src/main/scala/org/aspen_ddp/aspen/common/network/Codec.scala:1048-1070`

- [ ] **Step 1: Add fields to protobuf message**

In `src/main/protobuf/codec.proto`, modify the `StoragePoolState` message (line 469) to add three new fields:

```protobuf
message StoragePoolState {
  UUID poolId = 1;
  string name = 2;
  IDA ida = 3;
  uint32 maxObjectSize = 4;
  repeated PoolStoreEntry stores = 5;
  BackendConfig backendConfig = 6;
  int64 currentUsage = 7;
  int64 maximumStoreSize = 8;
  repeated UUID allocationGroups = 9;
}
```

- [ ] **Step 2: Add fields to Scala case class and companion object**

In `src/main/scala/org/aspen_ddp/aspen/common/metadata/StoragePoolState.scala`, add the import for `UUID` and update:

```scala
package org.aspen_ddp.aspen.common.metadata

import org.aspen_ddp.aspen.client.KeyValueObjectState
import org.aspen_ddp.aspen.codec
import org.aspen_ddp.aspen.common.ida.IDA
import org.aspen_ddp.aspen.common.network.Codec
import org.aspen_ddp.aspen.common.objects.Key
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.server.store.backend.BackendConfig

import java.util.UUID

object StoragePoolState:
  private [aspen] val ConfigKey = Key(Array[Byte](0))
  private [aspen] val ErrorTreeKey = Key(Array[Byte](1))
  private [aspen] val AllocationTreeKey = Key(Array[Byte](2))

  val BootstrapPoolId: PoolId = fixed_ids.BootstrapPoolId

  case class StoreEntry(hostId: HostId, storageDeviceId: StorageDeviceId)

  def getStoreUsageKey(poolIndex: Byte): Key = Key(s"sz${poolIndex}")

  def apply(cfg: Array[Byte]): StoragePoolState = Codec.decode(codec.StoragePoolState.parseFrom(cfg))

  def apply(kvos: KeyValueObjectState): StoragePoolState = StoragePoolState(kvos.contents(ConfigKey).value.bytes)


final case class StoragePoolState(
                                   poolId: PoolId,
                                   name: String,
                                   ida: IDA,
                                   maxObjectSize: Option[Int],
                                   stores: Array[StoragePoolState.StoreEntry],
                                   backendConfig: BackendConfig,
                                   currentUsage: Long = 0,
                                   maximumStoreSize: Long = 0,
                                   allocationGroups: List[UUID] = Nil
                                 ):

  def encode(): Array[Byte] = Codec.encode(this).toByteArray
```

- [ ] **Step 3: Update Codec.scala encode method**

In `src/main/scala/org/aspen_ddp/aspen/common/network/Codec.scala`, update the `encode(o: StoragePoolState)` method (line 1048) to:

```scala
  def encode(o: StoragePoolState): codec.StoragePoolState =
    val builder = codec.StoragePoolState.newBuilder()

    builder.setPoolId(encodeUUID(o.poolId.uuid))
    builder.setName(o.name)
    builder.setIda(encode(o.ida))
    builder.setMaxObjectSize(o.maxObjectSize.getOrElse(0))
    builder.setBackendConfig(encodeBackendConfig(o.backendConfig))

    o.stores.foreach: storeEntry =>
      builder.addStores(encode(storeEntry))

    builder.setCurrentUsage(o.currentUsage)
    builder.setMaximumStoreSize(o.maximumStoreSize)
    o.allocationGroups.foreach: uuid =>
      builder.addAllocationGroups(encodeUUID(uuid))

    builder.build
```

- [ ] **Step 4: Update Codec.scala decode method**

Update the `decode(m: codec.StoragePoolState)` method (line 1062) to:

```scala
  def decode(m: codec.StoragePoolState): StoragePoolState =
    val poolId = PoolId(decodeUUID(m.getPoolId))
    val name = m.getName
    val ida = decode(m.getIda)
    val maxObjectSize = if m.getMaxObjectSize == 0 then None else Some(m.getMaxObjectSize)
    val stores = m.getStoresList.asScala.map(decode).toArray
    val backendConfig = decodeBackendConfig(m.getBackendConfig)
    val currentUsage = m.getCurrentUsage
    val maximumStoreSize = m.getMaximumStoreSize
    val allocationGroups = m.getAllocationGroupsList.asScala.map(decodeUUID).toList

    StoragePoolState(poolId, name, ida, maxObjectSize, stores, backendConfig,
      currentUsage, maximumStoreSize, allocationGroups)
```

- [ ] **Step 5: Verify compilation**

Run: `sbt compile`
Expected: BUILD SUCCESSFUL. Any existing code that constructs `StoragePoolState` without the new fields will still compile due to the defaults.

- [ ] **Step 6: Run existing tests to verify backward compatibility**

Run: `sbt test`
Expected: All existing tests pass. The default values (0, 0, Nil) ensure backward compatibility.

- [ ] **Step 7: Commit**

```bash
git add src/main/protobuf/codec.proto \
  src/main/scala/org/aspen_ddp/aspen/common/metadata/StoragePoolState.scala \
  src/main/scala/org/aspen_ddp/aspen/common/network/Codec.scala
git commit -m "Add currentUsage, maximumStoreSize, allocationGroups to StoragePoolState"
```

---

### Task 3: Create UpdateAllocationGroupUsageTask

**Files:**
- Create: `src/main/scala/org/aspen_ddp/aspen/server/usage/UpdateAllocationGroupUsageTask.scala`
- Modify: `src/main/scala/org/aspen_ddp/aspen/client/internal/StaticTypeRegistry.scala`

- [ ] **Step 1: Create the usage package directory**

Run: `mkdir -p src/main/scala/org/aspen_ddp/aspen/server/usage`

- [ ] **Step 2: Create UpdateAllocationGroupUsageTask**

Create `src/main/scala/org/aspen_ddp/aspen/server/usage/UpdateAllocationGroupUsageTask.scala`:

```scala
package org.aspen_ddp.aspen.server.usage

import java.util.UUID
import org.aspen_ddp.aspen.client.{AspenClient, KeyValueObjectState, Transaction}
import org.aspen_ddp.aspen.common.HLCTimestamp
import org.aspen_ddp.aspen.common.objects.{Key, ObjectRevision}
import org.aspen_ddp.aspen.common.util.{uuid2byte, byte2uuid, long2byte, byte2long}
import org.aspen_ddp.aspen.compute.{DurableTask, DurableTaskPointer, DurableTaskFactory, TaskExecutor}

import scala.concurrent.{Future, Promise}

object UpdateAllocationGroupUsageTask extends DurableTaskFactory:
  val typeUUID: UUID = UUID.fromString("7A3F9B2E-1D4C-4E8A-B5F7-6C9D0E2A8B1F")

  private val TimestampKey = Key(1)
  private val CurrentUsageKey = Key(2)
  private val MaxStoreSizeKey = Key(3)
  private val AllocationGroupCountKey = Key(4)
  private val AllocationGroupBaseKey = 5

  def createTask(client: AspenClient,
                 pointer: DurableTaskPointer,
                 revision: ObjectRevision,
                 state: Map[Key, KeyValueObjectState.ValueState]): DurableTask =
    new UpdateAllocationGroupUsageTask(pointer)

  def prepareTask(timestamp: HLCTimestamp,
                  currentUsage: Long,
                  maximumStoreSize: Long,
                  allocationGroups: List[UUID],
                  taskExecutor: TaskExecutor)
                 (using tx: Transaction): Future[Future[Option[AnyRef]]] =
    val istate = List(
      TimestampKey -> long2byte(timestamp.asLong),
      CurrentUsageKey -> long2byte(currentUsage),
      MaxStoreSizeKey -> long2byte(maximumStoreSize),
      AllocationGroupCountKey -> long2byte(allocationGroups.size.toLong)
    ) ++ allocationGroups.zipWithIndex.map: (uuid, idx) =>
      Key(AllocationGroupBaseKey + idx) -> uuid2byte(uuid)

    taskExecutor.prepareTask(UpdateAllocationGroupUsageTask, istate)


class UpdateAllocationGroupUsageTask(
  val taskPointer: DurableTaskPointer
) extends DurableTask:
  private val promise = Promise[Option[AnyRef]]()
  promise.success(None)

  def completed: Future[Option[AnyRef]] = promise.future
```

- [ ] **Step 3: Register the task type in StaticTypeRegistry**

In `src/main/scala/org/aspen_ddp/aspen/client/internal/StaticTypeRegistry.scala`, add the import and registration:

Add import:
```scala
import org.aspen_ddp.aspen.server.usage.UpdateAllocationGroupUsageTask
```

Add to the `registry` list:
```scala
  private val registry: List[RegisteredTypeFactory] = List(
    KVObjectRootManager,
    SplitFinalizationAction,
    JoinFinalizationAction,
    AllocationFinalizationAction,
    DeletionFinalizationAction,
    MissedUpdateFinalizationAction,
    UpdateAllocationGroupUsageTask
  )
```

- [ ] **Step 4: Verify compilation**

Run: `sbt compile`
Expected: BUILD SUCCESSFUL

- [ ] **Step 5: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/server/usage/UpdateAllocationGroupUsageTask.scala \
  src/main/scala/org/aspen_ddp/aspen/client/internal/StaticTypeRegistry.scala
git commit -m "Add UpdateAllocationGroupUsageTask placeholder"
```

---

### Task 4: Create StoragePoolUsageManager

**Files:**
- Create: `src/main/scala/org/aspen_ddp/aspen/server/usage/StoragePoolUsageManager.scala`

- [ ] **Step 1: Create StoragePoolUsageManager**

Create `src/main/scala/org/aspen_ddp/aspen/server/usage/StoragePoolUsageManager.scala`:

```scala
package org.aspen_ddp.aspen.server.usage

import org.aspen_ddp.aspen.client.{AspenClient, FatalReadError, StopRetrying}
import org.aspen_ddp.aspen.common.HLCTimestamp
import org.aspen_ddp.aspen.common.metadata.StoragePoolState
import org.aspen_ddp.aspen.common.objects.{Insert, KeyValueObjectPointer}
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.transaction.KeyValueUpdate.{DoesNotExist, KeyRevision}
import org.aspen_ddp.aspen.common.util.{long2byte, byte2long}
import org.aspen_ddp.aspen.compute.TaskExecutor
import org.apache.logging.log4j.scala.Logging

import scala.concurrent.{ExecutionContext, Future}

class StoragePoolUsageManager(client: AspenClient) extends Logging:

  given ExecutionContext = client.clientContext

  private var taskExecutor: Option[TaskExecutor] = None
  private var cachedPointers: Map[PoolId, KeyValueObjectPointer] = Map()
  private var cachedSizes: Map[StoreId, Long] = Map()

  def setTaskExecutor(executor: TaskExecutor): Unit = synchronized:
    taskExecutor = Some(executor)

  private def exceedsThreshold(newValue: Long, oldValue: Long): Boolean =
    math.abs(newValue - oldValue).toDouble / math.max(oldValue, 1) > 0.005

  private def getPoolPointer(poolId: PoolId): Future[KeyValueObjectPointer] =
    val cached = synchronized(cachedPointers.get(poolId))
    cached match
      case Some(ptr) => Future.successful(ptr)
      case None =>
        client.getStoragePoolPointer(poolId).map: ptr =>
          synchronized(cachedPointers += poolId -> ptr)
          ptr

  def updateStoreSize(storeId: StoreId, currentEstimatedUsage: Long): Future[Unit] =
    val lastSize = synchronized(cachedSizes.getOrElse(storeId, 0L))

    if !exceedsThreshold(currentEstimatedUsage, lastSize) then
      Future.unit
    else
      def onFail(err: Throwable): Future[Unit] = err match
        case _: FatalReadError => throw StopRetrying(err)
        case _ => Future.unit

      client.transactUntilSuccessfulWithRecovery(onFail): tx =>
        for
          poolPtr <- getPoolPointer(storeId.poolId)
          poolKvos <- client.read(poolPtr)

          sizeKey = StoragePoolState.getStoreUsageKey(storeId.poolIndex)
          sizeBytes = long2byte(currentEstimatedUsage)

          sizeRequirement = poolKvos.contents.get(sizeKey) match
            case Some(vs) => KeyRevision(sizeKey, vs.revision)
            case None => DoesNotExist(sizeKey)

          poolState = StoragePoolState(poolKvos)
          numStores = poolState.stores.length

          totalUsage = (0 until numStores).foldLeft(0L): (acc, i) =>
            if i == storeId.poolIndex then
              acc + currentEstimatedUsage
            else
              val key = StoragePoolState.getStoreUsageKey(i.toByte)
              poolKvos.contents.get(key) match
                case Some(vs) => acc + byte2long(vs.value.bytes)
                case None => acc

          meanUsage = if numStores > 0 then totalUsage / numStores else 0L

          updateConfig = exceedsThreshold(meanUsage, poolState.currentUsage)

          configRequirements =
            if updateConfig then
              val configVs = poolKvos.contents(StoragePoolState.ConfigKey)
              List(KeyRevision(StoragePoolState.ConfigKey, configVs.revision))
            else Nil

          configOperations =
            if updateConfig then
              val updatedState = poolState.copy(currentUsage = meanUsage)
              List(Insert(StoragePoolState.ConfigKey, updatedState.encode()))
            else Nil

          requirements = sizeRequirement :: configRequirements
          operations = Insert(sizeKey, sizeBytes) :: configOperations

          needTask = updateConfig && poolState.allocationGroups.nonEmpty
          executor = synchronized(taskExecutor)

          _ <-
            if needTask && executor.isDefined then
              UpdateAllocationGroupUsageTask.prepareTask(
                HLCTimestamp.now,
                meanUsage,
                poolState.maximumStoreSize,
                poolState.allocationGroups,
                executor.get
              )(using tx).map(_ => ())
            else
              Future.unit
        yield
          tx.update(poolPtr, None, None, requirements, operations)

      .map: _ =>
        synchronized:
          cachedSizes += storeId -> currentEstimatedUsage
```

- [ ] **Step 2: Verify compilation**

Run: `sbt compile`
Expected: BUILD SUCCESSFUL

- [ ] **Step 3: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/server/usage/StoragePoolUsageManager.scala
git commit -m "Add StoragePoolUsageManager with threshold-based store size tracking"
```

---

### Task 5: Integrate TaskExecutor and Periodic Usage Task into StoreManager

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/server/StoreManager.scala`

- [ ] **Step 1: Add imports to StoreManager**

At the top of `src/main/scala/org/aspen_ddp/aspen/server/StoreManager.scala`, add these imports (after the existing imports):

```scala
import org.aspen_ddp.aspen.client.internal.allocation.SinglePoolObjectAllocator
import org.aspen_ddp.aspen.common.objects.{Key, Value}
import org.aspen_ddp.aspen.common.transaction.KeyValueUpdate.DoesNotExist
import org.aspen_ddp.aspen.compute.TaskExecutor
import org.aspen_ddp.aspen.compute.impl.SimpleTaskExecutor
import org.aspen_ddp.aspen.server.usage.StoragePoolUsageManager
import org.aspen_ddp.aspen.common.util.BackgroundTaskManager.ScheduledTask
```

- [ ] **Step 2: Add TaskExecutorRootKey to StoreManager companion object**

In the `StoreManager` companion object (line 33), add:

```scala
  private val TaskExecutorRootKey = Key(Array[Byte](3))
```

- [ ] **Step 3: Add usage manager state to StoreManager class**

After the existing `private var` declarations (around line 101), add:

```scala
  private val taskExecutorPromise: Promise[TaskExecutor] = Promise()
  private val usageManager = new StoragePoolUsageManager(client)
  private var usageUpdateTask: Option[ScheduledTask] = None
```

- [ ] **Step 4: Add TaskExecutor initialization method**

Add this method to the StoreManager class body (after the field declarations, before `tryLoadDevice`):

```scala
  private def initializeTaskExecutor(): Unit =
    val hostPointerFuture = client.getHostPointer(hostId)

    hostPointerFuture.foreach: hostPtr =>
      client.read(hostPtr).foreach: hostKvos =>
        hostKvos.contents.get(TaskExecutorRootKey) match
          case Some(vs) =>
            val executorPtr = KeyValueObjectPointer(vs.value.bytes)
            client.getStoragePool(Radicle.poolId).foreach: pool =>
              val allocator = new SinglePoolObjectAllocator(client, pool, pool.ida, None)
              SimpleTaskExecutor(client, allocator, executorPtr).foreach: executor =>
                synchronized:
                  taskExecutorPromise.success(executor)
                  startUsageTracking(executor)

          case None =>
            client.getStoragePool(Radicle.poolId).foreach: pool =>
              val allocator = new SinglePoolObjectAllocator(client, pool, pool.ida, None)

              client.transactUntilSuccessful: tx =>
                given scala.concurrent.ExecutionContext = ec
                for
                  executorRoot <- allocator.allocateKeyValueObject(Map())(using tx)
                  currentHostKvos <- client.read(hostPtr)
                yield
                  val reqs = List(DoesNotExist(TaskExecutorRootKey))
                  val ops = List(Insert(TaskExecutorRootKey, executorRoot.toArray))
                  tx.update(hostPtr, None, None, reqs, ops)
                  executorRoot
              .foreach: executorRoot =>
                SimpleTaskExecutor(client, allocator, executorRoot).foreach: executor =>
                  synchronized:
                    taskExecutorPromise.success(executor)
                    startUsageTracking(executor)

  private def startUsageTracking(executor: TaskExecutor): Unit =
    usageManager.setTaskExecutor(executor)
    usageUpdateTask = Some(backgroundTasks.schedulePeriodic(Duration(20, SECONDS)):
      synchronized:
        stores.valuesIterator.foreach: store =>
          usageManager.updateStoreSize(store.storeId, store.estimateSize())
    )

  def getTaskExecutor(): Future[TaskExecutor] = taskExecutorPromise.future
```

- [ ] **Step 5: Call initializeTaskExecutor during startup**

Add the call after the existing `events.put(CheckAllDevices())` line (line 129):

```scala
  initializeTaskExecutor()
```

- [ ] **Step 6: Cancel the usage update task in shutdown**

In the `shutdown()` method (line 583), add the cancellation before `shutdownPromise.future`:

```scala
  def shutdown()(using ec: ExecutionContext): Future[Unit] = {
    events.put(Exit())
    pendingStartTask.cancel()
    heartbeatTask.cancel()
    checkStorageDeviceTask.cancel()
    usageUpdateTask.foreach(_.cancel())
    shutdownPromise.future
  }
```

- [ ] **Step 7: Verify compilation**

Run: `sbt compile`
Expected: BUILD SUCCESSFUL

- [ ] **Step 8: Run existing tests**

Run: `sbt test`
Expected: All existing tests pass. The TestNetwork uses `BackgroundTaskManager.NoBackgroundTaskManager` which returns `ShutdownTask` for all scheduled tasks, so the periodic usage task won't fire during existing tests.

- [ ] **Step 9: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/server/StoreManager.scala
git commit -m "Add TaskExecutor initialization and periodic usage tracking to StoreManager"
```

---

### Task 6: Write Integration Tests for StoragePoolUsageManager

**Files:**
- Create: `src/test/scala/org/aspen_ddp/aspen/server/usage/StoragePoolUsageManagerSuite.scala`

- [ ] **Step 1: Create the test directory**

Run: `mkdir -p src/test/scala/org/aspen_ddp/aspen/server/usage`

- [ ] **Step 2: Write the test suite**

Create `src/test/scala/org/aspen_ddp/aspen/server/usage/StoragePoolUsageManagerSuite.scala`:

```scala
package org.aspen_ddp.aspen.server.usage

import org.aspen_ddp.aspen.IntegrationTestSuite
import org.aspen_ddp.aspen.client.internal.allocation.SinglePoolObjectAllocator
import org.aspen_ddp.aspen.client.{KeyValueObjectState, Transaction}
import org.aspen_ddp.aspen.common.Radicle
import org.aspen_ddp.aspen.common.metadata.StoragePoolState
import org.aspen_ddp.aspen.common.objects.{Insert, Key, ObjectRevision, Value}
import org.aspen_ddp.aspen.common.transaction.KeyValueUpdate.KeyRevision
import org.aspen_ddp.aspen.common.util.{long2byte, byte2long}
import org.aspen_ddp.aspen.compute.impl.SimpleTaskExecutor

import scala.concurrent.{ExecutionContext, Future}

class StoragePoolUsageManagerSuite extends IntegrationTestSuite:

  private def setupUsageManager(): Future[(StoragePoolUsageManager, SimpleTaskExecutor)] =
    given ExecutionContext = executionContext
    for
      pool <- client.getStoragePool(Radicle.poolId)
      allocator = new SinglePoolObjectAllocator(client, pool, pool.ida, None)

      tx0 = client.newTransaction()
      executorRoot <- allocator.allocateKeyValueObject(Map())(using tx0)
      kvos0 <- client.read(radicle)
      _ = tx0.bumpVersion(radicle, kvos0.revision)
      _ <- tx0.commit()

      executor <- SimpleTaskExecutor(client, allocator, executorRoot)
    yield
      val mgr = new StoragePoolUsageManager(client)
      mgr.setTaskExecutor(executor)
      (mgr, executor)

  private def readPoolState(): Future[(KeyValueObjectState, StoragePoolState)] =
    given ExecutionContext = executionContext
    for
      poolPtr <- client.getStoragePoolPointer(Radicle.poolId)
      poolKvos <- client.read(poolPtr)
    yield
      (poolKvos, StoragePoolState(poolKvos))

  atest("No update when size is unchanged"):
    given ExecutionContext = executionContext
    for
      (mgr, _) <- setupUsageManager()
      storeId = net.storeId0

      (kvos1, _) <- readPoolState()
      _ <- mgr.updateStoreSize(storeId, 1000L)
      _ <- waitForTransactionsToComplete()

      (kvos2, _) <- readPoolState()
      _ <- mgr.updateStoreSize(storeId, 1000L)
      _ <- waitForTransactionsToComplete()

      (kvos3, _) <- readPoolState()
    yield
      val sizeKey = StoragePoolState.getStoreUsageKey(storeId.poolIndex)
      kvos2.contents.contains(sizeKey) should be(true)
      byte2long(kvos2.contents(sizeKey).value.bytes) should be(1000L)
      kvos3.contents(sizeKey).revision should be(kvos2.contents(sizeKey).revision)

  atest("Update store size KV pair on first report"):
    given ExecutionContext = executionContext
    for
      (mgr, _) <- setupUsageManager()
      storeId = net.storeId0
      sizeKey = StoragePoolState.getStoreUsageKey(storeId.poolIndex)

      (kvos1, _) <- readPoolState()
      _ = kvos1.contents.contains(sizeKey) should be(false)

      _ <- mgr.updateStoreSize(storeId, 5000L)
      _ <- waitForTransactionsToComplete()

      (kvos2, _) <- readPoolState()
    yield
      kvos2.contents.contains(sizeKey) should be(true)
      byte2long(kvos2.contents(sizeKey).value.bytes) should be(5000L)

  atest("Update store size KV pair on significant change"):
    given ExecutionContext = executionContext
    for
      (mgr, _) <- setupUsageManager()
      storeId = net.storeId0
      sizeKey = StoragePoolState.getStoreUsageKey(storeId.poolIndex)

      _ <- mgr.updateStoreSize(storeId, 10000L)
      _ <- waitForTransactionsToComplete()

      (kvos1, _) <- readPoolState()
      rev1 = kvos1.contents(sizeKey).revision

      _ <- mgr.updateStoreSize(storeId, 10200L)
      _ <- waitForTransactionsToComplete()

      (kvos2, _) <- readPoolState()
    yield
      byte2long(kvos2.contents(sizeKey).value.bytes) should be(10200L)
      kvos2.contents(sizeKey).revision should not be rev1

  atest("No update when change is below threshold"):
    given ExecutionContext = executionContext
    for
      (mgr, _) <- setupUsageManager()
      storeId = net.storeId0
      sizeKey = StoragePoolState.getStoreUsageKey(storeId.poolIndex)

      _ <- mgr.updateStoreSize(storeId, 10000L)
      _ <- waitForTransactionsToComplete()

      (kvos1, _) <- readPoolState()
      rev1 = kvos1.contents(sizeKey).revision

      _ <- mgr.updateStoreSize(storeId, 10040L)
      _ <- waitForTransactionsToComplete()

      (kvos2, _) <- readPoolState()
    yield
      kvos2.contents(sizeKey).revision should be(rev1)

  atest("currentUsage updated when mean changes significantly"):
    given ExecutionContext = executionContext
    for
      (mgr, _) <- setupUsageManager()

      _ <- mgr.updateStoreSize(net.storeId0, 10000L)
      _ <- waitForTransactionsToComplete()
      _ <- mgr.updateStoreSize(net.storeId1, 10000L)
      _ <- waitForTransactionsToComplete()
      _ <- mgr.updateStoreSize(net.storeId2, 10000L)
      _ <- waitForTransactionsToComplete()

      (_, state1) <- readPoolState()
      _ = state1.currentUsage should be(10000L)

      _ <- mgr.updateStoreSize(net.storeId0, 15000L)
      _ <- waitForTransactionsToComplete()

      (_, state2) <- readPoolState()
    yield
      val expectedMean = (15000L + 10000L + 10000L) / 3
      state2.currentUsage should be(expectedMean)

  atest("Durable task prepared when allocationGroups non-empty"):
    given ExecutionContext = executionContext
    import java.util.UUID
    val allocGroup = UUID.randomUUID()

    for
      poolPtr <- client.getStoragePoolPointer(Radicle.poolId)
      poolKvos <- client.read(poolPtr)
      poolState = StoragePoolState(poolKvos)

      updatedState = poolState.copy(
        allocationGroups = List(allocGroup),
        maximumStoreSize = 100000L
      )
      tx0 = client.newTransaction()
      _ = tx0.update(poolPtr, None, None,
        List(KeyRevision(StoragePoolState.ConfigKey, poolKvos.contents(StoragePoolState.ConfigKey).revision)),
        List(Insert(StoragePoolState.ConfigKey, updatedState.encode())))
      _ <- tx0.commit()
      _ <- waitForTransactionsToComplete()

      (mgr, _) <- setupUsageManager()

      _ <- mgr.updateStoreSize(net.storeId0, 10000L)
      _ <- waitForTransactionsToComplete()
      _ <- mgr.updateStoreSize(net.storeId1, 10000L)
      _ <- waitForTransactionsToComplete()
      _ <- mgr.updateStoreSize(net.storeId2, 10000L)
      _ <- waitForTransactionsToComplete()

      _ <- mgr.updateStoreSize(net.storeId0, 15000L)
      _ <- waitForTransactionsToComplete()

      (_, finalState) <- readPoolState()
    yield
      val expectedMean = (15000L + 10000L + 10000L) / 3
      finalState.currentUsage should be(expectedMean)

  atest("No durable task when allocationGroups is empty"):
    given ExecutionContext = executionContext
    for
      (mgr, _) <- setupUsageManager()

      _ <- mgr.updateStoreSize(net.storeId0, 10000L)
      _ <- waitForTransactionsToComplete()
      _ <- mgr.updateStoreSize(net.storeId1, 10000L)
      _ <- waitForTransactionsToComplete()
      _ <- mgr.updateStoreSize(net.storeId2, 10000L)
      _ <- waitForTransactionsToComplete()

      _ <- mgr.updateStoreSize(net.storeId0, 15000L)
      _ <- waitForTransactionsToComplete()

      (_, finalState) <- readPoolState()
    yield
      finalState.allocationGroups should be(Nil)
      val expectedMean = (15000L + 10000L + 10000L) / 3
      finalState.currentUsage should be(expectedMean)
```

- [ ] **Step 3: Run the new tests**

Run: `sbt 'testOnly *StoragePoolUsageManagerSuite'`
Expected: All 7 tests PASS

- [ ] **Step 4: Run the full test suite to check for regressions**

Run: `sbt test`
Expected: All tests PASS

- [ ] **Step 5: Commit**

```bash
git add src/test/scala/org/aspen_ddp/aspen/server/usage/StoragePoolUsageManagerSuite.scala
git commit -m "Add integration tests for StoragePoolUsageManager"
```
