# UpdateAllocationGroupUsageTask Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement the `UpdateAllocationGroupUsageTask` durable task that propagates usage and maximum size changes upward through the allocation group hierarchy, with crash-recovery via a transactionally-advanced index.

**Architecture:** Custom stepping logic extending `DurableTask` directly. Groups are processed sequentially; each step atomically updates a parent group and advances a persisted index. Cascading tasks are created within the same transaction when aggregated values change by >0.5%. A signature change to `DurableTaskFactory.createTask` adds `TaskExecutor` across the codebase.

**Tech Stack:** Scala 3, ScalaTest (AsyncFunSuite), Aspen compute framework (DurableTask, TaskExecutor, SimpleTaskExecutor)

---

### Task 1: Add `TaskExecutor` parameter to `DurableTaskFactory.createTask`

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/compute/DurableTaskFactory.scala:10-13`
- Modify: `src/main/scala/org/aspen_ddp/aspen/compute/impl/SimpleTaskExecutor.scala:68` and `:126`
- Modify: `src/test/scala/org/aspen_ddp/aspen/compute/SteppedDurableTaskSuite.scala:20-26`
- Modify: `src/main/scala/org/aspen_ddp/aspen/amoebafs/impl/simple/CreateFileTask.scala:23-26`
- Modify: `src/main/scala/org/aspen_ddp/aspen/amoebafs/impl/simple/UnlinkFileTask.scala:19-22`
- Modify: `src/main/scala/org/aspen_ddp/aspen/amoebafs/impl/simple/SimpleFileContent.scala:285-288`
- Modify: `src/main/scala/org/aspen_ddp/aspen/amoebafs/impl/simple/SimpleDirectory.scala:196-199`
- Modify: `src/main/scala/org/aspen_ddp/aspen/common/metadata/management/UpdateAllocationGroupUsageTask.scala:20-23`

- [ ] **Step 1: Update `DurableTaskFactory` trait**

In `src/main/scala/org/aspen_ddp/aspen/compute/DurableTaskFactory.scala`, add `TaskExecutor` to the `createTask` signature:

```scala
trait DurableTaskFactory extends RegisteredTypeFactory:

  def createTask(client: AspenClient,
                 pointer: DurableTaskPointer,
                 revision: ObjectRevision,
                 state: Map[Key, KeyValueObjectState.ValueState],
                 taskExecutor: TaskExecutor): DurableTask
```

- [ ] **Step 2: Update `SimpleTaskExecutor` call sites**

In `src/main/scala/org/aspen_ddp/aspen/compute/impl/SimpleTaskExecutor.scala`, update both places that call `createTask` to pass `this`:

Line 68 (in the recovery loop in the constructor body):
```scala
dtt.createTask(client, taskPointer, kvos.revision, kvos.contents, this)
```

Line 126 (in `prepareTask`, post-commit):
```scala
val task = taskType.createTask(client, taskPointer, kvos.revision, kvos.contents, this)
```

- [ ] **Step 3: Update `TestSteppedTask`**

In `src/test/scala/org/aspen_ddp/aspen/compute/SteppedDurableTaskSuite.scala`, add the ignored parameter:

```scala
  def createTask(client: AspenClient,
                 pointer: DurableTaskPointer,
                 revision: ObjectRevision,
                 state: Map[Key, KeyValueObjectState.ValueState],
                 taskExecutor: TaskExecutor): DurableTask =
```

- [ ] **Step 4: Update `CreateFileTask`**

In `src/main/scala/org/aspen_ddp/aspen/amoebafs/impl/simple/CreateFileTask.scala`, add the ignored parameter:

```scala
  def createTask(client: AspenClient,
                 pointer: DurableTaskPointer,
                 revision: ObjectRevision,
                 state: Map[Key, KeyValueObjectState.ValueState],
                 taskExecutor: TaskExecutor): DurableTask =
```

- [ ] **Step 5: Update `UnlinkFileTask`**

In `src/main/scala/org/aspen_ddp/aspen/amoebafs/impl/simple/UnlinkFileTask.scala`, add the ignored parameter:

```scala
  def createTask(client: AspenClient,
                 pointer: DurableTaskPointer,
                 revision: ObjectRevision,
                 state: Map[Key, KeyValueObjectState.ValueState],
                 taskExecutor: TaskExecutor): DurableTask =
```

- [ ] **Step 6: Update `DeleteFileContentTask`**

In `src/main/scala/org/aspen_ddp/aspen/amoebafs/impl/simple/SimpleFileContent.scala`, add the ignored parameter:

```scala
    override def createTask(client: AspenClient,
                            pointer: DurableTaskPointer,
                            revision: ObjectRevision,
                            state: Map[Key, KeyValueObjectState.ValueState],
                            taskExecutor: TaskExecutor): DurableTask =
```

- [ ] **Step 7: Update `DeleteDirectoryContentTask`**

In `src/main/scala/org/aspen_ddp/aspen/amoebafs/impl/simple/SimpleDirectory.scala`, add the ignored parameter:

```scala
    override def createTask(client: AspenClient,
                            pointer: DurableTaskPointer,
                            revision: ObjectRevision,
                            state: Map[Key, KeyValueObjectState.ValueState],
                            taskExecutor: TaskExecutor): DurableTask =
```

- [ ] **Step 8: Update `UpdateAllocationGroupUsageTask.createTask` (stub only for now)**

In `src/main/scala/org/aspen_ddp/aspen/common/metadata/management/UpdateAllocationGroupUsageTask.scala`, add the parameter but keep the stub behavior:

```scala
  def createTask(client: AspenClient,
                 pointer: DurableTaskPointer,
                 revision: ObjectRevision,
                 state: Map[Key, KeyValueObjectState.ValueState],
                 taskExecutor: TaskExecutor): DurableTask =
    new UpdateAllocationGroupUsageTask(pointer)
```

- [ ] **Step 9: Verify compilation**

Run: `sbt compile`
Expected: Compiles successfully with no errors.

- [ ] **Step 10: Run existing tests**

Run: `sbt test`
Expected: All existing tests pass (the signature change is backward-compatible since all call sites are updated).

- [ ] **Step 11: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/compute/DurableTaskFactory.scala \
        src/main/scala/org/aspen_ddp/aspen/compute/impl/SimpleTaskExecutor.scala \
        src/test/scala/org/aspen_ddp/aspen/compute/SteppedDurableTaskSuite.scala \
        src/main/scala/org/aspen_ddp/aspen/amoebafs/impl/simple/CreateFileTask.scala \
        src/main/scala/org/aspen_ddp/aspen/amoebafs/impl/simple/UnlinkFileTask.scala \
        src/main/scala/org/aspen_ddp/aspen/amoebafs/impl/simple/SimpleFileContent.scala \
        src/main/scala/org/aspen_ddp/aspen/amoebafs/impl/simple/SimpleDirectory.scala \
        src/main/scala/org/aspen_ddp/aspen/common/metadata/management/UpdateAllocationGroupUsageTask.scala
git commit -m "Add TaskExecutor parameter to DurableTaskFactory.createTask"
```

---

### Task 2: Implement `UpdateAllocationGroupUsageTask`

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/common/metadata/management/UpdateAllocationGroupUsageTask.scala`

- [ ] **Step 1: Replace the file with the full implementation**

Replace the entire contents of `src/main/scala/org/aspen_ddp/aspen/common/metadata/management/UpdateAllocationGroupUsageTask.scala` with:

```scala
package org.aspen_ddp.aspen.common.metadata.management

import org.apache.logging.log4j.scala.Logging
import org.aspen_ddp.aspen.client.{AspenClient, KeyValueObjectState, ReadError, StopRetrying, Transaction, TransactionAborted}
import org.aspen_ddp.aspen.common.DataBuffer
import org.aspen_ddp.aspen.common.allocation_group.AllocationGroupId
import org.aspen_ddp.aspen.common.metadata.AllocationGroupState
import org.aspen_ddp.aspen.common.objects.{Insert, Key, ObjectRevision}
import org.aspen_ddp.aspen.common.transaction.KeyValueUpdate
import org.aspen_ddp.aspen.common.util.{byte2long, byte2uuid, byte2uuids, long2byte, uuid2byte, uuids2byte}
import org.aspen_ddp.aspen.compute.{DurableTask, DurableTaskFactory, DurableTaskPointer, TaskExecutor}

import java.util.UUID
import java.util.concurrent.ThreadLocalRandom
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.concurrent.duration.{Duration, MILLISECONDS}
import scala.util.{Failure, Success}

object UpdateAllocationGroupUsageTask extends DurableTaskFactory:
  val typeUUID: UUID = UUID.fromString("7A3F9B2E-1D4C-4E8A-B5F7-6C9D0E2A8B1F")

  private[management] val ChildUUIDKey = Key(1)
  private[management] val CurrentUsageKey = Key(2)
  private[management] val MaxSizeKey = Key(3)
  private[management] val AllocationGroupsKey = Key(4)
  private[management] val NextIndexKey = Key(5)

  def createTask(client: AspenClient,
                 pointer: DurableTaskPointer,
                 revision: ObjectRevision,
                 state: Map[Key, KeyValueObjectState.ValueState],
                 taskExecutor: TaskExecutor): DurableTask =
    val childUUID = byte2uuid(state(ChildUUIDKey).value.bytes)
    val currentUsage = byte2long(state(CurrentUsageKey).value.bytes)
    val maximumSize = byte2long(state(MaxSizeKey).value.bytes)
    val allocationGroups = byte2uuids(state(AllocationGroupsKey).value.bytes)

    new UpdateAllocationGroupUsageTask(pointer, client, childUUID, currentUsage,
      maximumSize, allocationGroups, taskExecutor)

  def prepareTask(childUUID: UUID,
                  currentUsage: Long,
                  maximumStoreSize: Long,
                  allocationGroups: List[UUID],
                  taskExecutor: TaskExecutor)
                 (using tx: Transaction): Future[Future[Option[AnyRef]]] =
    require(allocationGroups.nonEmpty)

    val istate = List(
      ChildUUIDKey -> uuid2byte(childUUID),
      CurrentUsageKey -> long2byte(currentUsage),
      MaxSizeKey -> long2byte(maximumStoreSize),
      AllocationGroupsKey -> uuids2byte(allocationGroups),
      NextIndexKey -> long2byte(0)
    )

    taskExecutor.prepareTask(UpdateAllocationGroupUsageTask, istate)


class UpdateAllocationGroupUsageTask(
  val taskPointer: DurableTaskPointer,
  client: AspenClient,
  childUUID: UUID,
  currentUsage: Long,
  maximumSize: Long,
  allocationGroups: List[UUID],
  taskExecutor: TaskExecutor
) extends DurableTask with Logging:

  given ExecutionContext = client.clientContext

  private val promise = Promise[Option[AnyRef]]()
  private var retryDelay: Int = 16

  def completed: Future[Option[AnyRef]] = promise.future

  updateNextGroup()

  private def exceedsThreshold(newValue: Long, oldValue: Long): Boolean =
    math.abs(newValue - oldValue).toDouble / math.max(oldValue, 1) > 0.005

  private def completeTask(): Unit = synchronized:
    if !promise.isCompleted then
      promise.success(None)

  private def failTask(err: Throwable): Unit = synchronized:
    if !promise.isCompleted then
      promise.failure(err)

  private def scheduleRetry(): Unit =
    val delay = ThreadLocalRandom.current().nextInt(retryDelay)
    retryDelay = Math.min(retryDelay * 2, 60000)
    client.backgroundTaskManager.schedule(Duration(delay, MILLISECONDS)):
      updateNextGroup()

  private def advanceIndex(kvos: KeyValueObjectState): Unit =
    val vs = kvos.contents(NextIndexKey)
    val nextIndex = byte2long(vs.value.bytes)

    val tx = client.newTransaction()
    val requirements = KeyValueUpdate.KeyRevision(NextIndexKey, vs.revision) :: Nil
    val operations = Insert(NextIndexKey, long2byte(nextIndex + 1)) :: Nil
    tx.update(taskPointer.kvPointer, None, None, requirements, operations)

    tx.commit().onComplete:
      case Failure(_) => scheduleRetry()
      case Success(_) =>
        retryDelay = 16
        updateNextGroup()

  private def updateNextGroup(): Unit =
    client.read(taskPointer.kvPointer).onComplete:
      case Failure(err) => scheduleRetry()
      case Success(kvos) =>
        val vs = kvos.contents(NextIndexKey)
        val nextIndex = byte2long(vs.value.bytes).toInt

        if nextIndex >= allocationGroups.length then
          completeTask()
        else
          val groupUUID = allocationGroups(nextIndex)
          val groupId = AllocationGroupId(groupUUID)

          val fUpdate = for
            agsPtr <- client.getAllocationGroupPointer(groupId)
            agsDos <- client.read(agsPtr)
          yield
            val ags = AllocationGroupState(agsDos)

            ags.members.find(_.uuid == childUUID) match
              case None =>
                advanceIndex(kvos)

              case Some(member) =>
                val oldGroupUsage = ags.currentUsage
                val oldGroupMaxSize = ags.maximumSize

                val updatedMembers = ags.members.map: m =>
                  if m.uuid == childUUID then
                    m.copy(currentUsage = currentUsage, maximumSize = maximumSize)
                  else m

                val nags = ags.copy(members = updatedMembers)
                val newGroupUsage = nags.currentUsage
                val newGroupMaxSize = nags.maximumSize

                val needsCascade = nags.parentGroups.nonEmpty &&
                  (exceedsThreshold(newGroupUsage, oldGroupUsage) ||
                    exceedsThreshold(newGroupMaxSize, oldGroupMaxSize))

                val tx = client.newTransaction()

                tx.overwrite(agsPtr, agsDos.revision, DataBuffer(nags.toBytes))

                val requirements = KeyValueUpdate.KeyRevision(NextIndexKey, vs.revision) :: Nil
                val operations = Insert(NextIndexKey, long2byte(nextIndex + 1)) :: Nil
                tx.update(taskPointer.kvPointer, None, None, requirements, operations)

                given Transaction = tx

                val fCascade =
                  if needsCascade then
                    UpdateAllocationGroupUsageTask.prepareTask(
                      groupId.uuid, newGroupUsage, newGroupMaxSize,
                      nags.parentGroups.map(_.uuid), taskExecutor
                    ).map(_ => ())
                  else
                    Future.unit

                fCascade.flatMap: _ =>
                  tx.commit().transform:
                    case Success(_) =>
                      retryDelay = 16
                      updateNextGroup()
                      Success(())
                    case Failure(err) =>
                      scheduleRetry()
                      Success(())

          fUpdate.onComplete:
            case Failure(err) => err match
              case _: NoSuchElementException | _: ReadError =>
                advanceIndex(kvos)
              case e: StopRetrying =>
                failTask(e)
              case _ =>
                scheduleRetry()
            case Success(_) => ()
```

- [ ] **Step 2: Verify compilation**

Run: `sbt compile`
Expected: Compiles successfully.

- [ ] **Step 3: Run existing tests**

Run: `sbt test`
Expected: All existing tests still pass.

- [ ] **Step 4: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/common/metadata/management/UpdateAllocationGroupUsageTask.scala
git commit -m "Implement UpdateAllocationGroupUsageTask with sequential index-based processing"
```

---

### Task 3: Write tests for single parent group update

**Files:**
- Create: `src/test/scala/org/aspen_ddp/aspen/common/metadata/management/UpdateAllocationGroupUsageTaskSuite.scala`

- [ ] **Step 1: Create the test file with setup helpers and the first test**

Create `src/test/scala/org/aspen_ddp/aspen/common/metadata/management/UpdateAllocationGroupUsageTaskSuite.scala`:

```scala
package org.aspen_ddp.aspen.common.metadata.management

import org.aspen_ddp.aspen.IntegrationTestSuite
import org.aspen_ddp.aspen.client.internal.allocation.PoolObjectAllocator
import org.aspen_ddp.aspen.common.Radicle
import org.aspen_ddp.aspen.common.allocation_group.AllocationGroupId
import org.aspen_ddp.aspen.common.metadata.{AllocationGroupState, StoragePoolState}
import org.aspen_ddp.aspen.common.util.byte2long
import org.aspen_ddp.aspen.compute.TaskExecutor
import org.aspen_ddp.aspen.compute.impl.SimpleTaskExecutor

import scala.concurrent.{ExecutionContext, Future}

class UpdateAllocationGroupUsageTaskSuite extends IntegrationTestSuite:

  private def setup(): Future[TaskExecutor] =
    given ExecutionContext = executionContext

    for
      pool <- client.getStoragePool(Radicle.poolId)
      allocator = new PoolObjectAllocator(client, pool, pool.ida, None)

      tx0 = client.newTransaction()
      executorRoot <- allocator.allocateKeyValueObject(Map())(using tx0)
      kvos0 <- client.read(radicle)
      _ = tx0.bumpVersion(radicle, kvos0.revision)
      _ <- tx0.commit()

      executor <- SimpleTaskExecutor(client, allocator, executorRoot)
    yield executor

  private def readGroupState(groupId: AllocationGroupId): Future[AllocationGroupState] =
    given ExecutionContext = executionContext

    for
      groupPtr <- client.getAllocationGroupPointer(groupId)
      groupDos <- client.read(groupPtr)
    yield AllocationGroupState(groupDos)

  private def readPoolState(): Future[StoragePoolState] =
    given ExecutionContext = executionContext

    for
      poolPtr <- client.getStoragePoolPointer(Radicle.poolId)
      poolKvos <- client.read(poolPtr)
    yield StoragePoolState(poolKvos)

  atest("updates member usage in single parent group"):

    given ExecutionContext = executionContext

    for
      executor <- setup()
      groupId <- client.createAllocationGroup("test-group", level = 0)
      _ <- waitForTransactionsToComplete()

      _ <- AllocationGroupState.addPool(client, Radicle.poolId, groupId, executor)
      _ <- waitForTransactionsToComplete()

      agsBefore <- readGroupState(groupId)
      memberBefore = agsBefore.members.find(_.uuid == Radicle.poolId.uuid).get

      newUsage = memberBefore.currentUsage + 50000L
      newMaxSize = memberBefore.maximumSize + 100000L

      kvos <- client.read(radicle)
      tx = client.newTransaction()
      taskFuture <- UpdateAllocationGroupUsageTask.prepareTask(
        Radicle.poolId.uuid, newUsage, newMaxSize,
        List(groupId.uuid), executor
      )(using tx)
      _ = tx.bumpVersion(radicle, kvos.revision)
      _ <- tx.commit()
      _ <- waitForTransactionsToComplete()
      _ <- taskFuture

      agsAfter <- readGroupState(groupId)
      memberAfter = agsAfter.members.find(_.uuid == Radicle.poolId.uuid).get
    yield
      memberAfter.currentUsage should be(newUsage)
      memberAfter.maximumSize should be(newMaxSize)
```

- [ ] **Step 2: Run the test**

Run: `sbt 'testOnly *UpdateAllocationGroupUsageTaskSuite -- -z "updates member usage in single parent group"'`
Expected: PASS

- [ ] **Step 3: Commit**

```bash
git add src/test/scala/org/aspen_ddp/aspen/common/metadata/management/UpdateAllocationGroupUsageTaskSuite.scala
git commit -m "Add test for UpdateAllocationGroupUsageTask single parent group update"
```

---

### Task 4: Write test for multiple parent groups

**Files:**
- Modify: `src/test/scala/org/aspen_ddp/aspen/common/metadata/management/UpdateAllocationGroupUsageTaskSuite.scala`

- [ ] **Step 1: Add the test**

Append this test to the `UpdateAllocationGroupUsageTaskSuite` class:

```scala
  atest("updates member usage across multiple parent groups"):
    given ExecutionContext = executionContext
    for
      executor <- setup()
      groupId1 <- client.createAllocationGroup("group-1", level = 0)
      _ <- waitForTransactionsToComplete()
      groupId2 <- client.createAllocationGroup("group-2", level = 0)
      _ <- waitForTransactionsToComplete()

      _ <- AllocationGroupState.addPool(client, Radicle.poolId, groupId1, executor)
      _ <- waitForTransactionsToComplete()
      _ <- AllocationGroupState.addPool(client, Radicle.poolId, groupId2, executor)
      _ <- waitForTransactionsToComplete()

      newUsage = 75000L
      newMaxSize = 200000L

      kvos <- client.read(radicle)
      tx = client.newTransaction()
      taskFuture <- UpdateAllocationGroupUsageTask.prepareTask(
        Radicle.poolId.uuid, newUsage, newMaxSize,
        List(groupId1.uuid, groupId2.uuid), executor
      )(using tx)
      _ = tx.bumpVersion(radicle, kvos.revision)
      _ <- tx.commit()
      _ <- waitForTransactionsToComplete()
      _ <- taskFuture

      ags1 <- readGroupState(groupId1)
      ags2 <- readGroupState(groupId2)
      member1 = ags1.members.find(_.uuid == Radicle.poolId.uuid).get
      member2 = ags2.members.find(_.uuid == Radicle.poolId.uuid).get
    yield
      member1.currentUsage should be(newUsage)
      member1.maximumSize should be(newMaxSize)
      member2.currentUsage should be(newUsage)
      member2.maximumSize should be(newMaxSize)
```

- [ ] **Step 2: Run the test**

Run: `sbt 'testOnly *UpdateAllocationGroupUsageTaskSuite -- -z "updates member usage across multiple parent groups"'`
Expected: PASS

- [ ] **Step 3: Commit**

```bash
git add src/test/scala/org/aspen_ddp/aspen/common/metadata/management/UpdateAllocationGroupUsageTaskSuite.scala
git commit -m "Add test for UpdateAllocationGroupUsageTask with multiple parent groups"
```

---

### Task 5: Write test for skipping when member not found

**Files:**
- Modify: `src/test/scala/org/aspen_ddp/aspen/common/metadata/management/UpdateAllocationGroupUsageTaskSuite.scala`

- [ ] **Step 1: Add the test**

Append this test to the suite:

```scala
  atest("skips silently when member not found in group"):
    given ExecutionContext = executionContext
    val unknownChildUUID = java.util.UUID.randomUUID()
    for
      executor <- setup()
      groupId <- client.createAllocationGroup("test-group", level = 0)
      _ <- waitForTransactionsToComplete()

      _ <- AllocationGroupState.addPool(client, Radicle.poolId, groupId, executor)
      _ <- waitForTransactionsToComplete()

      agsBefore <- readGroupState(groupId)

      kvos <- client.read(radicle)
      tx = client.newTransaction()
      taskFuture <- UpdateAllocationGroupUsageTask.prepareTask(
        unknownChildUUID, 99999L, 99999L,
        List(groupId.uuid), executor
      )(using tx)
      _ = tx.bumpVersion(radicle, kvos.revision)
      _ <- tx.commit()
      _ <- waitForTransactionsToComplete()
      result <- taskFuture

      agsAfter <- readGroupState(groupId)
    yield
      result should be(None)
      agsAfter.members should be(agsBefore.members)
```

- [ ] **Step 2: Run the test**

Run: `sbt 'testOnly *UpdateAllocationGroupUsageTaskSuite -- -z "skips silently when member not found"'`
Expected: PASS

- [ ] **Step 3: Commit**

```bash
git add src/test/scala/org/aspen_ddp/aspen/common/metadata/management/UpdateAllocationGroupUsageTaskSuite.scala
git commit -m "Add test for UpdateAllocationGroupUsageTask skip on missing member"
```

---

### Task 6: Write test for cascade to grandparent

**Files:**
- Modify: `src/test/scala/org/aspen_ddp/aspen/common/metadata/management/UpdateAllocationGroupUsageTaskSuite.scala`

- [ ] **Step 1: Add the test**

Append this test to the suite:

```scala
  atest("cascades to grandparent when change exceeds threshold"):
    given ExecutionContext = executionContext
    for
      executor <- setup()

      childGroupId <- client.createAllocationGroup("child-group", level = 0)
      _ <- waitForTransactionsToComplete()
      parentGroupId <- client.createAllocationGroup("parent-group", level = 1)
      _ <- waitForTransactionsToComplete()

      _ <- AllocationGroupState.addPool(client, Radicle.poolId, childGroupId, executor)
      _ <- waitForTransactionsToComplete()
      _ <- AllocationGroupState.addGroup(client, childGroupId, parentGroupId, executor)
      _ <- waitForTransactionsToComplete()

      childBefore <- readGroupState(childGroupId)
      poolMember = childBefore.members.find(_.uuid == Radicle.poolId.uuid).get

      newUsage = poolMember.currentUsage + 500000L
      newMaxSize = poolMember.maximumSize + 1000000L

      kvos <- client.read(radicle)
      tx = client.newTransaction()
      taskFuture <- UpdateAllocationGroupUsageTask.prepareTask(
        Radicle.poolId.uuid, newUsage, newMaxSize,
        List(childGroupId.uuid), executor
      )(using tx)
      _ = tx.bumpVersion(radicle, kvos.revision)
      _ <- tx.commit()
      _ <- waitForTransactionsToComplete()
      _ <- taskFuture
      _ <- waitForTransactionsToComplete()

      childAfter <- readGroupState(childGroupId)
      parentAfter <- readGroupState(parentGroupId)

      childMember = childAfter.members.find(_.uuid == Radicle.poolId.uuid).get
      parentMember = parentAfter.members.find(_.uuid == childGroupId.uuid).get
    yield
      childMember.currentUsage should be(newUsage)
      childMember.maximumSize should be(newMaxSize)
      parentMember.currentUsage should be(childAfter.currentUsage)
      parentMember.maximumSize should be(childAfter.maximumSize)
```

- [ ] **Step 2: Run the test**

Run: `sbt 'testOnly *UpdateAllocationGroupUsageTaskSuite -- -z "cascades to grandparent"'`
Expected: PASS

- [ ] **Step 3: Commit**

```bash
git add src/test/scala/org/aspen_ddp/aspen/common/metadata/management/UpdateAllocationGroupUsageTaskSuite.scala
git commit -m "Add test for UpdateAllocationGroupUsageTask cascade to grandparent"
```

---

### Task 7: Write test for no cascade below threshold

**Files:**
- Modify: `src/test/scala/org/aspen_ddp/aspen/common/metadata/management/UpdateAllocationGroupUsageTaskSuite.scala`

- [ ] **Step 1: Add the test**

Append this test to the suite:

```scala
  atest("does not cascade when change is below threshold"):
    given ExecutionContext = executionContext
    for
      executor <- setup()

      childGroupId <- client.createAllocationGroup("child-group", level = 0)
      _ <- waitForTransactionsToComplete()
      parentGroupId <- client.createAllocationGroup("parent-group", level = 1)
      _ <- waitForTransactionsToComplete()

      _ <- AllocationGroupState.addPool(client, Radicle.poolId, childGroupId, executor)
      _ <- waitForTransactionsToComplete()
      _ <- AllocationGroupState.addGroup(client, childGroupId, parentGroupId, executor)
      _ <- waitForTransactionsToComplete()

      childBefore <- readGroupState(childGroupId)
      parentBefore <- readGroupState(parentGroupId)
      poolMember = childBefore.members.find(_.uuid == Radicle.poolId.uuid).get

      tinyUsageChange = poolMember.currentUsage + 1L
      tinyMaxSizeChange = poolMember.maximumSize + 1L

      kvos <- client.read(radicle)
      tx = client.newTransaction()
      taskFuture <- UpdateAllocationGroupUsageTask.prepareTask(
        Radicle.poolId.uuid, tinyUsageChange, tinyMaxSizeChange,
        List(childGroupId.uuid), executor
      )(using tx)
      _ = tx.bumpVersion(radicle, kvos.revision)
      _ <- tx.commit()
      _ <- waitForTransactionsToComplete()
      _ <- taskFuture
      _ <- waitForTransactionsToComplete()

      childAfter <- readGroupState(childGroupId)
      parentAfter <- readGroupState(parentGroupId)

      childMember = childAfter.members.find(_.uuid == Radicle.poolId.uuid).get
      parentMemberBefore = parentBefore.members.find(_.uuid == childGroupId.uuid).get
      parentMemberAfter = parentAfter.members.find(_.uuid == childGroupId.uuid).get
    yield
      childMember.currentUsage should be(tinyUsageChange)
      childMember.maximumSize should be(tinyMaxSizeChange)
      parentMemberAfter.currentUsage should be(parentMemberBefore.currentUsage)
      parentMemberAfter.maximumSize should be(parentMemberBefore.maximumSize)
```

- [ ] **Step 2: Run the test**

Run: `sbt 'testOnly *UpdateAllocationGroupUsageTaskSuite -- -z "does not cascade when change is below threshold"'`
Expected: PASS

- [ ] **Step 3: Commit**

```bash
git add src/test/scala/org/aspen_ddp/aspen/common/metadata/management/UpdateAllocationGroupUsageTaskSuite.scala
git commit -m "Add test for UpdateAllocationGroupUsageTask no cascade below threshold"
```

---

### Task 8: Write test for completion when all groups skip

**Files:**
- Modify: `src/test/scala/org/aspen_ddp/aspen/common/metadata/management/UpdateAllocationGroupUsageTaskSuite.scala`

- [ ] **Step 1: Add the test**

Append this test to the suite:

```scala
  atest("task completes when all groups result in skips"):
    given ExecutionContext = executionContext
    val unknownChildUUID = java.util.UUID.randomUUID()
    for
      executor <- setup()
      groupId1 <- client.createAllocationGroup("group-1", level = 0)
      _ <- waitForTransactionsToComplete()
      groupId2 <- client.createAllocationGroup("group-2", level = 0)
      _ <- waitForTransactionsToComplete()

      kvos <- client.read(radicle)
      tx = client.newTransaction()
      taskFuture <- UpdateAllocationGroupUsageTask.prepareTask(
        unknownChildUUID, 99999L, 99999L,
        List(groupId1.uuid, groupId2.uuid), executor
      )(using tx)
      _ = tx.bumpVersion(radicle, kvos.revision)
      _ <- tx.commit()
      _ <- waitForTransactionsToComplete()
      result <- taskFuture
    yield
      result should be(None)
```

- [ ] **Step 2: Run the full test suite**

Run: `sbt 'testOnly *UpdateAllocationGroupUsageTaskSuite'`
Expected: All 6 tests PASS.

- [ ] **Step 3: Commit**

```bash
git add src/test/scala/org/aspen_ddp/aspen/common/metadata/management/UpdateAllocationGroupUsageTaskSuite.scala
git commit -m "Add test for UpdateAllocationGroupUsageTask completion on all-skip"
```

---

### Task 9: Final verification

- [ ] **Step 1: Run all tests**

Run: `sbt test`
Expected: All tests pass, including the new `UpdateAllocationGroupUsageTaskSuite` and all existing test suites.

- [ ] **Step 2: Verify no compilation warnings**

Run: `sbt compile 2>&1 | grep -i "warn"`
Expected: No new warnings introduced.
