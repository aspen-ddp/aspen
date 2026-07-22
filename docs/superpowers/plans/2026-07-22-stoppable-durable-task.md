# Stoppable DurableTask Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `DurableTask` instances stoppable so that retry loops and step progression abort on lease loss or clean shutdown.

**Architecture:** Add a `@volatile isStopped` flag and `final stop()` method to the `DurableTask` trait. `SteppedDurableTask` and `UpdateAllocationGroupUsageTask` check the flag at the top of their retry-loop entry functions. `SimpleTaskExecutor` gains a `shutdown()` method that stops all active tasks. A new `TaskStopped` exception signals the stop to promise consumers.

**Tech Stack:** Scala 3, ScalaTest (AsyncFunSuite), sbt

---

## File Map

| File | Action | Responsibility |
|---|---|---|
| `src/main/scala/org/aspen_ddp/aspen/compute/TaskStopped.scala` | Create | New exception class |
| `src/main/scala/org/aspen_ddp/aspen/compute/DurableTask.scala` | Modify | Add `isStopped`, `stop()`, `onStop()` |
| `src/main/scala/org/aspen_ddp/aspen/compute/SteppedDurableTask.scala` | Modify | Check `isStopped` at top of `doNextStep()` |
| `src/main/scala/org/aspen_ddp/aspen/common/metadata/management/UpdateAllocationGroupUsageTask.scala` | Modify | Check `isStopped` at top of `updateNextGroup()` |
| `src/main/scala/org/aspen_ddp/aspen/compute/impl/SimpleTaskExecutor.scala` | Modify | Change `active` to Map, add `shutdown()` |
| `src/test/scala/org/aspen_ddp/aspen/compute/SteppedDurableTaskSuite.scala` | Modify | Add stop tests |

---

### Task 1: Create `TaskStopped` exception

**Files:**
- Create: `src/main/scala/org/aspen_ddp/aspen/compute/TaskStopped.scala`

- [ ] **Step 1: Create the exception class**

```scala
package org.aspen_ddp.aspen.compute

class TaskStopped extends Exception
```

- [ ] **Step 2: Verify compilation**

Run: `sbt compile`
Expected: SUCCESS

- [ ] **Step 3: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/compute/TaskStopped.scala
git commit -m "Add TaskStopped exception for stoppable DurableTask"
```

---

### Task 2: Add stop API to `DurableTask` trait

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/compute/DurableTask.scala`

- [ ] **Step 1: Write failing test — calling stop() sets isStopped**

In `src/test/scala/org/aspen_ddp/aspen/compute/SteppedDurableTaskSuite.scala`, add this test at the end of the file:

```scala
class DurableTaskStopSuite extends IntegrationTestSuite:

  atest("stop() sets isStopped and calls onStop()"):
    given ExecutionContext = executionContext

    val onStopCalled = Promise[Unit]()

    val task = new DurableTask:
      val taskPointer: DurableTaskPointer =
        DurableTaskPointer(KeyValueObjectPointer(radicle.toArray))
      val completed: Future[Option[AnyRef]] = Future.successful(None)
      override protected def onStop(): Unit = onStopCalled.trySuccess(())

    task.isStopped should be(false)
    task.stop()
    task.isStopped should be(true)
    onStopCalled.future.map(_ => succeed)
```

Add these imports at the top of the file (alongside the existing ones):

```scala
import scala.concurrent.Promise
import org.aspen_ddp.aspen.common.objects.KeyValueObjectPointer
```

- [ ] **Step 2: Run test to verify it fails**

Run: `sbt 'testOnly *SteppedDurableTaskSuite *DurableTaskStopSuite -- -z "stop() sets isStopped"'`
Expected: FAIL — `stop()` and `isStopped` do not exist on `DurableTask`

- [ ] **Step 3: Implement the stop API on DurableTask**

Replace the `DurableTask` trait body in `src/main/scala/org/aspen_ddp/aspen/compute/DurableTask.scala` with:

```scala
trait DurableTask {

  val taskPointer: DurableTaskPointer

  def completed: Future[Option[AnyRef]]

  @volatile protected var isStopped: Boolean = false

  final def stop(): Unit =
    isStopped = true
    onStop()

  protected def onStop(): Unit = ()
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `sbt 'testOnly *DurableTaskStopSuite -- -z "stop() sets isStopped"'`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/compute/DurableTask.scala \
        src/test/scala/org/aspen_ddp/aspen/compute/SteppedDurableTaskSuite.scala
git commit -m "Add stop()/isStopped/onStop() to DurableTask trait"
```

---

### Task 3: Add `isStopped` check to `SteppedDurableTask.doNextStep()`

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/compute/SteppedDurableTask.scala`
- Modify: `src/test/scala/org/aspen_ddp/aspen/compute/SteppedDurableTaskSuite.scala`

- [ ] **Step 1: Write failing test — stopped task completes with TaskStopped**

Add this test to the `SteppedDurableTaskSuite` class in `src/test/scala/org/aspen_ddp/aspen/compute/SteppedDurableTaskSuite.scala`:

```scala
  atest("stopping a running stepped task completes with TaskStopped"):
    given ExecutionContext = executionContext

    for
      kvos <- client.read(radicle)
      rootPool <- client.getStoragePool(kvos.pointer.poolId)
      allocator = new PoolObjectAllocator(client, rootPool)

      tx0 = client.newTransaction()
      executorRoot <- allocator.allocateKeyValueObject(Map())(using tx0)
      dataPtr <- allocator.allocateDataObject(
        DataBuffer("initial".getBytes(StandardCharsets.UTF_8)))(using tx0)
      _ = tx0.bumpVersion(radicle, kvos.revision)
      _ <- tx0.commit()

      executor <- SimpleTaskExecutor(client, allocator, executorRoot)

      kvos2 <- client.read(radicle)
      tx1 = client.newTransaction()
      taskFuture <- TestSteppedTask.prepareTask(executor, dataPtr)(using tx1)
      _ = tx1.bumpVersion(radicle, kvos2.revision)
      _ <- tx1.commit()

      _ = executor.shutdown()

      result <- taskFuture.failed
    yield
      result shouldBe a[TaskStopped]
```

Add this import at the top of the file:

```scala
import org.aspen_ddp.aspen.compute.TaskStopped
```

- [ ] **Step 2: Run test to verify it fails**

Run: `sbt 'testOnly *SteppedDurableTaskSuite -- -z "stopping a running stepped task"'`
Expected: FAIL — `shutdown()` does not exist on `SimpleTaskExecutor` yet, and `doNextStep()` does not check `isStopped`

Note: This test depends on Task 4 (`SimpleTaskExecutor.shutdown()`) to compile. We'll implement both before running, but the logical order matters for understanding.

- [ ] **Step 3: Add isStopped check to doNextStep()**

In `src/main/scala/org/aspen_ddp/aspen/compute/SteppedDurableTask.scala`, replace the `doNextStep()` method (lines 47-93) with:

```scala
  def doNextStep(): Unit =
    if isStopped then
      synchronized:
        if !promise.isCompleted then
          promise.failure(new TaskStopped)
      return

    for
      kvos <- client.read(taskPointer.kvPointer)
    yield
      val vs = kvos.contents(StepStateKey)
      val (step, stateMap) = Codec.decodeSteppedDurableTaskState(vs.value.bytes)

      if step >= steps.length then
        synchronized:
          if !promise.isCompleted then
            promise.success(resultFromState(stateMap))
      else
        val tx = client.newTransaction()

        steps(step)(tx, stateMap, vs.revision).onComplete:
          case Failure(err) => err match
            case e: StopRetrying =>
              synchronized:
                if !promise.isCompleted then
                  promise.failure(e)
            case e =>
              logger.error(s"Step $step failed for task ${taskPointer.kvPointer}, retrying", e)
              scheduleRetry(step)

          case Success(newState) =>
            val encoded = Codec.encodeSteppedDurableTaskState(step + 1, newState)
            val requirements = KeyValueUpdate.KeyRevision(StepStateKey, vs.revision) :: Nil
            val operations = Insert(StepStateKey, encoded) :: Nil

            tx.update(taskPointer.kvPointer, None, None, requirements, operations)

            tx.commit().onComplete:
              case Failure(err) =>
                err match
                  case _: TransactionAborted =>
                    logger.warn(s"Commit aborted at step $step for task ${taskPointer.kvPointer}, retrying")
                  case _ =>
                    //logger.error(s"Commit failed at step $step for task ${taskPointer.kvPointer}, retrying", err)
                scheduleRetry(step)
              case Success(_) =>
                retryDelay = 16
                if step + 1 >= steps.length then
                  synchronized:
                    if !promise.isCompleted then
                      promise.success(resultFromState(newState))
                else
                  doNextStep()
```

Add the `TaskStopped` import — it's already in the same package (`org.aspen_ddp.aspen.compute`), so no import is needed.

- [ ] **Step 4: Defer running the test until Task 4 is complete** (test needs `SimpleTaskExecutor.shutdown()`)

- [ ] **Step 5: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/compute/SteppedDurableTask.scala \
        src/test/scala/org/aspen_ddp/aspen/compute/SteppedDurableTaskSuite.scala
git commit -m "Add isStopped check to SteppedDurableTask.doNextStep()"
```

---

### Task 4: Change `SimpleTaskExecutor.active` to Map and add `shutdown()`

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/compute/impl/SimpleTaskExecutor.scala`

- [ ] **Step 1: Update the import to include DurableTask**

In `src/main/scala/org/aspen_ddp/aspen/compute/impl/SimpleTaskExecutor.scala`, update the compute import (line 8):

```scala
import org.aspen_ddp.aspen.compute.{DurableTask, DurableTaskPointer, DurableTaskFactory, TaskExecutor}
```

- [ ] **Step 2: Change `active` from Set to Map**

Replace line 50:

```scala
  protected var active: Set[DurableTaskPointer] = Set()
```

with:

```scala
  protected var active: Map[DurableTaskPointer, DurableTask] = Map.empty
```

- [ ] **Step 3: Update constructor recovery to store task instances**

Replace lines 67-69 in the constructor recovery block:

```scala
              case Some(dtt) =>
                dtt.createTask(client, taskPointer, kvos.revision, kvos.contents, this)
                active += taskPointer
```

with:

```scala
              case Some(dtt) =>
                val task = dtt.createTask(client, taskPointer, kvos.revision, kvos.contents, this)
                active += taskPointer -> task
```

- [ ] **Step 4: Update `deallocateTask` to remove from Map**

Replace line 97:

```scala
            active -= task
```

(No change needed — `Map.-=` with a key works identically to `Set.-=`.)

- [ ] **Step 5: Update `prepareTask` to store task in Map**

Replace lines 126-131 in `prepareTask`:

```scala
            val task = taskType.createTask(client, taskPointer, kvos.revision, kvos.contents, this)

            active += taskPointer
            task.completed.foreach: _ =>
              deallocateTask(taskPointer)
            task.completed
```

with:

```scala
            val task = taskType.createTask(client, taskPointer, kvos.revision, kvos.contents, this)

            active += taskPointer -> task
            task.completed.foreach: _ =>
              deallocateTask(taskPointer)
            task.completed
```

- [ ] **Step 6: Add `shutdown()` method**

Add this method to `SimpleTaskExecutor`, after the `inactive` declaration (after line 51):

```scala
  def shutdown(): Unit = synchronized:
    active.values.foreach(_.stop())
```

- [ ] **Step 7: Run the deferred test from Task 3**

Run: `sbt 'testOnly *SteppedDurableTaskSuite -- -z "stopping a running stepped task"'`
Expected: PASS

- [ ] **Step 8: Run existing SteppedDurableTask tests to check for regressions**

Run: `sbt 'testOnly *SteppedDurableTaskSuite'`
Expected: All tests PASS

- [ ] **Step 9: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/compute/impl/SimpleTaskExecutor.scala
git commit -m "Change SimpleTaskExecutor.active to Map and add shutdown()"
```

---

### Task 5: Add `isStopped` check to `UpdateAllocationGroupUsageTask`

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/common/metadata/management/UpdateAllocationGroupUsageTask.scala`

- [ ] **Step 1: Add TaskStopped import**

Add this import to the file (alongside the existing compute imports on line 11):

```scala
import org.aspen_ddp.aspen.compute.{DurableTask, DurableTaskFactory, DurableTaskPointer, TaskExecutor, TaskStopped}
```

- [ ] **Step 2: Add isStopped check to updateNextGroup()**

Replace the `updateNextGroup()` method body (line 113-190). Add the guard at the top:

```scala
  private def updateNextGroup(): Unit =
    if isStopped then
      failTask(new TaskStopped)
      return

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

          val fFetch = for
            agsPtr <- client.getAllocationGroupPointer(groupId)
            agsDos <- client.read(agsPtr)
          yield (agsPtr, agsDos)

          fFetch.onComplete:
            case Failure(err) => err match
              case _: NoSuchElementException | _: ReadError =>
                advanceIndex(kvos)
              case e: StopRetrying =>
                failTask(e)
              case _ =>
                scheduleRetry()

            case Success((agsPtr, agsDos)) =>
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
                    tx.commit()
                  .onComplete:
                    case Success(_) =>
                      retryDelay = 16
                      updateNextGroup()
                    case Failure(_) =>
                      scheduleRetry()
```

- [ ] **Step 3: Verify compilation**

Run: `sbt compile`
Expected: SUCCESS

- [ ] **Step 4: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/common/metadata/management/UpdateAllocationGroupUsageTask.scala
git commit -m "Add isStopped check to UpdateAllocationGroupUsageTask.updateNextGroup()"
```

---

### Task 6: Run full test suite

- [ ] **Step 1: Run all tests**

Run: `sbt test`
Expected: All tests PASS

- [ ] **Step 2: Fix any failures**

If any tests fail, investigate and fix. The most likely issue would be compilation errors from the `active` type change in `SimpleTaskExecutor`.

- [ ] **Step 3: Final commit if any fixes were needed**

```bash
git add -A
git commit -m "Fix any regressions from stoppable DurableTask changes"
```
