# SteppedDurableTask Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a reusable `SteppedDurableTask` abstract class to the compute package that generalizes the step-based durable task pattern used by `CreateFileTask` and `UnlinkFileTask`.

**Architecture:** A protobuf message (`SteppedDurableTaskState`) stores both the step counter and a `Map[String, Array[Byte]]` state map as a single serialized value under `Key(1)` in the task's KV object. The `SteppedDurableTask` abstract class manages the step loop — reading state, invoking the current step function, atomically advancing the step counter via a `KeyRevision` guard, and retrying on failure.

**Tech Stack:** Scala 3, Google Protocol Buffers, Log4j2

**Spec:** `docs/superpowers/specs/2026-05-15-stepped-durable-task-design.md`

---

## File Map

| Action | File | Responsibility |
|--------|------|---------------|
| Modify | `src/main/protobuf/codec.proto` | Add `SteppedDurableTaskEntry` and `SteppedDurableTaskState` protobuf messages |
| Modify | `src/main/scala/org/aspen_ddp/aspen/common/network/Codec.scala` | Add `encodeSteppedDurableTaskState` / `decodeSteppedDurableTaskState` methods |
| Create | `src/main/scala/org/aspen_ddp/aspen/compute/SteppedDurableTask.scala` | Abstract class + companion object |

---

### Task 1: Add protobuf messages to codec.proto

**Files:**
- Modify: `src/main/protobuf/codec.proto` (append after line 609, the final `StorageDevice` message)

- [ ] **Step 1: Add the two new messages to the bottom of codec.proto**

Append these messages at the end of the file (before the closing blank line), after the existing `StorageDevice` message:

```protobuf
//---------------------------------------------------------------------------
//- Stepped Durable Task State
//---------------------------------------------------------------------------
message SteppedDurableTaskEntry {
  string key = 1;
  bytes value = 2;
}

message SteppedDurableTaskState {
  uint32 step = 1;
  repeated SteppedDurableTaskEntry entries = 2;
}
```

- [ ] **Step 2: Compile to verify protobuf generation succeeds**

Run: `sbt compile`
Expected: Compiles successfully. The protobuf compiler generates `SteppedDurableTaskEntry.java` and `SteppedDurableTaskState.java` in the codec package.

- [ ] **Step 3: Commit**

```bash
git add src/main/protobuf/codec.proto
git commit -m "Add SteppedDurableTaskState protobuf messages"
```

---

### Task 2: Add Codec encode/decode methods

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/common/network/Codec.scala` (append after the `StorageDevice` encode/decode block, around line 1329)

- [ ] **Step 1: Add encodeSteppedDurableTaskState to Codec.scala**

Add the following method at the bottom of the `Codec` object, after the `decode(m: codec.StorageDevice)` method:

```scala
  def encodeSteppedDurableTaskState(step: Int, state: Map[String, Array[Byte]]): Array[Byte] =
    val builder = codec.SteppedDurableTaskState.newBuilder()
    builder.setStep(step)
    state.foreach: (key, value) =>
      builder.addEntries(
        codec.SteppedDurableTaskEntry.newBuilder()
          .setKey(key)
          .setValue(ByteString.copyFrom(value))
          .build
      )
    builder.build.toByteArray
```

- [ ] **Step 2: Add decodeSteppedDurableTaskState to Codec.scala**

Add the following method immediately after `encodeSteppedDurableTaskState`:

```scala
  def decodeSteppedDurableTaskState(data: Array[Byte]): (Int, Map[String, Array[Byte]]) =
    val m = codec.SteppedDurableTaskState.parseFrom(data)
    val step = m.getStep
    val state = m.getEntriesList.asScala.map: entry =>
      entry.getKey -> entry.getValue.toByteArray
    .toMap
    (step, state)
```

- [ ] **Step 3: Compile to verify**

Run: `sbt compile`
Expected: Compiles successfully.

- [ ] **Step 4: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/common/network/Codec.scala
git commit -m "Add Codec methods for SteppedDurableTaskState serialization"
```

---

### Task 3: Create SteppedDurableTask class

**Files:**
- Create: `src/main/scala/org/aspen_ddp/aspen/compute/SteppedDurableTask.scala`

- [ ] **Step 1: Create the SteppedDurableTask.scala file with companion object and abstract class**

Create the file at `src/main/scala/org/aspen_ddp/aspen/compute/SteppedDurableTask.scala` with this content:

```scala
package org.aspen_ddp.aspen.compute

import org.apache.logging.log4j.scala.Logging
import org.aspen_ddp.aspen.client.{AspenClient, StopRetrying, Transaction}
import org.aspen_ddp.aspen.common.network.Codec
import org.aspen_ddp.aspen.common.objects.{Insert, Key, Value}
import org.aspen_ddp.aspen.common.transaction.KeyValueUpdate

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success}

object SteppedDurableTask:
  val StepStateKey: Key = Key(1)

  def getInitialContent(initialState: Map[String, Array[Byte]]): (Key, Value) =
    val encoded = Codec.encodeSteppedDurableTaskState(0, initialState)
    (StepStateKey, Value(encoded))

abstract class SteppedDurableTask(
  val taskPointer: DurableTaskPointer,
  val client: AspenClient
) extends DurableTask with Logging:

  import SteppedDurableTask.*

  given ExecutionContext = client.clientContext

  val steps: Array[(Transaction, Map[String, Array[Byte]]) => Future[Map[String, Array[Byte]]]]

  def resultFromState(state: Map[String, Array[Byte]]): Option[AnyRef] = Some(())

  private val promise = Promise[Option[AnyRef]]()

  def completed: Future[Option[AnyRef]] = promise.future

  doNextStep()

  def doNextStep(): Unit =
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

        steps(step)(tx, stateMap).onComplete:
          case Failure(err) => err match
            case e: StopRetrying =>
              synchronized:
                if !promise.isCompleted then
                  promise.failure(e)
            case e =>
              logger.error(s"Step $step failed for task ${taskPointer.kvPointer}, retrying", e)
              doNextStep()

          case Success(newState) =>
            val encoded = Codec.encodeSteppedDurableTaskState(step + 1, newState)
            val requirements = KeyValueUpdate.KeyRevision(StepStateKey, vs.revision) :: Nil
            val operations = Insert(StepStateKey, encoded) :: Nil

            tx.update(taskPointer.kvPointer, None, None, requirements, operations)

            tx.commit().onComplete:
              case Failure(err) =>
                logger.error(s"Commit failed at step $step for task ${taskPointer.kvPointer}, retrying", err)
                doNextStep()
              case Success(_) =>
                if step + 1 >= steps.length then
                  synchronized:
                    if !promise.isCompleted then
                      promise.success(resultFromState(newState))
                else
                  doNextStep()
```

- [ ] **Step 2: Compile to verify**

Run: `sbt compile`
Expected: Compiles successfully.

- [ ] **Step 3: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/compute/SteppedDurableTask.scala
git commit -m "Add SteppedDurableTask abstract class for multi-step durable tasks"
```

---

### Task 4: Run full test suite

- [ ] **Step 1: Run all tests**

Run: `sbt test`
Expected: All existing tests pass. No regressions from the new code.

- [ ] **Step 2: Commit (if any adjustments were needed)**

Only if compilation or tests required fixes. Otherwise skip.
