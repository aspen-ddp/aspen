# SteppedDurableTask Design

## Overview

Add a `SteppedDurableTask` abstract class to the `org.aspen_ddp.aspen.compute` package that generalizes the step-based durable task pattern currently used by `CreateFileTask` and `UnlinkFileTask` in AmoebaFS. It provides a reusable framework for multi-step, crash-recoverable tasks where each step executes within a transaction that atomically advances the task's persistent state.

## Motivation

`CreateFileTask` and `UnlinkFileTask` both implement the same pattern: read current step from a KV object, execute the step's logic within a transaction, atomically update the step counter, and retry on failure. `SteppedDurableTask` extracts this into a shared abstraction so that new multi-step durable tasks don't need to reimplement the step-management boilerplate.

## Protobuf Serialization

Two new messages added to the bottom of `codec.proto`:

```protobuf
message SteppedDurableTaskEntry {
  string key = 1;
  bytes value = 2;
}

message SteppedDurableTaskState {
  uint32 step = 1;
  repeated SteppedDurableTaskEntry entries = 2;
}
```

`SteppedDurableTaskState` encodes both the current step number and the full state map into a single protobuf message. This is stored as the value of a single key (`Key(1)`) in the task's KV object.

Corresponding `encodeSteppedDurableTaskState` and `decodeSteppedDurableTaskState` methods are added to `Codec.scala`, following the existing encode/decode pattern. They convert between `(Int, Map[String, Array[Byte]])` and the protobuf bytes.

## SteppedDurableTask Class

**Package:** `org.aspen_ddp.aspen.compute`
**File:** `SteppedDurableTask.scala`

### Class Signature

```scala
abstract class SteppedDurableTask(
  val taskPointer: DurableTaskPointer,
  val client: AspenClient
) extends DurableTask
```

### Companion Object

- `StepStateKey = Key(1)` — the KV key under which the serialized `SteppedDurableTaskState` is stored in the task object.
- `def getInitialContent(initialState: Map[String, Array[Byte]]): (Key, Value)` — helper that encodes the initial state map at step 0 via `Codec.encodeSteppedDurableTaskState(0, initialState)` and returns `(StepStateKey, Value(encodedBytes))`. Subclass factories use this when building the initial KV content list for `TaskExecutor.prepareTask`, avoiding direct knowledge of `StepStateKey` or the Codec encoding.

### Abstract Members

- `val steps: Array[(Transaction, Map[String, Array[Byte]]) => Future[Map[String, Array[Byte]]]]` — ordered array of step functions. Each receives the current Transaction and state map, adds its operations to the transaction, and returns a Future to the (potentially modified) state map.

### Overridable Members

- `def resultFromState(state: Map[String, Array[Byte]]): Option[AnyRef]` — extracts the task's completion value from the final state map. Defaults to `Some(())`. Subclasses override to return domain-specific results.

### Internal State

- A `Promise[Option[AnyRef]]` backing the `completed` Future.

### Execution Context

- Uses `client.clientContext` as the implicit `ExecutionContext`.

### Constructor Behavior

- Calls `doNextStep()` immediately upon construction, matching the pattern in `CreateFileTask`.

### doNextStep() Method

1. Read the task's KV object via `client.read(taskPointer.kvPointer)`.
2. Deserialize the `StepStateKey` value using `Codec.decodeSteppedDurableTaskState` to obtain `(step, stateMap)`.
3. If `step >= steps.length`, the task is complete: call `resultFromState(stateMap)` and complete the promise. Return.
4. Create a new `Transaction` via `client.newTransaction()`.
5. Invoke `steps(step)(tx, stateMap)`, which returns a `Future[Map[String, Array[Byte]]]`.
6. On Future success with the resulting state map:
   - Encode `(step + 1, resultMap)` via `Codec.encodeSteppedDurableTaskState`.
   - Call `tx.update()` on the task object with:
     - A `KeyRevision(StepStateKey, currentRevision)` requirement to ensure atomic step transition.
     - An `Insert(StepStateKey, encodedBytes)` operation to write the new state.
   - Call `tx.commit()`.
   - On commit success: if `step + 1 >= steps.length`, complete the promise via `resultFromState(resultMap)`; otherwise call `doNextStep()`.
   - On commit failure: log the error, call `doNextStep()`.
7. On Future failure:
   - If the throwable is a `StopRetrying`: complete the promise with `Failure(throwable)`.
   - Otherwise: log the error, call `doNextStep()`.

### Logging

Uses `org.apache.logging.log4j.scala.Logging` mixin, consistent with the rest of the codebase.

## Factory Approach

Each concrete subclass provides its own `DurableTaskFactory` companion object, following the existing pattern established by `CreateFileTask`. The factory's `createTask` method deserializes any subclass-specific constructor arguments from the task object's KV state and constructs the concrete `SteppedDurableTask` subclass.

## Initial State Preparation

When preparing a new stepped durable task for execution, subclass factories call `SteppedDurableTask.getInitialContent(initialStateMap)` to obtain the `(Key, Value)` pair for the step/state entry, then include it alongside any subclass-specific KV entries in the list passed to `TaskExecutor.prepareTask`. Subclass-specific state needed for factory reconstruction (e.g., filesystem UUID, inode data) should be stored as additional KV entries under separate keys, just as `CreateFileTask` does today.

## Files Changed

1. **`src/main/protobuf/codec.proto`** — Add `SteppedDurableTaskEntry` and `SteppedDurableTaskState` messages.
2. **`src/main/scala/org/aspen_ddp/aspen/common/network/Codec.scala`** — Add `encodeSteppedDurableTaskState` and `decodeSteppedDurableTaskState` methods.
3. **`src/main/scala/org/aspen_ddp/aspen/compute/SteppedDurableTask.scala`** — New file containing the abstract class.
