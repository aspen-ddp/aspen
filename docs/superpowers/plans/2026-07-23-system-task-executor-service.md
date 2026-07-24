# SystemTaskExecutorService Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a cluster-wide singleton `SystemTaskExecutorService` that dispatches system `DurableTask`s to arbitrary hosts, monitors their liveness via the task object's revision, and reassigns on stall — plus the client API to create-and-enroll such tasks.

**Architecture:** A leased `DurableService` (mirroring `RebalancingDurableService`) owns a TKVL of pending tasks. It dispatches each task to a randomly-selected host via a new `ExecuteSystemTask` `HostMessage`. The receiving host runs the task **transiently** (reconstructed from its durable state object) and heartbeats it by bumping the object's version every 15s. The service polls each task object's revision; if it stalls, the task is reassigned. Durability and reassignment live entirely in the service; hosts are stateless executors.

**Tech Stack:** Scala 3, ScalaPB (protobuf codegen from `codec.proto`), ScalaTest (`AnyFunSuite` for pure unit tests, `IntegrationTestSuite`/`TestNetwork` for integration), the existing TKVL / `DurableService` / `DurableTask` machinery.

**Spec:** `docs/superpowers/specs/2026-07-23-system-task-executor-service-design.md`

---

## Conventions used throughout this plan

- Build: `sbt compile`. Full tests: `sbt test`. Single suite: `sbt 'testOnly *SuiteName'`. Single test by substring: `sbt 'testOnly *SuiteName -- -z "substring"'`.
- Editing `src/main/protobuf/codec.proto` regenerates the `org.aspen_ddp.aspen.codec.*` classes on the next `sbt compile`.
- New UUID literals used by this feature (all verified distinct from existing registered UUIDs):
  - `SystemTaskExecutorService.ServiceTypeUUID = a1e5c9d4-3b7f-4a21-9c8e-2d6f0b13e7a4`
  - `SystemTaskExecutorService.ServiceUUID     = c4d8f2b6-1a09-4e73-8b5d-7f3c9e0a24d1`
  - test-only `CountingSystemTask.typeUUID     = e7b3a1c9-5d42-4f86-9a0b-3c1e8d7f206b`
- The per-task durable state object reuses `SimpleTaskExecutor.TaskTypeKey` to hold the task's type UUID, so the runner reconstructs the task exactly as `SimpleTaskExecutor` does.

---

## File Structure

**New files (`src/main/scala/org/aspen_ddp/aspen/compute/systemtask/`):**
- `SystemTaskExecutorService.scala` — companion (UUIDs, `TaskTreeKey`, factory) + service class (dispatch/monitor/reassign).
- `SystemTaskServiceState.scala` — task-TKVL helpers: enroll, scan, remove.
- `SystemTaskMessage.scala` — `ServiceMessage`-carried ADT (`NewSystemTaskAdded`, `SystemTaskComplete`) + encode/decode.
- `SystemTaskRunner.scala` — per-host transient executor + heartbeat.

**Modified main files:**
- `common/network/Message.scala` — add `ExecuteSystemTask` `HostMessage`.
- `src/main/protobuf/codec.proto` — proto messages + root-message oneof entry.
- `common/network/Codec.scala` — encode/decode for the new proto messages.
- `common/network/implementations/zmqnet/ProtobufMessageCodec.scala` — wire encode/decode for `ExecuteSystemTask`.
- `server/store/Bootstrap.scala` — allocate service state (with empty task tree) + services-tree entry.
- `server/TypeFactories.scala` — register `SystemTaskExecutorService`.
- `client/AspenClient.scala` — abstract `createSystemDurableTask`.
- `client/internal/BaseAspenClient.scala` — implement `createSystemDurableTask`.
- `server/StoreManager.scala` — own a `SystemTaskRunner`, dispatch `ExecuteSystemTask`, drive the 15s heartbeat.

**Modified/new test files:**
- `src/test/scala/org/aspen_ddp/aspen/compute/systemtask/SystemTaskMessageSuite.scala` (new)
- `src/test/scala/org/aspen_ddp/aspen/common/network/ExecuteSystemTaskCodecSuite.scala` (new)
- `src/test/scala/org/aspen_ddp/aspen/compute/systemtask/SystemTaskBootstrapSuite.scala` (new)
- `src/test/scala/org/aspen_ddp/aspen/compute/systemtask/SystemTaskServiceStateSuite.scala` (new)
- `src/test/scala/org/aspen_ddp/aspen/compute/systemtask/CreateSystemDurableTaskSuite.scala` (new)
- `src/test/scala/org/aspen_ddp/aspen/compute/systemtask/CountingSystemTask.scala` (new, test-only task + factory)
- `src/test/scala/org/aspen_ddp/aspen/compute/systemtask/SystemTaskRunnerSuite.scala` (new)
- `src/test/scala/org/aspen_ddp/aspen/compute/systemtask/SystemTaskExecutorServiceSuite.scala` (new)
- `src/test/scala/org/aspen_ddp/aspen/TestNetwork.scala` (modified — add a helper to drain captured host messages)

---

## Task 1: Service companion + Bootstrap registration

Creates the companion object holding the fixed UUIDs and `TaskTreeKey`, and seeds the service's state object (containing an empty task TKVL) plus an unclaimed services-tree entry at bootstrap. No factory/executor logic yet — this task only makes the durable state exist and be readable.

**Files:**
- Create: `src/main/scala/org/aspen_ddp/aspen/compute/systemtask/SystemTaskExecutorService.scala`
- Modify: `src/main/scala/org/aspen_ddp/aspen/server/store/Bootstrap.scala`
- Test: `src/test/scala/org/aspen_ddp/aspen/compute/systemtask/SystemTaskBootstrapSuite.scala`

- [ ] **Step 1: Write the failing test**

Create `src/test/scala/org/aspen_ddp/aspen/compute/systemtask/SystemTaskBootstrapSuite.scala`:

```scala
package org.aspen_ddp.aspen.compute.systemtask

import org.aspen_ddp.aspen.IntegrationTestSuite
import org.aspen_ddp.aspen.client.tkvl.{KVObjectRootManager, TieredKeyValueList}
import org.aspen_ddp.aspen.common.Radicle
import org.aspen_ddp.aspen.common.objects.Key
import org.aspen_ddp.aspen.compute.ServiceEntry

import scala.concurrent.ExecutionContext

class SystemTaskBootstrapSuite extends IntegrationTestSuite:

  atest("bootstrap registers SystemTaskExecutorService with an empty task tree"):
    given ExecutionContext = executionContext
    val servicesTkvl = TieredKeyValueList(client,
      KVObjectRootManager(client, Radicle.ServicesTreeKey, Radicle.pointer))
    for
      entryVs <- servicesTkvl.get(Key(SystemTaskExecutorService.ServiceUUID))
      entry = ServiceEntry.decode(entryVs.get.value.bytes)
      statePtr = entry.statePointer
      stateKvos <- client.read(statePtr)
      // The task tree hosted in the service state object must be present and empty.
      taskTkvl = TieredKeyValueList(client,
        KVObjectRootManager(client, SystemTaskExecutorService.TaskTreeKey, statePtr))
      count <- {
        var n = 0
        taskTkvl.foreach((_, _, _) => { n += 1; scala.concurrent.Future.unit }).map(_ => n)
      }
    yield
      entry.typeUUID shouldBe SystemTaskExecutorService.ServiceTypeUUID
      stateKvos.contents.contains(SystemTaskExecutorService.TaskTreeKey) shouldBe true
      count shouldBe 0
```

- [ ] **Step 2: Run test to verify it fails**

Run: `sbt 'testOnly *SystemTaskBootstrapSuite'`
Expected: FAIL to compile — `SystemTaskExecutorService` does not exist.

- [ ] **Step 3: Create the companion object**

Create `src/main/scala/org/aspen_ddp/aspen/compute/systemtask/SystemTaskExecutorService.scala`:

```scala
package org.aspen_ddp.aspen.compute.systemtask

import org.aspen_ddp.aspen.common.objects.Key

import java.util.UUID

/** System-level distributed DurableTask execution service.
 *
 *  This object currently holds only the fixed identifiers and state-object layout used by
 *  Bootstrap. The DurableServiceFactory and service class are added in a later task. */
object SystemTaskExecutorService:

  val ServiceTypeUUID: UUID = UUID.fromString("a1e5c9d4-3b7f-4a21-9c8e-2d6f0b13e7a4")
  val ServiceUUID: UUID     = UUID.fromString("c4d8f2b6-1a09-4e73-8b5d-7f3c9e0a24d1")

  /** Key in the service state object whose value is the encoded Root of the task TKVL
   *  (Key = task UUID -> Value = encoded KeyValueObjectPointer of the task state object). */
  val TaskTreeKey: Key = Key(0)
```

- [ ] **Step 4: Seed the service state + entry in Bootstrap**

In `src/main/scala/org/aspen_ddp/aspen/server/store/Bootstrap.scala`, add the import near the existing `RebalancingDurableService` import (line ~17):

```scala
import org.aspen_ddp.aspen.compute.systemtask.SystemTaskExecutorService
```

Then, immediately after the block that builds `rebalancingServiceEntry` (ends at line ~189) and before `val servicesTree = allocateTree(`, insert:

```scala
    // The SystemTaskExecutorService is likewise a system-critical singleton. Its state object
    // holds an (initially empty) TKVL of pending system tasks.
    val systemTaskTree = allocateTree(ByteArrayKeyOrdering)
    val systemTaskServicePtr = allocate(List(
      SystemTaskExecutorService.TaskTreeKey -> systemTaskTree.encode()
    ))
    val systemTaskServiceEntry = ServiceEntry(
      SystemTaskExecutorService.ServiceTypeUUID,
      ServiceEntry.UnclaimedHostId,
      HLCTimestamp.Zero,
      systemTaskServicePtr)
```

Then replace the existing `servicesTree` allocation:

```scala
    val servicesTree = allocateTree(
      ByteArrayKeyOrdering,
      Key(RebalancingDurableService.ServiceUUID) -> rebalancingServiceEntry.encode()
    )
```

with:

```scala
    val servicesTree = allocateTree(
      ByteArrayKeyOrdering,
      Key(RebalancingDurableService.ServiceUUID) -> rebalancingServiceEntry.encode(),
      Key(SystemTaskExecutorService.ServiceUUID) -> systemTaskServiceEntry.encode()
    )
```

- [ ] **Step 5: Run test to verify it passes**

Run: `sbt 'testOnly *SystemTaskBootstrapSuite'`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/compute/systemtask/SystemTaskExecutorService.scala \
        src/main/scala/org/aspen_ddp/aspen/server/store/Bootstrap.scala \
        src/test/scala/org/aspen_ddp/aspen/compute/systemtask/SystemTaskBootstrapSuite.scala
git commit -m "feat: register SystemTaskExecutorService state in Bootstrap"
```

---

## Task 2: SystemTaskMessage ADT (ServiceMessage wake-ups)

Adds the `ServiceMessage`-carried payloads the client and executing hosts send to the service owner. Best-effort; correctness comes from polling.

**Files:**
- Modify: `src/main/protobuf/codec.proto`
- Create: `src/main/scala/org/aspen_ddp/aspen/compute/systemtask/SystemTaskMessage.scala`
- Test: `src/test/scala/org/aspen_ddp/aspen/compute/systemtask/SystemTaskMessageSuite.scala`

- [ ] **Step 1: Write the failing test**

Create `src/test/scala/org/aspen_ddp/aspen/compute/systemtask/SystemTaskMessageSuite.scala`:

```scala
package org.aspen_ddp.aspen.compute.systemtask

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.util.UUID

class SystemTaskMessageSuite extends AnyFunSuite with Matchers:

  test("NewSystemTaskAdded round-trips"):
    val m = NewSystemTaskAdded(UUID.randomUUID())
    SystemTaskMessage.decode(SystemTaskMessage.encode(m)) shouldBe m

  test("SystemTaskComplete round-trips"):
    val m = SystemTaskComplete(UUID.randomUUID())
    SystemTaskMessage.decode(SystemTaskMessage.encode(m)) shouldBe m
```

- [ ] **Step 2: Run test to verify it fails**

Run: `sbt 'testOnly *SystemTaskMessageSuite'`
Expected: FAIL to compile — `SystemTaskMessage` does not exist.

- [ ] **Step 3: Add proto messages**

In `src/main/protobuf/codec.proto`, immediately after the `RebalancingMessage` block (around line 542, after the `message RebalancingMessage { ... }` closes), add:

```protobuf
message NewSystemTaskAdded {
  UUID taskId = 1;
}

message SystemTaskComplete {
  UUID taskId = 1;
}

message SystemTaskMessage {
  oneof msg {
    NewSystemTaskAdded newSystemTaskAdded = 1;
    SystemTaskComplete systemTaskComplete = 2;
  }
}
```

- [ ] **Step 4: Create the ADT + codec**

Create `src/main/scala/org/aspen_ddp/aspen/compute/systemtask/SystemTaskMessage.scala`:

```scala
package org.aspen_ddp.aspen.compute.systemtask

import org.aspen_ddp.aspen.codec
import org.aspen_ddp.aspen.common.network.Codec

import java.util.UUID

/** Payload types carried in the `encodedContent` of a ServiceMessage addressed to the
 *  SystemTaskExecutorService. Best-effort wake-ups; correctness comes from polling. */
sealed trait SystemTaskMessage

case class NewSystemTaskAdded(taskId: UUID) extends SystemTaskMessage
case class SystemTaskComplete(taskId: UUID) extends SystemTaskMessage

object SystemTaskMessage:

  def encode(m: SystemTaskMessage): Array[Byte] =
    val proto = m match
      case NewSystemTaskAdded(taskId) =>
        codec.SystemTaskMessage(codec.SystemTaskMessage.Msg.NewSystemTaskAdded(
          codec.NewSystemTaskAdded(taskId = Some(Codec.encodeUUID(taskId)))))
      case SystemTaskComplete(taskId) =>
        codec.SystemTaskMessage(codec.SystemTaskMessage.Msg.SystemTaskComplete(
          codec.SystemTaskComplete(taskId = Some(Codec.encodeUUID(taskId)))))
    proto.toByteArray

  def decode(bytes: Array[Byte]): SystemTaskMessage =
    val proto = codec.SystemTaskMessage.parseFrom(bytes)
    proto.msg match
      case codec.SystemTaskMessage.Msg.NewSystemTaskAdded(m) =>
        NewSystemTaskAdded(Codec.decodeUUID(m.taskId.get))
      case codec.SystemTaskMessage.Msg.SystemTaskComplete(m) =>
        SystemTaskComplete(Codec.decodeUUID(m.taskId.get))
      case codec.SystemTaskMessage.Msg.Empty =>
        throw new IllegalArgumentException("Empty SystemTaskMessage")
```

- [ ] **Step 5: Run test to verify it passes**

Run: `sbt 'testOnly *SystemTaskMessageSuite'`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add src/main/protobuf/codec.proto \
        src/main/scala/org/aspen_ddp/aspen/compute/systemtask/SystemTaskMessage.scala \
        src/test/scala/org/aspen_ddp/aspen/compute/systemtask/SystemTaskMessageSuite.scala
git commit -m "feat: add SystemTaskMessage wake-up ADT + codec"
```

---

## Task 3: ExecuteSystemTask HostMessage

Adds the wire message the service sends to an arbitrary host to run a task. Unlike the wake-ups, this is a real `HostMessage` routed to a specific host and must be handled by `ProtobufMessageCodec`.

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/common/network/Message.scala`
- Modify: `src/main/protobuf/codec.proto`
- Modify: `src/main/scala/org/aspen_ddp/aspen/common/network/Codec.scala`
- Modify: `src/main/scala/org/aspen_ddp/aspen/common/network/implementations/zmqnet/ProtobufMessageCodec.scala`
- Test: `src/test/scala/org/aspen_ddp/aspen/common/network/ExecuteSystemTaskCodecSuite.scala`

- [ ] **Step 1: Write the failing test**

Create `src/test/scala/org/aspen_ddp/aspen/common/network/ExecuteSystemTaskCodecSuite.scala`:

```scala
package org.aspen_ddp.aspen.common.network

import org.aspen_ddp.aspen.IntegrationTestSuite
import org.aspen_ddp.aspen.common.metadata.HostId
import org.aspen_ddp.aspen.common.network.implementations.zmqnet.ProtobufMessageCodec

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

class ExecuteSystemTaskCodecSuite extends IntegrationTestSuite:

  atest("ExecuteSystemTask round-trips through the wire codec"):
    given ExecutionContext = executionContext
    val codecInst = new ProtobufMessageCodec
    val msg = ExecuteSystemTask(
      toHost = HostId(UUID.randomUUID()),
      fromClient = client.clientId,
      taskId = UUID.randomUUID(),
      taskStatePointer = net.radicle)  // net.radicle is a valid KeyValueObjectPointer

    val decoded = codecInst.decodeMessage(codecInst.encodeMessage(msg)).get
      .asInstanceOf[ExecuteSystemTask]

    Future.successful:
      decoded.toHost shouldBe msg.toHost
      decoded.fromClient shouldBe msg.fromClient
      decoded.taskId shouldBe msg.taskId
      decoded.taskStatePointer.toArray shouldBe msg.taskStatePointer.toArray
```

- [ ] **Step 2: Run test to verify it fails**

Run: `sbt 'testOnly *ExecuteSystemTaskCodecSuite'`
Expected: FAIL to compile — `ExecuteSystemTask` does not exist.

- [ ] **Step 3: Add the case class to Message.scala**

In `src/main/scala/org/aspen_ddp/aspen/common/network/Message.scala`, add `KeyValueObjectPointer` to the objects import (line 7):

```scala
import org.aspen_ddp.aspen.common.objects.{KeyValueObjectPointer, ObjectId, ObjectPointer, ObjectRefcount, ObjectRevision, ObjectType, ReadError, ReadType}
```

Then add this case class immediately after the `ServiceMessage` definition (after line 61):

```scala
final case class ExecuteSystemTask(
                                    toHost: HostId,
                                    fromClient: ClientId,
                                    taskId: UUID,
                                    taskStatePointer: KeyValueObjectPointer
                                  ) extends HostMessage
```

- [ ] **Step 4: Add the proto message + root oneof entry**

In `src/main/protobuf/codec.proto`, add after the `ServiceMessage` message (after line 407):

```protobuf
message ExecuteSystemTask {
  UUID toHost = 1;
  UUID fromClient = 2;
  UUID taskId = 3;
  bytes taskStatePointer = 4;
}
```

Then add to the root `Message` oneof (after `ServiceMessage service_message = 25;`, line 437):

```protobuf
    ExecuteSystemTask execute_system_task = 26;
```

- [ ] **Step 5: Add Codec encode/decode**

In `src/main/scala/org/aspen_ddp/aspen/common/network/Codec.scala`, add after the `ServiceMessage` decode (after line 963). Note `KeyValueObjectPointer` and `ByteString` are already imported in this file:

```scala
  def encode(o: ExecuteSystemTask): codec.ExecuteSystemTask =
    codec.ExecuteSystemTask(
      toHost = Some(encodeUUID(o.toHost.uuid)),
      fromClient = Some(encodeUUID(o.fromClient.uuid)),
      taskId = Some(encodeUUID(o.taskId)),
      taskStatePointer = ByteString.copyFrom(o.taskStatePointer.toArray)
    )

  def decode(m: codec.ExecuteSystemTask): ExecuteSystemTask =
    val toHost = HostId(decodeUUID(m.toHost.get))
    val fromClient = ClientId(decodeUUID(m.fromClient.get))
    val taskId = decodeUUID(m.taskId.get)
    val taskStatePointer = KeyValueObjectPointer(m.taskStatePointer.toByteArray)
    ExecuteSystemTask(toHost, fromClient, taskId, taskStatePointer)
```

If `HostId` / `ClientId` are not already imported in `Codec.scala`, they are (used by the surrounding `StartStoreTransfer`/`ServiceMessage` codecs). If `KeyValueObjectPointer` is not imported, add it to the existing `common.objects` import line.

- [ ] **Step 6: Wire ProtobufMessageCodec encode/decode**

In `src/main/scala/org/aspen_ddp/aspen/common/network/implementations/zmqnet/ProtobufMessageCodec.scala`, in `encodeHostMessage` (line 114-119), add a case:

```scala
      case m: ExecuteSystemTask => codec.Message.Msg.ExecuteSystemTask(Codec.encode(m))
```

And in the decode match (after the `ServiceMessage` case, line 258-259), add:

```scala
        case codec.Message.Msg.ExecuteSystemTask(r) =>
          Some(Codec.decode(r))
```

- [ ] **Step 7: Run test to verify it passes**

Run: `sbt 'testOnly *ExecuteSystemTaskCodecSuite'`
Expected: PASS.

- [ ] **Step 8: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/common/network/Message.scala \
        src/main/protobuf/codec.proto \
        src/main/scala/org/aspen_ddp/aspen/common/network/Codec.scala \
        src/main/scala/org/aspen_ddp/aspen/common/network/implementations/zmqnet/ProtobufMessageCodec.scala \
        src/test/scala/org/aspen_ddp/aspen/common/network/ExecuteSystemTaskCodecSuite.scala
git commit -m "feat: add ExecuteSystemTask host message + wire codec"
```

---

## Task 4: SystemTaskServiceState — task-tree helpers

Provides the enroll / scan / remove operations over the service's task TKVL. These are pure helpers keyed off the service state object pointer; both the client (`createSystemDurableTask`) and the service reuse them.

**Files:**
- Create: `src/main/scala/org/aspen_ddp/aspen/compute/systemtask/SystemTaskServiceState.scala`
- Test: `src/test/scala/org/aspen_ddp/aspen/compute/systemtask/SystemTaskServiceStateSuite.scala`

- [ ] **Step 1: Write the failing test**

Create `src/test/scala/org/aspen_ddp/aspen/compute/systemtask/SystemTaskServiceStateSuite.scala`:

```scala
package org.aspen_ddp.aspen.compute.systemtask

import org.aspen_ddp.aspen.IntegrationTestSuite
import org.aspen_ddp.aspen.client.Transaction
import org.aspen_ddp.aspen.client.internal.allocation.PoolObjectAllocator
import org.aspen_ddp.aspen.client.tkvl.{KVObjectRootManager, TieredKeyValueList}
import org.aspen_ddp.aspen.common.Radicle
import org.aspen_ddp.aspen.common.objects.{Key, Value}
import org.aspen_ddp.aspen.compute.ServiceEntry

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

class SystemTaskServiceStateSuite extends IntegrationTestSuite:

  private def servicePtr(): Future[org.aspen_ddp.aspen.common.objects.KeyValueObjectPointer] =
    given ExecutionContext = executionContext
    val servicesTkvl = TieredKeyValueList(client,
      KVObjectRootManager(client, Radicle.ServicesTreeKey, Radicle.pointer))
    servicesTkvl.get(Key(SystemTaskExecutorService.ServiceUUID)).map: vs =>
      ServiceEntry.decode(vs.get.value.bytes).statePointer

  atest("enroll then scan returns the task; remove empties the tree"):
    given ExecutionContext = executionContext
    val taskId = UUID.randomUUID()
    for
      statePtr <- servicePtr()
      // Allocate a dummy task-state object to enroll.
      pool <- client.getStoragePool(Radicle.poolId)
      allocator = new PoolObjectAllocator(client, pool)
      taskStatePtr <- client.transactUntilSuccessful: tx =>
                        given Transaction = tx
                        allocator.allocateKeyValueObject(Map(Key(1) -> Value(Array[Byte](7))))
      _ <- SystemTaskServiceState.enroll(client, statePtr, taskId, taskStatePtr)
      afterEnroll <- SystemTaskServiceState.scan(client, statePtr)
      _ <- SystemTaskServiceState.remove(client, statePtr, taskId)
      afterRemove <- SystemTaskServiceState.scan(client, statePtr)
    yield
      afterEnroll.map(_._1) should contain (taskId)
      afterEnroll.find(_._1 == taskId).map(_._2.id) shouldBe Some(taskStatePtr.id)
      afterRemove.map(_._1) should not contain taskId
```

- [ ] **Step 2: Run test to verify it fails**

Run: `sbt 'testOnly *SystemTaskServiceStateSuite'`
Expected: FAIL to compile — `SystemTaskServiceState` does not exist.

- [ ] **Step 3: Implement the helpers**

Create `src/main/scala/org/aspen_ddp/aspen/compute/systemtask/SystemTaskServiceState.scala`:

```scala
package org.aspen_ddp.aspen.compute.systemtask

import org.aspen_ddp.aspen.client.{AspenClient, Transaction}
import org.aspen_ddp.aspen.client.tkvl.{KVObjectRootManager, TieredKeyValueList}
import org.aspen_ddp.aspen.common.objects.{Key, KeyValueObjectPointer, Value}
import org.aspen_ddp.aspen.common.util.{byte2uuid, uuid2byte}

import java.util.UUID
import scala.collection.mutable.ListBuffer
import scala.concurrent.{ExecutionContext, Future}

/** Helpers over the SystemTaskExecutorService task TKVL, hosted in the service state object
 *  under `SystemTaskExecutorService.TaskTreeKey`. Key = task UUID, Value = encoded pointer to
 *  the task's durable state object. */
object SystemTaskServiceState:

  private def tkvl(client: AspenClient, statePtr: KeyValueObjectPointer): TieredKeyValueList =
    TieredKeyValueList(client, KVObjectRootManager(client, SystemTaskExecutorService.TaskTreeKey, statePtr))

  /** Insert a task into the registry. Idempotent: a re-enroll of the same key overwrites the
   *  value (the pointer is stable for a given task UUID). */
  def enroll(client: AspenClient,
             statePtr: KeyValueObjectPointer,
             taskId: UUID,
             taskStatePtr: KeyValueObjectPointer): Future[Unit] =
    given ExecutionContext = client.clientContext
    client.transactUntilSuccessful: tx =>
      given Transaction = tx
      tkvl(client, statePtr).set(Key(uuid2byte(taskId)), Value(taskStatePtr.toArray))

  /** Read all enrolled (taskId, taskStatePointer) pairs. */
  def scan(client: AspenClient,
           statePtr: KeyValueObjectPointer): Future[List[(UUID, KeyValueObjectPointer)]] =
    given ExecutionContext = client.clientContext
    val buf = ListBuffer[(UUID, KeyValueObjectPointer)]()
    tkvl(client, statePtr).foreach: (_, key, vs) =>
      buf += ((byte2uuid(key.bytes), KeyValueObjectPointer(vs.value.bytes)))
      Future.unit
    .map(_ => buf.toList)

  /** Remove a task from the registry. Idempotent: a no-op if the key is already gone. */
  def remove(client: AspenClient,
             statePtr: KeyValueObjectPointer,
             taskId: UUID): Future[Unit] =
    given ExecutionContext = client.clientContext
    client.transactUntilSuccessful: tx =>
      given Transaction = tx
      tkvl(client, statePtr).delete(Key(uuid2byte(taskId)))
```

- [ ] **Step 4: Run test to verify it passes**

Run: `sbt 'testOnly *SystemTaskServiceStateSuite'`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/compute/systemtask/SystemTaskServiceState.scala \
        src/test/scala/org/aspen_ddp/aspen/compute/systemtask/SystemTaskServiceStateSuite.scala
git commit -m "feat: add SystemTaskServiceState task-tree helpers"
```

---

## Task 5: Test-only CountingSystemTask + TestNetwork helper

Provides a controllable `DurableTask` used by the `createSystemDurableTask`, runner, and service tests, plus a `TestNetwork` helper to drain captured host messages. `CountingSystemTask` bumps its own object's version `targetBumps` times (one bump per scheduled tick), then completes — letting tests observe both progress (revision advancing) and completion. Built before the consumers that reference it.

**Files:**
- Create: `src/test/scala/org/aspen_ddp/aspen/compute/systemtask/CountingSystemTask.scala`
- Modify: `src/test/scala/org/aspen_ddp/aspen/TestNetwork.scala`
- Test: exercised transitively by Tasks 6, 7, 8, 9 (no standalone suite).

- [ ] **Step 1: Create the test task + factory**

Create `src/test/scala/org/aspen_ddp/aspen/compute/systemtask/CountingSystemTask.scala`:

```scala
package org.aspen_ddp.aspen.compute.systemtask

import org.aspen_ddp.aspen.client.{AspenClient, KeyValueObjectState}
import org.aspen_ddp.aspen.common.objects.{Insert, Key, ObjectRevision}
import org.aspen_ddp.aspen.common.transaction.KeyValueUpdate
import org.aspen_ddp.aspen.common.util.{byte2long, long2byte}
import org.aspen_ddp.aspen.compute.{DurableTask, DurableTaskFactory, DurableTaskPointer, TaskExecutor, TaskStopped}

import java.util.UUID
import scala.concurrent.duration.{Duration, MILLISECONDS}
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success}

/** Test-only DurableTask. Performs `targetBumps` version-bump transactions on its own state
 *  object (one per scheduled tick), then completes. A `targetBumps` of 0 completes immediately. */
object CountingSystemTask extends DurableTaskFactory:
  val typeUUID: UUID = UUID.fromString("e7b3a1c9-5d42-4f86-9a0b-3c1e8d7f206b")

  val TargetKey: Key = Key(10)
  val DoneCountKey: Key = Key(11)

  def initialState(targetBumps: Long): Map[Key, Array[Byte]] =
    Map(TargetKey -> long2byte(targetBumps), DoneCountKey -> long2byte(0))

  def createTask(client: AspenClient,
                 pointer: DurableTaskPointer,
                 revision: ObjectRevision,
                 state: Map[Key, KeyValueObjectState.ValueState],
                 taskExecutor: TaskExecutor): DurableTask =
    new CountingSystemTask(pointer, client)


class CountingSystemTask(val taskPointer: DurableTaskPointer,
                         client: AspenClient) extends DurableTask:
  import CountingSystemTask.*

  private given ExecutionContext = client.clientContext
  private val promise = Promise[Option[AnyRef]]()
  def completed: Future[Option[AnyRef]] = promise.future

  step()

  override protected def onStop(): Unit = synchronized:
    if !promise.isCompleted then promise.failure(new TaskStopped)

  private def step(): Unit =
    if isStopped then
      synchronized { if !promise.isCompleted then promise.failure(new TaskStopped) }
    else
      client.read(taskPointer.kvPointer).onComplete:
        case Failure(_) =>
          client.backgroundTaskManager.schedule(Duration(20, MILLISECONDS))(step())
        case Success(kvos) =>
          val target = byte2long(kvos.contents(TargetKey).value.bytes)
          val done = byte2long(kvos.contents(DoneCountKey).value.bytes)
          if done >= target then
            synchronized { if !promise.isCompleted then promise.success(None) }
          else
            val tx = client.newTransaction()
            val reqs = KeyValueUpdate.KeyRevision(DoneCountKey, kvos.contents(DoneCountKey).revision) :: Nil
            val ops = Insert(DoneCountKey, long2byte(done + 1)) :: Nil
            tx.update(taskPointer.kvPointer, None, None, reqs, ops)
            tx.commit().onComplete:
              case Success(_) => step()
              case Failure(_) => client.backgroundTaskManager.schedule(Duration(20, MILLISECONDS))(step())
```

- [ ] **Step 2: Add the drain helper to TestNetwork**

In `src/test/scala/org/aspen_ddp/aspen/TestNetwork.scala`, add these methods inside the `class TestNetwork` body (e.g. right after the rebalancing helpers section, before `var otestThreadId`):

```scala
  /** Remove and return all currently-captured host messages (best-effort delivery is the
   *  caller's job). Mirrors how transfer tests inspect capturedHostMessages, but drains. */
  def takeCapturedHostMessages(): List[HostMessage] = synchronized:
    val msgs = capturedHostMessages
    capturedHostMessages = Nil
    msgs
```

- [ ] **Step 3: Verify it compiles**

Run: `sbt 'Test / compile'`
Expected: SUCCESS (no test executed yet; this task supplies fixtures for later tasks).

- [ ] **Step 4: Commit**

```bash
git add src/test/scala/org/aspen_ddp/aspen/compute/systemtask/CountingSystemTask.scala \
        src/test/scala/org/aspen_ddp/aspen/TestNetwork.scala
git commit -m "test: add CountingSystemTask fixture and captured-message drain helper"
```

---

## Task 6: createSystemDurableTask client API

Adds the client-facing method that allocates a task-state object (typed for `TypeRegistry` reconstruction), enrolls it, and sends a best-effort wake-up. Uses the `CountingSystemTask` fixture from Task 5.

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/client/AspenClient.scala`
- Modify: `src/main/scala/org/aspen_ddp/aspen/client/internal/BaseAspenClient.scala`
- Test: `src/test/scala/org/aspen_ddp/aspen/compute/systemtask/CreateSystemDurableTaskSuite.scala`

- [ ] **Step 1: Write the failing test**

Create `src/test/scala/org/aspen_ddp/aspen/compute/systemtask/CreateSystemDurableTaskSuite.scala`:

```scala
package org.aspen_ddp.aspen.compute.systemtask

import org.aspen_ddp.aspen.IntegrationTestSuite
import org.aspen_ddp.aspen.client.RegisteredTypeFactory
import org.aspen_ddp.aspen.client.tkvl.{KVObjectRootManager, TieredKeyValueList}
import org.aspen_ddp.aspen.common.Radicle
import org.aspen_ddp.aspen.common.network.ServiceMessage
import org.aspen_ddp.aspen.common.objects.Key
import org.aspen_ddp.aspen.compute.ServiceEntry
import org.aspen_ddp.aspen.compute.impl.SimpleTaskExecutor
import org.aspen_ddp.aspen.common.util.byte2uuid

import scala.concurrent.ExecutionContext

class CreateSystemDurableTaskSuite extends IntegrationTestSuite:

  // Register the test task factory so the type-registry sanity check passes.
  override def userTypeFactories: List[RegisteredTypeFactory] = List(CountingSystemTask)

  atest("createSystemDurableTask allocates a typed task object, enrolls it, and wakes the service"):
    given ExecutionContext = executionContext
    val servicesTkvl = TieredKeyValueList(client,
      KVObjectRootManager(client, Radicle.ServicesTreeKey, Radicle.pointer))
    for
      _ <- client.createSystemDurableTask(CountingSystemTask.typeUUID, CountingSystemTask.initialState(0))
      stateVs <- servicesTkvl.get(Key(SystemTaskExecutorService.ServiceUUID))
      statePtr = ServiceEntry.decode(stateVs.get.value.bytes).statePointer
      enrolled <- SystemTaskServiceState.scan(client, statePtr)
      taskKvos <- client.read(enrolled.head._2)
    yield
      enrolled.size shouldBe 1
      // Task object carries its type UUID for reconstruction.
      byte2uuid(taskKvos.contents(SimpleTaskExecutor.TaskTypeKey).value.bytes) shouldBe CountingSystemTask.typeUUID
      // A NewSystemTaskAdded wake-up was emitted (captured, not delivered, in the test net).
      net.capturedHostMessages.collect { case s: ServiceMessage => s }
        .exists(_.serviceUUID == SystemTaskExecutorService.ServiceUUID) shouldBe true
```

- [ ] **Step 2: Run test to verify it fails**

Run: `sbt 'testOnly *CreateSystemDurableTaskSuite'`
Expected: FAIL to compile — `createSystemDurableTask` does not exist on `AspenClient`.

- [ ] **Step 3: Add the abstract method to AspenClient**

In `src/main/scala/org/aspen_ddp/aspen/client/AspenClient.scala`, add near the other public task/service methods (e.g. just after `sendServiceMessage`, around line 267). Ensure `java.util.UUID` and `org.aspen_ddp.aspen.common.objects.Key` are imported (they are used elsewhere in the file):

```scala
  /** Create and enroll a system-level DurableTask, executed by the SystemTaskExecutorService
   *  on some host. Retries until the task object is created and enrolled. The returned Future
   *  completes at enrollment; there is no task-completion notification.
   *
   *  `taskTypeUUID` must resolve to a DurableTaskFactory in the type registry. */
  def createSystemDurableTask(taskTypeUUID: UUID,
                              initialState: Map[Key, Array[Byte]]): Future[Unit]
```

- [ ] **Step 4: Implement in BaseAspenClient**

In `src/main/scala/org/aspen_ddp/aspen/client/internal/BaseAspenClient.scala`, add the implementation (place it among the other overridden methods). Add any missing imports at the top: `java.util.UUID`, `org.aspen_ddp.aspen.common.Radicle`, `org.aspen_ddp.aspen.client.internal.allocation.PoolObjectAllocator`, `org.aspen_ddp.aspen.common.objects.{Key, Value}`, `org.aspen_ddp.aspen.compute.DurableTaskFactory`, `org.aspen_ddp.aspen.compute.impl.SimpleTaskExecutor`, `org.aspen_ddp.aspen.compute.systemtask.{SystemTaskExecutorService, SystemTaskServiceState, NewSystemTaskAdded, SystemTaskMessage}`, `org.aspen_ddp.aspen.common.util.uuid2byte`, `org.aspen_ddp.aspen.client.tkvl.{KVObjectRootManager, TieredKeyValueList}`, `org.aspen_ddp.aspen.compute.ServiceEntry`, `org.aspen_ddp.aspen.client.{StopRetrying, Transaction}`:

```scala
  override def createSystemDurableTask(taskTypeUUID: UUID,
                                       initialState: Map[Key, Array[Byte]]): Future[Unit] =
    given ExecutionContext = clientContext

    // Sanity check: the type must resolve to a DurableTaskFactory.
    typeRegistry.getType[DurableTaskFactory](taskTypeUUID) match
      case None =>
        Future.failed(new StopRetrying(
          s"createSystemDurableTask: no DurableTaskFactory registered for $taskTypeUUID"))
      case Some(_) =>
        val taskId = UUID.randomUUID()

        def serviceStatePtr(): Future[KeyValueObjectPointer] =
          val servicesTkvl = TieredKeyValueList(this,
            KVObjectRootManager(this, Radicle.ServicesTreeKey, radicle))
          servicesTkvl.get(Key(SystemTaskExecutorService.ServiceUUID)).map:
            case Some(vs) => ServiceEntry.decode(vs.value.bytes).statePointer
            case None => throw new IllegalStateException("SystemTaskExecutorService is not registered")

        val taskContent: Map[Key, Value] =
          (initialState + (SimpleTaskExecutor.TaskTypeKey -> uuid2byte(taskTypeUUID)))
            .map((k, v) => k -> Value(v))

        for
          statePtr <- serviceStatePtr()
          pool <- getStoragePool(Radicle.poolId)
          allocator = new PoolObjectAllocator(this, pool)
          taskStatePtr <- transactUntilSuccessful: tx =>
                            given Transaction = tx
                            allocator.allocateKeyValueObject(taskContent)
          _ <- SystemTaskServiceState.enroll(this, statePtr, taskId, taskStatePtr)
        yield
          sendServiceMessage(SystemTaskExecutorService.ServiceUUID,
            SystemTaskMessage.encode(NewSystemTaskAdded(taskId)))
          ()
```

Note on `KeyValueObjectPointer`: it is already imported in `BaseAspenClient`; if not, add `org.aspen_ddp.aspen.common.objects.KeyValueObjectPointer`.

- [ ] **Step 5: Run test to verify it passes**

Run: `sbt 'testOnly *CreateSystemDurableTaskSuite'`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/client/AspenClient.scala \
        src/main/scala/org/aspen_ddp/aspen/client/internal/BaseAspenClient.scala \
        src/test/scala/org/aspen_ddp/aspen/compute/systemtask/CreateSystemDurableTaskSuite.scala
git commit -m "feat: add AspenClient.createSystemDurableTask"
```

---

## Task 7: SystemTaskRunner — transient execution + heartbeat

The per-host component that runs assigned tasks transiently and heartbeats them. It reconstructs the `DurableTask` from the durable state object via the `TypeRegistry`, drives it to completion, then removes it from the service registry and sends a best-effort completion wake-up. A single `heartbeat()` call bumps the version of all currently-running task objects.

**Files:**
- Create: `src/main/scala/org/aspen_ddp/aspen/compute/systemtask/SystemTaskRunner.scala`
- Test: `src/test/scala/org/aspen_ddp/aspen/compute/systemtask/SystemTaskRunnerSuite.scala`

- [ ] **Step 1: Write the failing test**

Create `src/test/scala/org/aspen_ddp/aspen/compute/systemtask/SystemTaskRunnerSuite.scala`:

```scala
package org.aspen_ddp.aspen.compute.systemtask

import org.aspen_ddp.aspen.IntegrationTestSuite
import org.aspen_ddp.aspen.client.RegisteredTypeFactory
import org.aspen_ddp.aspen.client.tkvl.{KVObjectRootManager, TieredKeyValueList}
import org.aspen_ddp.aspen.common.Radicle
import org.aspen_ddp.aspen.common.metadata.HostId
import org.aspen_ddp.aspen.common.network.ExecuteSystemTask
import org.aspen_ddp.aspen.common.objects.Key
import org.aspen_ddp.aspen.compute.ServiceEntry

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.concurrent.duration.{Duration, MILLISECONDS}

class SystemTaskRunnerSuite extends IntegrationTestSuite:

  override def userTypeFactories: List[RegisteredTypeFactory] = List(CountingSystemTask)

  private def serviceStatePtr(): Future[org.aspen_ddp.aspen.common.objects.KeyValueObjectPointer] =
    given ExecutionContext = executionContext
    val servicesTkvl = TieredKeyValueList(client,
      KVObjectRootManager(client, Radicle.ServicesTreeKey, Radicle.pointer))
    servicesTkvl.get(Key(SystemTaskExecutorService.ServiceUUID)).map: vs =>
      ServiceEntry.decode(vs.get.value.bytes).statePointer

  private def pollUntil(cond: () => Future[Boolean], timeout: Duration): Future[Unit] =
    given ExecutionContext = executionContext
    val deadline = System.nanoTime() + timeout.toNanos
    def loop(): Future[Unit] =
      cond().flatMap: ok =>
        if ok then Future.unit
        else if System.nanoTime() > deadline then Future.failed(new AssertionError("timed out"))
        else
          val p = Promise[Unit]()
          client.backgroundTaskManager.schedule(Duration(50, MILLISECONDS))(p.success(()))
          p.future.flatMap(_ => loop())
    loop()

  atest("runner executes an assigned task to completion and removes it from the registry"):
    given ExecutionContext = executionContext
    for
      statePtr <- serviceStatePtr()
      // Enroll a task that completes after 2 bumps.
      _ <- client.createSystemDurableTask(CountingSystemTask.typeUUID, CountingSystemTask.initialState(2))
      enrolled <- SystemTaskServiceState.scan(client, statePtr)
      (taskId, taskStatePtr) = enrolled.head
      runner = new SystemTaskRunner(client, statePtr, HostId.BootstrapHostId)
      _ = runner.receive(ExecuteSystemTask(HostId.BootstrapHostId, client.clientId, taskId, taskStatePtr))
      _ <- pollUntil(() => SystemTaskServiceState.scan(client, statePtr).map(_.isEmpty),
                     Duration(20000, MILLISECONDS))
      finalScan <- SystemTaskServiceState.scan(client, statePtr)
    yield
      runner.shutdown()
      finalScan shouldBe empty
```

- [ ] **Step 2: Run test to verify it fails**

Run: `sbt 'testOnly *SystemTaskRunnerSuite'`
Expected: FAIL to compile — `SystemTaskRunner` does not exist.

- [ ] **Step 3: Implement the runner**

Create `src/main/scala/org/aspen_ddp/aspen/compute/systemtask/SystemTaskRunner.scala`:

```scala
package org.aspen_ddp.aspen.compute.systemtask

import org.aspen_ddp.aspen.client.{AspenClient, Transaction}
import org.aspen_ddp.aspen.common.metadata.HostId
import org.aspen_ddp.aspen.common.network.ExecuteSystemTask
import org.aspen_ddp.aspen.common.objects.KeyValueObjectPointer
import org.aspen_ddp.aspen.common.util.byte2uuid
import org.aspen_ddp.aspen.compute.impl.SimpleTaskExecutor
import org.aspen_ddp.aspen.compute.{DurableTask, DurableTaskFactory, DurableTaskPointer, TaskExecutor}
import scribe.Logging

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

/** Per-host transient executor for system tasks. Runs assigned tasks reconstructed from their
 *  durable state object; does NOT persist them to any per-host executor. If this host dies, the
 *  service detects the stalled heartbeat and reassigns.
 *
 *  `taskExecutor` is supplied to reconstructed tasks that spawn sub-tasks (e.g. cascading
 *  UpdateAllocationGroupUsageTask); tasks that don't spawn sub-tasks ignore it. */
class SystemTaskRunner(val client: AspenClient,
                       val serviceStatePtr: KeyValueObjectPointer,
                       val hostId: HostId,
                       taskExecutor: TaskExecutor = SystemTaskRunner.NoOpTaskExecutor) extends Logging:

  private given ExecutionContext = client.clientContext

  // taskId -> (task, state pointer)
  private var running: Map[UUID, (DurableTask, KeyValueObjectPointer)] = Map.empty
  @volatile private var stopped = false

  def shutdown(): Unit = synchronized:
    stopped = true
    running.values.foreach((task, _) => task.stop())
    running = Map.empty

  /** Handle an ExecuteSystemTask message: start the task if not already running it. */
  def receive(msg: ExecuteSystemTask): Unit =
    if synchronized(!stopped && !running.contains(msg.taskId)) then
      start(msg.taskId, msg.taskStatePointer)

  private def start(taskId: UUID, taskStatePtr: KeyValueObjectPointer): Unit =
    client.read(taskStatePtr).foreach: kvos =>
      kvos.contents.get(SimpleTaskExecutor.TaskTypeKey) match
        case None =>
          logger.warn(s"System task $taskId has no TaskTypeKey; ignoring")
        case Some(vs) =>
          val taskType = byte2uuid(vs.value.bytes)
          client.typeRegistry.getType[DurableTaskFactory](taskType) match
            case None =>
              logger.warn(s"No DurableTaskFactory for type $taskType (task $taskId); ignoring")
            case Some(factory) =>
              val ptr = DurableTaskPointer(taskStatePtr)
              val task = factory.createTask(client, ptr, kvos.revision, kvos.contents, taskExecutor)
              val added = synchronized:
                if stopped || running.contains(taskId) then false
                else { running += taskId -> (task, taskStatePtr); true }
              if !added then
                task.stop()
              else
                // Immediate claim/liveness bump.
                bumpOne(taskStatePtr)
                task.completed.onComplete: _ =>
                  onTaskComplete(taskId, taskStatePtr)

  private def onTaskComplete(taskId: UUID, taskStatePtr: KeyValueObjectPointer): Unit =
    synchronized { running -= taskId }
    // Remove from the registry and drop the task-state object, then wake the service.
    val f = client.transactUntilSuccessful: tx =>
      given Transaction = tx
      for
        taskKvos <- client.read(taskStatePtr)
        _ <- SystemTaskServiceState.removeInTx(client, serviceStatePtr, taskId)
      yield
        tx.setRefcount(taskStatePtr, taskKvos.refcount, taskKvos.refcount.decrement())
    f.foreach: _ =>
      client.sendServiceMessage(SystemTaskExecutorService.ServiceUUID,
        SystemTaskMessage.encode(SystemTaskComplete(taskId)))

  /** Bump the version of every running task's state object (the heartbeat). */
  def heartbeat(): Future[Unit] =
    val ptrs = synchronized(running.values.map(_._2).toList)
    Future.traverse(ptrs)(bumpOne).map(_ => ())

  private def bumpOne(taskStatePtr: KeyValueObjectPointer): Future[Unit] =
    client.transactUntilSuccessful: tx =>
      given Transaction = tx
      client.read(taskStatePtr).map: kvos =>
        tx.bumpVersion(taskStatePtr, kvos.revision)


object SystemTaskRunner:
  /** For tasks that never spawn sub-tasks. Fails loudly if a task tries to use it. */
  object NoOpTaskExecutor extends TaskExecutor:
    def prepareTask(taskType: DurableTaskFactory,
                    initialState: List[(org.aspen_ddp.aspen.common.objects.Key, Array[Byte])])
                   (using tx: Transaction): Future[Future[Option[AnyRef]]] =
      Future.failed(new UnsupportedOperationException(
        "SystemTaskRunner.NoOpTaskExecutor cannot prepare sub-tasks"))
```

- [ ] **Step 4: Add the in-transaction remove helper to SystemTaskServiceState**

In `src/main/scala/org/aspen_ddp/aspen/compute/systemtask/SystemTaskServiceState.scala`, add this method to the `object SystemTaskServiceState` (the runner needs a delete that participates in an existing transaction):

```scala
  /** Delete a task's registry entry within an existing transaction. */
  def removeInTx(client: AspenClient,
                 statePtr: KeyValueObjectPointer,
                 taskId: UUID)(using tx: Transaction): Future[Unit] =
    tkvl(client, statePtr).delete(Key(uuid2byte(taskId)))
```

- [ ] **Step 5: Run test to verify it passes**

Run: `sbt 'testOnly *SystemTaskRunnerSuite'`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/compute/systemtask/SystemTaskRunner.scala \
        src/main/scala/org/aspen_ddp/aspen/compute/systemtask/SystemTaskServiceState.scala \
        src/test/scala/org/aspen_ddp/aspen/compute/systemtask/SystemTaskRunnerSuite.scala
git commit -m "feat: add SystemTaskRunner transient executor + heartbeat"
```

---

## Task 8: SystemTaskExecutorService — dispatch, monitor, reassign

Adds the `DurableServiceFactory` to the companion and the service class. The service discovers enrolled tasks (slow poll + `NewSystemTaskAdded` wake-up), dispatches each to a random host via `ExecuteSystemTask`, and runs a faster liveness loop that reassigns a task whose state-object revision has not advanced within the stall timeout. Registered in `server/TypeFactories.scala`.

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/compute/systemtask/SystemTaskExecutorService.scala`
- Modify: `src/main/scala/org/aspen_ddp/aspen/server/TypeFactories.scala`
- Test: `src/test/scala/org/aspen_ddp/aspen/compute/systemtask/SystemTaskExecutorServiceSuite.scala`

- [ ] **Step 1: Write the failing tests**

Create `src/test/scala/org/aspen_ddp/aspen/compute/systemtask/SystemTaskExecutorServiceSuite.scala`:

```scala
package org.aspen_ddp.aspen.compute.systemtask

import org.aspen_ddp.aspen.IntegrationTestSuite
import org.aspen_ddp.aspen.client.RegisteredTypeFactory
import org.aspen_ddp.aspen.client.tkvl.{KVObjectRootManager, TieredKeyValueList}
import org.aspen_ddp.aspen.common.Radicle
import org.aspen_ddp.aspen.common.metadata.HostId
import org.aspen_ddp.aspen.common.network.ExecuteSystemTask
import org.aspen_ddp.aspen.common.objects.Key
import org.aspen_ddp.aspen.compute.ServiceEntry
import org.aspen_ddp.aspen.compute.impl.SimpleDurableServiceExecutor

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.concurrent.duration.{Duration, MILLISECONDS}

class SystemTaskExecutorServiceSuite extends IntegrationTestSuite:

  override def userTypeFactories: List[RegisteredTypeFactory] =
    List(SystemTaskExecutorService, CountingSystemTask)

  private def makeExecutor(): SimpleDurableServiceExecutor =
    new SimpleDurableServiceExecutor(
      client, HostId.BootstrapHostId, client.backgroundTaskManager,
      leaseDuration        = Duration(2000, MILLISECONDS),
      renewalInterval      = Duration(200, MILLISECONDS),
      minScanInterval      = Duration(40, MILLISECONDS),
      maxScanInterval      = Duration(80, MILLISECONDS),
      claimDelayPerService = Duration(0, MILLISECONDS))

  private def serviceStatePtr(): Future[org.aspen_ddp.aspen.common.objects.KeyValueObjectPointer] =
    given ExecutionContext = executionContext
    val servicesTkvl = TieredKeyValueList(client,
      KVObjectRootManager(client, Radicle.ServicesTreeKey, Radicle.pointer))
    servicesTkvl.get(Key(SystemTaskExecutorService.ServiceUUID)).map: vs =>
      ServiceEntry.decode(vs.get.value.bytes).statePointer

  private def sleep(d: Duration): Future[Unit] =
    val p = Promise[Unit]()
    client.backgroundTaskManager.schedule(d)(p.success(()))
    p.future

  atest("service dispatches an enrolled task and a runner completes it"):
    given ExecutionContext = executionContext
    SystemTaskExecutorService.reconcilePeriod = Duration(100, MILLISECONDS)
    SystemTaskExecutorService.monitorPeriod   = Duration(100, MILLISECONDS)
    SystemTaskExecutorService.firstCheckDelay = Duration(200, MILLISECONDS)
    SystemTaskExecutorService.stallTimeout    = Duration(1000, MILLISECONDS)
    val exec = makeExecutor()
    for
      statePtr <- serviceStatePtr()
      runner = new SystemTaskRunner(client, statePtr, HostId.BootstrapHostId)
      _ <- client.createSystemDurableTask(CountingSystemTask.typeUUID, CountingSystemTask.initialState(2))
      // Drive: repeatedly deliver captured ExecuteSystemTask messages to the runner until the
      // registry empties (the service dispatched, the runner executed, cleanup removed it).
      _ <- driveUntilEmpty(statePtr, runner, Duration(20000, MILLISECONDS))
      finalScan <- SystemTaskServiceState.scan(client, statePtr)
    yield
      exec.shutdown()
      runner.shutdown()
      resetTuning()
      finalScan shouldBe empty

  atest("service reassigns a task whose heartbeat stalls (no runner)"):
    given ExecutionContext = executionContext
    SystemTaskExecutorService.reconcilePeriod = Duration(100, MILLISECONDS)
    SystemTaskExecutorService.monitorPeriod   = Duration(100, MILLISECONDS)
    SystemTaskExecutorService.firstCheckDelay = Duration(150, MILLISECONDS)
    SystemTaskExecutorService.stallTimeout    = Duration(300, MILLISECONDS)
    val exec = makeExecutor()
    for
      statePtr <- serviceStatePtr()
      _ <- client.createSystemDurableTask(CountingSystemTask.typeUUID, CountingSystemTask.initialState(5))
      // Never deliver to a runner -> the task object never changes -> service must re-dispatch.
      dispatches <- countDispatchesFor(Duration(3000, MILLISECONDS))
    yield
      exec.shutdown()
      resetTuning()
      dispatches should be >= 2

  private def resetTuning(): Unit =
    SystemTaskExecutorService.reconcilePeriod = SystemTaskExecutorService.DefaultReconcilePeriod
    SystemTaskExecutorService.monitorPeriod   = SystemTaskExecutorService.DefaultMonitorPeriod
    SystemTaskExecutorService.firstCheckDelay = SystemTaskExecutorService.DefaultFirstCheckDelay
    SystemTaskExecutorService.stallTimeout    = SystemTaskExecutorService.DefaultStallTimeout

  /** Poll: deliver any captured ExecuteSystemTask messages to the runner, then check emptiness. */
  private def driveUntilEmpty(statePtr: org.aspen_ddp.aspen.common.objects.KeyValueObjectPointer,
                              runner: SystemTaskRunner,
                              timeout: Duration): Future[Unit] =
    given ExecutionContext = executionContext
    val deadline = System.nanoTime() + timeout.toNanos
    def loop(): Future[Unit] =
      net.takeCapturedHostMessages().foreach:
        case m: ExecuteSystemTask => runner.receive(m)
        case _ => ()
      SystemTaskServiceState.scan(client, statePtr).flatMap: active =>
        if active.isEmpty then Future.unit
        else if System.nanoTime() > deadline then Future.failed(new AssertionError(s"not empty: $active"))
        else sleep(Duration(50, MILLISECONDS)).flatMap(_ => loop())
    loop()

  /** Count distinct ExecuteSystemTask dispatch messages captured over `window`. */
  private def countDispatchesFor(window: Duration): Future[Int] =
    given ExecutionContext = executionContext
    val deadline = System.nanoTime() + window.toNanos
    def loop(count: Int): Future[Int] =
      val n = count + net.takeCapturedHostMessages().count(_.isInstanceOf[ExecuteSystemTask])
      if System.nanoTime() > deadline then Future.successful(n)
      else sleep(Duration(50, MILLISECONDS)).flatMap(_ => loop(n))
    loop(0)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `sbt 'testOnly *SystemTaskExecutorServiceSuite'`
Expected: FAIL to compile — `SystemTaskExecutorService` is not a `DurableServiceFactory` and has no tuning fields/service class.

- [ ] **Step 3: Extend the companion with the factory + tuning; add the service class**

Replace the entire contents of `src/main/scala/org/aspen_ddp/aspen/compute/systemtask/SystemTaskExecutorService.scala` with:

```scala
package org.aspen_ddp.aspen.compute.systemtask

import org.aspen_ddp.aspen.client.{AspenClient, KeyValueObjectState}
import org.aspen_ddp.aspen.common.metadata.HostId
import org.aspen_ddp.aspen.common.network.ExecuteSystemTask
import org.aspen_ddp.aspen.common.objects.{Key, KeyValueObjectPointer}
import org.aspen_ddp.aspen.common.util.BackgroundTaskManager.{NoTask, ScheduledTask}
import org.aspen_ddp.aspen.common.network.ServiceMessage
import org.aspen_ddp.aspen.compute.{DurableService, DurableServiceFactory}
import scribe.Logging

import java.util.UUID
import java.util.concurrent.ThreadLocalRandom
import scala.concurrent.duration.{Duration, MINUTES, SECONDS}
import scala.concurrent.{ExecutionContext, Future}

object SystemTaskExecutorService extends DurableServiceFactory with Logging:

  val ServiceTypeUUID: UUID = UUID.fromString("a1e5c9d4-3b7f-4a21-9c8e-2d6f0b13e7a4")
  val ServiceUUID: UUID     = UUID.fromString("c4d8f2b6-1a09-4e73-8b5d-7f3c9e0a24d1")

  val typeUUID: UUID = ServiceTypeUUID

  /** Key in the service state object whose value is the encoded Root of the task TKVL. */
  val TaskTreeKey: Key = Key(0)

  // Tuning (overridable test seams; mirrors RebalancingDurableService.pollPeriod).
  val DefaultReconcilePeriod: Duration = Duration(2, MINUTES)
  val DefaultMonitorPeriod: Duration   = Duration(15, SECONDS)
  val DefaultFirstCheckDelay: Duration = Duration(30, SECONDS)
  val DefaultStallTimeout: Duration    = Duration(45, SECONDS)
  val DefaultHostCacheTtl: Duration    = Duration(30, MINUTES)

  @volatile var reconcilePeriod: Duration = DefaultReconcilePeriod
  @volatile var monitorPeriod: Duration   = DefaultMonitorPeriod
  @volatile var firstCheckDelay: Duration = DefaultFirstCheckDelay
  @volatile var stallTimeout: Duration    = DefaultStallTimeout
  @volatile var hostCacheTtl: Duration    = DefaultHostCacheTtl

  override def createService(client: AspenClient,
                             statePointer: KeyValueObjectPointer,
                             state: KeyValueObjectState): DurableService =
    new SystemTaskExecutorService(client, statePointer)


/** Per-task liveness tracking held by the running service instance. */
private final case class TaskTrack(assignedHost: HostId,
                                   lastRevision: org.aspen_ddp.aspen.common.objects.ObjectRevision,
                                   lastChangeNanos: Long,
                                   dispatchedNanos: Long)

class SystemTaskExecutorService(val client: AspenClient,
                                val statePointer: KeyValueObjectPointer) extends DurableService with Logging:
  import SystemTaskExecutorService.*

  private given ExecutionContext = client.clientContext

  private var tracking: Map[UUID, TaskTrack] = Map.empty
  private var hostCache: List[HostId] = Nil
  private var hostCacheStampNanos: Long = 0L
  private var reconcileTask: ScheduledTask = NoTask
  private var monitorTask: ScheduledTask = NoTask
  @volatile private var stopped = false

  reconcile()
  reconcileTask = client.backgroundTaskManager.scheduleNonConcurrentPollingTask(reconcilePeriod):
    reconcile()
  monitorTask = client.backgroundTaskManager.scheduleNonConcurrentPollingTask(monitorPeriod):
    monitor()

  override def shutdown(): Unit = synchronized:
    stopped = true
    reconcileTask.cancel()
    monitorTask.cancel()
    tracking = Map.empty

  override def receiveMessage(msg: ServiceMessage): Unit =
    SystemTaskMessage.decode(msg.encodedContent) match
      case NewSystemTaskAdded(_)  => reconcile()
      case SystemTaskComplete(id) => synchronized { tracking -= id }

  private def refreshHostsIfNeeded(): Future[List[HostId]] =
    val now = System.nanoTime()
    val (cached, stamp) = synchronized((hostCache, hostCacheStampNanos))
    if cached.nonEmpty && (now - stamp) < hostCacheTtl.toNanos then
      Future.successful(cached)
    else
      client.listHosts().map: hosts =>
        val ids = hosts.map(_._2)
        synchronized:
          hostCache = ids
          hostCacheStampNanos = System.nanoTime()
        ids

  private def pickHost(): Future[Option[HostId]] =
    refreshHostsIfNeeded().map: all =>
      val offline = client.offlineHosts()
      val candidates = all.filterNot(offline.contains)
      if candidates.isEmpty then None
      else Some(candidates(ThreadLocalRandom.current().nextInt(candidates.size)))

  private def dispatch(taskId: UUID, taskStatePtr: KeyValueObjectPointer): Future[Unit] =
    pickHost().flatMap:
      case None =>
        logger.warn(s"No online host to run system task $taskId")
        Future.unit
      case Some(host) =>
        client.read(taskStatePtr).map: kvos =>
          client.sendHostMessage(ExecuteSystemTask(host, client.clientId, taskId, taskStatePtr))
          val now = System.nanoTime()
          synchronized:
            if !stopped then
              tracking += taskId -> TaskTrack(host, kvos.revision, now, now)
        .recover:
          case err => logger.warn(s"Dispatch of system task $taskId failed: $err")

  /** Discover enrolled tasks and dispatch any not yet tracked; drop tracking for vanished tasks. */
  private def reconcile(): Future[Unit] =
    if stopped then Future.unit
    else
      SystemTaskServiceState.scan(client, statePointer).flatMap: enrolled =>
        val enrolledIds = enrolled.map(_._1).toSet
        synchronized { tracking = tracking.filter((id, _) => enrolledIds.contains(id)) }
        val toDispatch = enrolled.filter((id, _) => synchronized(!tracking.contains(id)))
        Future.traverse(toDispatch)((id, ptr) => dispatch(id, ptr)).map(_ => ())
      .recover:
        case err => logger.warn(s"SystemTaskExecutorService reconcile failed: $err")

  /** Liveness check: reassign any tracked task whose state-object revision has stalled. */
  private def monitor(): Future[Unit] =
    if stopped then Future.unit
    else
      val snapshot = synchronized(tracking.toList)
      SystemTaskServiceState.scan(client, statePointer).flatMap: enrolled =>
        val enrolledMap = enrolled.toMap
        Future.traverse(snapshot): (taskId, track) =>
          enrolledMap.get(taskId) match
            case None =>
              // Completed (registry entry removed by the runner): drop tracking.
              synchronized { tracking -= taskId }
              Future.unit
            case Some(taskStatePtr) =>
              client.read(taskStatePtr).flatMap: kvos =>
                val now = System.nanoTime()
                if kvos.revision != track.lastRevision then
                  synchronized:
                    tracking.get(taskId).foreach: t =>
                      tracking += taskId -> t.copy(lastRevision = kvos.revision, lastChangeNanos = now)
                  Future.unit
                else
                  val stalled = (now - track.lastChangeNanos) > stallTimeout.toNanos
                  val pastFirstCheck = (now - track.dispatchedNanos) > firstCheckDelay.toNanos
                  if stalled && pastFirstCheck then
                    logger.info(s"System task $taskId stalled; reassigning")
                    dispatch(taskId, taskStatePtr)
                  else
                    Future.unit
              .recover:
                case err => logger.warn(s"Monitor read for system task $taskId failed: $err")
        .map(_ => ())
      .recover:
        case err => logger.warn(s"SystemTaskExecutorService monitor failed: $err")
```

- [ ] **Step 4: Register the service factory**

In `src/main/scala/org/aspen_ddp/aspen/server/TypeFactories.scala`, add the import and list entry:

```scala
package org.aspen_ddp.aspen.server

import org.aspen_ddp.aspen.client.RegisteredTypeFactory
import org.aspen_ddp.aspen.common.rebalancing.RebalancingDurableService
import org.aspen_ddp.aspen.compute.systemtask.SystemTaskExecutorService

object TypeFactories:

  val factories: List[RegisteredTypeFactory] = List(
    RebalancingDurableService,
    SystemTaskExecutorService
  )
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `sbt 'testOnly *SystemTaskExecutorServiceSuite'`
Expected: PASS (both `dispatches ... completes it` and `reassigns ... stalls`).

- [ ] **Step 6: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/compute/systemtask/SystemTaskExecutorService.scala \
        src/main/scala/org/aspen_ddp/aspen/server/TypeFactories.scala \
        src/test/scala/org/aspen_ddp/aspen/compute/systemtask/SystemTaskExecutorServiceSuite.scala
git commit -m "feat: add SystemTaskExecutorService dispatch/monitor/reassign"
```

---

## Task 9: StoreManager wiring

Wires production hosts to run assigned tasks: the `StoreManager` constructs a `SystemTaskRunner` once its task executor is ready, dispatches incoming `ExecuteSystemTask` messages to it, and drives the 15s heartbeat from its periodic heartbeat event.

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/server/StoreManager.scala`
- Test: `src/test/scala/org/aspen_ddp/aspen/compute/systemtask/StoreManagerSystemTaskSuite.scala`

- [ ] **Step 1: Write the failing test**

Create `src/test/scala/org/aspen_ddp/aspen/compute/systemtask/StoreManagerSystemTaskSuite.scala`:

```scala
package org.aspen_ddp.aspen.compute.systemtask

import org.aspen_ddp.aspen.IntegrationTestSuite
import org.aspen_ddp.aspen.client.RegisteredTypeFactory
import org.aspen_ddp.aspen.client.tkvl.{KVObjectRootManager, TieredKeyValueList}
import org.aspen_ddp.aspen.common.Radicle
import org.aspen_ddp.aspen.common.metadata.HostId
import org.aspen_ddp.aspen.common.network.ExecuteSystemTask
import org.aspen_ddp.aspen.common.objects.Key
import org.aspen_ddp.aspen.compute.ServiceEntry

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.concurrent.duration.{Duration, MILLISECONDS}

class StoreManagerSystemTaskSuite extends IntegrationTestSuite:

  override def userTypeFactories: List[RegisteredTypeFactory] = List(CountingSystemTask)

  private def serviceStatePtr(): Future[org.aspen_ddp.aspen.common.objects.KeyValueObjectPointer] =
    given ExecutionContext = executionContext
    val servicesTkvl = TieredKeyValueList(client,
      KVObjectRootManager(client, Radicle.ServicesTreeKey, Radicle.pointer))
    servicesTkvl.get(Key(SystemTaskExecutorService.ServiceUUID)).map: vs =>
      ServiceEntry.decode(vs.get.value.bytes).statePointer

  private def sleep(d: Duration): Future[Unit] =
    val p = Promise[Unit]()
    client.backgroundTaskManager.schedule(d)(p.success(()))
    p.future

  atest("StoreManager runs a task delivered via ExecuteSystemTask"):
    given ExecutionContext = executionContext
    for
      statePtr <- serviceStatePtr()
      _ <- client.createSystemDurableTask(CountingSystemTask.typeUUID, CountingSystemTask.initialState(1))
      enrolled <- SystemTaskServiceState.scan(client, statePtr)
      (taskId, taskStatePtr) = enrolled.head
      // Deliver the execute message straight to the StoreManager, as the network would.
      _ = net.smgr.receiveHostMessage(
            ExecuteSystemTask(HostId.BootstrapHostId, client.clientId, taskId, taskStatePtr))
      _ = net.handleEvents()
      _ <- driveEmpty(statePtr, Duration(20000, MILLISECONDS))
      finalScan <- SystemTaskServiceState.scan(client, statePtr)
    yield
      finalScan shouldBe empty

  private def driveEmpty(statePtr: org.aspen_ddp.aspen.common.objects.KeyValueObjectPointer,
                         timeout: Duration): Future[Unit] =
    given ExecutionContext = executionContext
    val deadline = System.nanoTime() + timeout.toNanos
    def loop(): Future[Unit] =
      net.handleEvents()
      SystemTaskServiceState.scan(client, statePtr).flatMap: active =>
        if active.isEmpty then Future.unit
        else if System.nanoTime() > deadline then Future.failed(new AssertionError(s"not empty: $active"))
        else sleep(Duration(50, MILLISECONDS)).flatMap(_ => loop())
    loop()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `sbt 'testOnly *StoreManagerSystemTaskSuite'`
Expected: FAIL — `ExecuteSystemTask` reaches the `HostMsg` match but hits the exhaustive-match/no-op path (task is never run; registry never empties → timeout).

- [ ] **Step 3: Wire the runner into StoreManager**

In `src/main/scala/org/aspen_ddp/aspen/server/StoreManager.scala`:

Add imports near the other compute imports (line ~24-25):

```scala
import org.aspen_ddp.aspen.compute.systemtask.{SystemTaskExecutorService, SystemTaskRunner}
import org.aspen_ddp.aspen.client.tkvl.{KVObjectRootManager, TieredKeyValueList}
import org.aspen_ddp.aspen.compute.ServiceEntry
import org.aspen_ddp.aspen.common.network.ExecuteSystemTask
```

Add a runner promise field near `serviceExecutorPromise` (line ~118):

```scala
  private val systemTaskRunnerPromise: Promise[SystemTaskRunner] = Promise()
```

In `initializeTaskExecutor()`, both branches create the `SimpleDurableServiceExecutor`. In each branch, immediately after `serviceExecutorPromise.success(serviceExec)`, add creation of the runner (it needs the service state object pointer, read from the services tree). Add this helper method to the class (e.g. right after `initializeTaskExecutor`):

```scala
  private def initializeSystemTaskRunner(executor: TaskExecutor): Unit =
    val servicesTkvl = TieredKeyValueList(client,
      KVObjectRootManager(client, Radicle.ServicesTreeKey, client.radicle))
    servicesTkvl.get(Key(SystemTaskExecutorService.ServiceUUID)).foreach:
      case Some(vs) =>
        val statePtr = ServiceEntry.decode(vs.value.bytes).statePointer
        val runner = new SystemTaskRunner(client, statePtr, hostId, executor)
        synchronized:
          if !systemTaskRunnerPromise.isCompleted then
            systemTaskRunnerPromise.success(runner)
      case None =>
        logger.debug("SystemTaskExecutorService not registered; system task runner not started")
```

Then in `initializeTaskExecutor`, in **both** the `Some(vs)` and `None` branches, after the line `val serviceExec = new SimpleDurableServiceExecutor(client, hostId, backgroundTasks)` and `serviceExecutorPromise.success(serviceExec)`, add:

```scala
                    initializeSystemTaskRunner(executor)
```

(Match the indentation of the surrounding `synchronized` block in each branch.)

Add the dispatch case in the `HostMsg(msg)` match (line ~782-787), after the `ServiceMessage` case:

```scala
        case m: ExecuteSystemTask =>
          systemTaskRunnerPromise.future.foreach(_.receive(m))
```

Drive the heartbeat from the existing `HeartbeatEvent()` handler (line ~814-816). Replace:

```scala
      case HeartbeatEvent() =>
        //logger.trace("Main loop got heartbeat event")
        stores.valuesIterator.foreach(_.heartbeat())
```

with:

```scala
      case HeartbeatEvent() =>
        //logger.trace("Main loop got heartbeat event")
        stores.valuesIterator.foreach(_.heartbeat())
        if systemTaskRunnerPromise.isCompleted then
          systemTaskRunnerPromise.future.foreach(_.heartbeat())
```

Note: the `HeartbeatEvent` fires on `heartbeatPeriod` (host-configured). The 15s cadence from the spec is `heartbeatPeriod` in production configuration; the runner heartbeats every host heartbeat tick. No separate timer is added.

Finally, cancel the runner on shutdown. In `shutdown()` (line ~698-706), after `serviceExecutorPromise.future.foreach(_.shutdown())`, add:

```scala
    systemTaskRunnerPromise.future.foreach(_.shutdown())
```

- [ ] **Step 4: Run test to verify it passes**

Run: `sbt 'testOnly *StoreManagerSystemTaskSuite'`
Expected: PASS.

- [ ] **Step 5: Run the full suite for regressions**

Run: `sbt test`
Expected: PASS (all existing suites plus the new ones).

- [ ] **Step 6: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/server/StoreManager.scala \
        src/test/scala/org/aspen_ddp/aspen/compute/systemtask/StoreManagerSystemTaskSuite.scala
git commit -m "feat: wire SystemTaskRunner into StoreManager (dispatch + heartbeat)"
```

---

## Final verification

- [ ] **Confirm no placeholders remain and everything builds**

Run: `sbt compile 'Test / compile'`
Expected: SUCCESS.

- [ ] **Full test run**

Run: `sbt test`
Expected: PASS.

---

## Notes for the implementer

- **Duplicate execution is safe by design.** Reassignment can transiently run a task on two hosts. `CountingSystemTask` (and any real task) use revision-checked writes, so a losing writer just retries — no corruption. Do not add cross-host locking.
- **Wake-up messages are best-effort.** In `TestNetwork`, `sendHostMessage` only captures; tests deliver messages explicitly. Production delivery is real. Correctness in both cases comes from the service's `reconcile`/`monitor` polling, not message delivery.
- **`SimpleTaskExecutor.TaskTypeKey` is intentionally reused** for the per-task state object so the runner reconstructs tasks with the exact pattern `SimpleTaskExecutor` uses internally.
- **Existing-cluster registration is out of scope.** Bootstrap seeds the service for new clusters. Adding it to a running cluster would use `SimpleDurableServiceExecutor.registerService`; not implemented here.
- If `sbt` reports an exhaustiveness warning on the `HostMsg` match after adding `ExecuteSystemTask`, that confirms the new case is required — add it as specified in Task 9.
```
