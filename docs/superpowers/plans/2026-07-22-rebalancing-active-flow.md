# Rebalancing Active Flow Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement on-demand rebalancing of a single storage device set — build a transfer plan for the set and drive it to completion one store transfer at a time — via a lease-held `RebalancingDurableService` and a `SetRebalanceDurableTask`, plus a `rebalance` CLI command and a pluggable store-transfer mechanism that makes transfers exercisable in the in-memory test framework.

**Architecture:** A singleton `RebalancingDurableService` (managed by the existing `SimpleDurableServiceExecutor`) discovers rebalance jobs from an `ActiveRebalancingTasks` list in its KV state object and directly constructs/drives/stops one `SetRebalanceDurableTask` per set. Each task drains a `pendingTransfers` list stored on the `StorageDeviceSetState`, initiating one `client.transferStore` at a time and polling `StorageDeviceState` for completion (woken early by a `TransferComplete` service message). A `rebalanceStorageDeviceSet` companion method builds the plan (`common.rebalancing.State`/`Plan`) and enrolls the job. Store transfers are refactored behind a `StoreTransferFactory` seam so an in-memory implementation can drive the real `StoreManager` transfer flow in tests without filesystem/`jar` subprocesses.

**Tech Stack:** Scala 3, ScalaPB (protobuf codegen via `sbt compile`), ScalaTest (`AsyncFunSuite` via `IntegrationTestSuite`), the Aspen durable-service/durable-task framework.

**Reference spec:** `docs/superpowers/specs/2026-07-22-rebalancing-active-flow-design.md`

---

## Ground rules for the implementer

- **Scala 3 quiet syntax** (no braces where indentation reads clearly); indented `if/then/else`.
- **`codec.proto` changes require `sbt compile`** to regenerate the `org.aspen_ddp.aspen.codec` package before Scala that references new fields will compile. Always compile after editing the proto.
- **Async tests use `atest(...)`** (not `test`) and `given ExecutionContext = executionContext`.
- **Fixed UUIDs**: two new fixed UUIDs are introduced (service type + service instance). They are hard-coded constants; verify they do not collide with the existing factory UUIDs listed in the spec.
- Run the full suite with `sbt test`; run one suite with `sbt 'testOnly *SuiteName'`.

---

## Fixed identifiers introduced by this plan

| Constant | Value | Where |
|---|---|---|
| `RebalancingDurableService.ServiceTypeUUID` | `d5f1e2a7-0b3c-4e6a-9f21-7c8a4b1e0d33` | new service file |
| `RebalancingDurableService.ServiceUUID` | `b7c04e18-2a9f-4d55-8e13-6f0a1c2d3e4b` | new service file |
| `RebalancingServiceState.ActiveTasksKey` | `Key(Array[Byte](0))` | new state file |
| `SetRebalanceTaskState.SetIdKey` | `Key(Array[Byte](0))` | new task-state file |

---

# Phase 1 — Data model: `pendingTransfers` on `StorageDeviceSetState`

### Task 1: Add `PendingTransfer` proto + `pendingTransfers` field

**Files:**
- Modify: `src/main/protobuf/codec.proto` (`StorageDeviceSetState` message, lines 511-519)

- [ ] **Step 1: Add the `PendingTransfer` message and the repeated field**

In `src/main/protobuf/codec.proto`, replace the existing `StorageDeviceSetState` message:

```proto
message StorageDeviceSetState {
  UUID setId = 1;
  string name = 2;
  int32 level = 3;
  UUID parent = 4;                   // absent = no parent
  repeated UUID memberDevices = 5;
  repeated UUID memberSets = 6;
  repeated UUID assignedPools = 7;
}
```

with:

```proto
message PendingTransfer {
  StoreId storeId = 1;
  StorageDeviceId fromDevice = 2;
  StorageDeviceId toDevice = 3;
}

message StorageDeviceSetState {
  UUID setId = 1;
  string name = 2;
  int32 level = 3;
  UUID parent = 4;                   // absent = no parent
  repeated UUID memberDevices = 5;
  repeated UUID memberSets = 6;
  repeated UUID assignedPools = 7;
  repeated PendingTransfer pendingTransfers = 8;
}
```

- [ ] **Step 2: Regenerate protobuf and confirm it compiles**

Run: `sbt compile`
Expected: BUILD SUCCESS (the `codec.StorageDeviceSetState` now has a `pendingTransfers` accessor and a `codec.PendingTransfer` type is generated). The existing `Codec.encode/decode(StorageDeviceSetState)` still compiles because the new repeated field defaults to empty.

- [ ] **Step 3: Commit**

```bash
git add src/main/protobuf/codec.proto
git commit -m "Add PendingTransfer proto and pendingTransfers field to StorageDeviceSetState"
```

---

### Task 2: Add `pendingTransfers` to the `StorageDeviceSetState` case class + Codec, with a round-trip test

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/common/metadata/StorageDeviceSetState.scala` (case class, lines 33-41)
- Modify: `src/main/scala/org/aspen_ddp/aspen/common/network/Codec.scala` (`encode`/`decode` for `StorageDeviceSetState`, lines 1104-1121)
- Test: `src/test/scala/org/aspen_ddp/aspen/common/metadata/StorageDeviceSetStateCodecSuite.scala` (new)

- [ ] **Step 1: Write the failing test**

Create `src/test/scala/org/aspen_ddp/aspen/common/metadata/StorageDeviceSetStateCodecSuite.scala`:

```scala
package org.aspen_ddp.aspen.common.metadata

import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.util.UUID

class StorageDeviceSetStateCodecSuite extends AnyFunSuite with Matchers:

  private def dev(): StorageDeviceId = StorageDeviceId(UUID.randomUUID())

  test("round-trips with a non-empty pendingTransfers list"):
    val poolId = PoolId(UUID.randomUUID())
    val from = dev()
    val to = dev()
    val transfers = List(
      (StoreId(poolId, 0.toByte), from, to),
      (StoreId(poolId, 1.toByte), to, from)
    )
    val original = StorageDeviceSetState(
      setId = StorageDeviceSetId(UUID.randomUUID()),
      name = "s",
      level = 0,
      parent = None,
      memberDevices = List(from, to),
      memberSets = Nil,
      assignedPools = List(poolId),
      pendingTransfers = transfers
    )
    val decoded = StorageDeviceSetState(original.toBytes)
    decoded.pendingTransfers shouldBe transfers

  test("round-trips with an empty pendingTransfers list"):
    val original = StorageDeviceSetState(
      setId = StorageDeviceSetId(UUID.randomUUID()),
      name = "s",
      level = 0,
      parent = None,
      memberDevices = Nil,
      memberSets = Nil,
      assignedPools = Nil,
      pendingTransfers = Nil
    )
    StorageDeviceSetState(original.toBytes).pendingTransfers shouldBe Nil
```

- [ ] **Step 2: Run it to verify it fails**

Run: `sbt 'testOnly *StorageDeviceSetStateCodecSuite'`
Expected: FAIL to compile — `StorageDeviceSetState` has no `pendingTransfers` parameter.

- [ ] **Step 3: Add the case-class field**

In `src/main/scala/org/aspen_ddp/aspen/common/metadata/StorageDeviceSetState.scala`, change the case class (currently ending at `assignedPools: List[PoolId]`) to add the new field. Also add the needed imports (`StoreId`). New case-class header:

```scala
final case class StorageDeviceSetState(
    setId: StorageDeviceSetId,
    name: String,
    level: Int,
    parent: Option[StorageDeviceSetId],
    memberDevices: List[StorageDeviceId],
    memberSets: List[StorageDeviceSetId],
    assignedPools: List[PoolId],
    pendingTransfers: List[(StoreId, StorageDeviceId, StorageDeviceId)] = Nil
):
```

Add the import near the top (after the existing `common.pool.PoolId` import):

```scala
import org.aspen_ddp.aspen.common.store.StoreId
```

(The default `= Nil` keeps existing constructor call sites — e.g. `Bootstrap.initialize` — compiling unchanged.)

- [ ] **Step 4: Update the Codec encode/decode**

In `src/main/scala/org/aspen_ddp/aspen/common/network/Codec.scala`, replace the `StorageDeviceSetState` encode/decode (lines 1104-1121) with:

```scala
  def encode(o: StorageDeviceSetState): codec.StorageDeviceSetState =
    codec.StorageDeviceSetState(
      setId = Some(encodeUUID(o.setId.uuid)),
      level = o.level,
      name = o.name,
      parent = o.parent.map(p => encodeUUID(p.uuid)),
      memberDevices = o.memberDevices.map(d => encodeUUID(d.uuid)),
      memberSets = o.memberSets.map(s => encodeUUID(s.uuid)),
      assignedPools = o.assignedPools.map(p => encodeUUID(p.uuid)),
      pendingTransfers = o.pendingTransfers.map: (storeId, from, to) =>
        codec.PendingTransfer(
          storeId = Some(encode(storeId)),
          fromDevice = Some(encode(from)),
          toDevice = Some(encode(to))
        )
    )

  def decode(m: codec.StorageDeviceSetState): StorageDeviceSetState =
    val setId = StorageDeviceSetId(decodeUUID(m.setId.get))
    val parent = m.parent.map(u => StorageDeviceSetId(decodeUUID(u)))
    val memberDevices = m.memberDevices.map(u => StorageDeviceId(decodeUUID(u))).toList
    val memberSets = m.memberSets.map(u => StorageDeviceSetId(decodeUUID(u))).toList
    val assignedPools = m.assignedPools.map(u => PoolId(decodeUUID(u))).toList
    val pendingTransfers = m.pendingTransfers.map: pt =>
      (decode(pt.storeId.get), decode(pt.fromDevice.get), decode(pt.toDevice.get))
    .toList
    new StorageDeviceSetState(setId, m.name, m.level, parent, memberDevices, memberSets,
      assignedPools, pendingTransfers)
```

(The `encode(storeId)`/`decode(codec.StoreId)` and `encode(StorageDeviceId)`/`decode(codec.StorageDeviceId)` helpers already exist in `Codec.scala`.)

- [ ] **Step 5: Run the test to verify it passes**

Run: `sbt 'testOnly *StorageDeviceSetStateCodecSuite'`
Expected: PASS (both tests).

- [ ] **Step 6: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/common/metadata/StorageDeviceSetState.scala \
        src/main/scala/org/aspen_ddp/aspen/common/network/Codec.scala \
        src/test/scala/org/aspen_ddp/aspen/common/metadata/StorageDeviceSetStateCodecSuite.scala
git commit -m "Add pendingTransfers to StorageDeviceSetState case class and Codec"
```

---

# Phase 2 — `offlineHosts` stub + write-threshold safety predicate

### Task 3: Add `AspenClient.offlineHosts()` stub

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/client/AspenClient.scala` (add a concrete default method near `backgroundTaskManager`, line 242)

- [ ] **Step 1: Add the method**

In `src/main/scala/org/aspen_ddp/aspen/client/AspenClient.scala`, immediately after `def backgroundTaskManager: BackgroundTaskManager` (line 242), add:

```scala

  /** Hosts currently believed to be offline. Stub returns empty until host
   *  liveness tracking exists; used by rebalancing to avoid moving stores onto,
   *  or counting availability from, offline hosts. */
  def offlineHosts(): Set[HostId] = Set()
```

Ensure `HostId` is imported in the file (it is used elsewhere in the trait via `getHostPointer(hostId: HostId)`, so the import already exists).

- [ ] **Step 2: Verify it compiles**

Run: `sbt compile`
Expected: BUILD SUCCESS.

- [ ] **Step 3: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/client/AspenClient.scala
git commit -m "Add AspenClient.offlineHosts() stub"
```

---

### Task 4: Pure write-threshold safety predicate `TransferSafety`

**Files:**
- Create: `src/main/scala/org/aspen_ddp/aspen/common/rebalancing/TransferSafety.scala`
- Test: `src/test/scala/org/aspen_ddp/aspen/common/rebalancing/TransferSafetySuite.scala`

- [ ] **Step 1: Write the failing test**

Create `src/test/scala/org/aspen_ddp/aspen/common/rebalancing/TransferSafetySuite.scala`:

```scala
package org.aspen_ddp.aspen.common.rebalancing

import org.aspen_ddp.aspen.common.ida.Replication
import org.aspen_ddp.aspen.common.metadata.{HostId, StorageDeviceId, StorageDeviceState}
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.util.UUID

class TransferSafetySuite extends AnyFunSuite with Matchers:

  private val poolId = PoolId(UUID.randomUUID())

  private def hostId() = HostId(UUID.randomUUID())
  private def devId() = StorageDeviceId(UUID.randomUUID())

  /** Build a PlanningState: one pool Replication(3,2) with 3 Active stores, each on its
   *  own device/host; `toDevice` is an extra empty device/host. */
  private def build(
      storeStatuses: List[StorageDeviceState.StoreStatus],
      offlineStoreHosts: Set[Int] = Set.empty
  ): (State.PlanningState, HostId, StorageDeviceId, HostId, Set[HostId]) =
    val hosts = (0 until storeStatuses.size).map(_ => hostId()).toVector
    val devs = (0 until storeStatuses.size).map(_ => devId()).toVector
    val stores = storeStatuses.zipWithIndex.map: (st, i) =>
      StoreId(poolId, i.toByte) -> State.Store(StoreId(poolId, i.toByte), 10L, st)
    val storesById = stores.toMap
    val devices = devs.zipWithIndex.map: (d, i) =>
      d -> State.Device(d, hosts(i), 10L, 1000L,
        Map(StoreId(poolId, i.toByte) -> storesById(StoreId(poolId, i.toByte))))
    val toHost = hostId()
    val toDev = devId()
    val devicesWithTarget = (devices :+ (toDev -> State.Device(toDev, toHost, 0L, 1000L, Map.empty))).toMap
    val pool = State.Pool(poolId, Replication(3, 2), storesById)
    val offline = offlineStoreHosts.map(hosts).toSet
    (State.PlanningState(devicesWithTarget, pool :: Nil match { case p :: Nil => Map(poolId -> p) }),
      hosts(0), toDev, toHost, offline)

  test("safe when moving one of three Active stores (3 active - 1 >= writeThreshold 2)"):
    val (state, _, toDev, _, offline) = build(List(
      StorageDeviceState.StoreStatus.Active,
      StorageDeviceState.StoreStatus.Active,
      StorageDeviceState.StoreStatus.Active))
    val fromDev = state.devices.find(_._2.stores.contains(StoreId(poolId, 0.toByte))).get._1
    val t = Plan.Transfer(StoreId(poolId, 0.toByte), fromDev, toDev)
    TransferSafety.isSafe(state, offline, t) shouldBe true

  test("unsafe when a second store's host is offline (usable 2 - 1 < 2)"):
    val (state, _, toDev, _, _) = build(List(
      StorageDeviceState.StoreStatus.Active,
      StorageDeviceState.StoreStatus.Active,
      StorageDeviceState.StoreStatus.Active), offlineStoreHosts = Set(2))
    val offline = state.devices.values
      .filter(d => d.stores.keySet.contains(StoreId(poolId, 2.toByte))).map(_.hostId).toSet
    val fromDev = state.devices.find(_._2.stores.contains(StoreId(poolId, 0.toByte))).get._1
    val t = Plan.Transfer(StoreId(poolId, 0.toByte), fromDev, toDev)
    TransferSafety.isSafe(state, offline, t) shouldBe false

  test("unsafe when the destination host is offline"):
    val (state, _, toDev, toHost, _) = build(List(
      StorageDeviceState.StoreStatus.Active,
      StorageDeviceState.StoreStatus.Active,
      StorageDeviceState.StoreStatus.Active))
    val fromDev = state.devices.find(_._2.stores.contains(StoreId(poolId, 0.toByte))).get._1
    val t = Plan.Transfer(StoreId(poolId, 0.toByte), fromDev, toDev)
    TransferSafety.isSafe(state, Set(toHost), t) shouldBe false
```

- [ ] **Step 2: Run it to verify it fails**

Run: `sbt 'testOnly *TransferSafetySuite'`
Expected: FAIL to compile — `TransferSafety` does not exist.

- [ ] **Step 3: Implement `TransferSafety`**

Create `src/main/scala/org/aspen_ddp/aspen/common/rebalancing/TransferSafety.scala`:

```scala
package org.aspen_ddp.aspen.common.rebalancing

import org.aspen_ddp.aspen.common.metadata.{HostId, StorageDeviceState}
import org.aspen_ddp.aspen.common.store.StoreId

/** Pure predicate: may a planned transfer be initiated right now without dropping its pool
 *  below the write threshold, and with an online destination host? Depends only on a
 *  PlanningState snapshot and the set of currently-offline hosts, so it is unit-testable. */
object TransferSafety:

  /** storeId -> the host currently owning it (via its device). */
  private def storeHosts(state: State.PlanningState): Map[StoreId, HostId] =
    state.devices.values.flatMap(d => d.stores.keys.map(sid => sid -> d.hostId)).toMap

  def isSafe(state: State.PlanningState,
             offlineHosts: Set[HostId],
             transfer: Plan.Transfer): Boolean =
    val hosts = storeHosts(state)
    state.devices.get(transfer.toDevice).exists: toDev =>
      !offlineHosts.contains(toDev.hostId) &&
        state.pools.get(transfer.storeId.poolId).exists: pool =>
          val usable = pool.stores.values.count: s =>
            s.status == StorageDeviceState.StoreStatus.Active &&
              hosts.get(s.storeId).exists(h => !offlineHosts.contains(h))
          // The store being moved goes TransferringOut (momentarily unavailable for writes),
          // so require the remaining usable count to still satisfy the write threshold.
          usable - 1 >= pool.ida.writeThreshold
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `sbt 'testOnly *TransferSafetySuite'`
Expected: PASS (all three tests).

- [ ] **Step 5: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/common/rebalancing/TransferSafety.scala \
        src/test/scala/org/aspen_ddp/aspen/common/rebalancing/TransferSafetySuite.scala
git commit -m "Add pure TransferSafety write-threshold predicate"
```

---

# Phase 3 — Service state + service messages (encode/decode)

### Task 5: `RebalancingServiceState` (`ActiveRebalancingTasks`) proto + codec + test

**Files:**
- Modify: `src/main/protobuf/codec.proto` (add two messages)
- Create: `src/main/scala/org/aspen_ddp/aspen/common/rebalancing/RebalancingServiceState.scala`
- Modify: `src/main/scala/org/aspen_ddp/aspen/common/network/Codec.scala` (add encode/decode)
- Test: `src/test/scala/org/aspen_ddp/aspen/common/rebalancing/RebalancingServiceStateSuite.scala`

- [ ] **Step 1: Add proto messages**

In `src/main/protobuf/codec.proto`, add (near the other rebalancing/pointer messages):

```proto
message ActiveRebalancingTask {
  UUID setId = 1;
  ObjectPointer taskState = 2;
}

message ActiveRebalancingTasks {
  repeated ActiveRebalancingTask tasks = 1;
}
```

Run: `sbt compile`
Expected: BUILD SUCCESS (generates `codec.ActiveRebalancingTask` and `codec.ActiveRebalancingTasks`).

- [ ] **Step 2: Write the failing test**

Create `src/test/scala/org/aspen_ddp/aspen/common/rebalancing/RebalancingServiceStateSuite.scala`:

```scala
package org.aspen_ddp.aspen.common.rebalancing

import org.aspen_ddp.aspen.common.Radicle
import org.aspen_ddp.aspen.common.metadata.StorageDeviceSetId
import org.aspen_ddp.aspen.common.objects.KeyValueObjectPointer
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.util.UUID

class RebalancingServiceStateSuite extends AnyFunSuite with Matchers:

  test("ActiveRebalancingTasks round-trips"):
    val entries = List(
      StorageDeviceSetId(UUID.randomUUID()) -> KeyValueObjectPointer(Radicle.objectId, Radicle.poolId),
      StorageDeviceSetId(UUID.randomUUID()) -> KeyValueObjectPointer(Radicle.objectId, Radicle.poolId)
    )
    val bytes = RebalancingServiceState.encodeActiveTasks(entries)
    val decoded = RebalancingServiceState.decodeActiveTasks(bytes)
    decoded.map(_._1) shouldBe entries.map(_._1)
    decoded.map(_._2.id) shouldBe entries.map(_._2.id)

  test("empty list round-trips"):
    RebalancingServiceState.decodeActiveTasks(
      RebalancingServiceState.encodeActiveTasks(Nil)) shouldBe Nil
```

- [ ] **Step 3: Run it to verify it fails**

Run: `sbt 'testOnly *RebalancingServiceStateSuite'`
Expected: FAIL to compile — `RebalancingServiceState` does not exist.

- [ ] **Step 4: Add Codec helpers**

In `src/main/scala/org/aspen_ddp/aspen/common/network/Codec.scala`, add near the other `StorageDeviceSetState`/`ObjectPointer` helpers:

```scala
  def encode(o: (org.aspen_ddp.aspen.common.metadata.StorageDeviceSetId,
                 org.aspen_ddp.aspen.common.objects.KeyValueObjectPointer)): codec.ActiveRebalancingTask =
    codec.ActiveRebalancingTask(
      setId = Some(encodeUUID(o._1.uuid)),
      taskState = Some(encode(o._2: org.aspen_ddp.aspen.common.objects.ObjectPointer))
    )

  def decodeActiveRebalancingTask(m: codec.ActiveRebalancingTask):
      (org.aspen_ddp.aspen.common.metadata.StorageDeviceSetId,
       org.aspen_ddp.aspen.common.objects.KeyValueObjectPointer) =
    val setId = org.aspen_ddp.aspen.common.metadata.StorageDeviceSetId(decodeUUID(m.setId.get))
    decode(m.taskState.get) match
      case p: org.aspen_ddp.aspen.common.objects.KeyValueObjectPointer => (setId, p)
      case other =>
        throw new IllegalArgumentException(
          s"ActiveRebalancingTask.taskState must be a KeyValueObjectPointer, got ${other.objectType}")
```

(Using fully-qualified names avoids adding imports; match the surrounding file's import style if you prefer — both `StorageDeviceSetId` and `KeyValueObjectPointer`/`ObjectPointer` are already imported in `Codec.scala`, in which case use the short names.)

- [ ] **Step 5: Implement `RebalancingServiceState`**

Create `src/main/scala/org/aspen_ddp/aspen/common/rebalancing/RebalancingServiceState.scala`:

```scala
package org.aspen_ddp.aspen.common.rebalancing

import org.aspen_ddp.aspen.codec
import org.aspen_ddp.aspen.common.metadata.StorageDeviceSetId
import org.aspen_ddp.aspen.common.network.Codec
import org.aspen_ddp.aspen.common.objects.{Key, KeyValueObjectPointer}

/** Helpers for the RebalancingDurableService's KV state object. The single
 *  ActiveTasksKey holds the encoded ActiveRebalancingTasks list: one
 *  (setId, taskStatePointer) entry per in-flight set rebalance. */
object RebalancingServiceState:

  val ActiveTasksKey: Key = Key(Array[Byte](0))

  type ActiveTasks = List[(StorageDeviceSetId, KeyValueObjectPointer)]

  def encodeActiveTasks(tasks: ActiveTasks): Array[Byte] =
    codec.ActiveRebalancingTasks(tasks = tasks.map(Codec.encode)).toByteArray

  def decodeActiveTasks(bytes: Array[Byte]): ActiveTasks =
    codec.ActiveRebalancingTasks.parseFrom(bytes).tasks.map(Codec.decodeActiveRebalancingTask).toList
```

- [ ] **Step 6: Run the test to verify it passes**

Run: `sbt 'testOnly *RebalancingServiceStateSuite'`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add src/main/protobuf/codec.proto \
        src/main/scala/org/aspen_ddp/aspen/common/rebalancing/RebalancingServiceState.scala \
        src/main/scala/org/aspen_ddp/aspen/common/network/Codec.scala \
        src/test/scala/org/aspen_ddp/aspen/common/rebalancing/RebalancingServiceStateSuite.scala
git commit -m "Add RebalancingServiceState ActiveRebalancingTasks encode/decode"
```

---

### Task 6: Rebalancing service-message payloads (`RebalancingMessage`) proto + codec + test

These are the **content** (`encodedContent`) of a `ServiceMessage`, not new `HostMessage` types. They live in the rebalancing package.

**Files:**
- Modify: `src/main/protobuf/codec.proto` (add three messages)
- Create: `src/main/scala/org/aspen_ddp/aspen/common/rebalancing/RebalancingMessage.scala`
- Test: `src/test/scala/org/aspen_ddp/aspen/common/rebalancing/RebalancingMessageSuite.scala`

- [ ] **Step 1: Add proto messages**

In `src/main/protobuf/codec.proto`, add:

```proto
message NewSetRebalanceInitiated {
  UUID setId = 1;
}

message TransferComplete {
  UUID setId = 1;
  StoreId storeId = 2;
  StorageDeviceId fromDevice = 3;
  StorageDeviceId toDevice = 4;
}

message RebalancingMessage {
  oneof msg {
    NewSetRebalanceInitiated newSetRebalanceInitiated = 1;
    TransferComplete transferComplete = 2;
  }
}
```

Run: `sbt compile`
Expected: BUILD SUCCESS.

- [ ] **Step 2: Write the failing test**

Create `src/test/scala/org/aspen_ddp/aspen/common/rebalancing/RebalancingMessageSuite.scala`:

```scala
package org.aspen_ddp.aspen.common.rebalancing

import org.aspen_ddp.aspen.common.metadata.{StorageDeviceId, StorageDeviceSetId}
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.util.UUID

class RebalancingMessageSuite extends AnyFunSuite with Matchers:

  test("NewSetRebalanceInitiated round-trips"):
    val m = NewSetRebalanceInitiated(StorageDeviceSetId(UUID.randomUUID()))
    RebalancingMessage.decode(RebalancingMessage.encode(m)) shouldBe m

  test("TransferComplete round-trips"):
    val m = TransferComplete(
      StorageDeviceSetId(UUID.randomUUID()),
      StoreId(PoolId(UUID.randomUUID()), 3.toByte),
      StorageDeviceId(UUID.randomUUID()),
      StorageDeviceId(UUID.randomUUID()))
    RebalancingMessage.decode(RebalancingMessage.encode(m)) shouldBe m
```

- [ ] **Step 3: Run it to verify it fails**

Run: `sbt 'testOnly *RebalancingMessageSuite'`
Expected: FAIL to compile — types do not exist.

- [ ] **Step 4: Implement `RebalancingMessage`**

Create `src/main/scala/org/aspen_ddp/aspen/common/rebalancing/RebalancingMessage.scala`:

```scala
package org.aspen_ddp.aspen.common.rebalancing

import org.aspen_ddp.aspen.codec
import org.aspen_ddp.aspen.common.metadata.{StorageDeviceId, StorageDeviceSetId}
import org.aspen_ddp.aspen.common.network.Codec
import org.aspen_ddp.aspen.common.store.StoreId

/** Payload types carried in the `encodedContent` of a ServiceMessage addressed to the
 *  RebalancingDurableService. Best-effort wake-ups; correctness comes from polling. */
sealed trait RebalancingMessage

case class NewSetRebalanceInitiated(setId: StorageDeviceSetId) extends RebalancingMessage

case class TransferComplete(setId: StorageDeviceSetId,
                            storeId: StoreId,
                            fromDevice: StorageDeviceId,
                            toDevice: StorageDeviceId) extends RebalancingMessage

object RebalancingMessage:

  def encode(m: RebalancingMessage): Array[Byte] =
    val proto = m match
      case NewSetRebalanceInitiated(setId) =>
        codec.RebalancingMessage(codec.RebalancingMessage.Msg.NewSetRebalanceInitiated(
          codec.NewSetRebalanceInitiated(setId = Some(Codec.encodeUUID(setId.uuid)))))
      case TransferComplete(setId, storeId, from, to) =>
        codec.RebalancingMessage(codec.RebalancingMessage.Msg.TransferComplete(
          codec.TransferComplete(
            setId = Some(Codec.encodeUUID(setId.uuid)),
            storeId = Some(Codec.encode(storeId)),
            fromDevice = Some(Codec.encode(from)),
            toDevice = Some(Codec.encode(to)))))
    proto.toByteArray

  def decode(bytes: Array[Byte]): RebalancingMessage =
    val proto = codec.RebalancingMessage.parseFrom(bytes)
    proto.msg match
      case codec.RebalancingMessage.Msg.NewSetRebalanceInitiated(m) =>
        NewSetRebalanceInitiated(StorageDeviceSetId(Codec.decodeUUID(m.setId.get)))
      case codec.RebalancingMessage.Msg.TransferComplete(m) =>
        TransferComplete(
          StorageDeviceSetId(Codec.decodeUUID(m.setId.get)),
          Codec.decode(m.storeId.get),
          Codec.decode(m.fromDevice.get),
          Codec.decode(m.toDevice.get))
      case codec.RebalancingMessage.Msg.Empty =>
        throw new IllegalArgumentException("Empty RebalancingMessage")
```

Note: `Codec.encodeUUID`/`decodeUUID`/`encode(StoreId)`/`decode(codec.StoreId)`/`encode(StorageDeviceId)`/`decode(codec.StorageDeviceId)` are existing public methods on the `Codec` object.

- [ ] **Step 5: Run the test to verify it passes**

Run: `sbt 'testOnly *RebalancingMessageSuite'`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add src/main/protobuf/codec.proto \
        src/main/scala/org/aspen_ddp/aspen/common/rebalancing/RebalancingMessage.scala \
        src/test/scala/org/aspen_ddp/aspen/common/rebalancing/RebalancingMessageSuite.scala
git commit -m "Add RebalancingMessage service-payload types and codec"
```

---

# Phase 4 — Pluggable store-transfer mechanism

### Task 7: Extract `StoreTransferIn`/`StoreTransferOut`/`StoreTransferFactory` and refactor `StoreManager`

**Files:**
- Create: `src/main/scala/org/aspen_ddp/aspen/server/transfer/StoreTransfer.scala`
- Modify: `src/main/scala/org/aspen_ddp/aspen/server/transfer/TransferringIn.scala`
- Modify: `src/main/scala/org/aspen_ddp/aspen/server/transfer/TransferringOut.scala`
- Modify: `src/main/scala/org/aspen_ddp/aspen/server/StoreManager.scala` (constructor line 72-84; fields 109-110; sites 354, 423)

This task has no new unit test (it is a behavior-preserving refactor); it is verified by `sbt compile` and the existing suite. Its payoff is tested in Phase 8.

- [ ] **Step 1: Create the traits + factory**

Create `src/main/scala/org/aspen_ddp/aspen/server/transfer/StoreTransfer.scala`:

```scala
package org.aspen_ddp.aspen.server.transfer

import org.aspen_ddp.aspen.client.AspenClient
import org.aspen_ddp.aspen.common.{DataBuffer, HLCTimestamp}
import org.aspen_ddp.aspen.common.metadata.{HostId, StorageDeviceId}
import org.aspen_ddp.aspen.common.store.StoreId

import java.nio.file.Path
import java.util.UUID
import scala.concurrent.Future

/** Receiving side of a store transfer. `complete` resolves when all data has arrived and
 *  the store is in place. */
trait StoreTransferIn:
  val storeId: StoreId
  val transferUUID: UUID
  def complete: Future[Unit]
  def dataReceived(db: DataBuffer): Unit

/** Sending side of a store transfer. `complete` resolves when all data has been sent. */
trait StoreTransferOut:
  val storeId: StoreId
  val transferUUID: UUID
  def complete: Future[Unit]
  def abort(): Unit

/** Constructs the in/out halves of a store transfer. The default `Filesystem` factory
 *  produces the real (filesystem + `jar` subprocess) implementations. Tests inject an
 *  in-memory factory that skips disk/process work. */
trait StoreTransferFactory:
  def createTransferIn(client: AspenClient,
                       storeId: StoreId,
                       storageDeviceId: StorageDeviceId,
                       devicePath: Path): StoreTransferIn

  def createTransferOut(client: AspenClient,
                        fromDevice: StorageDeviceId,
                        devicePath: Path,
                        storeId: StoreId,
                        toHost: HostId,
                        toDevice: StorageDeviceId,
                        timestamp: HLCTimestamp,
                        transferUUID: UUID): StoreTransferOut

object StoreTransferFactory:
  object Filesystem extends StoreTransferFactory:
    def createTransferIn(client: AspenClient,
                         storeId: StoreId,
                         storageDeviceId: StorageDeviceId,
                         devicePath: Path): StoreTransferIn =
      new TransferringIn(client, storeId, storageDeviceId, devicePath)

    def createTransferOut(client: AspenClient,
                          fromDevice: StorageDeviceId,
                          devicePath: Path,
                          storeId: StoreId,
                          toHost: HostId,
                          toDevice: StorageDeviceId,
                          timestamp: HLCTimestamp,
                          transferUUID: UUID): StoreTransferOut =
      new TransferringOut(client, fromDevice, devicePath, storeId, toHost, toDevice, timestamp, transferUUID)
```

- [ ] **Step 2: Make `TransferringIn` implement `StoreTransferIn`**

In `src/main/scala/org/aspen_ddp/aspen/server/transfer/TransferringIn.scala`:
- Change the class declaration `extends Logging` to `extends StoreTransferIn with Logging`.
- Change the public `complete` to return `Future[Unit]`. The internal `completionPromise` currently is `Promise[TransferringIn]`; change it to `Promise[Unit]`, change `def complete: Future[TransferringIn]` to `def complete: Future[Unit]`, change `completionPromise.success(this)` (line 95) to `completionPromise.success(())`, and `completionPromise.failure(...)` stays as-is.

Concretely:
- Line 27: `private val completionPromise: Promise[TransferringIn] = Promise()` → `private val completionPromise: Promise[Unit] = Promise()`
- Line 31: `def complete: Future[TransferringIn] = completionPromise.future` → `def complete: Future[Unit] = completionPromise.future`
- Line 95: `completionPromise.success(this)` → `completionPromise.success(())`

`storeId` and `transferUUID` are already `val` members, satisfying the trait.

- [ ] **Step 3: Make `TransferringOut` implement `StoreTransferOut`**

In `src/main/scala/org/aspen_ddp/aspen/server/transfer/TransferringOut.scala`:
- Line 24: `... val transferUUID: UUID ) extends Logging:` → `... val transferUUID: UUID ) extends StoreTransferOut with Logging:`
- Line 29: `private val completionPromise: Promise[TransferringOut] = Promise()` → `private val completionPromise: Promise[Unit] = Promise()`
- Line 31: `def complete: Future[TransferringOut] = completionPromise.future` → `def complete: Future[Unit] = completionPromise.future`
- Line 89: `completionPromise.success(this)` → `completionPromise.success(())`

`storeId`, `transferUUID`, and `abort()` are already present, satisfying the trait.

- [ ] **Step 4: Thread the factory through `StoreManager`**

In `src/main/scala/org/aspen_ddp/aspen/server/StoreManager.scala`:

(a) Add a constructor parameter with a default (line 84 area — append before `extends Logging`):

```scala
                     val heartbeatPeriod: Duration,
                     val checkStorageDevicePeriod: Duration,
                     val storeTransferFactory: StoreTransferFactory = StoreTransferFactory.Filesystem) extends Logging {
```

(b) Add the import (with the existing transfer import at line 22):

```scala
import org.aspen_ddp.aspen.server.transfer.{StoreTransferFactory, StoreTransferIn, StoreTransferOut, TransferringIn, TransferringOut}
```

(c) Change the field types (lines 109-110):

```scala
    private var transferringOut: Map[StoreId, StoreTransferOut] = Map()
    private var transferringInUUIDs: Map[UUID, StoreTransferIn] = Map()
```

(d) Replace the `new TransferringIn(...)` at line 354 with:

```scala
        val ti = storeTransferFactory.createTransferIn(
          client,
          storeId,
          toDeviceid,
          toDevice.devicePath
        )
```

(e) Replace the `new TransferringOut(...)` at line 423 with:

```scala
                    val to = storeTransferFactory.createTransferOut(
                      client,
                      sourceDs.storageDeviceId,
                      sourceDs.devicePath,
                      m.storeId,
                      toDevice.hostId,
                      toDevice.storageDeviceId,
                      m.timestamp,
                      m.transferUUID
                    )
```

- [ ] **Step 5: Verify compile and existing tests still pass**

Run: `sbt compile`
Expected: BUILD SUCCESS.

Run: `sbt test`
Expected: the full existing suite passes (behavior-preserving refactor). If a call site elsewhere used `TransferringIn.complete`'s resolved value (`this`), fix it — grep first:
Run: `grep -rn "\.complete" src/main/scala/org/aspen_ddp/aspen/server/`

- [ ] **Step 6: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/server/transfer/StoreTransfer.scala \
        src/main/scala/org/aspen_ddp/aspen/server/transfer/TransferringIn.scala \
        src/main/scala/org/aspen_ddp/aspen/server/transfer/TransferringOut.scala \
        src/main/scala/org/aspen_ddp/aspen/server/StoreManager.scala
git commit -m "Extract StoreTransferFactory seam and thread through StoreManager"
```

---

# Phase 5 — `SetRebalanceDurableTask`

### Task 8: Implement `SetRebalanceDurableTask`

**Files:**
- Create: `src/main/scala/org/aspen_ddp/aspen/common/rebalancing/SetRebalanceTaskState.scala`
- Create: `src/main/scala/org/aspen_ddp/aspen/common/rebalancing/SetRebalanceDurableTask.scala`

Tested end-to-end in Phase 7's integration suite (it needs the in-memory transfer harness). No standalone unit test — its logic is inherently async and I/O-bound.

- [ ] **Step 1: Task-state helper**

Create `src/main/scala/org/aspen_ddp/aspen/common/rebalancing/SetRebalanceTaskState.scala`:

```scala
package org.aspen_ddp.aspen.common.rebalancing

import org.aspen_ddp.aspen.common.metadata.StorageDeviceSetId
import org.aspen_ddp.aspen.common.objects.Key
import org.aspen_ddp.aspen.common.util.{uuid2byte, byte2uuid}

/** The minimal KV state object for a SetRebalanceDurableTask: it stores only the setId under
 *  SetIdKey. All rebalancing progress lives in the set's pendingTransfers list; this object
 *  exists to satisfy the DurableTask taskPointer contract and to be the refcount-tracked
 *  anchor the service points at. */
object SetRebalanceTaskState:
  val SetIdKey: Key = Key(Array[Byte](0))

  def initialContent(setId: StorageDeviceSetId): Map[Key, Array[Byte]] =
    Map(SetIdKey -> uuid2byte(setId.uuid))

  def decodeSetId(bytes: Array[Byte]): StorageDeviceSetId =
    StorageDeviceSetId(byte2uuid(bytes))
```

- [ ] **Step 2: The task**

Create `src/main/scala/org/aspen_ddp/aspen/common/rebalancing/SetRebalanceDurableTask.scala`:

```scala
package org.aspen_ddp.aspen.common.rebalancing

import org.aspen_ddp.aspen.client.AspenClient
import org.aspen_ddp.aspen.common.DataBuffer
import org.aspen_ddp.aspen.common.metadata.{StorageDeviceId, StorageDeviceSetId, StorageDeviceState}
import org.aspen_ddp.aspen.common.objects.Transaction
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.util.BackgroundTaskManager.{NoTask, ScheduledTask}
import org.aspen_ddp.aspen.compute.{DurableTask, DurableTaskPointer, TaskStopped}
import scribe.Logging

import scala.concurrent.duration.Duration
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success}

/** Drives one storage-device-set rebalance to completion, serially: one store transfer in
 *  flight at a time. Progress is the set's `pendingTransfers` list, which is drained as each
 *  transfer completes. Blocked transfers (write-threshold/offline) are skipped and revisited.
 *  Extends DurableTask for stop()/isStopped; it is constructed and driven directly by
 *  RebalancingDurableService (not via SimpleTaskExecutor). */
class SetRebalanceDurableTask(
    val client: AspenClient,
    val taskPointer: DurableTaskPointer,
    val setId: StorageDeviceSetId,
    pollPeriod: Duration
) extends DurableTask with Logging:

  private given ExecutionContext = client.clientContext

  private val promise = Promise[Option[AnyRef]]()
  def completed: Future[Option[AnyRef]] = promise.future

  private var pollTask: ScheduledTask = NoTask

  // Begin driving immediately.
  processNext()

  /** External wake-up (e.g. a TransferComplete message arrived). */
  def wake(): Unit = processNext()

  override protected def onStop(): Unit = synchronized:
    pollTask.cancel()
    if !promise.isCompleted then promise.failure(new TaskStopped)

  private def scheduleRecheck(): Unit = synchronized:
    if !isStopped then
      pollTask.cancel()
      pollTask = client.backgroundTaskManager.schedule(pollPeriod):
        processNext()

  private def finishOk(): Unit = synchronized:
    if !promise.isCompleted then promise.success(None)

  private type Transfer = (StoreId, StorageDeviceId, StorageDeviceId)

  private def processNext(): Unit =
    if isStopped then
      synchronized { if !promise.isCompleted then promise.failure(new TaskStopped) }
    else
      val f = client.getStorageDeviceSetState(setId).flatMap: setState =>
        val pending = setState.pendingTransfers
        if pending.isEmpty then
          finishOk()
          Future.unit
        else
          drive(pending)
      f.failed.foreach: err =>
        logger.warn(s"Rebalance set $setId: transient error, will retry: $err")
        scheduleRecheck()

  /** Given a non-empty pending list, either (a) finish an already-completed transfer by
   *  removing it, (b) wait for an in-flight transfer, or (c) start the first safe not-started
   *  transfer. Falls through to scheduleRecheck() when nothing can progress right now. */
  private def drive(pending: List[Transfer]): Future[Unit] =
    // Classify each pending transfer by reading destination + source device state.
    Future.traverse(pending)(classify).flatMap: classified =>
      // 1. Any transfer already completed on the stores? Remove it and loop.
      classified.collectFirst { case (t, Completed) => t } match
        case Some(t) =>
          removePending(t).map(_ => processNext())
        case None =>
          // 2. Any transfer in flight? Wait for it.
          if classified.exists(_._2 == InFlight) then
            scheduleRecheck()
            Future.unit
          else
            // 3. Start the first safe not-started transfer.
            client.getStorageDeviceSetState(setId).flatMap: setState =>
              State.getStateForRebalancePlanning(client, setState).map: planningState =>
                val offline = client.offlineHosts()
                val notStarted = classified.collect { case (t, NotStarted) => t }
                notStarted.find(t => TransferSafety.isSafe(planningState, offline,
                    Plan.Transfer(t._1, t._2, t._3))) match
                  case Some((storeId, _, toDevice)) =>
                    client.transferStore(storeId, toDevice)
                    scheduleRecheck()
                  case None =>
                    // Nothing safe right now; revisit later.
                    scheduleRecheck()

  private enum Phase:
    case Completed, InFlight, NotStarted
  import Phase.*

  private def classify(t: Transfer): Future[(Transfer, Phase)] =
    val (storeId, fromDevice, toDevice) = t
    for
      dst <- client.getStorageDeviceState(toDevice)
      src <- client.getStorageDeviceState(fromDevice)
    yield
      val dstEntry = dst.stores.get(storeId)
      val srcEntry = src.stores.get(storeId)
      val phase =
        if dstEntry.exists(_.status == StorageDeviceState.StoreStatus.Active) && srcEntry.isEmpty then
          Completed
        else if dstEntry.exists(_.status == StorageDeviceState.StoreStatus.TransferringIn) ||
                srcEntry.exists(_.status == StorageDeviceState.StoreStatus.TransferringOut) then
          InFlight
        else
          NotStarted
      (t, phase)

  /** Remove one completed transfer from the set's pendingTransfers, revision-checked. */
  private def removePending(t: Transfer): Future[Unit] =
    client.transactUntilSuccessful: tx =>
      given Transaction = tx
      for
        setPtr <- client.getStorageDeviceSetPointer(setId)
        dos <- client.read(setPtr)
      yield
        val current = StorageDeviceSetState(dos)
        val updated = current.copy(pendingTransfers = current.pendingTransfers.filterNot(_ == t))
        tx.overwrite(setPtr, dos.revision, DataBuffer(updated.toBytes))
```

Notes for the implementer:
- `client.getStorageDeviceSetPointer` is `private[aspen]` and returns `Future[DataObjectPointer]`; this file is under `org.aspen_ddp.aspen`, so access is allowed. `client.read(dataObjectPointer)` returns a `DataObjectState` with `.data` and `.revision`; `StorageDeviceSetState(dos)` decodes it.
- `Transaction` and `tx.overwrite(ptr, revision, DataBuffer)` follow the pattern in `SteppedDurableTaskSuite` (`tx.overwrite(dataPointer, dos.revision, DataBuffer(...))`). Confirm the `Transaction` import path matches what the client `transact` block expects — it is `org.aspen_ddp.aspen.client.Transaction` in most call sites; if the compiler objects to the `common.objects.Transaction` import, switch to `import org.aspen_ddp.aspen.client.Transaction`.
- `transactUntilSuccessful` requires `given ExecutionContext` (in scope) — good.

- [ ] **Step 3: Compile**

Run: `sbt compile`
Expected: BUILD SUCCESS. Fix any import path discrepancy for `Transaction`/`DataBuffer`/`DataObjectState` flagged by the compiler (all exist; only the package path may need adjusting to match sibling files — cross-check against `SteppedDurableTaskSuite.scala` imports).

- [ ] **Step 4: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/common/rebalancing/SetRebalanceTaskState.scala \
        src/main/scala/org/aspen_ddp/aspen/common/rebalancing/SetRebalanceDurableTask.scala
git commit -m "Add SetRebalanceDurableTask and its state helper"
```

---

# Phase 6 — `RebalancingDurableService` + companion

### Task 9: Implement the service, factory, and `rebalanceStorageDeviceSet`

**Files:**
- Create: `src/main/scala/org/aspen_ddp/aspen/common/rebalancing/RebalancingDurableService.scala`
- Modify: `src/main/scala/org/aspen_ddp/aspen/server/TypeFactories.scala`

- [ ] **Step 1: Implement the service + factory + companion**

Create `src/main/scala/org/aspen_ddp/aspen/common/rebalancing/RebalancingDurableService.scala`:

```scala
package org.aspen_ddp.aspen.common.rebalancing

import org.aspen_ddp.aspen.client.{AspenClient, KeyValueObjectState, Transaction}
import org.aspen_ddp.aspen.client.internal.allocation.PoolObjectAllocator
import org.aspen_ddp.aspen.client.tkvl.{KVObjectRootManager, TieredKeyValueList}
import org.aspen_ddp.aspen.common.Radicle
import org.aspen_ddp.aspen.common.metadata.StorageDeviceSetId
import org.aspen_ddp.aspen.common.network.ServiceMessage
import org.aspen_ddp.aspen.common.objects.{Key, KeyValueObjectPointer, Value}
import org.aspen_ddp.aspen.common.transaction.KeyValueUpdate
import org.aspen_ddp.aspen.common.util.BackgroundTaskManager.{NoTask, ScheduledTask}
import org.aspen_ddp.aspen.compute.{DurableService, DurableServiceExecutor, DurableServiceFactory, DurableTaskPointer, ServiceEntry}
import scribe.Logging

import java.util.UUID
import scala.concurrent.duration.{Duration, MINUTES}
import scala.concurrent.{ExecutionContext, Future}

object RebalancingDurableService extends DurableServiceFactory with Logging:

  val ServiceTypeUUID: UUID = UUID.fromString("d5f1e2a7-0b3c-4e6a-9f21-7c8a4b1e0d33")
  val ServiceUUID: UUID     = UUID.fromString("b7c04e18-2a9f-4d55-8e13-6f0a1c2d3e4b")

  val typeUUID: UUID = ServiceTypeUUID

  val DefaultPollPeriod: Duration = Duration(2, MINUTES)

  override def createService(client: AspenClient,
                             statePointer: KeyValueObjectPointer,
                             state: KeyValueObjectState): DurableService =
    new RebalancingDurableService(client, statePointer, DefaultPollPeriod)

  /** Register the singleton service (idempotent). Call once per system at host startup, from
   *  the same place the DurableServiceExecutor is created. */
  def register(executor: DurableServiceExecutor): Future[Unit] =
    executor.registerService(
      ServiceTypeUUID,
      ServiceUUID,
      Map(RebalancingServiceState.ActiveTasksKey -> RebalancingServiceState.encodeActiveTasks(Nil)))

  /** Read the service state object's pointer via the services TKVL. */
  private def readServiceStatePointer(client: AspenClient): Future[KeyValueObjectPointer] =
    given ExecutionContext = client.clientContext
    val tkvl = TieredKeyValueList(client, KVObjectRootManager(client, Radicle.ServicesTreeKey, Radicle.pointer))
    tkvl.get(Key(ServiceUUID)).map:
      case Some(vs) => ServiceEntry.decode(vs.value.bytes).statePointer
      case None => throw new IllegalStateException("RebalancingDurableService is not registered")

  /** Build a plan for `setId` and enroll a SetRebalanceDurableTask, unless one is already in
   *  progress. Idempotent and safe against concurrent callers (revision-checked writes). */
  def rebalanceStorageDeviceSet(client: AspenClient, setId: StorageDeviceSetId): Future[Unit] =
    given ExecutionContext = client.clientContext

    for
      setState <- client.getStorageDeviceSetState(setId)
      result <-
        if setState.pendingTransfers.nonEmpty then
          Future.unit
        else
          for
            statePtr <- readServiceStatePointer(client)
            stateKvos <- client.read(statePtr)
            active = RebalancingServiceState.decodeActiveTasks(
                       stateKvos.contents(RebalancingServiceState.ActiveTasksKey).value.bytes)
            r <-
              if active.exists(_._1 == setId) then
                Future.unit
              else
                planAndEnroll(client, setId, setState, statePtr)
          yield r
    yield result

  private def planAndEnroll(client: AspenClient,
                            setId: StorageDeviceSetId,
                            setState: org.aspen_ddp.aspen.common.metadata.StorageDeviceSetState,
                            statePtr: KeyValueObjectPointer): Future[Unit] =
    given ExecutionContext = client.clientContext
    for
      planningState <- State.getStateForRebalancePlanning(client, setState)
      plan = Plan.computePlan(planningState)
      _ <-
        if plan.isEmpty then Future.unit
        else enroll(client, setId, plan.map(t => (t.storeId, t.fromDevice, t.toDevice)), statePtr)
    yield ()

  private def enroll(client: AspenClient,
                     setId: StorageDeviceSetId,
                     transfers: List[(org.aspen_ddp.aspen.common.store.StoreId,
                                      org.aspen_ddp.aspen.common.metadata.StorageDeviceId,
                                      org.aspen_ddp.aspen.common.metadata.StorageDeviceId)],
                     statePtr: KeyValueObjectPointer): Future[Unit] =
    given ExecutionContext = client.clientContext

    val done: Future[Unit] = client.transactUntilSuccessful: tx =>
      given Transaction = tx
      for
        pool <- client.getStoragePool(Radicle.poolId)
        allocator = new PoolObjectAllocator(client, pool)
        taskStatePtr <- allocator.allocateKeyValueObject(
                          SetRebalanceTaskState.initialContent(setId).map((k, v) => k -> Value(v)))
        setPtr <- client.getStorageDeviceSetPointer(setId)
        setDos <- client.read(setPtr)
        stateKvos <- client.read(statePtr)
      yield
        // (a) write pendingTransfers onto the set (DataObject overwrite, revision-checked)
        val curSet = org.aspen_ddp.aspen.common.metadata.StorageDeviceSetState(setDos)
        val updatedSet = curSet.copy(pendingTransfers = transfers)
        tx.overwrite(setPtr, setDos.revision,
          org.aspen_ddp.aspen.common.DataBuffer(updatedSet.toBytes))

        // (b) add (setId, taskStatePtr) to ActiveRebalancingTasks (KV, revision-checked)
        val active = RebalancingServiceState.decodeActiveTasks(
                       stateKvos.contents(RebalancingServiceState.ActiveTasksKey).value.bytes)
        val newActive = active :+ (setId -> taskStatePtr)
        val reqs = KeyValueUpdate.KeyRevision(
                     RebalancingServiceState.ActiveTasksKey,
                     stateKvos.contents(RebalancingServiceState.ActiveTasksKey).revision) :: Nil
        val ops = org.aspen_ddp.aspen.common.objects.Insert(
                    RebalancingServiceState.ActiveTasksKey,
                    RebalancingServiceState.encodeActiveTasks(newActive)) :: Nil
        tx.update(statePtr, None, None, reqs, ops)

    done.map: _ =>
      // best-effort wake-up
      client.sendServiceMessage(ServiceUUID, RebalancingMessage.encode(NewSetRebalanceInitiated(setId)))
      ()


class RebalancingDurableService(val client: AspenClient,
                                val statePointer: KeyValueObjectPointer,
                                pollPeriod: Duration) extends DurableService with Logging:

  import RebalancingDurableService.*

  private given ExecutionContext = client.clientContext

  // setId -> running task
  private var tasks: Map[StorageDeviceSetId, SetRebalanceDurableTask] = Map.empty
  private var pollTask: ScheduledTask = NoTask
  @volatile private var stopped = false

  // Resume on construction, then poll periodically.
  reconcile()
  pollTask = client.backgroundTaskManager.scheduleNonConcurrentPollingTask(pollPeriod):
    reconcile()

  override def shutdown(): Unit = synchronized:
    stopped = true
    pollTask.cancel()
    tasks.values.foreach(_.stop())
    tasks = Map.empty

  override def receiveMessage(msg: ServiceMessage): Unit =
    RebalancingMessage.decode(msg.encodedContent) match
      case NewSetRebalanceInitiated(_) =>
        reconcile()
      case TransferComplete(setId, _, _, _) =>
        synchronized(tasks.get(setId)).foreach(_.wake())

  /** Read ActiveRebalancingTasks and reconcile the local task map: start tasks newly present.
   *  Returns a Future so it composes with scheduleNonConcurrentPollingTask. */
  private def reconcile(): Future[Unit] =
    if stopped then Future.unit
    else
      client.read(statePointer).map: kvos =>
        val active = RebalancingServiceState.decodeActiveTasks(
                       kvos.contents(RebalancingServiceState.ActiveTasksKey).value.bytes)
        synchronized:
          if !stopped then
            active.foreach: (setId, taskStatePtr) =>
              if !tasks.contains(setId) then
                startTask(setId, taskStatePtr)
      .recover:
        case err => logger.warn(s"RebalancingDurableService reconcile failed: $err")

  private def startTask(setId: StorageDeviceSetId, taskStatePtr: KeyValueObjectPointer): Unit =
    val task = new SetRebalanceDurableTask(client, DurableTaskPointer(taskStatePtr), setId, pollPeriod)
    tasks += setId -> task
    task.completed.onComplete: _ =>
      // On success, remove from ActiveRebalancingTasks and drop the task object.
      // (On TaskStopped failure we leave state intact so the next lease holder resumes.)
      synchronized { tasks -= setId }
      if !stopped then
        removeCompleted(setId, taskStatePtr)

  private def removeCompleted(setId: StorageDeviceSetId, taskStatePtr: KeyValueObjectPointer): Unit =
    client.transactUntilSuccessful: tx =>
      given Transaction = tx
      for
        kvos <- client.read(statePointer)
        taskKvos <- client.read(taskStatePtr)
      yield
        val active = RebalancingServiceState.decodeActiveTasks(
                       kvos.contents(RebalancingServiceState.ActiveTasksKey).value.bytes)
        val newActive = active.filterNot(_._1 == setId)
        val reqs = KeyValueUpdate.KeyRevision(
                     RebalancingServiceState.ActiveTasksKey,
                     kvos.contents(RebalancingServiceState.ActiveTasksKey).revision) :: Nil
        val ops = org.aspen_ddp.aspen.common.objects.Insert(
                    RebalancingServiceState.ActiveTasksKey,
                    RebalancingServiceState.encodeActiveTasks(newActive)) :: Nil
        tx.update(statePointer, None, None, reqs, ops)
        // drop the (vestigial) task state object
        tx.setRefcount(taskStatePtr, taskKvos.refcount, taskKvos.refcount.decrement())
```

Notes:
- `scheduleNonConcurrentPollingTask(period)(fn: => Future[T])` (BackgroundTaskManager line 73) takes a `Future`-returning body — `reconcile()` returns `Future[Unit]`, matching.
- `tx.setRefcount(ptr, current, decremented)` mirrors `SimpleDurableServiceExecutor.unregisterService` (line 213). `refcount.decrement()` is the same call used there.
- `KeyValueUpdate.KeyRevision` and `Insert` follow the exact pattern in `transferStore` (`AspenClient.scala` lines 208-211) and `SimpleTaskExecutor` (`Insert`).
- Verify the precise import for `Transaction` used inside `client.transactUntilSuccessful` blocks by matching `AspenClient.scala` (`org.aspen_ddp.aspen.client.Transaction`).

- [ ] **Step 2: Register the service factory**

In `src/main/scala/org/aspen_ddp/aspen/server/TypeFactories.scala`, replace the empty list:

```scala
package org.aspen_ddp.aspen.server

import org.aspen_ddp.aspen.client.RegisteredTypeFactory
import org.aspen_ddp.aspen.common.rebalancing.RebalancingDurableService

object TypeFactories:

  val factories: List[RegisteredTypeFactory] = List(
    RebalancingDurableService
  )
```

- [ ] **Step 3: Compile**

Run: `sbt compile`
Expected: BUILD SUCCESS. Resolve any import-path issues surfaced by the compiler (all referenced symbols exist; adjust package paths to match sibling files if needed).

- [ ] **Step 4: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/common/rebalancing/RebalancingDurableService.scala \
        src/main/scala/org/aspen_ddp/aspen/server/TypeFactories.scala
git commit -m "Add RebalancingDurableService, factory, and rebalanceStorageDeviceSet"
```

---

# Phase 7 — In-memory transfer harness + integration test

### Task 10: In-memory transfer factory + multi-device TestNetwork support

**Files:**
- Create: `src/test/scala/org/aspen_ddp/aspen/InMemoryStoreTransferFactory.scala`
- Modify: `src/test/scala/org/aspen_ddp/aspen/TestNetwork.scala`

- [ ] **Step 1: In-memory transfer factory**

Create `src/test/scala/org/aspen_ddp/aspen/InMemoryStoreTransferFactory.scala`:

```scala
package org.aspen_ddp.aspen

import org.aspen_ddp.aspen.client.AspenClient
import org.aspen_ddp.aspen.common.{DataBuffer, HLCTimestamp}
import org.aspen_ddp.aspen.common.metadata.{HostId, StorageDeviceId}
import org.aspen_ddp.aspen.common.network.StoreTransferData
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.server.transfer.{StoreTransferFactory, StoreTransferIn, StoreTransferOut}

import java.nio.file.Path
import java.util.UUID
import scala.concurrent.{Future, Promise}

/** Test transfer mechanism: no filesystem, no `jar`. Since every store in the harness is a
 *  process-resident MapBackend on a single StoreManager, moving a store between devices needs
 *  no byte movement — only the metadata flip that StoreManager performs when TransferringIn
 *  completes. The out side sends a single empty StoreTransferData terminator to the
 *  destination; the in side completes on receipt of that empty buffer. */
class InMemoryStoreTransferFactory extends StoreTransferFactory:

  def createTransferIn(client: AspenClient,
                       storeId: StoreId,
                       storageDeviceId: StorageDeviceId,
                       devicePath: Path): StoreTransferIn =
    new StoreTransferIn:
      val storeId: StoreId = InMemoryStoreTransferFactory.this.synchronized(storeId)
      val transferUUID: UUID = UUID.randomUUID()
      private val p = Promise[Unit]()
      def complete: Future[Unit] = p.future
      def dataReceived(db: DataBuffer): Unit =
        if db.isEmpty then p.trySuccess(())

  def createTransferOut(client: AspenClient,
                        fromDevice: StorageDeviceId,
                        devicePath: Path,
                        storeId: StoreId,
                        toHost: HostId,
                        toDevice: StorageDeviceId,
                        timestamp: HLCTimestamp,
                        transferUUID: UUID): StoreTransferOut =
    new StoreTransferOut:
      val storeId: StoreId = storeId
      val transferUUID: UUID = transferUUID
      private val p = Promise[Unit]()
      def complete: Future[Unit] = p.future
      def abort(): Unit = ()
      // Immediately signal end-of-data to the destination host.
      client.sendHostMessage(StoreTransferData(toHost, client.clientId, transferUUID, DataBuffer(new Array[Byte](0))))
      p.trySuccess(())
```

Note: fix the `val storeId` shadowing — Scala 3 will complain about `val storeId: StoreId = storeId`. Rename the constructor param locally or assign from the outer: use `val storeId: StoreId = InMemoryStoreTransferFactory` is wrong. Simpler — capture into a local first:

```scala
  def createTransferIn(client: AspenClient, storeId: StoreId,
                       storageDeviceId: StorageDeviceId, devicePath: Path): StoreTransferIn =
    val sid = storeId
    new StoreTransferIn:
      val storeId: StoreId = sid
      ...
```

Apply the same `val sid = storeId` / `val tuid = transferUUID` capture pattern in both factory methods to avoid self-referential `val`s.

- [ ] **Step 2: Extend `TestNetwork` for a second device, the in-memory factory, and loopback host-message delivery**

In `src/test/scala/org/aspen_ddp/aspen/TestNetwork.scala`, make these edits:

(a) Add a second (empty) storage device. After `val storageDeviceId = StorageDeviceId.BootstrapStorageDeviceId` (line 104), add:

```scala
    // A second, empty device in the bootstrap set, so stores have a transfer destination.
    val secondDeviceId: StorageDeviceId = StorageDeviceId(new UUID(0, 100))
```

(b) After `bootstrapSD` is defined (line 120) — note `Bootstrap.initialize` only registers `bootstrapSD`; the second device's `StorageDeviceState` is written into the metadata tree after bootstrap. Add a helper that the integration test can call to create it (keeps `TestNetwork` changes minimal and avoids touching `Bootstrap`):

```scala
    def createSecondDevice(): Future[Unit] =
      given ExecutionContext = executionContext
      val sd = StorageDeviceState(
        secondDeviceId,
        bootstrapHost.hostId,
        0L, 1_000_000L,
        Map.empty,
        StorageDeviceSetId.BootstrapStorageDeviceSetId)
      val devicesTkvl = TieredKeyValueList(client,
        KVObjectRootManager(client, Radicle.StorageDevicesTreeKey, Radicle.pointer))
      client.transactUntilSuccessful: tx =>
        given Transaction = tx
        for
          pool <- client.getStoragePool(Radicle.poolId)
          allocator = new PoolObjectAllocator(client, pool)
          devPtr <- allocator.allocateKeyValueObject(
                      Map(StorageDeviceState.StateKey -> Value(sd.encode())))
          setPtr <- client.getStorageDeviceSetPointer(StorageDeviceSetId.BootstrapStorageDeviceSetId)
          setDos <- client.read(setPtr)
          _ <- devicesTkvl.set(Key(secondDeviceId.uuid), Value(devPtr.toArray))
        yield
          val curSet = StorageDeviceSetState(setDos)
          val updated = curSet.copy(memberDevices = curSet.memberDevices :+ secondDeviceId)
          tx.overwrite(setPtr, setDos.revision, DataBuffer(updated.toBytes))
```

This mirrors `Bootstrap.initialize`'s device indexing (`server/store/Bootstrap.scala` lines 125-127, 167-170) and set-membership (lines 133-141): allocate the device's KV state object, insert it into the `StorageDevicesTree` under `Key(secondDeviceId.uuid)`, and append `secondDeviceId` to the bootstrap set's `memberDevices` (a `DataObject` overwrite). Add these imports to `TestNetwork.scala`:

```scala
import org.aspen_ddp.aspen.client.internal.allocation.PoolObjectAllocator
import org.aspen_ddp.aspen.client.tkvl.{KVObjectRootManager, TieredKeyValueList}
import org.aspen_ddp.aspen.common.DataBuffer
import org.aspen_ddp.aspen.common.metadata.StorageDeviceSetState
import org.aspen_ddp.aspen.common.objects.{Key, Value}
```

(`client` here is the `TClient`; `Transaction` is `org.aspen_ddp.aspen.client.Transaction`, already available via the existing `client` imports — add `import org.aspen_ddp.aspen.client.Transaction` if the compiler asks.)

(c) Inject the in-memory factory into `StoreManager`. Change the `smgr` construction (lines 167-180) to pass the factory as the final argument:

```scala
    val smgr = new StoreManager(
      client,
      HostId.BootstrapHostId,
      new UUID(0, 0),
      Path.of("/"),
      executionContext,
      objectCacheFactory,
      this,
      BackgroundTaskManager.NoBackgroundTaskManager,
      TestCRL,
      FinalizerFactory,
      TransactionDriver.noErrorRecoveryFactory,
      Duration(5, SECONDS),
      Duration(60, SECONDS),
      new InMemoryStoreTransferFactory())
```

(d) Loopback-deliver transfer-related host messages to `smgr`. The client messenger's `sendHostMessage` (lines 158-159) currently only captures. Change it to also route transfer messages to the store manager, while still capturing everything (so existing capture-based assertions keep working):

```scala
      def sendHostMessage(msg: HostMessage): Unit = TestNetwork.this.synchronized:
        capturedHostMessages = capturedHostMessages :+ msg
        msg match
          case _: StartStoreTransfer | _: StoreTransferData | _: CheckStorageDevice | _: ServiceMessage =>
            smgr.receiveHostMessage(msg)
            handleEvents()
          case _ => ()
```

Add the imports for those message types to `TestNetwork.scala`:

```scala
import org.aspen_ddp.aspen.common.network.{CheckStorageDevice, ServiceMessage, StartStoreTransfer, StoreTransferData}
```

(The existing import at line 12 already brings in `HostMessage`; extend it or add this line.)

> Implementer caveat: routing `ServiceMessage` through `smgr.receiveHostMessage` requires the store manager's `serviceExecutorPromise` to be completed (it dispatches to the executor at `StoreManager.scala` line 778). The integration test wires the executor (Task 11). If a test does not install an executor, `ServiceMessage` delivery is a harmless no-op (the promise callback never fires). Verify `serviceExecutorPromise` is exposed/settable — search `StoreManager.scala` for `serviceExecutorPromise`; if there is no setter, add a small `def setServiceExecutor(e: DurableServiceExecutor): Unit` that completes the promise, and call it from the test.

- [ ] **Step 3: Compile the test sources**

Run: `sbt Test/compile`
Expected: BUILD SUCCESS.

- [ ] **Step 4: Commit**

```bash
git add src/test/scala/org/aspen_ddp/aspen/InMemoryStoreTransferFactory.scala \
        src/test/scala/org/aspen_ddp/aspen/TestNetwork.scala
git commit -m "Add in-memory store transfer factory and multi-device TestNetwork support"
```

---

### Task 11: End-to-end integration test

**Files:**
- Test: `src/test/scala/org/aspen_ddp/aspen/common/rebalancing/RebalancingServiceSuite.scala` (new)

- [ ] **Step 1: Write the integration test**

Create `src/test/scala/org/aspen_ddp/aspen/common/rebalancing/RebalancingServiceSuite.scala`:

```scala
package org.aspen_ddp.aspen.common.rebalancing

import org.aspen_ddp.aspen.IntegrationTestSuite
import org.aspen_ddp.aspen.client.RegisteredTypeFactory
import org.aspen_ddp.aspen.common.metadata.{HostId, StorageDeviceSetId, StorageDeviceState}
import org.aspen_ddp.aspen.compute.impl.SimpleDurableServiceExecutor
import org.aspen_ddp.aspen.common.util.BackgroundTaskManager

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.{Duration, MILLISECONDS}

class RebalancingServiceSuite extends IntegrationTestSuite:

  val testHostId: HostId = HostId.BootstrapHostId

  override def userTypeFactories: List[RegisteredTypeFactory] = List(RebalancingDurableService)

  private def makeExecutor(): SimpleDurableServiceExecutor =
    new SimpleDurableServiceExecutor(
      client, testHostId, client.backgroundTaskManager,
      leaseDuration        = Duration(400, MILLISECONDS),
      renewalInterval      = Duration(80, MILLISECONDS),
      minScanInterval      = Duration(40, MILLISECONDS),
      maxScanInterval      = Duration(80, MILLISECONDS),
      claimDelayPerService = Duration(0, MILLISECONDS))

  atest("rebalanceStorageDeviceSet drains pendingTransfers and empties ActiveRebalancingTasks"):
    given ExecutionContext = executionContext
    val setId = StorageDeviceSetId.BootstrapStorageDeviceSetId
    val exec = makeExecutor()
    net.setServiceExecutor(exec)  // enable ServiceMessage loopback delivery (see Task 10 caveat)
    for
      _ <- RebalancingDurableService.register(exec)
      _ <- net.createSecondDevice()
      // The bootstrap pool has 3 stores all on the bootstrap device; a level-0 set with a
      // second empty device produces a non-empty balance/reliability plan.
      _ <- RebalancingDurableService.rebalanceStorageDeviceSet(client, setId)
      _ <- pollUntil(Duration(5000, MILLISECONDS)):
             client.getStorageDeviceSetState(setId).map(_.pendingTransfers.isEmpty)
      finalSet <- client.getStorageDeviceSetState(setId)
      statePtr <- serviceStatePointer()
      stateKvos <- client.read(statePtr)
      active = RebalancingServiceState.decodeActiveTasks(
                 stateKvos.contents(RebalancingServiceState.ActiveTasksKey).value.bytes)
    yield
      exec.shutdown()
      finalSet.pendingTransfers shouldBe empty
      active shouldBe empty

  /** Read the service state pointer via the services TKVL (mirrors the companion). */
  private def serviceStatePointer() =
    given ExecutionContext = executionContext
    import org.aspen_ddp.aspen.client.tkvl.{KVObjectRootManager, TieredKeyValueList}
    import org.aspen_ddp.aspen.common.Radicle
    import org.aspen_ddp.aspen.common.objects.Key
    import org.aspen_ddp.aspen.compute.ServiceEntry
    val tkvl = TieredKeyValueList(client,
      KVObjectRootManager(client, Radicle.ServicesTreeKey, Radicle.pointer))
    tkvl.get(Key(RebalancingDurableService.ServiceUUID)).map: opt =>
      ServiceEntry.decode(opt.get.value.bytes).statePointer

  /** Poll `check` every 100ms until true or timeout, driving the harness event loop. */
  private def pollUntil(timeout: Duration)(check: => Future[Boolean]): Future[Unit] =
    given ExecutionContext = executionContext
    val deadline = System.nanoTime() + timeout.toNanos
    def loop(): Future[Unit] =
      check.flatMap: done =>
        if done then Future.unit
        else if System.nanoTime() > deadline then
          Future.failed(new AssertionError("pollUntil timed out"))
        else
          val p = scala.concurrent.Promise[Unit]()
          client.backgroundTaskManager.schedule(Duration(100, MILLISECONDS)):
            p.success(())
          p.future.flatMap(_ => loop())
    loop()
```

> Implementer notes:
> - `net.setServiceExecutor(exec)` refers to the setter you may need to add in Task 10 Step 2(d). If `StoreManager` already exposes a way to install the executor, use it instead.
> - `System.nanoTime()` is allowed in test code (only workflow *scripts* forbid it).
> - If the bootstrap set + one empty device yields an *empty* plan (e.g. the balance thresholds aren't crossed by three tiny bootstrap objects), force a non-empty plan by making the second device large and pre-populating device usage, or assert against a hand-built plan by inserting a `pendingTransfers` list directly and constructing the task via the service. The essential assertion is: given a non-empty `pendingTransfers`, the service drives every transfer to `Active` on its destination and empties both `pendingTransfers` and `ActiveRebalancingTasks`.

- [ ] **Step 2: Run it to verify it fails, then passes**

Run: `sbt 'testOnly *RebalancingServiceSuite'`
Expected first: FAIL (missing `net.setServiceExecutor`/`net.createSecondDevice` if not yet complete, or a real assertion). Iterate on Task 10 helpers until this suite passes. Then:
Expected: PASS.

- [ ] **Step 3: Run the full suite**

Run: `sbt test`
Expected: PASS (no regressions from the `StoreManager`/`TestNetwork` changes).

- [ ] **Step 4: Commit**

```bash
git add src/test/scala/org/aspen_ddp/aspen/common/rebalancing/RebalancingServiceSuite.scala
git commit -m "Add RebalancingService integration test (in-memory transfers)"
```

---

# Phase 8 — `TransferComplete` emission from `StoreManager`

### Task 12: Emit `TransferComplete` to the service after a transfer lands

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/server/StoreManager.scala` (`updateStateForTransferredStore`, success block lines 338-344)

- [ ] **Step 1: Send the service message on success**

In `updateStateForTransferredStore`, the `tx.result.foreach` success block (lines 338-344) already sends a `CheckStorageDevice` to the source host. Add, immediately after that `sendHostMessage(msg)` call, a best-effort `TransferComplete` to the rebalancing service. `toDev` (the destination `StorageDeviceState`) is already in scope, so its `storageDeviceSet` gives the setId:

```scala
            tx.result.foreach: _ =>
              val msg = CheckStorageDevice(
                fromDev.hostId,
                client.clientId,
                fromDev.storageDeviceId
              )
              client.sendHostMessage(msg)

              // Best-effort wake-up for the rebalancing service (harmless for non-rebalance transfers).
              client.sendServiceMessage(
                RebalancingDurableService.ServiceUUID,
                RebalancingMessage.encode(RebalancingTransferComplete(
                  toDev.storageDeviceSet, storeId, fromDeviceId, toDeviceid)))
```

Add imports to `StoreManager.scala`:

```scala
import org.aspen_ddp.aspen.common.rebalancing.{RebalancingDurableService, RebalancingMessage, TransferComplete as RebalancingTransferComplete}
```

(The `as` rename avoids any clash with other `TransferComplete`-named symbols; if none exists, import `TransferComplete` directly and use it.)

- [ ] **Step 2: Compile and run the suite**

Run: `sbt test`
Expected: PASS. The integration test from Task 11 now also exercises the `TransferComplete` wake-up path (it accelerates completion; the poll remains the correctness fallback).

- [ ] **Step 3: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/server/StoreManager.scala
git commit -m "Emit TransferComplete service message after a store transfer completes"
```

---

# Phase 9 — CLI `rebalance` subcommand

### Task 13: Add `rebalance <setId>` to the cmdline

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/cmdline/Main.scala` (`Args` case class lines 55-67; parser after the `transfer-store` block lines 204-235; dispatch lines 250-257; add a handler mirroring `transfer_store` lines 799-860)

- [ ] **Step 1: Add an `Args` field**

In `Main.scala`, add a field to the `Args` case class (lines 55-67), e.g.:

```scala
    setId: String = "",
```

- [ ] **Step 2: Declare the subcommand**

Inside the `parser` in `main`, after the `transfer-store` `cmd(...)` block (ends line 235), add:

```scala
      cmd("rebalance").text("Rebalances a level-0 storage device set").
        action((_, c) => c.copy(mode = "rebalance")).
        children(
          arg[File]("<bootstrap-config-file>").text("Bootstrap Configuration File").
            action((x, c) => c.copy(bootstrapConfigFile = x)).
            validate(x => if (x.exists()) success else failure(s"Config file does not exist: $x")),

          arg[String]("<storage-device-set-id>").text("UUID of the storage device set to rebalance").
            action((x, c) => c.copy(setId = x)).
            validate { x =>
              try
                UUID.fromString(x)
                success
              catch
                case _: Throwable => failure("Storage device set id must be a valid UUID")
            },
        )
```

- [ ] **Step 3: Add dispatch**

In the `cfg.mode match` block (lines 250-257), add:

```scala
            case "rebalance" => rebalance(bootstrapConfigPath, cfg.setId)
```

- [ ] **Step 4: Add the handler**

Mirroring `transfer_store` (lines 799-860), add a `rebalance` method. Because `rebalanceStorageDeviceSet` enrolls a durable job and returns, block until the enrollment future completes so the process doesn't exit prematurely:

```scala
  def rebalance(bootstrapConfigFile: os.Path, setIdStr: String): Unit =
    configureLogging()

    val (client, network, radicle) = createAmoebaClient(bootstrapConfigFile)
    network.startIoThread(client)

    given ExecutionContext = client.clientContext

    val setId = StorageDeviceSetId(UUID.fromString(setIdStr))

    val f = RebalancingDurableService.rebalanceStorageDeviceSet(client, setId)
    f.onComplete:
      case scala.util.Success(_) =>
        println(s"Rebalance enrolled for storage device set $setIdStr")
      case scala.util.Failure(err) =>
        println(s"Rebalance failed to enroll: ${err.getMessage}")

    scala.concurrent.Await.ready(f, scala.concurrent.duration.Duration(30, scala.concurrent.duration.SECONDS))
```

Add imports to `Main.scala` as needed:

```scala
import org.aspen_ddp.aspen.common.metadata.StorageDeviceSetId
import org.aspen_ddp.aspen.common.rebalancing.RebalancingDurableService
import scala.concurrent.ExecutionContext
```

(`StorageDeviceId`, `StoreId`, `UUID`, `os.Path`, and `createAmoebaClient` are already imported/available in `Main.scala` per the `transfer_store` handler.)

- [ ] **Step 5: Compile**

Run: `sbt compile`
Expected: BUILD SUCCESS.

- [ ] **Step 6: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/cmdline/Main.scala
git commit -m "Add rebalance <setId> cmdline subcommand"
```

---

# Phase 10 — Service registration at host startup

### Task 14: Register the rebalancing service where the executor is created

**Files:**
- Modify: the host startup path that constructs a `SimpleDurableServiceExecutor` (search for it)

- [ ] **Step 1: Find where the executor is created in the running host**

Run: `grep -rn "new SimpleDurableServiceExecutor\|SimpleDurableServiceExecutor(" src/main/scala`
Expected: one or more construction sites in the host/server startup path (e.g. under `cmdline` `host(...)` or `server`).

- [ ] **Step 2: Register the service once after constructing the executor**

At the host startup site, after the `SimpleDurableServiceExecutor` is constructed and the client is ready, add:

```scala
    RebalancingDurableService.register(executor)
```

(where `executor` is the constructed `SimpleDurableServiceExecutor`). `register` is idempotent, so calling it on every host start is safe. Add the import:

```scala
import org.aspen_ddp.aspen.common.rebalancing.RebalancingDurableService
```

> If no running-host executor construction site exists yet (the durable-service framework may not be wired into the live host path), skip the code change and instead note this as a follow-up: the service must be registered wherever the executor is eventually wired in. The integration test (Task 11) registers it explicitly, so the feature is fully validated regardless.

- [ ] **Step 3: Compile and run the suite**

Run: `sbt compile && sbt test`
Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add -A
git commit -m "Register RebalancingDurableService at host startup"
```

---

## Final verification

- [ ] Run the full suite: `sbt test` → all green.
- [ ] Confirm the spec's requirements are all covered (see mapping below).
- [ ] Update `TODO.txt`: the "Rebalancing" active-flow item is implemented; leave the passive background rebalancer, set expansion, and reassignment items.

---

## Spec coverage map

| Spec requirement | Task(s) |
|---|---|
| `pendingTransfers` on `StorageDeviceSetState` (proto, codec, field, default-empty) | 1, 2 |
| `ActiveRebalancingTasks` on service state (encode/decode) | 5 |
| `RebalancingDurableService` (lease lifecycle, resume, poll, receiveMessage, shutdown) | 9 |
| Service directly manages tasks (no SimpleTaskExecutor) | 9 |
| `SetRebalanceDurableTask` (serial, in-flight detect, skip/continue/revisit, remove-on-complete) | 8 |
| `rebalanceStorageDeviceSet` entry point (idempotent, single tx, allocate task object) | 9 |
| Service state discovery via services TKVL | 9 |
| `NewSetRebalanceInitiated` / `TransferComplete` payloads | 6, 12 |
| `offlineHosts()` stub | 3 |
| Write-threshold safety predicate | 4 |
| Pluggable transfer mechanism (`StoreTransferFactory`, Filesystem impl) | 7 |
| In-memory transfer impl + 2nd device + loopback delivery | 10 |
| `TransferComplete` emission from `StoreManager` | 12 |
| CLI `rebalance <setId>` | 13 |
| Service registration | 14 |
| Unit tests: codec round-trips, safety predicate | 2, 5, 6, 4 |
| Integration test: drain + resume | 11 |
