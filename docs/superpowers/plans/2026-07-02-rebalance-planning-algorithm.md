# Rebalance Planning Algorithm Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement a pure, stable rebalance planning function that consumes a `State.PlanState` and returns a priority-ordered `List[Plan.Transfer]` (reliability > availability > balanced usage).

**Architecture:** A greedy, priority-phased single pass over a mutable working copy of the state. Three phases run in order — reliability repair (same pool per device), availability repair (same pool per host), balance (device fill ratio) — each observing the effects of the previous. A lower-priority phase never sacrifices a higher-priority goal. Stability (no flapping) is structural: repair phases only move when they strictly reduce a max co-location count, and the balance phase uses spread hysteresis plus a no-overshoot rule.

**Tech Stack:** Scala 3 (quiet syntax), ScalaTest (`AnyFunSuite` + `Matchers`), `scala.collection.mutable`.

**Spec:** `docs/superpowers/specs/2026-07-02-rebalance-planning-algorithm-design.md`

---

## File Structure

- `src/main/scala/org/aspen_ddp/aspen/common/rebalancing/State.scala` — **modify**: add `hostId` to `State.Device`; populate it in `getStateForRebalancePlanning`.
- `src/main/scala/org/aspen_ddp/aspen/common/rebalancing/Plan.scala` — **create**: `Transfer`, `Config`, `computePlan`, and the private `Working` model + phase functions.
- `src/test/scala/org/aspen_ddp/aspen/common/rebalancing/RebalancePlanSuite.scala` — **create**: builders + all tests.

The algorithm reads only `state.devices`; `state.pools` is unused (see spec "Note on IDA"), so tests build pools as `Map.empty`.

---

## Task 1: Add `hostId` to `State.Device`

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/common/rebalancing/State.scala:23-26` (Device definition) and `:85` (construction)

This is a pure data-shape change. There is no unit test for the (client-dependent) state gatherer, so this task is verified by compilation.

- [ ] **Step 1: Add the `hostId` field to `Device`**

In `State.scala`, replace the `Device` case class (currently lines 23-26):

```scala
  case class Device(deviceId: StorageDeviceId,
                    hostId: HostId,
                    currentUsage: Long,
                    totalSize: Long,
                    stores: Map[StoreId, Store])
```

- [ ] **Step 2: Add the `HostId` import**

At the top of `State.scala`, the metadata import currently reads:

```scala
import org.aspen_ddp.aspen.common.metadata.{StorageDeviceId, StorageDeviceSetState, StorageDeviceState, StoragePoolState}
```

Replace it with (adds `HostId`):

```scala
import org.aspen_ddp.aspen.common.metadata.{HostId, StorageDeviceId, StorageDeviceSetState, StorageDeviceState, StoragePoolState}
```

- [ ] **Step 3: Populate `hostId` when constructing each `Device`**

In `getStateForRebalancePlanning`, the device map construction (currently line 85) reads:

```scala
            ds.storageDeviceId -> Device(ds.storageDeviceId, ds.currentUsage, ds.totalSize, itsStores)
```

Replace it with:

```scala
            ds.storageDeviceId -> Device(ds.storageDeviceId, ds.hostId, ds.currentUsage, ds.totalSize, itsStores)
```

- [ ] **Step 4: Compile**

Run: `sbt compile`
Expected: SUCCESS (no errors).

- [ ] **Step 5: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/common/rebalancing/State.scala
git commit -m "Add hostId to rebalancing State.Device"
```

---

## Task 2: Scaffold `Plan` (types, Working model, no-op phases) + empty-plan tests

**Files:**
- Create: `src/main/scala/org/aspen_ddp/aspen/common/rebalancing/Plan.scala`
- Create: `src/test/scala/org/aspen_ddp/aspen/common/rebalancing/RebalancePlanSuite.scala`

- [ ] **Step 1: Write the failing tests (builders + empty cases)**

Create `src/test/scala/org/aspen_ddp/aspen/common/rebalancing/RebalancePlanSuite.scala`:

```scala
package org.aspen_ddp.aspen.common.rebalancing

import java.util.UUID

import org.aspen_ddp.aspen.common.metadata.{HostId, StorageDeviceId, StorageDeviceState}
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

object RebalancePlanSuite:

  import State.*

  def poolId(n: Int): PoolId = PoolId(new UUID(0L, n.toLong))

  def devId(n: Int): StorageDeviceId = StorageDeviceId(new UUID(1L, n.toLong))

  def hstId(n: Int): HostId = HostId(new UUID(2L, n.toLong))

  def sid(pool: Int, idx: Int): StoreId = StoreId(poolId(pool), idx.toByte)

  def store(pool: Int, idx: Int, size: Long,
            status: StorageDeviceState.StoreStatus = StorageDeviceState.StoreStatus.Active): Store =
    Store(sid(pool, idx), size, status)

  /** Build a Device: numeric device id, numeric host id, usage/total bytes, and its stores. */
  def device(id: Int, host: Int, usage: Long, total: Long, stores: Store*): Device =
    Device(devId(id), hstId(host), usage, total, stores.map(s => s.storeId -> s).toMap)

  /** PlanState from devices; pools are unused by the algorithm so left empty. */
  def planState(devices: Device*): PlanningState =
    PlanningState(devices.map(d => d.deviceId -> d).toMap, Map.empty)

  /** Apply a plan to a PlanState, producing the post-transfer state (for stability checks). */
  def applyPlan(state: PlanningState, plan: List[Plan.Transfer]): PlanningState =
    var devs = state.devices
    for t <- plan do
      val from = devs(t.fromDevice)
      val to = devs(t.toDevice)
      val st = from.stores(t.storeId)
      val nf = from.copy(currentUsage = from.currentUsage - st.currentSize,
        stores = from.stores - t.storeId)
      val nt = to.copy(currentUsage = to.currentUsage + st.currentSize,
        stores = to.stores + (t.storeId -> st))
      devs = devs + (nf.deviceId -> nf) + (nt.deviceId -> nt)
    state.copy(devices = devs)

class RebalancePlanSuite extends AnyFunSuite with Matchers:

  import RebalancePlanSuite.*

  test("empty state yields empty plan"):
    Plan.computePlan(planState()) shouldBe Nil

  test("single device yields empty plan"):
    Plan.computePlan(planState(device(1, 1, 10, 100, store(1, 0, 10)))) shouldBe Nil
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `sbt 'testOnly *RebalancePlanSuite'`
Expected: FAIL — compilation error, `Plan` not found.

- [ ] **Step 3: Create `Plan.scala` with types, Working model, and no-op phases**

Create `src/main/scala/org/aspen_ddp/aspen/common/rebalancing/Plan.scala`:

```scala
package org.aspen_ddp.aspen.common.rebalancing

import org.aspen_ddp.aspen.common.metadata.{HostId, StorageDeviceId, StorageDeviceState}
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId

import scala.collection.mutable

/** Pure rebalance planning. See docs/superpowers/specs/2026-07-02-rebalance-planning-algorithm-design.md */
object Plan:

  /** A single planned store transfer. */
  case class Transfer(storeId: StoreId, fromDevice: StorageDeviceId, toDevice: StorageDeviceId)

  /** Tuning knobs for the balance phase.
   *
   * @param balanceSpreadThreshold begin/stop balancing when (max fill ratio - min fill ratio) crosses this
   * @param minBalanceMoveGain     a balance move must reduce spread by at least this to be emitted
   */
  case class Config(balanceSpreadThreshold: Double = 0.05,
                    minBalanceMoveGain: Double = 0.01)

  /** Compute a stable, priority-ordered transfer plan for a level-0 device set. Pure: identical
   * input always yields identical output, and re-running on the applied plan yields Nil. */
  def computePlan(state: State.PlanningState, config: Config = Config()): List[Transfer] =
    val w = new Working(state)
    reliabilityRepair(w)
    availabilityRepair(w)
    balance(w, config)
    w.transfers.toList

  /** Mutable working copy of placement + usage. PlanState itself is never mutated. */
  private class Working(state: State.PlanningState):
    val deviceTotal: Map[StorageDeviceId, Long] =
      state.devices.map((id, d) => id -> d.totalSize)
    val deviceHost: Map[StorageDeviceId, HostId] =
      state.devices.map((id, d) => id -> d.hostId)
    /** Devices in a deterministic (uuid string) order. */
    val deviceIds: Vector[StorageDeviceId] =
      state.devices.keys.toVector.sortBy(_.uuid.toString)

    val storeSize: Map[StoreId, Long] =
      state.devices.values.flatMap(_.stores.values).map(s => s.storeId -> s.currentSize).toMap
    /** Stores eligible to be selected as a move source (Active only). */
    val activeStores: Set[StoreId] =
      state.devices.values.flatMap(_.stores.values)
        .filter(_.status == StorageDeviceState.StoreStatus.Active)
        .map(_.storeId).toSet

    val location: mutable.Map[StoreId, StorageDeviceId] =
      mutable.Map.from(
        state.devices.values.flatMap(d => d.stores.keys.map(sid => sid -> d.deviceId)))
    val usage: mutable.Map[StorageDeviceId, Long] =
      mutable.Map.from(state.devices.map((id, d) => id -> d.currentUsage))

    val pinned: mutable.Set[StoreId] = mutable.Set.empty
    val transfers: mutable.ArrayBuffer[Transfer] = mutable.ArrayBuffer.empty

    def storesOn(dev: StorageDeviceId): Seq[StoreId] =
      location.collect { case (s, d) if d == dev => s }.toSeq

    def samePoolOnDevice(dev: StorageDeviceId, pool: PoolId): Int =
      location.count { case (s, d) => d == dev && s.poolId == pool }

    def samePoolOnHost(host: HostId, pool: PoolId): Int =
      location.count { case (s, d) => deviceHost(d) == host && s.poolId == pool }

    def fillRatio(dev: StorageDeviceId): Double =
      val tot = deviceTotal(dev)
      if tot <= 0L then 1.0 else usage(dev).toDouble / tot.toDouble

    def fits(dev: StorageDeviceId, s: StoreId): Boolean =
      usage(dev) + storeSize(s) <= deviceTotal(dev)

    /** A store may be selected as a move source if it is Active and not already moved this plan. */
    def movable(s: StoreId): Boolean = activeStores.contains(s) && !pinned.contains(s)

    def move(s: StoreId, to: StorageDeviceId): Unit =
      val from = location(s)
      usage(from) = usage(from) - storeSize(s)
      usage(to) = usage(to) + storeSize(s)
      location(s) = to
      pinned += s
      transfers += Transfer(s, from, to)

  // Phases — filled in by later tasks.
  private def reliabilityRepair(w: Working): Unit = ()

  private def availabilityRepair(w: Working): Unit = ()

  private def balance(w: Working, config: Config): Unit = ()
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `sbt 'testOnly *RebalancePlanSuite'`
Expected: PASS (2 tests).

- [ ] **Step 5: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/common/rebalancing/Plan.scala src/test/scala/org/aspen_ddp/aspen/common/rebalancing/RebalancePlanSuite.scala
git commit -m "Scaffold rebalance Plan: types, working model, empty-plan tests"
```

---

## Task 3: Phase 1 — Reliability repair

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/common/rebalancing/Plan.scala`
- Modify: `src/test/scala/org/aspen_ddp/aspen/common/rebalancing/RebalancePlanSuite.scala`

Reliability rule: minimize the maximum same-pool-per-device count. Move a movable store off an over-loaded device to the best-ranked device where `destDeviceCount + 1 < sourceDeviceCount` (strictly reduces the max of the two). Where no such device exists (too few devices), no move is made — this yields the minimal achievable co-location and is stable.

- [ ] **Step 1: Write the failing tests**

Append to `class RebalancePlanSuite`:

```scala
  test("reliability: splits two same-pool stores off one device"):
    val st = planState(
      device(1, 1, 20, 100, store(1, 0, 10), store(1, 1, 10)),
      device(2, 2, 0, 100))
    val plan = Plan.computePlan(st)
    plan.size shouldBe 1
    // candidates sorted by poolIndex, so the idx-0 store moves
    plan.head.storeId shouldBe sid(1, 0)
    plan.head.fromDevice shouldBe devId(1)
    plan.head.toDevice shouldBe devId(2)

  test("reliability: not enough devices converges to minimal co-location (one move)"):
    // three pool-1 stores on dev1, only one other device available
    val st = planState(
      device(1, 1, 30, 100, store(1, 0, 10), store(1, 1, 10), store(1, 2, 10)),
      device(2, 2, 0, 100))
    val plan = Plan.computePlan(st)
    // dev1 3->2, dev2 0->1: max co-location becomes 2 (unavoidable); a second move (2->2) is rejected
    plan.size shouldBe 1
    plan.head.toDevice shouldBe devId(2)

  test("reliability: immovable (non-Active) stores are not selected as sources"):
    val st = planState(
      device(1, 1, 20, 100,
        store(1, 0, 10, StorageDeviceState.StoreStatus.Rebuilding),
        store(1, 1, 10)),
      device(2, 2, 0, 100))
    val plan = Plan.computePlan(st)
    plan.size shouldBe 1
    plan.head.storeId shouldBe sid(1, 1)   // only the Active store can move
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `sbt 'testOnly *RebalancePlanSuite'`
Expected: FAIL — reliability tests fail (no-op phase returns empty plan).

- [ ] **Step 3: Implement `reliabilityRepair` and the shared destination ranking**

In `Plan.scala`, replace `private def reliabilityRepair(w: Working): Unit = ()` with:

```scala
  /** Devices (other than `from`) that physically fit `s`, ranked deterministically:
   *  fewest same-pool on device, then on host, then lowest fill ratio, then uuid. */
  private def rankedDestinations(w: Working, s: StoreId, from: StorageDeviceId): Seq[StorageDeviceId] =
    val pool = s.poolId
    w.deviceIds
      .filter(d => d != from && w.fits(d, s))
      .sortBy(d => (w.samePoolOnDevice(d, pool),
                    w.samePoolOnHost(w.deviceHost(d), pool),
                    w.fillRatio(d),
                    d.uuid.toString))

  private def reliabilityRepair(w: Working): Unit =
    var progress = true
    while progress do
      progress = false
      for dev <- w.deviceIds do
        val poolCounts: Seq[(PoolId, Int)] =
          w.storesOn(dev).groupBy(_.poolId).map((p, ss) => p -> ss.size)
            .toSeq.sortBy(_._1.uuid.toString)
        for (pool, count) <- poolCounts if count >= 2 do
          val candidate: Option[StoreId] =
            w.storesOn(dev).filter(s => s.poolId == pool && w.movable(s))
              .sortBy(_.poolIndex).headOption
          candidate.foreach { s =>
            rankedDestinations(w, s, dev).headOption.foreach { dest =>
              // accept only if it strictly reduces the max co-location of the two devices
              if w.samePoolOnDevice(dest, pool) + 1 < count then
                w.move(s, dest)
                progress = true
            }
          }
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `sbt 'testOnly *RebalancePlanSuite'`
Expected: PASS (5 tests).

- [ ] **Step 5: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/common/rebalancing/Plan.scala src/test/scala/org/aspen_ddp/aspen/common/rebalancing/RebalancePlanSuite.scala
git commit -m "Implement reliability repair phase for rebalance planning"
```

---

## Task 4: Phase 2 — Availability repair

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/common/rebalancing/Plan.scala`
- Modify: `src/test/scala/org/aspen_ddp/aspen/common/rebalancing/RebalancePlanSuite.scala`

Availability rule: minimize the maximum same-pool-per-host count. A store may only move to a device on a **different host** that holds **0** stores of the pool (so it never creates a device-level co-location — reliability is preserved). Accept only if `destHostCount + 1 < sourceHostCount`.

- [ ] **Step 1: Write the failing tests**

Append to `class RebalancePlanSuite`:

```scala
  test("availability: spreads same-pool stores across hosts"):
    // host 1 has two devices, each with one pool-1 store; host 2 has an empty device
    val st = planState(
      device(1, 1, 10, 100, store(1, 0, 10)),
      device(2, 1, 10, 100, store(1, 1, 10)),
      device(3, 2, 0, 100))
    val plan = Plan.computePlan(st)
    plan.size shouldBe 1
    plan.head.toDevice shouldBe devId(3)

  test("availability: no move when it would create a device co-location (reliability preserved)"):
    // host 1 has two pool-1 stores; the only other host's device already holds pool 1
    val st = planState(
      device(1, 1, 10, 100, store(1, 0, 10)),
      device(2, 1, 10, 100, store(1, 1, 10)),
      device(3, 2, 10, 100, store(1, 2, 10)))
    val plan = Plan.computePlan(st)
    plan shouldBe Nil
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `sbt 'testOnly *RebalancePlanSuite'`
Expected: FAIL — the "spreads across hosts" test fails (no-op availability phase).

- [ ] **Step 3: Implement `availabilityRepair`**

In `Plan.scala`, replace `private def availabilityRepair(w: Working): Unit = ()` with:

```scala
  private def availabilityRepair(w: Working): Unit =
    var progress = true
    while progress do
      progress = false
      val hosts = w.deviceIds.map(w.deviceHost).distinct.sortBy(_.uuid.toString)
      for host <- hosts do
        val hostDevices = w.deviceIds.filter(d => w.deviceHost(d) == host)
        val poolCounts: Seq[(PoolId, Int)] =
          hostDevices.flatMap(w.storesOn).groupBy(_.poolId).map((p, ss) => p -> ss.size)
            .toSeq.sortBy(_._1.uuid.toString)
        for (pool, count) <- poolCounts if count >= 2 do
          val candidate: Option[StoreId] =
            hostDevices.flatMap(w.storesOn)
              .filter(s => s.poolId == pool && w.movable(s))
              .sortBy(_.poolIndex).headOption
          candidate.foreach { s =>
            // different host, physically fits, and no device-level co-location (reliability safe)
            val dest: Option[StorageDeviceId] =
              w.deviceIds
                .filter(d => w.deviceHost(d) != host && w.fits(d, s) && w.samePoolOnDevice(d, pool) == 0)
                .sortBy(d => (w.samePoolOnHost(w.deviceHost(d), pool),
                              w.fillRatio(d),
                              d.uuid.toString))
                .headOption
            dest.foreach { d =>
              if w.samePoolOnHost(w.deviceHost(d), pool) + 1 < count then
                w.move(s, d)
                progress = true
            }
          }
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `sbt 'testOnly *RebalancePlanSuite'`
Expected: PASS (7 tests).

- [ ] **Step 5: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/common/rebalancing/Plan.scala src/test/scala/org/aspen_ddp/aspen/common/rebalancing/RebalancePlanSuite.scala
git commit -m "Implement availability repair phase for rebalance planning"
```

---

## Task 5: Phase 3 — Balance

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/common/rebalancing/Plan.scala`
- Modify: `src/test/scala/org/aspen_ddp/aspen/common/rebalancing/RebalancePlanSuite.scala`

Balance rule: while `spread = maxFill - minFill > balanceSpreadThreshold`, move the largest movable store from the most-full device to the least-full, subject to: destination fits; destination has 0 same-pool stores (reliability safe); destination host has 0 same-pool stores unless it is the source's own host (availability safe); no-overshoot (`destFill' <= sourceFill'`); and the move reduces spread by at least `minBalanceMoveGain`. Stop when no such move exists.

- [ ] **Step 1: Write the failing tests**

Append to `class RebalancePlanSuite`:

```scala
  test("balance: migrates a store from a full device to an empty one"):
    val st = planState(
      device(1, 1, 80, 100, store(1, 0, 40)),
      device(2, 2, 0, 100))
    val plan = Plan.computePlan(st)
    plan.size shouldBe 1
    plan.head.storeId shouldBe sid(1, 0)
    plan.head.fromDevice shouldBe devId(1)
    plan.head.toDevice shouldBe devId(2)

  test("balance: no move when within threshold"):
    val st = planState(
      device(1, 1, 50, 100, store(1, 0, 10)),
      device(2, 2, 48, 100, store(2, 0, 10)))
    Plan.computePlan(st) shouldBe Nil

  test("balance: rejects a move that would overshoot"):
    // the only movable store is so large that moving it makes the sink fuller than the source
    val st = planState(
      device(1, 1, 60, 100, store(1, 0, 50)),
      device(2, 2, 0, 100))
    Plan.computePlan(st) shouldBe Nil
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `sbt 'testOnly *RebalancePlanSuite'`
Expected: FAIL — "migrates a store" fails (no-op balance phase).

- [ ] **Step 3: Implement `balance`**

In `Plan.scala`, replace `private def balance(w: Working, config: Config): Unit = ()` with:

```scala
  private def balance(w: Working, config: Config): Unit =
    if w.deviceIds.size < 2 then return

    def spread(): Double =
      val ratios = w.deviceIds.map(w.fillRatio)
      ratios.max - ratios.min

    /** Spread that would result from moving `s` from `from` to `to`, without mutating state. */
    def spreadIfMoved(s: StoreId, from: StorageDeviceId, to: StorageDeviceId): Double =
      val size = w.storeSize(s)
      val ratios = w.deviceIds.map { d =>
        if d == to then (w.usage(to) + size).toDouble / w.deviceTotal(to).toDouble
        else if d == from then (w.usage(from) - size).toDouble / w.deviceTotal(from).toDouble
        else w.fillRatio(d)
      }
      ratios.max - ratios.min

    def noOvershoot(s: StoreId, from: StorageDeviceId, to: StorageDeviceId): Boolean =
      val size = w.storeSize(s)
      val sinkAfter = (w.usage(to) + size).toDouble / w.deviceTotal(to).toDouble
      val sourceAfter = (w.usage(from) - size).toDouble / w.deviceTotal(from).toDouble
      sinkAfter <= sourceAfter

    var continue = spread() > config.balanceSpreadThreshold
    while continue do
      continue = false
      val byFill = w.deviceIds.sortBy(w.fillRatio)
      val sink = byFill.head
      val source = byFill.last
      val before = spread()
      // largest movable store on the source first; poolIndex as deterministic tiebreak
      val candidates = w.storesOn(source).filter(w.movable)
        .sortBy(s => (-w.storeSize(s), s.poolIndex))
      val chosen = candidates.find { s =>
        val pool = s.poolId
        w.fits(sink, s) &&
        w.samePoolOnDevice(sink, pool) == 0 &&
        (w.deviceHost(sink) == w.deviceHost(source) || w.samePoolOnHost(w.deviceHost(sink), pool) == 0) &&
        noOvershoot(s, source, sink) &&
        (before - spreadIfMoved(s, source, sink)) >= config.minBalanceMoveGain
      }
      chosen.foreach { s =>
        w.move(s, sink)
        continue = spread() > config.balanceSpreadThreshold
      }
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `sbt 'testOnly *RebalancePlanSuite'`
Expected: PASS (10 tests).

- [ ] **Step 5: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/common/rebalancing/Plan.scala src/test/scala/org/aspen_ddp/aspen/common/rebalancing/RebalancePlanSuite.scala
git commit -m "Implement balance phase for rebalance planning"
```

---

## Task 6: Cross-phase priority, stability, and determinism tests

**Files:**
- Modify: `src/test/scala/org/aspen_ddp/aspen/common/rebalancing/RebalancePlanSuite.scala`

These are characterization tests over the completed algorithm. They should pass without further implementation; if any fail, fix the phase implementation rather than weakening the test.

- [ ] **Step 1: Write the tests**

Append to `class RebalancePlanSuite`:

```scala
  test("priority: balance prefers a store whose move does not co-locate"):
    // dev1 (host1) is full with a pool-1 and a pool-2 store; dev2 (host2) already holds pool 1.
    // Balancing must move the pool-2 store (safe), never the pool-1 store (would co-locate on dev2).
    val st = planState(
      device(1, 1, 90, 100, store(1, 0, 50), store(2, 0, 40)),
      device(2, 2, 10, 100, store(1, 1, 10)))
    val plan = Plan.computePlan(st)
    plan.size shouldBe 1
    plan.head.storeId shouldBe sid(2, 0)
    plan.head.toDevice shouldBe devId(2)

  test("stable: re-running on the applied plan yields an empty plan"):
    val scenarios = List(
      planState(
        device(1, 1, 20, 100, store(1, 0, 10), store(1, 1, 10)),
        device(2, 2, 0, 100)),                                          // reliability
      planState(
        device(1, 1, 10, 100, store(1, 0, 10)),
        device(2, 1, 10, 100, store(1, 1, 10)),
        device(3, 2, 0, 100)),                                          // availability
      planState(
        device(1, 1, 80, 100, store(1, 0, 40)),
        device(2, 2, 0, 100)))                                          // balance
    for st <- scenarios do
      val plan = Plan.computePlan(st)
      plan should not be empty
      Plan.computePlan(applyPlan(st, plan)) shouldBe Nil

  test("deterministic: identical output regardless of device insertion order"):
    val a = planState(
      device(1, 1, 80, 100, store(1, 0, 40)),
      device(2, 2, 0, 100),
      device(3, 3, 0, 100))
    val b = planState(
      device(3, 3, 0, 100),
      device(2, 2, 0, 100),
      device(1, 1, 80, 100, store(1, 0, 40)))
    Plan.computePlan(a) shouldBe Plan.computePlan(b)

  test("deterministic: repeated calls produce the same plan"):
    val st = planState(
      device(1, 1, 90, 100, store(1, 0, 30), store(2, 0, 30)),
      device(2, 2, 0, 100),
      device(3, 3, 0, 100))
    Plan.computePlan(st) shouldBe Plan.computePlan(st)
```

- [ ] **Step 2: Run tests to verify they pass**

Run: `sbt 'testOnly *RebalancePlanSuite'`
Expected: PASS (14 tests).

- [ ] **Step 3: Run the full suite to confirm nothing regressed**

Run: `sbt test`
Expected: PASS (all suites).

- [ ] **Step 4: Commit**

```bash
git add src/test/scala/org/aspen_ddp/aspen/common/rebalancing/RebalancePlanSuite.scala
git commit -m "Add cross-phase priority, stability, and determinism tests"
```

---

## Self-Review Notes

- **Spec coverage:** Task 1 → `State.Device` hostId extension. Tasks 2–5 → interface, working copy, eligibility, the three phases with fit/no-overshoot/regression rules. Task 6 → priority guarantee, stability (no-flapping), determinism, and edge cases (empty/single covered in Task 2; immovable & not-enough-devices in Task 3).
- **IDA:** intentionally unused (per spec note); no task references it.
- **Type consistency:** `Transfer`, `Config`, `computePlan`, `Working`, `rankedDestinations`, `reliabilityRepair`, `availabilityRepair`, `balance`, and the builder names (`poolId`, `devId`, `hstId`, `sid`, `store`, `device`, `planState`, `applyPlan`) are used consistently across all tasks.
