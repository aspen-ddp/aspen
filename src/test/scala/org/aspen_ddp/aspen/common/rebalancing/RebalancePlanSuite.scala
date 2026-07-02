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
    plan.head.fromDevice shouldBe devId(1)
    plan.head.toDevice shouldBe devId(2)
    Seq(sid(1, 0), sid(1, 1), sid(1, 2)) should contain (plan.head.storeId)

  test("reliability: immovable (non-Active) stores are not selected as sources"):
    val st = planState(
      device(1, 1, 20, 100,
        store(1, 0, 10, StorageDeviceState.StoreStatus.Rebuilding),
        store(1, 1, 10)),
      device(2, 2, 0, 100))
    val plan = Plan.computePlan(st)
    plan.size shouldBe 1
    plan.head.storeId shouldBe sid(1, 1)   // only the Active store can move

  test("reliability: no move when no destination reduces co-location"):
    // both devices already hold two pool-1 stores; moving 2->3 never reduces the max, so no move
    val st = planState(
      device(1, 1, 20, 100, store(1, 0, 10), store(1, 1, 10)),
      device(2, 2, 20, 100, store(1, 2, 10), store(1, 3, 10)))
    Plan.computePlan(st) shouldBe Nil

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

  test("priority: balance prefers a store whose move does not co-locate"):
    // dev1 (host1) is full with a pool-1 and a pool-2 store; dev2 (host2) already holds pool 1.
    // Balancing must move the pool-2 store (safe), never the pool-1 store (would co-locate on dev2,
    // violating reliability constraints even in the balance phase).
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
      plan should have size 1   // each scenario triggers exactly one corrective move
      Plan.computePlan(applyPlan(st, plan)) shouldBe Nil

  test("deterministic: input Map ordering does not change the plan"):
    // Devices are keyed in a Map, so insertion order should be irrelevant; internal iteration is
    // sorted by device uuid. Assert a concrete expected plan so a regression cannot pass vacuously.
    val expected = List(Plan.Transfer(sid(1, 0), devId(1), devId(2)))
    val a = planState(
      device(1, 1, 80, 100, store(1, 0, 40)),
      device(2, 2, 0, 100),
      device(3, 3, 0, 100))
    val b = planState(
      device(3, 3, 0, 100),
      device(2, 2, 0, 100),
      device(1, 1, 80, 100, store(1, 0, 40)))
    Plan.computePlan(a) shouldBe expected
    Plan.computePlan(b) shouldBe expected

  test("deterministic: repeated calls produce the same plan"):
    val st = planState(
      device(1, 1, 90, 100, store(1, 0, 30), store(2, 0, 30)),
      device(2, 2, 0, 100),
      device(3, 3, 0, 100))
    Plan.computePlan(st) shouldBe Plan.computePlan(st)

  test("cross-phase: a reliability move and a balance move both fire in one plan"):
    // dev1(host1) is completely full and holds two pool-1 stores (reliability violation) plus a
    // pool-2 store. Reliability moves one pool-1 store to dev2; balance then moves the pool-2 store
    // to the empty dev3. Later phases observe earlier phases' effects (shared working copy).
    val st = planState(
      device(1, 1, 100, 100, store(1, 0, 30), store(1, 1, 30), store(2, 0, 40)),
      device(2, 2, 0, 100),
      device(3, 3, 0, 100))
    val plan = Plan.computePlan(st)
    plan shouldBe List(
      Plan.Transfer(sid(1, 0), devId(1), devId(2)),   // reliability
      Plan.Transfer(sid(1, 1), devId(1), devId(3)))   // balance
    // and the plan is stable
    Plan.computePlan(applyPlan(st, plan)) shouldBe Nil

  test("zero-capacity device is treated as full and receives no transfers"):
    // dev2 has totalSize 0 (fillRatio reported as 1.0). It must never be chosen as a move target,
    // and computePlan must not divide by zero or throw.
    val st = planState(
      device(1, 1, 80, 100, store(1, 0, 40)),
      device(2, 2, 0, 0))
    Plan.computePlan(st) shouldBe Nil
