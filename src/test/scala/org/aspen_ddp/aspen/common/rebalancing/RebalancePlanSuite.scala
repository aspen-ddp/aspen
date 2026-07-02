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
  def planState(devices: Device*): PlanState =
    PlanState(devices.map(d => d.deviceId -> d).toMap, Map.empty)

  /** Apply a plan to a PlanState, producing the post-transfer state (for stability checks). */
  def applyPlan(state: PlanState, plan: List[Plan.Transfer]): PlanState =
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
