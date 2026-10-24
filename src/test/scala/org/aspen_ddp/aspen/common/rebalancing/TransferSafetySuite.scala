package org.aspen_ddp.aspen.common.rebalancing

import org.aspen_ddp.aspen.common.ida.{IDA, Replication}
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

  /** Build a PlanningState: one pool (default Replication(3,2)) with the given stores, each on its
   *  own device/host; `toDevice` is an extra empty device/host. */
  private def build(
      storeStatuses: List[StorageDeviceState.StoreStatus],
      ida: IDA = Replication(3, 2),
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
    val pool = State.Pool(poolId, ida, storesById)
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

  import StorageDeviceState.StoreStatus.*

  private def transfer0(state: State.PlanningState, toDev: StorageDeviceId, idx: Int = 0) =
    val sid = StoreId(poolId, idx.toByte)
    val fromDev = state.devices.find(_._2.stores.contains(sid)).get._1
    Plan.Transfer(sid, fromDev, toDev)

  private def hostsOf(state: State.PlanningState, idxs: Int*): Set[HostId] =
    idxs.map(i => state.devices.values.find(_.stores.contains(StoreId(poolId, i.toByte))).get.hostId).toSet

  test("floor is min(writeThreshold + 2, width - 1)"):
    TransferSafety.floor(Replication(3, 2)) shouldBe 2
    TransferSafety.floor(Replication(5, 3)) shouldBe 4
    TransferSafety.floor(Replication(3, 3)) shouldBe 2
    TransferSafety.floor(Replication(1, 1)) shouldBe 0
    TransferSafety.floor(Replication(7, 2)) shouldBe 4

  test("width 7 writeThreshold 2 (floor 4): two concurrent TransferringOut still admits a third"):
    val (state, _, toDev, _, offline) = build(
      List(TransferringOut, TransferringOut, Active, Active, Active, Active, Active), Replication(7, 2))
    TransferSafety.onlineCount(state, offline, poolId) shouldBe 5
    TransferSafety.isSafe(state, offline, transfer0(state, toDev, 2)) shouldBe true

  test("width 7 writeThreshold 2 (floor 4): three concurrent TransferringOut refuses a fourth"):
    val (state, _, toDev, _, offline) = build(
      List(TransferringOut, TransferringOut, TransferringOut, Active, Active, Active, Active), Replication(7, 2))
    TransferSafety.onlineCount(state, offline, poolId) shouldBe 4
    TransferSafety.isSafe(state, offline, transfer0(state, toDev, 3)) shouldBe false

  test("a non-Active candidate is never safe"):
    val (state, _, toDev, _, offline) = build(
      List(TransferringOut, Active, Active, Active, Active, Active, Active), Replication(7, 2))
    TransferSafety.isSafe(state, offline, transfer0(state, toDev, 0)) shouldBe false

  test("unsafe when a Rebuilding store plus an offline-host store push below the floor"):
    val (state, _, toDev, _, _) = build(
      List(Active, Active, Active, Active, Rebuilding, Active), Replication(6, 1))
    // floor = min(3, 5) = 3. Online: 5 Active - 1 offline host = 4 Active; 4 - 1 = 3 >= 3 is safe
    val oneOff = hostsOf(state, 1)
    TransferSafety.isSafe(state, oneOff, transfer0(state, toDev, 0)) shouldBe true
    // second offline host: online 3 - 1 = 2 < 3
    val twoOff = hostsOf(state, 1, 2)
    TransferSafety.isSafe(state, twoOff, transfer0(state, toDev, 0)) shouldBe false

  test("width == writeThreshold with all Active admits exactly one transfer"):
    val (state, _, toDev, _, offline) = build(List(Active, Active, Active), Replication(3, 3))
    TransferSafety.isSafe(state, offline, transfer0(state, toDev, 0)) shouldBe true
    val (state2, _, toDev2, _, offline2) = build(List(TransferringOut, Active, Active), Replication(3, 3))
    TransferSafety.isSafe(state2, offline2, transfer0(state2, toDev2, 1)) shouldBe false

  test("isBelowFloor uses the clamped floor: Replication(3,3) with one TransferringOut is not below"):
    val (state, _, _, _, offline) = build(List(TransferringOut, Active, Active), Replication(3, 3))
    TransferSafety.isBelowFloor(state, offline, poolId) shouldBe false
    val (state2, _, _, _, offline2) = build(List(TransferringOut, TransferringOut, Active), Replication(3, 3))
    TransferSafety.isBelowFloor(state2, offline2, poolId) shouldBe true

  test("isBelowFloor counts offline-host stores"):
    val (state, _, _, _, _) = build(List(Active, Active, Active), Replication(3, 3))
    TransferSafety.isBelowFloor(state, hostsOf(state, 0), poolId) shouldBe false
    TransferSafety.isBelowFloor(state, hostsOf(state, 0, 1), poolId) shouldBe true
