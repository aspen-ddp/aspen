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
