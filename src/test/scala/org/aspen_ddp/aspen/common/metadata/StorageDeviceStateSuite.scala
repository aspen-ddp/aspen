package org.aspen_ddp.aspen.common.metadata

import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.util.UUID

import StorageDeviceState.{DeviceStatus, StoreEntry, StoreStatus}

class StorageDeviceStateSuite extends AnyFunSuite with Matchers:

  private val poolId = PoolId(UUID.randomUUID())
  private val dest = StorageDeviceId(UUID.randomUUID())
  private val xferId = UUID.randomUUID()

  private def sid(i: Int) = StoreId(poolId, i.toByte)

  private val sa = sid(0)
  private val sb = sid(1)
  private val sc = sid(2)
  private val sd = sid(3)
  private val se = sid(4)

  private def state(status: DeviceStatus = DeviceStatus.Operational) =
    StorageDeviceState(
      StorageDeviceId(UUID.randomUUID()),
      HostId(UUID.randomUUID()),
      0L, 0L,
      Map(
        sa -> StoreEntry(StoreStatus.Active, None),
        sb -> StoreEntry(StoreStatus.Rebuilding, None),
        sc -> StoreEntry(StoreStatus.TransferringOut, Some(dest), Some(xferId)),
        sd -> StoreEntry(StoreStatus.TransferringIn, None),
        se -> StoreEntry(StoreStatus.Initializing, None)),
      StorageDeviceSetId(UUID.randomUUID()),
      status)

  test("withActiveStoresOffline changes only Active entries"):
    val s = state()
    val o = s.withActiveStoresOffline
    o.stores(sa) shouldBe StoreEntry(StoreStatus.Offline, None)
    o.stores.filter((k, _) => k != sa) shouldBe s.stores.filter((k, _) => k != sa)
    o.stores(sc).transferDevice shouldBe Some(dest)
    o.stores(sc).transferId shouldBe Some(xferId)

  test("withOfflineStoresActive is the inverse on Offline entries and leaves others alone"):
    val s = state()
    s.withActiveStoresOffline.withOfflineStoresActive shouldBe s
    val withOtherOffline = s.copy(stores = s.stores + (sb -> StoreEntry(StoreStatus.Offline, None)))
    withOtherOffline.withOfflineStoresActive.stores(sb).status shouldBe StoreStatus.Active
    withOtherOffline.withOfflineStoresActive.stores(sc) shouldBe s.stores(sc)

  test("isFailed is true only for status Failed"):
    DeviceStatus.values.foreach: st =>
      state(st).isFailed shouldBe (st == DeviceStatus.Failed)
    val zeroed = state().copy(hostId = fixed_ids.FailedHostId,
                              storageDeviceId = fixed_ids.FailedStorageDeviceId)
    zeroed.isFailed shouldBe false

  test("isOperational is true only for Operational"):
    DeviceStatus.values.foreach: st =>
      state(st).isOperational shouldBe (st == DeviceStatus.Operational)
