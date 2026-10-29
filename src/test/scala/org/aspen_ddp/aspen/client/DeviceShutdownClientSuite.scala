package org.aspen_ddp.aspen.client

import org.aspen_ddp.aspen.IntegrationTestSuite
import org.aspen_ddp.aspen.common.metadata.{HostId, StorageDeviceId, StorageDeviceState, StoragePoolState}
import org.aspen_ddp.aspen.common.metadata.StorageDeviceState.{DeviceStatus, StoreEntry, StoreStatus}
import org.aspen_ddp.aspen.common.network.CheckStorageDevice
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.objects.Insert
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.transaction.KeyValueUpdate.KeyRevision

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

class DeviceShutdownClientSuite extends IntegrationTestSuite:

  private def sid(i: Int) = StoreId(PoolId.BootstrapPoolId, i.toByte)

  private def takeNudges(): List[CheckStorageDevice] =
    net.takeCapturedHostMessages().collect:
      case m: CheckStorageDevice => m

  private def seed(entries: Map[StoreId, StoreEntry], status: DeviceStatus): Future[Unit] =
    setDeviceState(net.secondDeviceId)(_.copy(stores = entries, status = status))

  private def secondState(): Future[StorageDeviceState] =
    client.getStorageDeviceState(net.secondDeviceId)

  private def expectOneNudge(nudges: List[CheckStorageDevice], state: StorageDeviceState) =
    nudges.size should be(1)
    nudges.head.toHost should be(state.hostId)
    nudges.head.fromClient should be(client.clientId)
    nudges.head.deviceId should be(net.secondDeviceId)

  atest("shutdownStorageDevice marks only Active stores Offline and the device ShuttingDown"):
    given ExecutionContext = executionContext
    val other = net.secondDeviceId
    for
      _ <- net.createSecondDevice()
      _ <- waitForTransactionsToComplete()
      _ <- seed(Map(
        sid(0) -> StoreEntry(StoreStatus.Active, None),
        sid(1) -> StoreEntry(StoreStatus.Rebuilding, None),
        sid(2) -> StoreEntry(StoreStatus.TransferringOut, Some(other)),
        sid(3) -> StoreEntry(StoreStatus.Initializing, None)), DeviceStatus.Operational)
      _ = takeNudges()
      _ <- client.shutdownStorageDevice(other)
      nudges = takeNudges()
      after <- secondState()
    yield
      after.status should be(DeviceStatus.ShuttingDown)
      after.stores should be(Map(
        sid(0) -> StoreEntry(StoreStatus.Offline, None),
        sid(1) -> StoreEntry(StoreStatus.Rebuilding, None),
        sid(2) -> StoreEntry(StoreStatus.TransferringOut, Some(other)),
        sid(3) -> StoreEntry(StoreStatus.Initializing, None)))
      expectOneNudge(nudges, after)

  for status <- List(DeviceStatus.ShuttingDown, DeviceStatus.Shutdown) do
    atest(s"shutdownStorageDevice on a $status device writes nothing and still nudges"):
      given ExecutionContext = executionContext
      for
        _ <- net.createSecondDevice()
        _ <- waitForTransactionsToComplete()
        _ <- seed(Map(sid(0) -> StoreEntry(StoreStatus.Offline, None)), status)
        before <- secondState()
        _ = takeNudges()
        _ <- client.shutdownStorageDevice(net.secondDeviceId)
        nudges = takeNudges()
        after <- secondState()
      yield
        after should be(before)
        expectOneNudge(nudges, after)

  /** Points the bootstrap pool's store `i` at `device`. Metadata only: the store's data stays
   *  where it is, which is all the threshold check reads. */
  private def placeBootstrapStore(i: Int, device: StorageDeviceId): Future[Unit] =
    given ExecutionContext = executionContext
    client.transactUntilSuccessful: tx =>
      for
        ptr <- client.getStoragePoolPointer(PoolId.BootstrapPoolId)
        kvos <- client.read(ptr)
      yield
        val cfg = StoragePoolState(kvos)
        cfg.stores(i) = StoragePoolState.StoreEntry(HostId.BootstrapHostId, device)
        tx.update(ptr, None, None,
          List(KeyRevision(StoragePoolState.ConfigKey, kvos.contents(StoragePoolState.ConfigKey).revision)),
          List(Insert(StoragePoolState.ConfigKey, cfg.encode())))

  // The bootstrap pool is Replication(3, 2) with all three stores on the bootstrap device.
  atest("shutdownStorageDevice refuses to take a pool below its write threshold"):
    given ExecutionContext = executionContext
    val device = net.storageDeviceId
    for
      before <- client.getStorageDeviceState(device)
      _ = takeNudges()
      r <- client.shutdownStorageDevice(device).failed
      after <- client.getStorageDeviceState(device)
    yield
      r shouldBe a[AspenClient.ShutdownWouldBreakPool]
      r.asInstanceOf[AspenClient.ShutdownWouldBreakPool].poolId should be(PoolId.BootstrapPoolId)
      after should be(before)
      after.status should be(DeviceStatus.Operational)
      takeNudges() should be(Nil)

  atest("shutdownStorageDevice counts only the pool's stores on other Operational devices"):
    given ExecutionContext = executionContext
    val other = net.secondDeviceId
    for
      _ <- net.createSecondDevice()
      _ <- net.createThirdDevice()
      _ <- waitForTransactionsToComplete()
      // Store 0 on the bootstrap device, 1 on the shutting-down third device, 2 on the device
      // being shut down: one Operational store would remain, below the threshold of two.
      _ <- placeBootstrapStore(1, net.thirdDeviceId)
      _ <- placeBootstrapStore(2, other)
      _ <- setDeviceState(net.thirdDeviceId)(_.copy(status = DeviceStatus.ShuttingDown,
                                                    stores = Map(sid(1) -> StoreEntry(StoreStatus.Offline, None))))
      _ <- seed(Map(sid(2) -> StoreEntry(StoreStatus.Active, None)), DeviceStatus.Operational)
      r <- client.shutdownStorageDevice(other).failed
      after <- secondState()
    yield
      r shouldBe a[AspenClient.ShutdownWouldBreakPool]
      after.status should be(DeviceStatus.Operational)
      after.stores(sid(2)).status should be(StoreStatus.Active)

  atest("shutdownStorageDevice counts only Active stores on other Operational devices"):
    given ExecutionContext = executionContext
    val other = net.secondDeviceId
    for
      _ <- net.createSecondDevice()
      _ <- net.createThirdDevice()
      _ <- waitForTransactionsToComplete()
      // Store 0 on the bootstrap device, 1 Rebuilding on the Operational third device, 2 on the
      // device being shut down: two stores would remain, exactly the threshold, but only one Active.
      _ <- placeBootstrapStore(1, net.thirdDeviceId)
      _ <- placeBootstrapStore(2, other)
      _ <- setDeviceState(net.thirdDeviceId)(_.copy(status = DeviceStatus.Operational,
                                                    stores = Map(sid(1) -> StoreEntry(StoreStatus.Rebuilding, None))))
      _ <- seed(Map(sid(2) -> StoreEntry(StoreStatus.Active, None)), DeviceStatus.Operational)
      r <- client.shutdownStorageDevice(other).failed
      after <- secondState()
    yield
      r shouldBe a[AspenClient.ShutdownWouldBreakPool]
      after.status should be(DeviceStatus.Operational)
      after.stores(sid(2)).status should be(StoreStatus.Active)

  atest("shutdownStorageDevice counts a pool's store on an unregistered device as unavailable"):
    given ExecutionContext = executionContext
    val other = net.secondDeviceId
    val missing = StorageDeviceId(UUID.fromString("eeeeeeee-0000-0000-0000-000000000001"))
    for
      _ <- net.createSecondDevice()
      _ <- waitForTransactionsToComplete()
      // Store 1 names a device with no entry in the device tree. It must count as unavailable
      // rather than fail the lookup, which the CLI would report as the target being not found.
      _ <- placeBootstrapStore(1, missing)
      _ <- placeBootstrapStore(2, other)
      _ <- seed(Map(sid(2) -> StoreEntry(StoreStatus.Active, None)), DeviceStatus.Operational)
      r <- client.shutdownStorageDevice(other).failed
      after <- secondState()
    yield
      r shouldBe a[AspenClient.ShutdownWouldBreakPool]
      after.status should be(DeviceStatus.Operational)
      after.stores(sid(2)).status should be(StoreStatus.Active)

  atest("shutdownStorageDevice proceeds when enough of each pool stays on Operational devices"):
    given ExecutionContext = executionContext
    val other = net.secondDeviceId
    for
      _ <- net.createSecondDevice()
      _ <- waitForTransactionsToComplete()
      _ <- placeBootstrapStore(2, other)
      _ <- seed(Map(sid(2) -> StoreEntry(StoreStatus.Active, None)), DeviceStatus.Operational)
      _ <- client.shutdownStorageDevice(other)
      after <- secondState()
    yield
      after.status should be(DeviceStatus.ShuttingDown)
      after.stores(sid(2)).status should be(StoreStatus.Offline)

  atest("shutdownStorageDevice on a Failed device fails with DeviceFailed"):
    given ExecutionContext = executionContext
    for
      _ <- net.createSecondDevice()
      _ <- waitForTransactionsToComplete()
      _ <- seed(Map.empty, DeviceStatus.Failed)
      _ = takeNudges()
      r <- client.shutdownStorageDevice(net.secondDeviceId).failed
    yield
      r shouldBe a[AspenClient.DeviceFailed]
      takeNudges() should be(Nil)

  for status <- List(DeviceStatus.Shutdown, DeviceStatus.ShuttingDown) do
    atest(s"restartStorageDevice returns Offline stores to Active and the device to Operational from $status"):
      given ExecutionContext = executionContext
      val other = net.secondDeviceId
      for
        _ <- net.createSecondDevice()
        _ <- waitForTransactionsToComplete()
        _ <- seed(Map(
          sid(0) -> StoreEntry(StoreStatus.Offline, None),
          sid(1) -> StoreEntry(StoreStatus.Rebuilding, None),
          sid(2) -> StoreEntry(StoreStatus.TransferringOut, Some(other))), status)
        _ = takeNudges()
        _ <- client.restartStorageDevice(other)
        nudges = takeNudges()
        after <- secondState()
      yield
        after.status should be(DeviceStatus.Operational)
        after.stores should be(Map(
          sid(0) -> StoreEntry(StoreStatus.Active, None),
          sid(1) -> StoreEntry(StoreStatus.Rebuilding, None),
          sid(2) -> StoreEntry(StoreStatus.TransferringOut, Some(other))))
        expectOneNudge(nudges, after)

  atest("restartStorageDevice on an Operational device writes nothing and still nudges"):
    given ExecutionContext = executionContext
    for
      _ <- net.createSecondDevice()
      _ <- waitForTransactionsToComplete()
      _ <- seed(Map(sid(0) -> StoreEntry(StoreStatus.Active, None)), DeviceStatus.Operational)
      before <- secondState()
      _ = takeNudges()
      _ <- client.restartStorageDevice(net.secondDeviceId)
      nudges = takeNudges()
      after <- secondState()
    yield
      after should be(before)
      expectOneNudge(nudges, after)

  atest("restartStorageDevice on a Failed device fails with DeviceFailed"):
    given ExecutionContext = executionContext
    for
      _ <- net.createSecondDevice()
      _ <- waitForTransactionsToComplete()
      _ <- seed(Map.empty, DeviceStatus.Failed)
      _ = takeNudges()
      r <- client.restartStorageDevice(net.secondDeviceId).failed
    yield
      r shouldBe a[AspenClient.DeviceFailed]
      takeNudges() should be(Nil)
