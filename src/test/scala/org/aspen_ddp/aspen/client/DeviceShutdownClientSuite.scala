package org.aspen_ddp.aspen.client

import org.aspen_ddp.aspen.IntegrationTestSuite
import org.aspen_ddp.aspen.common.metadata.StorageDeviceState
import org.aspen_ddp.aspen.common.metadata.StorageDeviceState.{DeviceStatus, StoreEntry, StoreStatus}
import org.aspen_ddp.aspen.common.network.CheckStorageDevice
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId

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
