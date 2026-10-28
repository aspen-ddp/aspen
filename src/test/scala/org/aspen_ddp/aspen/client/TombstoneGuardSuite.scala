package org.aspen_ddp.aspen.client

import org.aspen_ddp.aspen.IntegrationTestSuite
import org.aspen_ddp.aspen.common.ida.Replication
import org.aspen_ddp.aspen.common.metadata.StorageDeviceState.DeviceStatus
import org.aspen_ddp.aspen.common.metadata.{StorageDeviceId, StorageDeviceState, fixed_ids}
import org.aspen_ddp.aspen.common.objects.Insert
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.transaction.KeyValueUpdate.KeyRevision
import org.aspen_ddp.aspen.server.store.backend.RocksDBConfig

import scala.concurrent.{ExecutionContext, Future}

/** Every code path that places something on a tombstoned device, or places a tombstoned device
 *  somewhere, must refuse: a store put on a dead device is silently lost, and a dead device
 *  admitted to a live set pollutes every accounting that walks it. */
class TombstoneGuardSuite extends IntegrationTestSuite:

  /** Zero the host and device ids of `deviceId`'s state in place, leaving everything else --
   *  including its store map and its set membership -- untouched.
   *
   *  This is only the third write of FailedStorageDeviceDurableTask's step 1. The set and host
   *  removals are deliberately skipped: the guards under test read the device object alone, and
   *  leaving the device in its set is the harder case, because selection can still offer it.
   */
  private def tombstone(deviceId: StorageDeviceId): Future[Unit] =
    given ExecutionContext = executionContext
    client.transactUntilSuccessful: tx =>
      for
        ptr <- client.getStorageDevicePointer(deviceId)
        kvos <- client.read(ptr)
      yield
        val state = StorageDeviceState(kvos)
        val tombstoned = state.copy(hostId = fixed_ids.FailedHostId,
                                    storageDeviceId = fixed_ids.FailedStorageDeviceId,
                                    status = StorageDeviceState.DeviceStatus.Failed)
        val reqs = List(KeyRevision(StorageDeviceState.StateKey,
          kvos.contents(StorageDeviceState.StateKey).revision))
        val ops = List(Insert(StorageDeviceState.StateKey, tombstoned.encode()))
        tx.update(ptr, None, None, reqs, ops)

  atest("transferStore refuses a tombstoned destination"):
    given ExecutionContext = executionContext
    val storeId = StoreId(PoolId.BootstrapPoolId, 0.toByte)
    for
      _ <- net.createSecondDevice()
      _ <- waitForTransactionsToComplete()
      _ <- tombstone(net.secondDeviceId)
      _ <- waitForTransactionsToComplete()
      result <- recoverToSucceededIf[AspenClient.DeviceFailed](
                  client.transferStore(storeId, net.secondDeviceId))
    yield result

  atest("transferStore still works against a live destination"):
    given ExecutionContext = executionContext
    val storeId = StoreId(PoolId.BootstrapPoolId, 0.toByte)
    for
      _ <- net.createSecondDevice()
      _ <- waitForTransactionsToComplete()
      _ <- client.transferStore(storeId, net.secondDeviceId)
      _ <- waitForTransactionsToComplete()
      dst <- client.getStorageDeviceState(net.secondDeviceId)
    yield
      dst.stores(storeId).status should be(StorageDeviceState.StoreStatus.TransferringIn)

  atest("moveDeviceToSet refuses a tombstoned device"):
    given ExecutionContext = executionContext
    for
      setId <- client.createStorageDeviceSet("adopting-set", level = 0, parent = None)
      _ <- waitForTransactionsToComplete()
      _ <- net.createSecondDevice()
      _ <- waitForTransactionsToComplete()
      _ <- tombstone(net.secondDeviceId)
      _ <- waitForTransactionsToComplete()
      result <- recoverToSucceededIf[AspenClient.DeviceFailed](
                  client.moveDeviceToSet(net.secondDeviceId, setId))
    yield result

  atest("createNewStoragePool refuses to place a store on a tombstoned device"):
    given ExecutionContext = executionContext
    // A dedicated level-0 set holding exactly one device, which is then tombstoned, so
    // selection has nowhere else to put the pool's stores.
    for
      setId <- client.createStorageDeviceSet("doomed-set", level = 0, parent = None)
      _ <- waitForTransactionsToComplete()
      _ <- net.createSecondDevice()
      _ <- waitForTransactionsToComplete()
      _ <- client.moveDeviceToSet(net.secondDeviceId, setId)
      _ <- waitForTransactionsToComplete()
      _ <- tombstone(net.secondDeviceId)
      _ <- waitForTransactionsToComplete()
      result <- recoverToSucceededIf[AspenClient.DeviceFailed](
                  client.createNewStoragePool(
                    "doomed-pool",
                    Replication(1, 1),
                    None,
                    RocksDBConfig(),
                    setId,
                    1_000_000L))
    yield result

  atest("transferStore onto a ShuttingDown or Shutdown device fails with DeviceNotOperational"):
    given ExecutionContext = executionContext
    val storeId = StoreId(PoolId.BootstrapPoolId, 0.toByte)
    def attempt(status: DeviceStatus) =
      for
        _ <- setDeviceState(net.secondDeviceId)(_.copy(status = status))
        _ <- recoverToSucceededIf[AspenClient.DeviceNotOperational](
               client.transferStore(storeId, net.secondDeviceId))
      yield ()
    for
      _ <- net.createSecondDevice()
      _ <- waitForTransactionsToComplete()
      _ <- attempt(DeviceStatus.ShuttingDown)
      _ <- attempt(DeviceStatus.Shutdown)
      src <- client.getStorageDeviceState(StorageDeviceId.BootstrapStorageDeviceId)
      dst <- client.getStorageDeviceState(net.secondDeviceId)
    yield
      src.stores(storeId).status should be(StorageDeviceState.StoreStatus.Active)
      dst.stores.contains(storeId) should be(false)

  atest("createNewStoragePool avoids a Shutdown member device"):
    given ExecutionContext = executionContext
    for
      setId <- client.createStorageDeviceSet("spare-set", level = 0, parent = None)
      _ <- net.createSecondDevice()
      _ <- net.createThirdDevice()
      _ <- net.createDevice(fourth)
      _ <- waitForTransactionsToComplete()
      _ <- client.moveDeviceToSet(net.secondDeviceId, setId)
      _ <- client.moveDeviceToSet(net.thirdDeviceId, setId)
      _ <- client.moveDeviceToSet(fourth, setId)
      _ <- waitForTransactionsToComplete()
      _ <- setDeviceState(net.thirdDeviceId)(_.copy(status = DeviceStatus.Shutdown))
      poolId <- client.createNewStoragePool("avoid-pool", Replication(2, 2), None, RocksDBConfig(), setId, 1_000_000L)
      pool <- client.getStoragePoolState(poolId)
    yield
      pool.stores.map(_.storageDeviceId).toSet should be(Set(net.secondDeviceId, fourth))

  // A set narrower than the IDA already places several of a pool's stores on one device. With one
  // member shut down, the operational member takes the repeats rather than the shut-down one.
  atest("createNewStoragePool repeats an operational device rather than use a Shutdown one"):
    given ExecutionContext = executionContext
    for
      setId <- client.createStorageDeviceSet("tight-set", level = 0, parent = None)
      _ <- net.createSecondDevice()
      _ <- net.createThirdDevice()
      _ <- waitForTransactionsToComplete()
      _ <- client.moveDeviceToSet(net.secondDeviceId, setId)
      _ <- client.moveDeviceToSet(net.thirdDeviceId, setId)
      _ <- waitForTransactionsToComplete()
      _ <- setDeviceState(net.thirdDeviceId)(_.copy(status = DeviceStatus.Shutdown))
      poolId <- client.createNewStoragePool("tight-pool", Replication(2, 2), None, RocksDBConfig(),
                  setId, 1_000_000L)
      pool <- client.getStoragePoolState(poolId)
    yield
      pool.stores.map(_.storageDeviceId).toList should be(List(net.secondDeviceId, net.secondDeviceId))

  atest("createNewStoragePool fails with DeviceNotOperational when no member device is operational"):
    given ExecutionContext = executionContext
    for
      setId <- client.createStorageDeviceSet("dark-set", level = 0, parent = None)
      _ <- net.createSecondDevice()
      _ <- net.createThirdDevice()
      _ <- waitForTransactionsToComplete()
      _ <- client.moveDeviceToSet(net.secondDeviceId, setId)
      _ <- client.moveDeviceToSet(net.thirdDeviceId, setId)
      _ <- waitForTransactionsToComplete()
      _ <- setDeviceState(net.secondDeviceId)(_.copy(status = DeviceStatus.ShuttingDown))
      _ <- setDeviceState(net.thirdDeviceId)(_.copy(status = DeviceStatus.Shutdown))
      result <- recoverToSucceededIf[AspenClient.DeviceNotOperational](
                  client.createNewStoragePool("dark-pool", Replication(2, 2), None, RocksDBConfig(),
                    setId, 1_000_000L))
    yield result

  private val fourth = StorageDeviceId(new java.util.UUID(0, 102))
