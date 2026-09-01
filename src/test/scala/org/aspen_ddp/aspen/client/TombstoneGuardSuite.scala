package org.aspen_ddp.aspen.client

import org.aspen_ddp.aspen.IntegrationTestSuite
import org.aspen_ddp.aspen.common.ida.Replication
import org.aspen_ddp.aspen.common.metadata.{StorageDeviceId, StorageDeviceState, fixed_ids}
import org.aspen_ddp.aspen.common.objects.Insert
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.transaction.KeyValueUpdate.KeyRevision
import org.aspen_ddp.aspen.server.store.backend.RocksDBConfig

import scala.concurrent.{ExecutionContext, Future}

/** Every code path that writes a StoreEntry into a StorageDeviceState must refuse a tombstoned
 *  destination: a store placed on a dead device is silently lost. */
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
                                    storageDeviceId = fixed_ids.FailedStorageDeviceId)
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
