package org.aspen_ddp.aspen.client

import org.aspen_ddp.aspen.IntegrationTestSuite
import org.aspen_ddp.aspen.common.metadata.{StorageDeviceId, StorageDeviceState, fixed_ids}
import org.aspen_ddp.aspen.common.network.CheckStorageDevice
import org.aspen_ddp.aspen.common.objects.Insert
import org.aspen_ddp.aspen.common.transaction.KeyValueUpdate.KeyRevision
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId

import scala.concurrent.ExecutionContext

class AbortStoreTransferSuite extends IntegrationTestSuite:

  private val sourceId = StorageDeviceId.BootstrapStorageDeviceId
  private val storeId = StoreId(PoolId.BootstrapPoolId, 1)

  private def takeNudges(): List[CheckStorageDevice] =
    net.takeCapturedHostMessages().collect:
      case m: CheckStorageDevice => m

  atest("abortStoreTransfer nudges the source and destination hosts"):
    given ExecutionContext = executionContext
    for
      _ <- net.createSecondDevice()
      _ <- waitForTransactionsToComplete()
      _ <- client.transferStore(storeId, net.secondDeviceId)
      _ <- waitForTransactionsToComplete()
      _ = takeNudges()
      rolledBack <- client.abortStoreTransfer(storeId)
      nudges = takeNudges()
      src <- client.getStorageDeviceState(sourceId)
      dst <- client.getStorageDeviceState(net.secondDeviceId)
    yield
      rolledBack should be(true)
      nudges.map(n => (n.deviceId, n.toHost)).toSet should be(
        Set((sourceId, src.hostId), (net.secondDeviceId, dst.hostId)))
      nudges.size should be(2)
      nudges.map(_.fromClient).distinct should be(List(client.clientId))

  atest("abortStoreTransfer sends no nudge when nothing was outstanding"):
    given ExecutionContext = executionContext
    for
      _ <- waitForTransactionsToComplete()
      _ = takeNudges()
      rolledBack <- client.abortStoreTransfer(storeId)
      nudges = takeNudges()
    yield
      rolledBack should be(false)
      nudges should be(Nil)

  atest("abortStoreTransfer rolls back an outstanding transfer on both devices"):
    given ExecutionContext = executionContext
    for
      _ <- net.createSecondDevice()
      _ <- waitForTransactionsToComplete()
      _ <- client.transferStore(storeId, net.secondDeviceId)
      _ <- waitForTransactionsToComplete()
      before <- client.getStorageDeviceState(sourceId)
      _ <- client.abortStoreTransfer(storeId)
      _ <- waitForTransactionsToComplete()
      src <- client.getStorageDeviceState(sourceId)
      dst <- client.getStorageDeviceState(net.secondDeviceId)
      pool <- client.getStoragePoolState(PoolId.BootstrapPoolId)
    yield
      before.stores(storeId).status should be(StorageDeviceState.StoreStatus.TransferringOut)
      src.stores(storeId).status should be(StorageDeviceState.StoreStatus.Active)
      src.stores(storeId).transferDevice should be(None)
      dst.stores.contains(storeId) should be(false)
      pool.stores(storeId.poolIndex).storageDeviceId should be(sourceId)

  atest("abortStoreTransfer on an Active store with no transfer succeeds and changes nothing"):
    given ExecutionContext = executionContext
    for
      before <- client.getStorageDeviceState(sourceId)
      _ <- client.abortStoreTransfer(storeId)
      _ <- waitForTransactionsToComplete()
      after <- client.getStorageDeviceState(sourceId)
    yield
      after should be(before)

  atest("abortStoreTransfer after the transfer completed changes nothing"):
    given ExecutionContext = executionContext
    for
      _ <- net.createSecondDevice()
      _ <- waitForTransactionsToComplete()
      _ <- client.transferStore(storeId, net.secondDeviceId)
      _ <- waitForTransactionsToComplete()
      _ <- net.smgr.testingOnlyUpdateStateForTransferredStore(storeId, sourceId, net.secondDeviceId)
      _ <- waitForTransactionsToComplete()
      srcBefore <- client.getStorageDeviceState(sourceId)
      dstBefore <- client.getStorageDeviceState(net.secondDeviceId)
      poolBefore <- client.getStoragePoolState(PoolId.BootstrapPoolId)
      _ = takeNudges()
      _ <- client.abortStoreTransfer(storeId)
      nudges = takeNudges()
      _ <- waitForTransactionsToComplete()
      srcAfter <- client.getStorageDeviceState(sourceId)
      dstAfter <- client.getStorageDeviceState(net.secondDeviceId)
      poolAfter <- client.getStoragePoolState(PoolId.BootstrapPoolId)
    yield
      poolBefore.stores(storeId.poolIndex).storageDeviceId should be(net.secondDeviceId)
      nudges should be(Nil)
      srcAfter should be(srcBefore)
      dstAfter should be(dstBefore)
      poolAfter.stores(storeId.poolIndex).storageDeviceId should be(net.secondDeviceId)

  atest("abortStoreTransfer leaves a destination entry that is not TransferringIn untouched"):
    given ExecutionContext = executionContext
    for
      _ <- net.createSecondDevice()
      _ <- waitForTransactionsToComplete()
      _ <- client.transferStore(storeId, net.secondDeviceId)
      _ <- waitForTransactionsToComplete()
      // The drain has taken over the destination's entry.
      _ <- client.transactUntilSuccessful: tx =>
             for
               ptr <- client.getStorageDevicePointer(net.secondDeviceId)
               kvos <- client.read(ptr)
             yield
               val st = StorageDeviceState(kvos).setStoreEntry(
                 storeId, StorageDeviceState.StoreStatus.Rebuilding, None)
               tx.update(ptr, None, None,
                 List(KeyRevision(StorageDeviceState.StateKey, kvos.contents(StorageDeviceState.StateKey).revision)),
                 List(Insert(StorageDeviceState.StateKey, st.encode())))
      _ <- waitForTransactionsToComplete()
      _ = takeNudges()
      _ <- client.abortStoreTransfer(storeId)
      nudges = takeNudges()
      _ <- waitForTransactionsToComplete()
      src <- client.getStorageDeviceState(sourceId)
      dst <- client.getStorageDeviceState(net.secondDeviceId)
    yield
      // Only the source changed, so only it is nudged.
      nudges.map(_.deviceId) should be(List(sourceId))
      src.stores(storeId).status should be(StorageDeviceState.StoreStatus.Active)
      src.stores(storeId).transferDevice should be(None)
      dst.stores(storeId).status should be(StorageDeviceState.StoreStatus.Rebuilding)

  atest("abortStoreTransfer does not nudge a tombstoned destination"):
    given ExecutionContext = executionContext
    for
      _ <- net.createSecondDevice()
      _ <- waitForTransactionsToComplete()
      _ <- client.transferStore(storeId, net.secondDeviceId)
      _ <- waitForTransactionsToComplete()
      _ <- client.transactUntilSuccessful: tx =>
             for
               ptr <- client.getStorageDevicePointer(net.secondDeviceId)
               kvos <- client.read(ptr)
             yield
               val st = StorageDeviceState(kvos).copy(hostId = fixed_ids.FailedHostId)
               tx.update(ptr, None, None,
                 List(KeyRevision(StorageDeviceState.StateKey, kvos.contents(StorageDeviceState.StateKey).revision)),
                 List(Insert(StorageDeviceState.StateKey, st.encode())))
      _ <- waitForTransactionsToComplete()
      _ = takeNudges()
      _ <- client.abortStoreTransfer(storeId)
      nudges = takeNudges()
      _ <- waitForTransactionsToComplete()
      src <- client.getStorageDeviceState(sourceId)
    yield
      nudges.map(_.deviceId) should be(List(sourceId))
      src.stores(storeId).status should be(StorageDeviceState.StoreStatus.Active)
