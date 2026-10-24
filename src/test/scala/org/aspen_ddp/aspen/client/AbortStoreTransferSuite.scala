package org.aspen_ddp.aspen.client

import org.aspen_ddp.aspen.IntegrationTestSuite
import org.aspen_ddp.aspen.common.metadata.{StorageDeviceId, StorageDeviceState}
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId

import scala.concurrent.ExecutionContext

class AbortStoreTransferSuite extends IntegrationTestSuite:

  private val sourceId = StorageDeviceId.BootstrapStorageDeviceId
  private val storeId = StoreId(PoolId.BootstrapPoolId, 1)

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
      _ <- client.abortStoreTransfer(storeId)
      _ <- waitForTransactionsToComplete()
      srcAfter <- client.getStorageDeviceState(sourceId)
      dstAfter <- client.getStorageDeviceState(net.secondDeviceId)
      poolAfter <- client.getStoragePoolState(PoolId.BootstrapPoolId)
    yield
      poolBefore.stores(storeId.poolIndex).storageDeviceId should be(net.secondDeviceId)
      srcAfter should be(srcBefore)
      dstAfter should be(dstBefore)
      poolAfter.stores(storeId.poolIndex).storageDeviceId should be(net.secondDeviceId)
