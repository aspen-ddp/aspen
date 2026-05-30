package org.aspen_ddp.aspen.server.usage

import org.aspen_ddp.aspen.IntegrationTestSuite
import org.aspen_ddp.aspen.common.metadata.{StorageDeviceId, StorageDeviceState}

import scala.concurrent.{ExecutionContext, Future}

class StorageDeviceUsageManagerSuite extends IntegrationTestSuite:

  private def readDeviceState(): Future[StorageDeviceState] =
    given ExecutionContext = executionContext
    for
      devPtr <- client.getStorageDevicePointer(net.storageDeviceId)
      devKvos <- client.read(devPtr)
    yield
      StorageDeviceState(devKvos)

  atest("First report updates device state"):
    given ExecutionContext = executionContext
    val mgr = new StorageDeviceUsageManager(client)

    for
      state0 <- readDeviceState()
      _ = state0.currentUsage should be(0L)
      _ = state0.totalSize should be(0L)

      _ <- mgr.updateDeviceUsage(net.storageDeviceId, 5000L, 100000L)
      _ <- waitForTransactionsToComplete()

      state1 <- readDeviceState()
    yield
      state1.currentUsage should be(5000L)
      state1.totalSize should be(100000L)

  atest("No update when values are unchanged"):
    given ExecutionContext = executionContext
    val mgr = new StorageDeviceUsageManager(client)

    for
      _ <- mgr.updateDeviceUsage(net.storageDeviceId, 5000L, 100000L)
      _ <- waitForTransactionsToComplete()

      devPtr <- client.getStorageDevicePointer(net.storageDeviceId)
      kvos1 <- client.read(devPtr)
      rev1 = kvos1.contents(StorageDeviceState.StateKey).revision

      _ <- mgr.updateDeviceUsage(net.storageDeviceId, 5000L, 100000L)
      _ <- waitForTransactionsToComplete()

      kvos2 <- client.read(devPtr)
    yield
      kvos2.contents(StorageDeviceState.StateKey).revision should be(rev1)
