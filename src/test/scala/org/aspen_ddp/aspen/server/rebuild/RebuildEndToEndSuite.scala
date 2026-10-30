package org.aspen_ddp.aspen.server.rebuild

import org.aspen_ddp.aspen.IntegrationTestSuite
import org.aspen_ddp.aspen.client.Transaction
import org.aspen_ddp.aspen.common.metadata.{StorageDeviceId, StorageDeviceState}
import org.aspen_ddp.aspen.common.metadata.management.FailedStorageDeviceTestHarness
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.server.{StoreConfig, HostTestHarness}

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.{Duration, MILLISECONDS}

/** Both halves against each other: an operator fails a device, the producer moves its stores
 *  onto a live one marked Rebuilding, and the consumer reconstructs them. */
class RebuildEndToEndSuite extends IntegrationTestSuite
                              with HostTestHarness
                              with FailedStorageDeviceTestHarness:

  atest("a failed device's stores are reconstructed on a live device"):
    given ExecutionContext = executionContext
    val failedId = StorageDeviceId.BootstrapStorageDeviceId

    for
      // Content to reconstruct, and a live destination to reconstruct it onto.
      ids <- Future.sequence((0 until 20).toList.map: i =>
               client.transactUntilSuccessful: tx =>
                 given Transaction = tx
                 client.getStoragePool(PoolId.BootstrapPoolId).flatMap: pool =>
                   pool.allocator.allocateDataObject(Array[Byte](i.toByte)).map(_.id))
      _ <- waitForTransactionsToComplete()
      _ <- net.createSecondDevice()
      _ <- waitForTransactionsToComplete()

      // Producer.
      _ <- client.failStorageDevice(failedId)
      _ <- waitForTransactionsToComplete()
      task <- taskForEnrolled(failedId)
      _ <- withTimeout(task.completed.map(_ => ()), Duration(30000, MILLISECONDS),
                       "task completion")
      _ <- waitForTransactionsToComplete()

      // The bootstrap pool's three stores and the initial metadata pool's three
      staged <- client.getStorageDeviceState(net.secondDeviceId)
      _ = staged.stores.size should be(6)

      // Consumer: create a host after the producer finishes, so its constructor-time
      // device scan and the drain do not race.
      hostRoot = newHostDir()
      deviceDir = writeDevice(hostRoot, "dev0", net.secondDeviceId)
      host = newHost(hostRoot, maxConcurrentRebuilds = 3)

      _ <- completeRebuilds(host, net.secondDeviceId)
      _ <- waitForTransactionsToComplete()

      rebuilt <- client.getStorageDeviceState(net.secondDeviceId)
      bootstrapStores = List(
        StoreId(PoolId.BootstrapPoolId, 0.toByte),
        StoreId(PoolId.BootstrapPoolId, 1.toByte),
        StoreId(PoolId.BootstrapPoolId, 2.toByte)
      )
      expectedStores = bootstrapStores ++ net.metadataStores.map(_.storeId)
    yield
      // Every entry on net.secondDeviceId reads Active (the metadata flip landed).
      rebuilt.stores.size should be(6)
      rebuilt.stores.values.foreach: entry =>
        entry.status should be(StorageDeviceState.StoreStatus.Active)

      // The host decided to adopt all six stores, and the adoption happened after the
      // metadata flip to Active (loading is downstream of the metadata decision).
      val expectedLoadRequests = expectedStores.map(s => (net.secondDeviceId, s))
      host.loadStoreByIdRequests.toSet should be(expectedLoadRequests.toSet)
      // Six stores means exactly six load calls, not twelve (an unconditional load plus the
      // legitimate one would be hidden by .toSet).
      host.loadStoreByIdRequests.size should be(6)

      expectedStores.foreach: sid =>
        val key = (net.secondDeviceId, sid)
        val flipSeq = host.rebuildFlipSeq.get(key)
        val loadSeq = host.loadStoreByIdSeq.get(key)
        withClue(s"Store $sid: flip must complete before load (flipSeq=$flipSeq, loadSeq=$loadSeq)"):
          flipSeq should be(defined)
          loadSeq should be(defined)
          flipSeq.get should be < loadSeq.get

      // On disk, each store's final directory exists with a loadable StoreConfig and its staging
      // directory under rebuilding/ is gone. Each bootstrap store also holds flushed object data
      // (not just scaffolding); the initial metadata pool's stores are empty, so have none.
      expectedStores.foreach: sid =>
        val finalPath = deviceDir / sid.directoryName
        os.exists(finalPath) should be(true)
        StoreConfig.loadStoreConfig(finalPath / StoreConfig.configFilename).storeId should
          be(sid)

        if bootstrapStores.contains(sid) then
          os.list(finalPath).count(_.last.endsWith(".sst")) should be > 0

        val stagingPath = deviceDir / RebuildingStore.RebuildDirectory / sid.directoryName
        os.exists(stagingPath) should be(false)

      // A read-back assertion against the pool is not valid in this harness: RecordingHost
      // overrides tryLoadStore to record instead of opening a RocksDB backend, so the rebuilt stores
      // do not actually come online. Reads are served by net.smgr's in-memory MapBackends throughout,
      // and a read-back would therefore pass whether the rebuild ran or not -- a vacuous assertion.

      // The foreach above yields Unit; the assertions it made are the test.
      succeed
