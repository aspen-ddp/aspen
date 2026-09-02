package org.aspen_ddp.aspen.server.rebuild

import org.aspen_ddp.aspen.IntegrationTestSuite
import org.aspen_ddp.aspen.client.Transaction
import org.aspen_ddp.aspen.common.metadata.{StorageDeviceId, StorageDeviceState}
import org.aspen_ddp.aspen.common.metadata.management.FailedStorageDeviceTestHarness
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.server.{StoreConfig, StoreManagerTestHarness}

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.{Duration, MILLISECONDS}

/** Both halves against each other: an operator fails a device, the producer moves its stores
 *  onto a live one marked Rebuilding, and the consumer reconstructs them. */
class RebuildEndToEndSuite extends IntegrationTestSuite
                              with StoreManagerTestHarness
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

      staged <- client.getStorageDeviceState(net.secondDeviceId)
      _ = staged.stores.size should be(3)

      // Consumer: create a manager after the producer finishes, so its constructor-time
      // device scan and the drain do not race.
      hostRoot = newHostDir()
      deviceDir = writeDevice(hostRoot, "dev0", net.secondDeviceId)
      mgr = newManager(hostRoot, maxConcurrentRebuilds = 3)

      _ <- completeRebuilds(mgr, net.secondDeviceId)
      _ <- waitForTransactionsToComplete()

      rebuilt <- client.getStorageDeviceState(net.secondDeviceId)
      expectedStores = List(
        StoreId(PoolId.BootstrapPoolId, 0.toByte),
        StoreId(PoolId.BootstrapPoolId, 1.toByte),
        StoreId(PoolId.BootstrapPoolId, 2.toByte)
      )
    yield
      // Every entry on net.secondDeviceId reads Active (the metadata flip landed).
      rebuilt.stores.size should be(3)
      rebuilt.stores.values.foreach: entry =>
        entry.status should be(StorageDeviceState.StoreStatus.Active)

      // The manager decided to adopt all three stores, and the adoption happened after the
      // metadata flip to Active (loading is downstream of the metadata decision).
      val expectedLoadRequests = expectedStores.map(s => (net.secondDeviceId, s))
      mgr.loadStoreByIdRequests.toSet should be(expectedLoadRequests.toSet)
      // Three stores means exactly three load calls, not six (an unconditional load plus the
      // legitimate one would be hidden by .toSet).
      mgr.loadStoreByIdRequests.size should be(3)

      expectedStores.foreach: sid =>
        val key = (net.secondDeviceId, sid)
        val flipSeq = mgr.rebuildFlipSeq.get(key)
        val loadSeq = mgr.loadStoreByIdSeq.get(key)
        withClue(s"Store $sid: flip must complete before load (flipSeq=$flipSeq, loadSeq=$loadSeq)"):
          flipSeq should be(defined)
          loadSeq should be(defined)
          flipSeq.get should be < loadSeq.get

      // On disk, each store's final directory exists with a loadable StoreConfig, holds flushed
      // object data (not just scaffolding), and its staging directory under rebuilding/ is gone.
      expectedStores.foreach: sid =>
        val finalPath = os.Path(deviceDir) / sid.directoryName
        os.exists(finalPath) should be(true)
        StoreConfig.loadStoreConfig((finalPath / StoreConfig.configFilename).toIO).storeId should
          be(sid)

        // The store holds flushed object data, not just a config file.
        os.list(finalPath).count(_.last.endsWith(".sst")) should be > 0

        val stagingPath = os.Path(deviceDir) / RebuildingStore.RebuildDirectory / sid.directoryName
        os.exists(stagingPath) should be(false)

      // A read-back assertion against the pool is not valid in this harness: RecordingStoreManager
      // overrides tryLoadStore to record instead of opening a RocksDB backend, so the rebuilt stores
      // do not actually come online. Reads are served by net.smgr's in-memory MapBackends throughout,
      // and a read-back would therefore pass whether the rebuild ran or not -- a vacuous assertion.

      // The foreach above yields Unit; the assertions it made are the test.
      succeed
