package org.aspen_ddp.aspen.server.rebuild

import org.aspen_ddp.aspen.IntegrationTestSuite
import org.aspen_ddp.aspen.client.{AspenClient, Transaction}
import org.aspen_ddp.aspen.client.internal.transaction.MissedUpdateMarker
import org.aspen_ddp.aspen.common.objects.Key
import org.aspen_ddp.aspen.server.store.backend.{Backend, DefaultBackendOpener}
import org.aspen_ddp.aspen.common.metadata.{StorageDeviceId, StorageDeviceState}
import org.aspen_ddp.aspen.common.metadata.management.FailedStorageDeviceTestHarness
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.server.StoreConfig
import org.aspen_ddp.aspen.server.host.ControlFixture

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.{Duration, MILLISECONDS}
import scala.util.Success

/** Both halves against each other: an operator fails a device, the producer moves its stores
 *  onto a live one marked Rebuilding, and the consumer reconstructs them. */
class RebuildEndToEndSuite extends IntegrationTestSuite
                              with ControlFixture
                              with FailedStorageDeviceTestHarness:

  atest("an accepted loss lets a rebuild finish past an object its pool cannot supply, recorded in the ErrorTree (Rebuild F15)"):
    given ExecutionContext = executionContext
    val failedId = StorageDeviceId.BootstrapStorageDeviceId
    val storeId = StoreId(PoolId.BootstrapPoolId, 0.toByte)
    val backends = new DefaultBackendOpener(executionContext)
    val dev = os.temp.dir(prefix = "rebuild-accept-loss")

    def rebuild(failKeys: Set[Key], episode: Option[java.util.UUID],
                outOfSpaceKeys: Set[Key] = Set()): RebuildingStore =
      new RebuildingStore(client, storeId, net.secondDeviceId, dev, backends, episode,
                          testingOnlyFailKeys = failKeys, testingOnlyOutOfSpaceKeys = outOfSpaceKeys)
    val staging = dev / RebuildingStore.RebuildDirectory / storeId.directoryName

    for
      ids <- Future.sequence((0 until 5).toList.map: i =>
               client.transactUntilSuccessful: tx =>
                 given Transaction = tx
                 tx.replaySafe("test")
                 client.getStoragePool(PoolId.BootstrapPoolId).flatMap: pool =>
                   pool.allocator.allocateDataObject(Array[Byte](i.toByte)).map(_.id))
      _ <- waitForTransactionsToComplete()
      // The store is Active: there is no rebuild whose loss could be accepted
      refused <- client.acceptRebuildLoss(storeId).transform(Success(_))

      _ <- net.createSecondDevice()
      _ <- waitForTransactionsToComplete()
      _ <- client.failStorageDevice(failedId)
      _ <- waitForTransactionsToComplete()
      task <- taskForEnrolled(failedId)
      _ <- withTimeout(task.completed.map(_ => ()), Duration(30000, MILLISECONDS), "task completion")
      _ <- waitForTransactionsToComplete()

      doomed = Key(ids.head.toBytes)
      placed <- client.getStorageDeviceState(net.secondDeviceId)
      episode = placed.stores(storeId).rebuildEpisode
      unaccepted <- rebuild(Set(doomed), episode).complete.transform(Success(_))

      _ <- client.acceptRebuildLoss(storeId)
      _ <- waitForTransactionsToComplete()
      flagged <- client.getStorageDeviceState(net.secondDeviceId)

      // RB-W20: a resumed pass past the end of the tree, whose retry of a readable object hits a full destination.
      // The pass fails as out of space, not as an accepted loss of that object
      _ = RebuildState.save(staging, RebuildState.load(staging).get.copy(
            lastRestoredKey = Some(Key(Array.fill[Byte](16)(-1))), failedObjects = List(ids.head, ids(1))))
      full <- rebuild(Set(doomed), episode, outOfSpaceKeys = Set(Key(ids(1).toBytes))).complete.transform(Success(_))
      fullPool <- client.getStoragePool(PoolId.BootstrapPoolId)
      fullEntry <- fullPool.errorTree.get(MissedUpdateMarker.errorKey(storeId, ids(1)))
      fullLostEntry <- fullPool.errorTree.get(MissedUpdateMarker.errorKey(storeId, ids.head))
      fullLeft = RebuildState.load(staging).map(_.failedObjects.toSet)
      fullPlaced = os.exists(dev / storeId.directoryName)

      accepted <- rebuild(Set(doomed), episode).complete.transform(Success(_))

      pool <- client.getStoragePool(PoolId.BootstrapPoolId)
      lostEntry <- pool.errorTree.get(MissedUpdateMarker.errorKey(storeId, ids.head))
      restoredEntry <- pool.errorTree.get(MissedUpdateMarker.errorKey(storeId, ids(1)))
    yield
      refused.failed.get shouldBe a[AspenClient.StoreNotRebuilding]
      // Without the acceptance the unreadable object keeps the store out of place
      unaccepted.isFailure should be(true)
      flagged.stores(storeId).status should be(StorageDeviceState.StoreStatus.Rebuilding)
      flagged.stores(storeId).acceptLoss should be(true)
      // The acceptance keeps the entry's episode, so it applies to this rebuild
      flagged.stores(storeId).rebuildEpisode should be(episode)
      episode should not be(None)
      // A full destination during the retries fails the pass with the out-of-space cause, records nothing as lost, and
      // leaves both objects in the checkpoint for the next pass
      Backend.isOutOfSpace(full.failed.get) should be(true)
      fullPlaced should be(false)
      fullEntry should be(None)
      fullLostEntry should be(None)
      fullLeft should be(Some(Set(ids.head, ids(1))))
      // With it the rebuild finishes, and the lost object, only that one, is recorded for repair
      accepted should be(Success(()))
      os.exists(dev / storeId.directoryName / StoreConfig.configFilename) should be(true)
      lostEntry should be(defined)
      restoredEntry should be(None)

  atest("a rebuild re-placed on another device moves its device entry and its pool entry (Rebuild F12)"):
    given ExecutionContext = executionContext
    val failedId = StorageDeviceId.BootstrapStorageDeviceId
    val storeId = StoreId(PoolId.BootstrapPoolId, 0.toByte)

    for
      _ <- net.createSecondDevice()
      _ <- net.createThirdDevice()
      _ <- waitForTransactionsToComplete()
      // The store is Active: there is no rebuild to re-place
      refusedActive <- client.replaceRebuild(storeId, net.thirdDeviceId).transform(Success(_))
      _ <- client.failStorageDevice(failedId)
      _ <- waitForTransactionsToComplete()
      task <- taskForEnrolled(failedId)
      _ <- withTimeout(task.completed.map(_ => ()), Duration(30000, MILLISECONDS), "task completion")
      _ <- waitForTransactionsToComplete()
      placed <- client.getStoragePoolState(PoolId.BootstrapPoolId)
      from = placed.stores(storeId.poolIndex).storageDeviceId
      to = if from == net.secondDeviceId then net.thirdDeviceId else net.secondDeviceId
      refusedSame <- client.replaceRebuild(storeId, from).transform(Success(_))

      _ <- client.replaceRebuild(storeId, to)
      _ <- waitForTransactionsToComplete()
      pool <- client.getStoragePoolState(PoolId.BootstrapPoolId)
      fromState <- client.getStorageDeviceState(from)
      toState <- client.getStorageDeviceState(to)
      radicleConfig <- client.read(client.radicle)
    yield
      refusedActive.failed.get shouldBe a[AspenClient.StoreNotRebuilding]
      refusedSame.failed.get shouldBe a[AspenClient.InvalidDestination]
      fromState.stores.contains(storeId) should be(false)
      toState.stores.get(storeId).map(_.status) should be(Some(StorageDeviceState.StoreStatus.Rebuilding))
      pool.stores(storeId.poolIndex).storageDeviceId should be(to)
      pool.stores(storeId.poolIndex).hostId should be(toState.hostId)

  atest("a rebuild re-placed away and back starts a new episode and does not resume the old one's checkpoint (RB-W3)"):
    given ExecutionContext = executionContext
    val failedId = StorageDeviceId.BootstrapStorageDeviceId
    val storeId = StoreId(PoolId.BootstrapPoolId, 0.toByte)
    val backends = new DefaultBackendOpener(executionContext)
    // Stands in for the directory of the device the rebuild first runs on
    val dev = os.temp.dir(prefix = "rebuild-episode")

    for
      ids <- Future.sequence((0 until 10).toList.map: i =>
               client.transactUntilSuccessful: tx =>
                 given Transaction = tx
                 tx.replaySafe("test")
                 client.getStoragePool(PoolId.BootstrapPoolId).flatMap: pool =>
                   pool.allocator.allocateDataObject(Array[Byte](i.toByte)).map(_.id))
      _ <- net.createSecondDevice()
      _ <- net.createThirdDevice()
      _ <- waitForTransactionsToComplete()
      _ <- client.failStorageDevice(failedId)
      _ <- waitForTransactionsToComplete()
      task <- taskForEnrolled(failedId)
      _ <- withTimeout(task.completed.map(_ => ()), Duration(30000, MILLISECONDS), "task completion")
      _ <- waitForTransactionsToComplete()
      placed <- client.getStoragePoolState(PoolId.BootstrapPoolId)
      from = placed.stores(storeId.poolIndex).storageDeviceId
      to = if from == net.secondDeviceId then net.thirdDeviceId else net.secondDeviceId
      first <- client.getStorageDeviceState(from)
      earlier = first.stores(storeId).rebuildEpisode

      // The first episode's rebuild walks the whole tree, checkpointing, and is stopped (its host goes down)
      stopped = new RebuildingStore(client, storeId, from, dev, backends, earlier, checkpointInterval = 1,
                                    testingOnlyAfterWalk = () => Future.failed(new Exception("host down")))
      _ <- stopped.complete.transform(Success(_))
      staging = dev / RebuildingStore.RebuildDirectory / storeId.directoryName
      checkpoint = RebuildState.load(staging)

      // Re-placed away while its host is down, then back
      _ <- client.replaceRebuild(storeId, to)
      _ <- waitForTransactionsToComplete()
      _ <- client.replaceRebuild(storeId, from)
      _ <- waitForTransactionsToComplete()
      back <- client.getStorageDeviceState(from)
      current = back.stores(storeId).rebuildEpisode

      resumed = new RebuildingStore(client, storeId, from, dev, backends, current,
                                    testingOnlyTrackRestoredKeys = true)
      outcome <- resumed.complete.transform(Success(_))
    yield
      earlier should not be(None)
      checkpoint.flatMap(_.episode) should be(earlier)
      checkpoint.flatMap(_.lastRestoredKey) should be(defined)
      current should not be(None)
      current should not be(earlier)
      outcome should be(Success(()))
      // Every object restored again, none taken from the earlier episode's copy
      val restored = resumed.testingOnlyRestoredKeys.map(_.bytes.toList).toSet
      ids.map(id => Key(id.toBytes).bytes.toList).toSet.subsetOf(restored) should be(true)
      os.read(dev / storeId.directoryName / RebuildingStore.RebuildCompleteMarkerFile) should be(current.get.toString)

  atest("a failed device's stores are reconstructed on a live device"):
    given ExecutionContext = executionContext
    val failedId = StorageDeviceId.BootstrapStorageDeviceId

    for
      // Content to reconstruct, and a live destination to reconstruct it onto.
      ids <- Future.sequence((0 until 20).toList.map: i =>
               client.transactUntilSuccessful: tx =>
                 given Transaction = tx
                 tx.replaySafe("test")
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
      recorded(host).loadStoreByIdRequests.toSet should be(expectedLoadRequests.toSet)
      // Six stores means exactly six load calls, not twelve (an unconditional load plus the
      // legitimate one would be hidden by .toSet).
      recorded(host).loadStoreByIdRequests.size should be(6)

      expectedStores.foreach: sid =>
        val key = (net.secondDeviceId, sid)
        val flipSeq = recorded(host).rebuildFlipSeq.get(key)
        val loadSeq = recorded(host).loadStoreByIdSeq.get(key)
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

      // A read-back assertion against the pool is not valid in this harness: ControlFixture's
      // RecordingSeams make tryLoadStore record instead of opening a RocksDB backend, so the rebuilt stores
      // do not actually come online. Reads are served by net.smgr's in-memory MapBackends throughout,
      // and a read-back would therefore pass whether the rebuild ran or not -- a vacuous assertion.

      // The foreach above yields Unit; the assertions it made are the test.
      succeed
