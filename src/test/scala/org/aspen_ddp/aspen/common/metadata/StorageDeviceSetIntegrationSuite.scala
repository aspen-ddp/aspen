package org.aspen_ddp.aspen.common.metadata

import org.aspen_ddp.aspen.IntegrationTestSuite
import org.aspen_ddp.aspen.client.internal.MetadataTree
import org.aspen_ddp.aspen.client.{AllocationError, AspenClient, GroupObjectAllocatorId, TestTransactions}
import org.aspen_ddp.aspen.client.tkvl.{KeyValueListPointer, Root, SinglePoolNodeAllocator}
import org.aspen_ddp.aspen.common.Radicle
import org.aspen_ddp.aspen.common.allocation_group.AllocationGroupId
import org.aspen_ddp.aspen.common.objects.{DataObjectPointer, Key, KeyAlreadyExists, KeyValueObjectPointer, ObjectId, ObjectPointer, Value}
import org.aspen_ddp.aspen.server.store.backend.MapBackend
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.ida.{RandomAccessReedSolomon, ReedSolomon, Replication}
import org.aspen_ddp.aspen.server.store.backend.RocksDBConfig
import org.aspen_ddp.aspen.common.DataBuffer
import org.aspen_ddp.aspen.common.metadata.StorageDeviceState.DeviceStatus
import org.aspen_ddp.aspen.common.store.StoreId

import scala.concurrent.{ExecutionContext, Future}

class StorageDeviceSetIntegrationSuite extends IntegrationTestSuite:

  atest("bootstrap creates the 'bootstrap' storage device set"):
    given ExecutionContext = executionContext
    val tree = new MetadataTree(client, radicle, Radicle.StorageDeviceSetsTreeKey)
    for
      ptr <- tree.get(StorageDeviceSetId.BootstrapStorageDeviceSetId.uuid)
      dos <- client.read(ptr.asInstanceOf[DataObjectPointer])
    yield
      val sds = StorageDeviceSetState(dos)
      sds.setId should be(StorageDeviceSetId.BootstrapStorageDeviceSetId)
      sds.name should be("bootstrap")
      sds.level should be(0)
      sds.parent should be(None)
      sds.memberDevices should contain(StorageDeviceId.BootstrapStorageDeviceId)
      sds.memberSets should be(Nil)
      sds.assignedPools should contain(PoolId.BootstrapPoolId)

  atest("bootstrap pool and device reference the bootstrap set"):
    given ExecutionContext = executionContext
    for
      ps <- client.getStoragePoolState(PoolId.BootstrapPoolId)
      ds <- client.getStorageDeviceState(StorageDeviceId.BootstrapStorageDeviceId)
    yield
      ps.storageDeviceSet should be(StorageDeviceSetId.BootstrapStorageDeviceSetId)
      ds.storageDeviceSet should be(StorageDeviceSetId.BootstrapStorageDeviceSetId)

  atest("createStorageDeviceSet round-trips via the client"):
    given ExecutionContext = executionContext
    for
      setId <- client.createStorageDeviceSet("test-set", level = 0, parent = None)
      _ <- waitForTransactionsToComplete()
      sds <- client.getStorageDeviceSetState(setId)
    yield
      sds.setId should be(setId)
      sds.name should be("test-set")
      sds.level should be(0)
      sds.parent should be(None)
      sds.memberDevices should be(Nil)
      sds.memberSets should be(Nil)
      sds.assignedPools should be(Nil)

  atest("getStorageDeviceSetState reads the bootstrap set"):
    given ExecutionContext = executionContext
    client.getStorageDeviceSetState(StorageDeviceSetId.BootstrapStorageDeviceSetId).map: sds =>
      sds.name should be("bootstrap")

  atest("getStorageDeviceSetId resolves a created set by name"):
    given ExecutionContext = executionContext
    for
      setId <- client.createStorageDeviceSet("named-set", level = 0, parent = None)
      _ <- waitForTransactionsToComplete()
      resolved <- client.getStorageDeviceSetId("named-set")
    yield
      resolved should be(setId)

  atest("createStorageDeviceSet links the new set into its parent"):
    given ExecutionContext = executionContext
    for
      parentId <- client.createStorageDeviceSet("parent-set", level = 1, parent = None)
      _ <- waitForTransactionsToComplete()
      childId <- client.createStorageDeviceSet("child-set", level = 0, parent = Some(parentId))
      _ <- waitForTransactionsToComplete()
      parent <- client.getStorageDeviceSetState(parentId)
      child <- client.getStorageDeviceSetState(childId)
    yield
      child.parent should be(Some(parentId))
      parent.memberSets should contain(childId)
      parent.memberSets.count(_ == childId) should be(1)

  atest("createNewStoragePool selects devices from the set and records the pool in assignedPools"):
    given ExecutionContext = executionContext
    val setId = StorageDeviceSetId.BootstrapStorageDeviceSetId
    for
      poolId <- client.createNewStoragePool(
                  "rework-pool",
                  Replication(1, 1),
                  None,
                  RocksDBConfig(),
                  setId,
                  0L)
      _ <- waitForTransactionsToComplete()
      poolState <- client.getStoragePoolState(poolId)
      setState <- client.getStorageDeviceSetState(setId)
    yield
      poolState.storageDeviceSet should be(setId)
      poolState.stores.length should be(1)
      poolState.stores(0).storageDeviceId should be(StorageDeviceId.BootstrapStorageDeviceId)
      setState.assignedPools should contain(poolId)
      setState.assignedPools should contain(PoolId.BootstrapPoolId)

  atest("createNewStoragePool fails with KeyAlreadyExists when the name is taken"):
    given ExecutionContext = executionContext
    val setId = StorageDeviceSetId.BootstrapStorageDeviceSetId

    def create(): Future[PoolId] =
      client.createNewStoragePool("dup-pool", Replication(1, 1), None, RocksDBConfig(), setId, 0L)

    for
      _      <- create()
      _      <- waitForTransactionsToComplete()

      before <- client.listStoragePools()
      err    <- create().failed
      _      <- waitForTransactionsToComplete()
      after  <- client.listStoragePools()
    yield
      // The type matters beyond the message: createStoragePool's onFail matches on it to stop
      // the retry loop, and new-pool's reportError matches on it to print "already exists". It
      // is KeyAlreadyExists because the registration goes through Registry.prepareRegister --
      // Registry.DuplicateRegistration belongs to the non-transactional register() path.
      err shouldBe a[KeyAlreadyExists]
      after should be(before)

  atest("createNewStoragePool rejects a maxSliceSize below the IDA's minSliceSize"):
    given ExecutionContext = executionContext
    val setId = StorageDeviceSetId.BootstrapStorageDeviceSetId
    val ida = RandomAccessReedSolomon(5, 3, 4, 8)
    for
      before <- client.listStoragePools()
      _ = an[IllegalArgumentException] should be thrownBy
        client.createNewStoragePool("tiny-slice-pool", ida, Some(1), RocksDBConfig(), setId, 0L)
      _ <- waitForTransactionsToComplete()
      after <- client.listStoragePools()
    yield
      after should be(before)

  atest("createNewStoragePool rejects a backend maxSliceSize below the IDA's minSliceSize"):
    given ExecutionContext = executionContext
    val setId = StorageDeviceSetId.BootstrapStorageDeviceSetId
    val ida = RandomAccessReedSolomon(5, 3, 4, 8)
    for
      before <- client.listStoragePools()
      _ = an[IllegalArgumentException] should be thrownBy
        client.createNewStoragePool("tiny-backend-pool", ida, Some(1000), RocksDBConfig(Some(7)), setId, 0L)
      _ <- waitForTransactionsToComplete()
      after <- client.listStoragePools()
    yield
      after should be(before)

  atest("a backend maxSliceSize below the pool's sets the pool's effective limit"):
    given ExecutionContext = executionContext
    val setId = StorageDeviceSetId.BootstrapStorageDeviceSetId
    val ida = ReedSolomon(5, 3, 4)
    for
      poolId <- client.createNewStoragePool("backend-limited", ida, Some(2000), RocksDBConfig(Some(1000)), setId, 0L)
      _ <- waitForTransactionsToComplete()
      poolState <- client.getStoragePoolState(poolId)
      pool <- client.getStoragePool(poolId)
    yield
      poolState.maxSliceSize should be(Some(2000))
      poolState.effectiveMaxSliceSize should be(Some(1000))
      poolState.maxObjectSize should be(Some(ida.maxObjectSize(1000)))
      pool.maxSliceSize should be(Some(1000))
      pool.maxObjectSize should be(Some(ida.maxObjectSize(1000)))
      pool.allocator.maxObjectSize should be(Some(ida.maxObjectSize(1000)))

  atest("createNewStoragePool records maxSliceSize and derives maxObjectSize"):
    given ExecutionContext = executionContext
    val setId = StorageDeviceSetId.BootstrapStorageDeviceSetId
    for
      poolId <- client.createNewStoragePool("sliced-pool", Replication(1, 1), Some(1000), RocksDBConfig(), setId, 0L)
      _ <- waitForTransactionsToComplete()
      poolState <- client.getStoragePoolState(poolId)
      pool <- client.getStoragePool(poolId)
    yield
      poolState.maxSliceSize should be(Some(1000))
      pool.maxSliceSize should be(Some(1000))
      pool.maxObjectSize should be(Some(1000))
      pool.allocator.maxObjectSize should be(Some(1000))

  private val bootSet = StorageDeviceSetId.BootstrapStorageDeviceSetId

  private def createOrdinaryPool(name: String, metadataGroup: Option[AllocationGroupId] = None): Future[PoolId] =
    client.createNewStoragePool(name, Replication(1, 1), None, RocksDBConfig(), bootSet, 0L, metadataGroup)

  /** The pool's ErrorTree and AllocationTree roots, freshly read */
  private def treeRoots(poolId: PoolId): Future[List[Root]] =
    given ExecutionContext = executionContext
    for
      ptr <- client.getStoragePoolPointer(poolId)
      kvos <- client.read(ptr)
    yield
      List(StoragePoolState.ErrorTreeKey, StoragePoolState.AllocationTreeKey).map: key =>
        Root(client, kvos.contents(key).value.bytes)

  /** Loads a store for each of the pool's IDA elements into the test network's host */
  private def serveStores(poolId: PoolId, width: Int): Future[Unit] =
    given ExecutionContext = executionContext
    val loads = (0 until width).toList.map: i =>
      net.smgr.loadStore(net.storageDeviceId, new MapBackend(StoreId(poolId, i.toByte)), None)
    handleEvents()
    Future.sequence(loads).map(_ => ())

  /** Every node reachable from `root`: down from each upper-tier node and right along every tier */
  private def reachableNodes(root: Root): Future[List[KeyValueObjectPointer]] =
    given ExecutionContext = executionContext
    def visit(pending: List[(Int, KeyValueObjectPointer)],
              seen: Set[ObjectId],
              found: List[KeyValueObjectPointer]): Future[List[KeyValueObjectPointer]] =
      pending match
        case Nil => Future.successful(found)
        case (_, ptr) :: rest if seen.contains(ptr.id) => visit(rest, seen, found)
        case (tier, ptr) :: rest =>
          client.read(ptr).flatMap: kvos =>
            val right = kvos.right.map(v => (tier, KeyValueListPointer(v.bytes).pointer)).toList
            val down =
              if tier == 0 then
                Nil
              else
                kvos.contents.values.toList.map: vs =>
                  ObjectPointer(vs.value.bytes) match
                    case p: KeyValueObjectPointer => (tier - 1, p)
                    case p => fail(s"upper-tier entry is not a KeyValueObjectPointer: $p")
            visit(right ++ down ++ rest, seen + ptr.id, ptr :: found)
    visit(List((root.tier, root.rootKeyValuePointer.get)), Set.empty, Nil)

  atest("createNewStoragePool puts the trees in the default metadata group"):
    given ExecutionContext = executionContext
    for
      poolId <- createOrdinaryPool("ordinary")
      _ <- waitForTransactionsToComplete()
      roots <- treeRoots(poolId)
      poolPtr <- client.getStoragePoolPointer(poolId)
    yield
      roots.foreach: root =>
        root.rootKeyValuePointer.get.poolId should be(PoolId.InitialMetadataPoolId)
        root.nodeAllocator.tiers.head._1 should be(GroupObjectAllocatorId(AllocationGroupId.DefaultMetadataGroupId))
      poolPtr.poolId should be(Radicle.poolId)

  atest("createNewStoragePool uses an explicit metadata group"):
    given ExecutionContext = executionContext
    for
      metaPoolId <- client.createNewMetadataPool("meta-pool", Replication(1, 1), None, RocksDBConfig(), bootSet, 0L)
      _ <- serveStores(metaPoolId, 1)
      groupId <- client.createAllocationGroup("meta-group", 0, idaPolicy = IDAPolicy.AllowAll, isMetadataGroup = true)
      _ <- waitForTransactionsToComplete()
      _ <- AllocationGroupState.addPool(client, metaPoolId, groupId)
      _ <- waitForTransactionsToComplete()
      poolId <- createOrdinaryPool("ordinary", Some(groupId))
      _ <- waitForTransactionsToComplete()
      roots <- treeRoots(poolId)
    yield
      roots.foreach: root =>
        root.rootKeyValuePointer.get.poolId should be(metaPoolId)
        root.nodeAllocator.tiers.head._1 should be(GroupObjectAllocatorId(groupId))
      succeed

  atest("createNewStoragePool refuses a non-metadata group"):
    given ExecutionContext = executionContext
    for
      groupId <- client.createAllocationGroup("ordinary-group", 0, idaPolicy = IDAPolicy.AllowAll)
      _ <- waitForTransactionsToComplete()
      before <- client.listStoragePools()
      err <- createOrdinaryPool("ordinary", Some(groupId)).failed
      _ <- waitForTransactionsToComplete()
      after <- client.listStoragePools()
    yield
      err shouldBe a[AspenClient.NotAMetadataGroup]
      err.asInstanceOf[AspenClient.NotAMetadataGroup].groupId should be(groupId)
      after should be(before)

  atest("an ordinary pool's ErrorTree splits within the metadata group"):
    given ExecutionContext = executionContext
    // Each entry is a fifth of the default node size, so a handful force the tier-0 root to split
    val value = Value(new Array[Byte](SinglePoolNodeAllocator.DefaultMaxNodeSize / 5))
    for
      poolId <- createOrdinaryPool("ordinary")
      _ <- waitForTransactionsToComplete()
      pool <- client.getStoragePool(poolId)
      _ <- (0 until 8).foldLeft(Future.unit): (f, i) =>
        f.flatMap: _ =>
          val tx = TestTransactions.withoutMissedUpdateTracking(client.newTransaction())
          for
            _ <- pool.errorTree.set(Key(i), value)(using tx)
            _ <- tx.commit()
            _ <- waitForTransactionsToComplete()
          yield ()
      roots <- treeRoots(poolId)
      errorRoot = roots.head
      nodes <- reachableNodes(errorRoot)
    yield
      errorRoot.tier should be >= 1
      nodes.size should be >= 3
      nodes.foreach(_.poolId should be(PoolId.InitialMetadataPoolId))
      succeed

  atest("createNewStoragePool fails clearly when the metadata group is empty"):
    given ExecutionContext = executionContext
    for
      _ <- AllocationGroupState.removePool(client, PoolId.InitialMetadataPoolId, AllocationGroupId.DefaultMetadataGroupId)
      _ <- waitForTransactionsToComplete()
      before <- client.listStoragePools()
      err <- createOrdinaryPool("ordinary").failed
      _ <- waitForTransactionsToComplete()
      after <- client.listStoragePools()
    yield
      err shouldBe a[AllocationError]
      after should be(before)

  atest("createStorageDeviceSet rejects a parent at an equal or lower level"):
    given ExecutionContext = executionContext
    for
      parentId <- client.createStorageDeviceSet("bad-parent", level = 0, parent = None)
      _ <- waitForTransactionsToComplete()
      _ <- recoverToSucceededIf[AspenClient.InvalidDeviceSetLevel](
             client.createStorageDeviceSet("bad-child-equal", level = 0, parent = Some(parentId))
           )
      _ <- recoverToSucceededIf[AspenClient.InvalidDeviceSetLevel](
             client.createStorageDeviceSet("bad-child-higher", level = 1, parent = Some(parentId))
           )
    yield succeed

  atest("createStorageDeviceSet fails with KeyAlreadyExists when the name is taken"):
    given ExecutionContext = executionContext
    for
      _      <- client.createStorageDeviceSet("dup-set", level = 0, parent = None)
      _      <- waitForTransactionsToComplete()

      before <- client.listStorageDeviceSets()
      err    <- client.createStorageDeviceSet("dup-set", level = 1, parent = None).failed
      _      <- waitForTransactionsToComplete()
      after  <- client.listStorageDeviceSets()
    yield
      // The type matters beyond the message: createStorageDeviceSet's onFail matches on it to
      // stop the retry loop, and create-device-set's reportError matches on it to print
      // "already exists". It is KeyAlreadyExists because the registration goes through
      // Registry.prepareRegister -- Registry.DuplicateRegistration belongs to the
      // non-transactional register() path.
      err shouldBe a[KeyAlreadyExists]
      // The allocation and the sets-tree insert are already staged when the registration
      // rejects, so this holds because the failed transaction is aborted wholesale.
      after should be(before)

  atest("moveDeviceToSet moves the device and updates both sets"):
    given ExecutionContext = executionContext
    val deviceId = StorageDeviceId.BootstrapStorageDeviceId
    val oldSetId = StorageDeviceSetId.BootstrapStorageDeviceSetId
    for
      destId <- client.createStorageDeviceSet("dest-set", level = 0, parent = None)
      _ <- waitForTransactionsToComplete()

      _ <- client.moveDeviceToSet(deviceId, destId)
      _ <- waitForTransactionsToComplete()

      device <- client.getStorageDeviceState(deviceId)
      dest <- client.getStorageDeviceSetState(destId)
      old <- client.getStorageDeviceSetState(oldSetId)
    yield
      device.storageDeviceSet should be(destId)
      dest.memberDevices should contain(deviceId)
      old.memberDevices should not contain deviceId

  atest("moveDeviceToSet is a no-op when the device is already in the target set"):
    given ExecutionContext = executionContext
    val deviceId = StorageDeviceId.BootstrapStorageDeviceId
    val setId = StorageDeviceSetId.BootstrapStorageDeviceSetId
    for
      _ <- client.moveDeviceToSet(deviceId, setId)
      _ <- waitForTransactionsToComplete()

      device <- client.getStorageDeviceState(deviceId)
      set <- client.getStorageDeviceSetState(setId)
    yield
      device.storageDeviceSet should be(setId)
      set.memberDevices should contain(deviceId)
      set.memberDevices.count(_ == deviceId) should be(1)

  atest("moveDeviceToSet fails with NotLevelZero when the target set is not level 0"):
    given ExecutionContext = executionContext
    val deviceId = StorageDeviceId.BootstrapStorageDeviceId
    for
      level1Id <- client.createStorageDeviceSet("higher-set", level = 1, parent = None)
      _ <- waitForTransactionsToComplete()

      err <- client.moveDeviceToSet(deviceId, level1Id).failed
    yield
      err shouldBe a[StorageDeviceSetState.NotLevelZero]

  atest("moveDeviceToSet fails with NoSuchElementException for an unknown device"):
    given ExecutionContext = executionContext
    val unknownDevice = StorageDeviceId(java.util.UUID.randomUUID())
    val setId = StorageDeviceSetId.BootstrapStorageDeviceSetId
    recoverToSucceededIf[NoSuchElementException](
      client.moveDeviceToSet(unknownDevice, setId)
    )

  atest("moveDeviceToSet self-heals when the old set does not list the device"):
    given ExecutionContext = executionContext
    val deviceId = StorageDeviceId.BootstrapStorageDeviceId
    val oldSetId = StorageDeviceSetId.BootstrapStorageDeviceSetId
    for
      destId <- client.createStorageDeviceSet("selfheal-dest", level = 0, parent = None)
      _ <- waitForTransactionsToComplete()

      // Corrupt state: remove the device from the old set's memberDevices while the
      // device's storageDeviceSet still points at the old set.
      oldPtr <- client.getStorageDeviceSetPointer(oldSetId)
      oldDos <- client.read(oldPtr)
      oldSet = StorageDeviceSetState(oldDos)
      _ <- client.transactUntilSuccessful: tx =>
             tx.overwrite(oldPtr, oldDos.revision,
               DataBuffer(oldSet.copy(memberDevices = oldSet.memberDevices.filter(_ != deviceId)).toBytes))
             Future.unit
      _ <- waitForTransactionsToComplete()

      _ <- client.moveDeviceToSet(deviceId, destId)
      _ <- waitForTransactionsToComplete()

      device <- client.getStorageDeviceState(deviceId)
      dest <- client.getStorageDeviceSetState(destId)
      old <- client.getStorageDeviceSetState(oldSetId)
    yield
      device.storageDeviceSet should be(destId)
      dest.memberDevices should contain(deviceId)
      old.memberDevices should not contain deviceId

  private val bootSetId = StorageDeviceSetId.BootstrapStorageDeviceSetId

  atest("selectDeviceForStore skips a Shutdown device"):
    given ExecutionContext = executionContext
    def pickMany(n: Int, size: Long): Future[List[StorageDeviceId]] =
      client.getStorageDeviceSetState(bootSetId).flatMap: set =>
        Future.sequence(List.fill(n)(set.selectDeviceForStore(size, Set.empty, client)))
    for
      _ <- net.createSecondDevice()
      _ <- waitForTransactionsToComplete()
      _ <- setDeviceState(net.secondDeviceId)(_.copy(status = DeviceStatus.Shutdown))
      picks <- pickMany(20, 0L)
      // Only the shut-down device is big enough.
      _ <- setDeviceState(StorageDeviceId.BootstrapStorageDeviceId)(_.copy(totalSize = 100L))
      _ <- setDeviceState(net.secondDeviceId)(_.copy(totalSize = 1000L))
      set <- client.getStorageDeviceSetState(bootSetId)
      failed <- recoverToSucceededIf[AllocationError](set.selectDeviceForStore(500L, Set.empty, client))
    yield
      picks.toSet should be(Set(StorageDeviceId.BootstrapStorageDeviceId))
      failed

  atest("selectDeviceForRebuild skips a ShuttingDown device"):
    given ExecutionContext = executionContext
    def pickMany(n: Int): Future[List[StorageDeviceId]] =
      client.getStorageDeviceSetState(bootSetId).flatMap: set =>
        Future.sequence(List.fill(n)(set.selectDeviceForRebuild(PoolId.BootstrapPoolId, 0.toByte, client)))
    for
      _ <- net.createSecondDevice()
      _ <- net.createThirdDevice()
      _ <- waitForTransactionsToComplete()
      _ <- setDeviceState(net.secondDeviceId)(_.copy(status = DeviceStatus.ShuttingDown))
      picks <- pickMany(20)
      _ <- setDeviceState(net.thirdDeviceId)(_.copy(status = DeviceStatus.ShuttingDown))
      set <- client.getStorageDeviceSetState(bootSetId)
      failed <- recoverToSucceededIf[AllocationError](
                  set.selectDeviceForRebuild(PoolId.BootstrapPoolId, 0.toByte, client))
    yield
      picks.toSet should be(Set(net.thirdDeviceId))
      failed

  /** A store of the bootstrap pool at an index no host serves, so neither a host's reconcile nor
   *  its usage tracker acts on the metadata these tests write for it. */
  private val phantom = StoreId(PoolId.BootstrapPoolId, 9.toByte)

  atest("selectDeviceForStore counts a store transferring in against the device's free space"):
    given ExecutionContext = executionContext
    for
      _ <- net.createSecondDevice()
      _ <- waitForTransactionsToComplete()
      _ <- setDeviceState(StorageDeviceId.BootstrapStorageDeviceId)(_.copy(totalSize = 100L))
      before <- client.getStorageDeviceSetState(bootSetId)
                  .flatMap(_.selectDeviceForStore(500L, Set.empty, client))
      // The second device has 1,000,000 bytes free on its own, but 999,600 are on their way.
      _ <- setStoreSize(phantom, 999_600L)
      _ <- setDeviceState(net.secondDeviceId)(_.setStoreEntry(phantom,
             StorageDeviceState.StoreStatus.TransferringIn, Some(StorageDeviceId.BootstrapStorageDeviceId)))
      set <- client.getStorageDeviceSetState(bootSetId)
      failed <- recoverToSucceededIf[AllocationError](set.selectDeviceForStore(500L, Set.empty, client))
      fits <- set.selectDeviceForStore(400L, Set.empty, client)
    yield
      before should be(net.secondDeviceId)
      fits should be(net.secondDeviceId)
      failed

  atest("selectDeviceForStore counts a planned, not-yet-started transfer against its destination"):
    given ExecutionContext = executionContext
    for
      _ <- net.createSecondDevice()
      _ <- waitForTransactionsToComplete()
      _ <- setDeviceState(StorageDeviceId.BootstrapStorageDeviceId)(_.copy(totalSize = 100L))
      _ <- setStoreSize(phantom, 999_600L)
      setPtr <- client.getStorageDeviceSetPointer(bootSetId)
      setDos <- client.read(setPtr)
      _ <- client.transactUntilSuccessful: tx =>
             tx.overwrite(setPtr, setDos.revision, DataBuffer(StorageDeviceSetState(setDos).copy(
               pendingTransfers = List((phantom, StorageDeviceId.BootstrapStorageDeviceId,
                                        net.secondDeviceId))).toBytes))
             Future.unit
      _ <- waitForTransactionsToComplete()
      set <- client.getStorageDeviceSetState(bootSetId)
      failed <- recoverToSucceededIf[AllocationError](set.selectDeviceForStore(500L, Set.empty, client))
    yield
      failed
