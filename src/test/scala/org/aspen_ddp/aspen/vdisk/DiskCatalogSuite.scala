package org.aspen_ddp.aspen.vdisk

import java.util.UUID

import org.aspen_ddp.aspen.IntegrationTestSuite
import org.aspen_ddp.aspen.client.{GroupObjectAllocatorId, ObjectAllocator}
import org.aspen_ddp.aspen.client.internal.allocation.PoolObjectAllocator
import org.aspen_ddp.aspen.client.Transaction
import org.aspen_ddp.aspen.client.tkvl.{KVObjectRootManager, Root, TieredKeyValueList}
import org.aspen_ddp.aspen.common.{DataBuffer, Radicle}
import org.aspen_ddp.aspen.client.registries.NamespacedUUIDRegistry
import org.aspen_ddp.aspen.common.objects.{Key, Value}
import org.aspen_ddp.aspen.common.ida.{IDA, RandomAccessReedSolomon}
import org.aspen_ddp.aspen.common.metadata.AllocationGroupState

import scala.concurrent.Future

class DiskCatalogSuite extends IntegrationTestSuite:

  override def testIda: IDA = RandomAccessReedSolomon(5, 3, 4, 64)
  override def testPoolMaxSliceSize: Option[Int] =
    Some(RandomAccessReedSolomon(5, 3, 4, 64).minSliceSizeFor(16384).toInt)

  private def allocator(): Future[ObjectAllocator] =
    client.getStoragePool(Radicle.poolId).map(pool => new PoolObjectAllocator(client, pool))

  atest("create then load round-trips geometry"):
    for
      alloc <- allocator()
      created <- DiskCatalog.create(client, "d1", alloc, 65536)
      loaded <- DiskCatalog.load(client, "d1")
    yield
      created.geometry should be (DiskGeometry(65536, 4096, 16384))
      loaded should be (created)
      loaded.allocatorId should be (alloc.allocatorId)

  atest("a disk created in a group allocates its block map's nodes through the group"):
    for
      groupId <- client.createAllocationGroup("disk-group", level = 0, maxSliceSize = testPoolMaxSliceSize,
        ida = Some(testIda))
      _ <- AllocationGroupState.addPool(client, Radicle.poolId, groupId)
      alloc <- client.getAllocator(GroupObjectAllocatorId(groupId))
      info <- DiskCatalog.create(client, "d1", alloc, 65536)
      config <- client.read(info.configPointer)
      tree <- KVObjectRootManager(client, DiskConfig.MapRootKey, info.configPointer).getDataObjectTree()
      ptr <- client.transactUntilSuccessful: tx =>
        given Transaction = tx
        for
          ptr <- alloc.allocateDataObject(DataBuffer(new Array[Byte](16)))
          _ <- tree.set(Key(0L), Value(ptr.toArray))
        yield ptr
      mapped <- tree.get(Key(0L))
    yield
      info.allocatorId should be (GroupObjectAllocatorId(groupId))
      val root = Root(client, config.contents(DiskConfig.MapRootKey).value.bytes)
      root.nodeAllocator.tiers.map(_._1).toList should be (List(GroupObjectAllocatorId(groupId)))
      mapped.map(_.bytes.toList) should be (Some(ptr.toArray.toList))

  atest("create rejects a duplicate name"):
    for
      alloc <- allocator()
      _ <- DiskCatalog.create(client, "d1", alloc, 65536)
      r <- DiskCatalog.create(client, "d1", alloc, 65536).failed
    yield
      r should be (VDiskError.DiskAlreadyExists("d1"))

  atest("create rejects a name containing '@' or an empty name"):
    for
      alloc <- allocator()
      r1 <- DiskCatalog.create(client, "a@b", alloc, 65536).failed
      r2 <- DiskCatalog.create(client, "", alloc, 65536).failed
    yield
      r1 shouldBe a [VDiskError.InvalidRequest]
      r2 shouldBe a [VDiskError.InvalidRequest]

  atest("create rejects unaligned size"):
    for
      alloc <- allocator()
      r1 <- DiskCatalog.create(client, "d1", alloc, 65537).failed
      r2 <- DiskCatalog.create(client, "d2", alloc, 0).failed
      r3 <- DiskCatalog.create(client, "d3", alloc, 4096, 1024).failed
      r4 <- DiskCatalog.create(client, "d4", alloc, 4096 * 4, 512).map(_.geometry.blockObjectSize)
    yield
      r1 shouldBe a [VDiskError.InvalidGeometry]
      r2 shouldBe a [VDiskError.InvalidGeometry]
      r3 shouldBe a [VDiskError.InvalidGeometry]
      r4 should be (16384)

  atest("load of a missing disk fails with DiskNotFound"):
    DiskCatalog.load(client, "nope").failed.map(_ should be (VDiskError.DiskNotFound("nope")))

  atest("list returns disks by name"):
    for
      alloc <- allocator()
      _ <- DiskCatalog.create(client, "b", alloc, 65536)
      _ <- DiskCatalog.create(client, "a", alloc, 8192)
      _ <- DiskCatalog.create(client, "c", alloc, 16384)
      disks <- DiskCatalog.list(client)
    yield
      disks.map(_.name) should be (List("a", "b", "c"))
      disks.map(_.geometry.size) should be (List(8192L, 65536L, 16384L))

  atest("grow raises size, is idempotent and refuses shrink"):
    for
      alloc <- allocator()
      _ <- DiskCatalog.create(client, "d1", alloc, 65536)
      grown <- DiskCatalog.grow(client, "d1", 131072)
      loaded <- DiskCatalog.load(client, "d1")
      same <- DiskCatalog.grow(client, "d1", 131072)
      shrink <- DiskCatalog.grow(client, "d1", 65536).failed
      unaligned <- DiskCatalog.grow(client, "d1", 131072 + 1).failed
      after <- DiskCatalog.load(client, "d1")
    yield
      grown.geometry.size should be (131072L)
      loaded.geometry should be (DiskGeometry(131072, 4096, 16384))
      same.geometry should be (DiskGeometry(131072, 4096, 16384))
      shrink shouldBe a [VDiskError.InvalidGeometry]
      unaligned shouldBe a [VDiskError.InvalidGeometry]
      after.geometry.size should be (131072L)

  atest("takeOwnership increments the epoch"):
    val o1 = UUID.randomUUID()
    val o2 = UUID.randomUUID()
    for
      alloc <- allocator()
      info <- DiskCatalog.create(client, "d1", alloc, 65536)
      (s1, r1) <- DiskCatalog.takeOwnership(client, info, o1)
      (s2, r2) <- DiskCatalog.takeOwnership(client, info, o2)
      state <- client.read(info.fencePointer)
    yield
      s1 should be (FenceState(1, o1))
      s2 should be (FenceState(2, o2))
      r1 should not be r2
      state.revision should be (r2)
      FenceState.fromArray(state.data.getByteArray) should be (s2)

  atest("delete removes the disk and its registrations"):
    for
      alloc <- allocator()
      info <- DiskCatalog.create(client, "d1", alloc, 65536)
      _ <- DiskCatalog.create(client, "d2", alloc, 65536)
      _ <- DiskCatalog.delete(client, "d1")
      r <- DiskCatalog.load(client, "d1").failed
      ids <- client.listRegisteredIds("vdisk")
      obj <- client.getRegisteredKeyValueObject(info.diskId).failed
      cfg <- client.read(info.configPointer).failed
      again <- DiskCatalog.delete(client, "d1").failed
      d2 <- DiskCatalog.load(client, "d2")
    yield
      r should be (VDiskError.DiskNotFound("d1"))
      ids.map(_._1) should be (List("d2"))
      obj shouldBe a [NoSuchElementException]
      cfg shouldBe a [org.aspen_ddp.aspen.client.InvalidObject]
      again should be (VDiskError.DiskNotFound("d1"))
      d2.name should be ("d2")

  atest("delete deletes the block objects held by the map"):
    for
      alloc <- allocator()
      info <- DiskCatalog.create(client, "d1", alloc, 65536)
      tree <- KVObjectRootManager(client, DiskConfig.MapRootKey, info.configPointer).getDataObjectTree()
      blocks <- Future.sequence((0 until 3).toList.map: i =>
        client.transactUntilSuccessful: tx =>
          given Transaction = tx
          for
            ptr <- alloc.allocateDataObject(DataBuffer(new Array[Byte](16)))
            _ <- tree.set(Key(i.toLong), Value(ptr.toArray))
          yield ptr)
      _ <- DiskCatalog.delete(client, "d1")
      reads <- Future.sequence(blocks.map(b => client.read(b).failed))
    yield
      reads.foreach(_ shouldBe a [org.aspen_ddp.aspen.client.InvalidObject])
      succeed

  private def zeroRefcounts(info: DiskInfo): Future[Unit] =
    client.transactUntilSuccessful: tx =>
      for
        config <- client.read(info.configPointer)
        fence <- client.read(info.fencePointer)
      yield
        tx.setRefcount(config.pointer, config.refcount, config.refcount.update(0))
        tx.setRefcount(fence.pointer, fence.refcount, fence.refcount.update(0))

  private def removeKey(treeKey: Key, key: Key): Future[Unit] =
    val tree = TieredKeyValueList(client, KVObjectRootManager(client, treeKey, Radicle.pointer))
    client.transactUntilSuccessful: tx =>
      given Transaction = tx
      tree.prepareUpdates(Nil, List(key))

  atest("takeOwnership on a deleted disk fails with DiskNotFound"):
    for
      alloc <- allocator()
      info <- DiskCatalog.create(client, "d1", alloc, 65536)
      _ <- DiskCatalog.delete(client, "d1")
      r <- DiskCatalog.takeOwnership(client, info, UUID.randomUUID()).failed
    yield
      r should be (VDiskError.DiskNotFound("d1"))

  atest("grow on a deleted disk fails with DiskNotFound"):
    for
      alloc <- allocator()
      info <- DiskCatalog.create(client, "d1", alloc, 65536)
      _ <- zeroRefcounts(info)
      r <- DiskCatalog.grow(client, "d1", 131072).failed
    yield
      r should be (VDiskError.DiskNotFound("d1"))

  atest("two concurrent deletes of one disk both finish"):
    for
      alloc <- allocator()
      _ <- DiskCatalog.create(client, "d1", alloc, 65536)
      rs <- Future.sequence(List(DiskCatalog.delete(client, "d1"), DiskCatalog.delete(client, "d1"))
        .map(_.map(_ => None).recover { case e: VDiskError.DiskNotFound => Some(e) }))
      r <- DiskCatalog.load(client, "d1").failed
      ids <- client.listRegisteredIds("vdisk")
    yield
      rs.count(_.isEmpty) should be >= 1
      r should be (VDiskError.DiskNotFound("d1"))
      ids should be (Nil)

  atest("delete finishes after config and fence were deleted but registrations remain"):
    for
      alloc <- allocator()
      info <- DiskCatalog.create(client, "d1", alloc, 65536)
      _ <- zeroRefcounts(info)
      _ <- DiskCatalog.delete(client, "d1")
      r <- DiskCatalog.load(client, "d1").failed
      ids <- client.listRegisteredIds("vdisk")
      obj <- client.getRegisteredKeyValueObject(info.diskId).failed
    yield
      r should be (VDiskError.DiskNotFound("d1"))
      ids should be (Nil)
      obj shouldBe a [NoSuchElementException]

  atest("delete finishes after the object registration was removed but the name remains"):
    for
      alloc <- allocator()
      info <- DiskCatalog.create(client, "d1", alloc, 65536)
      _ <- removeKey(Radicle.ObjectRegistryKey, Key(info.diskId))
      _ <- DiskCatalog.delete(client, "d1")
      r <- DiskCatalog.load(client, "d1").failed
      ids <- client.listRegisteredIds("vdisk")
    yield
      r should be (VDiskError.DiskNotFound("d1"))
      ids should be (Nil)

  atest("delete stops with Fenced when a newer owner takes the disk mid-delete"):
    for
      alloc <- allocator()
      info <- DiskCatalog.create(client, "d1", alloc, 65536)
      tree <- KVObjectRootManager(client, DiskConfig.MapRootKey, info.configPointer).getDataObjectTree()
      ptr <- client.transactUntilSuccessful: tx =>
        given Transaction = tx
        for
          p <- alloc.allocateDataObject(DataBuffer(new Array[Byte](16)))
          _ <- tree.set(Key(0L), Value(p.toArray))
        yield p
      // a block deleted under a stale fence revision must not commit
      stale <- client.read(info.fencePointer)
      _ <- DiskCatalog.takeOwnership(client, info, UUID.randomUUID())
      r <- DiskCatalog.deleteBlock(client, info, ptr, stale.revision).failed
      still <- client.read(ptr)
    yield
      r should be (VDiskError.Fenced)
      still.pointer should be (ptr)

  atest("delete fails and keeps the registrations when the config still exists but the fence is gone"):
    for
      alloc <- allocator()
      info <- DiskCatalog.create(client, "d1", alloc, 65536)
      fence <- client.read(info.fencePointer)
      _ <- client.transactUntilSuccessful: tx =>
        tx.setRefcount(fence.pointer, fence.refcount, fence.refcount.update(0))
        Future.unit
      r <- DiskCatalog.delete(client, "d1").failed
      ids <- client.listRegisteredIds("vdisk")
      obj <- client.getRegisteredKeyValueObject(info.diskId)
      config <- client.read(info.configPointer)
    yield
      r shouldBe a [VDiskError]
      ids.map(_._1) should be (List("d1"))
      config.pointer should be (obj)

  atest("two concurrent deletes of a disk with blocks both finish"):
    for
      alloc <- allocator()
      info <- DiskCatalog.create(client, "d1", alloc, 65536)
      tree <- KVObjectRootManager(client, DiskConfig.MapRootKey, info.configPointer).getDataObjectTree()
      blocks <- Future.sequence((0 until 3).toList.map: i =>
        client.transactUntilSuccessful: tx =>
          given Transaction = tx
          for
            ptr <- alloc.allocateDataObject(DataBuffer(new Array[Byte](16)))
            _ <- tree.set(Key(i.toLong), Value(ptr.toArray))
          yield ptr)
      rs <- Future.sequence(List(DiskCatalog.delete(client, "d1"), DiskCatalog.delete(client, "d1"))
        .map(_.map(_ => None).recover { case e: VDiskError.DiskNotFound => Some(e) }))
      r <- DiskCatalog.load(client, "d1").failed
      ids <- client.listRegisteredIds("vdisk")
      reads <- Future.sequence(blocks.map(b => client.read(b).failed))
    yield
      rs.count(_.isEmpty) should be >= 1
      r should be (VDiskError.DiskNotFound("d1"))
      ids should be (Nil)
      reads.foreach(_ shouldBe a [org.aspen_ddp.aspen.client.InvalidObject])
      succeed
