package org.aspen_ddp.aspen.vdisk

import java.util.UUID

import org.aspen_ddp.aspen.client.{AspenClient, InvalidObject, ObjectAllocator, ObjectAllocatorId, StopRetrying, Transaction}
import org.aspen_ddp.aspen.client.registries.NamespacedUUIDRegistry
import org.aspen_ddp.aspen.client.tkvl.{KVObjectRootManager, Root, SinglePoolNodeAllocator, TieredKeyValueList, TreeKind}
import org.aspen_ddp.aspen.common.{DataBuffer, Radicle}
import org.aspen_ddp.aspen.common.objects.{DataObjectPointer, Insert, IntegerKeyOrdering, Key, KeyAlreadyExists,
  KeyValueObjectPointer, ObjectPointer, Value}
import org.aspen_ddp.aspen.common.transaction.KeyValueUpdate
import org.aspen_ddp.aspen.vdisk.VDiskError.*

import scala.concurrent.{ExecutionContext, Future}

/** The catalog of virtual disks. A disk is its name (registered in the `"vdisk"` namespace, mapping to the disk id),
  * its disk id (registered in the object registry, mapping to the config object), the config object, the fence
  * object, and the block map tree plus the block objects it points to.
  */
object DiskCatalog:

  private def isAligned(size: Long, logicalBlockSize: Int): Boolean = size > 0 && size % logicalBlockSize == 0

  def create(client: AspenClient,
             name: String,
             allocator: ObjectAllocator,
             size: Long,
             logicalBlockSize: Int = 4096): Future[DiskInfo] =
    given ExecutionContext = client.clientContext

    val validation: Either[InvalidGeometry, DiskGeometry] =
      if logicalBlockSize != 512 && logicalBlockSize != 4096 then
        Left(InvalidGeometry(s"logical block size $logicalBlockSize is not 512 or 4096"))
      else if !isAligned(size, logicalBlockSize) then
        Left(InvalidGeometry(s"size $size is not a positive multiple of $logicalBlockSize"))
      else
        allocator.maxObjectSize match
          case None => Left(InvalidGeometry("allocator has no defined maxObjectSize"))
          case Some(max) =>
            DiskGeometry.blockObjectSizeFor(max, logicalBlockSize) match
              case None => Left(InvalidGeometry(s"maxObjectSize $max cannot hold a $logicalBlockSize byte block"))
              case Some(bos) => Right(DiskGeometry(size, logicalBlockSize, bos))

    validation match
      case Left(err) => Future.failed(err)
      case Right(geometry) =>
        val diskId = UUID.randomUUID()

        // The name, both allocations and the object registration commit together, so there is never a name that
        // resolves to nothing or a disk that nothing can name. A single attempt: a retry would strand the
        // allocations of the failed one, and a taken name is not something a retry could fix.
        client.transact { t =>
          given tx: Transaction = t
          for
            _ <- client.prepareRegisterId(DiskConfig.RegistryNamespace, name, diskId)
            fence <- allocator.allocateDataObject(DataBuffer(FenceState.Initial.toArray))
            // Node allocator pool follows the fence object, which the allocator has already placed
            pool <- client.getStoragePool(fence.poolId)
            root <- Root.create(client, IntegerKeyOrdering, SinglePoolNodeAllocator.forPool(client, pool), Map(),
              TreeKind.DataObjects)
            content = Map(
              DiskConfig.SizeKey -> Value(DiskConfig.encodeLong(size)),
              DiskConfig.LogicalBlockSizeKey -> Value(DiskConfig.encodeInt(logicalBlockSize)),
              DiskConfig.BlockObjectSizeKey -> Value(DiskConfig.encodeInt(geometry.blockObjectSize)),
              DiskConfig.AllocatorKey -> Value(allocator.allocatorId.toArray),
              DiskConfig.FenceKey -> Value(fence.toArray),
              DiskConfig.MapRootKey -> Value(root.encode()))
            config <- allocator.allocateKeyValueObject(content)
            _ <- client.prepareRegisterObject(diskId, config)
          yield
            DiskInfo(name, diskId, config, fence, geometry, allocator.allocatorId)
        }.recoverWith:
          case _: KeyAlreadyExists => Future.failed(DiskAlreadyExists(name))

  def load(client: AspenClient, name: String): Future[DiskInfo] =
    given ExecutionContext = client.clientContext

    for
      diskId <- client.getRegisteredId(DiskConfig.RegistryNamespace, name).recoverWith:
        case _: NoSuchElementException => Future.failed(DiskNotFound(name))
      config <- client.getRegisteredKeyValueObject(diskId).recoverWith:
        case _: NoSuchElementException => Future.failed(DiskNotFound(name))
      info <- loadConfig(client, name, diskId, config)
    yield
      info

  private def loadConfig(client: AspenClient,
                         name: String,
                         diskId: UUID,
                         config: KeyValueObjectPointer): Future[DiskInfo] =
    given ExecutionContext = client.clientContext

    client.read(config, s"Config of virtual disk $name").map { state =>
      def bytes(key: Key): Array[Byte] = state.contents(key).value.bytes
      DiskInfo(name, diskId, config,
        ObjectPointer(bytes(DiskConfig.FenceKey)).asInstanceOf[DataObjectPointer],
        DiskGeometry(DiskConfig.decodeLong(bytes(DiskConfig.SizeKey)),
          DiskConfig.decodeInt(bytes(DiskConfig.LogicalBlockSizeKey)),
          DiskConfig.decodeInt(bytes(DiskConfig.BlockObjectSizeKey))),
        ObjectAllocatorId(bytes(DiskConfig.AllocatorKey)))
    }.recoverWith:
      case _: InvalidObject => Future.failed(DiskNotFound(name))

  /** All disks, sorted by name. A disk whose deletion has begun but not finished is skipped. */
  def list(client: AspenClient): Future[List[DiskInfo]] =
    given ExecutionContext = client.clientContext

    client.listRegisteredIds(DiskConfig.RegistryNamespace).flatMap: entries =>
      Future.sequence(entries.map: (name, _) =>
        load(client, name).map(Some(_)).recover { case _: DiskNotFound => None }
      ).map(_.flatten.sortBy(_.name))

  /** Raises the disk size. Fails with `InvalidGeometry` unless `newSize` is larger than the current size and
    * aligned to the logical block size.
    */
  def grow(client: AspenClient, name: String, newSize: Long): Future[DiskInfo] =
    given ExecutionContext = client.clientContext

    load(client, name).flatMap: info =>
      client.transactUntilSuccessful { tx =>
        client.read(info.configPointer, s"Growing virtual disk $name").map: state =>
          val sizeState = state.contents(DiskConfig.SizeKey)
          val current = DiskConfig.decodeLong(sizeState.value.bytes)
          if newSize <= current then
            throw StopRetrying(InvalidGeometry(s"new size $newSize does not exceed current size $current"))
          if !isAligned(newSize, info.geometry.logicalBlockSize) then
            throw StopRetrying(InvalidGeometry(
              s"new size $newSize is not a multiple of ${info.geometry.logicalBlockSize}"))
          tx.update(info.configPointer, None, None,
            KeyValueUpdate.KeyRevision(DiskConfig.SizeKey, sizeState.revision) :: Nil,
            Insert(DiskConfig.SizeKey, DiskConfig.encodeLong(newSize)) :: Nil)
          info.copy(geometry = info.geometry.copy(size = newSize))
      }

  /** Makes `owner` the disk's owner, fencing the previous one. Returns the new state and the revision at which it
    * committed.
    */
  def takeOwnership(client: AspenClient, info: DiskInfo, owner: UUID): Future[(FenceState, org.aspen_ddp.aspen.common.objects.ObjectRevision)] =
    given ExecutionContext = client.clientContext

    client.transactUntilSuccessful { tx =>
      client.read(info.fencePointer, s"Fence of virtual disk ${info.name}").map: state =>
        val next = FenceState(FenceState.fromArray(state.data.getByteArray).epoch + 1, owner)
        tx.overwrite(info.fencePointer, state.revision, DataBuffer(next.toArray))
        (next, tx.revision)
    }

  /** Deletes the disk and everything it holds. Each step can be re-run, so a crash midway leaves a disk that
    * `delete` can finish. Takes ownership first, which fences any live owner.
    */
  def delete(client: AspenClient, name: String): Future[Unit] =
    given ExecutionContext = client.clientContext

    def deleteBlock(value: Value): Future[Unit] =
      val ptr = ObjectPointer(value.bytes).asInstanceOf[DataObjectPointer]
      client.retryStrategy.retryUntilSuccessful:
        client.read(ptr, s"Deleting block of virtual disk $name").flatMap: state =>
          client.transact: tx =>
            tx.setRefcount(ptr, state.refcount, state.refcount.update(0))
            tx.deferAllocationRelease()
            Future.unit
        .recover:
          case _: InvalidObject => ()  // already deleted by an earlier run

    def deleteContents(info: DiskInfo): Future[Unit] =
      for
        _ <- takeOwnership(client, info, UUID.randomUUID())
        manager = KVObjectRootManager(client, DiskConfig.MapRootKey, info.configPointer)
        tree <- manager.getDataObjectTree()
        _ <- tree.deleteTree(Some((_, v) => deleteBlock(v)))
        _ <- client.transactUntilSuccessful: tx =>
          for
            config <- client.read(info.configPointer)
            fence <- client.read(info.fencePointer)
          yield
            tx.setRefcount(config.pointer, config.refcount, config.refcount.update(0))
            tx.setRefcount(fence.pointer, fence.refcount, fence.refcount.update(0))
      yield
        ()

    def removeKey(treeKey: Key, key: Key): Future[Unit] =
      val tree = TieredKeyValueList(client, KVObjectRootManager(client, treeKey, Radicle.pointer))
      client.transactUntilSuccessful: tx =>
        given Transaction = tx
        tree.prepareUpdates(Nil, List(key))

    for
      diskId <- client.getRegisteredId(DiskConfig.RegistryNamespace, name).recoverWith:
        case _: NoSuchElementException => Future.failed(DiskNotFound(name))
      oconfig <- client.getRegisteredKeyValueObject(diskId).map(Some(_)).recover:
        case _: NoSuchElementException => None
      oinfo <- oconfig match
        case None => Future.successful(None)
        case Some(config) => loadConfig(client, name, diskId, config).map(Some(_)).recover:
          case _: DiskNotFound => None  // config already deleted
      _ <- oinfo match
        case Some(info) => deleteContents(info)
        case None => Future.unit
      // Object registration first: until the name is removed, a re-run still finds the disk to finish
      _ <- removeKey(Radicle.ObjectRegistryKey, Key(diskId))
      _ <- removeKey(Radicle.NamespacedRegistryKey, NamespacedUUIDRegistry.makeKey(DiskConfig.RegistryNamespace, name))
    yield
      ()
