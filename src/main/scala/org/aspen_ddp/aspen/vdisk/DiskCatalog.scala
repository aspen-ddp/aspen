package org.aspen_ddp.aspen.vdisk

import java.util.UUID

import org.aspen_ddp.aspen.client.{AspenClient, InvalidObject, TransactionAborted, ObjectAllocator, ObjectAllocatorId, StopRetrying, Transaction}
import org.aspen_ddp.aspen.client.registries.NamespacedUUIDRegistry
import org.aspen_ddp.aspen.client.tkvl.{KVObjectRootManager, NodeAllocator, Root, SinglePoolNodeAllocator, TieredKeyValueList,
  TreeKind}
import org.aspen_ddp.aspen.common.{DataBuffer, Radicle}
import org.aspen_ddp.aspen.common.objects.{DataObjectPointer, Insert, IntegerKeyOrdering, Key, KeyAlreadyExists,
  KeyValueObjectPointer, ObjectPointer, ObjectRevision, Value}
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

    val validation: Either[VDiskError, DiskGeometry] =
      if name.isEmpty || name.contains('@') then
        Left(InvalidRequest(s"invalid disk name '$name': must be non-empty and contain no '@'"))
      else if logicalBlockSize != 512 && logicalBlockSize != 4096 then
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
            // The map's nodes come from the disk's allocator too, so neither the map nor the blocks are confined to
            // one pool of a group
            nodes = new NodeAllocator(client, List((allocator.allocatorId,
              NodeAllocator.fittingNodeSize(SinglePoolNodeAllocator.DefaultMaxNodeSize,
                NodeAllocator.nodeSizeLimit(allocator, TreeKind.DataObjects)))))
            root <- Root.create(client, IntegerKeyOrdering, nodes, Map(), TreeKind.DataObjects)
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

  /** Raises the disk size to `newSize`, which must be aligned to the logical block size. Idempotent: a disk whose
    * stored size already equals `newSize` is returned as it is, so a grow whose reply was lost (or one already done
    * through another path) can be repeated. A stored size larger than `newSize` fails with `InvalidGeometry`, since
    * disks never shrink. The stored size is re-read on every attempt, so an attempt that committed despite reporting
    * a failure is recognized by the retry.
    */
  def grow(client: AspenClient, name: String, newSize: Long): Future[DiskInfo] =
    given ExecutionContext = client.clientContext

    load(client, name).flatMap: info =>
      val grown = info.copy(geometry = info.geometry.copy(size = newSize))
      client.transactUntilSuccessful { tx =>
        stopOnMissing(name, client.read(info.configPointer, s"Growing virtual disk $name")).map: state =>
          val sizeState = state.contents.getOrElse(DiskConfig.SizeKey,
            throw StopRetrying(DiskFailed(s"config of $name has no size")))
          val current = DiskConfig.decodeLong(sizeState.value.bytes)
          if newSize == current then
            throw StopRetrying(AlreadyAtSize)
          if newSize < current then
            throw StopRetrying(InvalidGeometry(s"new size $newSize is smaller than the current size $current"))
          if !isAligned(newSize, info.geometry.logicalBlockSize) then
            throw StopRetrying(InvalidGeometry(
              s"new size $newSize is not a multiple of ${info.geometry.logicalBlockSize}"))
          tx.update(info.configPointer, None, None,
            KeyValueUpdate.KeyRevision(DiskConfig.SizeKey, sizeState.revision) :: Nil,
            Insert(DiskConfig.SizeKey, DiskConfig.encodeLong(newSize)) :: Nil)
          grown
      }.recover:
        case AlreadyAtSize => grown

  /** Ends a grow attempt that finds the size already committed; never escapes `grow` */
  private object AlreadyAtSize extends Exception("disk already has the requested size"):
    override def fillInStackTrace(): Throwable = this

  /** Makes `owner` the disk's owner, fencing the previous one. Returns the new state and the revision at which it
    * committed.
    */
  def takeOwnership(client: AspenClient, info: DiskInfo, owner: UUID): Future[(FenceState, ObjectRevision)] =
    given ExecutionContext = client.clientContext

    client.transactUntilSuccessful { tx =>
      stopOnMissing(info.name, client.read(info.fencePointer, s"Fence of virtual disk ${info.name}")).map: state =>
        val current =
          try FenceState.fromArray(state.data.getByteArray)
          catch case e: IllegalArgumentException => throw StopRetrying(DiskFailed(s"malformed fence: ${e.getMessage}"))
        val next = FenceState(current.epoch + 1, owner)
        tx.overwrite(info.fencePointer, state.revision, DataBuffer(next.toArray))
        (next, tx.revision)
    }

  /** A deleted object can never be read again, so a retry loop must give up rather than spin */
  private def stopOnMissing[A](name: String, read: Future[A])(using ExecutionContext): Future[A] =
    read.recoverWith:
      case _: InvalidObject => Future.failed(StopRetrying(DiskNotFound(name)))

  /** Sets the block's refcount to 0, under a shared lock on the fence revision the caller took ownership at. Fails
    * with `Fenced` once the fence has moved on, since a newer owner may be adding blocks.
    */
  private[vdisk] def deleteBlock(client: AspenClient,
                                 info: DiskInfo,
                                 ptr: DataObjectPointer,
                                 fenceRevision: ObjectRevision): Future[Unit] =
    given ExecutionContext = client.clientContext

    client.retryStrategy.retryUntilSuccessful:
      client.read(ptr, s"Deleting block of virtual disk ${info.name}").flatMap: state =>
        client.transact: tx =>
          tx.sharedLockRevision(info.fencePointer, fenceRevision)
          tx.setRefcount(ptr, state.refcount, state.refcount.update(0))
          tx.deferAllocationRelease()
          Future.unit
      .recoverWith:
        case _: InvalidObject => Future.unit  // already deleted by an earlier run
        case e: StopRetrying => Future.failed(e)
        case e =>
          // A failed commit may be the shared lock losing to a newer owner, which no retry can fix
          client.read(info.fencePointer).flatMap: fence =>
            if fence.revision != fenceRevision then
              Future.failed(StopRetrying(Fenced))
            else
              Future.failed(e)

  /** Deletes the disk and everything it holds. Each step can be re-run, so a crash midway leaves a disk that
    * `delete` can finish. Takes ownership first, which fences any live owner.
    */
  def delete(client: AspenClient, name: String): Future[Unit] =
    given ExecutionContext = client.clientContext

    def deleteContents(info: DiskInfo): Future[Unit] =
      for
        (_, fenceRevision) <- takeOwnership(client, info, UUID.randomUUID())
        manager = KVObjectRootManager(client, DiskConfig.MapRootKey, info.configPointer)
        tree <- manager.getDataObjectTree()
        // Block deletions carry delete's own fence revision, so a newer owner cannot add blocks that then leak
        _ <- tree.deleteTree(Some((_, v) =>
          deleteBlock(client, info, ObjectPointer(v.bytes).asInstanceOf[DataObjectPointer], fenceRevision)))
          .recoverWith:
            // The tree walk's own transactions are not retried: an abort starts delete over, as Fenced does. Most
            // often a concurrent delete took ownership after ours, but the loser of a race can be the newer owner
            // too: an older delete that has not yet noticed it was fenced is still walking the same nodes. Starting
            // over takes ownership again, which fences it, and every step is safe to repeat. Anything else is a real
            // failure.
            case _: TransactionAborted => Future.failed(Fenced)
        _ <- deleteConfigAndFence(info)
      yield
        ()

    // Whichever of the two is already gone is skipped; with both gone there is nothing to do
    def deleteConfigAndFence(info: DiskInfo): Future[Unit] =
      def readOpt[A](f: Future[A]): Future[Option[A]] = f.map(Some(_)).recover { case _: InvalidObject => None }
      client.retryStrategy.retryUntilSuccessful:
        for
          config <- readOpt(client.read(info.configPointer))
          fence <- readOpt(client.read(info.fencePointer))
          _ <- if config.isEmpty && fence.isEmpty then Future.unit else
            client.transact: tx =>
              config.foreach(c => tx.setRefcount(c.pointer, c.refcount, c.refcount.update(0)))
              fence.foreach(f => tx.setRefcount(f.pointer, f.refcount, f.refcount.update(0)))
              Future.unit
        yield
          ()

    // Skipped when already absent: a transaction that deletes nothing has nothing to commit
    def removeKey(treeKey: Key, key: Key): Future[Unit] =
      val tree = TieredKeyValueList(client, KVObjectRootManager(client, treeKey, Radicle.pointer))
      client.retryStrategy.retryUntilSuccessful:
        tree.get(key).flatMap:
          case None => Future.unit
          case Some(_) =>
            client.transact: tx =>
              given Transaction = tx
              tree.prepareUpdates(Nil, List(key))

    // Only a vanished config proves another delete finished the job. Any other failure (a missing map node,
    // a missing fence under a live config) must leave the registrations so that delete can be re-run.
    def configGone(info: DiskInfo): Future[Boolean] =
      client.read(info.configPointer).map(_ => false).recover { case _: InvalidObject => true }

    def attempt(restarted: Boolean): Future[Unit] =
      val run = for
        diskId <- client.getRegisteredId(DiskConfig.RegistryNamespace, name).recoverWith:
          case _: NoSuchElementException => Future.failed(DiskNotFound(name))
        oconfig <- client.getRegisteredKeyValueObject(diskId).map(Some(_)).recover:
          case _: NoSuchElementException => None
        oinfo <- oconfig match
          case None => Future.successful(None)
          case Some(config) => loadConfig(client, name, diskId, config).map(Some(_)).recover:
            case _: DiskNotFound => None  // config already deleted
        _ <- oinfo match
          case Some(info) => deleteContents(info).recoverWith:
            case e @ (_: DiskNotFound | _: InvalidObject) =>
              configGone(info).flatMap: gone =>
                if gone then Future.unit else Future.failed(e)
          case None => Future.unit
        // Object registration first: until the name is removed, a re-run still finds the disk to finish
        _ <- removeKey(Radicle.ObjectRegistryKey, Key(diskId))
        _ <- removeKey(Radicle.NamespacedRegistryKey,
          NamespacedUUIDRegistry.makeKey(DiskConfig.RegistryNamespace, name))
      yield
        ()
      run.recoverWith:
        // A concurrent delete took ownership after ours. Start over: it ends once the disk is gone.
        case Fenced => attempt(restarted = true)
        case _: DiskNotFound if restarted => Future.unit

    attempt(restarted = false)
