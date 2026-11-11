package org.aspen_ddp.aspen.amoebafs.impl.simple

import scala.language.implicitConversions
import org.aspen_ddp.aspen.client.KeyValueObjectState.ValueState
import org.aspen_ddp.aspen.client.{AspenClient, KeyValueObjectState, StopRetrying, Transaction}
import org.aspen_ddp.aspen.client.tkvl.{KVObjectRootManager, KeyValueListNode, NoCache, PutRequirement, Root, TKVLInsert, TieredKeyValueList}
import org.aspen_ddp.aspen.common.objects.{Delete, Key, ObjectRevision, Value}
import org.aspen_ddp.aspen.common.transaction.KeyValueUpdate
import org.aspen_ddp.aspen.amoebafs.error.{DirectoryEntryDoesNotExist, DirectoryEntryExists, DirectoryNotEmpty}
import org.aspen_ddp.aspen.amoebafs.{BaseFile, Directory, DirectoryEntry, DirectoryInode, DirectoryPointer, FileSystem, InodePointer}
import scribe.Logging
import org.aspen_ddp.aspen.common.util.{byte2uuid, uuid2byte}
import org.aspen_ddp.aspen.compute.{DurableTask, DurableTaskFactory, DurableTaskPointer, TaskExecutor}

import java.util.UUID
import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.{ExecutionContext, Future, Promise}

class SimpleDirectory(override val pointer: DirectoryPointer,
                      cachedInodeRevision: ObjectRevision,
                      initialInode: DirectoryInode,
                      fs: FileSystem) extends SimpleBaseFile(pointer, cachedInodeRevision, initialInode, fs) with Directory with Logging {

  import SimpleDirectory.*

  override def inode: DirectoryInode = super.inode.asInstanceOf[DirectoryInode]

  def tree: TieredKeyValueList = {
    val rootManager = new SimpleDirectoryRootManager(fs.client, pointer.pointer)
    new TieredKeyValueList(fs.client, rootManager)
  }

  override def isEmpty(): Future[Boolean] = {
    tree.foldLeft(true)( (s, t) => s && t.isEmpty)
  }

  override def getContents(): Future[List[DirectoryEntry]] = {

    def scan(sum: List[DirectoryEntry], m: Map[Key, ValueState]): List[DirectoryEntry] = {
      m.foldLeft(sum) { (s, t) =>
        DirectoryEntry(t._1.stringValue, InodePointer(t._2.value.bytes)) :: s
      }
    }

    tree.foldLeft(List[DirectoryEntry]())(scan).map(l => l.reverse)
  }

  override def getEntry(name: String): Future[Option[InodePointer]] = {
    for {
      ovs <- tree.get(Key(name))
    } yield {
      ovs.map(vs => InodePointer(vs.value.bytes))
    }
  }

  override def prepareInsert(name: String,
                             pointer: InodePointer,
                             incref: Boolean)(using tx: Transaction): Future[Unit] = {
    val key = Key(name)

    val fcleanupOldEntry = tree.get(key).flatMap:
      case None => Future.successful(None)
      case Some(vs) =>
        val oldPtr = InodePointer(vs.value.bytes)
        checkForDeletion(oldPtr).flatMap: _ =>
          UnlinkFileTask.prepareTask(fs, oldPtr).map(_ => Some(vs.revision))

    val fincref = if (incref) {
      fs.readInode(pointer) map { t =>
        val (finode, _, frevision) = t
        val updatedInode = finode.update(links=Some(finode.links+1))
        tx.overwrite(pointer.pointer, frevision, updatedInode.toArray)
      }
    } else {
      Future.unit
    }

    // The entry's requirement follows the read above: a name seen absent must still be absent, so a concurrent
    // create of it is never overwritten (its inode would leak), and a replaced entry must be the one unlinked.
    // AtRevision is a pin (KeyRevision with the Insert); for an absent name the pin comes from the incref inode
    // overwrite or the caller's other writes.
    for {
      oOldRevision <- fcleanupOldEntry
      _ <- fincref
      requirement = oOldRevision.fold(PutRequirement.MustNotExist)(rev => PutRequirement.AtRevision(rev))
      _ <- tree.set(key, Value(pointer.toArray), requirement)
    } yield ()
  }

  private def checkForDeletion(ptr: InodePointer)(using tx: Transaction): Future[Unit] =
    ptr match
      case dptr: DirectoryPointer =>
        for
          d <- fs.loadDirectory(dptr)
          (_, revision) <- d.getInode()
          isEmpty <- d.isEmpty()
        yield
          if !isEmpty then
            throw DirectoryNotEmpty(dptr)
          else
            // Use this to ensure that no entries are added to the directory
            // while we're in the process of deleting it
            tx.bumpVersion(d.pointer.pointer, revision)

      case _ => Future.unit

  override def prepareDelete(name: String, decref: Boolean)(using tx: Transaction): Future[Future[Unit]] = {
    val key = Key(name)

    def onptr(ovs: Option[ValueState]): Future[Future[Unit]] = ovs match
      case None => Future.failed(DirectoryEntryDoesNotExist(pointer, name))
      case Some(vs) =>
        val fptr = InodePointer(vs.value.bytes)
        // Only a decref removes the inode. Without it (a move) the inode lives on under another entry, so a
        // non-empty directory may go and its inode is left for the move to update.
        val fcheck = if decref then checkForDeletion(fptr) else Future.unit

        for
          _ <- fcheck
          // KeyRevision of the entry as read: the pin, and a concurrent change of the entry aborts. Gone by the time
          // the delete reads its node (a concurrent unlink), nothing is deleted and the pin is missing: fail as if
          // the entry had been absent from the start
          deleted <- tree.deleteIfPresent(key, None, List(KeyValueUpdate.KeyRevision(key, vs.revision)))
          _ <- if deleted then Future.unit else Future.failed(DirectoryEntryDoesNotExist(pointer, name))
          ftaskComplete <- if (decref)
            UnlinkFileTask.prepareTask(fs, fptr).map: f =>
              f.map(_ => ())
          else
            Future.successful(Future.unit)

        yield
          ftaskComplete

    for
      ovs <- tree.get(key)
      ftaskComplete <- onptr(ovs)
    yield
      ftaskComplete
  }

  override def prepareRename(oldName: String, newName: String)(using tx: Transaction): Future[Unit] =
    if oldName == newName then
      Future.unit
    else
      val oldKey = Key(oldName)
      val newKey = Key(newName)
      val tr = tree
      for
        oold <- tr.get(oldKey)
        onew <- tr.get(newKey)
        old = oold.getOrElse(throw DirectoryEntryDoesNotExist(pointer, oldName))
        // An entry already under the new name is replaced: unlink it
        _ <- onew match
          case None => Future.unit
          case Some(vs) =>
            val iptr = InodePointer(vs.value.bytes)
            checkForDeletion(iptr).flatMap: _ =>
              UnlinkFileTask.prepareTask(fs, iptr)
        // One batch, so a node is updated once even when deleting the old key joins its node into the new key's.
        // Both keys are written under the state just read: the old one is deleted at its revision as read (the pin;
        // a concurrent change of it aborts, so the value moved is the one read), the new one requires its revision
        // as read, or that it is still absent, so the entry unlinked above is the one replaced
        newInsert = onew match
          case None => TKVLInsert(newKey, old.value, requireDoesNotExist = true)
          case Some(vs) => TKVLInsert(newKey, old.value, requiredRevision = Some(vs.revision))
        _ <- tr.prepareUpdates(List(newInsert), Nil, List(oldKey -> old.revision))
      yield
        ()

  override def prepareHardLink(name: String, file: BaseFile)(using tx: Transaction): Future[Unit] =
    getEntry(name).flatMap:
      case None => prepareInsert(name, file.pointer)
      case Some(_) => throw DirectoryEntryExists(this.pointer, name)
}

object SimpleDirectory:
  /** Deletes the content tree of a directory whose inode is gone, unlinking every entry still in it (a create
    * racing the directory's removal can leave one). The tree's root is held in the task object, not the inode,
    * since the transaction enrolling the task deletes the inode.
    */
  object DeleteDirectoryContentTask extends DurableTaskFactory:
    private val FileSystemUUIDKey: Key = Key(1)
    private val RootKey: Key = Key(2)

    val typeUUID: UUID = UUID.fromString("c1fb782f-7f13-4921-8ddf-155123445730")

    override def createTask(client: AspenClient,
                            pointer: DurableTaskPointer,
                            revision: ObjectRevision,
                            state: Map[Key, KeyValueObjectState.ValueState],
                            taskExecutor: TaskExecutor): DurableTask =
      val fsUUID = byte2uuid(state(FileSystemUUIDKey).value.bytes)
      val fs = FileSystem.getRegisteredFileSystem(fsUUID).get

      new DeleteDirectoryContentTask(client, pointer, fs)

    /** Returns a Future that completes when the task creation is prepared for commit of the transaction.
     * The inner future completes when the task finishes
     */
    def prepareTask(fs: FileSystem,
                    root: Root)
                   (using tx: Transaction, ec: ExecutionContext): Future[Future[Unit]] =
      fs.taskExecutor.prepareTask(
        DeleteDirectoryContentTask,
        List(
          FileSystemUUIDKey -> uuid2byte(fs.uuid),
          RootKey -> root.encode()
        )
      ).map: fcomplete =>
        fcomplete.map(_ => ())

    /** Signals an entry found by `deleteTree` after the drain: the drain runs again */
    private class EntryRemains extends Exception("Directory entry remains after the drain")

  class DeleteDirectoryContentTask private(client: AspenClient,
                                           val taskPointer: DurableTaskPointer,
                                           fs: FileSystem
                                          ) extends DurableTask:

    import DeleteDirectoryContentTask.*

    given ExecutionContext = client.clientContext

    private val promise = Promise[Option[AnyRef]]()

    def completed: Future[Option[AnyRef]] = promise.future

    resume()

    def resume(): Unit =
      // Read fresh: an entry seen in a stale node would fail its KeyRevision on every pass
      val tree = new TieredKeyValueList(client, new KVObjectRootManager(client, RootKey, taskPointer.kvPointer, NoCache))

      // The entry's removal and its unlink commit together, so each entry is unlinked exactly once. A failed
      // attempt (the entry moved or went) is logged by foreach and left for the next pass
      def unlinkEntry(node: KeyValueListNode, key: Key, vs: ValueState): Future[Future[Unit]] =
        val tx = client.newTransaction()
        tx.update(node.pointer, None, None, List(KeyValueUpdate.KeyRevision(key, vs.revision)), List(Delete(key)))
        for
          funlinked <- UnlinkFileTask.prepareTask(fs, InodePointer(vs.value.bytes))(using tx)
          _ <- tx.commit()
        yield funlinked.map(_ => ())

      // The unlinks enrolled by every pass, awaited before the task completes
      val unlinked = new AtomicReference(List.empty[Future[Unit]])

      // Unlinks every entry, then deletes the emptied tree. An entry deleteTree still finds (a pass that lost a
      // race, or a late insert) fails the attempt, so the drain runs again rather than the entry being dropped
      // without its unlink
      client.retryStrategy.retryUntilSuccessful:
        tree.foreach: (node, key, vs) =>
          unlinkEntry(node, key, vs).map: funlinked =>
            unlinked.updateAndGet(funlinked :: _)
            ()
        .flatMap: _ =>
          tree.deleteTree(Some((_, _) => Future.failed(EntryRemains())))
      .flatMap(_ => Future.sequence(unlinked.get))
      .onComplete: _ =>
        promise.success(None)
