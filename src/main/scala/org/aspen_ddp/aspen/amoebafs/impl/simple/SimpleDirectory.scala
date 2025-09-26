package org.aspen_ddp.aspen.amoebafs.impl.simple

import scala.language.implicitConversions
import org.aspen_ddp.aspen.client.KeyValueObjectState.ValueState
import org.aspen_ddp.aspen.client.{AspenClient, KeyValueObjectState, StopRetrying, Transaction}
import org.aspen_ddp.aspen.client.tkvl.{Root, TieredKeyValueList}
import org.aspen_ddp.aspen.common.objects.{Key, ObjectRevision, Value}
import org.aspen_ddp.aspen.amoebafs.error.{DirectoryEntryDoesNotExist, DirectoryEntryExists, DirectoryNotEmpty}
import org.aspen_ddp.aspen.amoebafs.{BaseFile, Directory, DirectoryEntry, DirectoryInode, DirectoryPointer, FileSystem, InodePointer}
import org.apache.logging.log4j.scala.Logging
import org.aspen_ddp.aspen.common.util.{byte2uuid, uuid2byte}
import org.aspen_ddp.aspen.compute.{DurableTask, DurableTaskFactory, DurableTaskPointer}

import java.util.UUID
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
    val fcheck = tree.getContainingNode(name).flatMap:
      case None => Future.unit
      case Some(node) =>
        if node.contains(name) then
          prepareDelete(name, true)
        else
          Future.unit

    val fincref = if (incref) {
      fs.readInode(pointer) map { t =>
        val (finode, _, frevision) = t
        val updatedInode = finode.update(links=Some(finode.links+1))
        tx.overwrite(pointer.pointer, frevision, updatedInode.toArray)
      }
    } else {
      Future.successful(())
    }

    for {
      _ <- fcheck
      _ <- fincref
      _ <- tree.set(Key(name), Value(pointer.toArray))
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
        val fcheck = checkForDeletion(fptr)

        for
          _ <- fcheck
          _ <- tree.delete(key)
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
        oa <- tr.getContainingNode(oldKey)
        ob <- tr.getContainingNode(newKey)
        _ <- (oa, ob) match
          case (Some(a), Some(b)) =>
            if !a.contains(oldKey) then
              Future.failed(DirectoryEntryDoesNotExist(pointer, oldKey.stringValue))
            else
              // If the directory entry already exists, unlink it
              val newNameVs = a.get(newName)

              def prepRename(): Future[Unit] =
                if a.nodeUUID == b.nodeUUID then
                  a.rename(oldKey, newKey)
                else
                  val keyValue = a.get(oldKey).get.value
                  Future.sequence(List(
                    a.delete(oldKey),
                    b.set(newKey, keyValue))).map(_ => ())

              for
                _ <- newNameVs match
                  case None => Future.unit
                  case Some(vs) =>
                    val iptr = InodePointer(vs.value.bytes)
                    checkForDeletion(iptr).flatMap: _ =>
                      UnlinkFileTask.prepareTask(fs, iptr)

                _ <- prepRename()
              yield
                ()

          case _ => Future.failed(DirectoryEntryDoesNotExist(pointer, oldName))
      yield
        ()

  override def prepareHardLink(name: String, file: BaseFile)(using tx: Transaction): Future[Unit] =
    getEntry(name).flatMap:
      case None => prepareInsert(name, file.pointer)
      case Some(_) => throw DirectoryEntryExists(this.pointer, name)

  override def freeResources(): Future[Unit] =
    given ExecutionContext = fs.executionContext

    fs.client.retryStrategy.retryUntilSuccessful:
      inode.contents.orootObject match
        case None => Future.unit
        case Some(root) =>
          val tx = fs.client.newTransaction()
          DeleteDirectoryContentTask.prepareTask(fs, pointer)(using tx)
          tx.commit().map(_ => ())
}

object SimpleDirectory:
  object DeleteDirectoryContentTask extends DurableTaskFactory:
    private val FileSystemUUIDKey: Key = Key(1)
    private val InodePointerKey: Key = Key(2)

    val typeUUID: UUID = UUID.fromString("c1fb782f-7f13-4921-8ddf-155123445730")

    override def createTask(client: AspenClient,
                            pointer: DurableTaskPointer,
                            revision: ObjectRevision,
                            state: Map[Key, KeyValueObjectState.ValueState]): DurableTask =
      val fsUUID = byte2uuid(state(FileSystemUUIDKey).value.bytes)
      val dptr = InodePointer(state(InodePointerKey).value.bytes).asInstanceOf[DirectoryPointer]

      val fs = FileSystem.getRegisteredFileSystem(fsUUID).get

      new DeleteDirectoryContentTask(client, pointer, revision, state, fs, dptr)

    /** Returns a Future that completes then the task creation is prepared for commit of the transaction.
     * The inner future completes when the task finishes
     */
    def prepareTask(fs: FileSystem,
                    directoryPointer: DirectoryPointer)
                   (using tx: Transaction, ec: ExecutionContext): Future[Future[Unit]] =
      fs.taskExecutor.prepareTask(
        DeleteDirectoryContentTask,
        List(
          FileSystemUUIDKey -> uuid2byte(fs.uuid),
          InodePointerKey -> directoryPointer.toArray
        )
      ).map: fcomplete =>
        fcomplete.map(_ => ())

  class DeleteDirectoryContentTask private(client: AspenClient,
                                           val taskPointer: DurableTaskPointer,
                                           revision: ObjectRevision,
                                           initialState: Map[Key, KeyValueObjectState.ValueState],
                                           fs: FileSystem,
                                           directoryPointer: DirectoryPointer
                                          ) extends DurableTask:

    given ExecutionContext = client.clientContext

    private val promise = Promise[Option[AnyRef]]()

    def completed: Future[Option[AnyRef]] = promise.future

    resume()

    def resume(): Unit =

      // Although directory deletion is protected by isEmpty checks, race
      // conditions could allow the directory to be deleted between the
      // check and the removal from the parent directory. Simply delete any
      // content found
      def deleteEntry(key: Key, vs: ValueState): Future[Unit] =
        client.transactUntilSuccessful: tx =>
          UnlinkFileTask.prepareTask(fs, InodePointer(vs.value.bytes))(using tx).map(_ => ())

      val rootManager = new SimpleDirectoryRootManager(client, directoryPointer.pointer)
      val tree = new TieredKeyValueList(fs.client, rootManager)

      client.retryStrategy.retryUntilSuccessful:
        tree.deleteTree(Some(deleteEntry)).map: _ =>
          promise.success(None)
