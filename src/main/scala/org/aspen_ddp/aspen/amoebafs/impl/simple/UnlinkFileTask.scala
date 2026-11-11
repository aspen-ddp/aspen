package org.aspen_ddp.aspen.amoebafs.impl.simple

import java.util.UUID
import org.aspen_ddp.aspen.client.{AspenClient, KeyValueObjectState, Transaction}
import org.aspen_ddp.aspen.common.objects.{Key, ObjectRefcount, ObjectRevision}
import org.aspen_ddp.aspen.compute.{DurableTask, DurableTaskFactory, DurableTaskPointer, SteppedDurableTask, TaskExecutor}
import org.aspen_ddp.aspen.common.util.{byte2int, byte2uuid, int2byte, uuid2byte}
import org.aspen_ddp.aspen.amoebafs.{DirectoryInode, FileInode, FileSystem, Inode, InodePointer}

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Success
import scala.language.implicitConversions

object UnlinkFileTask extends DurableTaskFactory:
  val typeUUID: UUID = UUID.fromString("B02539DC-3AE1-4E50-B52B-A5EFA6B5B330")

  private val FileSystemUUIDKey = Key(1)
  private val InodePointerKey   = Key(2)

  def createTask(client: AspenClient,
                 pointer: DurableTaskPointer,
                 revision: ObjectRevision,
                 state: Map[Key, KeyValueObjectState.ValueState],
                 taskExecutor: TaskExecutor): DurableTask =
    val fsUUID = byte2uuid(state(FileSystemUUIDKey).value.bytes)
    val ptr = InodePointer(state(InodePointerKey).value.bytes)
    val fs = FileSystem.getRegisteredFileSystem(fsUUID).get

    new UnlinkFileTask(pointer, fs, ptr)

  def prepareTask(fileSystem: FileSystem,
                  inodePointer: InodePointer)(using tx: Transaction): Future[Future[Option[AnyRef]]] =
    val (stepKey, stepValue) = SteppedDurableTask.getInitialContent(Map.empty)
    val istate = List(
      stepKey -> stepValue.bytes,
      FileSystemUUIDKey -> uuid2byte(fileSystem.uuid),
      InodePointerKey -> inodePointer.toArray
    )
    fileSystem.taskExecutor.prepareTask(this, istate)

  /** Stages in `tx` the enrollment of the task that deletes the content `inode` owns. The outer future completes once
    * staged, the inner one when the deletion finishes in this process.
    */
  private def prepareContentDeletion(fs: FileSystem, inode: Inode)(using tx: Transaction): Future[Future[Unit]] =
    given ExecutionContext = fs.executionContext
    inode match
      case f: FileInode if f.contents.orootObject.isDefined =>
        SimpleFileContent.DeleteFileContentTask.prepareTask(fs, f.contents)
      case d: DirectoryInode if d.contents.orootObject.isDefined =>
        SimpleDirectory.DeleteDirectoryContentTask.prepareTask(fs, d.contents)
      case _ => Future.successful(Future.unit)


/** Removes one link to an inode. The task owns the link count decrement and frees the inode's
  * resources once the count reaches zero, so callers must not decrement the link count themselves.
  */
class UnlinkFileTask(
  taskPointer: DurableTaskPointer,
  val fs: FileSystem,
  val iptr: InodePointer
) extends SteppedDurableTask(taskPointer, fs.client):

  def decrementLinkCount(tx: Transaction, state: Map[String, Array[Byte]], stepRevision: ObjectRevision): Future[Map[String, Array[Byte]]] =
    given Transaction = tx
    for
      (inode, _, revision) <- fs.readInode(iptr)
    yield
      val newLinks = inode.links - 1
      tx.overwrite(iptr.pointer, revision, inode.update(links = Some(newLinks)).toArray)
      state.updated("updatedLink", int2byte(newLinks))

  /** Once the last link is gone: removes the inode from the inode table, deletes the inode object and enrolls the
    * deletion of the content the inode owns, all in the step's transaction, so the content task is enrolled exactly
    * once and only with the inode's removal. The inode is read fresh and its revision is required, so the content
    * root enrolled is the one the deleted inode held: an open handle growing the file aborts the step.
    */
  def checkForDeletion(tx: Transaction, state: Map[String, Array[Byte]], stepRevision: ObjectRevision): Future[Map[String, Array[Byte]]] =
    given Transaction = tx
    val updatedLink = byte2int(state("updatedLink"))
    updatedLink match
      case 0 =>
        for
          (inode, _, revision) <- fs.readInode(iptr)
          ffreed <- UnlinkFileTask.prepareContentDeletion(fs, inode)
          _ <- fs.inodeTable.delete(iptr)
        yield
          tx.bumpVersion(iptr.pointer, revision)
          tx.setRefcount(iptr.pointer, ObjectRefcount(0, 1), ObjectRefcount(1, 0))
          // Each attempt of this step replaces the last, so the attempt that commits sets it last
          contentFreed.set(ffreed)
          state
      case _ => Future.successful(state)

  /** Completes once the content deletion this task enrolled has finished in this process as well. A task resumed
    * after the step committed elsewhere has nothing to wait for. Lazy, so it is ready for steps run before this
    * class's constructor completes
    */
  private lazy val contentFreed = new AtomicReference[Future[Unit]](Future.unit)

  override def completed: Future[Option[AnyRef]] =
    super.completed.flatMap: result =>
      contentFreed.get.transform(_ => Success(result))

  val steps: Array[(Transaction, Map[String, Array[Byte]], ObjectRevision) => Future[Map[String, Array[Byte]]]] =
    Array(decrementLinkCount, checkForDeletion)
