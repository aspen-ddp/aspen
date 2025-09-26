package org.aspen_ddp.aspen.amoebafs.impl.simple

import java.util.UUID
import org.aspen_ddp.aspen.client.{AspenClient, KeyValueObjectState, Transaction}
import org.aspen_ddp.aspen.common.objects.{Insert, Key, ObjectRefcount, ObjectRevision}
import org.aspen_ddp.aspen.common.transaction.KeyValueUpdate.KeyRevision
import org.aspen_ddp.aspen.compute.{DurableTask, DurableTaskFactory, DurableTaskPointer, TaskExecutor}
import org.aspen_ddp.aspen.common.util.{byte2int, byte2uuid, int2byte, uuid2byte}
import org.aspen_ddp.aspen.amoebafs.{File, FilePointer, FileSystem, Inode, InodePointer}

import java.nio.{ByteBuffer, ByteOrder}
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success}
import scala.language.implicitConversions

object UnlinkFileTask extends DurableTaskFactory:
  val typeUUID: UUID = UUID.fromString("B02539DC-3AE1-4E50-B52B-A5EFA6B5B330")

  private val FileSystemUUIDKey = Key(1)
  private val InodePointerKey   = Key(2)
  private val StepKey           = Key(3)
  private val UpdatedLinkKey    = Key(4)

  def createTask(client: AspenClient,
                 pointer: DurableTaskPointer,
                 revision: ObjectRevision,
                 state: Map[Key, KeyValueObjectState.ValueState]): DurableTask =
    val fsUUID = byte2uuid(state(FileSystemUUIDKey).value.bytes)
    val ptr = InodePointer(state(InodePointerKey).value.bytes)

    val fs = FileSystem.getRegisteredFileSystem(fsUUID).get

    new UnlinkFileTask(pointer, fs, ptr)

  def prepareTask(fileSystem: FileSystem,
                  inodePointer: InodePointer)(using tx: Transaction): Future[Future[Option[AnyRef]]] =
    val istate = List(
      FileSystemUUIDKey -> uuid2byte(fileSystem.uuid),
      InodePointerKey -> inodePointer.toArray,
      StepKey -> Array[Byte](0)
    )
    fileSystem.taskExecutor.prepareTask(this, istate)


class UnlinkFileTask(val taskPointer: DurableTaskPointer,
                     val fs: FileSystem,
                     val iptr: InodePointer) extends DurableTask:

  import UnlinkFileTask._

  given ExecutionContext = fs.executionContext

  private val promise = Promise[Option[AnyRef]]()

  def completed: Future[Option[AnyRef]] = promise.future

  doNextStep()

  def doNextStep(): Unit =
    for
      kvos <- fs.client.read(taskPointer.kvPointer)
      (inode, _, revision) <- fs.readInode(iptr)
      file <- fs.loadFile(FilePointer(iptr.number, iptr.pointer))
      step = kvos.contents(StepKey)
      onewLinkk = kvos.contents.get(UpdatedLinkKey).map: vs =>
        byte2int(vs.value.bytes)
    yield
      step.value.bytes(0) match
        case 0 => decrementLinkCount(step.revision, inode, revision) onComplete:
          case Failure(_) => doNextStep()
          case Success(_) => doNextStep()

        case 1 => checkForDeletion(step.revision, inode, onewLinkk.get, revision, file) onComplete:
          case Failure(_) => doNextStep()
          case Success(_) => doNextStep()

        case _ =>
          synchronized:
            if ! promise.isCompleted then
              promise.success(None)

  def decrementLinkCount(stepRevision: ObjectRevision,
                         inode: Inode,
                         revision: ObjectRevision): Future[Unit] =
    val newLinks = inode.links - 1
    val nextStep = List(
      Insert(StepKey, Array[Byte](1)),
      Insert(UpdatedLinkKey, int2byte(newLinks))
    )
    given tx: Transaction = fs.client.newTransaction()

    tx.overwrite(iptr.pointer, revision, inode.update(links=Some(newLinks)).toArray)
    tx.update(taskPointer.kvPointer, None, None, KeyRevision(StepKey, stepRevision) :: Nil, nextStep)
    tx.commit().map(_=>())

  def checkForDeletion(stepRevision: ObjectRevision,
                       inode: Inode,
                       updatedLink: Int,
                       revision: ObjectRevision,
                       file: File): Future[Unit] =
    val nextStep = Insert(StepKey, Array[Byte](2)) :: Nil

    given tx: Transaction = fs.client.newTransaction()

    val fdelete = updatedLink match
      case 0 =>
        file.freeResources().map: _ =>
          fs.inodeTable.delete(iptr)
          tx.setRefcount(iptr.pointer, ObjectRefcount(0, 1), ObjectRefcount(1, 0))

      case _ => Future.unit

    fdelete.flatMap: _ =>
      tx.update(taskPointer.kvPointer, None, None, KeyRevision(StepKey, stepRevision) :: Nil, nextStep)
      tx.commit().map(_ => ())


