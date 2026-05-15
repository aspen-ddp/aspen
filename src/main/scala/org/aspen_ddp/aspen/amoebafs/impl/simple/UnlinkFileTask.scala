package org.aspen_ddp.aspen.amoebafs.impl.simple

import java.util.UUID
import org.aspen_ddp.aspen.client.{AspenClient, KeyValueObjectState, Transaction}
import org.aspen_ddp.aspen.common.objects.{Key, ObjectRefcount, ObjectRevision}
import org.aspen_ddp.aspen.compute.{DurableTask, DurableTaskFactory, DurableTaskPointer, SteppedDurableTask}
import org.aspen_ddp.aspen.common.util.{byte2int, byte2uuid, int2byte, uuid2byte}
import org.aspen_ddp.aspen.amoebafs.{FileSystem, InodePointer}

import scala.concurrent.Future
import scala.language.implicitConversions

object UnlinkFileTask extends DurableTaskFactory:
  val typeUUID: UUID = UUID.fromString("B02539DC-3AE1-4E50-B52B-A5EFA6B5B330")

  private val FileSystemUUIDKey = Key(1)
  private val InodePointerKey   = Key(2)

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
    val (stepKey, stepValue) = SteppedDurableTask.getInitialContent(Map.empty)
    val istate = List(
      stepKey -> stepValue.bytes,
      FileSystemUUIDKey -> uuid2byte(fileSystem.uuid),
      InodePointerKey -> inodePointer.toArray
    )
    fileSystem.taskExecutor.prepareTask(this, istate)


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

  def checkForDeletion(tx: Transaction, state: Map[String, Array[Byte]], stepRevision: ObjectRevision): Future[Map[String, Array[Byte]]] =
    given Transaction = tx
    val updatedLink = byte2int(state("updatedLink"))
    updatedLink match
      case 0 =>
        for
          entry <- fs.lookup(iptr)
          _ <- entry.freeResources()
          _ <- fs.inodeTable.delete(iptr)
        yield
          tx.setRefcount(iptr.pointer, ObjectRefcount(0, 1), ObjectRefcount(1, 0))
          state
      case _ => Future.successful(state)

  val steps: Array[(Transaction, Map[String, Array[Byte]], ObjectRevision) => Future[Map[String, Array[Byte]]]] =
    Array(decrementLinkCount, checkForDeletion)
