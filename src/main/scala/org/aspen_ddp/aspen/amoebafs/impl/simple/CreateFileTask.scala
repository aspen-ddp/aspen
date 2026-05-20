package org.aspen_ddp.aspen.amoebafs.impl.simple

import java.nio.charset.StandardCharsets
import java.util.UUID
import org.aspen_ddp.aspen.client.tkvl.TieredKeyValueList
import org.aspen_ddp.aspen.client.{AspenClient, KeyValueObjectState, Transaction}
import org.aspen_ddp.aspen.common.objects.{Key, ObjectRevision, Value}
import org.aspen_ddp.aspen.compute.{DurableTask, DurableTaskPointer, DurableTaskFactory, SteppedDurableTask, TaskExecutor}
import org.aspen_ddp.aspen.common.util.{byte2uuid, uuid2byte}
import org.aspen_ddp.aspen.amoebafs.{DirectoryPointer, FileSystem, Inode, InodePointer}

import scala.concurrent.Future
import scala.language.implicitConversions

object CreateFileTask extends DurableTaskFactory:
  val typeUUID: UUID = UUID.fromString("48A4F255-7B78-4D7F-B8AB-D9301B8CDA40")

  private val FileSystemUUIDKey  = Key(1)
  private val DirectoryInodeKey  = Key(2)
  private val InodeKey           = Key(3)
  private val FileNameKey        = Key(4)

  def createTask(client: AspenClient,
                 pointer: DurableTaskPointer,
                 revision: ObjectRevision,
                 state: Map[Key, KeyValueObjectState.ValueState]): DurableTask =
    val fsUUID = byte2uuid(state(FileSystemUUIDKey).value.bytes)
    val ptr = InodePointer(state(DirectoryInodeKey).value.bytes).asInstanceOf[DirectoryPointer]
    val inode = Inode(client, state(InodeKey).value.bytes)
    val fileName = new String(state(FileNameKey).value.bytes, StandardCharsets.UTF_8)
    val fs = FileSystem.getRegisteredFileSystem(fsUUID).get

    new CreateFileTask(pointer, fs, ptr, fileName, inode)

  def prepareTask(fileSystem: FileSystem,
                  directoryPointer: DirectoryPointer,
                  fileName: String,
                  inode: Inode)(using tx: Transaction): Future[Future[Option[AnyRef]]] =
    val (stepKey, stepValue) = SteppedDurableTask.getInitialContent(Map.empty)
    val istate = List(
      stepKey -> stepValue.bytes,
      FileSystemUUIDKey -> uuid2byte(fileSystem.uuid),
      DirectoryInodeKey -> directoryPointer.toArray,
      InodeKey -> inode.toArray,
      FileNameKey -> fileName.getBytes(StandardCharsets.UTF_8))
    fileSystem.taskExecutor.prepareTask(this, istate)


class CreateFileTask(
  taskPointer: DurableTaskPointer,
  val fs: FileSystem,
  val directoryPointer: DirectoryPointer,
  val fileName: String,
  val inode: Inode
) extends SteppedDurableTask(taskPointer, fs.client):

  override def resultFromState(state: Map[String, Array[Byte]]): Option[AnyRef] =
    Some(InodePointer(state("newFilePointer")))

  def allocateInode(tx: Transaction, state: Map[String, Array[Byte]], stepRevision: ObjectRevision): Future[Map[String, Array[Byte]]] =
    given Transaction = tx
    fs.inodeTable.prepareInodeAllocation(inode).map: iptr =>
      state.updated("newFilePointer", iptr.toArray)

  def addToDirectory(tx: Transaction, state: Map[String, Array[Byte]], stepRevision: ObjectRevision): Future[Map[String, Array[Byte]]] =
    given Transaction = tx
    val newFile = InodePointer(state("newFilePointer"))
    val rootMgr = new SimpleDirectoryRootManager(fs.client, directoryPointer.pointer)
    val tkvl = new TieredKeyValueList(fs.client, rootMgr)
    val fkey = Key(fileName)

    for
      onode <- tkvl.getContainingNode(fkey)
      _ <- onode match
        case None =>
          tkvl.set(fkey, Value(newFile.toArray), requirement = Some(Left(true)))
        case Some(node) => node.get(fkey) match
          case None =>
            node.set(fkey, Value(newFile.toArray), requirement = Some(Left(true)))
          case Some(vs) =>
            node.set(fkey, Value(newFile.toArray), requirement = Some(Right(vs.revision))).flatMap: _ =>
              UnlinkFileTask.prepareTask(fs, InodePointer(vs.value.bytes))
    yield
      state

  val steps: Array[(Transaction, Map[String, Array[Byte]], ObjectRevision) => Future[Map[String, Array[Byte]]]] =
    Array(allocateInode, addToDirectory)
