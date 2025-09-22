package org.aspen_ddp.aspen.amoebafs.impl.simple

import java.nio.charset.StandardCharsets
import java.util.UUID
import org.aspen_ddp.aspen.client.tkvl.TieredKeyValueList
import org.aspen_ddp.aspen.client.{AspenClient, KeyValueObjectState, Transaction}
import org.aspen_ddp.aspen.common.objects.{Insert, Key, KeyRevisionGuard, ObjectRevision, Value}
import org.aspen_ddp.aspen.common.transaction.KeyValueUpdate.KeyRevision
import org.aspen_ddp.aspen.compute.{DurableTask, DurableTaskPointer, DurableTaskFactory, TaskExecutor}
import org.aspen_ddp.aspen.common.util.{byte2uuid, uuid2byte}
import org.aspen_ddp.aspen.amoebafs.{DirectoryPointer, FileSystem, Inode, InodePointer}

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success}
import scala.language.implicitConversions

object CreateFileTask extends DurableTaskFactory {
  val typeUUID: UUID = UUID.fromString("48A4F255-7B78-4D7F-B8AB-D9301B8CDA40")

  private val FileSystemUUIDKey  = Key(1)
  private val DirectoryInodeKey  = Key(2)
  private val InodeKey           = Key(3)
  private val FileNameKey        = Key(4)
  private val NewFilePointerKey  = Key(5)
  private val StepKey            = Key(6)

  def createTask(client: AspenClient,
                 pointer: DurableTaskPointer,
                 revision: ObjectRevision,
                 state: Map[Key, KeyValueObjectState.ValueState]): DurableTask = {

    val fsUUID = byte2uuid(state(FileSystemUUIDKey).value.bytes)
    val ptr = InodePointer(state(DirectoryInodeKey).value.bytes).asInstanceOf[DirectoryPointer]
    val inode = Inode(client, state(InodeKey).value.bytes)
    val fileName = new String(state(FileNameKey).value.bytes, StandardCharsets.UTF_8)
    val fs = FileSystem.getRegisteredFileSystem(fsUUID).get

    new CreateFileTask(pointer, fs, ptr, fileName, inode)
  }

  def prepareTask(fileSystem: FileSystem,
                  directoryPointer: DirectoryPointer,
                  fileName: String,
                  inode: Inode)(using tx: Transaction): Future[Future[Option[AnyRef]]] = {
    val istate = List(
      StepKey -> Array[Byte](0),
      FileSystemUUIDKey -> uuid2byte(fileSystem.uuid),
      DirectoryInodeKey -> directoryPointer.toArray,
      InodeKey -> inode.toArray,
      FileNameKey -> fileName.getBytes(StandardCharsets.UTF_8))
    fileSystem.taskExecutor.prepareTask(this, istate)
  }
}

class CreateFileTask(val taskPointer: DurableTaskPointer,
                     val fs: FileSystem,
                     val directoryPointer: DirectoryPointer,
                     val fileName: String,
                     val inode: Inode) extends DurableTask {

  import CreateFileTask._

  given ExecutionContext = fs.executionContext

  private val promise = Promise[Option[AnyRef]]()

  def completed: Future[Option[AnyRef]] = promise.future

  doNextStep()

  def doNextStep(): Unit = {
    for {
      kvos <- fs.client.read(taskPointer.kvPointer)
      step = kvos.contents(StepKey)
    } yield {
      step.value.bytes(0) match {
        case 0 => allocateInode(step.revision) onComplete {
          case Failure(_) => doNextStep()
          case Success(_) => doNextStep()
        }

        case 1 =>
          val newFilePointer = InodePointer(kvos.contents(NewFilePointerKey).value.bytes)
          addToDirectory(step.revision, newFilePointer) onComplete {
          case Failure(_) => doNextStep()
          case Success(_) => doNextStep()
        }

        case _ => synchronized {
          val newFilePointer = InodePointer(kvos.contents(NewFilePointerKey).value.bytes)
          if (! promise.isCompleted) {
            promise.success(Some(newFilePointer))
          }
        }
      }
    }
  }

  def allocateInode(stepRevision: ObjectRevision): Future[Unit] = {

    given tx: Transaction = fs.client.newTransaction()

    val guard = KeyRevisionGuard(taskPointer.kvPointer, StepKey, stepRevision)

    fs.inodeTable.prepareInodeAllocation(inode, guard).onComplete {
      case Failure(err) => tx.invalidateTransaction(err)

      case Success(iptr) =>
      val nextStep = Insert(StepKey, Array[Byte](1)) ::
        Insert(NewFilePointerKey, iptr.toArray) :: Nil

      tx.update(taskPointer.kvPointer, None, None, KeyRevision(StepKey, stepRevision) :: Nil, nextStep)

      tx.commit()
    }

    tx.result.map(_=>())
  }

  def addToDirectory(stepRevision: ObjectRevision, newFile: InodePointer): Future[Unit] = {
    given tx: Transaction = fs.client.newTransaction()

    val nextStep = Insert(StepKey, Array[Byte](2)) :: Nil

    val rootMgr = new SimpleDirectoryRootManager(fs.client, directoryPointer.pointer)
    val tkvl = new TieredKeyValueList(fs.client, rootMgr)
    val fkey = Key(fileName)

    for {
      onode <- tkvl.getContainingNode(fkey)
      _ <- onode match {
        case None =>
          tkvl.set(fkey, Value(newFile.toArray), requirement = Some(Left(true)))
        case Some(node) => node.get(fkey) match {
          case None =>
            node.set(fkey, Value(newFile.toArray), requirement = Some(Left(true)))
          case Some(vs) =>
            node.set(fkey, Value(newFile.toArray), requirement = Some(Right(vs.revision))).flatMap { _ =>
              UnlinkFileTask.prepareTask(fs, InodePointer(vs.value.bytes))
            }
        }
      }
    } yield {
      tx.update(taskPointer.kvPointer, None, None, KeyRevision(StepKey, stepRevision) :: Nil, nextStep)

      tx.commit().map(_=>())
    }
  }
}