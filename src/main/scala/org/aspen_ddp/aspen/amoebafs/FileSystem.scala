package org.aspen_ddp.aspen.amoebafs

import java.util.UUID

import org.aspen_ddp.aspen.client.{AspenClient, CorruptedObject, InvalidObject, ObjectAllocator, ObjectAllocatorId, RetryStrategy}
import org.aspen_ddp.aspen.common.objects.ObjectRevision
import org.aspen_ddp.aspen.compute.TaskExecutor
import org.aspen_ddp.aspen.amoebafs.error.InvalidInode
import org.apache.logging.log4j.scala.Logging

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success}

trait FileSystem extends Logging {
  val uuid: UUID

  private var openFiles: Map[Long, File] = Map()

  def readInode(inodeNumber: Long): Future[(Inode, InodePointer, ObjectRevision)] = {
    given ExecutionContext = executionContext
    inodeTable.lookup(inodeNumber).flatMap {
      case None => Future.failed(InvalidInode(inodeNumber))
      case Some(iptr) => readInode(iptr)
    }
  }
  def readInode(iptr: InodePointer): Future[(Inode, InodePointer, ObjectRevision)] = {
    given ExecutionContext = executionContext
    //client.read(iptr.pointer).map { dos =>
    //  (Inode(client, dos.data), dos.revision)
    //}
    val pload = Promise[(Inode, InodePointer, ObjectRevision)]()

    client.read(iptr.pointer) onComplete {
      case Success(dos) =>
        pload.success((Inode(client, dos.data), iptr, dos.revision))

      case Failure(_: InvalidObject) =>
        // Probably deleted
        pload.failure(InvalidInode(iptr.number))

      case Failure(e: CorruptedObject) =>
        // TODO: If pointer fails to read, we'll need to read the inode table (could be stale pointer). If new pointer
        //       for that inode exists, we'll need to use a callback function to update the stale pointer
        pload.failure(e)
        logger.error(s"Corrupted Inode: $iptr")

      case Failure(cause) =>
        pload.failure(cause)
        logger.error(s"Unexpected error encountered during load of inode $iptr, $cause")
    }

    pload.future
  }

  def loadRoot()(using ec: ExecutionContext): Future[Directory] = {
    inodeTable.lookupRoot() flatMap { pointer => loadDirectory(pointer) }
  }

  def lookup(inodeNumber: Long): Future[Option[BaseFile]] = {
    given ExecutionContext = executionContext
    val p = Promise[Option[BaseFile]]()
    inodeTable.lookup(inodeNumber) onComplete {
      case Success(oiptr) =>
        oiptr match {
          case Some(iptr) =>
            lookup(iptr) onComplete {
              case Success(bfile) => p.success(Some(bfile))
              case Failure(_) => p.success(None)
            }
          case None => p.success(None)
        }
      case Failure(_) => p.success(None)
    }
    p.future
  }

  def lookup(iptr: InodePointer)(using ec: ExecutionContext): Future[BaseFile] =  {
    getCachedFile(iptr.number) match {
      case Some(f) => Future.successful(f)
      case None =>
        iptr match {
          case ptr: DirectoryPointer => loadDirectory(ptr)
          case ptr: FilePointer =>
            synchronized { openFiles.get(iptr.number) } match {
              case Some(file) => Future.successful(file)
              case None => loadFile(ptr)
            }
          case ptr: SymlinkPointer => loadSymlink(ptr)
          case ptr: UnixSocketPointer => loadUnixSocket(ptr)
          case ptr: FIFOPointer => loadFIFO(ptr)
          case ptr: CharacterDevicePointer => loadCharacterDevice(ptr)
          case ptr: BlockDevicePointer => loadBlockDevice(ptr)
        }
    }
  }

  private def doLoad[
    PointerType <: InodePointer,
    InodeType <: Inode,
    FileType <: BaseFile](pointer: PointerType,
                          createFn: (FileSystem, PointerType, InodeType, ObjectRevision) => Future[FileType])(using ec: ExecutionContext): Future[FileType] = {
    getCachedFile(pointer.number) match {
      case Some(f) => Future.successful(f.asInstanceOf[FileType])
      case None => synchronized {
        loading.get(pointer.number) match {
          case Some(f) => f.map(base => base.asInstanceOf[FileType])
          case None =>
            val f = readInode(pointer).flatMap { t =>
              val (inode, _, revision) = t
              createFn(this, pointer, inode.asInstanceOf[InodeType], revision)
            }
            loading += (pointer.number -> f)
            f.foreach { _ =>
              synchronized {
                loading -= pointer.number
              }
            }
            f
        }
      }
    }
  }

  def loadDirectory(pointer: DirectoryPointer)(using ec: ExecutionContext): Future[Directory] = {
    doLoad[DirectoryPointer, DirectoryInode, Directory](pointer, fileFactory.createDirectory)
  }

  def loadFile(pointer: FilePointer)(using ec: ExecutionContext): Future[File] = {
    doLoad[FilePointer, FileInode, File](pointer, fileFactory.createFile)
  }

  def loadSymlink(pointer: SymlinkPointer)(using ec: ExecutionContext): Future[Symlink] = {
    doLoad[SymlinkPointer, SymlinkInode, Symlink](pointer, fileFactory.createSymlink)
  }

  def loadUnixSocket(pointer: UnixSocketPointer)(using ec: ExecutionContext): Future[UnixSocket] = {
    doLoad[UnixSocketPointer, UnixSocketInode, UnixSocket](pointer, fileFactory.createUnixSocket)
  }

  def loadFIFO(pointer: FIFOPointer)(using ec: ExecutionContext): Future[FIFO] = {
    doLoad[FIFOPointer, FIFOInode, FIFO](pointer, fileFactory.createFIFO)
  }

  def loadCharacterDevice(pointer: CharacterDevicePointer)(using ec: ExecutionContext): Future[CharacterDevice] = {
    doLoad[CharacterDevicePointer, CharacterDeviceInode, CharacterDevice](pointer, fileFactory.createCharacterDevice)
  }

  def loadBlockDevice(pointer: BlockDevicePointer)(using ec: ExecutionContext): Future[BlockDevice] = {
    doLoad[BlockDevicePointer, BlockDeviceInode, BlockDevice](pointer, fileFactory.createBlockDevice)
  }

  protected def getCachedFile(inodeNumber: Long): Option[BaseFile] = None
  protected def cacheFile(file: BaseFile): Unit = ()
  private var loading: Map[Long, Future[BaseFile]] = Map()

  protected val fileFactory: FileFactory

  def openFileHandle(file: File): FileHandle
  def closeFileHandle(fh: FileHandle): Unit

  def defaultSegmentSize: Int
  def defaultFileIndexNodeSize(iter: Int): Int

  def shutdown(): Unit

  private[amoebafs] def retryStrategy: RetryStrategy
  private[amoebafs] def taskExecutor: TaskExecutor
  private[amoebafs] def defaultInodeAllocator: ObjectAllocator
  private[amoebafs] def defaultSegmentAllocator(): Future[ObjectAllocator]
  private[amoebafs] def defaultFileContentTierAllocator(tier: Int): Future[ObjectAllocator]
  private[amoebafs] def client: AspenClient
  private[amoebafs] def executionContext: ExecutionContext
  private[amoebafs] def getObjectAllocator(id: ObjectAllocatorId): Future[ObjectAllocator]
  private[amoebafs] def inodeTable: InodeTable
}

object FileSystem {
  private var loadedFileSystems = Map[UUID, FileSystem]()

  def register(fs: FileSystem): Unit = synchronized {
    loadedFileSystems += (fs.uuid -> fs)
  }

  def getRegisteredFileSystem(uuid: UUID): Option[FileSystem] = synchronized {
    loadedFileSystems.get(uuid)
  }
}
