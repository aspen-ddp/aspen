package org.aspen_ddp.aspen.amoebafs.nfs

import scala.language.implicitConversions
import java.io.IOException
import java.nio.ByteBuffer
import com.github.blemale.scaffeine.{LoadingCache, Scaffeine}
import org.aspen_ddp.aspen.client.{FatalReadError, StopRetrying}
import org.aspen_ddp.aspen.amoebafs.error.{DirectoryEntryDoesNotExist, DirectoryEntryExists, InvalidInode}
import org.aspen_ddp.aspen.amoebafs.{BaseFile, BlockDevice, CharacterDevice, Directory, DirectoryInode, File, FileHandle, FileMode, FileSystem, FileType, InodePointer, Symlink, Timespec}

import javax.security.auth.Subject
import org.apache.logging.log4j.scala.Logging
import org.aspen_ddp.aspen.amoebafs.nfs.AmoebaNFS.given_Conversion_Long_Inode
import org.dcache.nfs.util.UnixSubjects
import org.dcache.nfs.status.{ExistException, InvalException, NoEntException, NotDirException, NotSuppException, ServerFaultException}
import org.dcache.nfs.v4.{NfsIdMapping, SimpleIdMap}
import org.dcache.nfs.v4.xdr.nfsace4
import org.dcache.nfs.vfs.{AclCheckable, DirectoryEntry, DirectoryStream, FsStat, Inode, Stat, VirtualFileSystem}

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration.Duration


// mount -v -t nfs4 -o "vers=4.1" 192.168.56.1:/ /mnt

object AmoebaNFS {
  import scala.language.implicitConversions
  import scala.Conversion

  def inode2long(inode: Inode): Long = ByteBuffer.wrap(inode.getFileId).getLong
  
  def long2inode(fd: Long): Inode = {
    val arr = new Array[Byte](8)
    ByteBuffer.wrap(arr).putLong(fd)
    Inode.forFile(arr)
  }

  given Conversion[Inode, Long] = inode => inode2long(inode)
  given Conversion[Long, Inode] = fd => long2inode(fd)

  def nfsFileType(pointer: InodePointer): Int = FileType.toMode(pointer.ftype)

  def nfsStat(file: BaseFile): Stat = {
    val stats: Stat = new Stat

    stats.setMode(file.mode)
    stats.setNlink(file.links)
    stats.setGeneration(0L)
    stats.setIno(file.pointer.number)
    stats.setATime(file.atime.millis)
    stats.setMTime(file.mtime.millis)
    stats.setCTime(file.ctime.millis)
    stats.setUid(file.uid)
    stats.setGid(file.gid)

    file match {
      case f: File => stats.setSize(f.size)
      case l: Symlink => stats.setSize(l.symLink.length)
      case _ => stats.setSize(0)
    }

    file match {
      case d: CharacterDevice => stats.setRdev(d.rdev)
      case d: BlockDevice => stats.setRdev(d.rdev)
      case _ =>
    }

    stats
  }

  def blockingCall[T](fn: => Future[T])(using ExecutionContext): T = try {
    Await.result(fn, Duration.Inf)
  } catch {
    case e: DirectoryEntryExists => throw new ExistException(e.toString)
    case e: DirectoryEntryDoesNotExist => throw new NoEntException(e.toString)
    case _: FatalReadError => throw new IOException("Fatal Aspen Read Error")
    case t: Throwable => throw t
  }


}

class AmoebaNFS(val fs: FileSystem,
                ec: ExecutionContext,
                inodeCacheMax: Int = 1000,
                fileHandleCacheMax: Int = 250,
                writeBufferSize: Int = 4*1024*1024) extends VirtualFileSystem with Logging {

  import AmoebaNFS._
  import org.aspen_ddp.aspen.amoebafs.nfs.AmoebaNFS.given_Conversion_Inode_Long

  given ExecutionContext = ec

  private val NoAcl = new Array[nfsace4](0)
  private val IdMapper = new SimpleIdMap

  private[this] val fileHandles: LoadingCache[Long, FileHandle] = Scaffeine().
    maximumSize(fileHandleCacheMax).
    removalListener[Long, FileHandle]( (_,v,_) => v.close()).
    build[Long, FileHandle]((inode: Long) => {
      load(inode) match {
        case f: File => f.open()
        case _ => throw new NotSuppException("Cannot open non-regular file")
      }
    })

  private def getFileHandle(inode: Inode): FileHandle = fileHandles.get(inode)

  def load(inode: Long): BaseFile = blockingCall {
    fs.lookup(inode) map {
      case Some(f) => f
      case None => throw new NoEntException(s"no such inode $inode")
    } recover {
      case _: FatalReadError => throw new ServerFaultException("sumthin broke")
    }
  }

  private def getDirectory(inode: Long): Directory = load(inode) match {
    case d: Directory => d
    case _ => throw new NotDirException(s"Inode $inode is not a directory")
  }

  private def getSymLink(inode: Long): Symlink = load(inode) match {
    case f: Symlink => f
    case _ => throw new NotSuppException(s"Operation supported only on regular file")
  }


  /**
    * Check access to file system object.
    *
    * @param inode inode of the object to check.
    * @param mode  a mask of permission bits to check.
    * @return an allowed subset of permissions from the given mask.
    * @throws IOException meh
    */
  @throws[IOException]
  def access(inode: Inode, mode: Int): Int = mode // always allow

  /**
    * Create a new object in a given directory with a specific name.
    *
    * @param parent  directory where new object must be created.
    * @param type    the type of the object to be created.
    * @param name    name of the object.
    * @param subject the owner subject of a newly created object.
    * @param mode    initial permission mask.
    * @return the inode of the newly created object.
    * @throws IOException meh
    */
  @throws[IOException]
  def create(parent: Inode, `type`: Stat.Type, name: String, subject: Subject, mode: Int): Inode = {
    logger.info(s"Create ${`type`} $name in directory ${inode2long(parent)}")

    val dir = getDirectory(parent)

    val uid = UnixSubjects.getUid(subject).toInt
    val gid = UnixSubjects.getPrimaryGid(subject).toInt

    val iptr = `type` match {
      case Stat.Type.REGULAR => blockingCall { dir.createFile(name, mode, uid, gid) }
      case Stat.Type.DIRECTORY => blockingCall { dir.createDirectory(name, mode, uid, gid) }
      case Stat.Type.SYMLINK => blockingCall { dir.createSymlink(name, mode, uid, gid, "") }
      case Stat.Type.CHAR => blockingCall { dir.createCharacterDevice(name, mode, uid, gid, 0) }
      case Stat.Type.BLOCK => blockingCall { dir.createBlockDevice(name, mode, uid, gid, 0) }
      case Stat.Type.FIFO => blockingCall { dir.createFIFO(name, mode, uid, gid) }
      case Stat.Type.SOCK => blockingCall { dir.createUnixSocket(name, mode, uid, gid) }
      case _ =>
        throw new NotSuppException("Unknown FileType")
    }

    logger.info(s"Created ${iptr.ftype} with name $name and inode ${iptr.number}")

    iptr.number
  }

  /**
    * Get file system's usage information.
    *
    * @return file system's usage information.
    * @throws IOException meh
    */
  @throws[IOException]
  def getFsStat: FsStat = {
    logger.info("getFsStat")
    val totalSpace = 1024*1024*1024
    val totalFiles = 100
    val usedSpace  = 1024*1024
    val usedFiles  = 50
    new FsStat(totalSpace, totalFiles, usedSpace, usedFiles)
  }

  /**
    * Get inode of the root object for given file system.
    *
    * @return inode of the root object.
    * @throws IOException meh
    */
  @throws[IOException]
  def getRootInode: Inode = 1L

  /**
    * Get inode of the object with a given name in provided directory.
    *
    * @param parent parent directory's inode.
    * @param name   object name.
    * @return inode of the object.
    * @throws IOException meh
    */
  @throws[IOException]
  def lookup(parent: Inode, name: String): Inode = {
    logger.info(s"lookup ${inode2long(parent)} $name")
    val dir = getDirectory(parent)

    blockingCall { dir.getEntry(name) } match {
      case Some(iptr) => iptr.number
      case None =>
        val d = getDirectory(parent)
        val mm = Stat.S_IFDIR | 0x1ff
        logger.info(s"   lookup throwing NoEntException. Known good mode: ${mm.toOctalString}. Actual: ${nfsStat(d).getMode.toOctalString}")
        throw new NoEntException(s"No such file $name")
    }
  }

  /**
    * Create a hard-link to an existing file system object.
    *
    * @param parent  directory, where new object must be created.
    * @param link    an inode of existing file system object.
    * @param name    name of the new object.
    * @param subject the owner subject of a newly created object.
    * @return inode of the newly created object.
    * @throws IOException meh
    */
  @throws[IOException]
  def link(parent: Inode, link: Inode, name: String, subject: Subject): Inode = {
    logger.info(s"link $name")
    val pdir = getDirectory(parent)
    val target = load(link)

    blockingCall { pdir.hardLink(name, target) }

    target.inodeNumber
  }

  /**
    * Get list of file system objects in the given directory. The provided
    * cookie identifies a logical offset of the listing, if directory
    * listing is processed in chunks. The verifie argument used to
    * validate cookies as directory content can be changes and earlier generated
    * cookie cannot be used any more. For initial listing a zero cookie and verifier
    * is used. The returned listing will contain only entries with cookies
    * greater than specified value.
    *
    * @param inode    inode of the directory to list.
    * @param verifier opaque verifier to identify { @code snapshot} to list.
    * @param cookie a logical offset in the listing.
    * @return DirectoryStream containing directory listing.
    * @throws IOException meh
    */
  @throws[IOException]
  def list(inode: Inode, verifier: Array[Byte], cookie: Long): DirectoryStream = {
    logger.info(s"List directory ${inode2long(inode)}")

    val dir = getDirectory(inode)

    logger.info(s"Directory stats: ${nfsStat(dir)}")

    val entries = blockingCall { dir.getContents() }.zipWithIndex.map { t =>
      val (org.aspen_ddp.aspen.amoebafs.DirectoryEntry(name, iptr), cookie) = t
      val file = load(iptr.number)
      new DirectoryEntry(name, iptr.number, nfsStat(file), cookie)
    }

    import scala.jdk.CollectionConverters._

    new DirectoryStream(DirectoryStream.ZERO_VERIFIER, entries.asJavaCollection)
  }

  /**
    * Generate a opaque directory verifier which is identified with can
    * be used as identifier of directory's state snapshot.
    *
    * @param inode inode of the directory to create verifier.
    * @return opaque verifier.
    * @throws IOException meh
    */
  @throws[IOException]
  def directoryVerifier(inode: Inode): Array[Byte] = DirectoryStream.ZERO_VERIFIER

  /**
    * Create a new sub-directory in a given directory.
    *
    * @param parent  directory, where new sub-directory must be created.
    * @param name    the name of the newly created sub-directory.
    * @param subject the owner subject of a newly created sub-directory.
    * @param mode    initial permission mask.
    * @return inode of the newly created sub-directory.
    * @throws IOException meh
    */
  @throws[IOException]
  def mkdir(parent: Inode, name: String, subject: Subject, mode: Int): Inode = synchronized {
    logger.info(s"mkdir in dir ${inode2long(parent)} name $name mode ${mode.toOctalString}")

    val pdir = getDirectory(parent)

    val uid = UnixSubjects.getUid(subject).toInt
    val gid = UnixSubjects.getPrimaryGid(subject).toInt

    val iptr = blockingCall { pdir.createDirectory(name, mode, uid, gid) }

    logger.info(s"Created new directory $name in parent dir ${pdir.inodeNumber} with inode number ${iptr.number}")

    iptr.number
  }

  /**
    * Move file system object from one directory to another.
    *
    * @param src     the directory from which to move the object.
    * @param oldName object's name in the source directory.
    * @param dest    the directory where the file system object should be moved.
    * @param newName object's name in the new directory.
    * @return true if file system was changed.
    * @throws IOException meh
    */
  @throws[IOException]
  def move(src: Inode, oldName: String, dest: Inode, newName: String): Boolean = synchronized {
    val s = getDirectory(src)
    val d = getDirectory(dest)

    logger.info(s"move $oldName to $newName. Src ${s.inodeNumber} Dst ${d.inodeNumber}")

    blockingCall { s.getEntry(oldName) } match {
      case None => throw new NoEntException(s"$oldName does not exist")

      case Some(iptr) =>

        val odir = iptr.ftype match {
          case FileType.Directory => Some(getDirectory(iptr.number))
          case _ => None
        }

        val refreshOnFailure = s :: d :: odir.map(t => t :: Nil).getOrElse(Nil)

        def onFail(err: Throwable): Future[Unit] = err match {
          case e: InvalidInode => throw StopRetrying(e)
          case e: DirectoryEntryExists => throw StopRetrying(e)
          case e: DirectoryEntryDoesNotExist => throw StopRetrying(e)
          case e: FatalReadError => throw StopRetrying(e)
          case _ =>
            val refreshOld = s.getEntry(oldName)
            val refreshNew = d.getEntry(newName)
            for {
              _ <- Future.sequence(refreshOnFailure.map(_.refresh())).map(_ => ())
              oldf <- refreshOld
              newf <- refreshNew
            } yield {
              if (oldf.isEmpty)
                throw StopRetrying(DirectoryEntryDoesNotExist(s.pointer, oldName))
              if (newf.nonEmpty)
                throw StopRetrying(DirectoryEntryExists(d.pointer, newName))
            }
        }

        fs.client.transactUntilSuccessfulWithRecovery(onFail) {  implicit tx =>

          s.prepareDelete(oldName, decref = false)
          d.prepareInsert(newName, iptr, incref = false)

          odir.foreach { tdir =>
            tdir.prepareSetParentDirectory(d)
          }

          Future.unit
        }

        true
    }
  }

  /**
    * Get parent directory of a given object.
    *
    * @param inode inode of a file system object for which the parent inode is
    *              desired.
    * @return parent directory of the given object.
    * @throws IOException meh
    */
  @throws[IOException]
  def parentOf(inode: Inode): Inode = getDirectory(inode).inode.asInstanceOf[DirectoryInode].oparent match {
    case None => throw new NoEntException()
    case Some(ptr) => ptr.number
  }


  /**
    * Read data from file with a given inode into data.
    *
    * @param inode  inode of the file to read from.
    * @param data   byte array for writing.
    * @param offset file's position to read from.
    * @param count  number of bytes to read.
    * @return number of bytes read from the file, possibly zero. -1 if EOF is
    *         reached.
    * @throws IOException meh
    */
  @throws[IOException]
  def read(inode: Inode, data: Array[Byte], offset: Long, count: Int): Int = {
    logger.info(s"read ${inode2long(inode)} offset $offset count $count")
    val fh = getFileHandle(inode)
    if (offset > fh.file.size)
      -1
    else {
      blockingCall { fh.read(offset, count) } match {
        case None => -1
        case Some(db) =>
          db.asReadOnlyBuffer().get(data, 0, db.size)
          db.size
      }
    }
  }

  /**
    * Get value of a symbolic link object.
    *
    * @param inode symbolic link's inode.
    * @return value of a symbolic link.
    * @throws IOException meh
    */
  @throws[IOException]
  def readlink(inode: Inode): String = getSymLink(inode).symLinkAsString

  /**
    * Remove a file system object from the given directory and possibly the
    * object itself.
    *
    * @param parent directory from which file system object is removed.
    * @param name   object's name in the directory.
    * @throws IOException meh
    */
  @throws[IOException]
  def remove(parent: Inode, name: String): Unit = {
    val dir = getDirectory(parent)

    blockingCall { dir.delete(name) }
  }

  /**
    * Create a symbolic link.
    *
    * @param parent  inode of the directory, where symbolic link is created.
    * @param name    name of the symbolic link object.
    * @param link    the value the symbolic link points to.
    * @param subject the owner subject of a newly created object.
    * @param mode    initial permission mask.
    * @return inode of newly created object.
    * @throws IOException meh
    */
  @throws[IOException]
  def symlink(parent: Inode, name: String, link: String, subject: Subject, mode: Int): Inode = {
    val pdir = getDirectory(parent)

    val uid = UnixSubjects.getUid(subject).toInt
    val gid = UnixSubjects.getPrimaryGid(subject).toInt

    val iptr = blockingCall { pdir.createSymlink(name, mode, uid, gid, link) }

    logger.info(s"Created new symlink $name in parent dir ${pdir.pointer.number} with inode number ${iptr.number} pointing to $link")

    iptr.number
  }

  /**
    * Write provided data into inode with a given stability level.
    *
    * @param inode          inode of the file to write.
    * @param data           data to be written.
    * @param offset         the file position to begin writing at.
    * @param count          number of bytes to write.
    * @param stabilityLevel data stability level.
    * @return write result.
    * @throws IOException meh
    */
  @throws[IOException]
  def write(inode: Inode, data: Array[Byte], offset: Long, count: Int, stabilityLevel: VirtualFileSystem.StabilityLevel): VirtualFileSystem.WriteResult = {
    logger.info(s"write ${inode2long(inode)} offset $offset count $count")

    val fh = getFileHandle(inode)

    blockingCall { fh.write(offset, data) }

    if (stabilityLevel != VirtualFileSystem.StabilityLevel.DATA_SYNC)
      blockingCall { fh.flush() }

    if (stabilityLevel == VirtualFileSystem.StabilityLevel.FILE_SYNC)
      blockingCall { Future.sequence( fh.flush() :: fh.file.flush() :: Nil ) }

    new VirtualFileSystem.WriteResult(stabilityLevel, count)
  }

  /**
    * Flush data in dirty state to the stable storage. Typically
    * follows write() operation.
    *
    * @param inode  inode of the file to commit.
    * @param offset the file position to start commit at.
    * @param count  number of bytes to commit.
    * @throws IOException meh
    */
  @throws[IOException]
  def commit(inode: Inode, offset: Long, count: Int): Unit = getFileHandle(inode).flush()

  /**
    * Get file system object's attributes.
    *
    * @param inode inode of the file system object.
    * @return file's attributes.
    * @throws IOException meh
    */
  @throws[IOException]
  def getattr(inode: Inode): Stat = {
    val stat = nfsStat(load(inode))

    if (inode2long(inode) == 1)
      stat.setMode(stat.getMode | FileMode.S_IRWXO)

    logger.info(s"getattr ${inode2long(inode)}: $stat")
    stat
  }

  /**
    * Set/update file system object's attributes.
    *
    * @param inode inode of the file system object.
    * @param stat  file's attributes to set.
    * @throws IOException meh
    */
  @throws[IOException]
  def setattr(inode: Inode, stat: Stat): Unit = {
    val i = load(inode)

    if (stat.isDefined(Stat.StatAttribute.OWNER))
      blockingCall { i.setUID(stat.getUid) }

    if (stat.isDefined(Stat.StatAttribute.GROUP))
      blockingCall { i.setGID(stat.getGid) }

    if (stat.isDefined(Stat.StatAttribute.DEV)) i match {
      case _: BlockDevice => // TODO what to do with this?
      case _: CharacterDevice => // TODO what to do with this?
      case _ => new InvalException("Can't set device attribute on non-device file")
    }

    if (stat.isDefined(Stat.StatAttribute.RDEV)) i match {
      case d: BlockDevice => blockingCall { d.setrdev(stat.getRdev) }
      case d: CharacterDevice => blockingCall { d.setrdev(stat.getRdev) }
      case _ => new InvalException("Can't set rdevice attribute on non-device file")
    }

    if (stat.isDefined(Stat.StatAttribute.MODE))
      blockingCall { i.setMode(stat.getMode) }

    if (stat.isDefined(Stat.StatAttribute.SIZE)) i match {

      case _: File =>
        blockingCall { getFileHandle(inode).truncate(stat.getSize) }

      case _ => throw new InvalException("Can't change size of non-regular file")
    }

    if (stat.isDefined(Stat.StatAttribute.ATIME)) blockingCall { i.setAtime(Timespec(stat.getATime)) }
    if (stat.isDefined(Stat.StatAttribute.MTIME)) blockingCall { i.setMtime(Timespec(stat.getMTime)) }
    if (stat.isDefined(Stat.StatAttribute.CTIME)) blockingCall { i.setCtime(Timespec(stat.getCTime)) }
  }

  /**
    * Get file system object's Access Control List.
    *
    * @param inode inode of the file system object.
    * @return object's access control list.
    * @throws IOException meh
    */
  @throws[IOException]
  def getAcl(inode: Inode): Array[nfsace4] = NoAcl

  /**
    * Set file system object's Access Control List.
    *
    * @param inode inode of the file system object.
    * @param acl   access control list to set.
    * @throws IOException meh
    */
  @throws[IOException]
  def setAcl(inode: Inode, acl: Array[nfsace4]): Unit = ()

  /**
    * Returns true if file system object eligible for pNFS operations.
    *
    * @param inode inode of the file system object to test for PNFS operations.
    * @return true if file system object eligible for pNFS operations.
    * @throws IOException meh
    */
  @throws[IOException]
  def hasIOLayout(inode: Inode): Boolean =  false

  /**
    * Get instance of a AclCheckable object which can perform access
    * control list check.
    *
    * @return instance of AclCheckable.
    */
  def getAclCheckable: AclCheckable = AclCheckable.UNDEFINED_ALL

  /**
    * Get instance of a NfsIdMapping object which can provide principal
    * identity mapping.
    *
    * @return instance of NfsIdMapping.
    */
  def getIdMapper: NfsIdMapping = IdMapper

  def access(s: javax.security.auth.Subject, ino: org.dcache.nfs.vfs.Inode, num: Int) = 0xFFFF

  def getCaseInsensitive: Boolean = false

  def getCasePreserving: Boolean = true
}

