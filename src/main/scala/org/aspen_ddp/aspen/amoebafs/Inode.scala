package org.aspen_ddp.aspen.amoebafs

import scala.language.implicitConversions
import java.nio.{ByteBuffer, ByteOrder}
import java.nio.charset.StandardCharsets

import org.aspen_ddp.aspen.client.AspenClient
import org.aspen_ddp.aspen.client.tkvl.{Root, RootManager}
import org.aspen_ddp.aspen.common.DataBuffer
import org.aspen_ddp.aspen.common.objects.{DataObjectPointer, KeyValueObjectPointer}
import org.aspen_ddp.aspen.common.util.Varint
import org.aspen_ddp.aspen.amoebafs.impl.simple.SimpleDirectoryRootManager
import org.apache.logging.log4j.scala.Logging

object Inode {

  def apply(client: AspenClient,
            content: DataBuffer): Inode = {
    val bb: ByteBuffer = content
    bb.position(1 + 8)
    val mode = bb.getInt()
    val fileType = FileType.fromMode(mode)
    fileType match {
      case FileType.File            => FileInode(client, content)
      case FileType.Directory       => DirectoryInode(client, content)
      case FileType.Symlink         => SymlinkInode(content)
      case FileType.UnixSocket      => UnixSocketInode(content)
      case FileType.CharacterDevice => CharacterDeviceInode(content)
      case FileType.BlockDevice     => BlockDeviceInode(content)
      case FileType.FIFO            => FIFOInode(content)
    }
  }

  // First byte is the fileTypeVersionEncoding
  def decode(bb: ByteBuffer): (Byte, Long, Int, Int, Int, Int, Timespec, Timespec, Timespec, Option[KeyValueObjectPointer]) = {
    val encodingFormat = bb.get() & 0x0F
    val inodeNumber = bb.getLong()
    val mode = bb.getInt()
    val uid = bb.getInt()
    val gid = bb.getInt()
    val links = bb.getInt()
    val ctime = Timespec(bb)
    val mtime = Timespec(bb)
    val atime = Timespec(bb)
    val oxattr = if (bb.get() == 0) None else Some(KeyValueObjectPointer(bb))
    (encodingFormat.toByte, inodeNumber, mode, uid, gid, links, ctime, mtime, atime, oxattr)
  }
}

sealed abstract class Inode(val inodeNumber: Long,
                            val mode: Int,
                            val uid: Int,
                            val gid: Int,
                            val links: Int,
                            val ctime: Timespec,
                            val mtime: Timespec,
                            val atime: Timespec,
                            val oxattrs: Option[KeyValueObjectPointer]
                           ) {

  def update(mode: Option[Int] = None, uid: Option[Int] = None, gid: Option[Int] = None, links: Option[Int] = None,
             ctime: Option[Timespec] = None, mtime: Option[Timespec] = None, atime: Option[Timespec] = None,
             oxattrs: Option[Option[KeyValueObjectPointer]] = None, inodeNumber: Option[Long] = None): Inode

  def fileType: FileType.Value = FileType.fromMode(mode)

  val baseEncodingVersion: Byte = 0

  def fileTypeEncodingVersion: Byte = 0

  def encodedSize: Int = 1 + 8 + 4*4 + 12*3 + 1 + oxattrs.map(_.encodedSize).getOrElse(0)

  def encodeInto(bb: ByteBuffer): Unit = {
    // Base inode encoding version is high nibble, file-type-specific encoding format is low nibble
    val fullVersion = baseEncodingVersion << 4 | (fileTypeEncodingVersion & 0xF)

    bb.put(fullVersion.toByte)
    bb.putLong(inodeNumber)
    bb.putInt(mode)
    bb.putInt(uid)
    bb.putInt(gid)
    bb.putInt(links)
    ctime.encodeInto(bb)
    mtime.encodeInto(bb)
    atime.encodeInto(bb)
    oxattrs match {
      case None =>
        bb.put(0.toByte)

      case Some(p) =>
        bb.put(1.toByte)
        p.encodeInto(bb)
    }
  }

  def toArray: Array[Byte] = {
    val arr = new Array[Byte](encodedSize)
    val bb = ByteBuffer.wrap(arr)
    encodeInto(bb)
    arr
  }

  def toDataBuffer: DataBuffer = DataBuffer(toArray)
}

object DirectoryInode {

  def init(mode: Int, uid: Int,
           gid: Int,
           parentDirectory: Option[DirectoryPointer],
           oinodeNumber: Option[Long],
           contents: Root): DirectoryInode = {
    val now = Timespec.now
    val correctedMode = FileType.ensureModeFileType(mode, FileType.Directory)
    new DirectoryInode(oinodeNumber.getOrElse(0), correctedMode, uid, gid, 1, now, now, now, None,
      parentDirectory, contents)
  }

  def apply(client: AspenClient, content: DataBuffer): DirectoryInode = {
    val bb = content.asReadOnlyBuffer()
    val (_, inodeNumber, mode, uid, gid, links, ctime, mtime, atime, oxattrs) = Inode.decode(bb)
    val mask = bb.get()
    val oparent = if ((mask & 1 << 1) == 0) None else Some(InodePointer(bb).asInstanceOf[DirectoryPointer])
    val dcontent = Root(client, bb)

    new DirectoryInode(inodeNumber, mode, uid, gid, links, ctime, mtime, atime, oxattrs, oparent, dcontent)
  }
}

class DirectoryInode(inodeNumber: Long,
                     mode: Int,
                     uid: Int,
                     gid: Int,
                     links: Int,
                     ctime: Timespec,
                     mtime: Timespec,
                     atime: Timespec,
                     oxattrs: Option[KeyValueObjectPointer],
                     val oparent: Option[DirectoryPointer],
                     val contents: Root) extends Inode(inodeNumber, mode, uid, gid, links,
  ctime, mtime, atime, oxattrs) {

  override def update(mode: Option[Int] = None, uid: Option[Int] = None, gid: Option[Int] = None,
                      links: Option[Int] = None, ctime: Option[Timespec] = None, mtime: Option[Timespec] = None,
                      atime: Option[Timespec] = None, oxattrs: Option[Option[KeyValueObjectPointer]] = None,
                      inodeNumber: Option[Long] = None): Inode = {
    new DirectoryInode(inodeNumber.getOrElse(this.inodeNumber), mode.getOrElse(this.mode), uid.getOrElse(this.uid),
      gid.getOrElse(this.gid),
      links.getOrElse(this.links), ctime.getOrElse(this.ctime), mtime.getOrElse(this.mtime),
      atime.getOrElse(this.atime), oxattrs.getOrElse(this.oxattrs), this.oparent, this.contents)
  }

  def setParentDirectory(newParent: Option[DirectoryPointer]): DirectoryInode = {
    new DirectoryInode(inodeNumber, mode, uid, gid, links, ctime, mtime, atime, oxattrs, newParent, contents)
  }

  def setContentTree(newContents: Root): DirectoryInode = {
    new DirectoryInode(inodeNumber, mode, uid, gid, links, ctime, mtime, atime, oxattrs, oparent, newContents)
  }

  override def encodedSize: Int = {
    super.encodedSize + 1 + oparent.map(_.encodedSize).getOrElse(0) + contents.encodedSize
  }

  override def encodeInto(bb: ByteBuffer): Unit = {
    super.encodeInto(bb)
    val mask = oparent.map(_ => 1 << 1).getOrElse(0)
    bb.put(mask.toByte)
    oparent.foreach(_.encodeInto(bb))
    contents.encodeInto(bb)
  }
}

object FileInode {

  def init(mode: Int, uid: Int, gid: Int, contents: Root): FileInode = {
    val now = Timespec.now
    val correctedMode = FileType.ensureModeFileType(mode, FileType.File)
    new FileInode(0, correctedMode, uid, gid, 1, now, now, now, None, 0, contents)
  }

  def apply(client: AspenClient, content: DataBuffer): FileInode = {
    val bb = content.asReadOnlyBuffer()
    val (_, inodeNumber, mode, uid, gid, links, ctime, mtime, atime, oxattrs) = Inode.decode(bb)
    val size = bb.getLong()
    val root = Root(client, bb)

    new FileInode(inodeNumber, mode, uid, gid, links, ctime, mtime, atime, oxattrs, size, root)
  }
}

class FileInode(inodeNumber: Long,
                mode: Int,
                uid: Int,
                gid: Int,
                links: Int,
                ctime: Timespec,
                mtime: Timespec,
                atime: Timespec,
                oxattrs: Option[KeyValueObjectPointer],
                val size: Long,
                val contents: Root) extends Inode(inodeNumber, mode, uid, gid, links,
  ctime, mtime, atime, oxattrs) with Logging {

  //logger.info(s"Created FileInode with ocontents ${ocontents.map(dp => dp.uuid)}")

  def updateContent(newSize: Long, newMtime: Timespec): FileInode =
    new FileInode(inodeNumber, mode, uid, gid, links, ctime, newMtime, atime, oxattrs, newSize, contents)
    
  override def update(mode: Option[Int] = None, uid: Option[Int] = None, gid: Option[Int] = None,
                      links: Option[Int] = None, ctime: Option[Timespec] = None, mtime: Option[Timespec] = None,
                      atime: Option[Timespec] = None, oxattrs: Option[Option[KeyValueObjectPointer]] = None,
                      inodeNumber: Option[Long] = None): Inode = {
    new FileInode(inodeNumber.getOrElse(this.inodeNumber), mode.getOrElse(this.mode), uid.getOrElse(this.uid),
      gid.getOrElse(this.gid),
      links.getOrElse(this.links), ctime.getOrElse(this.ctime), mtime.getOrElse(this.mtime),
      atime.getOrElse(this.atime), oxattrs.getOrElse(this.oxattrs), size, contents)
  }
  
  override def encodedSize: Int = {
    super.encodedSize + 8 + contents.encodedSize
  }

  override def encodeInto(bb: ByteBuffer): Unit = {
    super.encodeInto(bb)
    bb.putLong(size)
    contents.encodeInto(bb)
  }
}

object SymlinkInode {

  def init(mode: Int, uid: Int, gid: Int, content: String): SymlinkInode = {
    val now = Timespec.now
    val correctedMode = FileType.ensureModeFileType(mode, FileType.Symlink)
    new SymlinkInode(0, correctedMode, uid, gid, 1, now, now, now, None,
      content.getBytes(StandardCharsets.UTF_8))
  }

  def apply(content: DataBuffer): SymlinkInode = {
    val bb = content.asReadOnlyBuffer()
    val (_, inodeNumber, mode, uid, gid, links, ctime, mtime, atime, oxattrs) = Inode.decode(bb)
    val contentSize = Varint.getUnsignedInt(bb)
    val contents = new Array[Byte](contentSize)
    bb.get(contents)

    new SymlinkInode(inodeNumber, mode, uid, gid, links, ctime, mtime, atime, oxattrs, contents)
  }
}

class SymlinkInode(inodeNumber: Long,
                   mode: Int,
                   uid: Int,
                   gid: Int,
                   links: Int,
                   ctime: Timespec,
                   mtime: Timespec,
                   atime: Timespec,
                   oxattrs: Option[KeyValueObjectPointer],
                   val content: Array[Byte]) extends Inode(inodeNumber, mode, uid, gid, links,
  ctime, mtime, atime, oxattrs) {

  override def update(mode: Option[Int] = None, uid: Option[Int] = None, gid: Option[Int] = None,
                      links: Option[Int] = None, ctime: Option[Timespec] = None, mtime: Option[Timespec] = None,
                      atime: Option[Timespec] = None, oxattrs: Option[Option[KeyValueObjectPointer]] = None,
                      inodeNumber: Option[Long] = None): Inode = {
    new SymlinkInode(inodeNumber.getOrElse(this.inodeNumber), mode.getOrElse(this.mode), uid.getOrElse(this.uid),
      gid.getOrElse(this.gid),
      links.getOrElse(this.links), ctime.getOrElse(this.ctime), mtime.getOrElse(this.mtime), atime.getOrElse(this.atime),
      oxattrs.getOrElse(this.oxattrs), content)
  }

  def setContents(newContents: Array[Byte]): SymlinkInode = {
    new SymlinkInode(inodeNumber, mode, uid, gid, links, ctime, mtime, atime, oxattrs, newContents)
  }

  def size: Int = content.length

  def asString: String = new String(content, StandardCharsets.UTF_8)

  override def encodedSize: Int = {
    super.encodedSize + Varint.getUnsignedIntEncodingLength(content.length) + content.length
  }

  override def encodeInto(bb: ByteBuffer): Unit = {
    super.encodeInto(bb)
    Varint.putUnsignedInt(bb, content.length)
    bb.put(content)
  }
}


object UnixSocketInode {

  def init(mode: Int, uid: Int, gid: Int): UnixSocketInode = {
    val now = Timespec.now
    val correctedMode = FileType.ensureModeFileType(mode, FileType.UnixSocket)
    new UnixSocketInode(0, correctedMode, uid, gid, 1, now, now, now, None)
  }

  def apply(content: DataBuffer): UnixSocketInode = {
    val bb = content.asReadOnlyBuffer()
    val (_, inodeNumber, mode, uid, gid, links, ctime, mtime, atime, oxattrs) = Inode.decode(bb)

    new UnixSocketInode(inodeNumber, mode, uid, gid, links, ctime, mtime, atime, oxattrs)
  }
}

class UnixSocketInode(inodeNumber: Long,
                      mode: Int,
                      uid: Int,
                      gid: Int,
                      links: Int,
                      ctime: Timespec,
                      mtime: Timespec,
                      atime: Timespec,
                      oxattrs: Option[KeyValueObjectPointer]) extends Inode(inodeNumber, mode, uid, gid, links,
  ctime, mtime, atime, oxattrs) {

  override def update(mode: Option[Int] = None, uid: Option[Int] = None, gid: Option[Int] = None,
                      links: Option[Int] = None, ctime: Option[Timespec] = None, mtime: Option[Timespec] = None,
                      atime: Option[Timespec] = None, oxattrs: Option[Option[KeyValueObjectPointer]] = None,
                      inodeNumber: Option[Long] = None): Inode = {
    new UnixSocketInode(inodeNumber.getOrElse(this.inodeNumber), mode.getOrElse(this.mode), uid.getOrElse(this.uid),
      gid.getOrElse(this.gid),
      links.getOrElse(this.links), ctime.getOrElse(this.ctime), mtime.getOrElse(this.mtime),
      atime.getOrElse(this.atime), oxattrs.getOrElse(this.oxattrs))
  }
}


object FIFOInode {

  def init(mode: Int, uid: Int, gid: Int): FIFOInode = {
    val now = Timespec.now
    val correctedMode = FileType.ensureModeFileType(mode, FileType.FIFO)
    new FIFOInode(0, correctedMode, uid, gid, 1, now, now, now, None)
  }

  def apply(content: DataBuffer): FIFOInode = {
    val bb = content.asReadOnlyBuffer()
    val (_, inodeNumber, mode, uid, gid, links, ctime, mtime, atime, oxattrs) = Inode.decode(bb)

    new FIFOInode(inodeNumber, mode, uid, gid, links, ctime, mtime, atime, oxattrs)
  }
}

class FIFOInode(inodeNumber: Long,
                mode: Int,
                uid: Int,
                gid: Int,
                links: Int,
                ctime: Timespec,
                mtime: Timespec,
                atime: Timespec,
                oxattrs: Option[KeyValueObjectPointer]) extends Inode(inodeNumber, mode, uid, gid, links,
  ctime, mtime, atime, oxattrs) {

  override def update(mode: Option[Int] = None, uid: Option[Int] = None, gid: Option[Int] = None,
                      links: Option[Int] = None, ctime: Option[Timespec] = None, mtime: Option[Timespec] = None,
                      atime: Option[Timespec] = None, oxattrs: Option[Option[KeyValueObjectPointer]] = None,
                      inodeNumber: Option[Long] = None): Inode = {
    new FIFOInode(inodeNumber.getOrElse(this.inodeNumber), mode.getOrElse(this.mode), uid.getOrElse(this.uid),
      gid.getOrElse(this.gid),
      links.getOrElse(this.links), ctime.getOrElse(this.ctime), mtime.getOrElse(this.mtime),
      atime.getOrElse(this.atime), oxattrs.getOrElse(this.oxattrs))
  }
}

object CharacterDeviceInode {

  def init(mode: Int, uid: Int, gid: Int, rdev: Int): CharacterDeviceInode = {
    val now = Timespec.now
    val correctedMode = FileType.ensureModeFileType(mode, FileType.CharacterDevice)
    new CharacterDeviceInode(0, correctedMode, uid, gid, 1, now, now, now, None, rdev)
  }

  def apply(content: DataBuffer): CharacterDeviceInode = {
    val bb = content.asReadOnlyBuffer()
    val (_, inodeNumber, mode, uid, gid, links, ctime, mtime, atime, oxattrs) = Inode.decode(bb)
    val rdev = bb.getInt()

    new CharacterDeviceInode(inodeNumber, mode, uid, gid, links, ctime, mtime, atime, oxattrs, rdev)
  }
}

class CharacterDeviceInode(inodeNumber: Long,
                           mode: Int,
                           uid: Int,
                           gid: Int,
                           links: Int,
                           ctime: Timespec,
                           mtime: Timespec,
                           atime: Timespec,
                           oxattrs: Option[KeyValueObjectPointer],
                           val rdev: Int) extends Inode(inodeNumber, mode, uid, gid, links,
  ctime, mtime, atime, oxattrs) {

  override def update(mode: Option[Int] = None, uid: Option[Int] = None, gid: Option[Int] = None,
                      links: Option[Int] = None, ctime: Option[Timespec] = None, mtime: Option[Timespec] = None,
                      atime: Option[Timespec] = None, oxattrs: Option[Option[KeyValueObjectPointer]] = None,
                      inodeNumber: Option[Long] = None): Inode = {
    new CharacterDeviceInode(inodeNumber.getOrElse(this.inodeNumber), mode.getOrElse(this.mode),
      uid.getOrElse(this.uid), gid.getOrElse(this.gid),
      links.getOrElse(this.links), ctime.getOrElse(this.ctime), mtime.getOrElse(this.mtime),
      atime.getOrElse(this.atime), oxattrs.getOrElse(this.oxattrs), rdev)
  }

  def setDeviceType(newrdev: Int): CharacterDeviceInode = {
    new CharacterDeviceInode(inodeNumber, mode, uid, gid, links, ctime, mtime, atime, oxattrs, newrdev)
  }

  override def encodedSize: Int = {
    super.encodedSize + 4
  }

  override def encodeInto(bb: ByteBuffer): Unit = {
    super.encodeInto(bb)
    bb.putInt(rdev)
  }
}


object BlockDeviceInode {

  def init(mode: Int, uid: Int, gid: Int, rdev: Int): BlockDeviceInode = {
    val now = Timespec.now
    val correctedMode = FileType.ensureModeFileType(mode, FileType.BlockDevice)
    new BlockDeviceInode(0, correctedMode, uid, gid, 1, now, now, now, None, rdev)
  }

  def apply(content: DataBuffer): BlockDeviceInode = {
    val bb = content.asReadOnlyBuffer()
    val (_, inodeNumber, mode, uid, gid, links, ctime, mtime, atime, oxattrs) = Inode.decode(bb)
    val rdev = bb.getInt()

    new BlockDeviceInode(inodeNumber, mode, uid, gid, links, ctime, mtime, atime, oxattrs, rdev)
  }
}

class BlockDeviceInode(inodeNumber: Long,
                       mode: Int,
                       uid: Int,
                       gid: Int,
                       links: Int,
                       ctime: Timespec,
                       mtime: Timespec,
                       atime: Timespec,
                       oxattrs: Option[KeyValueObjectPointer],
                       val rdev: Int) extends Inode(inodeNumber, mode, uid, gid, links,
  ctime, mtime, atime, oxattrs) {

  override def update(mode: Option[Int] = None, uid: Option[Int] = None, gid: Option[Int] = None,
                      links: Option[Int] = None, ctime: Option[Timespec] = None, mtime: Option[Timespec] = None,
                      atime: Option[Timespec] = None, oxattrs: Option[Option[KeyValueObjectPointer]] = None,
                      inodeNumber: Option[Long] = None): Inode = {
    new BlockDeviceInode(inodeNumber.getOrElse(this.inodeNumber), mode.getOrElse(this.mode), uid.getOrElse(this.uid),
      gid.getOrElse(this.gid),
      links.getOrElse(this.links), ctime.getOrElse(this.ctime), mtime.getOrElse(this.mtime),
      atime.getOrElse(this.atime), oxattrs.getOrElse(this.oxattrs), rdev)
  }

  def setDeviceType(newrdev: Int): BlockDeviceInode = {
    new BlockDeviceInode(inodeNumber, mode, uid, gid, links, ctime, mtime, atime, oxattrs, newrdev)
  }

  override def encodedSize: Int = {
    super.encodedSize + 4
  }

  override def encodeInto(bb: ByteBuffer): Unit = {
    super.encodeInto(bb)
    bb.putInt(rdev)
  }
}
