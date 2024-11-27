package org.aspen_ddp.aspen.amoebafs.impl.simple

import org.aspen_ddp.aspen.common.objects.ObjectRevision
import org.aspen_ddp.aspen.amoebafs.{FileSystem, Inode, Symlink, SymlinkInode, SymlinkPointer}

import scala.concurrent.Future

object SimpleSymlink {
  case class SetSymLink(newLink: Array[Byte]) extends SimpleBaseFile.SimpleSet {
    def update(inode: Inode): Inode = inode.asInstanceOf[SymlinkInode].setContents(newLink)
  }
}

class SimpleSymlink(override val pointer: SymlinkPointer,
                    initialInode: SymlinkInode,
                    revision: ObjectRevision,
                    fs: FileSystem) extends SimpleBaseFile(pointer, revision, initialInode, fs) with Symlink {

  import SimpleSymlink._

  override def inode: SymlinkInode = super.inode.asInstanceOf[SymlinkInode]

  def size: Int = inode.size

  def symLink: Array[Byte] = inode.content

  def setSymLink(newLink: Array[Byte]): Future[Unit] = enqueueOp(SetSymLink(newLink))
}
