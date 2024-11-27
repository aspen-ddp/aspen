package org.aspen_ddp.aspen.amoebafs.impl.simple

import org.aspen_ddp.aspen.common.objects.ObjectRevision
import org.aspen_ddp.aspen.amoebafs.{BlockDevice, BlockDeviceInode, BlockDevicePointer, FileSystem, Inode}

import scala.concurrent.Future


object SimpleBlockDevice {
  case class SetDeviceType(rdev: Int) extends SimpleBaseFile.SimpleSet {
    def update(inode: Inode): Inode = inode.asInstanceOf[BlockDeviceInode].setDeviceType(rdev)
  }
}

class SimpleBlockDevice(override val pointer: BlockDevicePointer,
                        initialInode: BlockDeviceInode,
                        revision: ObjectRevision,
                        fs: FileSystem) extends SimpleBaseFile(pointer, revision, initialInode, fs) with BlockDevice {

  import SimpleBlockDevice._

  override def inode: BlockDeviceInode = super.inode.asInstanceOf[BlockDeviceInode]

  def rdev: Int = inode.rdev

  def setrdev(newrdev: Int): Future[Unit] = enqueueOp(SetDeviceType(rdev))
}

