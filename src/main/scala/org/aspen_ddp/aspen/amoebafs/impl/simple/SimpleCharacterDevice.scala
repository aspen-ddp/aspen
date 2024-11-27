package org.aspen_ddp.aspen.amoebafs.impl.simple

import org.aspen_ddp.aspen.common.objects.ObjectRevision
import org.aspen_ddp.aspen.amoebafs.{CharacterDevice, CharacterDeviceInode, CharacterDevicePointer, FileSystem, Inode}

import scala.concurrent.Future

object SimpleCharacterDevice {
  case class SetDeviceType(rdev: Int) extends SimpleBaseFile.SimpleSet {
    def update(inode: Inode): Inode = inode.asInstanceOf[CharacterDeviceInode].setDeviceType(rdev)
  }
}

class SimpleCharacterDevice(override val pointer: CharacterDevicePointer,
                            initialInode: CharacterDeviceInode,
                            revision: ObjectRevision,
                            fs: FileSystem) extends SimpleBaseFile(pointer, revision, initialInode, fs) with CharacterDevice {

  import SimpleCharacterDevice._

  override def inode: CharacterDeviceInode = super.inode.asInstanceOf[CharacterDeviceInode]

  def rdev: Int = inode.rdev

  def setrdev(newrdev: Int): Future[Unit] = enqueueOp(SetDeviceType(rdev))
}
