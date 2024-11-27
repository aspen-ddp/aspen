package org.aspen_ddp.aspen.amoebafs

import org.aspen_ddp.aspen.common.DataBuffer

import scala.concurrent.{ExecutionContext, Future}

trait File extends BaseFile {
  val pointer: FilePointer

  def inode: FileInode

  def size: Long = inode.size

  def read(offset: Long, nbytes: Int): Future[Option[DataBuffer]]

  def write(offset: Long,
            buffers: List[DataBuffer]): Future[(Long, List[DataBuffer])]

  /** Outer future completes when the Inode and index have been updated to the new size. Inner future completes
    * when the background data deletion operation is done
    */
  def truncate(offset: Long): Future[Future[Unit]]

  def write(offset: Long,
            buffer: DataBuffer): Future[(Long, List[DataBuffer])] = {
    write(offset, List(buffer))
  }

  def debugReadFully(): Future[Array[Byte]]

  private[this] var openHandles: Set[FileHandle] = Set()

  def open(): FileHandle = {
    val fh = fs.openFileHandle(this)
    synchronized { openHandles += fh }
    fh
  }

  private[aspen] def close(fh: FileHandle): Unit =  {
    synchronized { openHandles -= fh }
    fs.closeFileHandle(fh)
  }

  private[aspen] def hasOpenHandles: Boolean = synchronized { openHandles.nonEmpty }

}
