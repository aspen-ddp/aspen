package org.aspen_ddp.aspen.amoebafs

import org.aspen_ddp.aspen.common.DataBuffer

import scala.concurrent.Future

trait FileHandle {
  val file: File

  def read(offset: Long, nbytes: Int): Future[Option[DataBuffer]]

  def write(offset: Long, buffers: List[DataBuffer]): Future[Unit]

  def write(offset: Long, buffer: DataBuffer): Future[Unit] = write(offset, List(buffer))

  /** Outer future completes when the Inode and index have been updated to the new size. Inner future completes
    * when the background data deletion operation is done
    */
  def truncate(offset: Long): Future[Future[Unit]]

  def flush(): Future[Unit]

  def close(): Unit = file.close(this)
}
