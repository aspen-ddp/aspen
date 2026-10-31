package org.aspen_ddp.aspen.vdisk

import scala.concurrent.Future

/** The block-device interface a frontend (the tcmu-runner bridge, or later a direct TCMU ring driver) serves.
  * Requests may be outstanding together and may complete in any order. Failures are reported as [[VDiskError]]s,
  * which the frontend translates to SCSI status.
  *
  * Writes complete once the data is held in memory. `flush`, and writes with `fua`, complete only once the covered
  * data is committed. A read always returns the bytes of every write that completed before it was issued.
  */
trait BlockDevice:

  def geometry: DiskGeometry

  def read(offset: Long, length: Int): Future[Array[Byte]]

  def write(offset: Long, data: Array[Byte], fua: Boolean): Future[Unit]

  /** Completes once every write that completed before the call is committed */
  def flush(): Future[Unit]

  /** Each range is (offset, length) */
  def unmap(ranges: List[(Long, Long)]): Future[Unit]

  def grow(newSize: Long): Future[Unit]

  /** Crash-only: discards dirty data without flushing it and fails pending requests with `Fenced` */
  def close(): Unit
