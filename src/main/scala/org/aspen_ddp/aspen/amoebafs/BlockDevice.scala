package org.aspen_ddp.aspen.amoebafs

import scala.concurrent.Future

trait BlockDevice extends BaseFile {
  val pointer: BlockDevicePointer

  def rdev: Int

  def setrdev(newrdev: Int): Future[Unit]
}
