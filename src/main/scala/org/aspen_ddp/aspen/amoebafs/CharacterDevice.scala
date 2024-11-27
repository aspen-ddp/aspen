package org.aspen_ddp.aspen.amoebafs

import scala.concurrent.Future

trait CharacterDevice extends BaseFile {
  val pointer: CharacterDevicePointer

  def rdev: Int

  def setrdev(newrdev: Int): Future[Unit]
}
