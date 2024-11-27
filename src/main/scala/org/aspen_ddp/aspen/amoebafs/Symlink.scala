package org.aspen_ddp.aspen.amoebafs

import java.nio.charset.StandardCharsets

import scala.concurrent.Future

trait Symlink extends BaseFile {
  val pointer: SymlinkPointer

  def size: Int

  def symLink: Array[Byte]

  def setSymLink(newLink: Array[Byte]): Future[Unit]

  def symLinkAsString: String = new String(symLink, StandardCharsets.UTF_8)
}
