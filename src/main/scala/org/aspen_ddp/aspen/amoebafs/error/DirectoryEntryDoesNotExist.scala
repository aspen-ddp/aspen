package org.aspen_ddp.aspen.amoebafs.error

import org.aspen_ddp.aspen.amoebafs.DirectoryPointer

case class DirectoryEntryDoesNotExist(pointer: DirectoryPointer, name: String) extends FSError {
  override def toString: String = s"$name does not exist in directory ${pointer.uuid}"
}