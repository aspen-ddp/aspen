package org.aspen_ddp.aspen.amoebafs.error

import org.aspen_ddp.aspen.amoebafs.DirectoryPointer

case class DirectoryEntryExists(pointer: DirectoryPointer, name: String) extends FSError {
  override def toString: String = s"$name already exists in directory ${pointer.uuid}"
}
