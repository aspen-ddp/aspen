package org.aspen_ddp.aspen.amoebafs.error

import org.aspen_ddp.aspen.amoebafs.DirectoryPointer

/** A rename or move of a directory would replace `name` of `pointer`, which is not a directory (POSIX ENOTDIR) */
case class NotADirectory(pointer: DirectoryPointer, name: String) extends FSError {
  override def toString: String = s"$name in directory ${pointer.uuid} is not a directory"
}
