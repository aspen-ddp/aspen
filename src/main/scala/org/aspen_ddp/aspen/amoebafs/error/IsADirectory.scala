package org.aspen_ddp.aspen.amoebafs.error

import org.aspen_ddp.aspen.amoebafs.DirectoryPointer

/** A rename or move of a non-directory would replace `name` of `pointer`, which is a directory (POSIX EISDIR) */
case class IsADirectory(pointer: DirectoryPointer, name: String) extends FSError {
  override def toString: String = s"$name in directory ${pointer.uuid} is a directory"
}
