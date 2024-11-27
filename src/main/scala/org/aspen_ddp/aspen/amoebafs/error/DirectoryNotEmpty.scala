package org.aspen_ddp.aspen.amoebafs.error

import org.aspen_ddp.aspen.amoebafs.DirectoryPointer

case class DirectoryNotEmpty(pointer: DirectoryPointer) extends FSError
