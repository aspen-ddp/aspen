package org.aspen_ddp.aspen.amoebafs

import org.aspen_ddp.aspen.amoebafs.error.InvalidPointer

object FileType extends Enumeration {
  import FileMode._

  val File: FileType.Value            = Value("File")
  val Directory: FileType.Value       = Value("Directory")
  val Symlink: FileType.Value         = Value("Symlink")
  val UnixSocket: FileType.Value      = Value("UnixSocket")
  val CharacterDevice: FileType.Value = Value("CharaceterDevice")
  val BlockDevice: FileType.Value     = Value("BlockDevice")
  val FIFO: FileType.Value            = Value("FIFO")



  def fromMode(mode: Int): Value = mode & S_IFMT match {
    case S_IFSOCK => UnixSocket
    case S_IFLNK  => Symlink
    case S_IFREG  => File
    case S_IFBLK  => BlockDevice
    case S_IFDIR  => Directory
    case S_IFCHR  => CharacterDevice
    case S_IFFIFO => FIFO
    case _        => throw InvalidPointer((mode & S_IFMT).asInstanceOf[Byte])
  }

  def toMode(value: Value): Int = value match {
    case File            => S_IFREG
    case Directory       => S_IFDIR
    case Symlink         => S_IFLNK
    case UnixSocket      => S_IFSOCK
    case CharacterDevice => S_IFCHR
    case BlockDevice     => S_IFBLK
    case FIFO            => S_IFFIFO
  }

  def ensureModeFileType(mode: Int, fileType: FileType.Value): Int = mode & ~S_IFMT | toMode(fileType)

  def toByte(value: Value): Byte = {
    val i = value match {
      case File            => 0
      case Directory       => 1
      case Symlink         => 2
      case UnixSocket      => 3
      case CharacterDevice => 4
      case BlockDevice     => 5
      case FIFO            => 6
    }

    i.asInstanceOf[Byte]
  }

  def fromByte(b: Byte): Value = b match {
    case 0 => File
    case 1 => Directory
    case 2 => Symlink
    case 3 => UnixSocket
    case 4 => CharacterDevice
    case 5 => BlockDevice
    case 6 => FIFO
    case _ => throw InvalidPointer(b)
  }
}
