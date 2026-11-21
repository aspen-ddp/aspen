package org.aspen_ddp.aspen.server.store.backend.fsif

import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.StandardOpenOption

import org.aspen_ddp.aspen.server.{DurableFiles, FileStat}

/** A positional-I/O file. Reads see everything written so far; only `force` makes it durable. */
trait FsifFile:
  def size: Long
  /** Returns fewer bytes than requested if the read reaches EOF */
  def read(position: Long, length: Int): Array[Byte]
  /** Extends the file as needed */
  def write(position: Long, bytes: Array[Byte]): Unit
  def truncate(size: Long): Unit
  /** Data and size durable (FileChannel.force(false)) */
  def force(): Unit
  def close(): Unit

trait FsifFiles:
  /** Creates the file if missing */
  def open(name: String): FsifFile
  /** The file's current size (including unforced writes), or 0 if it is known not to exist. Opens nothing. A stat
   *  that fails throws its IOException, never reads as an absent or empty file */
  def size(name: String): Long
  /** False only for a file known absent: a stat that fails throws its IOException (server.FileStat), which the host's
   *  load retries, so recovery never replays without a journal or index that is there (fsif/DESIGN.md 4.1) */
  def exists(name: String): Boolean

object FsifIo:
  /** Backend.isOutOfSpace */
  def isOutOfSpace(t: Throwable): Boolean = org.aspen_ddp.aspen.server.store.backend.Backend.isOutOfSpace(t)

final class NioFsifFiles(dir: os.Path) extends FsifFiles:

  def open(name: String): FsifFile =
    val path = dir / name
    val channel = FileChannel.open(path.toNIO,
      StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE)
    // Always, not only when this call creates the file: a crash between an earlier open's create and its sync
    // could otherwise leave the directory entry non-durable for good. open is rare, so this is cheap.
    try DurableFiles.syncDirectory(dir)
    catch
      case t: Throwable =>
        channel.close()
        throw t
    new NioFsifFile(channel)

  def exists(name: String): Boolean = FileStat.exists(dir / name)

  def size(name: String): Long = FileStat.stat(dir / name).map(_.size).getOrElse(0L)

private final class NioFsifFile(channel: FileChannel) extends FsifFile:

  def size: Long = channel.size()

  def read(position: Long, length: Int): Array[Byte] =
    val buf = ByteBuffer.allocate(length)
    var pos = position
    var eof = false
    while buf.hasRemaining && !eof do
      val n = channel.read(buf, pos)
      if n < 0 then
        eof = true
      else
        pos += n
    if buf.hasRemaining then
      java.util.Arrays.copyOf(buf.array(), buf.position())
    else
      buf.array()

  def write(position: Long, bytes: Array[Byte]): Unit =
    val buf = ByteBuffer.wrap(bytes)
    var pos = position
    while buf.hasRemaining do
      pos += channel.write(buf, pos)

  def truncate(size: Long): Unit =
    if size < channel.size() then
      channel.truncate(size)
    else if size > channel.size() then
      // FileChannel.truncate never grows a file; extend by writing a zero byte at the end
      channel.write(ByteBuffer.wrap(Array[Byte](0)), size - 1)

  def force(): Unit = channel.force(false)

  def close(): Unit = channel.close()
