package org.aspen_ddp.aspen.server.store.backend.fsif

import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.StandardOpenOption

import org.aspen_ddp.aspen.server.DurableFiles

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
  def exists(name: String): Boolean

object FsifIo:
  /** True if "no space left" (ENOSPC) or a quota error (EDQUOT, "disk quota exceeded") appears, ignoring case, in
   *  the message of the throwable or of any cause. RebuildingStore.isOutOfSpace uses it too. */
  def isOutOfSpace(t: Throwable): Boolean =
    def matches(e: Throwable): Boolean =
      val msg = Option(e.getMessage).getOrElse("").toLowerCase
      msg.contains("no space left") || msg.contains("disk full") ||
        msg.contains("insufficient space") || msg.contains("injected out of space") ||
        msg.contains("disk quota exceeded") || msg.contains("edquot")
    Iterator.iterate(t)(_.getCause).takeWhile(_ != null).take(8).exists(matches)

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

  def exists(name: String): Boolean = os.exists(dir / name)

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
