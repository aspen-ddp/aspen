package org.aspen_ddp.aspen.server

import java.io.{BufferedOutputStream, OutputStream}
import java.nio.channels.{Channels, FileChannel}
import java.nio.file.StandardOpenOption
import java.util.concurrent.atomic.AtomicBoolean
import scribe.Logging

/** Small files that must survive a power cut or a pulled disk the moment their write returns:
 *  the device markers, the per-store transfer-out marker and CRL save file that a shutdown
 *  leaves behind, and anything else a "safe to remove" decision depends on.
 *
 *  os-lib writes go through the page cache and return before anything reaches the disk, so a
 *  disk pulled right after a shutdown reports Shutdown can come back without the file, or with
 *  a zero-length one. Every write here forces the file's data, then forces the directory that
 *  names it, so both the bytes and the directory entry are on stable storage. java.nio is used
 *  only because os-lib has no fsync (see CLAUDE.md); paths cross the boundary with toNIO.
 *
 *  The durability claim holds on Linux only, the supported host platform. See `syncDirectory`.
 */
object DurableFiles extends Logging:

  /** Replaces `file` with `content`. Atomic: the bytes go to a temp file in the same directory,
   *  which is forced and renamed over `file`, and the directory is then forced. A crash leaves
   *  either the old file or the new one, never a partial one; at worst a stray temp file. A write
   *  that fails before the rename removes its temp file before rethrowing. */
  def write(file: os.Path, content: Array[Byte]): Unit =
    writeWith(file): out =>
      out.write(content)

  def write(file: os.Path, content: String): Unit =
    write(file, content.getBytes(java.nio.charset.StandardCharsets.UTF_8))

  /** As `write`, with the content produced by `fill` writing to a buffered stream. */
  def writeWith(file: os.Path)(fill: OutputStream => Unit): Unit =
    val tmp = file / os.up / s".${file.last}.tmp"
    try
      val channel = FileChannel.open(tmp.toNIO,
        StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)
      try
        // Not closed: closing the stream would close the channel before it is forced.
        val out = new BufferedOutputStream(Channels.newOutputStream(channel))
        fill(out)
        out.flush()
        channel.force(true)
      finally
        channel.close()
      os.move(tmp, file, replaceExisting = true, atomicMove = true)
    catch
      case e: Throwable =>
        // Best effort: a failed removal must not hide the original error.
        try os.remove(tmp)
        catch case r: Throwable => e.addSuppressed(r)
        throw e
    syncDirectory(file / os.up)

  /** Removes `file` if present, then forces its directory so the removal is durable too. */
  def remove(file: os.Path): Unit =
    if os.exists(file) then
      os.remove(file)
      syncDirectory(file / os.up)

  /** Forces a directory's entries to stable storage. Opening a directory for READ and forcing it
   *  is how Linux exposes fsync on a directory.
   *
   *  Elsewhere it degrades rather than fails, with a warning logged once. Windows cannot open a
   *  directory this way at all, so the call is a no-op there. macOS accepts it but issues a plain
   *  fsync rather than F_FULLFSYNC, so the entries may still sit in the drive's cache. */
  def syncDirectory(dir: os.Path): Unit =
    if isWindows then
      warnOnce("directory fsync is unsupported on Windows; renames and removals are not durable")
    else
      if isMacOS then
        warnOnce("directory fsync on macOS does not issue F_FULLFSYNC; renames and removals may not be durable")
      val channel = FileChannel.open(dir.toNIO, StandardOpenOption.READ)
      try channel.force(true)
      finally channel.close()

  private val osName = System.getProperty("os.name", "").toLowerCase
  private val isWindows = osName.startsWith("windows")
  private val isMacOS = osName.startsWith("mac")
  private val warned = new AtomicBoolean(false)

  private def warnOnce(message: String): Unit =
    if warned.compareAndSet(false, true) then
      logger.warn(s"DurableFiles: $message. Linux is the supported host platform.")
