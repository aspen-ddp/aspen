package org.aspen_ddp.aspen.server.crl.simple

import org.aspen_ddp.aspen.server.FileStat
import org.aspen_ddp.aspen.server.crl.CrlLocked

import java.nio.channels.{FileChannel, FileLock}
import java.nio.file.{Files, StandardOpenOption}
import java.nio.file.attribute.BasicFileAttributes

/** The exclusive lock a SimpleCRL holds on `<crl dir>/lock` while it is open, so no second writer, in this process or
 *  another, interleaves its entries with ours (crash-recovery-log 5.1). Acquired before the stream files are read and
 *  released once the writer has closed them.
 *
 *  The OS lock is a POSIX record lock, which belongs to the process, and Linux drops every such lock a process holds on
 *  a file when the process closes any descriptor of it. A second opener in this process must therefore never open the
 *  lock file: `held` (keyed by the file's identity, so another path to the same file is caught too) refuses it first,
 *  and every acquire and release runs under `held`'s monitor, so no create or open of the file overlaps a lock held
 *  here. The lock fences only openers that take it: two hosts mounting one shared device need real fencing
 *  (crash-recovery-log AS3). */
final class CrlLock private (key: AnyRef, channel: FileChannel, lock: FileLock):
  private var released = false

  /** Idempotent */
  def release(): Unit = CrlLock.held.synchronized:
    if !released then
      released = true
      try lock.release()
      finally
        try channel.close()
        finally CrlLock.held -= key

object CrlLock:
  val LockFilename = "lock"

  private val held = scala.collection.mutable.Set[AnyRef]()

  /** @throws CrlLocked if the CRL in `dir` is open anywhere else */
  def acquire(dir: os.Path): CrlLock = held.synchronized:
    os.makeDir.all(dir)
    val path = dir / LockFilename
    if !FileStat.exists(path) then
      os.write(path, "")
    val attrs = Files.readAttributes(path.toNIO, classOf[BasicFileAttributes])
    val key: AnyRef = Option(attrs.fileKey()).getOrElse(path.toNIO.toRealPath())
    if held.contains(key) then
      throw CrlLocked(dir)
    // FileChannel rather than os-lib: os-lib has no file locks
    val channel = FileChannel.open(path.toNIO, StandardOpenOption.WRITE)
    // No lock on this file is held in this process (`held`), so closing the channel here drops none
    val lock =
      try channel.tryLock()
      catch case t: Throwable =>
        channel.close()
        throw t
    if lock == null then
      channel.close()
      throw CrlLocked(dir)
    held += key
    new CrlLock(key, channel, lock)
