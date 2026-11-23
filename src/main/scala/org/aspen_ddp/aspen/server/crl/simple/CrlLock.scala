package org.aspen_ddp.aspen.server.crl.simple

import org.aspen_ddp.aspen.server.FileStat
import org.aspen_ddp.aspen.server.crl.CrlLocked

import java.nio.channels.{FileChannel, FileLock}
import java.nio.file.{FileAlreadyExistsException, Files, StandardOpenOption}
import java.nio.file.attribute.BasicFileAttributes
import java.util.concurrent.ConcurrentHashMap

/** The exclusive lock a SimpleCRL holds on `<crl dir>/lock` while it is open, so no second writer, in this process or
 *  another, interleaves its entries with ours (crash-recovery-log 5.1). Acquired before the stream files are read and
 *  released once the writer has closed them.
 *
 *  The OS lock is a POSIX record lock, which belongs to the process, and Linux drops every such lock a process holds on
 *  a file when the process closes any descriptor of it. A second opener in this process must therefore never open the
 *  lock file: `held` (keyed by the file's identity, so another path to the same file is caught too) refuses it first.
 *  Each lock file has its own monitor (its `Slot`), and every open, lock, release and close of the file's descriptor
 *  runs under it, so none overlaps a lock held here; a device whose I/O hangs inside its acquire delays only openers of
 *  its own file, never another device's (device I/O isolation). Creating the file when absent opens no descriptor of
 *  an existing one (create-new). The lock fences only openers that take it: two hosts mounting one shared device need
 *  real fencing (crash-recovery-log AS3). */
final class CrlLock private (slot: CrlLock.Slot, channel: FileChannel, lock: FileLock):
  private var released = false

  /** Idempotent */
  def release(): Unit = slot.synchronized:
    if !released then
      released = true
      try lock.release()
      finally
        try channel.close()
        finally slot.held = false

object CrlLock:
  val LockFilename = "lock"

  /** One lock file's monitor, and whether this process holds its lock. Kept once made: a slot removed while another
   *  opener waited on it would let a third take a fresh one and open the file beside a held lock */
  private[simple] final class Slot:
    var held = false

  private val held = new ConcurrentHashMap[AnyRef, Slot]()

  /** Testing hook: run with the CRL directory inside the lock file's monitor, just before its tryLock */
  @volatile private[crl] var beforeTryLock: os.Path => Unit = _ => ()

  /** @throws CrlLocked if the CRL in `dir` is open anywhere else */
  def acquire(dir: os.Path): CrlLock =
    os.makeDir.all(dir)
    val path = dir / LockFilename
    if !FileStat.exists(path) then
      // Created by another opener meanwhile: that is the file to lock
      try os.write(path, "")
      catch case _: FileAlreadyExistsException => ()
    val attrs = Files.readAttributes(path.toNIO, classOf[BasicFileAttributes])
    val key: AnyRef = Option(attrs.fileKey()).getOrElse(path.toNIO.toRealPath())
    val slot = held.computeIfAbsent(key, _ => new Slot)
    slot.synchronized:
      if slot.held then
        throw CrlLocked(dir)
      beforeTryLock(dir)
      // FileChannel rather than os-lib: os-lib has no file locks
      val channel = FileChannel.open(path.toNIO, StandardOpenOption.WRITE)
      // No lock on this file is held in this process (`slot.held`), so closing the channel here drops none
      val lock =
        try channel.tryLock()
        catch case t: Throwable =>
          channel.close()
          throw t
      if lock == null then
        channel.close()
        throw CrlLocked(dir)
      slot.held = true
      new CrlLock(slot, channel, lock)
