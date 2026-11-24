package org.aspen_ddp.aspen.server.store.backend.fsif

import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.StandardOpenOption
import java.util.UUID

import org.aspen_ddp.aspen.server.{DurableFiles, FileStat, StorageDeviceConfig}

/** The journal's configuration cannot give a valid sizing */
class JournalConfigInvalid(msg: String) extends Exception(msg)

/** The journal directory's contents do not match its sizing file, or the sizing file is unreadable */
class JournalSizingMismatch(msg: String) extends java.io.IOException(msg)

/** The journal directory is open elsewhere */
class JournalLocked(msg: String) extends Exception(msg)

/** The device journal's directory: creation, sizing and validation. The ring's files are `DeviceJournalScan.fileName`.
  * Locking is `crl.simple.CrlLock.acquire(dir)`. */
object DeviceJournalFiles:
  val DirName = "fsif-journal"
  val CreatingName = "fsif-journal.creating"
  val SizingName = "sizing"
  val DefaultFiles = 4
  val DefaultFileSizeMb = 256
  private val Mib = 1L << 20

  def lostName(id: UUID): String = s"fsif-journal.lost-$id"

  def sizingFor(cfg: StorageDeviceConfig): JournalSizing =
    val files = cfg.fsifJournalFiles.getOrElse(DefaultFiles)
    val mb = cfg.fsifJournalFileSizeMb.getOrElse(DefaultFileSizeMb)
    if files < 3 then
      throw new JournalConfigInvalid(s"fsif-journal-files must be at least 3, not $files")
    if mb < 1 || mb > 1024 then
      throw new JournalConfigInvalid(s"fsif-journal-file-size-mb must be from 1 to 1024, not $mb")
    JournalSizing(UUID.randomUUID(), files, mb * Mib)

  /** For an empty scratch directory. Zero-fills with real writes, never fallocate: a sparse or unallocated file would
    * make later writes allocate and defeat the ring's preallocated-latency purpose. */
  def create(dir: os.Path, sizing: JournalSizing): Unit =
    os.makeDir.all(dir)
    DurableFiles.write(dir / SizingName, DeviceJournalFormat.encodeSizing(sizing))
    val chunk = ByteBuffer.allocate(Mib.toInt min sizing.fileSize.toInt)
    for n <- 0 until sizing.files do
      val ch = FileChannel.open((dir / DeviceJournalScan.fileName(n)).toNIO,
        StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)
      try
        var pos = 0L
        while pos < sizing.fileSize do
          val len = (sizing.fileSize - pos).min(chunk.capacity.toLong).toInt
          chunk.clear().limit(len)
          while chunk.hasRemaining do
            pos += ch.write(chunk, pos)
        ch.force(true)
      finally
        ch.close()
    DurableFiles.syncDirectory(dir)

  def readSizing(dir: os.Path): JournalSizing =
    val sizing =
      try DeviceJournalFormat.decodeSizing(os.read.bytes(dir / SizingName))
      catch case e: java.io.IOException => throw new JournalSizingMismatch(s"cannot read $dir/$SizingName: $e")
    val s = sizing.getOrElse(throw new JournalSizingMismatch(s"$dir/$SizingName is unreadable or corrupt"))
    val expected = (0 until s.files).map(DeviceJournalScan.fileName).toSet
    val present = os.list(dir).map(_.last).filter(_.endsWith(".fj")).toSet
    if present != expected then
      throw new JournalSizingMismatch(s"$dir holds journal files ${present.toSeq.sorted}, expected ${expected.toSeq.sorted}")
    expected.foreach: name =>
      val size = os.size(dir / name)
      if size != s.fileSize then
        throw new JournalSizingMismatch(s"$dir/$name is $size bytes, expected ${s.fileSize}")
    s

  def exists(dir: os.Path): Boolean = FileStat.exists(dir)
