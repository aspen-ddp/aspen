package org.aspen_ddp.aspen.server.transfer

import org.aspen_ddp.aspen.server.FileStat

/** The disk work of StoreTransferManager, as plain functions of paths. None runs on a caller's thread: a receive's
 *  set-up and a send's checks run on the transfer's own connection thread, and every removal of a received directory
 *  runs as a job on its device's lane (TransferLane). */
object StoreTransferDisk:
  /** A receive's directory, `dir`, emptied and made afresh */
  def prepareDirectory(dir: os.Path): Unit =
    os.remove.all(dir)
    os.makeDir.all(dir)

  /** Whether `dir` is a directory (FileStat: a stat that fails throws) */
  def isDirectory(dir: os.Path): Boolean = FileStat.isDir(dir)

  /** `dir` and everything under it removed */
  def removeAll(dir: os.Path): Unit = os.remove.all(dir)
