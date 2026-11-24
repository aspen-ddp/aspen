package org.aspen_ddp.aspen.server.transfer

/** The disk work of StoreTransferManager, as plain functions of paths. None runs on a caller's thread: a receive's
 *  set-up and a send's removal of its archiver's temporary directory run on the transfer's own connection thread, and
 *  every removal of a received directory runs as a job on its device's lane (TransferLane). */
object StoreTransferDisk:
  /** A receive's directory, `dir`, emptied and made afresh */
  def prepareDirectory(dir: os.Path): Unit =
    os.remove.all(dir)
    os.makeDir.all(dir)

  /** `dir` and everything under it removed */
  def removeAll(dir: os.Path): Unit = os.remove.all(dir)
