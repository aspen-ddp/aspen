package org.aspen_ddp.aspen.server.host

import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.server.DurableFiles
import org.aspen_ddp.aspen.server.crl.{CrashRecoveryLog, StoreRecoveryState}

/** A loaded store's disk work that its executor must not do. Every call blocks on the disk, so it runs as a
 *  `JobKind.SaveFile` job on the store's device's DeviceIo (`StoreRouter.crlLoaded`, `StoreLoader.handleClose`), never
 *  on the store's executor or the control thread. */
object StoreDisk:

  /** Removes `file` (crl_save.log) durably, forcing its directory even when the file is already absent: an earlier
   *  attempt may have unlinked it and failed to force. */
  def removeSaveFile(file: os.Path): Unit =
    os.remove(file, checkExists = false)
    DurableFiles.syncDirectory(file / os.up)

  /** Writes `state` to `file` atomically and durably (CrashRecoveryLog.saveStoreState) */
  def saveState(storeId: StoreId, file: os.Path, state: StoreRecoveryState): Unit =
    CrashRecoveryLog.saveStoreState(storeId, state, file)
