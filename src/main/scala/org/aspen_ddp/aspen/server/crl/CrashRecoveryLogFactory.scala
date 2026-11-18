package org.aspen_ddp.aspen.server.crl

import org.aspen_ddp.aspen.server.StorageDeviceConfig

/** Opens the CRL of one storage device. Every store on a device saves to, recovers from and deletes from its
 *  device's CRL. */
trait CrashRecoveryLogFactory {
  /** Opens the CRL of the device at `devicePath`, recovering its existing files. Throws if they cannot be
   *  recovered (CrlCorrupt when the media returned other than what was written), with CrlLocked if another writer, in
   *  this process or another, has them open, and with CrlSizingMismatch if its files do not match the sizing the CRL
   *  recorded when it was created, and with CrlConfigInvalid if a CRL it would create cannot be sized by `config`.
   *
   *  @param holdsStores the device holds stores. Stores exist only on a device whose CRL has been opened, so its CRL
   *                     must exist then: one with no files holding entries (none ever created, or all removed) throws
   *                     CrlMissing instead of being created empty. */
  def openCRL(devicePath: os.Path, config: StorageDeviceConfig, holdsStores: Boolean): CrashRecoveryLog
}

/** Thrown by openCRL, asked to open the CRL of a device that holds stores, when the CRL has no files that could hold
 *  entries: they were removed (by hand, a file system repair, a partial restore) or never created. The stores'
 *  transaction state is gone, so the CRL is lost. Nothing in the CRL directory is written then. */
class CrlMissing(crlDir: os.Path)
  extends Exception(s"The CRL in $crlDir has no stream files, but its device holds stores")

/** Thrown by openCRL when another writer, in this process or another, has the device's CRL open. The CRL is in use, not
 *  lost: the opener must leave its files alone and may retry later */
class CrlLocked(crlDir: os.Path)
  extends Exception(s"The CRL in $crlDir is already open, by this process or another. Refusing a second writer")

/** The failure of every Future a CRL returns once it has failed (CrashRecoveryLog.failed): its thread or its writer
 *  died of `cause`, and nothing more is written. The files hold what was written before, so a restart recovers it */
class CrlFailed(val cause: Throwable) extends Exception(s"The CRL has failed: $cause", cause)

/** The failure of every Future a CRL returns once it is shut down (CrashRecoveryLog.shutdown) and has not answered:
 *  whatever its unwritten entries held is discarded unwritten, as at a crash, and nothing more is written */
class CrlClosed extends Exception("The CRL is shut down")

/** Thrown by openCRL when the CRL's files hold other than what was written: an entry recovery needs fails its checksums,
 *  or an intact entry newer than the last one recovery found exists. The CRL cannot be recovered, as with any other
 *  failure to recover it, but the cause is named apart so corruption can be counted */
class CrlCorrupt(reason: String) extends Exception(s"CRL is corrupt: $reason")

/** Thrown by openCRL when the CRL's stream files do not match the sizing the CRL recorded when it was created, or that
 *  record is missing or unreadable while stream files exist. Recovering with another sizing would ignore files holding
 *  live entries, so the CRL is not opened. Configuration cannot cause it (an existing CRL keeps its recorded sizing
 *  whatever the device config says), so it means a hand edit or damage of the CRL directory. It is never treated as a
 *  lost CRL: the device is refused until an operator repairs the directory (systemic-loss-protection.md S6) */
class CrlSizingMismatch(crlDir: os.Path, reason: String)
  extends Exception(s"CRL stream files in $crlDir do not match the sizing the CRL was created with: $reason")

/** Thrown by openCRL when the CRL directory holds no CRL yet (neither a sizing file nor stream files) and the device
 *  config cannot size a new one (CrlSizingDefaults.resolve refuses it). Nothing is created in the CRL directory. It is
 *  a refusal, never a lost CRL: the device is left unloaded, and the next scan after the config is fixed opens it.
 *  An existing CRL never throws it: it opens with the sizing it recorded whatever the config says */
class CrlConfigInvalid(crlDir: os.Path, cause: IllegalArgumentException)
  extends Exception(s"The device config cannot size a new CRL in $crlDir: ${cause.getMessage}", cause)
