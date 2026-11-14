package org.aspen_ddp.aspen.server.crl

import org.aspen_ddp.aspen.server.StorageDeviceConfig

/** Opens the CRL of one storage device. Every store on a device saves to, recovers from and deletes from its
 *  device's CRL. */
trait CrashRecoveryLogFactory {
  /** Opens the CRL of the device at `devicePath`, recovering its existing files. Throws if they cannot be
   *  recovered (CrlCorrupt when the media returned other than what was written), and with CrlLocked if another writer, in this process or another, has them open. */
  def openCRL(devicePath: os.Path, config: StorageDeviceConfig): CrashRecoveryLog
}

/** Thrown by openCRL when another writer, in this process or another, has the device's CRL open. The CRL is in use, not
 *  lost: the opener must leave its files alone and may retry later */
class CrlLocked(crlDir: os.Path)
  extends Exception(s"The CRL in $crlDir is already open, by this process or another. Refusing a second writer")

/** Thrown by openCRL when the CRL's files hold other than what was written: an entry recovery needs fails its checksums,
 *  or an intact entry newer than the last one recovery found exists. The CRL cannot be recovered, as with any other
 *  failure to recover it, but the cause is named apart so corruption can be counted (Todo 3.1) */
class CrlCorrupt(reason: String) extends Exception(s"CRL is corrupt: $reason")
