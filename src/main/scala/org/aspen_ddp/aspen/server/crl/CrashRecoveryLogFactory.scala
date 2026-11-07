package org.aspen_ddp.aspen.server.crl

import org.aspen_ddp.aspen.server.StorageDeviceConfig

/** Opens the CRL of one storage device. Every store on a device saves to, recovers from and deletes from its
 *  device's CRL. */
trait CrashRecoveryLogFactory {
  /** Opens the CRL of the device at `devicePath`, recovering its existing files. Throws if they cannot be
   *  recovered. */
  def openCRL(devicePath: os.Path, config: StorageDeviceConfig): CrashRecoveryLog
}
