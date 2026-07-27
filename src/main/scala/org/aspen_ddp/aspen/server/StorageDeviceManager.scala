package org.aspen_ddp.aspen.server

import org.aspen_ddp.aspen.common.metadata.StorageDeviceId

import java.nio.file.Path
import java.util.UUID

/** Creation and on-disk setup of storage devices.
 *
 *  Stateless by design: StoreManager will call the same functions when device creation is
 *  automated, so nothing here may depend on running inside the CLI.
 */
object StorageDeviceManager:

  /** Name of the directory under a host's root directory that holds its storage devices.
   *  StoreManager scans the direct children of this directory at startup. */
  val StorageDevicesDirName = "storage-devices"

  class WrongAspenSystem(val expected: UUID, val found: UUID)
    extends Exception(s"Host config belongs to Aspen system $found, not $expected")

  class DeviceDirectoryNotUnderHost(val directory: Path, val expectedParent: Path)
    extends Exception(s"$directory is not a direct child of $expectedParent")

  class DeviceDirectoryNotFound(val directory: Path)
    extends Exception(s"Storage device directory does not exist: $directory")

  class DeviceAlreadyConfigured(val directory: Path)
    extends Exception(s"$directory already contains ${StorageDeviceConfig.configFilename}")

  /** Raised when the device metadata committed but the config file could not be written.
   *  Carries the device id because it is the only handle an operator has for recovery. */
  class ConfigWriteFailed(val storageDeviceId: StorageDeviceId,
                          val configFile: Path,
                          cause: Throwable)
    extends Exception(
      s"Storage device ${storageDeviceId.uuid} was registered but writing $configFile failed: ${cause.getMessage}",
      cause)

  /** Resolve the conventional location of a named storage device directory:
   *  `<host-directory>/storage-devices/<device-name>`.
   *
   *  `deviceName` is deliberately not sanitised here. A name containing a separator, a
   *  `..`, or an absolute path resolves outside the expected parent and is rejected by
   *  createStorageDevice's containment check. */
  def deviceDirectory(hostDirectory: Path, deviceName: String): Path =
    hostDirectory.resolve(StorageDevicesDirName).resolve(deviceName)
