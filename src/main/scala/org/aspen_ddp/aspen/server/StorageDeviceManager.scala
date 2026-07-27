package org.aspen_ddp.aspen.server

import org.aspen_ddp.aspen.client.AspenClient
import org.aspen_ddp.aspen.common.metadata.{StorageDeviceId, StorageDeviceSetId}

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

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

  /** Register `deviceDirectory` as a new storage device on `hostConfig`'s host and place it
   *  in the level-0 set `deviceSetId`, then write the device's config file into the
   *  directory so the host's StoreManager will discover it.
   *
   *  The directory must already exist. In production it is a mount point, or a symlink
   *  under `<host-directory>/storage-devices/` pointing at one; the config file has to land
   *  on the device itself, so this function never creates the directory.
   *
   *  Ordering: the transaction commits before the file is written, and the two cannot be
   *  made atomic. A crash in between leaves registered metadata with no on-disk device --
   *  inert, and recoverable by writing the file or removing the device. The reverse order
   *  would let a host load a device whose StorageDeviceState object does not exist.
   */
  def createStorageDevice(client: AspenClient,
                          hostConfig: HostConfig,
                          hostDirectory: Path,
                          deviceDirectory: Path,
                          deviceSetId: StorageDeviceSetId,
                          aspenSystemId: UUID)
                         (using ExecutionContext): Future[StorageDeviceId] =

    // Deliberately lexical: normalize but do not call toRealPath, so a symlink at
    // storage-devices/<name> pointing at a mount elsewhere still counts as contained.
    // Only direct children are accepted, because StoreManager scans only direct children.
    val expectedParent = hostDirectory.resolve(StorageDevicesDirName).toAbsolutePath.normalize
    val devDir = deviceDirectory.toAbsolutePath.normalize
    val configFile = devDir.resolve(StorageDeviceConfig.configFilename)

    if hostConfig.aspenSystemId != aspenSystemId then
      Future.failed(new WrongAspenSystem(aspenSystemId, hostConfig.aspenSystemId))

    else if devDir.getParent != expectedParent then
      Future.failed(new DeviceDirectoryNotUnderHost(devDir, expectedParent))

    else if !Files.isDirectory(devDir) then
      Future.failed(new DeviceDirectoryNotFound(devDir))

    else if Files.exists(configFile) then
      Future.failed(new DeviceAlreadyConfigured(devDir))

    else
      client.createStorageDevice(hostConfig.hostId, deviceSetId).map: deviceId =>
        val cfg = StorageDeviceConfig(deviceId, aspenSystemId)
        try
          Files.write(configFile, cfg.yamlConfig.getBytes(StandardCharsets.UTF_8))
        catch
          case t: Throwable => throw new ConfigWriteFailed(deviceId, configFile, t)
        deviceId
