package org.aspen_ddp.aspen.server

import org.aspen_ddp.aspen.IntegrationTestSuite
import org.aspen_ddp.aspen.common.metadata.{HostId, StorageDeviceSetId}

import java.nio.charset.StandardCharsets
import java.nio.file.attribute.PosixFilePermissions
import java.nio.file.{Files, Path, Paths}
import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

class StorageDeviceManagerSuite extends IntegrationTestSuite:

  private val systemId = UUID.fromString("22222222-2222-2222-2222-222222222222")
  private val bootstrapSet = StorageDeviceSetId.BootstrapStorageDeviceSetId

  /** A HostConfig for the TestNetwork's bootstrap host. Only hostId and aspenSystemId
   *  are consulted by createStorageDevice; the rest is filler. */
  private def hostConfig(sysId: UUID = systemId) = HostConfig(
    HostId.BootstrapHostId,
    sysId,
    "testhost",
    "localhost",
    1234,
    1235,
    1236,
    HostConfig.SimpleCRL(numStreams = 3, fileSizeMb = 300))

  /** Creates `<tmp>/host/storage-devices` and returns the host directory. */
  private def newHostDir(): Path =
    val base = Files.createTempDirectory("aspen-sdm-test")
    val hostDir = base.resolve("host")
    Files.createDirectories(hostDir.resolve(StorageDeviceManager.StorageDevicesDirName))
    hostDir

  atest("deviceDirectory resolves <host-dir>/storage-devices/<name>"):
    Future.successful(
      StorageDeviceManager.deviceDirectory(Paths.get("/srv/aspen/node_a"), "dev2") should be(
        Paths.get("/srv/aspen/node_a/storage-devices/dev2")))

  atest("rejects a host config from a different Aspen system"):
    given ExecutionContext = executionContext
    val hostDir = newHostDir()
    val dir = StorageDeviceManager.deviceDirectory(hostDir, "dev2")
    Files.createDirectories(dir)
    recoverToSucceededIf[StorageDeviceManager.WrongAspenSystem](
      StorageDeviceManager.createStorageDevice(
        client, hostConfig(UUID.randomUUID()), hostDir, dir, bootstrapSet, systemId))

  atest("rejects a device directory that is a grandchild of storage-devices"):
    given ExecutionContext = executionContext
    val hostDir = newHostDir()
    val dir = StorageDeviceManager.deviceDirectory(hostDir, "foo/bar")
    Files.createDirectories(dir)
    recoverToSucceededIf[StorageDeviceManager.DeviceDirectoryNotUnderHost](
      StorageDeviceManager.createStorageDevice(
        client, hostConfig(), hostDir, dir, bootstrapSet, systemId))

  atest("rejects a device name that escapes upward"):
    given ExecutionContext = executionContext
    val hostDir = newHostDir()
    val dir = StorageDeviceManager.deviceDirectory(hostDir, "..")
    recoverToSucceededIf[StorageDeviceManager.DeviceDirectoryNotUnderHost](
      StorageDeviceManager.createStorageDevice(
        client, hostConfig(), hostDir, dir, bootstrapSet, systemId))

  atest("rejects an absolute device name"):
    given ExecutionContext = executionContext
    val hostDir = newHostDir()
    val elsewhere = Files.createTempDirectory("aspen-elsewhere")
    val dir = StorageDeviceManager.deviceDirectory(hostDir, elsewhere.toString)
    recoverToSucceededIf[StorageDeviceManager.DeviceDirectoryNotUnderHost](
      StorageDeviceManager.createStorageDevice(
        client, hostConfig(), hostDir, dir, bootstrapSet, systemId))

  atest("rejects a device directory that does not exist"):
    given ExecutionContext = executionContext
    val hostDir = newHostDir()
    val dir = StorageDeviceManager.deviceDirectory(hostDir, "nope")
    recoverToSucceededIf[StorageDeviceManager.DeviceDirectoryNotFound](
      StorageDeviceManager.createStorageDevice(
        client, hostConfig(), hostDir, dir, bootstrapSet, systemId))

  atest("rejects a device directory that already holds a device config"):
    given ExecutionContext = executionContext
    val hostDir = newHostDir()
    val dir = StorageDeviceManager.deviceDirectory(hostDir, "dev2")
    Files.createDirectories(dir)
    Files.write(dir.resolve(StorageDeviceConfig.configFilename),
                "storage-device-id: x\n".getBytes(StandardCharsets.UTF_8))
    recoverToSucceededIf[StorageDeviceManager.DeviceAlreadyConfigured](
      StorageDeviceManager.createStorageDevice(
        client, hostConfig(), hostDir, dir, bootstrapSet, systemId))

  atest("creates the device and writes its config into a plain directory"):
    given ExecutionContext = executionContext
    val hostDir = newHostDir()
    val dir = StorageDeviceManager.deviceDirectory(hostDir, "dev2")
    Files.createDirectories(dir)
    for
      deviceId <- StorageDeviceManager.createStorageDevice(
                    client, hostConfig(), hostDir, dir, bootstrapSet, systemId)
      _        <- waitForTransactionsToComplete()
      ds       <- client.getStorageDeviceState(deviceId)
    yield
      val written = dir.resolve(StorageDeviceConfig.configFilename)
      Files.isRegularFile(written) should be(true)
      val cfg = StorageDeviceConfig.loadHostConfig(written.toFile)
      cfg.storageDeviceId should be(deviceId)
      cfg.aspenSystemId should be(systemId)
      ds.hostId should be(HostId.BootstrapHostId)
      ds.storageDeviceSet should be(bootstrapSet)

  atest("accepts a symlink under storage-devices and writes the config through it"):
    given ExecutionContext = executionContext
    val hostDir = newHostDir()
    val elsewhere = Files.createTempDirectory("aspen-real-device")
    val link = hostDir.resolve(StorageDeviceManager.StorageDevicesDirName).resolve("dev2")
    Files.createSymbolicLink(link, elsewhere)
    for
      deviceId <- StorageDeviceManager.createStorageDevice(
                    client, hostConfig(), hostDir, link, bootstrapSet, systemId)
      _        <- waitForTransactionsToComplete()
    yield
      val written = elsewhere.resolve(StorageDeviceConfig.configFilename)
      Files.isRegularFile(written) should be(true)
      StorageDeviceConfig.loadHostConfig(written.toFile).storageDeviceId should be(deviceId)

  atest("reports ConfigWriteFailed, naming the device, when the write fails"):
    given ExecutionContext = executionContext
    val hostDir = newHostDir()
    val dir = StorageDeviceManager.deviceDirectory(hostDir, "dev2")
    Files.createDirectories(dir)
    Files.setPosixFilePermissions(dir, PosixFilePermissions.fromString("r-xr-xr-x"))
    // A process running as root can still write to a read-only directory.
    assume(!Files.isWritable(dir))
    StorageDeviceManager
      .createStorageDevice(client, hostConfig(), hostDir, dir, bootstrapSet, systemId)
      .failed
      .map: err =>
        err shouldBe a[StorageDeviceManager.ConfigWriteFailed]
        val cwf = err.asInstanceOf[StorageDeviceManager.ConfigWriteFailed]
        err.getMessage should include(cwf.storageDeviceId.uuid.toString)
