package org.aspen_ddp.aspen.client

import org.aspen_ddp.aspen.IntegrationTestSuite
import org.aspen_ddp.aspen.common.ida.Replication
import org.aspen_ddp.aspen.common.metadata.{HostId, StorageDeviceId, StorageDeviceSetId}
import org.aspen_ddp.aspen.common.network.CheckStorageDevice
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.server.store.backend.RocksDBConfig
import org.aspen_ddp.aspen.server.{HostConfig, StorageDeviceManager}

import java.nio.file.{Files, Path}
import java.util.UUID
import scala.collection.mutable
import scala.concurrent.ExecutionContext

/** Both of these are regression guards rather than demonstrations of the bug they exist for.
 *
 *  The bug is a race: a nudge sent from a detached tx.result callback may not have been sent
 *  when the future the caller awaits completes, so a CLI command that exits at that moment
 *  loses it. Observing the race needs a genuinely concurrent clientContext, which the CLI has
 *  and TestNetwork deliberately does not -- AsyncFunSuite's ExecutionContext is serial, so the
 *  detached callback is always queued ahead of the assertions below.
 *
 *  What these tests do catch is the sends drifting back out of the chain the returned future
 *  waits on, which is the regression that would silently reintroduce the race.
 */
class DeviceCheckNudgeSuite extends IntegrationTestSuite:

  private val systemId = UUID.fromString("33333333-3333-3333-3333-333333333333")
  private val bootstrapSet = StorageDeviceSetId.BootstrapStorageDeviceSetId

  /** Temp trees created by the current test, removed by teardown. */
  private val tempRoots = mutable.ListBuffer[os.Path]()

  override def subFixtureTeardown(): Unit =
    tempRoots.foreach: p =>
      try os.remove.all(p)
      catch case _: Throwable => ()
    tempRoots.clear()

  /** A HostConfig for the TestNetwork's bootstrap host. Only hostId and aspenSystemId are
   *  consulted by createStorageDevice; the rest is filler. */
  private def hostConfig() = HostConfig(
    HostId.BootstrapHostId,
    systemId,
    "testhost",
    "localhost",
    1234,
    1235,
    1236,
    HostConfig.SimpleCRL(numStreams = 3, fileSizeMb = 300))

  /** Creates `<tmp>/host/storage-devices` and returns the host directory. */
  private def newHostDir(): Path =
    val base = Files.createTempDirectory("aspen-nudge-test")
    tempRoots += os.Path(base)
    val hostDir = base.resolve("host")
    Files.createDirectories(hostDir.resolve(StorageDeviceManager.StorageDevicesDirName))
    hostDir

  private def takeNudges(): List[CheckStorageDevice] =
    net.takeCapturedHostMessages().collect:
      case m: CheckStorageDevice => m

  atest("createNewStoragePool has sent its device-check nudges when its future completes"):
    given ExecutionContext = executionContext
    // Discard anything bootstrap left behind so the assertions see only this pool's nudges.
    takeNudges()
    for
      poolId <- client.createNewStoragePool(
                  "nudge-pool",
                  Replication(1, 1),
                  None,
                  RocksDBConfig(),
                  bootstrapSet,
                  0L)
      // Captured with no intervening await: anything not yet sent at this point is exactly
      // what a CLI command exiting here would lose.
      nudges =  takeNudges()
      devState <- client.getStorageDeviceState(StorageDeviceId.BootstrapStorageDeviceId)
    yield
      nudges.map(_.deviceId) should be(List(StorageDeviceId.BootstrapStorageDeviceId))
      nudges.map(_.toHost) should be(List(devState.hostId))
      nudges.map(_.fromClient) should be(List(client.clientId))

  atest("transferStore has sent its device-check nudge when its future completes"):
    given ExecutionContext = executionContext
    // TestNetwork ships with a single storage device, so the destination has to be made.
    val hostDir = newHostDir()
    val dir = StorageDeviceManager.deviceDirectory(hostDir, "dev2")
    Files.createDirectories(dir)
    val storeId = StoreId(PoolId.BootstrapPoolId, 0.toByte)
    for
      destId <- StorageDeviceManager.createStorageDevice(
                  client, hostConfig(), hostDir, dir, bootstrapSet, systemId)
      _ <- net.waitForTransactionsToComplete()
      // Discard anything device creation sent, so the assertions see only the transfer's nudge.
      _ =  takeNudges()
      _ <- client.transferStore(storeId, destId)
      nudges =  takeNudges()
      destState <- client.getStorageDeviceState(destId)
    yield
      nudges.map(_.deviceId) should be(List(destId))
      nudges.map(_.toHost) should be(List(destState.hostId))
      nudges.map(_.fromClient) should be(List(client.clientId))
