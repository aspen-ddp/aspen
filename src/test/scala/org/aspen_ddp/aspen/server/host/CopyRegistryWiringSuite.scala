package org.aspen_ddp.aspen.server.host

import org.aspen_ddp.aspen.IntegrationTestSuite
import org.aspen_ddp.aspen.client.AspenClient
import org.aspen_ddp.aspen.common.HLCTimestamp
import org.aspen_ddp.aspen.common.metadata.StorageDeviceId
import org.aspen_ddp.aspen.common.network.StartStoreTransfer
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.server.Host
import org.aspen_ddp.aspen.server.crl.CrashRecoveryLog
import org.aspen_ddp.aspen.server.rebuild.{RebuildingStore, StoreRebuild, StoreRebuildFactory}
import org.aspen_ddp.aspen.server.store.backend.BackendOpener
import org.aspen_ddp.aspen.server.transfer.StoreTransfer

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Success, Try}

/** A rebuild that finishes at once, its copy placed at the store's path as RebuildingStore.finish places it: a store
 *  config and the rebuild-complete marker of its episode */
private class PlacingRebuildFactory(writeConfig: (os.Path, StoreId) => os.Path) extends StoreRebuildFactory:
  def createRebuild(client: AspenClient, storeId: StoreId, storageDeviceId: StorageDeviceId, devicePath: os.Path,
                    backends: BackendOpener, episode: Option[UUID]): StoreRebuild =
    val dir = writeConfig(devicePath, storeId)
    os.write.over(dir / RebuildingStore.RebuildCompleteMarkerFile, RebuildingStore.rebuildCompleteContent(episode))
    val id = storeId
    new StoreRebuild:
      val storeId: StoreId = id
      def complete: Future[Unit] = Future.unit
      def cancel(): Unit = ()

/** The copy registry in shadow mode (store copy state machine, stage 1): a device load registers each store directory
 *  with its phase, every disk mutation keeps it, and the drain check (Host.Seams.verifyCopyRegistry) finds no
 *  disagreement with the disk */
class CopyRegistryWiringSuite extends IntegrationTestSuite with ControlFixture:

  private def sid(index: Int): StoreId =
    StoreId(PoolId(UUID.fromString("11111111-1111-1111-1111-111111111111")), index.toByte)

  /** A device directory holding its CRL directory, so the load does not take its CRL for lost */
  private def device(hostRoot: os.Path, deviceId: StorageDeviceId): os.Path =
    val dev = writeDevice(hostRoot, "dev0", deviceId)
    os.makeDir.all(CrashRecoveryLog.deviceCrlDir(dev))
    dev

  private def phases(host: Host, deviceId: StorageDeviceId): Map[StoreId, Phase] =
    host.control.copyRegistry.device(deviceId).map(_.copies.view.mapValues(_.phase).toMap).getOrElse(Map())

  test("a device load registers each store directory with its phase"):
    val hostRoot = newHostDir()
    val dev = device(hostRoot, deviceA)
    writeStoreConfig(dev, sid(0))
    os.write(writeStoreConfig(dev, sid(1)) / RebuildingStore.RebuildRequiredMarkerFile, "")
    os.write(writeStoreConfig(dev, sid(2)) / StoreTransfer.TransferOutMarkerFile, "t-out\n")
    os.write(writeStoreConfig(dev, sid(3)) / StoreTransfer.TransferredInMarkerFile, "t-in")
    os.write(writeStoreConfig(dev, sid(4)) / RebuildingStore.RebuildCompleteMarkerFile, "ep")
    // Not store directories: a scratch area and a directory whose name is no store's
    os.makeDir.all(dev / "creating")
    os.makeDir.all(dev / "not-a-store")

    val host = newHost(hostRoot, verifyCopyRegistry = true)
    host.testingOnlyHandleEvents()

    phases(host, deviceA) should be(Map(
      sid(0) -> Phase.Live,
      sid(1) -> Phase.Lost,
      sid(2) -> Phase.Outbound("t-out"),
      sid(3) -> Phase.Arrived("t-in"),
      sid(4) -> Phase.Rebuilt("ep")))
    host.control.copyRegistry.device(deviceA).get.copies.values.map(_.activity).toSet should be(Set(Activity.Idle))
    host.control.copyRegistry.verifyAgainstDisk() should be(Nil)

  test("an unreadable copy registers Unread and stays silent"):
    val hostRoot = newHostDir()
    val dev = device(hostRoot, deviceA)
    val dir = writeStoreConfig(dev, sid(0))
    os.perms.set(dir, "rw-------")
    try
      val host = newHost(hostRoot, verifyCopyRegistry = true)
      // The drain's check skips what cannot be read: it neither throws nor reports
      host.testingOnlyHandleEvents()
      phases(host, deviceA) should be(Map(sid(0) -> Phase.Unread))
      host.control.copyRegistry.verifyAgainstDisk() should be(Nil)
      host.control.copyRegistry.takeShadowErrors() should be(Vector())
    finally os.perms.set(dir, "rwx------")

  atest("a restart rebuilds the same phases"):
    given ExecutionContext = executionContext
    // A store of the bootstrap pool, whose pool entry names the bootstrap device: startOut reads both fresh
    val shipped = StoreId(PoolId.BootstrapPoolId, 1)
    val source = StorageDeviceId.BootstrapStorageDeviceId
    val rebuiltId = sid(7)
    val hostRoot = newHostDir()
    val dev = device(hostRoot, source)
    writeStoreConfig(dev, shipped)
    writeStoreConfig(dev, sid(0))
    os.write(writeStoreConfig(dev, sid(1)) / StoreTransfer.TransferredInMarkerFile, "t-in")
    os.write(writeStoreConfig(dev, sid(2)) / RebuildingStore.RebuildCompleteMarkerFile, "kept")
    val sends = new StagedSendService
    val host = newHost(hostRoot, transferService = sends,
                       storeRebuildFactory = new PlacingRebuildFactory(writeStoreConfig),
                       verifyCopyRegistry = true)
    host.testingOnlyHandleEvents()
    val local = loadedDevices(host)(source)
    val episode = UUID.randomUUID()

    for
      _ <- net.createSecondDevice()
      _ <- waitForTransactionsToComplete()
      _ <- client.transferStore(shipped, net.secondDeviceId)
      _ <- waitForTransactionsToComplete()
      started <- host.control.transfersOut.startOut(StartStoreTransfer(host.hostId, client.clientId, source,
                   shipped, HLCTimestamp.now, UUID.randomUUID())).transform(Success(_))
      _ <- yieldUntil(sends.isTransferringOut(shipped))
      _ = host.testingOnlyHandleEvents()
      _ = host.control.rebuilds.startRebuild(local, rebuiltId, Some(episode), acceptLoss = false)
      _ <- yieldUntil(recorded(host).rebuildOutcomes.contains((source, rebuiltId)))
      _ <- waitForTransactionsToComplete()
      _ = host.testingOnlyHandleEvents()
      before = phases(host, source)
      restarted = newHost(hostRoot, verifyCopyRegistry = true)
      _ = restarted.testingOnlyHandleEvents()
    yield
      started should be(Success(()))
      before(shipped) should be(a[Phase.Outbound])
      before(sid(0)) should be(Phase.Live)
      before(sid(1)) should be(Phase.Arrived("t-in"))
      before(sid(2)) should be(Phase.Rebuilt("kept"))
      // The bootstrap device's entry does not list the rebuilt store, so its flip discards the copy
      recorded(host).rebuildOutcomes((source, rebuiltId)) should be(DeviceMetadata.RebuildOutcome.Discard)
      before.contains(rebuiltId) should be(false)
      phases(restarted, source) should be(before)
      val copies = restarted.control.copyRegistry.device(source).get
      copies.copies.values.map(_.activity).toSet should be(Set(Activity.Idle))
      copies.work should be(Map())
      host.control.copyRegistry.verifyAgainstDisk() should be(Nil)
      restarted.control.copyRegistry.verifyAgainstDisk() should be(Nil)
