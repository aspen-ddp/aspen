package org.aspen_ddp.aspen.server

import org.aspen_ddp.aspen.{IntegrationTestSuite, TestNetwork}
import org.aspen_ddp.aspen.client.AspenClient
import org.aspen_ddp.aspen.common.metadata.HostId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.util.BackgroundTaskManager
import org.aspen_ddp.aspen.server.network.Messenger as ServerMessenger
import org.aspen_ddp.aspen.server.rebuild.{RebuildingStore, StoreRebuildFactory}
import org.aspen_ddp.aspen.server.store.backend.Backend
import org.aspen_ddp.aspen.server.store.backend.fsif.{FsifBackend, FsifLayout, FsifRecovery}
import org.aspen_ddp.aspen.server.store.cache.ObjectCache
import org.aspen_ddp.aspen.server.transaction.{TransactionDriver, TransactionFinalizer}
import org.aspen_ddp.aspen.server.transfer.StoreTransferService

import java.io.IOException
import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.{Duration, SECONDS}

/** A real Host whose FSIF backends are opened through `openFsifBackend`, which it can make fail with out-of-space and
 *  whose `onFatal` callbacks it keeps, so a test can fire one. Its fields are read only after construction: the
 *  tests write their store directories once the host exists and load them with loadStoreById.
 */
private class FsifSeamHost(mgrClient: AspenClient,
                           systemId: UUID,
                           hostRootDir: os.Path,
                           execCtx: ExecutionContext,
                           cacheFactory: StoreId => ObjectCache,
                           messenger: ServerMessenger,
                           finalizers: TransactionFinalizer.Factory)
  extends Host(mgrClient, HostId.BootstrapHostId, systemId, hostRootDir, execCtx, cacheFactory, messenger,
    BackgroundTaskManager.NoBackgroundTaskManager, TestNetwork.TestCRL, finalizers,
    TransactionDriver.noErrorRecoveryFactory, Duration(5, SECONDS), Duration(60, SECONDS),
    StoreTransferService.Disabled, StoreRebuildFactory.Filesystem, 2):

  @volatile var outOfSpaceOpens: Int = 0
  @volatile var opens: Int = 0
  @volatile var onFatals: List[Throwable => Unit] = Nil

  override protected def openFsifBackend(storeId: StoreId, path: os.Path, layout: FsifLayout,
                                         onFatal: Throwable => Unit): Backend =
    opens += 1
    if outOfSpaceOpens > 0 then
      outOfSpaceOpens -= 1
      throw new IOException("No space left on device")
    onFatals = onFatal :: onFatals
    super.openFsifBackend(storeId, path, layout, onFatal)

class HostFsifLoadSuite extends IntegrationTestSuite with HostTestHarness:

  private val layout = FsifLayout(1024, 64)

  private def newSeamHost(hostRoot: os.Path): FsifSeamHost =
    new FsifSeamHost(client, systemId, hostRoot, executionContext, net.objectCacheFactory, net, net.FinalizerFactory)

  private def writeFsifStoreConfig(deviceDir: os.Path): os.Path =
    val storeDir = deviceDir / storeId.directoryName
    os.makeDir.all(storeDir)
    os.write.over(storeDir / StoreConfig.configFilename,
      StoreConfig(storeId, StoreConfig.Fsif(layout.segmentPayloadSize, layout.segmentCount), None).yamlConfig)
    storeDir

  /** Loads the store as tryLoadStore does after a restart, and runs the events that load queues */
  private def load(host: Host): Unit =
    host.loadStoreById(deviceA, storeId)
    host.testingOnlyHandleEvents()

  atest("an FSIF store whose creation was cut off before its marker loads as an empty store"):
    val hostRoot = newHostDir()
    val deviceDir = writeDevice(hostRoot, "dev0", deviceA)
    val host = newSeamHost(hostRoot)
    host.testingOnlyHandleEvents()
    val storeDir = writeFsifStoreConfig(deviceDir)
    // As a crash part way through initialize leaves it: data and journal, no index, no marker
    os.write(storeDir / FsifRecovery.DataFile, Array.fill[Byte](10)(1))
    os.write(storeDir / FsifRecovery.JournalFiles(0), Array.fill[Byte](10)(2))
    load(host)

    Future.successful:
      host.getStoreIds should contain(storeId)
      host.testingOnlyOfflineStores should not contain storeId
      os.exists(storeDir / FsifBackend.InitializedMarkerFile) should be(true)
      os.exists(storeDir / RebuildingStore.RebuildRequiredMarkerFile) should be(false)

  atest("an FSIF store whose recovery finds it lost is marked for rebuild and left offline"):
    val hostRoot = newHostDir()
    val deviceDir = writeDevice(hostRoot, "dev0", deviceA)
    val host = newSeamHost(hostRoot)
    host.testingOnlyHandleEvents()
    val storeDir = writeFsifStoreConfig(deviceDir)
    FsifBackend.create(storeId, storeDir, layout, executionContext, _ => ()).close().map: _ =>
      os.remove(storeDir / FsifRecovery.IndexFiles(0))
      load(host)
      host.getStoreIds should not contain storeId
      host.testingOnlyOfflineStores should contain(storeId)
      os.exists(storeDir / RebuildingStore.RebuildRequiredMarkerFile) should be(true)

  atest("an FSIF store that cannot open for lack of space stays offline and loads on a later scan"):
    val hostRoot = newHostDir()
    val deviceDir = writeDevice(hostRoot, "dev0", deviceA)
    val host = newSeamHost(hostRoot)
    host.testingOnlyHandleEvents()
    val storeDir = writeFsifStoreConfig(deviceDir)
    FsifBackend.create(storeId, storeDir, layout, executionContext, _ => ()).close().map: _ =>
      host.outOfSpaceOpens = 2
      load(host)
      host.getStoreIds should not contain storeId
      host.testingOnlyOfflineStores should contain(storeId)
      os.exists(storeDir / RebuildingStore.RebuildRequiredMarkerFile) should be(false)

      // Still full on the first retry
      host.testingOnlyScanDevices()
      host.testingOnlyHandleEvents()
      host.opens should be(2)
      host.getStoreIds should not contain storeId

      host.testingOnlyScanDevices()
      host.testingOnlyHandleEvents()
      host.opens should be(3)
      host.getStoreIds should contain(storeId)
      host.testingOnlyOfflineStores should not contain storeId
      os.exists(storeDir / RebuildingStore.RebuildRequiredMarkerFile) should be(false)

      // Loaded: later scans no longer retry it
      host.testingOnlyScanDevices()
      host.testingOnlyHandleEvents()
      host.opens should be(3)

  atest("a served FSIF store that fails fatally is unloaded, marked for rebuild and left offline"):
    val hostRoot = newHostDir()
    val deviceDir = writeDevice(hostRoot, "dev0", deviceA)
    val host = newSeamHost(hostRoot)
    host.testingOnlyHandleEvents()
    val storeDir = writeFsifStoreConfig(deviceDir)
    load(host)
    host.getStoreIds should contain(storeId)

    host.onFatals.head(new IOException("Input/output error"))
    host.testingOnlyHandleEvents()

    Future.successful:
      host.getStoreIds should not contain storeId
      host.testingOnlyOfflineStores should contain(storeId)
      os.exists(storeDir / RebuildingStore.RebuildRequiredMarkerFile) should be(true)

  atest("a fatal failure of an FSIF backend no longer served is ignored"):
    val hostRoot = newHostDir()
    val deviceDir = writeDevice(hostRoot, "dev0", deviceA)
    val host = newSeamHost(hostRoot)
    host.testingOnlyHandleEvents()
    val storeDir = writeFsifStoreConfig(deviceDir)
    load(host)
    val staleOnFatal = host.onFatals.head
    host.closeStore(storeId)
    host.testingOnlyHandleEvents()
    host.getStoreIds should not contain storeId

    staleOnFatal(new IOException("Input/output error"))
    host.testingOnlyHandleEvents()

    Future.successful:
      os.exists(storeDir / RebuildingStore.RebuildRequiredMarkerFile) should be(false)
