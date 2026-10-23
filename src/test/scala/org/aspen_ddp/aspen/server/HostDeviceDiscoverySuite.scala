package org.aspen_ddp.aspen.server

import org.aspen_ddp.aspen.{IntegrationTestSuite, TestNetwork}
import org.aspen_ddp.aspen.common.HLCTimestamp
import org.aspen_ddp.aspen.common.metadata.{HostId, StorageDeviceId, StorageDeviceState, fixed_ids}
import org.aspen_ddp.aspen.common.network.{CheckStorageDevice, StartStoreTransfer, StoreMoved}
import org.aspen_ddp.aspen.common.objects.Insert
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.transaction.KeyValueUpdate.KeyRevision
import org.aspen_ddp.aspen.common.util.BackgroundTaskManager
import org.aspen_ddp.aspen.server.rebuild.StoreRebuildFactory
import org.aspen_ddp.aspen.server.transaction.TransactionDriver
import org.aspen_ddp.aspen.server.transfer.{StoreTransfer, StoreTransferService}

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}
import java.util.UUID
import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.concurrent.duration.{Duration, SECONDS}


/** A StoreTransferService whose transfers move no bytes and finish only when the test says so.
 *
 *  The real StoreTransferManager needs a live sending host, a TLS socket and a `jar` subprocess,
 *  none of which exists here, but the interesting half of a transfer for these tests is what Host
 *  does once the Future from beginTransferIn resolves. beginTransferOut is unimplemented on
 *  purpose: nothing in this suite ships a store out, and a silently-working stub would hide it if
 *  something started to.
 */
private class StagedTransferService extends StoreTransferService:
  private val calls = mutable.ArrayBuffer[(StoreId, UUID, os.Path, Promise[Unit])]()
  private val cancels = mutable.ArrayBuffer[(StoreId, UUID)]()

  /** Every cancelTransferIn call, in order. */
  def cancelledTransfersIn: Vector[(StoreId, UUID)] = synchronized(cancels.toVector)

  /** Every beginTransferIn call, in order. */
  def transfersIn: Vector[(StoreId, UUID, os.Path)] =
    synchronized(calls.map((sid, uuid, dir, _) => (sid, uuid, dir)).toVector)

  /** The stores a transfer in has been begun for. */
  def startedTransfersIn: Set[StoreId] = transfersIn.map(_._1).toSet

  private def latest(storeId: StoreId): Option[(StoreId, UUID, os.Path, Promise[Unit])] =
    synchronized(calls.findLast(_._1 == storeId))

  /** Resolves `storeId`'s latest transfer as if every byte had arrived. Leaves behind what
   *  extraction would have: `<dir>/<storeId.directoryName>`, still carrying the source's
   *  transfer-out marker, for Host's promotion step to move into place. */
  def finishTransferIn(storeId: StoreId): Unit =
    latest(storeId).foreach: (sid, _, dir, p) =>
      val staged = dir / sid.directoryName
      os.makeDir.all(staged)
      os.write.over(staged / StoreTransfer.TransferOutMarkerFile, "")
      p.success(())

  /** Fails `storeId`'s latest transfer, as a timed-out or truncated stream would. */
  def failTransferIn(storeId: StoreId): Unit =
    latest(storeId).foreach(_._4.failure(new Exception(s"Simulated failure of transfer in of $storeId")))

  def beginTransferIn(storeId: StoreId, transferUUID: UUID, destinationDirectory: os.Path): Future[Unit] =
    synchronized:
      val p = Promise[Unit]()
      calls += ((storeId, transferUUID, destinationDirectory, p))
      p.future

  def cancelTransferIn(storeId: StoreId, transferUUID: UUID): Unit =
    val p = synchronized:
      cancels += ((storeId, transferUUID))
      calls.find(c => c._1 == storeId && c._2 == transferUUID).map(_._4)
    p.foreach(_.tryFailure(new Exception(s"Cancelled transfer in of $storeId")))

  def beginTransferOut(storeId: StoreId, transferUUID: UUID, sourceDirectory: os.Path,
                       destHost: String, destPort: Int, timestamp: HLCTimestamp): Unit =
    throw new UnsupportedOperationException("StagedTransferService does not send stores")

  def isTransferringOut(storeId: StoreId): Boolean = false

  def transferringOutStores: Set[StoreId] = Set.empty


/** Delegates execution to `underlying`, and records every reportFailure call instead of
 *  passing it on.
 *
 *  A NonFatal throw out of a Future callback goes to the ExecutionContext's reportFailure and
 *  nowhere else, so this is the only way for a test to see which exception escaped -- which is
 *  exactly the question when a finally can replace one exception with another. (A fatal
 *  Throwable is rethrown instead, so it never lands here.)
 *
 *  Recording rather than forwarding means the underlying context never prints the throwable.
 *  That is deliberate: a test whose passing state involves an expected throw should not print a
 *  stack trace on every green run. The assertions on `failures` carry the diagnostic instead.
 */
private class RecordingExecutionContext(underlying: ExecutionContext) extends ExecutionContext:
  private val recorded = mutable.ListBuffer[Throwable]()

  def execute(runnable: Runnable): Unit = underlying.execute(runnable)

  def reportFailure(cause: Throwable): Unit = synchronized:
    recorded += cause

  def failures: List[Throwable] = synchronized(recorded.toList)


class HostDeviceDiscoverySuite extends IntegrationTestSuite with HostTestHarness:

  /** `foreignSystemId` is the rejection case: a device config carrying it must be ignored by
   *  the scan. */
  private val foreignSystemId = UUID.fromString("44444444-4444-4444-4444-444444444444")

  /** Overwrites the config file with bytes SnakeYAML cannot parse. */
  private def writeUnparseableDeviceConfig(deviceDir: Path): Unit =
    Files.write(deviceDir.resolve(StorageDeviceConfig.configFilename),
                "this is not: valid: device config yaml".getBytes(StandardCharsets.UTF_8))

  /** Creates an empty device directory with no config file. */
  private def newDeviceDirWithoutConfig(hostRoot: Path, name: String): Path =
    val dir = StorageDeviceManager.deviceDirectory(hostRoot, name)
    Files.createDirectories(dir)
    dir

  /** A second store in the same pool. Used where one test has to pin both halves of the
   *  abandoned-transfer-out guard at once, since each half needs its own device-tree entry. */
  private val storeIdB = StoreId(storeId.poolId, 1.toByte)

  /** A tombstoned StorageDeviceState for a device that is still mounted here. Both ids are
   *  zeroed, which is what an operator's fail-storage-device leaves behind. */
  private def tombstonedState(stores: Map[StoreId, StorageDeviceState.StoreEntry] = Map()):
      StorageDeviceState =
    StorageDeviceState(fixed_ids.FailedStorageDeviceId, fixed_ids.FailedHostId,
                       0L, 1024L, stores, deviceSetId)

  atest("constructor loads a device that already exists on disk"):
    val hostRoot = newHostDir()
    val deviceDir = writeDevice(hostRoot, "dev0", deviceA)

    val host = newHost(hostRoot)

    Future.successful:
      host.loadedDevices.keySet should be(Set(deviceA))
      host.loadedDevices(deviceA).devicePath should be(deviceDir)

      // Also pins the recording seam itself: tryLoadStore is offered every child of the
      // device directory, and dev0's only child is the device config file.
      host.storeLoadAttempts.toList should be(
        List((deviceA, deviceDir.resolve(StorageDeviceConfig.configFilename))))

  atest("a device created after construction is discovered on the next CheckAllDevices"):
    val hostRoot = newHostDir()
    val host = newHost(hostRoot)

    host.loadedDevices.keySet should be(empty)

    writeDevice(hostRoot, "dev0", deviceA)
    host.testingOnlyCheckAllDevices()

    Future.successful(host.loadedDevices.keySet should be(Set(deviceA)))

  atest("a missing storage-devices directory does not throw"):
    val host = newHost(newHostDir(withStorageDevicesDir = false))

    noException should be thrownBy host.testingOnlyCheckAllDevices()

    Future.successful(host.loadedDevices.keySet should be(empty))

  atest("an already-loaded device is not reloaded"):
    val hostRoot = newHostDir()
    val deviceDir = writeDevice(hostRoot, "dev0", deviceA)
    Files.createDirectories(deviceDir.resolve("some-store-dir"))

    val host = newHost(hostRoot)
    val firstState = host.loadedDevices(deviceA)
    val attemptsAfterConstruction = host.storeLoadAttempts.toList

    // tryLoadDevice offers every child of the device directory to tryLoadStore: here the
    // device config file and some-store-dir. The real tryLoadStore would load neither -- it
    // requires a store config file inside the candidate, and the config file is not a
    // directory while some-store-dir is empty -- but the recording override takes both.
    // Compared as a set because listFiles() ordering is unspecified.
    attemptsAfterConstruction.map(_._2).toSet should be(Set(
      deviceDir.resolve(StorageDeviceConfig.configFilename),
      deviceDir.resolve("some-store-dir")))

    host.testingOnlyCheckAllDevices()
    host.testingOnlyCheckAllDevices()

    // Same instance: the device's offlineStores tracking survives a rescan.
    host.loadedDevices(deviceA) should be theSameInstanceAs firstState
    Future.successful(host.storeLoadAttempts.toList should be(attemptsAfterConstruction))

  atest("a device whose stores fail to load is retried on a later scan"):
    val hostRoot = newHostDir()
    writeDevice(hostRoot, "dev0", deviceA)

    val host = newHost(hostRoot, failFirstStoreLoad = true)

    // The load threw part-way through, so the device must not be recorded as loaded. Were it
    // recorded, the idempotency guard would skip it forever and its stores would never load.
    host.loadedDevices.keySet should be(empty)

    // A later scan retries it from scratch and succeeds.
    host.testingOnlyCheckAllDevices()
    Future.successful(host.loadedDevices.keySet should be(Set(deviceA)))

  atest("a second directory claiming a loaded device id is ignored"):
    val hostRoot = newHostDir()
    val originalDir = writeDevice(hostRoot, "dev0", deviceA)

    val host = newHost(hostRoot)
    val originalState = host.loadedDevices(deviceA)
    originalState.devicePath should be(originalDir)

    // A duplicate mount or a copied config file: same device id, different directory.
    writeDevice(hostRoot, "dev0-copy", deviceA)
    host.testingOnlyCheckAllDevices()

    host.loadedDevices.keySet should be(Set(deviceA))
    Future.successful(host.loadedDevices(deviceA) should be theSameInstanceAs originalState)

  atest("a device directory with no config is skipped, then loaded once the config appears"):
    val hostRoot = newHostDir()
    val deviceDir = newDeviceDirWithoutConfig(hostRoot, "dev0")

    val host = newHost(hostRoot)
    host.loadedDevices.keySet should be(empty)

    // createStorageDevice requires the directory to already exist and never creates one, so a
    // mounted-but-not-yet-registered directory is a normal steady state, not a brief window.
    // Every scan has to tolerate it and pick the device up whenever the config does appear.
    writeDeviceConfig(deviceDir, deviceA)
    host.testingOnlyCheckAllDevices()

    Future.successful(host.loadedDevices.keySet should be(Set(deviceA)))

  atest("CheckStorageDevice for an unknown but on-disk device triggers discovery"):
    val hostRoot = newHostDir()
    val host = newHost(hostRoot)

    host.loadedDevices.keySet should be(empty)

    writeDevice(hostRoot, "dev0", deviceA)
    host.testingOnlyHandleHostMessage(
      CheckStorageDevice(HostId.BootstrapHostId, client.clientId, deviceA))

    Future.successful(host.loadedDevices.keySet should be(Set(deviceA)))

  atest("an unusable entry under storage-devices does not stop its siblings from loading"):
    val hostRoot = newHostDir()
    // .DS_Store and friends: a plain file where the scan expects a device directory.
    Files.write(StorageDeviceManager.deviceDirectory(hostRoot, ".DS_Store"),
                "not a device".getBytes(StandardCharsets.UTF_8))
    newDeviceDirWithoutConfig(hostRoot, "unconfigured")
    writeDevice(hostRoot, "dev0", deviceA)

    val host = newHost(hostRoot)

    Future.successful(host.loadedDevices.keySet should be(Set(deviceA)))

  atest("a device belonging to another Aspen system is ignored"):
    val hostRoot = newHostDir()
    writeDevice(hostRoot, "dev0", deviceA, foreignSystemId)
    writeDevice(hostRoot, "dev1", deviceB)

    val host = newHost(hostRoot)

    host.loadedDevices.keySet should be(Set(deviceB))

    host.testingOnlyCheckAllDevices()

    Future.successful(host.loadedDevices.keySet should be(Set(deviceB)))

  atest("an unparseable device config is skipped and retried on the next scan"):
    val hostRoot = newHostDir()
    val deviceDir = newDeviceDirWithoutConfig(hostRoot, "dev0")
    // Two ':' separators in one flow scalar, so SnakeYAML throws rather than returning a
    // config missing its required keys. This is the only test covering tryLoadDevice's
    // catch-all; keep the string genuinely unparseable or it silently becomes a duplicate of
    // the missing-config test above.
    writeUnparseableDeviceConfig(deviceDir)

    val host = newHost(hostRoot)
    host.loadedDevices.keySet should be(empty)

    // A config that stays broken must not poison later scans or half-register the device.
    host.testingOnlyCheckAllDevices()
    host.loadedDevices.keySet should be(empty)

    writeDeviceConfig(deviceDir, deviceA)
    host.testingOnlyCheckAllDevices()

    Future.successful(host.loadedDevices.keySet should be(Set(deviceA)))

  atest("stores on a newly discovered device are offered to tryLoadStore"):
    val hostRoot = newHostDir()
    val host = newHost(hostRoot)

    host.storeLoadAttempts should be(empty)

    val deviceDir = writeDevice(hostRoot, "dev0", deviceA)
    val storeDir = deviceDir.resolve(storeId.directoryName)
    Files.createDirectories(storeDir)

    host.testingOnlyCheckAllDevices()

    host.loadedDevices.keySet should be(Set(deviceA))
    // tryLoadDevice offers every child of the device directory to tryLoadStore: here the
    // device config file and the store directory. The real tryLoadStore would load neither --
    // it requires a store config file inside the candidate, and the config file is not a
    // directory while the store directory is empty -- but the recording override takes both.
    Future.successful(host.storeLoadAttempts.toList should contain((deviceA, storeDir)))

  atest("a device check that fails does not wedge later checks of the same device"):
    val hostRoot = newHostDir()
    writeDevice(hostRoot, "dev0", deviceA)

    val host = newHost(hostRoot)
    host.loadedDevices.keySet should be(Set(deviceA))

    // deviceA is on disk but absent from the StorageDevicesTree the TestNetwork bootstrapped,
    // so getStorageDeviceState fails with NoSuchElementException. In production that exact
    // state -- a device on disk with no entry in the storage-devices tree -- means a config
    // written out-of-band naming an id that was never registered, or a tree entry removed
    // after the fact; neither is something a command produces. What is routinely reachable
    // is not that state at all but a failed metadata read, which arrives here identically.
    // (A copied or moved config is not one of these: its device is registered, so the lookup
    // succeeds.) The failure must still release the activeDeviceChecks entry.
    host.testingOnlyCheckAllDevices()

    yieldUntil(host.testingOnlyActiveDeviceChecks.isEmpty).map: _ =>
      // yieldUntil gives up silently, so this is the assertion that turns an exhausted wait
      // into a failure rather than a pass.
      host.testingOnlyActiveDeviceChecks should be(empty)

      // And the release lets a later check of the same device start rather than being skipped
      // forever by the in-progress guard.
      host.testingOnlyCheckAllDevices()
      host.testingOnlyActiveDeviceChecks should be(Set(deviceA))

  atest("an armed lookup holds the device check open until the test completes it"):
    val hostRoot = newHostDir()
    writeDevice(hostRoot, "dev0", deviceA)

    val host = newHost(hostRoot)
    val p = host.armLookup(deviceA)

    host.testingOnlyCheckAllDevices()

    host.lookupAttempts.toList should be(List(deviceA))

    // Drain everything the check queued. A real client read of deviceA fails immediately --
    // deviceA is absent from the storage-devices tree -- so an unarmed check would have
    // released the guard by the time this wait exhausts. Still holding it is what proves the
    // armed promise, and not the client's read, is what the check is waiting on.
    yieldUntil(host.testingOnlyActiveDeviceChecks.isEmpty).flatMap: _ =>
      host.testingOnlyActiveDeviceChecks should be(Set(deviceA))

      p.failure(new RuntimeException("test-controlled lookup failure"))

      yieldUntil(host.testingOnlyActiveDeviceChecks.isEmpty).map: _ =>
        // yieldUntil gives up silently, so this is the assertion that turns an exhausted wait
        // into a failure rather than a pass.
        host.testingOnlyActiveDeviceChecks should be(empty)

  atest("a check started before its device loads does not mark the loaded device's stores offline"):
    val hostRoot = newHostDir()
    val host = newHost(hostRoot)

    host.loadedDevices.keySet should be(empty)

    // Two arms: the lookup held in flight across the load, and the one the deferred request
    // issues once it completes.
    val p1 = host.armLookup(deviceA)
    // The lookup the re-dispatch issues. Armed so it does not fall through to the real client.
    host.armLookup(deviceA)

    host.testingOnlyHandleHostMessage(
      CheckStorageDevice(HostId.BootstrapHostId, client.clientId, deviceA))

    host.loadedDevices.keySet should be(empty)
    host.testingOnlyActiveDeviceChecks should be(Set(deviceA))

    // The config appears and a later event loads the device while the lookup is outstanding.
    writeDevice(hostRoot, "dev0", deviceA)
    host.testingOnlyCheckAllDevices()
    host.loadedDevices.keySet should be(Set(deviceA))

    // That event's own check request collided with the outstanding lookup. It is the request
    // the guard used to discard outright, costing a full checkStorageDevicePeriod.
    host.testingOnlyDeferredDeviceChecks should be(Set(deviceA))

    // An Active store makes reconcileDeviceState a no-op in every one of its branches, so this
    // pins branch selection alone rather than dragging in store creation or transfers.
    p1.success(deviceState(
      deviceA,
      Map(storeId -> StorageDeviceState.StoreEntry(StorageDeviceState.StoreStatus.Active, None))))

    yieldUntil(host.lookupAttempts.size == 2).map: _ =>
      // yieldUntil gives up silently, so assert its condition first. This also proves the
      // first callback ran, without which the negative assertion below would pass vacuously.
      host.lookupAttempts.toList should be(List(deviceA, deviceA))
      host.testingOnlyDeferredDeviceChecks should be(empty)

      // The device was loaded before the lookup returned, so its stores must not be marked
      // offline by a decision taken back when it was not. In production nothing would clear
      // them afterwards: tryLoadStore and the LoadStore handler both ran on the way in, and
      // reconcileDeviceState's deleted-stores pass only removes ids recorded in the device's
      // own offlineStores set, which ids marked by this branch never enter.
      host.testingOnlyOfflineStores should not contain storeId

  atest("a check for a device that never loads marks its stores offline"):
    val hostRoot = newHostDir()
    val host = newHost(hostRoot)

    val p = host.armLookup(deviceA)

    host.testingOnlyHandleHostMessage(
      CheckStorageDevice(HostId.BootstrapHostId, client.clientId, deviceA))

    // Nothing was written under storage-devices/, so the check runs against a device this
    // host has never loaded -- the case the offline marking exists for.
    host.loadedDevices.keySet should be(empty)

    p.success(deviceState(
      deviceA,
      Map(storeId -> StorageDeviceState.StoreEntry(StorageDeviceState.StoreStatus.Active, None))))

    yieldUntil(host.testingOnlyActiveDeviceChecks.isEmpty).map: _ =>
      // yieldUntil gives up silently, so assert the condition it waited on.
      host.testingOnlyActiveDeviceChecks should be(empty)

      // Suppresses TxUnknownStore and ReadResponse(StoreNotFound) for stores on a device that
      // is down. Deleting this marking is silent in production and, until this test, silent in
      // the suite too.
      host.testingOnlyOfflineStores should contain(storeId)

  atest("a check request arriving during an in-flight check is deferred, not dropped"):
    val hostRoot = newHostDir()
    writeDevice(hostRoot, "dev0", deviceA)

    val host = newHost(hostRoot)
    val p1 = host.armLookup(deviceA)
    // The lookup the re-dispatch issues. Armed so it does not fall through to the real client.
    host.armLookup(deviceA)

    host.testingOnlyCheckAllDevices()
    host.testingOnlyActiveDeviceChecks should be(Set(deviceA))
    host.lookupAttempts.toList should be(List(deviceA))

    // Collides with the outstanding lookup. This is the create-storage-device nudge landing
    // during a periodic sweep, and dropping it costs a full checkStorageDevicePeriod.
    host.testingOnlyCheckAllDevices()
    host.testingOnlyDeferredDeviceChecks should be(Set(deviceA))
    host.lookupAttempts.toList should be(List(deviceA))

    // A third request collapses into the same deferral. The testingOnlyDeferredDeviceChecks
    // assertion after the drain is what pins Set semantics: a queue or a counter would still
    // be holding a second request for deviceA there, having only shed one on the re-dispatch.
    // lookupAttempts cannot pin it -- the re-dispatch consumes the second armed lookup, which
    // this test never completes, so no implementation gets as far as a second re-dispatch.
    host.testingOnlyCheckAllDevices()
    host.testingOnlyDeferredDeviceChecks should be(Set(deviceA))

    p1.success(deviceState(deviceA))

    yieldUntil(host.lookupAttempts.size == 2).map: _ =>
      // yieldUntil gives up silently, so assert the condition it waited on.
      host.lookupAttempts.toList should be(List(deviceA, deviceA))
      host.testingOnlyDeferredDeviceChecks should be(empty)

  atest("a deferred check still runs when the in-flight lookup fails"):
    val hostRoot = newHostDir()
    writeDevice(hostRoot, "dev0", deviceA)

    val host = newHost(hostRoot)
    val p1 = host.armLookup(deviceA)
    // The lookup the re-dispatch issues. Armed so it does not fall through to the real client.
    host.armLookup(deviceA)

    host.testingOnlyCheckAllDevices()
    host.testingOnlyCheckAllDevices()
    host.testingOnlyDeferredDeviceChecks should be(Set(deviceA))

    // A failed lookup never reaches the completion-time re-read, so the deferral is the only
    // thing that rescues the request that collided with it.
    p1.failure(new RuntimeException("test-controlled lookup failure"))

    yieldUntil(host.lookupAttempts.size == 2).map: _ =>
      // yieldUntil gives up silently, so assert the condition it waited on.
      host.lookupAttempts.toList should be(List(deviceA, deviceA))
      host.testingOnlyDeferredDeviceChecks should be(empty)

  atest("a lookup that throws synchronously does not wedge the device"):
    val hostRoot = newHostDir()
    writeDevice(hostRoot, "dev0", deviceA)

    val host = newHost(hostRoot)
    host.loadedDevices.keySet should be(Set(deviceA))

    // A failed Promise cannot stage this: it still yields a Future, so the callback and its
    // finally still run. Only a throw before the Future exists skips them, and the guard entry
    // is added before the lookup call.
    host.armLookupThrow(deviceA, new RuntimeException("test-controlled synchronous lookup failure"))

    // Treated as a failed lookup, not propagated. testingOnlyHandleHostMessage calls handleEvent
    // directly rather than going through the event loop, so start()'s catch-all is not in the
    // way and a propagating throw would surface right here.
    noException should be thrownBy host.testingOnlyHandleHostMessage(
      CheckStorageDevice(HostId.BootstrapHostId, client.clientId, deviceA))

    host.lookupAttempts.toList should be(List(deviceA))

    // Synchronous release is required here, not merely observed, so there is no yieldUntil.
    // Routing the throw back through the Future machinery instead -- catch it, wrap it in
    // Future.failed, let the existing onComplete arm release the guard -- also un-wedges the
    // device, and this assertion deliberately rejects it. That shape would report the throw with
    // the Failure(err) arm's warning, which explains itself with a missing storage-devices
    // registration that a call never reaching the tree does not have, and it would make the
    // release contingent on an ExecutionContext that actually drains -- the same class of leak
    // this test exists to close. A failure here means the release is no longer happening on the
    // calling thread -- either moved off it, or not happening at all.
    host.testingOnlyActiveDeviceChecks should be(empty)

    // A sanity anchor rather than a discriminator: no deferral can exist yet. checkStorageDevice
    // and startDeviceCheck both run under the instance monitor, so nothing can slip a deferral in
    // between the guard entry and the lookup call that throws.
    host.testingOnlyDeferredDeviceChecks should be(empty)

    // The assertion that distinguishes released from wedged. A leaked entry makes every later
    // check of this device a no-op: checkStorageDevice records a deferral instead of starting a
    // lookup, and the only thing that would clear that deferral is the finally that never ran.
    val p = host.armLookup(deviceA)
    host.testingOnlyHandleHostMessage(
      CheckStorageDevice(HostId.BootstrapHostId, client.clientId, deviceA))

    host.lookupAttempts.toList should be(List(deviceA, deviceA))
    host.testingOnlyActiveDeviceChecks should be(Set(deviceA))

    // An empty store map makes reconcileDeviceState a no-op in every branch, so the check
    // finishes cleanly rather than leaving the suite's teardown to tidy up after it.
    p.success(deviceState(deviceA))

    yieldUntil(host.testingOnlyActiveDeviceChecks.isEmpty).map: _ =>
      // yieldUntil gives up silently, so assert the condition it waited on.
      host.testingOnlyActiveDeviceChecks should be(empty)
      host.testingOnlyDeferredDeviceChecks should be(empty)

  atest("a throw out of the check callback is not replaced by the re-dispatch's own throw"):
    val hostRoot = newHostDir()
    val recordingEc = new RecordingExecutionContext(executionContext)
    val host = newHost(hostRoot, ec = recordingEc)

    // A device the scan could not produce: devicePath is relative, so reconcileDeviceState's
    // deleted-stores pass throws at os.Path(local.devicePath) -- the first line on that path
    // above the try/catch wrapping os.remove.all. local.offlineStores holds the id that makes
    // the pass iterate at all; the armed remote state below omits it, which is what marks it
    // deleted.
    val local = new Host.LocalStorageDeviceState(
      deviceA, Paths.get("relative-device-dir"), hostRoot.toFile)
    local.offlineStores = Set(storeId)
    host.injectLoadedDevice(local)

    val p1 = host.armLookup(deviceA)

    // The lookup the re-dispatch issues, armed to throw. Under the old code this throw escapes
    // the finally and replaces the reconcile's, which is the defect under test.
    val redispatchError = new RuntimeException("test-controlled re-dispatch lookup failure")
    host.armLookupThrow(deviceA, redispatchError)

    host.testingOnlyHandleHostMessage(
      CheckStorageDevice(HostId.BootstrapHostId, client.clientId, deviceA))
    host.testingOnlyActiveDeviceChecks should be(Set(deviceA))

    // Collides with the outstanding lookup, so the finally has a deferral to re-dispatch.
    host.testingOnlyHandleHostMessage(
      CheckStorageDevice(HostId.BootstrapHostId, client.clientId, deviceA))
    host.testingOnlyDeferredDeviceChecks should be(Set(deviceA))

    // Matching hostId keeps reconcileDeviceState off its host-migration branch; no stores means
    // storeId counts as deleted and the pass runs.
    p1.success(deviceState(deviceA))

    yieldUntil(host.lookupAttempts.size == 2).map: _ =>
      // yieldUntil gives up silently, so assert its condition first. It also proves the
      // re-dispatch happened, without which the assertions below would pass vacuously.
      host.lookupAttempts.toList should be(List(deviceA, deviceA))

      // The discriminating pair, deliberately ahead of the release assertions below. Against the
      // pre-fix code those two failed with the same "was not empty" message the synchronous-throw
      // test above already reported, so leading with them left this test's red output saying
      // nothing about the defect it is named for. Ordered this way, a regression that revives the
      // masking reports the wrong exception by name instead.
      //
      // Old code records exactly one failure and it is redispatchError, because the throw out
      // of the finally replaced the reconcile's. New code absorbs redispatchError inside the
      // re-dispatch and lets the reconcile's escape, so the one recorded failure is os-lib's.
      //
      // Size before content, because this recorder sees every callback the host runs
      // through its ExecutionContext: a bare non-empty check would also pass on an unrelated
      // failure with the reconcile silently not throwing at all. Matched on the message
      // fragment rather than IllegalArgumentException, which is too common a class to pin
      // anything to.
      recordingEc.failures should have size 1
      recordingEc.failures.head.getMessage should include("is not an absolute path")

      // The finally still released, even though the try body threw.
      host.testingOnlyActiveDeviceChecks should be(empty)
      host.testingOnlyDeferredDeviceChecks should be(empty)

  atest("a device owned by another host is claimed with updateHostId"):
    val hostRoot = newHostDir()
    writeDevice(hostRoot, "dev0", deviceA)
    val host = newHost(hostRoot)

    val armed = host.armLookup(deviceA)
    host.testingOnlyCheckAllDevices()
    armed.success(StorageDeviceState(deviceA, HostId(UUID.randomUUID()),
                                     0L, 1024L, Map(), deviceSetId))

    yieldUntil(host.hostIdClaims.nonEmpty).map: _ =>
      host.hostIdClaims.toList should be(List(deviceA))

  atest("a tombstoned device is not claimed and nothing is reconciled"):
    val hostRoot = newHostDir()
    writeDevice(hostRoot, "dev0", deviceA)
    val host = newHost(hostRoot)

    val before = host.storeLoadAttempts.size

    val armed = host.armLookup(deviceA)
    host.testingOnlyCheckAllDevices()
    // An Initializing store would normally drive createNewStore; on a tombstone nothing runs.
    armed.success(tombstonedState(
      Map(storeId -> StorageDeviceState.StoreEntry(
        StorageDeviceState.StoreStatus.Initializing, None))))

    yieldUntil(!host.testingOnlyActiveDeviceChecks.contains(deviceA)).map: _ =>
      host.hostIdClaims.toList should be(Nil)
      host.storeLoadAttempts.size should be(before)
      // The guard entry is released, so later checks of this device still run. That release
      // is the actual regression: the old code held it for the life of the process.
      host.testingOnlyActiveDeviceChecks should not contain deviceA

  /** Creates `<deviceDir>/<sid.directoryName>` holding a transfer-out marker file, which is what
   *  startStoreTransferOut leaves in the source's store directory the moment a transfer begins. */
  private def writeTransferringOutStore(deviceDir: Path, sid: StoreId = storeId): Path =
    val storeDir = deviceDir.resolve(sid.directoryName)
    Files.createDirectories(storeDir)
    Files.write(storeDir.resolve(StoreTransfer.TransferOutMarkerFile), Array.emptyByteArray)
    storeDir

  /** Stages a store that this host skipped at load time because of its transfer-out marker.
   *
   *  The directory and the marker are written after construction and `offlineStores` is set by
   *  hand, because the recording tryLoadStore does neither: it never honours the marker and
   *  never maintains the offline sets. Writing them before construction would only add a
   *  recorded attempt, not the state under test.
   */
  private def stageOfflineTransferringOutStore(host: RecordingHost,
                                               deviceDir: Path,
                                               sids: StoreId*): List[Path] =
    val dirs = sids.map(sid => writeTransferringOutStore(deviceDir, sid)).toList
    val local = host.loadedDevices(deviceA)
    local.offlineStores = local.offlineStores ++ sids
    dirs

  atest("an abandoned transfer-out's marker is cleared and the store loaded"):
    val hostRoot = newHostDir()
    val deviceDir = writeDevice(hostRoot, "dev0", deviceA)
    val host = newHost(hostRoot)

    val List(storeDir) = stageOfflineTransferringOutStore(host, deviceDir, storeId)
    val before = host.storeLoadAttempts.size

    val armed = host.armLookup(deviceA)
    host.testingOnlyCheckAllDevices()

    // Active with no transferDevice: the destination was declared failed mid-flight and the
    // source's entry has been put back. Nothing will ever clear the marker on its own -- the
    // destination only deletes its own unpacked copy's, and a normal completion deletes the
    // whole source directory -- so without this pass the pool names a device whose local copy
    // stays offline through every restart, with the metadata reading entirely healthy.
    armed.success(deviceState(
      deviceA,
      Map(storeId -> StorageDeviceState.StoreEntry(StorageDeviceState.StoreStatus.Active, None))))

    yieldUntil(!host.testingOnlyActiveDeviceChecks.contains(deviceA)).map: _ =>
      // yieldUntil gives up silently, so assert the condition it waited on.
      host.testingOnlyActiveDeviceChecks should not contain deviceA

      // Both representations of "not served" have to go. The marker is the one that survives a
      // restart, so leaving it would make the repair last only until the process bounced.
      Files.exists(storeDir.resolve(StoreTransfer.TransferOutMarkerFile)) should be(false)

      // And the load goes back through tryLoadStore rather than a parallel path, which is what
      // clears the two offlineStores sets in production. The recording override does not
      // maintain those sets, so this attempt is the assertion available here.
      host.storeLoadAttempts.toList.drop(before) should be(List((deviceA, storeDir)))

  atest("a store whose transfer-out is still outstanding keeps its marker"):
    val hostRoot = newHostDir()
    val deviceDir = writeDevice(hostRoot, "dev0", deviceA)
    val host = newHost(hostRoot)

    val List(outDir, stagedDir) =
      stageOfflineTransferringOutStore(host, deviceDir, storeId, storeIdB)
    val before = host.storeLoadAttempts.size

    val armed = host.armLookup(deviceA)
    host.testingOnlyCheckAllDevices()

    // The two states that mean a transfer is still outstanding and the marker is doing its job.
    // Only Active *and* no transferDevice says the transfer is over and was abandoned.
    armed.success(deviceState(
      deviceA,
      Map(storeId -> StorageDeviceState.StoreEntry(
            StorageDeviceState.StoreStatus.TransferringOut, Some(deviceB)),
          storeIdB -> StorageDeviceState.StoreEntry(
            StorageDeviceState.StoreStatus.Active, Some(deviceB)))))

    yieldUntil(!host.testingOnlyActiveDeviceChecks.contains(deviceA)).map: _ =>
      // yieldUntil gives up silently, so assert the condition it waited on.
      host.testingOnlyActiveDeviceChecks should not contain deviceA

      Files.exists(outDir.resolve(StoreTransfer.TransferOutMarkerFile)) should be(true)
      Files.exists(stagedDir.resolve(StoreTransfer.TransferOutMarkerFile)) should be(true)
      host.storeLoadAttempts.size should be(before)

  /** Zeroes both of `deviceId`'s ids in the storage-devices tree, exactly as
   *  FailedStorageDeviceDurableTask's step 1 does, without running the task. */
  private def tombstoneDevice(deviceId: StorageDeviceId): Future[Unit] =
    given ExecutionContext = executionContext
    client.transactUntilSuccessful: tx =>
      for
        ptr <- client.getStorageDevicePointer(deviceId)
        kvos <- client.read(ptr)
      yield
        val tombstoned = StorageDeviceState(kvos).copy(
          hostId = fixed_ids.FailedHostId,
          storageDeviceId = fixed_ids.FailedStorageDeviceId)
        tx.update(ptr, None, None,
          List(KeyRevision(StorageDeviceState.StateKey,
            kvos.contents(StorageDeviceState.StateKey).revision)),
          List(Insert(StorageDeviceState.StateKey, tombstoned.encode())))

  /** Waits for `condition` by alternating yields with transaction drains.
   *
   *  yieldUntil alone is enough for work that is already queued on the test's ExecutionContext,
   *  but the post-transfer path runs a real transaction, and those only make progress while
   *  waitForTransactionsToComplete is pumping the host.s event queue.
   *
   *  Gives up silently after `rounds`, for the same reason yieldUntil does: the caller's own
   *  assertion reports what went wrong far better than a timeout would. Callers MUST assert.
   */
  private def pumpUntil(condition: => Boolean, rounds: Int = 20): Future[Unit] =
    given ExecutionContext = executionContext
    if condition || rounds == 0 then
      Future.unit
    else
      for
        _ <- yieldUntil(condition, 20)
        _ <- waitForTransactionsToComplete()
        _ <- pumpUntil(condition, rounds - 1)
      yield ()

  atest("a transfer arriving on a tombstoned destination is discarded, not loaded"):
    given ExecutionContext = executionContext

    // This host owns the transfer's destination, which the operator is about to declare dead.
    val hostRoot = newHostDir()
    val deviceDir = writeDevice(hostRoot, "dev0", net.secondDeviceId)
    val transfers = new StagedTransferService
    val host = newHost(hostRoot, transferService = transfers)

    val sourceId = StorageDeviceId.BootstrapStorageDeviceId
    val movingStore = StoreId(PoolId.BootstrapPoolId, 1)

    // Where Host promotes the staged copy once the bytes "arrive": an unpacked store directory
    // with the transfer marker removed, so tryLoadStore would happily load it. It appears only
    // on completion, which keeps it out of the construction scan.
    val arrived = deviceDir.resolve(movingStore.directoryName)

    for
      _ <- net.createSecondDevice()
      _ <- waitForTransactionsToComplete()

      // Source goes TransferringOut, destination TransferringIn, pool still names the source.
      _ <- client.transferStore(movingStore, net.secondDeviceId)
      _ <- waitForTransactionsToComplete()

      // The destination host's own poll is what starts the receiving half. Driving it through
      // the device check rather than calling startStoreTransferIn directly is the point of the
      // test: the round-3 fix was tested through a seam that skipped this path entirely.
      _ = host.testingOnlyCheckAllDevices()
      _ <- pumpUntil(transfers.startedTransfersIn.contains(movingStore))
      _ = transfers.startedTransfersIn should contain(movingStore)

      // The operator declares the destination dead while the bytes are still moving.
      _ <- tombstoneDevice(net.secondDeviceId)
      _ <- waitForTransactionsToComplete()

      // Everything captured from here on belongs to the completion path.
      _ = net.takeCapturedHostMessages()
      _ = transfers.finishTransferIn(movingStore)

      // Not awaited: the pre-fix hazard in this area is a Future that never completes, and
      // awaiting one would hang the suite rather than fail it.
      _ <- pumpUntil(host.transferOutcomes.contains(movingStore))
      _ <- pumpUntil(!Files.exists(arrived))

      nudges = net.takeCapturedHostMessages().collect:
        case m: CheckStorageDevice => m

      source <- client.getStorageDeviceState(sourceId)
      poolState <- client.getStoragePoolState(PoolId.BootstrapPoolId)
    yield
      // pumpUntil gives up silently, so assert what it waited on.
      host.transferOutcomes.get(movingStore) should be(
        Some(Host.TransferOutcome.SourceRestored))

      // The metadata half, from round 3: the pool keeps naming the source and the source's
      // entry goes back to Active.
      poolState.stores(movingStore.poolIndex).storageDeviceId should be(sourceId)
      source.stores(movingStore).status should be(StorageDeviceState.StoreStatus.Active)

      // The disk half. Loading the arrived copy would put a live store on a device the metadata
      // has just decided must not own it, with nothing to reclaim it: the deleted-stores pass
      // only considers offlineStores, and reconcileDeviceState ignores a failed device outright.
      // Same host as the source and it would replace the source's entry in `stores`, silently
      // discarding every update applied to it.
      host.loadStoreByIdRequests.toList should be(Nil)

      // And the directory goes too, or the next restart's scan loads exactly what this refused.
      Files.exists(arrived) should be(false)
      // Gone from staging as well, so the absence above is the promoted copy being discarded
      // rather than a copy that was never moved into place.
      Files.exists(deviceDir.resolve(StoreTransfer.StagingDirectory).resolve(movingStore.directoryName)) should be(false)

      // This runs on the destination host, so the source is a different Host and reads
      // nothing of the restore above. Its copy stays offline behind the transfer-out marker
      // while the pool names it, so without the nudge the slice answers nothing until that
      // host's next poll -- up to Main.CheckStorageDevicesPeriod later.
      nudges should contain(
        CheckStorageDevice(HostId.BootstrapHostId, client.clientId, sourceId))

  atest("a transfer arriving on a healthy destination is loaded"):
    given ExecutionContext = executionContext

    // The other half of the same decision. Without this, a fix that simply never loaded an
    // arrived store would pass the test above.
    val hostRoot = newHostDir()
    val deviceDir = writeDevice(hostRoot, "dev0", net.secondDeviceId)
    val transfers = new StagedTransferService
    val host = newHost(hostRoot, transferService = transfers)

    val movingStore = StoreId(PoolId.BootstrapPoolId, 1)
    val arrived = deviceDir.resolve(movingStore.directoryName)

    for
      _ <- net.createSecondDevice()
      _ <- waitForTransactionsToComplete()
      _ <- client.transferStore(movingStore, net.secondDeviceId)
      _ <- waitForTransactionsToComplete()

      _ = host.testingOnlyCheckAllDevices()
      _ <- pumpUntil(transfers.startedTransfersIn.contains(movingStore))
      _ = transfers.startedTransfersIn should contain(movingStore)

      _ = transfers.finishTransferIn(movingStore)
      _ <- pumpUntil(host.transferOutcomes.contains(movingStore))

      poolState <- client.getStoragePoolState(PoolId.BootstrapPoolId)
    yield
      host.transferOutcomes.get(movingStore) should be(
        Some(Host.TransferOutcome.PoolRepointed))

      poolState.stores(movingStore.poolIndex).storageDeviceId should be(net.secondDeviceId)
      host.loadStoreByIdRequests.toList should be(List((net.secondDeviceId, movingStore)))
      Files.exists(arrived) should be(true)

  atest("a committed transfer broadcasts StoreMoved with the new host and the pool config's timestamp"):
    given ExecutionContext = executionContext

    val hostRoot = newHostDir()
    writeDevice(hostRoot, "dev0", net.secondDeviceId)
    val transfers = new StagedTransferService
    val host = newHost(hostRoot, transferService = transfers)

    val movingStore = StoreId(PoolId.BootstrapPoolId, 1)

    for
      _ <- net.createSecondDevice()
      _ <- waitForTransactionsToComplete()
      _ <- client.transferStore(movingStore, net.secondDeviceId)
      _ <- waitForTransactionsToComplete()

      _ = host.testingOnlyCheckAllDevices()
      _ <- pumpUntil(transfers.startedTransfersIn.contains(movingStore))
      _ = net.takeCapturedGossip()
      _ = transfers.finishTransferIn(movingStore)
      _ <- pumpUntil(host.transferOutcomes.contains(movingStore))
      _ <- pumpUntil(net.capturedGossip.exists:
        case m: StoreMoved => m.storeId == movingStore
        case _ => false)
      (poolState, poolTimestamp) <- client.getStoragePoolStateAndTimestamp(movingStore.poolId)
    yield
      host.transferOutcomes.get(movingStore) should be(Some(Host.TransferOutcome.PoolRepointed))
      poolState.stores(movingStore.poolIndex).hostId should be(host.hostId)
      val gossip = net.takeCapturedGossip()
      gossip should have size 1
      gossip.collect { case m: StoreMoved => (m.storeId, m.hostId, m.timestamp) } should be(
        List((movingStore, host.hostId, poolTimestamp)))

  /** A host owning the transfer destination, with the receiving half of a transfer of
   *  `movingStore` already begun through the device check, exactly as the two tests above
   *  drive it. */
  private case class TransferInStarted(hostRoot: Path,
                                       deviceDir: Path,
                                       host: RecordingHost,
                                       transfers: StagedTransferService,
                                       movingStore: StoreId)

  private def startTransferIn(): Future[TransferInStarted] =
    given ExecutionContext = executionContext

    val hostRoot = newHostDir()
    val deviceDir = writeDevice(hostRoot, "dev0", net.secondDeviceId)
    val transfers = new StagedTransferService
    val host = newHost(hostRoot, transferService = transfers)
    val movingStore = StoreId(PoolId.BootstrapPoolId, 1)

    for
      _ <- net.createSecondDevice()
      _ <- waitForTransactionsToComplete()
      _ <- client.transferStore(movingStore, net.secondDeviceId)
      _ <- waitForTransactionsToComplete()

      _ = host.testingOnlyCheckAllDevices()
      _ <- pumpUntil(startTransferSent(transfers, movingStore, 1))
    yield
      // pumpUntil gives up silently, so assert what it waited on.
      startTransferSent(transfers, movingStore, 1) should be(true)
      TransferInStarted(hostRoot, deviceDir, host, transfers, movingStore)

  /** True once the `attempt`th beginTransferIn for `sid` has been made and the StartStoreTransfer
   *  for it captured. The service records the call before Host sends the message, so waiting on
   *  the call alone would let a test drain the captured messages before the send. Peeks rather
   *  than drains. */
  private def startTransferSent(transfers: StagedTransferService, sid: StoreId, attempt: Int): Boolean =
    val attempts = transfers.transfersIn.filter(_._1 == sid)
    attempts.size >= attempt && net.synchronized(net.capturedHostMessages).exists:
      case m: StartStoreTransfer => m.storeId == sid && m.transferUUID == attempts(attempt - 1)._2
      case _ => false

  /** Drains the captured host messages and returns the StartStoreTransfers for `sid`. */
  private def takeStartTransfers(sid: StoreId): List[StartStoreTransfer] =
    net.takeCapturedHostMessages().collect:
      case m: StartStoreTransfer if m.storeId == sid => m

  atest("transfer in stages under the transferring-in directory"):
    given ExecutionContext = executionContext

    startTransferIn().map: t =>
      t.transfers.transfersIn.map(_._3) should be(
        Vector(os.Path(t.deviceDir) / "transferring-in"))

  atest("successful transfer in promotes the staged store and drops the marker"):
    given ExecutionContext = executionContext

    for
      t <- startTransferIn()
      _ = t.transfers.finishTransferIn(t.movingStore)
      _ <- pumpUntil(t.host.transferOutcomes.contains(t.movingStore))
    yield
      val dirName = t.movingStore.directoryName
      val promoted = os.Path(t.deviceDir) / dirName

      t.host.transferOutcomes.get(t.movingStore) should be(
        Some(Host.TransferOutcome.PoolRepointed))
      os.exists(promoted) should be(true)
      // Left in place, either marker would keep the promoted store offline on every restart.
      os.exists(promoted / "transferring-out") should be(false)
      os.exists(promoted / "transferred-in") should be(false)
      os.exists(os.Path(t.deviceDir) / "transferring-in" / dirName) should be(false)

  atest("StartStoreTransfer carries the UUID registered with the service"):
    given ExecutionContext = executionContext

    startTransferIn().map: t =>
      val sent = takeStartTransfers(t.movingStore)
      val List((_, registered, _)) = t.transfers.transfersIn.toList: @unchecked

      sent should not be empty
      sent.map(_.transferUUID).toSet should be(Set(registered))

  atest("failed transfer in restarts with a fresh UUID"):
    given ExecutionContext = executionContext

    for
      t <- startTransferIn()
      first = takeStartTransfers(t.movingStore).last
      _ = t.transfers.failTransferIn(t.movingStore)
      // The restart waits out a delay that never elapses under NoBackgroundTaskManager, so fire it
      _ <- pumpUntil(t.host.testingOnlyScheduledTransferInRetries.contains(t.movingStore))
      _ = t.host.testingOnlyRetryTransferIns()
      _ <- pumpUntil(startTransferSent(t.transfers, t.movingStore, 2))
    yield
      val attempts = t.transfers.transfersIn
      attempts.size should be(2)
      val retryUUID = attempts(1)._2
      retryUUID should not be attempts(0)._2
      first.transferUUID should be(attempts(0)._2)

      val newest = takeStartTransfers(t.movingStore).last
      newest.transferUUID should be(retryUUID)
      newest.timestamp > first.timestamp should be(true)

      // The retry's message replaced the failed attempt's in the retransmit set, so the old
      // UUID is never sent again.
      t.host.retransmitPendingStartTransfers(force = true)
      takeStartTransfers(t.movingStore).map(_.transferUUID) should be(List(retryUUID))

  atest("retransmit stops once the transfer in completes"):
    given ExecutionContext = executionContext

    for
      t <- startTransferIn()
      _ = takeStartTransfers(t.movingStore)

      // Pins the forced retransmit itself, so the empty result below is not vacuous.
      _ = t.host.retransmitPendingStartTransfers(force = true)
      beforeCompletion = takeStartTransfers(t.movingStore)

      _ = t.transfers.finishTransferIn(t.movingStore)
      _ <- pumpUntil(t.host.transferOutcomes.contains(t.movingStore))
      _ = takeStartTransfers(t.movingStore)
      _ = t.host.retransmitPendingStartTransfers(force = true)
    yield
      beforeCompletion.size should be(1)
      t.host.transferOutcomes.contains(t.movingStore) should be(true)
      takeStartTransfers(t.movingStore) should be(Nil)

  atest("failed transfer in waits before restarting"):
    given ExecutionContext = executionContext

    for
      t <- startTransferIn()
      _ = takeStartTransfers(t.movingStore)
      _ = t.transfers.failTransferIn(t.movingStore)
      _ <- pumpUntil(startTransferSent(t.transfers, t.movingStore, 2), rounds = 3)

      // A device check during the wait must not start a parallel attempt either
      _ = t.host.testingOnlyCheckAllDevices()
      _ <- pumpUntil(startTransferSent(t.transfers, t.movingStore, 2), rounds = 3)
      _ = t.host.retransmitPendingStartTransfers(force = true)
    yield
      t.transfers.transfersIn.size should be(1)
      t.host.testingOnlyTransferringIn should contain(t.movingStore)
      t.host.testingOnlyScheduledTransferInRetries should contain(t.movingStore)
      // The failed attempt's request is no longer retransmitted while the retry waits
      takeStartTransfers(t.movingStore) should be(Nil)

  atest("transfer in stays registered until the metadata outcome is handled"):
    given ExecutionContext = executionContext

    for
      t <- startTransferIn()
      gate = t.host.holdTransferOutcomes()
      promoted = os.Path(t.deviceDir) / t.movingStore.directoryName
      _ = t.transfers.finishTransferIn(t.movingStore)
      _ <- pumpUntil(os.exists(promoted))
      heldTransferringIn = t.host.testingOnlyTransferringIn

      // With the outcome still pending, a device check must neither start a second attempt nor
      // (once promoted) report the arrived copy as a conflicting store directory
      _ = t.host.testingOnlyCheckAllDevices()
      _ <- pumpUntil(t.transfers.transfersIn.size > 1, rounds = 3)
      attemptsWhileHeld = t.transfers.transfersIn.size

      _ = gate.success(())
      _ <- pumpUntil(t.host.transferOutcomes.contains(t.movingStore) &&
                     !t.host.testingOnlyTransferringIn.contains(t.movingStore))
    yield
      os.exists(promoted) should be(true)
      heldTransferringIn should contain(t.movingStore)
      attemptsWhileHeld should be(1)
      t.host.transferOutcomes.get(t.movingStore) should be(Some(Host.TransferOutcome.PoolRepointed))
      t.host.loadStoreByIdRequests.toList should be(List((net.secondDeviceId, t.movingStore)))
      t.host.testingOnlyTransferringIn should not contain t.movingStore

  /** Removes `sid`'s entry from `deviceId` in the storage-devices tree, as an abandon running
   *  elsewhere would, without this host's own abandon path. */
  private def removeDeviceEntry(deviceId: StorageDeviceId, sid: StoreId): Future[Unit] =
    given ExecutionContext = executionContext
    client.transactUntilSuccessful: tx =>
      for
        ptr <- client.getStorageDevicePointer(deviceId)
        kvos <- client.read(ptr)
      yield
        tx.update(ptr, None, None,
          List(KeyRevision(StorageDeviceState.StateKey,
            kvos.contents(StorageDeviceState.StateKey).revision)),
          List(Insert(StorageDeviceState.StateKey, StorageDeviceState(kvos).removeStore(sid).encode())))

  atest("an abandoned transfer in is cancelled and no longer retransmitted"):
    given ExecutionContext = executionContext

    for
      t <- startTransferIn()
      List((_, registered, _)) = t.transfers.transfersIn.toList: @unchecked

      // The source dies mid-transfer; the destination's next device check abandons the entry.
      _ <- tombstoneDevice(StorageDeviceId.BootstrapStorageDeviceId)
      _ <- waitForTransactionsToComplete()
      _ = t.host.testingOnlyCheckAllDevices()
      _ <- pumpUntil(!t.host.testingOnlyTransferringIn.contains(t.movingStore))
      // The cancellation fails the attempt's future, which must not schedule a restart.
      _ <- pumpUntil(t.host.testingOnlyScheduledTransferInRetries.nonEmpty, rounds = 3)

      _ = takeStartTransfers(t.movingStore)
      _ = t.host.retransmitPendingStartTransfers(force = true)
      dest <- client.getStorageDeviceState(net.secondDeviceId)
    yield
      dest.stores.contains(t.movingStore) should be(false)
      t.host.testingOnlyTransferringIn should not contain t.movingStore
      t.transfers.cancelledTransfersIn should be(Vector((t.movingStore, registered)))
      t.host.testingOnlyScheduledTransferInRetries should be(Set())
      takeStartTransfers(t.movingStore) should be(Nil)

  atest("an abandon during the retry delay cancels the retry and the staged copy"):
    given ExecutionContext = executionContext

    for
      t <- startTransferIn()
      _ = t.transfers.failTransferIn(t.movingStore)
      _ <- pumpUntil(t.host.testingOnlyScheduledTransferInRetries.contains(t.movingStore))
      // What a failed promotion leaves behind for the retry to replace.
      staged = os.Path(t.deviceDir) / StoreTransfer.StagingDirectory / t.movingStore.directoryName
      _ = os.makeDir.all(staged)

      _ <- tombstoneDevice(StorageDeviceId.BootstrapStorageDeviceId)
      _ <- waitForTransactionsToComplete()
      _ = t.host.testingOnlyCheckAllDevices()
      _ <- pumpUntil(!t.host.testingOnlyTransferringIn.contains(t.movingStore))

      _ = t.host.testingOnlyRetryTransferIns()
      _ <- pumpUntil(t.transfers.transfersIn.size > 1, rounds = 3)
      _ = takeStartTransfers(t.movingStore)
      _ = t.host.retransmitPendingStartTransfers(force = true)
    yield
      t.host.testingOnlyTransferringIn should not contain t.movingStore
      t.host.testingOnlyScheduledTransferInRetries should be(Set())
      t.transfers.transfersIn.size should be(1)
      os.exists(staged) should be(false)
      takeStartTransfers(t.movingStore) should be(Nil)

  atest("a transfer-in retry gives up once the destination no longer records the transfer"):
    given ExecutionContext = executionContext

    for
      t <- startTransferIn()
      _ = t.transfers.failTransferIn(t.movingStore)
      _ <- pumpUntil(t.host.testingOnlyScheduledTransferInRetries.contains(t.movingStore))

      // Removed behind this host's back, so only the retry's own re-read can notice.
      _ <- removeDeviceEntry(net.secondDeviceId, t.movingStore)
      _ <- waitForTransactionsToComplete()
      _ = takeStartTransfers(t.movingStore)
      _ = t.host.testingOnlyRetryTransferIns()
      _ <- pumpUntil(!t.host.testingOnlyTransferringIn.contains(t.movingStore))
    yield
      t.host.testingOnlyTransferringIn should not contain t.movingStore
      t.host.testingOnlyScheduledTransferInRetries should be(Set())
      t.transfers.transfersIn.size should be(1)
      takeStartTransfers(t.movingStore) should be(Nil)

  atest("a stale transfer staging directory is removed when its device loads"):
    val hostRoot = newHostDir()
    val deviceDir = writeDevice(hostRoot, "dev0", deviceA)
    val staged = deviceDir.resolve(StoreTransfer.StagingDirectory).resolve(storeId.directoryName)
    Files.createDirectories(staged)
    Files.write(staged.resolve("partial"), Array[Byte](1, 2, 3))

    val host = newHost(hostRoot)

    Future.successful:
      host.loadedDevices.keySet should be(Set(deviceA))
      Files.exists(deviceDir.resolve(StoreTransfer.StagingDirectory)) should be(false)

  atest("a destination crash after promotion resumes the pool update instead of loading the copy"):
    given ExecutionContext = executionContext

    for
      t <- startTransferIn()
      // Holding the metadata step leaves the first host exactly where a crash after promotion
      // would: the copy is at its final path and the pool still names the source. Crash-only, so
      // the "restart" is simply a fresh Host over the same root; the held one never proceeds.
      _ = t.host.holdTransferOutcomes()
      promoted = os.Path(t.deviceDir) / t.movingStore.directoryName
      _ = t.transfers.finishTransferIn(t.movingStore)
      _ <- pumpUntil(os.exists(promoted))
      markedAtCrash = os.exists(promoted / StoreTransfer.TransferredInMarkerFile)
      poolAtCrash <- client.getStoragePoolState(PoolId.BootstrapPoolId)

      transfers2 = new StagedTransferService
      host2 = newHost(t.hostRoot, transferService = transfers2)
      _ = host2.testingOnlyCheckAllDevices()
      _ <- pumpUntil(host2.transferOutcomes.contains(t.movingStore) &&
                     !host2.testingOnlyTransferringIn.contains(t.movingStore))

      poolState <- client.getStoragePoolState(PoolId.BootstrapPoolId)
    yield
      markedAtCrash should be(true)
      poolAtCrash.stores(t.movingStore.poolIndex).storageDeviceId should not be net.secondDeviceId

      // The device check resumed the metadata step rather than refusing the existing directory
      // or shipping the store again.
      transfers2.transfersIn should be(Vector())
      host2.transferOutcomes.get(t.movingStore) should be(Some(Host.TransferOutcome.PoolRepointed))
      poolState.stores(t.movingStore.poolIndex).storageDeviceId should be(net.secondDeviceId)

      // Loaded only once the pool names this device, and the marker goes with it.
      host2.loadStoreByIdRequests.toList should be(List((net.secondDeviceId, t.movingStore)))
      os.exists(promoted / StoreTransfer.TransferredInMarkerFile) should be(false)

  atest("a store carrying the transferred-in marker is not loaded at startup"):
    // A real Host rather than RecordingHost, whose tryLoadStore records every candidate and
    // honours no marker.
    val hostRoot = newHostDir()
    val deviceDir = writeDevice(hostRoot, "dev0", deviceA)
    val storeDir = deviceDir.resolve(storeId.directoryName)
    Files.createDirectories(storeDir)
    Files.write(storeDir.resolve(StoreConfig.configFilename),
                StoreConfig(storeId, StoreConfig.RocksDB()).yamlConfig.getBytes(StandardCharsets.UTF_8))
    Files.write(storeDir.resolve(StoreTransfer.TransferredInMarkerFile), Array.emptyByteArray)

    val host = new Host(client, HostId.BootstrapHostId, systemId, hostRoot, executionContext,
      net.objectCacheFactory, net, BackgroundTaskManager.NoBackgroundTaskManager, TestNetwork.TestCRL,
      net.FinalizerFactory, TransactionDriver.noErrorRecoveryFactory, Duration(5, SECONDS),
      Duration(60, SECONDS), StoreTransferService.Disabled, StoreRebuildFactory.Filesystem, 2)

    Future.successful:
      host.testingOnlyOfflineStores should contain(storeId)
      host.getStoreIds should not contain storeId
      // Nothing was opened in the copy's directory.
      Files.list(storeDir).toArray.length should be(2)

  atest("an Active store still carrying the transferred-in marker is loaded; a TransferringIn one is not"):
    val hostRoot = newHostDir()
    val deviceDir = writeDevice(hostRoot, "dev0", deviceA)
    val host = newHost(hostRoot)

    // Written after construction and marked offline by hand, as stageOfflineTransferringOutStore
    // does, because the recording tryLoadStore neither honours the marker nor keeps the sets.
    val List(activeDir, pendingDir) = List(storeId, storeIdB).map: sid =>
      val dir = deviceDir.resolve(sid.directoryName)
      Files.createDirectories(dir)
      Files.write(dir.resolve(StoreTransfer.TransferredInMarkerFile), Array.emptyByteArray)
      dir
    val local = host.loadedDevices(deviceA)
    local.offlineStores = local.offlineStores ++ Set(storeId, storeIdB)
    val before = host.storeLoadAttempts.size

    val armed = host.armLookup(deviceA)
    host.testingOnlyCheckAllDevices()

    // Active: the pool update committed and the destination crashed before removing the marker.
    // TransferringIn with no source named keeps the other out of startStoreTransferIn, so it
    // isolates the guard: the pool update for it has not committed.
    armed.success(deviceState(
      deviceA,
      Map(storeId -> StorageDeviceState.StoreEntry(StorageDeviceState.StoreStatus.Active, None),
          storeIdB -> StorageDeviceState.StoreEntry(StorageDeviceState.StoreStatus.TransferringIn, None))))

    yieldUntil(!host.testingOnlyActiveDeviceChecks.contains(deviceA)).map: _ =>
      host.testingOnlyActiveDeviceChecks should not contain deviceA

      Files.exists(activeDir.resolve(StoreTransfer.TransferredInMarkerFile)) should be(false)
      Files.exists(pendingDir.resolve(StoreTransfer.TransferredInMarkerFile)) should be(true)
      host.storeLoadAttempts.toList.drop(before) should be(List((deviceA, activeDir)))
