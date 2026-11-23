package org.aspen_ddp.aspen.server.host

import org.aspen_ddp.aspen.common.HLCTimestamp
import org.aspen_ddp.aspen.common.metadata.{StorageDeviceId, StorageDeviceState}
import org.aspen_ddp.aspen.common.network.CheckStorageDevice
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.server.{Host, StoreConfig}
import scribe.Logging

import scala.concurrent.Future
import scala.util.{Failure, Success}
import scala.util.control.NonFatal

/** Runs only on the control thread. See HostThreading.scala.
 *
 *  Reconciles each loaded device against the state recorded for it in the storage-devices tree, and owns the
 *  bookkeeping that keeps one lookup per device in flight.
 *
 *  Metadata reads go through `ctx.metadata`; the load of a store goes through the adapter on `ctx`, so the seams on
 *  Host that tests replace still take effect. */
class DeviceReconciler(ctx: DeviceControl) extends Logging:
  import ctx.given

  private var activeChecks: Set[StorageDeviceId] = Set()
  // Devices with a re-check waiting out TransferOutRecheckDelay. At most one each.
  private var scheduledRechecks: Set[StorageDeviceId] = Set()
  private var deferredChecks: Set[StorageDeviceId] = Set()
  // Stores whose leftover device-CRL entries are being dropped (dropLeftoverCrlState)
  private var crlDropsRunning: Set[StoreId] = Set()

  private[server] def activeDeviceChecks: Set[StorageDeviceId] = activeChecks

  private[server] def leftoverCrlDropsRunning: Set[StoreId] = crlDropsRunning

  private[server] def pendingDeleteLookupsInFlight: Set[os.Path] = ctx.copies.releaseLookupsInFlight

  private[server] def scheduledDeviceRechecks: Set[StorageDeviceId] = scheduledRechecks

  private[server] def deferredDeviceChecks: Set[StorageDeviceId] = deferredChecks

  /** The CheckAllDevices event: discovers new devices, then checks every loaded one. */
  def checkAll(): Unit =
    // Discover first so a device found by this scan is checked within the same event
    // and its Initializing stores are created now rather than a period from now.
    ctx.discovery.checkForNewDevices()
    ctx.devices.devices.valuesIterator.foreach: sds =>
      checkStorageDevice(sds.storageDeviceId)

  /** The ScanDevices event. */
  def scan(): Unit =
    // Local only: listing the directory reads nothing over the network. A device this scan
    // loads is checked at once, as CheckAllDevices would, so its Initializing stores are
    // created now rather than up to checkStorageDevicePeriod from now.
    // The one caller that counts transient CRL open failures (DeviceDiscovery.CrlOpenAttempts)
    ctx.discovery.checkForNewDevices(scan = true).foreach(checkStorageDevice)
    ctx.loader.retryFailedLoads()

  /** A CheckStorageDevice message. */
  def handleCheckMessage(m: CheckStorageDevice): Unit =
    // A device id we do not know may simply be one that has appeared on disk since
    // our last scan -- create-storage-device sends this message as a discovery nudge
    // for exactly that case -- so rescan before concluding the device is not ours. Any
    // other device the rescan loads is checked too, as the periodic scan checks it.
    val loaded =
      if ! ctx.devices.devices.contains(m.deviceId) then ctx.discovery.checkForNewDevices()
      else Nil
    (loaded :+ m.deviceId).distinct.foreach(checkStorageDevice)

  /** Drops the device-CRL entries of every store the device no longer holds: one its entry does not name, with no
   *  directory on the device, not loaded and not being retired. Retiring a copy (StoreCopies.retire) drops them
   *  before the copy leaves the store's path, so this is a safety net: for a directory removed by hand, or for
   *  entries a crash left behind when the drop came after the directory went. Left there, they would be copied
   *  forward forever, uncounted by admission, and recovered if the store ever came back. The CRL is what is read, not
   *  a list kept in memory, so a restart forgets nothing. A store that comes back before they are dropped has its
   *  entry, so they are not dropped here; it arrives with its crl_save.log, and its load drops them before saving the
   *  file (StoreLoader.awaitCrlLoad).
   *
   *  A store its entry still names is never a leftover, even one declared missing and being rebuilt: its directory
   *  may come back (a stat that failed, a restore or mount finishing late) before its Rebuilding flip commits, or
   *  after it and before the first reconcile retires it, and a copy loaded without its entries would answer for
   *  transactions whose state and locks it lost. Its entries go when its rebuilt copy first
   *  loads (an empty crl_save.log), when a retire saves them into the copy, or here once a re-placement takes its
   *  entry off the device. A directory whose existence a stat cannot tell (FileStat) is taken as present.
   *
   *  The directory and loaded checks run when the CRL's answer arrives, in the same control task that starts the
   *  drop: a store that lands on the device has its directory before anything saves entries for it. A failed drop is
   *  retried by the device's next reconcile.
   *
   *  The drop releases only the admission charge retained for the store when it was issued, and only if it is still
   *  unchanged when the drop completes (CrlAdmission.releaseRetained(storeId, charge)). A store landing meanwhile
   *  charges its crl_save.log state under the same id (CrlAdmission.tryChargeLoaded) for records saved behind the drop,
   *  which the drop does not remove; its recovery releases the whole charge once it has counted them, so the records
   *  stay counted throughout (crash-recovery-log 6.1). */
  private def dropLeftoverCrlState(local: LocalStorageDeviceState, remote: StorageDeviceState): Unit =
    val deviceId = local.storageDeviceId
    val listed =
      try local.crl.storesWithEntries()
      catch case NonFatal(t) => Future.failed(t)
    listed.onComplete:
      case Failure(t) =>
        logger.error(s"Failed to list the stores in the CRL of storage device $deviceId. Retrying at its next " +
                     s"check. Error: $t")
      case Success(stores) =>
        def leftover(storeId: StoreId): Boolean =
          !remote.stores.contains(storeId) && !ctx.copyRegistry.openCopies(local).contains(storeId) &&
            ctx.copyRegistry.existence(local, storeId).contains(false) &&
            !crlDropsRunning.contains(storeId) &&
            !ctx.copies.isRetiring(storeId)
        // Only while the device is still loaded: an unloaded one's CRL is shut down
        if ctx.devices.isLoaded(local) then
          stores.filter(leftover).toList.sortBy(s => (s.poolId.uuid, s.poolIndex)).foreach: storeId =>
            logger.info(s"Dropping the device-CRL entries of store $storeId, which storage device $deviceId no " +
                        s"longer holds")
            crlDropsRunning += storeId
            // Read with the decision: only this charge covers the entries the drop removes
            val charge = local.admission.retainedCharge(storeId)
            val dropped =
              try local.crl.dropStore(storeId)
              catch case NonFatal(t) => Future.failed(t)
            dropped.onComplete: result =>
              crlDropsRunning -= storeId
              if result.isSuccess then
                charge.foreach(local.admission.releaseRetained(storeId, _))
              result.failed.foreach: t =>
                logger.error(s"Failed to drop the device-CRL entries of store $storeId, which storage device " +
                             s"$deviceId no longer holds. Retrying at the device's next check. Error: $t")

  /** Reconciles a loaded device's on-disk state against the state recorded for it in the
   *  storage-devices tree.
   *
   *  If the tree records a different host, nothing else happens and the device is relinquished
   *  (DeviceShutdown.relinquishDevice): another host has adopted it, normally because its drive
   *  was pulled from this running host and plugged in there. Reconciling against another host's
   *  records could delete or create stores that are not ours, and serving them would compete
   *  with the host that now does.
   *  A device the tree records as failed is retired (DeviceShutdown.retireFailedDevice): marked so it
   *  is never loaded again, and unloaded. Its stores are being rebuilt elsewhere.
   *  A device the tree records as ShuttingDown (or Shutdown) is handed to shutDownDevice instead
   *  of the store decisions, so nothing new starts on a disk about to be shelved.
   *
   *  An Operational device of this host has its leftover device-CRL entries dropped, then each of its stores decided
   *  in turn (reconcileStores: CopyDecision.decide over the store's inputs, read fresh at its turn, and its actions
   *  carried out in order before the next store's inputs are read), the staging of rebuilds no longer listed swept,
   *  and its pending-delete copies released.
   */
  private def reconcileDeviceState(local: LocalStorageDeviceState,
                                   remote: StorageDeviceState): Unit =
    local.reconciledEntries = Some(remote.stores)
    // Copies whose move was not durable (CopyMoves) are forced again first, so the decisions
    // see them synced, and copies whose move to lost/ failed are moved (StoreRebuilds.moveToLost)
    ctx.moves.resync(local)
    ctx.rebuilds.retryPendingLost(local)
    if remote.isFailed then
      // An operator has declared this device dead. Its directory can still mount -- a dead
      // controller rather than a dead platter -- and its zeroed host id must not be mistaken for
      // another owner by the branch below. Its stores are being rebuilt elsewhere, so it stops
      // serving them, for good
      ctx.shutdown.retireFailedDevice(local, remote)
    else if remote.hostId != ctx.hostId then
      ctx.shutdown.relinquishDevice(local, remote)
    else if remote.status == StorageDeviceState.DeviceStatus.ShuttingDown ||
            remote.status == StorageDeviceState.DeviceStatus.Shutdown ||
            ctx.shutdown.isShuttingDown(local.storageDeviceId) then
      // None of the store decisions run: each would start work on a device about to be shelved.
      // A store transferred away meanwhile keeps its directory until the device is next
      // reconciled while Operational, here or on an adopting host.
      //
      // That includes an Operational read while a shutdown is still in flight -- the restart's
      // own nudge, typically. The in-flight sequence ends in Cancelled and checks the device
      // again once its own work has stopped, and that check runs the decisions. shutDownDevice
      // itself does nothing then, as the guard is held.
      ctx.shutdown.shutDownDevice(local, remote)
    else
      // Any entries a store transferred away left in the device CRL go
      dropLeftoverCrlState(local, remote)
      // A store a cancelled shutdown closed, or whose load an unsynced move held back, is loaded by its decision
      // (CopyDecision.Load)
      reconcileStores(local, remote)
      ctx.rebuilds.sweepUnlistedStaging(local, remote)
      // Pending deletes: retired copies whose store is Active on its owner device, and the remains of interrupted
      // deletes
      ctx.copies.release(local, remote)

  /** Decides each store of `local` in turn, in (pool, index) order (stage 3, D2): every store its entries name, that
   *  has a copy or work on the device, or whose transfer in the host tracks (to any device). Each store's inputs are
   *  read at its turn, after the actions of the stores before it, and its actions run in decide's order.
   *
   *  The Missing Stores declaration (local.declaredMissing) is read from its value before the loop and set, after it,
   *  to exactly the stores given DeclareMissing: a declared store stays declared while its entry is Active here and
   *  it is not served. The record is in memory only: a restart's device load serves a store whose directory is back,
   *  with its kept CRL entries. */
  private def reconcileStores(local: LocalStorageDeviceState, remote: StorageDeviceState): Unit =
    val declaredBefore = local.declaredMissing
    // Read once: a store is in either set only by Rebuild work of its own, which only its own actions change, and each
    // store has one turn, so the sets read now are what each store's turn would read
    val rebuildReads = ctx.copyRegistry.rebuilding(HostCopies.RebuildReads)
    val rebuildHolds = ctx.copyRegistry.rebuilding(HostCopies.RebuildHolds)
    var declared = Set.empty[StoreId]
    val copies = ctx.copyRegistry.copiesOf(local)
    val stores = remote.stores.keySet ++
      copies.map(_.copies.keySet).getOrElse(Set()) ++
      copies.map(_.work.keySet).getOrElse(Set()) ++
      ctx.transfers.pendingStartTransfers.keySet ++
      ctx.transfers.transferInRetries.keySet
    stores.toList.sortBy(s => (s.poolId.uuid, s.poolIndex)).foreach: storeId =>
      CopyDecision.decide(storeInputs(local, remote, storeId, declaredBefore, rebuildReads, rebuildHolds)).foreach:
        action =>
          action match
            case CopyDecision.Action.DeclareMissing(_) => declared += storeId
            case _ => ()
          run(local, storeId, action)
    local.declaredMissing = declared
    ctx.copyRegistry.missingIs(local)

  /** The inputs of `storeId`'s decision on `local`, read now (CopyDecision.StoreFacts names each source).
   *  `declaredBefore`: the stores declared missing before this reconcile's loop. `rebuildReads`, `rebuildHolds`:
   *  `rebuilding(RebuildReads)` and `rebuilding(RebuildHolds)`, read before the loop */
  private def storeInputs(local: LocalStorageDeviceState, remote: StorageDeviceState, storeId: StoreId,
                          declaredBefore: Set[StoreId], rebuildReads: Set[StoreId],
                          rebuildHolds: Set[StoreId]): CopyDecision.StoreInputs =
    val registry = ctx.copyRegistry
    val copies = registry.copiesOf(local)
    val copy = copies.flatMap(_.copies.get(storeId)).map: r =>
      CopyDecision.CopyState(r.phase, r.unsynced, CopyDecision.ActivityKind.of(r.activity),
        ctx.moves.isPendingLost(local, storeId))
    val tracked =
      ctx.transfers.pendingStartTransfers.get(storeId).map(p => CopyDecision.TrackedIn(p.msg.fromDevice, p.toDeviceId))
        .toList ++
      ctx.transfers.transferInRetries.get(storeId).map(r => CopyDecision.TrackedIn(r.fromDeviceId, r.toDeviceId))
        .toList
    val facts = CopyDecision.StoreFacts(
      servedOnHost = ctx.router.get(storeId).isDefined,
      openingOnHost = ctx.loader.isOpening(storeId),
      retiringOnHost = ctx.copies.isRetiring(storeId),
      sending = ctx.storeTransferService.isTransferringOut(storeId),
      receivingOnHost = ctx.transfers.receiving.contains(storeId),
      rebuildReadsOnHost = rebuildReads.contains(storeId),
      rebuildHoldsOnHost = rebuildHolds.contains(storeId),
      rebuildHandle = ctx.rebuilds.hasRunningRebuild(storeId),
      dirPresent = registry.existence(local, storeId),
      configPresent = registry.existence(local, storeId, Some(StoreConfig.configFilename)),
      declaredMissing = declaredBefore.contains(storeId),
      tracked = tracked)
    CopyDecision.StoreInputs(local.storageDeviceId, storeId, remote.stores.get(storeId), copy,
      copies.flatMap(_.work.get(storeId)), facts)

  /** Carries out one of `storeId`'s actions on `local` (CopyDecision.decide has the rule for each) */
  private def run(local: LocalStorageDeviceState, storeId: StoreId, action: CopyDecision.Action): Unit =
    import CopyDecision.Action.*
    val deviceId = local.storageDeviceId
    action match
      case EndQueuedRebuild =>
        ctx.rebuilds.endQueuedRebuild(local, storeId)
      case TakeOutOfService =>
        ctx.rebuilds.retireUnlistedServed(local, storeId)
      case ForgetGoneCopy =>
        ctx.copyRegistry.removed("retireUnowned: no directory", local, storeId)
      case Retire =>
        // Its copy's record, which keeps the store silent, goes only once its path is clear (the retire's move into
        // pending-delete), so a failed retire is retried by the next check. A copy that reaches pending-delete has
        // the device checked again, so it is released without waiting for the periodic check
        logger.info(s"Store $storeId is no longer owned by device $deviceId. Retiring its copy")
        ctx.copies.retire(local, storeId).foreach: clear =>
          if clear then
            checkStorageDevice(deviceId)
      case DeclareMissing(first) =>
        // Silenced before its rebuild is requested
        local.declaredMissing += storeId
        ctx.copyRegistry.missingIs(local)
        if first then
          val where = ctx.copyRegistry.pathOf(local, storeId).map(p => s"$p holds no store")
            .getOrElse("it has no copy there")
          logger.error(s"Store $storeId is Active on storage device $deviceId, but $where. Rebuilding it")
        ctx.rebuilds.requestRebuild(deviceId, storeId)
      case RequestRebuild =>
        ctx.rebuilds.requestRebuild(deviceId, storeId)
      case Recheck =>
        scheduleDeviceRecheck(deviceId)
      case ReinstateOut =>
        ctx.transfersOut.reinstateAbandonedOut(local, storeId)
      case BringArrivedLive =>
        logger.warn(s"Store $storeId on device $deviceId is Active but its copy is still in arrived/. " +
                    s"Completing the transfer and loading the store")
        if ctx.transfers.bringArrivedLive(local, storeId) == MoveResult.Durable then
          ctx.copyRegistry.pathOf(local, storeId).foreach(ctx.tryLoadStore(local, _, None))
      case BringRebuiltLive =>
        logger.warn(s"Store $storeId on device $deviceId is Active but its copy is still in rebuilt/. " +
                    s"Completing the rebuild and loading the store")
        if ctx.rebuilds.bringRebuiltLive(local, storeId) == MoveResult.Durable then
          ctx.copyRegistry.pathOf(local, storeId).foreach(ctx.tryLoadStore(local, _, None))
      case Load =>
        // Its online gossip goes with the load, as a reinstated transfer out's does
        logger.info(s"Loading store $storeId on device $deviceId: its copy is Live, synced and served nowhere")
        ctx.copyRegistry.pathOf(local, storeId).foreach(
          ctx.tryLoadStore(local, _, Some(() => ctx.gossipStoreOnline(storeId))))
      case Create =>
        ctx.rebuilds.createNewStore(local, storeId)
      case DropTransferIn(fromDevice, toDevice) =>
        ctx.transfers.dropAbandonedTransferIn(storeId, fromDevice, toDevice)
      case CancelRebuild =>
        ctx.rebuilds.cancelRebuild(local, storeId)
      case StartTransferIn(fromDevice, transferId) =>
        startTransferIn(local, storeId, fromDevice, transferId)
      case StartRebuild(episode, acceptLoss) =>
        ctx.rebuilds.startRebuild(local, storeId, episode, acceptLoss)

  /** Starts, resumes or abandons the transfer in of `storeId` onto `local` from `fromDeviceId`, its entry there
   *  TransferringIn, once the source's state is read */
  private def startTransferIn(local: LocalStorageDeviceState, storeId: StoreId, fromDeviceId: StorageDeviceId,
                              transferId: Option[java.util.UUID]): Unit =
    ctx.metadata.readTransferSourceState(fromDeviceId).foreach: fromDevice =>
      if fromDevice.isFailed then
        // A tombstoned source has both ids zeroed, so fromDevice.hostId is HostId(0,0).
        // Starting the transfer would address every StartStoreTransfer to a host that
        // does not exist; the messages are dropped, the entry never resolves, and this
        // check re-sends them forever. Nothing reports the stall either: rebalancing's
        // ownedStores filters TransferringIn out of its accounting, so the store is
        // invisible to the rebalancer too.
        ctx.transfers.abandonIn(local.storageDeviceId, storeId, fromDeviceId)
      else if ! fromDevice.isOperational then
        // The source is shutting down or shut down, and its host refuses to send. The
        // transfer stays pending: the next check of this device reads the source again,
        // owner included, so a source adopted by another host is asked there.
        logger.info(s"Not starting the transfer in of store $storeId onto device " +
                    s"${local.storageDeviceId}: source device $fromDeviceId is " +
                    s"${fromDevice.status}. Retrying on a later check")
      else
        // An attempt already asking the source's old host -- the source was shut down and
        // adopted since -- is pointed at the new one and asked again now, rather than at
        // the next retransmit.
        ctx.transfers.pendingStartTransfers.get(storeId).filter(_.msg.fromDevice == fromDeviceId).foreach: pt =>
          if ctx.transfers.readdressPendingTransfer(pt, fromDevice) then
            pt.lastSend = HLCTimestamp.now
            ctx.client.sendHostMessage(pt.msg)
        ctx.transfers.startIn(storeId, fromDevice.hostId, fromDeviceId, local.storageDeviceId, transferId)

  /** Checks `storageDeviceId` again after TransferOutRecheckDelay, so a store left offline in
   *  outbound/ is reinstated soon after an aborted transfer's send ends rather than at
   *  the next periodic check. At most one is scheduled per device; requests made meanwhile are
   *  covered by it.
   */
  def scheduleDeviceRecheck(storageDeviceId: StorageDeviceId): Unit =
    if ! scheduledRechecks.contains(storageDeviceId) then
      scheduledRechecks += storageDeviceId
      ctx.scheduler.schedule(Host.TransferOutRecheckDelay)(runDeviceRecheck(storageDeviceId))

  private[server] def runDeviceRecheck(storageDeviceId: StorageDeviceId): Unit =
    if scheduledRechecks.contains(storageDeviceId) then
      scheduledRechecks -= storageDeviceId
      checkStorageDevice(storageDeviceId)

  /** Requests a check of one storage device.
   *
   *  At most one lookup per device is outstanding at a time. A request arriving while one is
   *  in flight is deferred rather than dropped: dropping it costs a full
   *  checkStorageDevicePeriod -- Main.CheckStorageDevicesPeriod in production, an hour at
   *  present -- which is the same cost as losing the notification message outright. At most
   *  one deferral is held per device, so this still throttles the pile-up of checks that
   *  builds up over an offline period.
   *
   *  One deferral suffices because a check is a reconcile, not a delta: it re-reads the
   *  storage-devices tree and acts on whatever that says, so a single re-run subsumes any
   *  number of requests coalesced into it. That is what makes a Set correct here rather than
   *  lossy.
   */
  def checkStorageDevice(storageDeviceId: StorageDeviceId): Unit =
    if activeChecks.contains(storageDeviceId) then
      deferredChecks += storageDeviceId
    else
      startDeviceCheck(storageDeviceId)

  /** Issues the one outstanding lookup for a device and acts on its result: a loaded device
   *  is reconciled against the tree, a device this process has not loaded -- never seen on disk,
   *  rejected by the load decision, or unloaded by a shutdown -- has its stores
   *  recorded as an unloaded device's (HostCopies.recordUnloadedEntries) so they stay silent rather than answering
   *  UnknownStore unless served here, and a
   *  failed lookup is warned about. The guard entry is then released and any request deferred
   *  behind it re-dispatched.
   *
   *  Caller has established that no check is active for this device.
   *
   *  Which of those two branches runs is decided when the lookup completes, not when it is
   *  issued. Runtime device discovery can load a device while its check is in flight, and a
   *  branch chosen at dispatch time would then record a loaded device's stores as an unloaded
   *  device's, silent until its next reconcile, rather than reconciling it now.
   *
   *  The lookup is issued on the control thread and the callback runs there too, so the whole
   *  of reconcileDeviceState -- filesystem work included -- runs with the control thread busy.
   *  That same thread is what makes the load-state re-read safe: every handler runs on it, so
   *  no device can be loading while the callback runs.
   *
   *  It is also what keeps a deferral from being stranded. checkStorageDevice's deferral
   *  write and this callback's release-and-read of the guard all happen on that one thread,
   *  so a colliding request either finds no active check and starts its own, or records a
   *  deferral the in-flight check's endDeviceCheck is guaranteed to see.
   *
   *  Releasing the guard is hostage to the lookup completing. The default
   *  lookupStorageDeviceState reads through the client, whose read driver retransmits with
   *  exponential backoff and imposes no timeout, so an unreachable pool holds this device's
   *  guard for the whole outage while the deferral coalesces everything arriving meanwhile
   *  into one re-check. That is intended, and it is what "one lookup in flight" amounts to in
   *  production.
   *
   *  The entry must be released on four exit paths: both outcomes of the lookup, a throw out of
   *  the callback body, and a throw from lookupStorageDeviceState itself. The first three are
   *  the finally. The fourth cannot be -- a throw before the Future exists means no callback is
   *  ever registered and so no finally ever runs -- hence the wrapper around the lookup call,
   *  which treats such a throw as a failed lookup. Releasing on fewer than all four would skip
   *  every later check of that device for the life of the process.
   *
   *  The callback-body path is not hypothetical: the reconcile touches the filesystem and
   *  issues transactions, so it can throw. It and the synchronous throw are both covered by
   *  HostDeviceDiscoverySuite.
   *
   *  Treating a synchronous throw as a failed lookup rather than letting it propagate leaves
   *  this method with no non-fatal synchronous throw path, and that is what makes
   *  endDeviceCheck's re-dispatch safe: a call that cannot throw cannot replace an exception
   *  already unwinding out of the try body, which would lose a reconcileDeviceState failure. Not literally none: the onComplete registration below sits outside the wrapper and
   *  does rethrow if the ExecutionContext throws a fatal, which no real thread-pool EC does.
   *
   *  Totality also keeps the CheckAllDevices sweep whole. That handler iterates storageDevices
   *  and calls this method per device with nothing catching in between, so a propagating throw
   *  would abandon every device after the throwing one until the next period -- an hour in
   *  production. No test covers that, and a refactor reintroducing a throw here would lose it
   *  silently.
   *
   *  The wrapper is scoped to the lookup call and not to the onComplete registration, but not
   *  for the reason one might expect. Widening it would not catch a non-fatal throw out of the
   *  callback body even under an inline or parasitic ExecutionContext: Future's
   *  Transformation.run absorbs those and routes them to reportFailure, so onComplete returns
   *  normally. What a wider scope would catch is a fatal throw, turning it into a logged
   *  warning. What the narrow scope gives up is an ExecutionContext that rejects the submission
   *  or accepts it and never runs the callback: either still leaks the entry, and neither is
   *  guarded here.
   */
  private def startDeviceCheck(storageDeviceId: StorageDeviceId): Unit =
    // Releases the guard and re-dispatches whatever was deferred behind it. Called from the
    // callback's finally and from the synchronous-throw path below; both run on the control
    // thread. Nested rather than a method on the class because it has no other caller, and
    // because closing over storageDeviceId leaves the two call sites no way to disagree about
    // which device they are releasing.
    //
    // The deferral flag is cleared before the re-dispatch, not after, and the order is enforced
    // by HostDeviceDiscoverySuite rather than argued. Clearing after recurses an extra
    // frame whenever the re-dispatched lookup throws synchronously -- the nested call absorbs
    // its own throw and re-enters here with the flag still set -- which needs no inline or
    // parasitic ExecutionContext to reach. The callback-body test catches it as a third lookup
    // attempt.
    def endDeviceCheck(): Unit =
      activeChecks -= storageDeviceId
      if deferredChecks.contains(storageDeviceId) then
        deferredChecks -= storageDeviceId
        startDeviceCheck(storageDeviceId)

    activeChecks += storageDeviceId

    // Deliberately around the lookup call alone and not the onComplete registration below; the
    // scaladoc has why. Throwable rather than NonFatal is also deliberate: the re-dispatch
    // safety argument needs this method to be total, so do not condense to Try(...), which
    // would narrow the catch to NonFatal and reopen the leak for a fatal throw.
    val lookup =
      try Success(ctx.metadata.lookupStorageDeviceState(storageDeviceId))
      catch case t: Throwable => Failure(t)

    lookup match
      case Success(pendingState) =>
        pendingState.onComplete: result =>
          try
            result match
              case Success(remote) =>
                // Load state re-read here, not at dispatch. See startDeviceCheck's scaladoc.
                ctx.devices.devices.get(storageDeviceId) match
                  case Some(local) => reconcileDeviceState(local, remote)
                  case None =>
                    // A device not loaded right now: one that has not appeared on disk in this
                    // process, one the load decision rejected, or one a shutdown unloaded.
                    // Silence its stores rather than sending "UnknownStore". Served dominates: a store served from
                    // another device of this host stays served
                    ctx.copyRegistry.recordUnloadedEntries(storageDeviceId, remote.stores.keySet)

              // The lookup fails whenever the device has no entry in the storage-devices tree:
              // a config written out-of-band naming an id that was never registered -- the
              // supported path cannot produce this, since
              // StorageDeviceManager.createStorageDevice commits the registration before
              // writing the config file, so its orphan is the reverse one, a registration with
              // no directory (see the ConfigWriteFailed advice in the cmdline Main) -- or a tree
              // entry removed after the fact, which no command does today. It also fails on any
              // failure of the metadata read itself, transient or not, which is the only
              // routinely reachable case.
              //
              // A copied or moved config is NOT one of these: its device is registered, so the
              // lookup succeeds. A device carried to another host keeps its registration too, so
              // a check of it there succeeds and takes the not-loaded branch above: it is not
              // loaded there until adopted (its owning-host file names its old host).
              case Failure(err) =>
                val what =
                  if ctx.devices.devices.contains(storageDeviceId) then "storage device"
                  else "never-loaded storage device"
                logger.warn(s"Failed to read state for $what $storageDeviceId. It may not " +
                            s"be registered in the storage-devices tree. Error: $err")
          finally endDeviceCheck()

      // A distinct message from the Failure(err) branch above, which explains itself with a
      // missing tree registration. True of a failed read; not necessarily true of a throw, which
      // an override is free to raise after reads have already gone out.
      //
      // The throwable is passed rather than interpolated, as the event loop's catch-all does and
      // unlike the read failures above. A read failure is a reachable outcome that its message
      // fully explains; a throw here is a bug on the dispatch path, where the class and message
      // alone can amount to a bare "java.lang.NullPointerException" naming no location. Before
      // the wrapper existed such a throw reached that catch-all and was logged with its stack,
      // so dropping it would make this a diagnostic downgrade.
      case Failure(err) =>
        logger.warn(s"Lookup of state for storage device $storageDeviceId threw instead of " +
                    s"returning a pending read", err)
        endDeviceCheck()
