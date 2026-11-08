package org.aspen_ddp.aspen.server.host

import org.aspen_ddp.aspen.common.HLCTimestamp
import org.aspen_ddp.aspen.common.metadata.{StorageDeviceId, StorageDeviceState}
import org.aspen_ddp.aspen.common.network.CheckStorageDevice
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.server.Host
import org.aspen_ddp.aspen.server.rebuild.RebuildingStore
import org.aspen_ddp.aspen.server.transfer.StoreTransfer
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
    ctx.discovery.checkForNewDevices().foreach(checkStorageDevice)
    ctx.loader.retryStoresAwaitingSpace()

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
   *  directory on the device and not loaded. That is a store transferred away, left so by a crash between a transfer
   *  out's save file and the deletion of its entries, or by one between the deletion of its directory and the drop of
   *  its entries (or by a failed drop). Left there, they would be copied forward forever, uncounted by admission, and
   *  recovered if the store ever came back. The CRL is what is read, not a list kept in memory, so a restart forgets
   *  nothing. A store transferred back has its entry, and so its entries are its own once more.
   *
   *  The directory and loaded checks run when the CRL's answer arrives, in the same control task that starts the
   *  drop: a store that lands on the device has its directory before anything saves entries for it. A failed drop is
   *  retried by the device's next reconcile. */
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
          !remote.stores.contains(storeId) && !local.loadedStores.contains(storeId) &&
            !os.exists(local.devicePath / storeId.directoryName) && !crlDropsRunning.contains(storeId)
        // Only while the device is still loaded: an unloaded one's CRL is shut down
        if ctx.devices.devices.get(deviceId).exists(_ eq local) then
          stores.filter(leftover).toList.sortBy(s => (s.poolId.uuid, s.poolIndex)).foreach: storeId =>
            logger.info(s"Dropping the device-CRL entries of store $storeId, which storage device $deviceId no " +
                        s"longer holds")
            crlDropsRunning += storeId
            val dropped =
              try local.crl.dropStore(storeId)
              catch case NonFatal(t) => Future.failed(t)
            dropped.onComplete: result =>
              crlDropsRunning -= storeId
              result.failed.foreach: t =>
                logger.error(s"Failed to drop the device-CRL entries of store $storeId, which storage device " +
                             s"$deviceId no longer holds. Retrying at the device's next check. Error: $t")

  /** Reconciles a loaded device's on-disk state against the state recorded for it in the
   *  storage-devices tree: deletes stores transferred away, flips stores whose CRL state was lost
   *  to Rebuilding, reinstates stores left offline by an abandoned transfer out or a cancelled
   *  shutdown, creates Initializing stores, and starts transfers in.
   *
   *  If the tree records a different host, none of that happens and the device is not claimed.
   *  tryLoadDevice only loads a device whose `owning-host` file names this host, so a mismatch
   *  means the file and the tree disagree -- a hand-edited or copied file, or a device adopted
   *  by another host and then carried back here without a shutdown (an adoption itself rewrites
   *  the file only after its commit, while shutdown.state still keeps the device unloaded) --
   *  and reconciling against another host's records could delete or create
   *  stores that are not ours. It is logged once per device and left for an operator.
   *  A device the tree records as failed is ignored outright: it is neither claimed nor
   *  reconciled, and its stores are being rebuilt elsewhere.
   *  A device the tree records as ShuttingDown (or Shutdown) is handed to shutDownDevice instead
   *  of any of the passes, so nothing new starts on a disk about to be shelved.
   */
  private def reconcileDeviceState(local: LocalStorageDeviceState,
                                   remote: StorageDeviceState): Unit =
    if remote.isFailed then
      // An operator has declared this device dead. Its directory can still mount -- a dead
      // controller rather than a dead platter -- and its zeroed host id must not be mistaken for
      // another owner by the branch below. Its stores are being rebuilt elsewhere.
      logger.info(s"Ignoring failed storage device ${local.storageDeviceId}")
    else if remote.hostId != ctx.hostId then
      ctx.discovery.warnOnce(local.devicePath,
        s"Storage device ${local.storageDeviceId} owned by host ${remote.hostId} is loaded here. Not claiming it")
    else if remote.status == StorageDeviceState.DeviceStatus.ShuttingDown ||
            remote.status == StorageDeviceState.DeviceStatus.Shutdown ||
            ctx.shutdown.isShuttingDown(local.storageDeviceId) then
      // None of the passes below run: each would start work on a device about to be shelved.
      // A store transferred away meanwhile keeps its directory until the device is next
      // reconciled while Operational, here or on an adopting host.
      //
      // That includes an Operational read while a shutdown is still in flight -- the restart's
      // own nudge, typically. The in-flight sequence ends in Cancelled and checks the device
      // again once its own work has stopped, and that check runs the passes. shutDownDevice
      // itself does nothing then, as the guard is held.
      ctx.shutdown.shutDownDevice(local, remote)
    else
      //----------------------
      // Deleted Stores
      //
      local.offlineStores.filter(storeId =>
        !remote.stores.contains(storeId)
      ).foreach: storeId =>
        ctx.router.clearOffline(storeId)
        local.offlineStores -= storeId
        // Dropped from loadedStores too, or a later shutdown would take this store for one still
        // open (it is no longer in offlineStores) and fail writing a marker into a deleted
        // directory on every attempt.
        local.loadedStores -= storeId
        val storePath = local.devicePath / storeId.directoryName
        if os.exists(storePath) then
          logger.info(s"Deleting successfully transferred store $storePath")
          try
            os.remove.all(storePath)
          catch
            case t: Throwable => logger.error(s"Failed to delete store $storePath. Error: $t")
      // Any entries a store transferred away left in the device CRL go too
      dropLeftoverCrlState(local, remote)

      //----------------------
      // Lost CRL State
      //
      // A store marked rebuild-required (crlSaveFileLost, or an adoption) whose entry still
      // reads Active is flipped to Rebuilding; the Rebuilding pass below then discards its
      // directory and rebuilds it. Before the abandoned-transfers-out pass, which would
      // otherwise take a marked store left behind a transfer-out marker for one to reinstate.
      val lostCrl = local.offlineStores.filter(storeId =>
        os.exists(local.devicePath / storeId.directoryName / RebuildingStore.RebuildRequiredMarkerFile))
      lostCrl.foreach: storeId =>
        if remote.stores.get(storeId).exists(_.status == StorageDeviceState.StoreStatus.Active) then
          ctx.rebuilds.requestRebuild(local.storageDeviceId, storeId)

      //----------------------
      // Abandoned Transfers Out
      //
      // Nothing else ever clears a transfer-out marker from the source. StoreTransfers.startIn
      // deletes it only from the destination's unpacked copy, and a transfer that completes
      // normally has the source's whole store directory deleted by the pass above -- the marker
      // goes with it. So a transfer that ends without completing, which is what a destination
      // declared failed mid-flight produces, strands the marker forever, and tryLoadStore re-reads
      // it on every restart and puts the store straight back into offlineStores.
      //
      // That pairs badly with the metadata repair for the same event. That repair puts this
      // entry back to Active and leaves the pool naming this device, so afterwards the metadata
      // reads entirely healthy while the slice is served by nobody, here or anywhere. Repairing
      // the metadata alone would trade a visibly stalled TransferringOut entry for an invisible
      // outage.
      //
      // On the source host's own poll rather than inline at either repair site, because only
      // this host can touch this disk and one of those two sites -- the drain -- runs wherever
      // the durable task happens to live. Polling for desired state rather than being told is
      // also what the rest of this method does.
      //
      // The guard is narrow on purpose. Active *and* no transferDevice is the only combination
      // that says no transfer is outstanding; an entry still TransferringOut, or Active with a
      // destination still named, means the marker is doing its job. isTransferringOut is the
      // same question asked of this process rather than the tree, and covers the window where
      // the drain restores the entry while this host is still shipping bytes.
      //
      // A store still sending is left alone but re-checked shortly. An abort's nudge usually
      // arrives while the send is still running, and the send ends (the destination cancels its
      // receive) with nothing to prompt another check for a full checkStorageDevicePeriod.
      val (stillSending, abandonedOut) = (local.offlineStores -- lostCrl).filter(storeId =>
        remote.stores.get(storeId).exists(entry =>
          entry.status == StorageDeviceState.StoreStatus.Active && entry.transferDevice.isEmpty
        ) &&
        os.exists(local.devicePath / storeId.directoryName / StoreTransfer.TransferOutMarkerFile)
      ).partition(ctx.storeTransferService.isTransferringOut)
      abandonedOut.foreach: storeId =>
        ctx.transfers.reinstateAbandonedOut(local, storeId)
      if stillSending.nonEmpty then
        scheduleDeviceRecheck(local.storageDeviceId)

      //----------------------
      // Cancelled Shutdown
      //
      // A shutdown cancelled by a restart leaves the stores it closed offline here, with nothing
      // on disk to say so: their state stayed in the device CRL. Loaded again once Active.
      ctx.shutdown.reloadClosedStores(local, remote)

      //----------------------
      // Completed Transfers In
      //
      // A crash between the pool update committing and the transferred-in marker's removal
      // leaves an Active entry naming this device over a copy that tryLoadStore skips. The entry
      // is no longer TransferringIn, so StoreTransfers.startIn never sees it, and nothing else
      // removes the marker. Active is the tree's word that the pool now names this device, so
      // the copy is ours to load. transferringIn covers the window where this process's own
      // metadata step has committed but has not yet removed the marker itself.
      local.offlineStores.filter(storeId =>
        !ctx.transfers.transferringIn.contains(storeId) &&
        remote.stores.get(storeId).exists(_.status == StorageDeviceState.StoreStatus.Active) &&
        os.exists(local.devicePath / storeId.directoryName / StoreTransfer.TransferredInMarkerFile)
      ).foreach: storeId =>
        val storePath = local.devicePath / storeId.directoryName
        logger.warn(s"Store $storeId on device ${local.storageDeviceId} is Active but still " +
                    s"carries its transferred-in marker. Completing the transfer and loading the store")
        if ctx.transfers.clearTransferredInMarker(storeId, storePath) then
          ctx.tryLoadStore(local, storePath, None)

      //----------------------
      // New Stores
      //
      remote.stores.filter((storeId, entry) =>
        entry.status == StorageDeviceState.StoreStatus.Initializing
      ).map( (storeId, _) =>
        storeId
      ).foreach: storeId =>
        ctx.rebuilds.createNewStore(local, storeId)

      //----------------------
      // Aborted Transfers In
      //
      // An abort removes this device's TransferringIn entry and tells nobody, so this check is the
      // only thing that learns of it. Left alone, an attempt still waiting on bytes keeps
      // retransmitting its request and holds the store in transferringIn, which would make the
      // pass below skip a later transfer of the same store. Before that pass for that reason.
      // An attempt past promotion is not covered here; updateStateForTransferredStore sees the
      // abort in the source's entry instead.
      val trackedTransfersIn =
        ctx.transfers.pendingStartTransfers.valuesIterator
          .filter(_.toDeviceId == local.storageDeviceId)
          .map(p => (p.msg.storeId, p.msg.fromDevice)).toList ++
        ctx.transfers.transferInRetries.iterator
          .filter((_, r) => r.toDeviceId == local.storageDeviceId)
          .map((storeId, r) => (storeId, r.fromDeviceId)).toList
      trackedTransfersIn.filterNot((storeId, fromDeviceId) =>
        remote.stores.get(storeId).exists(entry =>
          entry.status == StorageDeviceState.StoreStatus.TransferringIn &&
          entry.transferDevice.contains(fromDeviceId))
      ).foreach: (storeId, fromDeviceId) =>
        ctx.transfers.dropAbandonedTransferIn(storeId, fromDeviceId, local.storageDeviceId)

      // The same store can be re-targeted at another device of this host. transferringIn is keyed
      // by store, so the earlier attempt would block the new one until its own device's next
      // check noticed the abort. This device's entry is the newer word on who receives the store,
      // so the other device's attempt is dropped here instead of a poll later.
      val retargeted =
        ctx.transfers.pendingStartTransfers.valuesIterator
          .filter(_.toDeviceId != local.storageDeviceId)
          .map(p => (p.msg.storeId, p.msg.fromDevice, p.toDeviceId)).toList ++
        ctx.transfers.transferInRetries.iterator
          .filter((_, r) => r.toDeviceId != local.storageDeviceId)
          .map((storeId, r) => (storeId, r.fromDeviceId, r.toDeviceId)).toList
      retargeted.filter((storeId, _, _) =>
        remote.stores.get(storeId).exists(_.status == StorageDeviceState.StoreStatus.TransferringIn)
      ).foreach: (storeId, fromDeviceId, toDeviceId) =>
        ctx.transfers.dropAbandonedTransferIn(storeId, fromDeviceId, toDeviceId)

      //----------------------
      // Transferring In Stores
      //
      remote.stores.filter { (storeId, entry) =>
        entry.status == StorageDeviceState.StoreStatus.TransferringIn
      }.map { (storeId, status) =>
        (storeId, status.transferDevice, status.transferId)
      }.toList.foreach: (storeId, ofromDeviceId, transferId) =>
        ofromDeviceId.foreach: fromDeviceId =>
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

      //----------------------
      // Rebuilding Stores
      //
      remote.stores.filter((_, entry) =>
        entry.status == StorageDeviceState.StoreStatus.Rebuilding
      ).keys.toList.sortBy(_.poolIndex).foreach: storeId =>
        ctx.rebuilds.startRebuild(local, storeId)

  /** Checks `storageDeviceId` again after TransferOutRecheckDelay, so a store left offline behind
   *  a transfer-out marker is reinstated soon after an aborted transfer's send ends rather than at
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
   *  added to offlineStores so they stay silent rather than answering UnknownStore, and a
   *  failed lookup is warned about. The guard entry is then released and any request deferred
   *  behind it re-dispatched.
   *
   *  Caller has established that no check is active for this device.
   *
   *  Which of those two branches runs is decided when the lookup completes, not when it is
   *  issued. Runtime device discovery can load a device while its check is in flight, and a
   *  branch chosen at dispatch time would then mark a loaded device's stores offline --
   *  re-adding ids that tryLoadStore and the LoadStore handler have just removed, and that
   *  nothing removes again: the only other site that clears offlineStores is
   *  reconcileDeviceState's deleted-stores pass, which touches ids recorded in the device's
   *  own offlineStores set, which these never enter.
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
   *  already unwinding out of the try body, which is how a reconcileDeviceState failure used to
   *  be lost. Not literally none: the onComplete registration below sits outside the wrapper and
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
                    // Silence its stores rather than sending "UnknownStore".
                    remote.stores.keysIterator.foreach: storeId =>
                      ctx.router.markOffline(storeId)

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
