package org.aspen_ddp.aspen.server.host

import org.aspen_ddp.aspen.client.{AspenClient, KeyValueObjectState, StopRetrying, Transaction}
import org.aspen_ddp.aspen.common.HLCTimestamp
import org.aspen_ddp.aspen.common.config.BootstrapConfig
import org.aspen_ddp.aspen.common.metadata.{HostId, HostState, StorageDeviceId, StorageDeviceState, StoragePoolState}
import org.aspen_ddp.aspen.common.network.{CheckStorageDevice, StoreMoved, StoreOffline}
import org.aspen_ddp.aspen.common.rebalancing.{RebalancingDurableService, RebalancingMessage, TransferComplete}
import org.aspen_ddp.aspen.server.GossipRetransmitter
import org.aspen_ddp.aspen.common.objects.{Insert, KeyValueObjectPointer}
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.transaction.KeyValueUpdate.KeyRevision
import DeviceMetadata.{AdoptOutcome, RebuildOutcome, ShutdownOutcome, TransferOutcome}
import scribe.Logging

import scala.concurrent.Future

/** The reads and transactions a Host performs against the storage-device, storage-pool and host metadata. Each
 *  method is a seam: a test replaces or wraps the metadata through Host.Seams. Where a body needs Host state, that
 *  state is a parameter of the method.
 *  See ClientDeviceMetadata for what each method does. */
trait DeviceMetadata:
  def updateStateForTransferredStore(storeId: StoreId,
                                     fromDeviceId: StorageDeviceId,
                                     toDeviceid: StorageDeviceId,
                                     gossipRetransmitter: GossipRetransmitter): Future[TransferOutcome]

  def readHostStateForEndpoint(): Future[HostState]

  def abortLostTransfer(storeId: StoreId): Future[Boolean]

  def markRebuiltStoreActive(storageDeviceId: StorageDeviceId,
                             storeId: StoreId,
                             gossipStoreOnline: StoreId => Unit): Future[RebuildOutcome]

  def adoptDevice(storageDeviceId: StorageDeviceId,
                  previousHostId: HostId,
                  devicePath: os.Path,
                  uncleanStores: (os.Path, StorageDeviceState) => List[StoreId],
                  crlLostStores: (os.Path, StorageDeviceState) => List[StoreId],
                  markRebuildRequired: os.Path => Unit): Future[AdoptOutcome]

  def lookupStorageDeviceState(storageDeviceId: StorageDeviceId): Future[StorageDeviceState]

  def readTransferSourceState(fromDeviceId: StorageDeviceId): Future[StorageDeviceState]

  def completeDeviceShutdown(storageDeviceId: StorageDeviceId): Future[ShutdownOutcome]

/** What the metadata methods decide, returned rather than acted on so that the caller can do the local disk work. */
object DeviceMetadata:
  /** What updateStateForTransferredStore decided about a transfer that has finished shipping
   *  bytes, so its caller knows whether the destination is allowed to keep the copy that just
   *  arrived.
   *
   *  Returned rather than acted on inside that method because the two halves live on opposite
   *  sides of the boundary: the decision is metadata and belongs in the transaction, while
   *  loading or discarding a store directory is local disk work that only the event loop does.
   */
  private[aspen] enum TransferOutcome:
    /** The pool now names the destination, which therefore owns the arrived copy. */
    case PoolRepointed

    /** As PoolRepointed, but the destination is shutting down or shut down, so its entry was
     *  written Offline rather than Active. The arrived copy is ours but must not be served: it
     *  stays unloaded behind its transferred-in marker until a restart or adoption makes the
     *  entry Active, when the reconcile's completed-transfers-in pass loads it. */
    case PoolRepointedOffline

    /** The destination was declared failed mid-flight, so the source was put back to Active and
     *  the pool left naming it. The arrived copy belongs to nobody. */
    case SourceRestored

    /** The transfer was aborted (AspenClient.abortStoreTransfer) before its bytes were recorded:
     *  the source's entry is no longer TransferringOut to the destination. The pool still names
     *  the source, which kept its copy, so the arrived copy belongs to nobody.
     *
     *  Also the outcome when the source was tombstoned and drained first: the store was rebuilt on
     *  another device, the pool names that one, and the destination's stale entry is removed. */
    case Aborted

    /** The destination device now names another host: it was shut down and adopted while this
     *  host's pool update was still in flight. Nothing was written. The arrived copy is left as
     *  it is, behind its transferred-in marker, for the owner's reconcile to finish: its
     *  TransferringIn pass finds the promoted copy and runs this update itself. */
    case NotOwned

  /** What markRebuiltStoreActive decided about a rebuilt store, so its caller knows whether the
   *  device is allowed to adopt the copy the rebuild placed.
   *
   *  Returned rather than acted on inside that method because the two halves live on opposite
   *  sides of the boundary: the decision is metadata and belongs in the transaction, while
   *  loading or discarding a store directory is local disk work that only the event loop does.
   */
  private[aspen] enum RebuildOutcome:
    /** The device entry was flipped to Active (or was already Active), so the store should be
     *  loaded. */
    case Adopt

    /** The device is shutting down or shut down, so the entry was written Offline rather than
     *  Active (or was found Offline already). The rebuilt store is the device's, but must not be
     *  served: it is kept offline behind a transfer-out marker, like every other store the
     *  shutdown closed, until a restart or adoption brings it back. */
    case AdoptOffline

    /** The tree does not say this device owns a rebuilt store: the device was tombstoned,
     *  the entry vanished, or the entry has a status other than Rebuilding/Active. The rebuilt
     *  store belongs to nobody and must be discarded. */
    case Discard

    /** The device now names another host: it was shut down and adopted while the rebuild was
     *  finishing. Nothing was written and the rebuilt copy is left in place. The owner's
     *  reconcile restarts the rebuild, which finds the store already at its final path and only
     *  flips the entry. */
    case NotOwned

  /** What completeDeviceShutdown decided, so shutDownDevice knows whether to unload the device,
   *  go round again, or undo its own on-disk work.
   *
   *  Returned rather than acted on inside that method for the same reason as TransferOutcome:
   *  the decision is metadata, while unloading and marker files are local work.
   */
  private[aspen] enum ShutdownOutcome:
    /** The device is Shutdown and has no Active entry. It may be unloaded. */
    case Completed

    /** Some entry was Active although the shutdown request had committed -- defence in depth,
     *  as every writer of an Active entry writes Offline on a shutting-down device -- and has
     *  been flipped to Offline instead of committing Shutdown. Its store may be open, so the
     *  shutdown goes round again to close it. */
    case StoresReactivated

    /** The device is no longer ShuttingDown: a restart arrived (or the device was declared
     *  failed). Nothing was written. */
    case Cancelled

  /** What adoptDevice decided, from the device state it re-read inside its transaction. */
  private[aspen] enum AdoptOutcome:
    /** Committed. `state` is exactly what was written, for the post-commit steps to act on. */
    case Adopted(state: StorageDeviceState)

    /** The device is no longer Shutdown and owned by the previous host. Nothing was written. */
    case Changed

    /** These Offline stores were not closed cleanly on disk (adoptionClass). Nothing was
     *  written. */
    case Unclean(stores: List[StoreId])

/** Runs only on the control thread. See HostThreading.scala.
 *
 *  The real metadata reads and transactions. Its Future callbacks, including the functions passed in
 *  (`uncleanStores`, `crlLostStores`, `markRebuildRequired`, `gossipStoreOnline`), run on the control context it
 *  imports from `ctx`. */
class ClientDeviceMetadata(control: => DeviceControl) extends DeviceMetadata with Logging:
  // By name: Host builds this before the DeviceControl that holds it, which only calls it once constructed. Used
  // during that construction, `control` would still be null: fail then, rather than cache the null for good (a
  // failed lazy val initializer is retried on next use).
  private lazy val ctx: DeviceControl =
    val c = control
    require(c != null, "ClientDeviceMetadata used before its DeviceControl was constructed")
    c
  import ctx.given

  private def client: AspenClient = ctx.client
  private def hostId: HostId = ctx.hostId

  /** Records the outcome of a store transfer that has finished shipping bytes.
   *
   *  Normally: the pool is repointed at the destination, the source drops the store, and the
   *  destination gains an Active entry.
   *
   *  If the destination was declared failed while the transfer was in flight, none of that is
   *  safe, and the failure is silent rather than loud. Its hostId reads as HostId(0,0), so the
   *  pool would be repointed at a host that does not exist. reconcileDeviceState ignores a failed
   *  device outright, so nothing on the destination would ever serve the store. And the only
   *  thing that could rebuild it -- FailedStorageDeviceDurableTask's drain -- has already dropped
   *  the entry, or will, precisely because a TransferringIn entry means the destination does not
   *  own the store. The slice would be lost with a pool entry that still looks healthy.
   *
   *  So take the repair instead: put the source's entry back to Active, leave the pool naming the
   *  source (it never stopped), and leave the tombstone's own store map alone -- that map is the
   *  drain's work list and its to remove.
   *
   *  The returned TransferOutcome is how the caller learns which of the two happened. It must not
   *  load the arrived copy on the restore path: nothing would ever reclaim it, since the
   *  deleted-stores pass looks only at offlineStores and reconcileDeviceState ignores a failed
   *  device outright.
   *
   *  Host's wrapper is protected so a test subclass can observe the returned Future; the production caller is
   *  StoreTransfers.startIn.
   */
  def updateStateForTransferredStore(storeId: StoreId,
                                     fromDeviceId: StorageDeviceId,
                                     toDeviceid: StorageDeviceId,
                                     gossipRetransmitter: GossipRetransmitter): Future[TransferOutcome] =
    // Only a collision is retried. Any other error -- an id with no entry in its tree, a pool
    // index the config does not have, generateBootstrapConfig's two requires, an object that
    // cannot be read -- is permanent, and retrying it would be a 60 s loop for the life of the
    // process that reports nothing at all.
    client.transactUntilSuccessfulWithRecovery(StopRetrying.unlessCollision): tx =>
      for
        poolPtr <- client.getStoragePoolPointer(storeId.poolId)
        fromDevPtr <- client.getStorageDevicePointer(fromDeviceId)
        toDevPtr <- client.getStorageDevicePointer(toDeviceid)
        poolKvos <- client.read(poolPtr)
        fromDevKvos <- client.read(fromDevPtr)
        toDevKvos <- client.read(toDevPtr)
        poolCfg = StoragePoolState(poolKvos)
        toDev = StorageDeviceState(toDevKvos)
        fromDev = StorageDeviceState(fromDevKvos)
        // Skipped entirely on the restore path. A tombstone's hostId is HostId(0,0), which has no
        // entry in the host tree, so prepRadicleUpdate's getHostState on it fails on every
        // attempt -- and the restore does not move the store between hosts, so there is nothing
        // for the bootstrap config to learn.
        //
        // On the normal path it is called before poolCfg.stores is mutated below, which is the
        // opposite of the order FailedStorageDeviceDurableTask.moveStore uses. Safe only because
        // prepRadicleUpdate filters its host list down to the hosts its store map references:
        // without that, the moved store's old host reaches generateBootstrapConfig with no stores
        // and trips its require(storesOnHost.nonEmpty).
        _ <-
          if toDev.isFailed || toDev.hostId != hostId then
            Future.unit
          else
            BootstrapConfig.prepRadicleUpdate(client, storeId, poolCfg, toDev.hostId)(using tx)
      yield
        if toDev.isFailed then
          logger.warn(s"Transfer of store $storeId onto device $toDeviceid finished after that " +
                      s"device was declared failed. Restoring the store on source device " +
                      s"$fromDeviceId rather than repointing the pool at a dead device")

          // None when the drain's disown pass has already restored the entry, or when a
          // concurrent call to this method has. Writing nothing is then correct.
          fromDev.restoreAbandonedTransferSource(storeId, toDeviceid).foreach: restored =>
            tx.update(fromDevPtr, None, None,
              List(KeyRevision(StorageDeviceState.StateKey,
                fromDevKvos.contents(StorageDeviceState.StateKey).revision)),
              List(Insert(StorageDeviceState.StateKey, restored.encode())))

            // This method runs on the destination host, so the source is a different
            // host and learns nothing from the write above. Its copy is still offline
            // behind the transfer-out marker while the pool goes on naming it, which means the
            // slice answers nothing until that host's own poll reinstates it -- up to
            // Main.CheckStorageDevicesPeriod away. Best-effort, because that poll is the
            // guarantee and this is only the latency.
            tx.result.foreach: _ =>
              client.sendBestEffortHostMessage(
                CheckStorageDevice(fromDev.hostId, client.clientId, fromDeviceId))

          TransferOutcome.SourceRestored

        // The destination was shut down and adopted by another host while this update was
        // pending. Only its owner may write its entries or announce where the store lives, so
        // nothing is written here; that host's reconcile resumes this update from the arrived
        // copy, which stays behind its transferred-in marker.
        else if toDev.hostId != hostId then
          logger.info(s"Transfer of store $storeId onto device $toDeviceid finished after the " +
                      s"device moved to host ${toDev.hostId}. Leaving the pool update to that host")
          TransferOutcome.NotOwned

        // Aborted (AspenClient.abortStoreTransfer) while the bytes were in flight, which puts the
        // source back to Active and leaves the pool naming it. Repointing the pool now would
        // complete a transfer its initiator has been told never happened.
        else if fromDev.stores.get(storeId).exists(entry =>
                  entry.status != StorageDeviceState.StoreStatus.TransferringOut ||
                  !entry.transferDevice.contains(toDeviceid)) then
          logger.info(s"Transfer of store $storeId from device $fromDeviceId onto device " +
                      s"$toDeviceid finished after it was aborted. Discarding the arrived copy")
          TransferOutcome.Aborted

        // If the from device doesn't contain the storeId, we're already done.
        // A concurrent call to this method must have succeeded
        else if fromDev.stores.contains(storeId) then
          // toDev.hostId, which the guard above has established is this host: the pool names the
          // device's owner, never merely whoever ran the update.
          poolCfg.stores(storeId.poolIndex) = StoragePoolState.StoreEntry(toDev.hostId, toDeviceid)
          val poolReqs = List(KeyRevision(StoragePoolState.ConfigKey,
                                          poolKvos.contents(StoragePoolState.ConfigKey).revision))
          val poolOps = List(Insert(StoragePoolState.ConfigKey, poolCfg.encode()))
          tx.update(poolPtr, None, None, poolReqs, poolOps)

          val newFromStores = fromDev.stores - storeId
          val newFromDev = fromDev.copy(stores = newFromStores)
          val fromDevReqs = List(KeyRevision(StorageDeviceState.StateKey,
                                             fromDevKvos.contents(StorageDeviceState.StateKey).revision))
          val fromDevOps = List(Insert(StorageDeviceState.StateKey, newFromDev.encode()))
          tx.update(fromDevPtr, None, None, fromDevReqs, fromDevOps)

          // Offline on a destination asked to shut down since the transfer began; see
          // serviceStatus. The pool is repointed all the same: the bytes are here.
          val newEntry = StorageDeviceState.StoreEntry(toDev.serviceStatus, None)
          val newToStores = toDev.stores + (storeId -> newEntry)
          val newtoDev = toDev.copy(stores = newToStores)
          val toDevReqs = List(KeyRevision(StorageDeviceState.StateKey,
                                           toDevKvos.contents(StorageDeviceState.StateKey).revision))
          val toDevOps = List(Insert(StorageDeviceState.StateKey, newtoDev.encode()))
          tx.update(toDevPtr, None, None, toDevReqs, toDevOps)

          // If state update transaction is successful, send a CheckStorageDevice
          // message to the host of the old storage device so they can delete
          // the store content
          tx.result.foreach: timestamp =>
            val msg = CheckStorageDevice(
              fromDev.hostId,
              client.clientId,
              fromDev.storageDeviceId
            )
            client.sendHostMessage(msg)

            // Best-effort wake-up for the rebalancing service (harmless for non-rebalance
            // transfers; the service also polls, so delivery is not required for correctness).
            client.sendServiceMessage(
              RebalancingDurableService.ServiceUUID,
              RebalancingMessage.encode(
                TransferComplete(toDev.storageDeviceSet, storeId, fromDeviceId, toDeviceid)))

            // Tell every other host (and client) where the store now lives. Sent only on this
            // branch: the other outcomes did not repoint the pool. This transaction wrote the pool
            // config, so its timestamp is the config value's. An Offline entry is announced
            // too, followed by its StoreOffline: the store does live here now, it just does not
            // answer until its device is back in service.
            gossipRetransmitter.broadcast(StoreMoved(storeId, toDev.hostId, timestamp, HLCTimestamp.now))
            if newEntry.status == StorageDeviceState.StoreStatus.Offline then
              gossipRetransmitter.broadcast(StoreOffline(storeId, HLCTimestamp.now))

          if newEntry.status == StorageDeviceState.StoreStatus.Offline then TransferOutcome.PoolRepointedOffline
          else TransferOutcome.PoolRepointed

        // The source has already dropped the store. Either a concurrent call to this method
        // repointed the pool at the destination, which owns the arrived copy either way, so this
        // is the same outcome reached by somebody else. Or the source was tombstoned and its
        // drain rebuilt the store elsewhere, in which case the pool names some other device and
        // loading the arrived copy would serve a store nobody routes to.
        else if poolCfg.stores(storeId.poolIndex).storageDeviceId == toDeviceid then
          // Offline when that call wrote it Offline, or the shutdown request has taken it Offline
          // since: either way the copy is ours but must not be served.
          if toDev.stores.get(storeId).exists(_.status == StorageDeviceState.StoreStatus.Offline) then
            TransferOutcome.PoolRepointedOffline
          else
            TransferOutcome.PoolRepointed

        else
          logger.info(s"Transfer of store $storeId from device $fromDeviceId onto device " +
                      s"$toDeviceid finished after the source was drained and the pool repointed " +
                      s"to ${poolCfg.stores(storeId.poolIndex).storageDeviceId}. Discarding the " +
                      s"arrived copy")

          // The drain touches only the tombstone and the rebuild device, so this half of the
          // transfer is still recorded here. Guarded on the entry so a concurrent call, or an
          // entry that has since become something else, writes nothing.
          toDev.stores.get(storeId)
            .filter: entry =>
              entry.status == StorageDeviceState.StoreStatus.TransferringIn &&
              entry.transferDevice.contains(fromDeviceId)
            .foreach: _ =>
              tx.update(toDevPtr, None, None,
                List(KeyRevision(StorageDeviceState.StateKey,
                  toDevKvos.contents(StorageDeviceState.StateKey).revision)),
                List(Insert(StorageDeviceState.StateKey, toDev.removeStore(storeId).encode())))

          TransferOutcome.Aborted

  /** The read that decides whether the endpoint needs updating. */
  def readHostStateForEndpoint(): Future[HostState] = client.getHostState(hostId)

  /** The abort returnLostTransferToRebuild issues. */
  def abortLostTransfer(storeId: StoreId): Future[Boolean] = client.abortStoreTransfer(storeId)

  /** Decides whether a rebuilt store should be adopted or discarded.
   *
   *  The device may have been tombstoned or removed from the tree between a rebuild starting and
   *  finishing. Reading the state inside the transaction and checking isFailed before the status
   *  check ensures the decision is made from current metadata, not stale assumptions. A device
   *  that has since been adopted by another host reads NotOwned, and nothing is written.
   *
   *  Only a collision is retried. Retrying a permanent error -- getStorageDevicePointer's
   *  NoSuchElementException, an unreadable device object -- would never complete the future, and
   *  this device's rebuild queue would never advance.
   */
  def markRebuiltStoreActive(storageDeviceId: StorageDeviceId,
                             storeId: StoreId,
                             gossipStoreOnline: StoreId => Unit): Future[RebuildOutcome] =
    client.transactUntilSuccessfulWithRecovery(StopRetrying.unlessCollision): tx =>
      for
        ptr <- client.getStorageDevicePointer(storageDeviceId)
        kvos <- client.read(ptr)
        state = StorageDeviceState(kvos)
      yield
        // Check isFailed first. A concurrent tombstone leaves stores intact but zeroes the ids,
        // so the entry still reads Rebuilding. Loading hands a live copy to a failed device, and
        // reconcileDeviceState bails on isFailed so nothing ever takes it back. Meanwhile the
        // drain is moving the store elsewhere, so two hosts serve a live copy of the same StoreId.
        if state.isFailed then
          RebuildOutcome.Discard
        // Shut down and adopted by another host while the rebuild was finishing. Only the owner
        // may write the device's entries, and the copy is the owner's to adopt, not ours to drop.
        else if state.hostId != hostId then
          RebuildOutcome.NotOwned
        else
          state.stores.get(storeId) match
            case Some(entry) if entry.status == StorageDeviceState.StoreStatus.Rebuilding =>
              // Offline on a device asked to shut down since the rebuild began; see serviceStatus.
              val status = state.serviceStatus
              val newState = state.setStoreEntry(storeId, status, None)

              val reqs = List(KeyRevision(StorageDeviceState.StateKey,
                kvos.contents(StorageDeviceState.StateKey).revision))
              val ops = List(Insert(StorageDeviceState.StateKey, newState.encode()))

              logger.info(s"Updating device state to mark rebuilt store $storeId as $status")
              tx.update(ptr, None, None, reqs, ops)

              if status == StorageDeviceState.StoreStatus.Active then
                tx.result.foreach: _ =>
                  logger.info(s"Rebuild of store $storeId complete")
                  gossipStoreOnline(storeId)
                RebuildOutcome.Adopt
              else
                tx.result.foreach: _ =>
                  logger.info(s"Rebuild of store $storeId complete. Its device is shutting down, " +
                              s"so it stays offline")
                RebuildOutcome.AdoptOffline

            case Some(entry) if entry.status == StorageDeviceState.StoreStatus.Active =>
              // Already flipped by a concurrent rebuild completion. No update staged.
              RebuildOutcome.Adopt

            case Some(entry) if entry.status == StorageDeviceState.StoreStatus.Offline =>
              // Already flipped by a concurrent completion, onto a device shutting down -- or
              // flipped Active and then taken Offline by the shutdown request. Either way the
              // copy is this device's and must stay out of service. No update staged.
              RebuildOutcome.AdoptOffline

            case _ =>
              // Entry missing entirely, or has a status other than Rebuilding/Active (Initializing,
              // TransferringIn, TransferringOut). In all these cases the tree does not say this
              // device owns a rebuilt store. Latent rather than live: no current in-tree writer
              // produces "healthy device, entry vanished", but it's the wrong default.
              RebuildOutcome.Discard

  /** Moves a device shut down by `previousHostId` onto this host, in one transaction:
   *  - the device: Offline stores Active, status Operational, hostId this host;
   *  - every pool entry naming this device: hostId this host. Entries naming another device are
   *    left alone -- a TransferringIn entry's pool still names its source;
   *  - the previous host's HostState loses the device, this host's gains it;
   *  - for a bootstrap-pool store, the bootstrap config in the radicle, as a transfer does.
   *
   *  Every object written carries a KeyRevision, so a restart on the old host or a second
   *  adopter racing this one makes one of them lose and re-read. Returns Changed, writing nothing,
   *  when the re-read finds the device no longer Shutdown and owned by `previousHostId`, and
   *  Unclean, writing nothing, when an Offline entry of the re-read state has a directory under
   *  `devicePath` that was not closed cleanly: the state validated is the state committed, so an
   *  entry gone Offline since the caller's lookup is checked too. Adopted carries the state
   *  written, which the post-commit steps act on. The
   *  previous host's HostState is skipped if that host has no entry in the hosts tree: it was
   *  removed, and there is nothing left to take the device out of.
   *
   *  Only a collision is retried, as everywhere else in Host: a permanent error would otherwise
   *  hold the device's pendingMarkerResolutions entry for the life of the process.
   *
   *  Host's wrapper is protected so a test subclass can record the calls.
   */
  def adoptDevice(storageDeviceId: StorageDeviceId,
                  previousHostId: HostId,
                  devicePath: os.Path,
                  uncleanStores: (os.Path, StorageDeviceState) => List[StoreId],
                  crlLostStores: (os.Path, StorageDeviceState) => List[StoreId],
                  markRebuildRequired: os.Path => Unit): Future[AdoptOutcome] =
    import StorageDeviceState.DeviceStatus

    case class PoolUpdate(pointer: KeyValueObjectPointer, kvos: KeyValueObjectState, config: StoragePoolState)

    def readPool(poolId: PoolId): Future[PoolUpdate] =
      for
        ptr <- client.getStoragePoolPointer(poolId)
        kvos <- client.read(ptr)
      yield PoolUpdate(ptr, kvos, StoragePoolState(kvos))

    def readHost(id: HostId): Future[(KeyValueObjectPointer, KeyValueObjectState)] =
      for
        ptr <- client.getHostPointer(id)
        kvos <- client.read(ptr)
      yield (ptr, kvos)

    def writeHost(ptr: KeyValueObjectPointer, kvos: KeyValueObjectState, hs: HostState)(using tx: Transaction): Unit =
      tx.update(ptr, None, None,
        List(KeyRevision(HostState.StateKey, kvos.contents(HostState.StateKey).revision)),
        List(Insert(HostState.StateKey, hs.encode())))

    client.transactUntilSuccessfulWithRecovery(StopRetrying.unlessCollision): tx =>
      given Transaction = tx
      for
        devPtr <- client.getStorageDevicePointer(storageDeviceId)
        devKvos <- client.read(devPtr)
        state = StorageDeviceState(devKvos)
        unclean = uncleanStores(devicePath, state)
        adopted <-
          if state.status != DeviceStatus.Shutdown || state.hostId != previousHostId then
            Future.successful(AdoptOutcome.Changed)
          else if unclean.nonEmpty then
            Future.successful(AdoptOutcome.Unclean(unclean))
          else
            // Pool entries are rewritten before prepRadicleUpdate reads them, so its store map
            // names this host for every moved bootstrap store, not only the one it is given.
            val moved = state.stores.keysIterator.toList.sortBy(s => (s.poolId.uuid, s.poolIndex))
            for
              pools <- Future.sequence(moved.map(_.poolId).distinct.map(readPool))
              newHost <- readHost(hostId)
              oldHost <- readHost(previousHostId).map(Some(_)).recover:
                           case _: NoSuchElementException => None
              repointed = moved.filter: storeId =>
                pools.find(_.config.poolId == storeId.poolId).exists: pu =>
                  val entry = pu.config.stores(storeId.poolIndex)
                  if entry.storageDeviceId == storageDeviceId && entry.hostId != hostId then
                    pu.config.stores(storeId.poolIndex) = entry.copy(hostId = hostId)
                    true
                  else
                    false
              _ <- repointed.find(_.poolId == PoolId.BootstrapPoolId) match
                     case Some(storeId) =>
                       val cfg = pools.find(_.config.poolId == storeId.poolId).get.config
                       BootstrapConfig.prepRadicleUpdate(client, storeId, cfg, hostId)
                     case None => Future.unit
            yield
              val changedPools = repointed.map(_.poolId).toSet
              pools.filter(pu => changedPools.contains(pu.config.poolId)).foreach: pu =>
                tx.update(pu.pointer, None, None,
                  List(KeyRevision(StoragePoolState.ConfigKey, pu.kvos.contents(StoragePoolState.ConfigKey).revision)),
                  List(Insert(StoragePoolState.ConfigKey, pu.config.encode())))

              // Stores whose CRL state is gone are rebuilt rather than refused. The marker is
              // written before the commit and is correct whatever its outcome: the state is lost
              // either way, and the marker is what keeps the directory from ever being loaded.
              val crlLost = crlLostStores(devicePath, state)
              crlLost.foreach: storeId =>
                logger.warn(s"Store $storeId on storage device $storageDeviceId has a missing or " +
                            s"unusable CRL save file. Adopting it as Rebuilding")
                markRebuildRequired(devicePath / storeId.directoryName)
              val reactivated = state.withOfflineStoresActive.copy(hostId = hostId, status = DeviceStatus.Operational)
              val newState = crlLost.foldLeft(reactivated): (st, storeId) =>
                st.setStoreEntry(storeId, StorageDeviceState.StoreStatus.Rebuilding, None)
              tx.update(devPtr, None, None,
                List(KeyRevision(StorageDeviceState.StateKey, devKvos.contents(StorageDeviceState.StateKey).revision)),
                List(Insert(StorageDeviceState.StateKey, newState.encode())))

              oldHost match
                case Some((ptr, kvos)) => writeHost(ptr, kvos, HostState(kvos).removeStorageDevice(storageDeviceId))
                case None =>
                  logger.info(s"Previous host $previousHostId of storage device $storageDeviceId is not " +
                              s"registered. Adopting the device without updating its host state")
              val (newPtr, newKvos) = newHost
              writeHost(newPtr, newKvos, HostState(newKvos).addStorageDevice(storageDeviceId))

              tx.result.foreach: _ =>
                logger.info(s"Adopted storage device $storageDeviceId from host $previousHostId")
              AdoptOutcome.Adopted(newState)
      yield adopted

  /** Reads the state recorded for `storageDeviceId` in the storage-devices tree.
   *
   *  Host's wrapper is a seam so a test can hold a lookup in flight while the
   *  device it names is loaded underneath it. That interleaving is what a device appearing on
   *  disk during its own check produces, and it cannot be staged through the real client,
   *  whose reads resolve on their own schedule.
   *
   *  Called on the control thread, so an override must return promptly rather than
   *  block: that thread runs every handler (HostThreading.scala, rule 2).
   *
   *  May throw rather than return a failed Future. startDeviceCheck treats the two alike, so an
   *  override is free to do either.
   */
  def lookupStorageDeviceState(storageDeviceId: StorageDeviceId): Future[StorageDeviceState] =
    client.getStorageDeviceState(storageDeviceId)

  /** Reads the source device of a transfer in, which the reconcile's TransferringIn pass re-reads
   *  on every check so a source that has moved, shut down or failed is seen. Host's wrapper is a seam so
   *  a test can tell when the read has been acted on, which is otherwise unobservable when the pass decides
   *  not to start anything. */
  def readTransferSourceState(fromDeviceId: StorageDeviceId): Future[StorageDeviceState] =
    client.getStorageDeviceState(fromDeviceId)

  /** The last step of a shutdown: one transaction on the device state that either commits
   *  Shutdown or explains why it did not.
   *
   *  The Active check is defence in depth. Every transaction that brings a store into service
   *  writes StorageDeviceState.serviceStatus, which is Offline once the shutdown request has
   *  committed, so no Active entry should be found here. Should one appear anyway (a writer
   *  that misses that rule), committing Shutdown would shelve a disk whose metadata says one of
   *  its stores is serving. Instead those entries are flipped to Offline, as the request itself
   *  did for the entries Active at the time, and the caller goes round again to close them.
   *
   *  An already-Shutdown device counts as Completed with nothing written. Only this method writes
   *  Shutdown, so seeing it means an earlier attempt committed -- a retry after a commit whose
   *  outcome was not learned, or a crash before the unload -- and a restart would have made it
   *  Operational rather than leaving it Shutdown. Reporting Cancelled there would remove
   *  shutdown.state from a device that is shut down.
   *
   *  Only a collision is retried. Nothing else would complete the future, and the device's
   *  shutdown guard would be held for the life of the process.
   *
   *  Host's wrapper is protected so a test can run work between the stores closing and this transaction.
   */
  def completeDeviceShutdown(storageDeviceId: StorageDeviceId): Future[ShutdownOutcome] =
    client.transactUntilSuccessfulWithRecovery(StopRetrying.unlessCollision): tx =>
      for
        ptr <- client.getStorageDevicePointer(storageDeviceId)
        kvos <- client.read(ptr)
      yield
        val state = StorageDeviceState(kvos)

        def write(newState: StorageDeviceState): Unit =
          tx.update(ptr, None, None,
            List(KeyRevision(StorageDeviceState.StateKey, kvos.contents(StorageDeviceState.StateKey).revision)),
            List(Insert(StorageDeviceState.StateKey, newState.encode())))

        state.status match
          case StorageDeviceState.DeviceStatus.Shutdown => ShutdownOutcome.Completed
          case StorageDeviceState.DeviceStatus.ShuttingDown =>
            if state.stores.valuesIterator.exists(_.status == StorageDeviceState.StoreStatus.Active) then
              write(state.withActiveStoresOffline)
              ShutdownOutcome.StoresReactivated
            else
              write(state.copy(status = StorageDeviceState.DeviceStatus.Shutdown))
              ShutdownOutcome.Completed
          case _ => ShutdownOutcome.Cancelled
