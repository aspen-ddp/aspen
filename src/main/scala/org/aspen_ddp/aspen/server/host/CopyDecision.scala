package org.aspen_ddp.aspen.server.host

import java.util.UUID

import org.aspen_ddp.aspen.common.metadata.{StorageDeviceId, StorageDeviceState}
import org.aspen_ddp.aspen.common.metadata.StorageDeviceState.StoreStatus
import org.aspen_ddp.aspen.common.store.StoreId

/** The device reconcile's per-store decisions as a pure function of values (stage 3, D1). It reads no host state and
 *  does nothing: the reconcile gathers a StoreInputs for each store, runs decide, and carries out the actions it
 *  returns, in order. Because it is pure, every combination of inputs can be enumerated and the invariants checked
 *  over all of them (CopyDecisionSuite). */
object CopyDecision:

  /** What a copy's Activity is, without its payload */
  enum ActivityKind:
    case Idle, AwaitingRetry, Opening, Served, Closing, Retiring

  object ActivityKind:
    def of(a: Activity): ActivityKind = a match
      case Activity.Idle => Idle
      case Activity.AwaitingRetry(_) => AwaitingRetry
      case Activity.Opening(_) => Opening
      case Activity.Served => Served
      case Activity.Closing(_) => Closing
      case Activity.Retiring => Retiring

  /** The copy of a store on the device, as the rules see it. `pendingLost`: its move to lost/ failed and is retried by
   *  each device check (CopyMoves.pendingLost) */
  final case class CopyState(phase: Phase, unsynced: Boolean, activity: ActivityKind, pendingLost: Boolean)

  /** A transfer in the host tracks (pendingStartTransfers or transferInRetries) for the store */
  final case class TrackedIn(fromDevice: StorageDeviceId, toDevice: StorageDeviceId)

  /** What decide cannot see in the device's own state: the rest of the host, and stats of the copy */
  final case class StoreFacts(
    servedOnHost: Boolean,          // ctx.router.get(storeId).isDefined
    openingOnHost: Boolean,         // ctx.loader.isOpening(storeId)
    retiringOnHost: Boolean,        // ctx.copies.isRetiring(storeId)
    sending: Boolean,               // ctx.storeTransferService.isTransferringOut(storeId)
    receivingOnHost: Boolean,       // ctx.transfers.receiving.contains(storeId)
    rebuildReadsOnHost: Boolean,    // registry.rebuilding(HostCopies.RebuildReads).contains(storeId)
    rebuildHoldsOnHost: Boolean,    // registry.rebuilding(HostCopies.RebuildHolds).contains(storeId)
    rebuildHandle: Boolean,         // StoreRebuilds.runningRebuilds.contains(storeId)
    dirPresent: Option[Boolean],    // registry.existence(local, storeId)
    configPresent: Option[Boolean], // registry.existence(local, storeId, Some(StoreConfig.configFilename))
    declaredMissing: Boolean,       // local.declaredMissing.contains(storeId)
    heldForSync: Boolean,           // registry.heldForSyncOn(local).contains(storeId)
    tracked: List[TrackedIn])       // this store's pendingStartTransfers and transferInRetries, any device

  object StoreFacts:
    val none: StoreFacts = StoreFacts(false, false, false, false, false, false, false, false, Some(false), Some(false),
                                      false, false, Nil)

  final case class StoreInputs(deviceId: StorageDeviceId, storeId: StoreId,
                               entry: Option[StorageDeviceState.StoreEntry], copy: Option[CopyState],
                               work: Option[Work], facts: StoreFacts)

  /** Declared in pass order: decide emits actions in ordinal order */
  enum Action:
    /** The queued Rebuild work of a store no longer Rebuilding here ends */
    case EndQueuedRebuild
    /** A served copy with no entry is closed, moved to lost/ and left offline */
    case TakeOutOfService
    /** An offline copy whose directory is gone loses its record */
    case ForgetGoneCopy
    /** An offline copy with no entry is retired into pending-delete */
    case Retire
    /** An Active entry with no store behind it is declared missing (`first`: not declared before, so logged) */
    case DeclareMissing(first: Boolean)
    /** The entry flips to Rebuilding */
    case RequestRebuild
    /** The device is checked again shortly */
    case Recheck
    /** An outbound or parked copy goes back into service */
    case ReinstateOut
    /** An arrived copy goes Live and is loaded */
    case BringArrivedLive
    /** A rebuilt copy goes Live and is loaded */
    case BringRebuiltLive
    /** A held Live copy is loaded */
    case LoadResynced
    /** The store is created */
    case Create
    /** A tracked transfer in is dropped */
    case DropTransferIn(fromDevice: StorageDeviceId, toDevice: StorageDeviceId)
    /** The running rebuild of an episode no longer listed is cancelled */
    case CancelRebuild
    /** The transfer in is started or resumed */
    case StartTransferIn(fromDevice: StorageDeviceId, transferId: Option[UUID])
    /** The rebuild is started */
    case StartRebuild(episode: Option[UUID], acceptLoss: Boolean)

  import Action.*

  /** The actions the reconcile takes for one store, in the order it takes them. A work-starting action (Create,
   *  StartTransferIn, StartRebuild) is refused unless the store has no work or work of its own kind (I3). */
  def decide(in: StoreInputs): List[Action] =
    val f = in.facts
    val entry = in.entry
    val status = entry.map(_.status)
    val active = status.contains(StoreStatus.Active)
    val rebuilding = status.contains(StoreStatus.Rebuilding)

    // Queued rebuild work of a store no longer Rebuilding here ends first, so the rest of the rules see its copy free
    // (D4; rebuild-process 6.2)
    val endQueued = in.work match
      case Some(Work.Rebuild(_, RebuildStage.Queued)) => !rebuilding
      case _ => false
    val work: Option[Work] = if endQueued then None else in.work

    // The copy the passes may act on: Idle or awaiting a load retry (neither served, opening, closing nor retiring),
    // with no work, not Retired, and no failed move to lost/ to retry. I3: a transfer in, rebuild, create or transfer
    // out owns its copy, as a close or a retire does, and each one's end checks the device again
    val offline = in.copy.exists: c =>
      c.phase != Phase.Retired && !c.pendingLost && work.isEmpty &&
        (c.activity == ActivityKind.Idle || c.activity == ActivityKind.AwaitingRetry)
    val open = in.copy.exists(c => c.activity == ActivityKind.Opening || c.activity == ActivityKind.Served)
    val phase = in.copy.map(_.phase)

    val out = List.newBuilder[Action]

    if endQueued then
      out += EndQueuedRebuild

    // A served store whose entry is absent is a copy put at the store path by hand: every path that serves a store
    // has its entry first. It is taken out of service (closed, moved to lost/, offline), and the check that follows
    // retires it like any other offline one.
    if entry.isEmpty && in.copy.exists(_.activity == ActivityKind.Served) then
      out += TakeOutOfService

    // An offline store whose entry is gone from this device is no longer owned here: a transfer out completed, or its
    // rebuild was re-placed onto another device (rebuild-process 4.2). Its copy is retired, not deleted: it and its
    // CRL state are kept in pending-delete until the store is Active on the device its pool names (StoreCopies). For
    // a completed transfer that is already so, and the check the retire asks for deletes it. It stays offline
    // (silent) until its copy has left its path. A record whose directory is known gone (removed by hand) goes at
    // once; a directory whose stat fails waits; one being retired is not retired again.
    if entry.isEmpty && offline then
      if f.dirPresent.contains(false) then
        out += ForgetGoneCopy
      else if f.dirPresent.isDefined && !f.retiringOnHost then
        out += Retire

    // An Active entry with no store behind it: its directory, or the directory's config.yaml, is gone (a bad
    // restore, a partial mount, a hand removal). No other pass sees such a store, since each starts from a copy, a
    // phase or a transitional entry, and the pool counts it writable meanwhile: one fault would cost a unit of margin
    // for good. It is silenced and a rebuild requested, whose Rebuilding pass retires whatever is at the path. Its
    // device-CRL entries stay: the directory may come back before the flip commits, and a copy loaded without them
    // would answer for transactions whose state it lost. A config whose stat fails is no absence (FileStat): the
    // store is skipped until a stat answers. A store served, opening or retiring has its directory, as does one a
    // shutdown closed, so those are skipped only as a guard.
    //
    // A declared store stays declared while its entry is Active here and it is not served: its rebuild is requested
    // again at each reconcile even once its directory is back, since a request that failed would otherwise leave it
    // offline, with no pass to load or rebuild it, until a restart.
    val unserved = !f.servedOnHost && !open && !f.openingOnHost && !f.retiringOnHost
    if active && unserved && (f.declaredMissing || f.configPresent.contains(false)) then
      out += DeclareMissing(first = !f.declaredMissing)

    // A store whose copy is Lost (crlSaveFileLost, a failed backend, or an adoption) whose entry still reads Active is
    // flipped to Rebuilding; the Rebuilding rule then discards its directory and rebuilds it. A store still being
    // shipped (its transfer aborted while the send runs) waits, re-checked shortly: its rebuild's discard would move
    // the directory the send may still be reading (I3, one piece of work per (device, store)).
    if offline && phase.contains(Phase.Lost) && active then
      out += (if f.sending then Recheck else RequestRebuild)

    // Nothing else ever moves an Outbound copy back to Live at the source, and a transfer that completes normally has
    // the source's copy retired. So a transfer that ends without completing, which is what a destination declared
    // failed mid-flight produces, strands the copy in outbound/ forever, and every restart keeps it offline there.
    // A parked copy (an AdoptOffline rebuild's, in parked/) is brought into service the same way (T5) once its entry
    // is Active with no transfer device.
    //
    // That pairs badly with the metadata repair for the same event: it puts the entry back to Active and leaves the
    // pool naming this device, so afterwards the metadata reads entirely healthy while the slice is served by nobody.
    // Repairing the metadata alone would trade a visibly stalled TransferringOut entry for an invisible outage. It is
    // done on the source host's own poll because only this host can touch this disk.
    //
    // The guard is narrow on purpose. Active *and* no transferDevice is the only combination that says no transfer is
    // outstanding; an entry still TransferringOut, or Active with a destination still named, means outbound/ is doing
    // its job. `sending` is the same question asked of this process rather than the tree, and covers the window where
    // the drain restores the entry while this host is still shipping bytes. A store still sending is left alone but
    // re-checked shortly: the send ends with nothing to prompt another check for a full checkStorageDevicePeriod.
    val outboundOrParked = phase.exists:
      case Phase.Outbound(_) | Phase.Parked => true
      case _ => false
    if offline && outboundOrParked && entry.exists(noTransferOut) then
      out += (if f.sending then Recheck else ReinstateOut)

    // A crash between the pool update committing and the copy's move out of arrived/ leaves an Active entry naming
    // this device over a copy that tryLoadStore skips. The entry is no longer TransferringIn, so the transfer's start
    // never sees it. Active is the tree's word that the pool now names this device, so the copy is ours to load. A
    // move that failed in recordTransferIn is retried here too. The store's receive attempt (on any device of the
    // host) covers the window where this process's own metadata step has committed but has not yet moved the copy.
    val arrived = phase.exists(_.isInstanceOf[Phase.Arrived])
    if offline && arrived && active && !f.receivingOnHost then
      out += BringArrivedLive

    // A crash between a rebuild's flip committing and its copy's move out of rebuilt/ leaves an Active entry over a
    // copy that tryLoadStore skips; the rebuild's completion, which would move it, is gone. Active is the tree's word
    // that the flip committed, so the copy is ours to load. A rebuild still running here moves its own copy once its
    // flip commits. A move that failed here or in the rebuild's completion is retried too.
    val rebuilt = phase.exists(_.isInstanceOf[Phase.Rebuilt])
    if offline && rebuilt && active && !f.rebuildReadsOnHost then
      out += BringRebuiltLive

    // A copy held back from loading for a move whose sync failed is loaded once it is synced again: the removal's own
    // load was held back (an unsynced copy is never loaded), and the rule that made it keys off the phase it left.
    // Only a Live copy still offline and free of work, whose entry is Active; one that is not yet stays held, for
    // every later check, until a load opens it or it leaves its path or device.
    if f.heldForSync && offline && in.copy.exists(!_.unsynced) && phase.contains(Phase.Live) && active then
      out += LoadResynced

    // New stores. Other work holds the store: its end checks the device again.
    if status.contains(StoreStatus.Initializing) && work.forall(_ == Work.Create) then
      out += Create

    // An abort removes this device's TransferringIn entry and tells nobody, so this check is the only thing that
    // learns of it. Left alone, an attempt still waiting on bytes keeps retransmitting its request and holds the
    // store's receive attempt, which would make the transfer-in start skip a later transfer of the same store, so this
    // runs before the transfer-in start (StartTransferIn). An attempt past promotion is not covered here; updateStateForTransferredStore sees the
    // abort in the source's entry instead.
    //
    // The same store can be re-targeted at another device of this host. A receive attempt is held per store, so the
    // earlier attempt would block the new one until its own device's next check noticed the abort. This device's
    // entry is the newer word on who receives the store, so the other device's attempt is dropped here instead of a
    // poll later.
    val transferringIn = status.contains(StoreStatus.TransferringIn)
    // One drop per (from, to): a store may be tracked in both pendingStartTransfers and transferInRetries
    in.facts.tracked.distinct.foreach: t =>
      if t.toDevice == in.deviceId then
        if !(transferringIn && entry.exists(_.transferDevice.contains(t.fromDevice))) then
          out += DropTransferIn(t.fromDevice, t.toDevice)
      else if transferringIn then
        out += DropTransferIn(t.fromDevice, t.toDevice)

    // Rebuild work for stores no longer Rebuilding here (re-placed elsewhere, rebuild-process 4.2) is cancelled first:
    // a transfer in of such a store waits while that work is registered on the device. A running rebuild without its
    // handle has nothing to cancel.
    work match
      case Some(Work.Rebuild(episode, RebuildStage.Running)) =>
        val current = rebuilding && entry.exists(_.rebuildEpisode == episode)
        if !current && f.rebuildHandle then
          out += CancelRebuild
      case _ => ()

    // Starts, resumes or abandons the transfer in of each TransferringIn entry. Other work holds the store.
    if transferringIn && work.forall(_.isInstanceOf[Work.TransferIn]) then
      entry.flatMap(_.transferDevice).foreach: from =>
        out += StartTransferIn(from, entry.flatMap(_.transferId))

    // Starts the rebuild of each Rebuilding entry. Other work holds the store, and its end checks the device again (a
    // transfer out's close too, whatever its outcome: StoreTransfersOut), which starts it.
    if rebuilding && work.forall(_.isInstanceOf[Work.Rebuild]) then
      entry.foreach: e =>
        out += StartRebuild(e.rebuildEpisode, e.acceptLoss)

    out.result()

  /** Whether an entry says no transfer out of its store is outstanding: Active, no destination named. A copy in
   *  outbound/ under it was left by a transfer that ended without completing */
  def noTransferOut(entry: StorageDeviceState.StoreEntry): Boolean =
    entry.status == StoreStatus.Active && entry.transferDevice.isEmpty

  enum ReleaseDecision:
    case Release, Keep, AskOwner

  /** What to do with a pending-delete copy of a store whose entry here is `entryHere`, the device holding a Lost or
   *  pendingLost copy of it iff `lostHere` (StoreCopies.release's rule): released once the store is Active on its
   *  owner device and no lost copy here still needs it, kept while the entry says otherwise, and the owner device
   *  asked when this device has no entry */
  def release(entryHere: Option[StorageDeviceState.StoreEntry], lostHere: Boolean): ReleaseDecision =
    entryHere match
      case Some(e) if e.status == StoreStatus.Active && !lostHere => ReleaseDecision.Release
      case Some(_) => ReleaseDecision.Keep
      case None => ReleaseDecision.AskOwner
