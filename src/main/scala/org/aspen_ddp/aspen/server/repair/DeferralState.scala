package org.aspen_ddp.aspen.server.repair

import org.aspen_ddp.aspen.common.objects.{Key, ObjectRevision}
import org.aspen_ddp.aspen.server.store.ReconcileResult

/** Lock deferrals of one store's repair entries, kept in memory across its scans by RepairService. A host restart
  * forgets them: the next pass then reconciles each whole object again and restarts the count.
  *
  * Keyed per entry and object (StoreRepairer.errorDeferralKey, unackedDeferralKey). An errorTree entry's key survives
  * its rewrites by later marks, so its counts do too (observeEntry). Only parts deferred by a lock
  * count: `deferredKeys` and `objectLevelDeferred`, the latter including a refcount deferred under an object lock. A
  * pass that is only `stale` defers nothing by a lock: the object is retried whole on the next pass, but the pass
  * neither counts toward `RepairPolicy.proofAfterDeferrals` nor triggers a proof, since a proof cannot help an object
  * whose copy is busy with an outstanding write. Nor does it reset the count: on a hot object stale and lock-deferred
  * passes alternate, and only a complete pass shows the locks are gone. Stale passes are counted on their own
  * (`recordStale`): after `StoreRepairer.ParkAfterStalePasses` of them the store is asked to wait for the object to
  * settle before reconciling (RepairSnapshot.awaitSettle, repair-process 8.2), and keeps being asked until a complete
  * pass, so a parked pass that meets a lock is followed by another parked pass rather than by a stale one. Together
  * these let a hot object's lock deferrals reach a proof. Safe to call from any thread.
  */
final class DeferralState:

  private var entries: Map[Key, DeferralState.Deferral] = Map()
  private var stalePasses: Map[Key, Int] = Map()
  private var revisions: Map[Key, ObjectRevision] = Map()

  /** Records the revision of the entry a pass is about to reconcile for. A rewritten entry (a later mark) may name a
    * miss of keys the last pass did not defer, so the next pass reconciles the whole object; the counts are kept, since
    * marks of a hot object would otherwise keep it from ever reaching a proof or parking
    */
  def observeEntry(entryKey: Key, revision: ObjectRevision): Unit = synchronized:
    if revisions.get(entryKey).exists(_ != revision) then
      entries.get(entryKey).foreach(d => entries += entryKey -> d.copy(only = None))
    revisions += entryKey -> revision

  /** Records whether one pass's final result was stale. Returns the count of stale passes since the last complete
    * one, this one included, or 0 when it was not stale. A complete pass resets the count; a lock-deferred one keeps
    * it, so a repair that has started waiting for the object to settle goes on waiting
    */
  def recordStale(entryKey: Key, result: ReconcileResult): Int = synchronized:
    if result.stale then
      val count = stalePasses.getOrElse(entryKey, 0) + 1
      stalePasses += entryKey -> count
      count
    else
      if result.complete then stalePasses -= entryKey
      0

  /** Stale passes of the entry's object since its last complete pass */
  def staleCount(entryKey: Key): Int = synchronized(stalePasses.getOrElse(entryKey, 0))

  /** Records one pass's reconciliation of the entry's object. Returns the count of passes in which something was
    * deferred by a lock since the last complete one, this one included, or 0 when this pass deferred nothing by a lock.
    * A complete pass resets the count and forgets the entry. A pass that is only stale keeps the count as it is (it
    * triggers no proof), and the next pass reconciles the whole object.
    */
  def record(entryKey: Key, result: ReconcileResult): Int = synchronized:
    if result.complete then
      entries -= entryKey
      0
    else if result.deferredKeys.isEmpty && !result.objectLevelDeferred then
      entries.get(entryKey) match
        case Some(d) => entries += entryKey -> d.copy(only = None)
        case None => ()
      0
    else
      val count = entries.get(entryKey).map(_.count).getOrElse(0) + 1
      // The next pass may reconcile only the deferred keys once everything else of the object is settled
      val only = Option.when(!result.objectLevelDeferred && !result.stale)(result.deferredKeys)
      entries += entryKey -> DeferralState.Deferral(count, only)
      count

  /** Records the result of the reconcile that followed a proof on the same pass, in place of that pass's pre-proof
    * result: the count is kept (the pass was already counted), the entry is forgotten if the result is complete, and
    * otherwise the next pass reconciles only what is still deferred, or the whole object after a stale result
    */
  def afterProof(entryKey: Key, result: ReconcileResult): Unit = synchronized:
    if result.complete then
      entries -= entryKey
    else
      entries.get(entryKey).foreach: d =>
        val only = Option.when(!result.objectLevelDeferred && !result.stale)(result.deferredKeys)
        entries += entryKey -> d.copy(only = only)

  def clear(entryKey: Key): Unit = synchronized:
    entries -= entryKey
    stalePasses -= entryKey
    revisions -= entryKey

  /** The keys the next pass reconciles, when the last pass deferred only keys and left nothing else undone. None
    * means the whole object
    */
  def deferredKeys(entryKey: Key): Option[Set[Key]] = synchronized:
    entries.get(entryKey).flatMap(_.only)

  /** Forgets every entry not in `entryKeys`: those a completed scan did not see are gone or were rewritten. Sweep keys
    * (StoreRepairer.sweepDeferralKey) are kept: they belong to the sweep, which prunes them with `retainSweep`
    */
  private[repair] def retain(entryKeys: Set[Key]): Unit = synchronized:
    def kept(k: Key): Boolean =
      StoreRepairer.isSweepDeferralKey(k) || entryKeys.contains(k)
    entries = entries.filter((k, _) => kept(k))
    stalePasses = stalePasses.filter((k, _) => kept(k))
    revisions = revisions.filter((k, _) => kept(k))

  /** Forgets every sweep key not in `sweepKeys`: the nodes a complete sweep did not visit are gone */
  private[repair] def retainSweep(sweepKeys: Set[Key]): Unit = synchronized:
    entries = entries.filter((k, _) => !StoreRepairer.isSweepDeferralKey(k) || sweepKeys.contains(k))

  private[repair] def size: Int = synchronized(entries.size)

object DeferralState:
  private final case class Deferral(count: Int, only: Option[Set[Key]])
