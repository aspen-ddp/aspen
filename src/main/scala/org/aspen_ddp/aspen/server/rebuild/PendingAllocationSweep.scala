package org.aspen_ddp.aspen.server.rebuild

import org.aspen_ddp.aspen.client.{AspenClient, InvalidObject}
import org.aspen_ddp.aspen.client.internal.CurrentAllocationsQuery
import org.aspen_ddp.aspen.common.objects.ObjectPointer
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.ida.IDA
import org.aspen_ddp.aspen.common.network.PendingAllocation
import org.aspen_ddp.aspen.common.transaction.TransactionId
import scribe.Logging

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.{FiniteDuration, SECONDS}
import scala.util.control.NonFatal

object PendingAllocationSweep:
  /** `earlier` and `now`, by transaction, with the pointers of both: an allocation listed by any pass stays listed */
  def mergeListed(earlier: Map[TransactionId, PendingAllocation],
                          now: Map[TransactionId, PendingAllocation]): Map[TransactionId, PendingAllocation] =
    now.foldLeft(earlier): (acc, entry) =>
      val (txid, p) = entry
      acc.get(txid) match
        case None => acc + entry
        case Some(e) =>
          acc + (txid -> PendingAllocation(txid, e.committed || p.committed,
                                           (e.pointers ++ p.pointers).distinctBy(_.id)))

/** The pending-allocation part of a rebuild pass (rebuild-process 5.3): asking the pool's other stores which
  * allocations they hold unfinalized, and restoring those objects directly. RebuildingStore owns the pass and the
  * object restore itself; this holds none of its state, reaching it through four callbacks.
  *
  * @param aborted whether the pass is aborted, which ends every wait
  * @param abortCause the pass's abort cause, if latched
  * @param restore restores one object: None once restored, else the read failure (RebuildingStore.restorePending)
  * @param checkpoint best-effort checkpoint of the pass's progress, given the context for its log line; never fails
  */
final class PendingAllocationSweep(storeId: StoreId,
                                   client: AspenClient,
                                   peerQueryTimeout: FiniteDuration,
                                   pendingWaitLimit: FiniteDuration,
                                   aborted: () => Boolean,
                                   abortCause: () => Option[Throwable],
                                   restore: ObjectPointer => Future[Option[Throwable]],
                                   checkpoint: String => Future[Unit])
                                  (using ec: ExecutionContext) extends Logging:

  /** The allocations this pool's other stores hold unfinalized, by transaction, once `width - writeThreshold + 1` of
   *  them have answered (rebuild-process 5.3).
   *
   *  A committed transaction is held by at least `writeThreshold` of its commit voters in this pool until TxFinalized,
   *  which comes only after its AllocationFinalizationAction recorded its objects. The lost store may have been one of
   *  them, so that many replies include a holder of every committed allocation whose entry may not have landed. It is
   *  never more than the consistent read threshold the rebuild needs anyway. Waits until the quorum answers; fails
   *  with the abort cause if the rebuild is aborted meanwhile, and with PeerQueryTimedOut if the quorum has not
   *  answered within `peerQueryTimeout` (noticed at the query's next resend), so a pool short of live peers fails the
   *  pass into the host's backoff retry rather than holding its rebuild slot.
   */
  def queryPeers(ida: IDA): Future[Map[TransactionId, PendingAllocation]] =
    val peers = (0 until ida.width).filter(_ != storeId.poolIndex.toInt)
      .map(i => StoreId(storeId.poolId, i.toByte)).toList
    val needed = ida.width - ida.writeThreshold + 1
    if needed > peers.size then
      // A single-store pool: there is no one to ask, and nothing to read the objects from either
      Future.successful(Map())
    else
      val deadline = System.nanoTime() + peerQueryTimeout.toNanos
      val stop = () => aborted() || System.nanoTime() - deadline >= 0
      client.currentAllocations(peers, needed, stop).transform:
        case scala.util.Success(replies) =>
          val merged = replies.valuesIterator.flatten.toList.groupBy(_.transactionId).map: (txid, as) =>
            txid -> PendingAllocation(txid, as.exists(_.committed), as.flatMap(_.pointers).distinctBy(_.id))
          scala.util.Success(merged)
        case scala.util.Failure(_: CurrentAllocationsQuery.Abandoned) =>
          scala.util.Failure(abortCause().getOrElse(new RebuildingStore.PeerQueryTimedOut(storeId, peerQueryTimeout)))
        case scala.util.Failure(err) => scala.util.Failure(err)

  /** Restores the objects of the allocations the peers listed before this pass's walk or an earlier pass's
   *  (rebuild-process 5.3), directly rather than through the tree, whose entries may not land until their transactions
   *  finalize, and may then land behind the walk. Each round asks the peers again:
   *  - still listed, unresolved: wait for its resolution
   *  - still listed, committed: restore each object. Any read failure is retried, since while the transaction is held
   *    its commit may not have applied yet, which reads like a deletion
   *  - no longer listed: finalized, or aborted and forgotten. A restore, or a read proving the object is not live
   *    (`InvalidObject.provesDeletion`: the allocation aborted, or the object was deleted since), settles it
   *  Transactions first listed in later rounds started after the store was lost and are covered by its missed-update
   *  marks, so they are ignored.
   *
   *  What is still unsettled at `deadlineMillis` (PendingWaitLimit after the sweep began) is marked in the ErrorTree
   *  against this store and the sweep ends (markUnsettled). The wait cannot be unbounded: a transaction whose
   *  live voters disagree is held past expiry while any of its stores is silent (ExpiryTally rule G), and this store
   *  is silent until the rebuild finishes. Dropping it unmarked is not safe either: it may be decided commit, applied
   *  and acknowledged by the lost incarnation, with its peers not knowing yet, so no finalizer marks this store.
   */
  def sweepPending(ida: IDA, pending: Map[TransactionId, PendingAllocation], round: Int,
                           deadlineMillis: Long): Future[Unit] =
    if pending.isEmpty || aborted() then
      Future.unit
    else
      def settle(p: PendingAllocation, now: Option[PendingAllocation]): Future[Option[PendingAllocation]] =
        now match
          case Some(c) if !c.committed => Future.successful(Some(p))
          case _ =>
            val gone = now.isEmpty
            Future.sequence(p.pointers.map: ptr =>
              restore(ptr).map:
                case None => None
                case Some(e: InvalidObject) if gone && e.provesDeletion => None
                case Some(_) => Some(ptr))
            .map: left =>
              val remaining = left.flatten
              if remaining.isEmpty then None else Some(p.copy(pointers = remaining))

      for
        now <- queryPeers(ida).recoverWith:
                 case e: RebuildingStore.PeerQueryTimedOut =>
                   // The walk is done: keep its progress for the next pass, which sweeps the listed set again
                   checkpoint("after a timed-out peer query").flatMap(_ => Future.failed(e))
        settled <- Future.sequence(pending.values.toList.map(p => settle(p, now.get(p.transactionId))))
        remaining = settled.flatten.map(p => p.transactionId -> p).toMap
        _ <- nextRound(ida, remaining, round, deadlineMillis)
      yield ()

  private def nextRound(ida: IDA, remaining: Map[TransactionId, PendingAllocation], round: Int,
                        deadlineMillis: Long): Future[Unit] =
    if remaining.isEmpty || aborted() then
      Future.unit
    else if System.currentTimeMillis() >= deadlineMillis then
      markUnsettled(remaining)
    else
      if round % 10 == 9 then
        logger.info(s"Rebuild of $storeId: still waiting on ${remaining.size} allocations its peers hold " +
                    s"unfinalized (${remaining.keys.take(5).mkString(", ")})")
      val delay = FiniteDuration(math.min(1L << math.min(round, 5), 30L), SECONDS)
      client.after(delay).flatMap(_ => sweepPending(ida, remaining, round + 1, deadlineMillis))

  /** Records every object of `remaining` in the pool's ErrorTree as missed by this store (missed at
   *  HLCTimestamp.Zero, so it never displaces a newer mark), durably before finish. Repair restores the slice if the
   *  allocation commits, and settles the entry by a read proving the object is not live if it aborts. A failed mark
   *  fails the pass, which is retried */
  private def markUnsettled(remaining: Map[TransactionId, PendingAllocation]): Future[Unit] =
    val marks = remaining.values.toList.flatMap(_.pointers).filter(_.poolId == storeId.poolId).map: ptr =>
      RebuildingStore.missMark(storeId, ptr.id, ptr)
    logger.warn(s"Rebuild of $storeId: ${remaining.size} allocations its peers hold are still unsettled after " +
                s"$pendingWaitLimit; recording their ${marks.size} objects for repair " +
                s"(${remaining.keys.take(5).mkString(", ")})")
    client.missedUpdateMarker.mark(marks).recoverWith:
      case NonFatal(t) =>
        checkpoint("after a failed unsettled-allocation record").flatMap: _ =>
          Future.failed(new Exception(s"Rebuild of $storeId: failed to record its unsettled allocations: $t", t))
