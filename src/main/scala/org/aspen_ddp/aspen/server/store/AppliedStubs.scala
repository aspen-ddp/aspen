package org.aspen_ddp.aspen.server.store

import org.aspen_ddp.aspen.common.HLCTimestamp
import org.aspen_ddp.aspen.common.transaction.TransactionId

import scala.concurrent.duration.Duration

/** The transactions a store holds an applied stub for (StoreRecoveryState), with their start timestamps: the store
  * holds their updates. Loaded at recovery, dropped once past their expiry (`dueForDrop`). Written only on the store's
  * executor; `@volatile` so tests may read `contains` from their own thread.
  *
  * Each stub expires at the transaction's start plus the horizon plus the expiry margin (as Tx.pastHorizon). The CRL
  * and admission side effects of recording and dropping a stub stay with the Frontend.
  */
final class AppliedStubs(horizon: Duration, expiryMargin: Duration):
  @volatile private var stubs: Map[TransactionId, HLCTimestamp] = Map()

  /** Each stub's expiry (`expiresAt`) and txid, soonest first, so a heartbeat looks only at the stubs due. One entry
    * per stub recorded; an entry whose stub is gone, or carries another start, is stale and skipped when it comes up
    */
  private val expiry = scala.collection.mutable.PriorityQueue[(Long, TransactionId)]()(
    Ordering.by[(Long, TransactionId), Long](_._1).reverse)

  /** Stubs past their expiry that a Tx for their transaction still kept, rechecked at each heartbeat */
  private val kept = scala.collection.mutable.HashSet[TransactionId]()

  /** The wall-clock milliseconds past which a stub for a transaction started at `start` may be dropped */
  private def expiresAt(start: HLCTimestamp): Long = start.wallTime + horizon.toMillis + expiryMargin.toMillis

  def contains(txid: TransactionId): Boolean = stubs.contains(txid)

  def all: Map[TransactionId, HLCTimestamp] = stubs

  def startOf(txid: TransactionId): HLCTimestamp = stubs(txid)

  /** Holds a stub not already held, queued for its expiry. Returns whether it is new */
  def add(txid: TransactionId, start: HLCTimestamp): Boolean =
    if stubs.contains(txid) then
      false
    else
      stubs += (txid -> start)
      expiry.enqueue((expiresAt(start), txid))
      true

  /** Drops the stub for `txid`. Returns whether the store held one */
  def remove(txid: TransactionId): Boolean =
    if stubs.contains(txid) then
      stubs -= txid
      kept -= txid
      true
    else
      false

  /** Removes and returns the stubs past their expiry for which `txHeld` is false. Only the stubs due are looked at,
    * plus those a Tx kept past theirs, so a heartbeat costs O(k log n) for k stubs due, not a pass over all n
    */
  def dueForDrop(now: Long, txHeld: TransactionId => Boolean): List[TransactionId] =
    val dropped = List.newBuilder[TransactionId]
    kept.toList.foreach: txid =>
      if !txHeld(txid) then
        remove(txid)
        dropped += txid
    while expiry.nonEmpty && expiry.head._1 < now do
      val (due, txid) = expiry.dequeue()
      stubs.get(txid) match
        case Some(start) if expiresAt(start) == due =>
          if txHeld(txid) then
            kept += txid
          else
            remove(txid)
            dropped += txid
        case _ => // Stale: the stub was dropped, or is held again under its own entry
    dropped.result()
