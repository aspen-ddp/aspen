package org.aspen_ddp.aspen.server.transaction

import com.github.blemale.scaffeine.{Cache, Scaffeine}
import com.github.benmanes.caffeine.cache.{RemovalCause, Ticker}
import org.aspen_ddp.aspen.common.transaction.{TransactionHorizon, TransactionId, TransactionStatus}
import org.aspen_ddp.aspen.common.util.LogRateLimit
import scribe.Logging

import java.util.concurrent.atomic.{AtomicLong, AtomicReference}
import scala.concurrent.duration.{FiniteDuration, SECONDS}

object TransactionStatusCache {
  case class Entry(status: TransactionStatus.Value, finalized: Boolean, late: Boolean = false)

  /** A safety net of about 700 MB, at about 175 bytes an entry. An evicted entry behaves like an expired one */
  val DefaultMaxEntries: Long = 4_000_000

  /** How often, at most, a size eviction is logged (recordSizeEviction). Under the overload that fills the cache,
    * about one entry is evicted per insert
    */
  val EvictionLogPeriod: FiniteDuration = FiniteDuration(10, SECONDS)
}

class TransactionStatusCache(cacheDuration: FiniteDuration = FiniteDuration(30, SECONDS),
                             maxEntries: Long = TransactionStatusCache.DefaultMaxEntries,
                             wallClockMillis: () => Long = () => System.currentTimeMillis(),
                             ticker: Ticker = Ticker.systemTicker()) extends Logging {
  import TransactionStatusCache._

  private val watermark = new AtomicLong(0)
  private val sizeEvictionCount = new AtomicLong(0)
  private val splitDecisionCount = new AtomicLong(0)
  // A size eviction is logged once per EvictionLogPeriod at most
  private val evictionLog = new LogRateLimit(EvictionLogPeriod.toMillis, wallClockMillis)

  /** Zero until a size eviction. Then the wall clock at the latest size eviction plus MaxStartLead (monotone): a
    * transaction whose startTimestamp is not past it may have had an entry evicted, so a store must not trust the
    * absence of an entry for it. Expiry by time does not raise it: entries outlast the horizon (Host).
    */
  def evictionWatermarkMillis: Long = watermark.get()

  /** The number of size evictions so far */
  private[aspen] def sizeEvictions: Long = sizeEvictionCount.get()

  /** The number of opposite outcomes recorded so far (updateStatus): split decisions, which should never arise */
  private[aspen] def splitDecisions: Long = splitDecisionCount.get()

  private def recordSizeEviction(txid: TransactionId): Unit =
    val mark = wallClockMillis() + TransactionHorizon.MaxStartLead.toMillis
    watermark.accumulateAndGet(mark, math.max)
    val total = sizeEvictionCount.incrementAndGet()
    evictionLog.admit().foreach: held =>
      logger.error(s"Transaction status cache is full: evicted the entry of $txid before it expired. $total size " +
        s"evictions so far${LogRateLimit.heldText(held)}. Lower the transaction rate or raise the cache size")

  // The listener runs synchronously with the removal, so a reader that finds the entry gone sees the raised watermark
  private val cache: Cache[TransactionId,Entry] =
    Scaffeine().expireAfterWrite(cacheDuration).maximumSize(maxEntries).ticker(ticker)
      .evictionListener((txid: TransactionId, _: Entry, cause: RemovalCause) =>
        if cause == RemovalCause.SIZE then recordSizeEviction(txid))
      .build[TransactionId, Entry]()

  /** Records what is known of `txid`. Monotone, atomically (paxos-transactions 10.6): `finalized` and `late` are
    * sticky, and a resolved status (Committed or Aborted) never reverts to Unresolved. Several stores of a host write
    * the same entry, and a write from one that learned less (a Tx created, or a delayed non-late resolution) must not
    * erase what another recorded: the finalized relay (Store) reports from it.
    *
    * Opposite outcomes should never arise (transaction horizon, 2026-11-13): no store joins a transaction it may have
    * forgotten (Frontend.forgetFloorMillis), so a forgotten transaction is never decided again by a later round. One that
    * does arise is a split decision (a false abort of a commit, or an abort decided again as a
    * commit; P19): it is logged as an error and counted (splitDecisions). Committed wins, because a commit is what the
    * stores hold and nothing rolls it back. An abort never replaces a commit, and the flags describe the outcome they
    * came with: an abort's `finalized` and `late` do not pass to a commit that replaces it. Otherwise the relay would
    * spread the abort as finalized, or report a commit still being finalized as late, so that its driver skipped the
    * finalization actions
    */
  def updateStatus(txid: TransactionId, status: TransactionStatus.Value, finalized: Boolean=false, late: Boolean=false): Unit = {
    var opposite: Option[TransactionStatus.Value] = None
    cache.underlying.asMap().compute(txid, (_, old) =>
      if old == null then
        Entry(status, finalized, late)
      else if status == TransactionStatus.Aborted && old.status == TransactionStatus.Committed then
        opposite = Some(old.status)
        old
      else if status == TransactionStatus.Committed && old.status == TransactionStatus.Aborted then
        opposite = Some(old.status)
        Entry(status, finalized, late)
      else
        val merged = if status == TransactionStatus.Unresolved then old.status else status
        Entry(merged, old.finalized || finalized, old.late || late))
    opposite.foreach: recorded =>
      recordSplitDecision(txid, s"recorded $recorded and now reported $status in the status cache. Committed is kept")
  }

  /** Logs a split decision of `txid` as an error and counts it (splitDecisions). Called by the cache itself on an
    * opposite outcome, and by Txs and drivers of the host that are given a conflicting second outcome (P19)
    */
  def recordSplitDecision(txid: TransactionId, detail: String): Unit =
    splitDecisionCount.incrementAndGet()
    logger.error(s"Split decision: transaction $txid: $detail")

  def getStatus(txid: TransactionId): Option[Entry] = {
    cache.getIfPresent(txid)
  }

  private[aspen] def cleanUpForTest(): Unit = cache.cleanUp()

  /** Drops `txid`'s entry, as a size eviction would */
  private[aspen] def evictForTest(txid: TransactionId): Unit =
    cache.invalidate(txid)
    recordSizeEviction(txid)

  private[aspen] def size: Long = {
    cache.cleanUp()
    cache.estimatedSize()
  }
}
