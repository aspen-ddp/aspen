package org.aspen_ddp.aspen.server.transaction

import com.github.blemale.scaffeine.{Cache, Scaffeine}
import org.aspen_ddp.aspen.common.transaction.{TransactionId, TransactionStatus}

import scala.concurrent.duration.{FiniteDuration, SECONDS}

object TransactionStatusCache {
  case class Entry(status: TransactionStatus.Value, finalized: Boolean, late: Boolean = false)

  /** A safety net of about 700 MB, at about 175 bytes an entry. An evicted entry behaves like an expired one */
  val DefaultMaxEntries: Long = 4_000_000
}

class TransactionStatusCache(cacheDuration: FiniteDuration = FiniteDuration(30, SECONDS),
                             maxEntries: Long = TransactionStatusCache.DefaultMaxEntries) {
  import TransactionStatusCache._

  private val cache: Cache[TransactionId,Entry] =
    Scaffeine().expireAfterWrite(cacheDuration).maximumSize(maxEntries).build[TransactionId, Entry]()

  /** Records what is known of `txid`. Monotone, atomically (Ruling 30): `finalized` and `late` are sticky, and a
    * resolved status (Committed or Aborted) never reverts to Unresolved. Several stores of a host write the same
    * entry, and a write from one that learned less (a Tx created, or a delayed non-late resolution) must not erase
    * what another recorded: the finalized relay (Store) reports from it.
    *
    * Opposite outcomes arise only from a split decision: a forgotten transaction decided again by a later round
    * (paxos F2, a false abort of a commit; P-N4, an abort decided again as a commit). Committed wins, because a commit is
    * what the stores hold and nothing rolls it back. An abort never replaces a commit, and the flags describe the
    * outcome they came with: an abort's `finalized` and `late` do not pass to a commit that replaces it. Otherwise the
    * relay would spread the false abort as finalized, or report a commit still being finalized as late, so that its
    * driver skipped the finalization actions
    */
  def updateStatus(txid: TransactionId, status: TransactionStatus.Value, finalized: Boolean=false, late: Boolean=false): Unit = {
    cache.underlying.asMap().compute(txid, (_, old) =>
      if old == null then
        Entry(status, finalized, late)
      else if status == TransactionStatus.Aborted && old.status == TransactionStatus.Committed then
        old
      else if status == TransactionStatus.Committed && old.status == TransactionStatus.Aborted then
        Entry(status, finalized, late)
      else
        val merged = if status == TransactionStatus.Unresolved then old.status else status
        Entry(merged, old.finalized || finalized, old.late || late))
  }

  def getStatus(txid: TransactionId): Option[Entry] = {
    cache.getIfPresent(txid)
  }

  private[aspen] def cleanUpForTest(): Unit = cache.cleanUp()

  /** Drops `txid`'s entry, as a size eviction would */
  private[aspen] def evictForTest(txid: TransactionId): Unit = cache.invalidate(txid)

  private[aspen] def size: Long = {
    cache.cleanUp()
    cache.estimatedSize()
  }
}
