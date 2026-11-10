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
    * what another recorded: the finalized relay (Store) reports from it
    */
  def updateStatus(txid: TransactionId, status: TransactionStatus.Value, finalized: Boolean=false, late: Boolean=false): Unit = {
    cache.underlying.asMap().compute(txid, (_, old) =>
      if old == null then
        Entry(status, finalized, late)
      else
        val merged = if status == TransactionStatus.Unresolved then old.status else status
        Entry(merged, old.finalized || finalized, old.late || late))
  }

  def getStatus(txid: TransactionId): Option[Entry] = {
    cache.getIfPresent(txid)
  }

  private[aspen] def cleanUpForTest(): Unit = cache.cleanUp()

  private[aspen] def size: Long = {
    cache.cleanUp()
    cache.estimatedSize()
  }
}
