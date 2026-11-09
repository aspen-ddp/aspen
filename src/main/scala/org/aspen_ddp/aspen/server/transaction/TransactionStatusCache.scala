package org.aspen_ddp.aspen.server.transaction

import com.github.benmanes.caffeine.cache.RemovalCause
import com.github.blemale.scaffeine.{Cache, Scaffeine}
import org.aspen_ddp.aspen.common.transaction.{TransactionId, TransactionStatus}
import scribe.Logging

import scala.concurrent.duration.{Duration, FiniteDuration, MINUTES, SECONDS}

object TransactionStatusCache {
  case class Entry(status: TransactionStatus.Value, finalized: Boolean, late: Boolean = false):
    /** When the entry was made, and so written (System.nanoTime), for the early-eviction warning. Not part of equality */
    val insertedAt: Long = System.nanoTime()

  /** A safety net of about 700 MB, at about 175 bytes an entry. An evicted entry behaves like an expired one */
  val DefaultMaxEntries: Long = 4_000_000

  /** The least time between early-eviction warnings */
  val EarlyEvictionWarnEvery: FiniteDuration = FiniteDuration(1, MINUTES)
}

/** @param earlyEvictionAge an entry evicted for size while younger than this is counted, and logged at warn at most
  *                         once every `EarlyEvictionWarnEvery` with the count since the last warning. The host sets it
  *                         to its horizon (Host.txHorizon): such an eviction reopens the window the horizon closes
  */
class TransactionStatusCache(cacheDuration: FiniteDuration = FiniteDuration(30, SECONDS),
                             maxEntries: Long = TransactionStatusCache.DefaultMaxEntries,
                             earlyEvictionAge: Option[FiniteDuration] = None) extends Logging {
  import TransactionStatusCache._

  // Guarded by `this`: the eviction listener runs on whichever thread performs the cache's maintenance
  private var evictedEarly: Long = 0L
  private var evictedSinceWarning: Long = 0L
  private var lastWarning: Option[Long] = None

  private def evicted(age: FiniteDuration, entry: Entry, cause: RemovalCause): Unit =
    val now = System.nanoTime()
    if cause == RemovalCause.SIZE && now - entry.insertedAt < age.toNanos then
      val warnCount = synchronized:
        evictedEarly += 1
        evictedSinceWarning += 1
        if lastWarning.forall(last => now - last >= EarlyEvictionWarnEvery.toNanos) then
          lastWarning = Some(now)
          val n = evictedSinceWarning
          evictedSinceWarning = 0
          Some(n)
        else
          None
      warnCount.foreach: n =>
        logger.warn(s"Transaction status cache evicted $n entries younger than $age for size (max $maxEntries " +
          s"entries) since the last warning: late prepares for those transactions are no longer answered from it")

  // An eviction listener runs synchronously, inside the maintenance that evicts, and sees evictions only (not
  // replacements or explicit removals). It is registered only when there is an age to check, so other caches, the
  // client's included, pay nothing per write
  private val cache: Cache[TransactionId,Entry] =
    val base = Scaffeine().expireAfterWrite(cacheDuration).maximumSize(maxEntries)
    earlyEvictionAge match
      case Some(age) =>
        base.evictionListener[TransactionId, Entry]((_, entry, cause) => evicted(age, entry, cause))
          .build[TransactionId, Entry]()
      case None => base.build[TransactionId, Entry]()

  def updateStatus(txid: TransactionId, status: TransactionStatus.Value, finalized: Boolean=false, late: Boolean=false): Unit = {
    cache.put(txid, Entry(status, finalized, late))
  }

  def getStatus(txid: TransactionId): Option[Entry] = {
    cache.getIfPresent(txid)
  }

  /** Size evictions of entries younger than `earlyEvictionAge` */
  private[aspen] def earlyEvictions: Long = synchronized(evictedEarly)

  private[aspen] def cleanUpForTest(): Unit = cache.cleanUp()

  private[aspen] def size: Long = {
    cache.cleanUp()
    cache.estimatedSize()
  }
}
