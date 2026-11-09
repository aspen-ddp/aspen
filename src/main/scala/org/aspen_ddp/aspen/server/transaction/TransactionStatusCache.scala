package org.aspen_ddp.aspen.server.transaction

import com.github.benmanes.caffeine.cache.RemovalCause
import com.github.blemale.scaffeine.{Cache, Scaffeine}
import org.aspen_ddp.aspen.common.transaction.{TransactionId, TransactionStatus}
import scribe.Logging

import scala.concurrent.duration.{Duration, FiniteDuration, MINUTES, SECONDS}

object TransactionStatusCache {
  case class Entry(status: TransactionStatus.Value, finalized: Boolean):
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

  // Guarded by `this`: the removal listener runs on the cache's maintenance executor
  private var evictedEarly: Long = 0L
  private var evictedSinceWarning: Long = 0L
  private var lastWarning: Option[Long] = None

  private def evicted(entry: Entry, cause: RemovalCause): Unit =
    earlyEvictionAge.foreach: age =>
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

  private val cache: Cache[TransactionId,Entry] = Scaffeine()
    .expireAfterWrite(cacheDuration)
    .maximumSize(maxEntries)
    .removalListener[TransactionId, Entry]((_, entry, cause) => evicted(entry, cause))
    .build[TransactionId, Entry]()

  def updateStatus(txid: TransactionId, status: TransactionStatus.Value, finalized: Boolean=false): Unit = {
    cache.put(txid, Entry(status, finalized))
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
