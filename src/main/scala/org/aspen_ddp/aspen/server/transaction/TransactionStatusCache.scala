package org.aspen_ddp.aspen.server.transaction

import com.github.blemale.scaffeine.{Cache, Scaffeine}
import org.aspen_ddp.aspen.common.transaction.{TransactionId, TransactionStatus}

import scala.concurrent.duration.{Duration, FiniteDuration, SECONDS}

object TransactionStatusCache {
  case class Entry(status: TransactionStatus.Value, finalized: Boolean)

  /** A safety net of about 700 MB, at about 175 bytes an entry. An evicted entry behaves like an expired one */
  val DefaultMaxEntries: Long = 4_000_000
}

class TransactionStatusCache(cacheDuration: FiniteDuration = FiniteDuration(30, SECONDS),
                             maxEntries: Long = TransactionStatusCache.DefaultMaxEntries) {
  import TransactionStatusCache._

  private val cache: Cache[TransactionId,Entry] = Scaffeine()
    .expireAfterWrite(cacheDuration)
    .maximumSize(maxEntries)
    .build[TransactionId, Entry]()

  def updateStatus(txid: TransactionId, status: TransactionStatus.Value, finalized: Boolean=false): Unit = {
    cache.put(txid, Entry(status, finalized))
  }

  def getStatus(txid: TransactionId): Option[Entry] = {
    cache.getIfPresent(txid)
  }

  private[aspen] def size: Long = {
    cache.cleanUp()
    cache.estimatedSize()
  }
}
