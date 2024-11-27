package org.aspen_ddp.aspen.server.transaction

import com.github.blemale.scaffeine.{Cache, Scaffeine}
import org.aspen_ddp.aspen.common.transaction.{TransactionId, TransactionStatus}

import scala.concurrent.duration.{Duration, FiniteDuration, SECONDS}

object TransactionStatusCache {
  case class Entry(status: TransactionStatus.Value, finalized: Boolean)
}

class TransactionStatusCache(cacheDuration: FiniteDuration = FiniteDuration(30, SECONDS)) {
  import TransactionStatusCache._

  private val cache: Cache[TransactionId,Entry] = Scaffeine()
    .expireAfterWrite(cacheDuration)
    .build[TransactionId, Entry]()

  def updateStatus(txid: TransactionId, status: TransactionStatus.Value, finalized: Boolean=false): Unit = {
    cache.put(txid, Entry(status, finalized))
  }

  def getStatus(txid: TransactionId): Option[Entry] = {
    cache.getIfPresent(txid)
  }
}
