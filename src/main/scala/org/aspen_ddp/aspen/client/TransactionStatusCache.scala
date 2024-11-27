package org.aspen_ddp.aspen.client

import com.github.blemale.scaffeine.{Cache, Scaffeine}
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.transaction.TransactionId

import scala.concurrent.duration.{Duration, FiniteDuration, SECONDS}

object TransactionStatusCache {
  private sealed abstract class TxStatus

  private class Aborted extends TxStatus

  /** ourCommitErrors contains a list of object UUIDs for which we didn't meet the transaction requirements and couldn't commit */
  private class Committed(var ourCommitErrors: Map[StoreId, List[TransactionId]] = Map()) extends TxStatus

  private class Finalized extends TxStatus

  object NoCache extends TransactionStatusCache {
    override def transactionAborted(txid: TransactionId): Unit = None

    override def transactionCommitted(txid: TransactionId): Unit = None

    override def transactionFinalized(txid: TransactionId): Unit = None
  }
}

class TransactionStatusCache(cacheDuration: FiniteDuration = FiniteDuration(30, SECONDS)) {

  import TransactionStatusCache._

  private val transactionCache: Cache[TransactionId,TxStatus] = Scaffeine()
    .expireAfterWrite(cacheDuration)
    .build[TransactionId, TxStatus]()

  def transactionAborted(txid: TransactionId): Unit = transactionCache.put(txid, new Aborted)

  def transactionCommitted(txid: TransactionId): Unit = synchronized {
    transactionCache.getIfPresent(txid) match {
      case None => transactionCache.put(txid, new Committed())
      case Some(_) =>
    }
  }

  def onLocalCommit(txid: TransactionId, storeId: StoreId, commitErrors: List[TransactionId]): Unit = synchronized {
    transactionCache.getIfPresent(txid) match {
      case None => transactionCache.put(txid, new Committed(Map(storeId -> commitErrors)))
      case Some(status) => status match {
        case c: Committed => c.ourCommitErrors += (storeId -> commitErrors)
        case _ =>
      }
    }
  }

  def transactionFinalized(txuuid: TransactionId): Unit = transactionCache.put(txuuid, new Finalized)

  def getTransactionResolution(txid: TransactionId): Option[(Boolean, Option[Map[StoreId, List[TransactionId]]])] = transactionCache.getIfPresent(txid).map {
    case _: Aborted => (false, None)
    case c: Committed => synchronized {
      (true, Some(c.ourCommitErrors))
    }
    case _ => (true, None)
  }

  def getTransactionFinalizedResult(txid: TransactionId): Option[Boolean] = transactionCache.getIfPresent(txid).flatMap {
    case _: Aborted => Some(false)
    case _: Committed => None
    case _: Finalized => Some(true)
  }
}
