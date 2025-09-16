package org.aspen_ddp.aspen.server.store

import com.github.blemale.scaffeine.Scaffeine
import org.aspen_ddp.aspen.common.network.*
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.transaction.{TransactionDescription, TransactionId, TransactionStatus}
import org.aspen_ddp.aspen.common.util.BackgroundTask
import org.aspen_ddp.aspen.server.crl.CrashRecoveryLog
import org.aspen_ddp.aspen.server.network.Messenger
import org.aspen_ddp.aspen.server.store.backend.Backend
import org.aspen_ddp.aspen.server.store.cache.ObjectCache
import org.aspen_ddp.aspen.server.transaction.{TransactionDriver, TransactionFinalizer, TransactionStatusCache}
import org.apache.logging.log4j.scala.Logging
import org.aspen_ddp.aspen.client.ObjectState as ClientObjectState

import scala.concurrent.{Future, Promise}
import scala.concurrent.duration.*

class Store(val backend: Backend,
            val objectCache: ObjectCache,
            val net: Messenger,
            val backgroundTasks: BackgroundTask,
            val crl: CrashRecoveryLog,
            val txStatusCache: TransactionStatusCache,
            val finalizerFactory: TransactionFinalizer.Factory,
            val txDriverFactory: TransactionDriver.Factory,
            val txHeartbeatTimeout: Duration) extends Logging {

  val storeId: StoreId = backend.storeId
  val frontend = new Frontend(backend.storeId, backend, objectCache, net, crl, txStatusCache)

  private var transactionDrivers: Map[TransactionId, TransactionDriver] = Map()
  
  def close(): Future[Unit] = frontend.close()

  def heartbeat(): Unit = {
    transactionDrivers.values.foreach { td =>
      td.heartbeat()
    }
    frontend.transactions.values.foreach { tx =>
      if (tx.durationSinceLastEvent > txHeartbeatTimeout && !transactionDrivers.contains(tx.transactionId))
        driveTransaction(tx.txd)
    }
  }

  private val prepareResponseCache = Scaffeine().expireAfterWrite(10.seconds)
    .maximumSize(1000)
    .build[TransactionId, List[TxPrepareResponse]]()

  def driveTransaction(txd: TransactionDescription): Unit = synchronized {
    if (!transactionDrivers.contains(txd.transactionId)) {

      val driver = txDriverFactory.create(storeId, net, backgroundTasks, txd, finalizerFactory)

      transactionDrivers += txd.transactionId -> driver

      prepareResponseCache.getIfPresent(txd.transactionId).foreach { lst =>
        lst.foreach(driver.receiveTxPrepareResponse(_, txStatusCache))
        prepareResponseCache.invalidate(txd.transactionId)
      }
    }
  }

  def repair(os: ClientObjectState, completion: Promise[Unit]) = synchronized {
    frontend.readObjectForRepair(os, completion)
  }

  def hasTransactions: Boolean = synchronized { transactionDrivers.nonEmpty }

  def logTransactionStatus(log: String => Unit): Unit = synchronized {
    transactionDrivers.values.foreach(_.printState(log))
  }

  def receiveTransactionMessage(msg: TxMessage): Unit = synchronized {

    msg match {
      case m: TxPrepare =>

        val notFinalized = txStatusCache.getStatus(m.txd.transactionId).forall { entry =>
          val (r, committed: Boolean) = entry.status match {
            case TransactionStatus.Unresolved => (None, false)
            case TransactionStatus.Aborted =>
              (Some(TxResolved(msg.to, msg.from, m.txd.transactionId, committed = false)), false)
            case TransactionStatus.Committed =>
              (Some(TxResolved(msg.to, msg.from, m.txd.transactionId, committed = true)), true)
          }

          r.foreach(resolved => net.sendTransactionMessage(resolved))

          if (entry.finalized) {
            net.sendTransactionMessage(TxFinalized(msg.from, msg.to, m.txd.transactionId, committed))

            m.txd.originatingClient.foreach { client =>
              net.sendClientResponse(TransactionFinalized(client, msg.to, m.txd.transactionId, committed))
            }

            false
          } else
            true
        }

        if (notFinalized && m.txd.primaryObject.poolId == storeId.poolId && m.txd.designatedLeaderUID == storeId.poolIndex) {
          driveTransaction(m.txd)
        }

      case m: TxPrepareResponse =>
        // cache if driver doesn't exist deliver otherwise
        transactionDrivers.get(m.transactionId) match {
          case Some(driver) => driver.receiveTxPrepareResponse(m, txStatusCache)
          case None =>
            val lst = prepareResponseCache.getIfPresent(m.transactionId).getOrElse(Nil)
            prepareResponseCache.put(m.transactionId, m :: lst)
        }

      case m: TxAcceptResponse => transactionDrivers.get(m.transactionId).foreach { driver =>
        driver.receiveTxAcceptResponse(m)
      }

      case m: TxResolved => transactionDrivers.get(m.transactionId).foreach { driver =>
        driver.receiveTxResolved(m)
      }

      case m: TxCommitted => transactionDrivers.get(m.transactionId).foreach { driver =>
        driver.receiveTxCommitted(m)
      }

      case m: TxFinalized => transactionDrivers.get(m.transactionId).foreach { driver =>
        driver.receiveTxFinalized(m)
        transactionDrivers -= m.transactionId
      }

      case _ =>
    }

    frontend.receiveTransactionMessage(msg)
  }
}
