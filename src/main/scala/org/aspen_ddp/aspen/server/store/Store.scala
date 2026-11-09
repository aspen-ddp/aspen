package org.aspen_ddp.aspen.server.store

import org.aspen_ddp.aspen.common.HLCTimestamp

import com.github.blemale.scaffeine.Scaffeine
import org.aspen_ddp.aspen.common.network.*
import org.aspen_ddp.aspen.common.objects.{ObjectId, ObjectPointer}
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.transaction.{TransactionDescription, TransactionId, TransactionStatus}
import org.aspen_ddp.aspen.common.util.BackgroundTaskManager
import org.aspen_ddp.aspen.server.crl.{CrashRecoveryLog, CrlAdmission}
import org.aspen_ddp.aspen.server.network.Messenger
import org.aspen_ddp.aspen.server.store.backend.Backend
import org.aspen_ddp.aspen.server.store.cache.ObjectCache
import org.aspen_ddp.aspen.server.transaction.{TransactionDriver, TransactionFinalizer, TransactionStatusCache}
import scribe.Logging
import org.aspen_ddp.aspen.client.ObjectState as ClientObjectState
import org.aspen_ddp.aspen.common.metadata.StorageDeviceId

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.concurrent.duration.*

class Store(val storageDeviceId: StorageDeviceId,
            val executionContext: ExecutionContext,
            val backend: Backend,
            val objectCache: ObjectCache,
            val net: Messenger,
            val backgroundTasks: BackgroundTaskManager,
            val crl: CrashRecoveryLog,
            val txStatusCache: TransactionStatusCache,
            val finalizerFactory: TransactionFinalizer.Factory,
            val txDriverFactory: TransactionDriver.Factory,
            val txHeartbeatTimeout: Duration,
            /** The store's effective slice size limit (StoreConfig.effectiveMaxSliceSize). None means unlimited. */
            val maxSliceSize: Option[Long],
            /** The admission of the store's device's CRL, shared by the device's stores */
            val admission: CrlAdmission = CrlAdmission.Unlimited,
            /** Looks up recovered commit votes in the pool's UnackedCommits tree (Frontend.requestUnackedLookup) */
            val unackedLookup: UnackedCommitLookup = UnackedCommitLookup.Never,
            /** Posts a task to the executor of the store installed under this store's id (StoreRouter.post). None
              * runs tasks inline, for tests with synchronous backends
              */
            val postToStore: Option[(Store => Unit) => Unit] = None,
            /** Unknown prepares older than this vote abort unchecked (Frontend.horizon). Infinite: off */
            val horizon: Duration = Duration.Inf,
            /** A rebuilt store's acceptor start (Frontend.acceptorSince). None: an ordinary store */
            val acceptorSince: Option[HLCTimestamp] = None,
            /** Recover the CRL's transactions only when frontend.recover is called (Frontend.deferRecovery) */
            deferRecovery: Boolean = false) extends Logging {

  val storeId: StoreId = backend.storeId
  val frontend = new Frontend(backend.storeId, backend, objectCache, net, crl, txStatusCache, maxSliceSize,
    admission = admission, unackedLookup = unackedLookup,
    // A reload installs a new Store under the same id: a task of this one's is dropped then
    post = postToStore match
      case None => task => task()
      case Some(postTo) => task => postTo(s => if s eq this then task()),
    horizon = horizon, acceptorSince = acceptorSince, deferRecovery = deferRecovery)

  /** Every method of a Store runs on its store executor (StoreRouter), so no lock guards its state. The one
   *  exception is this map: `hasTransactions` and `logTransactionStatus` read it from other threads (TestNetwork's
   *  checker thread, through Host.hasTransactions), so it is an immutable Map in a `@volatile` field and those two
   *  read one snapshot of it. TransactionDrivers keep their own locks for their timer threads, and are never handed
   *  the Store, so those threads never reach its state. */
  @volatile private var transactionDrivers: Map[TransactionId, TransactionDriver] = Map()

  def close(): Future[Unit] = frontend.close()

  def estimateSize(): Long = backend.estimateSize()

  def heartbeat(): Unit = {
    transactionDrivers.values.foreach { td =>
      td.heartbeat()
    }
    frontend.transactions.values.foreach(_.checkCollisionWait())
    frontend.retryUnackedLookups()
    frontend.transactions.values.foreach { tx =>
      tx.reportIfStuck()
      if (tx.durationSinceLastEvent > txHeartbeatTimeout && !transactionDrivers.contains(tx.transactionId))
        driveTransaction(tx.txd)
    }
  }

  private val prepareResponseCache = Scaffeine().expireAfterWrite(10.seconds)
    .maximumSize(1000)
    .build[TransactionId, List[TxPrepareResponse]]()

  /** Starts a driver for `txd`, unless one runs or the store cannot yet know whether it owes the transaction.
    *
    * A recovered unresolved commit vote waiting for its first UnackedCommits lookup (Tx.awaitsFirstUnackedLookup), or
    * any transaction while the store's CRL state is still being loaded (Frontend.recoveryPending: such a vote may be
    * among it), gets no driver yet. The other stores may have finalized and forgotten the transaction, and a driver
    * without the proof an entry gives could decide abort on their votes, a decision a later proof cannot undo. The
    * heartbeat starts the driver once the lookup has answered (Ruling 20). A lookup that never completes leaves the Tx
    * waiting, holding its locks, as the design intends for an unavailable lookup
    */
  def driveTransaction(txd: TransactionDescription): Unit = {
    val awaitsLookup = frontend.recoveryPending ||
      frontend.transactions.get(txd.transactionId).exists(_.awaitsFirstUnackedLookup)
    if (!transactionDrivers.contains(txd.transactionId) && !awaitsLookup) {

      val driver = txDriverFactory.create(executionContext, storeId, net, backgroundTasks, txd, finalizerFactory)

      transactionDrivers += txd.transactionId -> driver

      // A recovered commit vote that found its UnackedCommits entry knows the transaction committed: its driver holds
      // that proof from the start, exactly as if a prover had sent it (Tx.foundUnackedEntry). The other stores may
      // have forgotten the transaction, so deciding abort on their votes would be wrong. Given before the cached
      // prepare responses below, which could otherwise complete a tally at the initial proposal first
      if frontend.transactions.get(txd.transactionId).exists(_.provesCommitToItsDriver) then
        driver.receiveTxResolved(TxResolved(storeId, storeId, txd.transactionId, committed = true, late = false,
          proof = true))

      prepareResponseCache.getIfPresent(txd.transactionId).foreach { lst =>
        lst.foreach(driver.receiveTxPrepareResponse(_))
        prepareResponseCache.invalidate(txd.transactionId)
      }
    }
  }

  def snapshotForRepair(pointer: ObjectPointer, p: Promise[RepairSnapshot]): Unit =
    frontend.snapshotForRepair(pointer, p)

  def reconcileForRepair(snapshot: RepairSnapshot, read: ClientObjectState, p: Promise[ReconcileResult]): Unit =
    frontend.reconcileForRepair(snapshot, read, p)

  def repairDelete(objectId: ObjectId,
                   storePointer: Array[Byte],
                   completion: Promise[Unit]): Unit = {
    frontend.deleteObjectForRepair(objectId, storePointer, completion)
  }

  /** Callable from any thread: reads one snapshot of `transactionDrivers` */
  def hasTransactions: Boolean = transactionDrivers.nonEmpty

  /** Callable from any thread: see Frontend.mayHoldTransaction */
  def holdsTransaction(txid: TransactionId): Boolean = frontend.mayHoldTransaction(txid)

  /** Callable from any thread: reads one snapshot of `transactionDrivers`; printState takes each driver's own lock */
  def logTransactionStatus(log: String => Unit): Unit =
    transactionDrivers.values.foreach(_.printState(log))

  def receiveTransactionMessage(msg: TxMessage): Unit = msg match {
    // Backpressure: while the device's CRL is filling, a prepare for a transaction this store has not seen is dropped
    // without a reply. The client retransmits; other stores may commit it without this one, which repair catches up
    case m: TxPrepare if !frontend.admitPrepare(m) =>
      logger.trace(s"Store $storeId dropped the prepare of unseen Tx ${m.txd.transactionId}: its device's CRL is filling")
    case _ => handleTransactionMessage(msg)
  }

  private def handleTransactionMessage(msg: TxMessage): Unit = {

    msg match {
      case m: TxPrepare =>

        // The finalized relay. A committed transaction finalized here is reported late: the prepare is for a transaction
        // whose finalization completed, so its driver runs no finalization actions and stores apply the commit only where
        // it is owed (TransactionDriver.resolveLate). A cached entry is finalized only from a real TxFinalized or a late
        // outcome
        val notFinalized = txStatusCache.getStatus(m.txd.transactionId).forall { entry =>
          val late = entry.late || entry.finalized
          val (r, committed: Boolean) = entry.status match {
            case TransactionStatus.Unresolved => (None, false)
            case TransactionStatus.Aborted =>
              (Some(TxResolved(msg.to, msg.from, m.txd.transactionId, committed = false, late = false, proof = false)), false)
            case TransactionStatus.Committed =>
              (Some(TxResolved(msg.to, msg.from, m.txd.transactionId, committed = true, late = late, proof = false)), true)
          }

          r.foreach(resolved => net.sendTransactionMessage(resolved))

          if (entry.finalized) {
            net.sendTransactionMessage(TxFinalized(msg.from, msg.to, m.txd.transactionId, committed, late = committed))

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
          case Some(driver) => driver.receiveTxPrepareResponse(m)
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

      case m: TxUnknownStore => transactionDrivers.get(m.transactionId).foreach { driver =>
        driver.receiveTxUnknownStore(m)
      }

      case _ =>
    }

    frontend.receiveTransactionMessage(msg)
  }
}
