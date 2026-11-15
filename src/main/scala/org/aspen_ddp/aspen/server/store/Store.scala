package org.aspen_ddp.aspen.server.store

import org.aspen_ddp.aspen.common.ida.IDA
import org.aspen_ddp.aspen.common.HLCTimestamp

import com.github.blemale.scaffeine.Scaffeine
import org.aspen_ddp.aspen.common.network.*
import org.aspen_ddp.aspen.common.objects.{ObjectId, ObjectPointer}
import org.aspen_ddp.aspen.common.store.{StoreId, StoreIncarnation}
import org.aspen_ddp.aspen.common.transaction.{TransactionDescription, TransactionHorizon, TransactionId, TransactionStatus}
import org.aspen_ddp.aspen.common.util.BackgroundTaskManager
import org.aspen_ddp.aspen.server.crl.{AdmissionTier, CrashRecoveryLog, CrlAdmission}
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
            /** Verifies quarantined objects through repair (Frontend.quarantine) */
            val quarantineVerifier: QuarantineVerifier = QuarantineVerifier.Never,
            /** Posts a task to the executor of the store installed under this store's id (StoreRouter.post). None
              * runs tasks inline, for tests with synchronous backends
              */
            val postToStore: Option[(Store => Unit) => Unit] = None,
            /** A rebuilt store's acceptor start (Frontend.acceptorSince). None: an ordinary store */
            val acceptorSince: Option[HLCTimestamp] = None,
            /** Recover the CRL's transactions only when frontend.recover is called (Frontend.deferRecovery) */
            deferRecovery: Boolean = false,
            /** How long a Tx waits for its client's prepare (Frontend.missingDataWait). A test seam */
            missingDataWait: Duration = Frontend.MissingDataWait,
            /** How long after its start a transaction may be joined (Frontend.horizon) */
            horizon: FiniteDuration = TransactionHorizon.Horizon,
            /** When the store was loaded (Frontend.loadedAt). None: now. A test seam */
            loadedAt: Option[Long] = None,
            /** When the store last arrived on this host from another (Frontend.arrivedAt). None: never */
            arrivedAt: Option[Long] = None,
            /** The host's heartbeat period, which calls heartbeat (Frontend.expiryMargin) */
            heartbeatPeriod: FiniteDuration = FiniteDuration(1, SECONDS),
            /** The host's wall clock (Frontend.wallClockMillis). A test seam */
            wallClockMillis: () => Long = () => System.currentTimeMillis(),
            /** Called on the store's executor when the store fail-stops (Frontend.failStop): its host takes it
              * offline and rebuilds it (Host.StoreFailStopped)
              */
            onFailStop: (Store, Throwable) => Unit = (_, _) => (),
            /** Which life of the store's data this is (Frontend.incarnation). None: StoreIncarnation.default(storeId) */
            incarnationOverride: Option[StoreIncarnation] = None,
            /** The IDA of the store's pool (Frontend.poolIDA). None: unchecked, tests only */
            poolIDA: Option[IDA] = None,
            /** The store's CRL admission lane (Frontend.admissionTier) */
            admissionTier: AdmissionTier = AdmissionTier.Data) extends Logging {

  /** How long past the horizon an unresolved Tx starts its status queries: Frontend.expiryMargin */
  val expiryMargin: FiniteDuration = Frontend.expiryMargin(
    txHeartbeatTimeout match
      case d: FiniteDuration => d
      case _ => 5 * heartbeatPeriod,
    heartbeatPeriod)

  /** Every method of a Store runs on its store executor (StoreRouter), so no lock guards its state. The one
   *  exception is this map: `hasTransactions` and `logTransactionStatus` read it from other threads (TestNetwork's
   *  checker thread, through Host.hasTransactions), so it is an immutable Map in a `@volatile` field and those two
   *  read one snapshot of it. TransactionDrivers keep their own locks for their timer threads, and are never handed
   *  the Store, so those threads never reach its state. Declared before the Frontend, whose
   *  onCommitProven may run while it is built. */
  @volatile private var transactionDrivers: Map[TransactionId, TransactionDriver] = Map()

  val storeId: StoreId = backend.storeId
  val frontend = new Frontend(backend.storeId, backend, objectCache, net, crl, txStatusCache, maxSliceSize,
    missingDataWait = missingDataWait, admission = admission, unackedLookup = unackedLookup,
    quarantineVerifier = quarantineVerifier,
    // A reload installs a new Store under the same id: a task of this one's is dropped then
    post = postToStore match
      case None => task => task()
      case Some(postTo) => task => postTo(s => if s eq this then task()),
    acceptorSince = acceptorSince, deferRecovery = deferRecovery, wallClockMillis = wallClockMillis,
    horizon = horizon, loadedAt = loadedAt, arrivedAt = arrivedAt, expiryMargin = expiryMargin,
    onFailStop = err => onFailStop(this, err), incarnationOverride = incarnationOverride, poolIDA = poolIDA,
    admissionTier = admissionTier,
    onCommitProven = txid => transactionDrivers.get(txid).foreach(giveProof(txid, _)))

  /** Stops the store's transaction drivers first (Ruling 31): left running, their timers would keep re-preparing
    * transactions for a store that is gone. Drivers on other stores, and their heartbeat recovery, take over
    */
  def close(): Future[Unit] =
    val drivers = transactionDrivers.values
    transactionDrivers = Map()
    drivers.foreach(_.shutdown())
    frontend.close()

  def estimateSize(): Long = backend.estimateSize()

  /** A fail-stopped store (Frontend.failStop) does nothing more: its drivers and Txs wait for its unload */
  def heartbeat(): Unit = if !frontend.failStopped then {
    transactionDrivers.values.foreach { td =>
      td.heartbeat()
    }
    frontend.transactions.values.foreach(_.checkCollisionWait())
    frontend.retryUnackedLookups()
    frontend.retryQuarantineVerifications()
    frontend.expiryHeartbeat()
    removeExpiredDrivers()
    frontend.transactions.values.foreach { tx =>
      tx.reportIfStuck()
      if (tx.durationSinceLastEvent > txHeartbeatTimeout && !transactionDrivers.contains(tx.transactionId))
        driveTransaction(tx.txd)
    }
  }

  /** Stops and removes each driver of a transaction past the horizon plus the expiry margin that has learned no outcome
    * and whose store holds no Tx for it: its Tx expired (Frontend.expiryHeartbeat) or never existed. Past the horizon
    * no store that forgot the transaction answers it, and a driver holds no durable state, so dropping it is safe. A
    * driver with an outcome is kept: its finalization must complete. Nothing is removed while the store's CRL state
    * loads, whose transactions are not registered yet
    */
  private def removeExpiredDrivers(): Unit =
    if !frontend.recoveryPending then
      transactionDrivers.foreach: (txid, driver) =>
        if !frontend.transactions.contains(txid) && !driver.hasOutcome && frontend.pastExpiry(driver.txd) then
          logger.info(s"Store $storeId stops its driver of transaction $txid: past the transaction horizon with no " +
            s"outcome, and the store holds no Tx for it")
          transactionDrivers -= txid
          driver.shutdown()

  private val prepareResponseCache = Scaffeine().expireAfterWrite(10.seconds)
    .maximumSize(1000)
    .build[TransactionId, List[TxPrepareResponse]]()

  /** Starts a driver for `txd`, unless one runs or the store's CRL state is still being loaded
    * (Frontend.recoveryPending): a recovered Tx may be among it, and the driver must start above what its acceptor
    * promised or accepted (TransactionDriver.Start.highestSeen, paxos F5).
    *
    * A recovered unresolved commit vote is driven at once, before its UnackedCommits lookup answers (Todo 4.4, R-N3;
    * this replaces Ruling 20's wait). The wait guarded against a driver without the entry's proof deciding abort on
    * the fresh votes of stores that had finalized and forgotten the transaction. The forget floor closed that: a store
    * that forgot T refuses its prepares, or answers from its status cache, so Paxos chooses abort only from real
    * acceptor state, and never once commit was chosen (paxos 10.10). The Tx still accepts no abort before a lookup
    * finds no entry. A lookup that finds the entry later hands the proof to the running driver (giveProof)
    *
    * @param initialProposal the client's prepare is creating the transaction at its designated leader: this is the
    *                        leader's first driver for it, which keeps the prepare's initial proposal (1, L). Every
    *                        other driver starts above round 1 and above what this store's acceptor promised or
    *                        accepted (TransactionDriver.Start, paxos F5)
    */
  def driveTransaction(txd: TransactionDescription, initialProposal: Boolean = false): Unit = {
    if (!transactionDrivers.contains(txd.transactionId) && !frontend.recoveryPending) {

      val start = TransactionDriver.Start(
        initialProposal = initialProposal,
        highestSeen = frontend.transactions.get(txd.transactionId).flatMap(_.highestProposal),
        onSplitDecision = txStatusCache.recordSplitDecision,
        pastExpiry = () => frontend.pastExpiry(txd))
      val driver = txDriverFactory.create(executionContext, storeId, net, backgroundTasks, txd, finalizerFactory, start)

      transactionDrivers += txd.transactionId -> driver

      // Given before the cached prepare responses below, which could otherwise complete a tally at the initial
      // proposal first
      giveProof(txd.transactionId, driver)

      prepareResponseCache.getIfPresent(txd.transactionId).foreach { lst =>
        lst.foreach(driver.receiveTxPrepareResponse(_))
        prepareResponseCache.invalidate(txd.transactionId)
      }
    }
  }

  /** A commit vote that found its UnackedCommits entry (of any incarnation) knows the transaction committed: its
    * driver holds that proof, exactly as if a prover had sent it (Tx.foundUnackedEntry), so it never proposes abort
    * and resolves late on a memoryless post-proof quorum. Given when the driver starts, or when the lookup answers
    * while one runs (Frontend.onCommitProven). A second proof, or one reaching a resolved driver, starts no round
    */
  private def giveProof(txid: TransactionId, driver: TransactionDriver): Unit =
    if frontend.transactions.get(txid).exists(_.provesCommitToItsDriver) then
      driver.receiveTxResolved(TxResolved(storeId, storeId, txid, committed = true, late = false, proof = true))

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
    case _ if frontend.failStopped =>
      logger.trace(s"Store $storeId dropped $msg: fail-stopped")
    // Backpressure: while the device's CRL is filling past this store's lane (CrlAdmission), a prepare for a
    // transaction this store has not seen is dropped without a reply. The client retransmits; other stores may commit it without this one, which repair catches up
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
            net.sendTransactionMessage(TxFinalized(msg.from, msg.to, m.txd.transactionId, committed, late = committed, owed = Set()))

            m.txd.originatingClient.foreach { client =>
              net.sendClientResponse(TransactionFinalized(client, msg.to, m.txd.transactionId, committed))
            }

            false
          } else
            true
        }

        // A transaction started too far ahead of this host's clock is muted here and likely at the other stores too
        // (Frontend.MaxStartLead): a driver would only run data-less rounds, and could abort it for want of the
        // client's prepare once the clocks catch up. The client's retransmission starts the driver then. Nor is one
        // started below the store's forget floor for a transaction it does not hold: the Frontend refuses the prepare
        // (or, with an outcome cached, needs no driver to learn it), and a driver could only run rounds with stores
        // that may have forgotten the transaction
        val refused = frontend.belowForgetFloor(m.txd) && !frontend.transactions.contains(m.txd.transactionId)
        if (notFinalized && m.txd.primaryObject.poolId == storeId.poolId && m.txd.designatedLeaderUID == storeId.poolIndex
            && !frontend.startsTooFarAhead(m.txd) && !refused) {
          // Handled before the Frontend sees the prepare, so no Tx yet means this prepare creates it
          driveTransaction(m.txd, initialProposal = m.fromClient && !frontend.transactions.contains(m.txd.transactionId))
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
