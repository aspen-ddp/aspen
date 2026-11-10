package org.aspen_ddp.aspen.client.internal.transaction

import java.nio.{ByteBuffer, ByteOrder}
import java.util.UUID
import org.aspen_ddp.aspen.client.{AspenClient, FinalizationAction, FinalizationActionFactory, RegisteredTypeFactory, Transaction}
import org.aspen_ddp.aspen.client.internal.transaction.MissedUpdateMarker.{MissedUnackedCommit, MissedUpdate}
import org.aspen_ddp.aspen.common.HLCTimestamp
import org.aspen_ddp.aspen.common.objects.{ObjectId, ObjectPointer, Value}
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.transaction.{FinalizationActionId, SerializedFinalizationAction, TransactionDescription, TransactionId}
import org.aspen_ddp.aspen.common.util.BackgroundTaskManager
import org.aspen_ddp.aspen.common.util.BackgroundTaskManager.ScheduledTask
import org.aspen_ddp.aspen.server.store.RequirementsApplyer

import scala.concurrent.duration.{Duration, SECONDS}
import scala.concurrent.{ExecutionContext, Future, Promise}

class MissedUpdateFinalizationAction(val client: AspenClient,
                                     val txd: TransactionDescription) extends FinalizationAction {

  given ExecutionContext = client.clientContext

  private var commitErrors: Map[StoreId, List[ObjectId]] = Map()

  // The commit voters the driver counted, if it counted them. None: unknown, so every silent store may hold one
  private var voters: Option[Set[StoreId]] = None

  override def commitVoters(voters: Option[Set[StoreId]]): Unit = synchronized {
    this.voters = voters
  }

  private var markTask: Option[ScheduledTask] = None

  /** Set by `cancel`: nothing is scheduled or written from then on */
  private var cancelled = false

  /** The transaction was finalized another way (a late resolution, or TxFinalized from another driver), whose
    * finalization already wrote what this would. Stops a scheduled mark and any later execute; a mark already handed
    * to the MissedUpdateMarker is not recalled. `complete` stays pending, as for the other actions
    */
  override def cancel(): Unit = synchronized {
    cancelled = true
    markTask.foreach(_.cancel())
    markTask = None
  }

  /** Set once the UnackedCommits entries are durable; empty until then, and if the marker is cancelled before writing */
  private var owed: Set[StoreId] = Set()

  override def owedStores: Set[StoreId] = synchronized(owed)

  private val completionPromise: Promise[Unit] = Promise()

  // Never marked: a store that misses a lock has nothing to repair, and since the lock doesn't change the object, its
  // timestamp would not reach this transaction's until some later write, so the entry could not be cleared before then.
  // A store whose lock failed because its copy is behind was marked by the transaction that wrote the newer copy.
  private lazy val lockOnly: Set[ObjectId] = txd.lockOnlyObjects

  logger.debug(s"Created MissedUpdateFinalizationAction for Tx ${txd.transactionId}")

  def complete: Future[Unit] = completionPromise.future

  override def updateCommitErrors(commitErrors: Map[StoreId, List[ObjectId]]): Unit = synchronized {
    this.commitErrors = commitErrors

    if txd.allDataStores.size == commitErrors.keysIterator.toSet.size && !commitErrors.valuesIterator.exists(_.exists(!lockOnly.contains(_))) then
      logger.trace(s"All stores responded to commit of tx ${txd.transactionId} without error. No MissedUpdates.")
      markTask.foreach(_.cancel())
      if (!completionPromise.isCompleted)
        MissedUpdateFinalizationAction.report.recordClean()
        completionPromise.success(())
  }

  /** Runs after the transaction's other finalization actions complete (RegisteredTransactionFinalizerFactory.Finalizer).
    * Commit errors and voters given before then are kept. Nothing is scheduled once every store reported clean
    */
  def execute(): Unit = synchronized {
    if !completionPromise.isCompleted && !cancelled then markTask = Some(scheduler.schedule(MissedUpdateFinalizationAction.errorTimeout) {
      markMissedUpdates()
    })
  }

  // Records missed updates with the missed transaction's timestamp and storePointer bytes. Object
  // deletion removes the pool's allocationTree entry, which is the only other place those bytes
  // are persisted, so without this a repair pass cannot locate a deleted object's per-store state.
  //
  // An empty storePointer means "this object has no storePointer". A repair pass treats it as
  // "delete by ObjectId alone" rather than as an error, which RocksDB stores still rely on.
  // See StoreRepairer.repairDeletion in aspen.server.repair.
  def markMissedUpdates(): Unit = synchronized {
    if !cancelled then writeMissedUpdates()
  }

  private def writeMissedUpdates(): Unit = {
    // Build a map from ObjectId to ObjectPointer to recover pointers for ids in commitErrors
    val idToPointer = txd.allReferencedObjectsSet.map(ptr => ptr.id -> ptr).toMap

    val errors = txd.allDataStores.foldLeft(List[MissedUpdate]()) { (l, storeId) =>
      commitErrors.get(storeId) match
        case None =>
          txd.allHostedObjects(storeId).filterNot(ptr => lockOnly.contains(ptr.id))
            .map(ptr => MissedUpdate(storeId, ptr.id, txd.startTimestamp, ptr.storePointer)) ++ l
        case Some(lst) =>
          // Note: this case is not reachable today (ids in commitErrors come from RequirementsChecker,
          // which only reports r.objectPointer.id from this same txd's requirements), but we handle it
          // defensively by looking up pointers from allReferencedObjectsSet, falling back to EmptyArray.
          lst.filterNot(lockOnly.contains).map { objectId =>
            val storePointer = idToPointer.get(objectId).map(_.storePointer).getOrElse(ObjectPointer.EmptyArray)
            MissedUpdate(storeId, objectId, txd.startTimestamp, storePointer)
          } ++ l
    }

    // A counted commit voter that sent no TxCommitted had not acknowledged applying the transaction when it was
    // finalized, so it owes it: it may have crashed holding its saved vote, and if it recovers after the transaction
    // is forgotten this entry is how it learns the transaction committed. The entry is written whether or not the
    // change leaves a trace. A store that never voted holds no vote to recover: the ErrorTree marks above repair it,
    // so it gets no entry
    val changed = RequirementsApplyer.changedObjects(txd.requirements)
    val unacked = txd.allDataStores.toList.flatMap: storeId =>
      if commitErrors.contains(storeId) || voters.exists(!_.contains(storeId)) then
        None
      else
        val objects = txd.allHostedObjects(storeId).filter(ptr => changed.contains(ptr.id))
        Option.when(objects.nonEmpty):
          MissedUnackedCommit(storeId, txd.transactionId, UnackedCommitEntry(txd.startTimestamp, objects))

    reportMarks(errors.map(_.store))

    writeMarks(errors, unacked).foreach { _ =>
      synchronized {
        owed = unacked.map(_.store).toSet
        if (!completionPromise.isCompleted)
          completionPromise.success(())
      }
    }
  }

  /** Where the mark timer and the report are scheduled. A seam for tests */
  protected def scheduler: BackgroundTaskManager = client.backgroundTaskManager

  /** Writes the marks and entries. A seam for tests */
  protected def writeMarks(errors: List[MissedUpdate], unacked: List[MissedUnackedCommit]): Future[Unit] =
    client.missedUpdateMarker.mark(errors, unacked)

  /** Marks are normally trace only, and a burst of them can overwhelm the system, so they're summarized once a second */
  private def reportMarks(stores: List[StoreId]): Unit =
    import MissedUpdateFinalizationAction.report
    if stores.nonEmpty then
      val noReply = stores.count(!commitErrors.contains(_))
      val age = (HLCTimestamp.now - txd.startTimestamp).toMillis
      if report.recordMarks(txd.transactionId, stores, noReply, age) then
        val task = scheduler.schedule(MissedUpdateFinalizationAction.reportPeriod) {
          report.take().foreach(s => logger.warn(s"$s. Entries waiting to be written: ${client.missedUpdateMarker.queued}"))
        }
        if task == BackgroundTaskManager.ShutdownTask then
          report.take()
}

object MissedUpdateFinalizationAction extends RegisteredTypeFactory with FinalizationActionFactory {
  val typeUUID: UUID = UUID.fromString("E1F61997-61A7-4C86-A211-0EBA601DEDA3")

  /** Disables missed-update tracking on `tx`, an ErrorTree write about `entryPool`, only when the
    * write itself touches that pool. Only then could a missed update of the write mark an entry in
    * the same tree. Metadata placement keeps every other pool's ErrorTree out of the pool it describes.
    */
  private[aspen] def limitTracking(entryPool: PoolId, tx: Transaction): Unit =
    if tx.referencedPoolIds.contains(entryPool) then
      tx.disableMissedUpdateTracking()

  /** How long stores have to report a commit before they're marked as having missed it, which schedules them for repair.
    * Ordinary load delays a commit by far more than a disk or network hiccup would. The finalizer holds up TxFinalized
    * meanwhile, so this must stay well below `Host.txHeartbeatTimeout`, when stores start recovering a lost TxFinalized.
    */
  var errorTimeout: Duration = Duration(2, SECONDS)

  private val reportPeriod = Duration(1, SECONDS)

  private val report = new MissedUpdateReport

  /** Totals of the missed updates marked since the last report, for a periodic log line */
  private[aspen] class MissedUpdateReport:
    private var clean = 0L
    private var executions = 0L
    private var transactions: Set[TransactionId] = Set()
    private var marks = 0L
    private var noReply = 0L
    private var perStore: Map[StoreId, Long] = Map()
    private var totalAge = 0L
    private var maxAge = 0L

    /** A transaction whose stores all replied in time, so it marked nothing */
    def recordClean(): Unit = synchronized:
      clean += 1

    /** Records one execution's marks, one store per mark.
      *
      * @param noReplyMarks how many of them are for stores that never replied. The others reported a commit error
      * @param ageMillis the time since the transaction started
      * @return true if a report is now due and nothing has arranged for it yet
      */
    def recordMarks(txid: TransactionId, stores: List[StoreId], noReplyMarks: Int, ageMillis: Long): Boolean = synchronized:
      val firstOfReport = executions == 0
      executions += 1
      transactions += txid
      marks += stores.size
      noReply += noReplyMarks
      stores.foreach(s => perStore = perStore.updated(s, perStore.getOrElse(s, 0L) + 1))
      totalAge += ageMillis
      maxAge = math.max(maxAge, ageMillis)
      firstOfReport

    /** The report if there's anything to report, and starts the next one */
    def take(): Option[String] = synchronized:
      if executions == 0 then
        None
      else
        val stores = perStore.toList.sortBy((store, count) => (-count, store.toString)).map((store, count) => s"$store=$count")
        val text = s"Missed updates since last report: $marks marks ($noReply with no reply from the store, " +
          s"${marks - noReply} with a commit error) from ${transactions.size} transactions in $executions executions. " +
          s"Transactions with no marks: $clean. Time since transaction start: average ${totalAge / executions}ms, " +
          s"max ${maxAge}ms. Marks per store: ${stores.mkString(" ")}"
        clean = 0
        executions = 0
        transactions = Set()
        marks = 0
        noReply = 0
        perStore = Map()
        totalAge = 0
        maxAge = 0
        Some(text)

  /** An errorTree entry's value: the missed transaction's start timestamp, big-endian, then the
    * object's storePointer bytes (possibly empty).
    *
    * The timestamp is the one a store gives the object when it applies that transaction, so a
    * repair that installs a state at least that new has caught the store up past the miss. The
    * entry's own key timestamp cannot serve: it belongs to the later transaction that wrote it.
    */
  def encodeErrorEntry(missedTimestamp: HLCTimestamp, storePointer: Array[Byte]): Value =
    val bb = ByteBuffer.allocate(HLCTimestamp.EncodedSize + storePointer.length)
    bb.order(ByteOrder.BIG_ENDIAN)
    bb.putLong(missedTimestamp.asLong)
    bb.put(storePointer)
    Value(bb.array())

  /** Inverse of `encodeErrorEntry`: the missed transaction's timestamp and the storePointer */
  def decodeErrorEntry(value: Value): (HLCTimestamp, Array[Byte]) =
    val bb = ByteBuffer.wrap(value.bytes)
    bb.order(ByteOrder.BIG_ENDIAN)
    val ts = HLCTimestamp(bb.getLong())
    val storePointer = new Array[Byte](bb.remaining())
    bb.get(storePointer)
    (ts, storePointer)

  def createFinalizationAction(client: AspenClient,
                               txd: TransactionDescription,
                               data: Array[Byte]): FinalizationAction = {

    new MissedUpdateFinalizationAction(client, txd)
  }

  def createSerializedFA(missedCommitDelayInMs: Int): SerializedFinalizationAction = {
    SerializedFinalizationAction(FinalizationActionId(typeUUID), Array[Byte]())
  }

}