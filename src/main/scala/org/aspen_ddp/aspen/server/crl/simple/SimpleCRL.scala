package org.aspen_ddp.aspen.server.crl.simple

import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.transaction.TransactionId
import org.aspen_ddp.aspen.server.StorageDeviceConfig
import org.aspen_ddp.aspen.server.crl.{CrashRecoveryLog, CrashRecoveryLogFactory, StoreRecoveryState,
  TransactionRecoveryState}
import scribe.Logging

import java.util.UUID
import java.util.concurrent.{CountDownLatch, LinkedBlockingQueue}
import java.util.concurrent.atomic.AtomicBoolean
import scala.collection.immutable.HashMap
import scala.concurrent.{Future, Promise}

object SimpleCRL:

  sealed abstract class ActionRequest()

  case class SaveTransaction(txid: TransactionId,
                             state: TransactionRecoveryState,
                             completionHandler: () => Unit) extends ActionRequest

  case class DropTransactionData(storeId: StoreId, transactionId: TransactionId) extends ActionRequest

  case class DeleteTransaction(storeId: StoreId, transactionId: TransactionId) extends ActionRequest

  case class DeleteTransactions(storeId: StoreId, transactionIds: List[TransactionId],
                                completionHandler: () => Unit) extends ActionRequest

  case class SaveAppliedStub(storeId: StoreId, transactionId: TransactionId,
                             completionHandler: () => Unit) extends ActionRequest

  case class DeleteAppliedStub(storeId: StoreId, transactionId: TransactionId) extends ActionRequest

  case class WriteComplete(serialNumber: Long) extends ActionRequest

  /** Queued when the writer thread dies, so the open-time re-save does not wait forever for its write */
  case class WriterDied(cause: Throwable) extends ActionRequest

  case class Shutdown(completionHandler: () => Unit) extends ActionRequest

  case class GetRecoveryState(storeId: StoreId,
                              completion: StoreRecoveryState => Unit
                             ) extends ActionRequest

  case class GetStoresWithEntries(completion: Set[StoreId] => Unit) extends ActionRequest


  case class InitialCRLState(crl: SimpleCRL,
                             trsList: List[TransactionRecoveryState],
                             appliedStubs: Set[TxId])

  /** Opens each device's SimpleCRL in `CrashRecoveryLog.deviceCrlDir`, creating the directory if absent, sized by
   *  the device's config. `onThreadDeath` is called if a CRL thread dies: that device's transactions stop. */
  case class DeviceFactory(onThreadDeath: Throwable => Unit = _ => ()) extends CrashRecoveryLogFactory:
    def openCRL(devicePath: os.Path, config: StorageDeviceConfig): CrashRecoveryLog =
      val dir = CrashRecoveryLog.deviceCrlDir(devicePath)
      os.makeDir.all(dir)
      SimpleCRL(dir, config.crlNumStreams, config.crlFileSizeMb * 1024L * 1024L, onThreadDeath).crl

  /** Thrown by apply when the stream files on disk were created with another count or size than it is given */
  class SizingMismatch(streamsDir: os.Path, reason: String)
    extends Exception(s"CRL stream files in $streamsDir do not match the configured sizing: $reason")

  /** @param onThreadDeath called if a CRL thread dies. No transaction of the CRL's stores can make progress after
    *                      that: stores vote only once a save completes.
    * @throws SizingMismatch if `streamsDir` holds stream files other than `numStreams` of `maxSizeInBytes` each.
    *                        The sizing is fixed when the CRL is created: recovering with another would silently
    *                        ignore files holding live entries, so such a CRL is treated as unrecoverable.
    */
  def apply(streamsDir: os.Path,
            numStreams: Int,
            maxSizeInBytes: Long,
            onThreadDeath: Throwable => Unit = _ => ()): InitialCRLState =
    // A rotation copies the next stream's live data into the one it recycles, which needs a second stream
    require(numStreams >= 2, s"A CRL needs at least 2 streams, not $numStreams")
    val files: List[(StreamId, os.Path)] = (0 until numStreams).map(i =>
      (StreamId(i), streamsDir / s"$i.crl")
    ).toList

    checkSizing(streamsDir, files.map(_._2).toSet, maxSizeInBytes)

    val r = Recovery.recover(files)

    InitialCRLState(
      new SimpleCRL(
        maxSizeInBytes,
        files,
        r,
        onThreadDeath),
      r.trsList,
      r.appliedStubs)

  /** A directory with no stream files is a new CRL. Otherwise its files must be exactly `expected`, each
    * `maxSizeInBytes` long (StreamWriter creates them at that size) */
  private def checkSizing(streamsDir: os.Path, expected: Set[os.Path], maxSizeInBytes: Long): Unit =
    val present =
      if os.isDir(streamsDir) then os.list(streamsDir).filter(_.ext == "crl").toSet else Set[os.Path]()
    if present.nonEmpty then
      if present != expected then
        throw SizingMismatch(streamsDir, s"found ${present.map(_.last).toList.sorted.mkString(", ")}, expected " +
          s"${expected.size} streams")
      present.foreach: f =>
        if os.size(f) != maxSizeInBytes then
          throw SizingMismatch(streamsDir, s"${f.last} is ${os.size(f)} bytes, expected $maxSizeInBytes")

class SimpleCRL private (val maxSizeInBytes: Long,
                         files: List[(StreamId, os.Path)],
                         r: Recovery.Result,
                         onThreadDeath: Throwable => Unit) extends CrashRecoveryLog with Logging:

  import SimpleCRL._

  val numStreams: Int = files.size

  /** Half a stream: when the ring recycles a stream, everything live is at most half a stream, so the copy-forward of
    * the next stream's live data always fits. Stream.writeEntry's require stays as the backstop
    */
  override def admissionLimitBytes: Long = maxSizeInBytes / 2

  private val ioQueue = new LinkedBlockingQueue[ActionRequest]()

  private val writer = StreamWriter(maxSizeInBytes, files, t =>
    ioQueue.put(WriterDied(t))
    onThreadDeath(t))
  private var currentStream: Int = r.activeStreamId.number
  private var currentLogEntry: LogEntry = new LogEntry(r.initialPreviousEntryLocation,
    r.initialNextEntrySerialNumber, r.initialOldestEntryNeeded)
  private val streams: Array[Stream] = new Array[Stream](files.length)

  private var transactions: HashMap[TxId, Tx] = new HashMap()

  /** Applied stubs: entries apart from transactions, under the same ids (StoreRecoveryState) */
  private var stubs: HashMap[TxId, Stub] = new HashMap()

  private var queueHead: Option[Record] = None
  private var queueTail: Option[Record] = None

  private var initializing = true
  private var writeInProgress = false

  files.foreach: tpl =>
    if tpl._1 == r.activeStreamId then
      streams(tpl._1.number) = new Stream(r.activeStreamId, writer,
        r.currentStreamUUID, r.initialNextEntryOffset)
    else
      streams(tpl._1.number) = new Stream(tpl._1, writer, UUID.randomUUID(), 0)

  // A failed open shuts its writer down: the caller may open the same files again
  try
    // Stubs first: a stub's save replaces a transaction's state of the same id, and both may be live
    r.appliedStubs.foreach(id => onSaveAppliedStub(id.storeId, id.transactionId, () => ()))
    r.trsList.foreach(trs => onSaveTransaction(trs.txd.transactionId, trs, () => ()))

    if !currentLogEntry.isEmpty then
      writeCurrentLogEntry()
      ioQueue.take() match
        case WriterDied(t) => throw t
        case event => ioQueue.put(event) // The WriteComplete, for the CRL thread
  catch case t: Throwable =>
    val closed = new CountDownLatch(1)
    writer.shutdown(() => closed.countDown())
    closed.await()
    throw t

  initializing = false

  // Before the thread starts: its catch counts shutdownComplete down
  private val shutdownRequested = new AtomicBoolean(false)
  private val shutdownComplete = new CountDownLatch(1)

  private val ioThread = new Thread {
    override def run(): Unit =
      try crlThread()
      catch case t: Throwable =>
        logger.error("CRL thread died", t)
        // Nothing takes the Shutdown that shutdown() queues now, so the shutdown is completed here: the files are
        // closed and the latch counted down once the writer finishes what it holds. Without it the device could not
        // be loaded again (its path stays in closingCrls) and the host's stop would wait forever.
        writer.shutdown(() => shutdownComplete.countDown())
        onThreadDeath(t)
  }
  ioThread.start()

  def currentEntrySerialNumber: Long = currentLogEntry.entrySerialNumber

  def currentStreamNumber: Int = currentStream

  def oldestEntryNeeded: Long = queueTail match
    case None => currentLogEntry.entrySerialNumber
    case Some(lc) => lc.entrySerialNumber

  private def startWrite(): Unit =
    if !initializing && !writeInProgress && !currentLogEntry.isEmpty then
      writeCurrentLogEntry()

  private def writeCurrentLogEntry(): Unit =
    if !streams(currentStream).canWriteEntry(currentLogEntry) then
      currentStream += 1
      if currentStream == streams.length then
        currentStream = 0

      // The stream recycled now holds nothing needed: the previous rotation copied its live data forward. This entry
      // copies forward the live data of the next stream to be recycled, so a torn write here, or there at the next
      // rotation, overwrites only entries no recovery needs. The last durable entry's oldestEntryNeeded never points
      // into the stream after the active one
      val recycled = StreamId(currentStream)
      val nextToRecycle = StreamId((currentStream + 1) % streams.length)
      streams(currentStream).recycleStream()
      def copyForward(record: Record)(add: => Unit): Unit =
        val inRecycled = record.closeStream(recycled)
        val inNext = record.closeStream(nextToRecycle)
        if inRecycled then
          logger.error(s"CRL entry ${record.id} still had data in recycled stream $recycled. Copying it forward")
        if inRecycled || inNext then
          moveToQueueHead(record)
          add
      transactions.valuesIterator.foreach(tx => copyForward(tx)(currentLogEntry.addTx(tx, () => ())))
      stubs.valuesIterator.foreach(stub => copyForward(stub)(currentLogEntry.addStub(stub, () => ())))

      currentLogEntry.oldestEntryNeeded = oldestEntryNeeded

    writeInProgress = true
    logger.trace(s"CRL beginning write for entry ${currentLogEntry.entrySerialNumber}")
    val location = streams(currentStream).writeEntry(currentLogEntry, () => {
      ioQueue.put(WriteComplete(currentLogEntry.entrySerialNumber))
    })
    val nextEntrySerialNumber = currentLogEntry.entrySerialNumber+1
    currentLogEntry = new LogEntry(location, nextEntrySerialNumber, oldestEntryNeeded)

  private def crlThread(): Unit =
    while true do
      ioQueue.take() match
        case SaveTransaction(transactionId, state, completionHandler) => onSaveTransaction(transactionId, state, completionHandler)

        case DropTransactionData(storeId, transactionId) => onDropTransactionObjectData(storeId, transactionId)

        case DeleteTransaction(storeId, transactionId) => onDeleteTransaction(storeId, transactionId)

        case DeleteTransactions(storeId, transactionIds, completionHandler) =>
          if transactionIds.isEmpty then
            completionHandler()
          else
            // All in the current entry, with the handler, before its write starts: the handler then runs once
            // every deletion is durable
            transactionIds.foreach: transactionId =>
              val txid = TxId(storeId, transactionId)
              currentLogEntry.deleteTx(txid)
              transactions.get(txid).foreach(tx => removeFromQueue(tx))
              transactions -= txid
              currentLogEntry.deleteStub(txid)
              stubs.get(txid).foreach(stub => removeFromQueue(stub))
              stubs -= txid
            currentLogEntry.addCompletionHandler(completionHandler)
            startWrite()

        case SaveAppliedStub(storeId, transactionId, completionHandler) =>
          onSaveAppliedStub(storeId, transactionId, completionHandler)

        case DeleteAppliedStub(storeId, transactionId) => onDeleteAppliedStub(storeId, transactionId)

        case WriterDied(_) => () // Reported through onThreadDeath

        case WriteComplete(serialNumber) =>
          logger.trace(s"CRL write complete for entry serial number: ${serialNumber}")

          writeInProgress = false
          startWrite()

        case GetRecoveryState(storeId, completion) =>
          onGetRecoveryState(storeId, completion)

        case GetStoresWithEntries(completion) =>
          completion((transactions.keysIterator ++ stubs.keysIterator).map(_.storeId).toSet)

        case Shutdown(completionHandler) =>
          writer.shutdown(completionHandler)
          return

  private def moveToQueueHead(lc: Record): Unit =
    lc.clearDynamicData()
    removeFromQueue(lc)
    addToQueueHead(lc)

  private def removeFromQueue(lc: Record): Unit =
    queueHead.foreach: h =>
      if h eq lc then
        queueHead = lc.prev

    queueTail.foreach: t =>
      if t eq lc then
        queueTail = t.next

    lc.prev.foreach(p => p.next = lc.next)
    lc.next.foreach(n => n.prev = lc.prev)

    lc.prev = None
    lc.next = None

  private def addToQueueHead(lc: Record): Unit =
    lc.next = None
    lc.prev = queueHead
    queueHead.foreach(_.next = Some(lc))
    lc.entrySerialNumber = currentLogEntry.entrySerialNumber
    queueHead = Some(lc)
    if queueTail.isEmpty then
      queueTail = Some(lc)

  /** Leaves the transactions live: only a deletion, which is written, takes them out of the log */
  private def onGetRecoveryState(storeId: StoreId,
                                 completion: StoreRecoveryState => Unit
                                ): Unit =
    completion(StoreRecoveryState(
      transactions.valuesIterator.filter(_.id.storeId == storeId).map(_.state).toList,
      stubs.keysIterator.filter(_.storeId == storeId).map(_.transactionId).toSet))

  /** Replaces the transaction state held for the id, if any, in the same entry: its deletion and the stub are durable
    * together */
  private def onSaveAppliedStub(storeId: StoreId, transactionId: TransactionId, completionHandler: () => Unit): Unit =
    val txid = TxId(storeId, transactionId)
    transactions.get(txid).foreach: tx =>
      removeFromQueue(tx)
      transactions -= txid
      currentLogEntry.deleteTx(txid)
    val stub = stubs.get(txid) match
      case Some(stub) => stub
      case None =>
        val stub = new Stub(txid)
        addToQueueHead(stub)
        stubs += (txid -> stub)
        stub
    currentLogEntry.addStub(stub, completionHandler)
    startWrite()

  private def onDeleteAppliedStub(storeId: StoreId, transactionId: TransactionId): Unit =
    val txid = TxId(storeId, transactionId)
    stubs.get(txid).foreach: stub =>
      removeFromQueue(stub)
      stubs -= txid
      currentLogEntry.deleteStub(txid)
      startWrite()

  private def onSaveTransaction(transactionId: TransactionId,
                      state: TransactionRecoveryState,
                      completionHandler: () => Unit): Unit =
    val txid = TxId(state.storeId, transactionId)
    val tx = transactions.get(txid) match
      case Some(tx) => tx
      case None =>
        val tx = new Tx(txid, state, None, None)

        addToQueueHead(tx)

        transactions += (tx.id -> tx)
        tx

    // A Tx started by a driver's data-less prepare saves again once a client's prepare supplies the data
    if state.objectUpdates.size != tx.state.objectUpdates.size then
      tx.objectUpdateLocations = None

    tx.state = state

    logger.trace(s"CRL adding transaction ${tx.id} to log entry ${currentLogEntry.entrySerialNumber}")
    currentLogEntry.addTx(tx, completionHandler)
    startWrite()

  private def onDeleteTransaction(storeId: StoreId, transactionid: TransactionId): Unit =
    val txid = TxId(storeId, transactionid)
    currentLogEntry.deleteTx(txid)
    transactions.get(txid).foreach(tx => removeFromQueue(tx))
    transactions -= txid
    startWrite()

  private def onDropTransactionObjectData(storeId: StoreId, transactionid: TransactionId): Unit =
    transactions.get(TxId(storeId, transactionid)).foreach(_.dropTransactionObjectData())

  def getFullRecoveryState(storeId: StoreId): List[TransactionRecoveryState] = blockingRead(storeId).transactions

  def getAppliedStubs(storeId: StoreId): Set[TransactionId] = blockingRead(storeId).appliedStubs

  private def blockingRead(storeId: StoreId): StoreRecoveryState =
    val blockingQueue = new LinkedBlockingQueue[StoreRecoveryState]()
    ioQueue.put(GetRecoveryState(storeId, state => blockingQueue.put(state)))
    blockingQueue.take()

  def save(transactionId: TransactionId,
           state: TransactionRecoveryState,
           completionHandler: () => Unit): Unit =
    ioQueue.put(SaveTransaction(transactionId, state, completionHandler))

  def deleteTransaction(storeId: StoreId, transactionid: TransactionId): Unit =
    ioQueue.put(DeleteTransaction(storeId, transactionid))

  def dropTransactionObjectData(storeId: StoreId, transactionid: TransactionId): Unit =
    ioQueue.put(DropTransactionData(storeId, transactionid))

  def saveAppliedStub(storeId: StoreId, txid: TransactionId, completionHandler: () => Unit): Unit =
    ioQueue.put(SaveAppliedStub(storeId, txid, completionHandler))

  def deleteAppliedStub(storeId: StoreId, txid: TransactionId): Unit =
    ioQueue.put(DeleteAppliedStub(storeId, txid))

  override def deleteTransactions(storeId: StoreId, txids: List[TransactionId]): Future[Unit] =
    val p = Promise[Unit]()
    ioQueue.put(DeleteTransactions(storeId, txids, () => p.trySuccess(())))
    p.future

  override def readStoreState(storeId: StoreId): Future[StoreRecoveryState] =
    val p = Promise[StoreRecoveryState]()
    ioQueue.put(GetRecoveryState(storeId, state => p.success(state)))
    p.future

  override def storesWithEntries(): Future[Set[StoreId]] =
    val p = Promise[Set[StoreId]]()
    ioQueue.put(GetStoresWithEntries(stores => p.success(stores)))
    p.future

  /** Idempotent: a later call waits for the first one's shutdown, as a second Shutdown would find no thread */
  override def shutdown(): Unit =
    if shutdownRequested.compareAndSet(false, true) then
      ioQueue.put(Shutdown(() => shutdownComplete.countDown()))
    shutdownComplete.await()
