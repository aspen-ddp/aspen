package org.aspen_ddp.aspen.server.crl.simple

import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.transaction.TransactionId
import org.aspen_ddp.aspen.server.crl.{AllocationRecoveryState, CrashRecoveryLog, CrashRecoveryLogFactory, TransactionRecoveryState}
import org.apache.logging.log4j.scala.Logging

import java.io.File
import java.nio.file.{Path, Paths}
import java.util.UUID
import java.util.concurrent.LinkedBlockingQueue
import scala.collection.immutable.HashMap
import scala.concurrent.{Future, Promise}

object SimpleCRL:

  sealed abstract class ActionRequest()

  case class SaveTransaction(txid: TransactionId,
                             state: TransactionRecoveryState,
                             completionHandler: () => Unit) extends ActionRequest

  case class SaveAllocation(ars: AllocationRecoveryState, completionHandler: () => Unit) extends ActionRequest

  case class DropTransactionData(storeId: StoreId, transactionId: TransactionId) extends ActionRequest

  case class DeleteTransaction(storeId: StoreId, transactionId: TransactionId) extends ActionRequest

  case class DeleteAllocation(storeId: StoreId, transactionId: TransactionId) extends ActionRequest

  case class WriteComplete(serialNumber: Long) extends ActionRequest

  case class Shutdown(completionHandler: () => Unit) extends ActionRequest

  case class GetRecoveryState(storeId: StoreId,
                              removeStore: Boolean,
                              completion: (List[TransactionRecoveryState], List[AllocationRecoveryState]) => Unit
                             ) extends ActionRequest


  case class InitialCRLState(crl: SimpleCRL,
                             trsList: List[TransactionRecoveryState],
                             arsList: List[AllocationRecoveryState])

  case class Factory(streamsDir: Path,
                     numStreams: Int,
                     maxSizeInBytes: Long) extends CrashRecoveryLogFactory:
    def createCRL(): CrashRecoveryLog =
      SimpleCRL(streamsDir, numStreams, maxSizeInBytes).crl



  def apply(streamsDir: Path, numStreams: Int, maxSizeInBytes: Long): InitialCRLState =
    val files: List[(StreamId, Path)] = (0 until numStreams).map(i =>
      (StreamId(i), streamsDir.resolve(s"$i.crl"))
    ).toList

    val r = Recovery.recover(files)

    InitialCRLState(
      new SimpleCRL(
        maxSizeInBytes,
        files,
        r),
      r.trsList,
      r.arsList)


class SimpleCRL private (val maxSizeInBytes: Long,
                         files: List[(StreamId, Path)],
                         r: Recovery.Result) extends CrashRecoveryLog with Logging:

  import SimpleCRL._
  
  val numStreams: Int = files.size

  private val writer = StreamWriter(maxSizeInBytes, files)
  private var currentStream: Int = r.activeStreamId.number
  private var currentLogEntry: LogEntry = new LogEntry(r.initialPreviousEntryLocation,
    r.initialNextEntrySerialNumber, r.initialOldestEntryNeeded)
  private val streams: Array[Stream] = new Array[Stream](files.length)

  private var transactions: HashMap[TxId, Tx] = new HashMap()
  private var allocations: HashMap[TxId, List[Alloc]] = new HashMap()

  private var queueHead: Option[LogContent] = None
  private var queueTail: Option[LogContent] = None

  private val ioQueue = new LinkedBlockingQueue[ActionRequest]()

  private var initializing = true
  private var writeInProgress = false

  files.foreach: tpl =>
    if tpl._1 == r.activeStreamId then
      streams(tpl._1.number) = new Stream(r.activeStreamId, writer,
        r.currentStreamUUID, r.initialNextEntryOffset)
    else
      streams(tpl._1.number) = new Stream(tpl._1, writer, UUID.randomUUID(), 0)

  r.trsList.foreach(trs => onSaveTransaction(trs.txd.transactionId, trs, () => ()))
  r.arsList.foreach(ars => onSaveAllocation(ars, () => ()))

  // If we have any outstanding transactions or allocations, create a new log entry with
  // their full content. This ensures our oldestSerialNumberNeeded is equal to the current
  // entry serial number. Otherwise we'd have to figure out where each tx/alloc is to
  // accurately track how far back we need to go.
  if !currentLogEntry.isEmpty then
    writeCurrentLogEntry()
    // Only the writer has a reference to this instance during initialization so this call
    // will block until the writer enqueues a WriteComplete message
    val event = ioQueue.take()
    // Put this back on the queue for the I/O thread to handle the WriteComplete event
    ioQueue.put(event)

  initializing = false

  private val ioThread = new Thread {
    override def run(): Unit = crlThread()
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

      val streamId = StreamId(currentStream)
      streams(currentStream).recycleStream()
      transactions.foreach: tx =>
        if tx._2.closeStream(streamId) then
          moveToQueueHead(tx._2)
          currentLogEntry.addTx(tx._2, () => ())
      allocations.foreach: tpl =>
        tpl._2.foreach: a =>
          if a.closeStream(streamId) then
            moveToQueueHead(a)
            currentLogEntry.addAllocation(a, () => ())

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

        case SaveAllocation(ars, completionHandler) => onSaveAllocation(ars, completionHandler)

        case DropTransactionData(storeId, transactionId) => onDropTransactionObjectData(storeId, transactionId)

        case DeleteTransaction(storeId, transactionId) => onDeleteTransaction(storeId, transactionId)

        case DeleteAllocation(storeId, transactionId) => onDeleteAllocation(storeId, transactionId)

        case WriteComplete(serialNumber) =>
          logger.trace(s"CRL write complete for entry serial number: ${serialNumber}")

          writeInProgress = false
          startWrite()

        case GetRecoveryState(storeId, removeStore, completion) =>
          onGetRecoveryState(storeId, removeStore, completion)

        case Shutdown(completionHandler) =>
          writer.shutdown(completionHandler)
          return

  private def moveToQueueHead(lc: LogContent): Unit =
    lc.clearDynamicData()
    removeFromQueue(lc)
    addToQueueHead(lc)

  private def removeFromQueue(lc: LogContent): Unit =
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

  private def addToQueueHead(lc: LogContent): Unit =
    lc.next = None
    lc.prev = queueHead
    lc.entrySerialNumber = currentLogEntry.entrySerialNumber
    queueHead = Some(lc)
    if queueTail.isEmpty then
      queueTail = Some(lc)

  private def onGetRecoveryState(storeId: StoreId,
                                 removeStore: Boolean,
                                 completion: (List[TransactionRecoveryState], List[AllocationRecoveryState]) => Unit
                                ): Unit =

    var trsList: List[TransactionRecoveryState] = Nil
    var arsList: List[AllocationRecoveryState] = Nil

    transactions = transactions.filter: (txid, tx) =>
      if txid.storeId == storeId then
        trsList = tx.state :: trsList
        !removeStore
      else
        true

    allocations = allocations.filter: (txid, allocList) =>
      if txid.storeId == storeId then
        allocList.foreach: alloc =>
          arsList = alloc.state :: arsList
        !removeStore
      else
        true

    completion(trsList, arsList)

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

    tx.state = state

    logger.trace(s"CRL adding transaction ${tx.id} to log entry ${currentLogEntry.entrySerialNumber}")
    currentLogEntry.addTx(tx, completionHandler)
    startWrite()

  private def onSaveAllocation(ars: AllocationRecoveryState,
                               completionHandler: () => Unit): Unit =
    val txid = TxId(ars.storeId, ars.allocationTransactionId)
    val allocs = allocations.get(txid) match
      case Some(lst) => lst
      case None => Nil

    val alloc = new Alloc(None, ars)

    addToQueueHead(alloc)

    val lst: List[Alloc] = alloc :: allocs
    allocations += (txid -> lst)

    logger.trace(s"CRL adding allocation to log entry ${currentLogEntry.entrySerialNumber}")
    currentLogEntry.addAllocation(alloc, completionHandler)
    startWrite()

  private def onDeleteTransaction(storeId: StoreId, transactionid: TransactionId): Unit =
    val txid = TxId(storeId, transactionid)
    currentLogEntry.deleteTx(txid)
    transactions.get(txid).foreach(tx => removeFromQueue(tx))
    transactions -= txid
    startWrite()

  private def onDeleteAllocation(storeId: StoreId, transactionid: TransactionId): Unit =
    val txid = TxId(storeId, transactionid)
    currentLogEntry.deleteAllocation(txid)
    allocations.get(txid).foreach(lst => lst.foreach(removeFromQueue(_)))
    allocations -= txid
    startWrite()

  private def onDropTransactionObjectData(storeId: StoreId, transactionid: TransactionId): Unit =
    // Don't add to current entry. Allow the dropped data to take effect on the next
    // state write or propagation
    transactions.get(TxId(storeId, transactionid)).foreach(_.dropTransactionObjectData())

  def getFullRecoveryState(storeId: StoreId): (List[TransactionRecoveryState], List[AllocationRecoveryState]) =
    var trsList: List[TransactionRecoveryState] = Nil
    var arsList: List[AllocationRecoveryState] = Nil

    val blockingQueue = new LinkedBlockingQueue[String]()

    def completion(tl: List[TransactionRecoveryState], al: List[AllocationRecoveryState]): Unit =
      trsList = tl
      arsList = al
      blockingQueue.put("")

    ioQueue.put(GetRecoveryState(storeId, false, completion))

    blockingQueue.take()

    (trsList, arsList)

  def save(transactionId: TransactionId,
           state: TransactionRecoveryState,
           completionHandler: () => Unit): Unit =
    ioQueue.put(SaveTransaction(transactionId, state, completionHandler))

  def save(ars: AllocationRecoveryState,
           completionHandler: () => Unit): Unit =
    ioQueue.put(SaveAllocation(ars, completionHandler))

  def deleteTransaction(storeId: StoreId, transactionid: TransactionId): Unit =
    ioQueue.put(DeleteTransaction(storeId, transactionid))

  def deleteAllocation(storeId: StoreId, transactionid: TransactionId): Unit =
    ioQueue.put(DeleteAllocation(storeId, transactionid))

  def dropTransactionObjectData(storeId: StoreId, transactionid: TransactionId): Unit =
    ioQueue.put(DropTransactionData(storeId, transactionid))

  def closeStore(storeId: StoreId): Future[(List[TransactionRecoveryState], List[AllocationRecoveryState])] =
    val p = Promise[(List[TransactionRecoveryState], List[AllocationRecoveryState])]()

    def completion(trsList: List[TransactionRecoveryState], arsList: List[AllocationRecoveryState]): Unit =
      p.success((trsList, arsList))

    ioQueue.put(GetRecoveryState(storeId, true, completion))

    p.future

  def loadStoreState(storeId: StoreId, 
                     trsList: List[TransactionRecoveryState], 
                     arsList: List[AllocationRecoveryState]): Future[Unit] =
    
    logger.info(s"Loading transaction and allocation state for store: $storeId")
    
    if trsList.isEmpty && arsList.isEmpty then
      logger.info(s"Completed load of empty transaction and allocation state for store: $storeId")
      return Future.successful(())
    
    val completion = Promise[Unit]()
    var outstandingSaves = 1
    var allStateWritten = false

    def onComplete(): Unit = synchronized {
      outstandingSaves -= 1
      // Use allStateWritten to prevent race condition of backend thread writing so fast that we
      // hit zero before all of the recovery state instances have been written
      if allStateWritten && outstandingSaves == 0 then
        logger.info(s"Completed load of transaction and allocation state for store: $storeId")
        completion.success(())
    }
    
    trsList.foreach: trs =>
      synchronized {
        outstandingSaves += 1
      }
      save(trs.txd.transactionId, trs, onComplete)

    arsList.foreach: ars =>
      synchronized {
        outstandingSaves += 1
      }
      save(ars, onComplete)

    synchronized{
      allStateWritten = true  
    }
    onComplete() // Ensure there's at least one call after allStateWritten has been set
    
    completion.future

  def shutdown(): Unit = 
    val q = new LinkedBlockingQueue[String]()
    ioQueue.put(Shutdown(() => q.put("")))
    q.take()


