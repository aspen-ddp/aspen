package org.aspen_ddp.aspen.server.crl.simple

import org.aspen_ddp.aspen.common.HLCTimestamp
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.transaction.TransactionId
import org.aspen_ddp.aspen.server.{DurableFiles, FilePreallocation, StorageDeviceConfig}
import org.aspen_ddp.aspen.server.crl.{CrashRecoveryLog, CrashRecoveryLogFactory, CrlAdmission, CrlFailed, CrlLocked,
  CrlMissing, CrlSizingDefaults, CrlSizingMismatch, StoreRecoveryState, TransactionRecoveryState}
import scribe.Logging

import java.util.UUID
import java.util.concurrent.{CountDownLatch, LinkedBlockingQueue}
import java.util.concurrent.atomic.AtomicBoolean
import scala.collection.immutable.HashMap
import scala.concurrent.{Future, Promise}

object SimpleCRL extends Logging:

  /** What maxTransactionBytes keeps back from half a stream for the entry's own headers and record framing */
  val EntryOverheadReserve: Long = 8192

  sealed abstract class ActionRequest()

  case class SaveTransaction(txid: TransactionId,
                             state: TransactionRecoveryState,
                             completionHandler: () => Unit) extends ActionRequest

  case class DropTransactionData(storeId: StoreId, transactionId: TransactionId) extends ActionRequest

  case class DeleteTransaction(storeId: StoreId, transactionId: TransactionId) extends ActionRequest

  case class DeleteTransactions(storeId: StoreId, transactionIds: List[TransactionId],
                                completionHandler: () => Unit) extends ActionRequest

  case class SaveAppliedStub(storeId: StoreId, transactionId: TransactionId, start: HLCTimestamp,
                             completionHandler: () => Unit) extends ActionRequest

  case class DeleteAppliedStub(storeId: StoreId, transactionId: TransactionId) extends ActionRequest

  /** Drops every entry of the store held when it is processed (CrashRecoveryLog.dropStore) */
  case class DropStore(storeId: StoreId, completionHandler: () => Unit) extends ActionRequest

  case class WriteComplete(serialNumber: Long) extends ActionRequest

  /** Queued when the writer thread dies: the CRL thread dies of it */
  case class WriterDied(cause: Throwable) extends ActionRequest

  case class Shutdown(completionHandler: () => Unit) extends ActionRequest

  case class GetRecoveryState(storeId: StoreId,
                              completion: StoreRecoveryState => Unit
                             ) extends ActionRequest

  case class GetStoresWithEntries(completion: Set[StoreId] => Unit) extends ActionRequest

  /** Runs `f` on the CRL thread, in order with the other requests: for tests that hold the thread */
  case class RunOnCrlThread(f: () => Unit) extends ActionRequest

  /** Runs the handler once every request queued before it is durable (CrashRecoveryLog.whenDurable) */
  case class Barrier(handler: () => Unit) extends ActionRequest


  case class InitialCRLState(crl: SimpleCRL,
                             trsList: List[TransactionRecoveryState],
                             appliedStubs: Map[TxId, HLCTimestamp])

  /** Opens each device's SimpleCRL in `CrashRecoveryLog.deviceCrlDir`, creating the directory if absent. A new CRL is
   *  sized by the device's config; an existing one keeps the sizing it recorded (SizingFile). `onThreadDeath` is
   *  called if a CRL thread dies: that device's transactions stop. */
  case class DeviceFactory(onThreadDeath: Throwable => Unit = _ => ()) extends CrashRecoveryLogFactory:
    def openCRL(devicePath: os.Path, config: StorageDeviceConfig, holdsStores: Boolean): CrashRecoveryLog =
      val dir = CrashRecoveryLog.deviceCrlDir(devicePath)
      os.makeDir.all(dir)
      val sizing = CrlSizingDefaults.resolve(config, devicePath.toIO.getTotalSpace)
      SimpleCRL(dir, sizing.numStreams, sizing.streamBytes, onThreadDeath,
        mustExist = holdsStores).crl

  /** The CRL's own record of its sizing and id, written durably when it is created, before any stream file. The
   *  sizing is fixed then: an existing CRL is always opened with the sizing it recorded, whatever it is asked for, so
   *  a change of the device config or of the code defaults cannot make it unopenable */
  val SizingFile = "sizing"

  /** The sizing a CRL was created with, and its id: a random UUID chosen then, which every entry header bears
   *  (LogEntry). Recovery takes only headers bearing it for entries, so object data in the streams, which nobody
   *  outside the device can make bear an id it never sees, cannot pass for one */
  case class Sizing(numStreams: Int, maxSizeInBytes: Long, crlId: UUID):
    def encode: String = s"num-streams: $numStreams\nmax-size-in-bytes: $maxSizeInBytes\ncrl-id: $crlId\n"

  object Sizing:
    /** None if `text` is not a sizing file's content */
    def decode(text: String): Option[Sizing] =
      val fields = text.linesIterator.map(_.split(":", 2)).collect:
        case Array(k, v) => k.trim -> v.trim
      .toMap
      for
        n <- fields.get("num-streams").flatMap(_.toIntOption)
        m <- fields.get("max-size-in-bytes").flatMap(_.toLongOption)
        id <- fields.get("crl-id").flatMap(v => scala.util.Try(UUID.fromString(v)).toOption)
      yield Sizing(n, m, id)

  /** @param numStreams     the stream count of a CRL created by this call. An existing CRL keeps its recorded count
    * @param maxSizeInBytes the stream size of a CRL created by this call. An existing CRL keeps its recorded size
    * @param onThreadDeath called if a CRL thread dies. No transaction of the CRL's stores can make progress after
    *                      that: stores vote only once a save completes.
    * @throws CrlLocked if the CRL is open elsewhere (CrlLock). Nothing in `streamsDir` is read or written then.
    * @throws CrlSizingMismatch if `streamsDir` holds stream files but no readable SizingFile, or stream files other
    *                           than the recorded count of the recorded size each. Recovering with another sizing would
    *                           silently ignore files holding live entries, so the CRL is not opened. Nothing in
    *                           `streamsDir` is written then.
    * @param mustExist the CRL must already exist (its device holds stores). With no stream files in `streamsDir`
    *                  (only a SizingFile or unfinished `.crl.tmp` files at most), CrlMissing is thrown instead of a
    *                  new CRL being created, and nothing in `streamsDir` is written.
    */
  def apply(streamsDir: os.Path,
            numStreams: Int,
            maxSizeInBytes: Long,
            onThreadDeath: Throwable => Unit = _ => (),
            mustExist: Boolean = false): InitialCRLState =
    // Held from before the files are read until the writer has closed them. A failed open releases it
    val lock = CrlLock.acquire(streamsDir)
    try
      if mustExist && streamFiles(streamsDir).isEmpty then
        throw CrlMissing(streamsDir)
      val sizing = recordedSizing(streamsDir, numStreams, maxSizeInBytes)
      val files: List[(StreamId, os.Path)] = (0 until sizing.numStreams).map(i =>
        (StreamId(i), streamsDir / s"$i.crl")
      ).toList

      createStreamsIfNew(streamsDir, files.map(_._2), sizing.maxSizeInBytes)
      checkSizing(streamsDir, files.map(_._2).toSet, sizing.maxSizeInBytes)

      val r = Recovery.recover(files, sizing.crlId)

      InitialCRLState(
        new SimpleCRL(
          sizing.maxSizeInBytes,
          sizing.crlId,
          files,
          r,
          onThreadDeath,
          lock),
        r.trsList,
        r.appliedStubs)
    catch case t: Throwable =>
      lock.release()
      throw t

  private def streamFiles(streamsDir: os.Path): Set[os.Path] =
    if os.isDir(streamsDir) then os.list(streamsDir).filter(_.ext == "crl").toSet else Set[os.Path]()

  /** The sizing `streamsDir`'s CRL recorded. A directory with neither a SizingFile nor stream files is a new CRL: it
    * records the requested sizing and a new random id first. A SizingFile with no stream files is a creation that
    * crashed before them: its sizing and id stand. Stream files without a readable SizingFile throw CrlSizingMismatch */
  private def recordedSizing(streamsDir: os.Path, numStreams: Int, maxSizeInBytes: Long): Sizing =
    val file = streamsDir / SizingFile
    if os.exists(file) then
      val recorded = Sizing.decode(os.read(file)).filter(_.numStreams >= 2).getOrElse:
        throw CrlSizingMismatch(streamsDir, s"its $SizingFile file is unreadable")
      if recorded.numStreams != numStreams || recorded.maxSizeInBytes != maxSizeInBytes then
        logger.warn(s"The CRL in $streamsDir was created with ${recorded.numStreams} streams of " +
                    s"${recorded.maxSizeInBytes} bytes, not the $numStreams of $maxSizeInBytes configured. " +
                    s"Opening it with the sizing it was created with")
      recorded
    else if streamFiles(streamsDir).nonEmpty then
      throw CrlSizingMismatch(streamsDir, s"it holds stream files but no $SizingFile file")
    else
      // A rotation copies the next stream's live data into the one it recycles, which needs a second stream
      require(numStreams >= 2, s"A CRL needs at least 2 streams, not $numStreams")
      val created = Sizing(numStreams, maxSizeInBytes, UUID.randomUUID())
      DurableFiles.write(file, created.encode)
      created

  private val warnedCopyOnWrite = new AtomicBoolean(false)

  /** Creates the stream files of a new CRL, preallocated at their full size (FilePreallocation), so no CRL write ever
    * allocates a block and a full device cannot fail one with ENOSPC (crash-recovery-log F7). Each is written as
    * `<name>.tmp` and renamed into place, and the directory is forced once all are: only then does the writer start.
    * So a `.tmp` file left at open means a creation that never finished and wrote no entry, and it starts over.
    * A copy-on-write file system allocates on every overwrite, which no preallocation prevents: warned once. */
  private def createStreamsIfNew(streamsDir: os.Path, streams: List[os.Path], maxSizeInBytes: Long): Unit =
    def tmpOf(f: os.Path): os.Path = f / os.up / s"${f.last}.tmp"
    val leftoverTmps = os.list(streamsDir).filter(_.last.endsWith(".crl.tmp"))
    if leftoverTmps.nonEmpty then
      logger.warn(s"The creation of the CRL in $streamsDir did not finish. Creating its stream files again")
      (leftoverTmps ++ streamFiles(streamsDir)).foreach(os.remove(_))
    if streamFiles(streamsDir).isEmpty then
      if FilePreallocation.isCopyOnWrite(streamsDir) && warnedCopyOnWrite.compareAndSet(false, true) then
        logger.warn(s"The CRL in $streamsDir is on a copy-on-write file system: its writes allocate, so a full " +
                    s"device can stop it. Use a file system that overwrites in place, or disable copy-on-write " +
                    s"for the directory (chattr +C)")
      streams.foreach(f => FilePreallocation.preallocate(tmpOf(f), maxSizeInBytes))
      streams.foreach(f => os.move(tmpOf(f), f, atomicMove = true))
      DurableFiles.syncDirectory(streamsDir)

  /** A directory with no stream files is a new CRL. Otherwise its files must be exactly `expected`, each
    * `maxSizeInBytes` long (createStreamsIfNew creates them at that size) */
  private def checkSizing(streamsDir: os.Path, expected: Set[os.Path], maxSizeInBytes: Long): Unit =
    val present = streamFiles(streamsDir)
    if present.nonEmpty then
      if present != expected then
        throw CrlSizingMismatch(streamsDir, s"found ${present.map(_.last).toList.sorted.mkString(", ")}, expected " +
          s"${expected.size} streams")
      present.foreach: f =>
        if os.size(f) != maxSizeInBytes then
          throw CrlSizingMismatch(streamsDir, s"${f.last} is ${os.size(f)} bytes, expected $maxSizeInBytes")

class SimpleCRL private (val maxSizeInBytes: Long,
                         crlId: UUID,
                         files: List[(StreamId, os.Path)],
                         r: Recovery.Result,
                         onThreadDeath: Throwable => Unit,
                         lock: CrlLock) extends CrashRecoveryLog with Logging:

  import SimpleCRL._

  val numStreams: Int = files.size

  /** Half a stream: when the ring recycles a stream, everything live is at most half a stream, so the copy-forward of
    * the next stream's live data always fits. Stream.writeEntry's require stays as the backstop
    */
  override def admissionLimitBytes: Long = maxSizeInBytes / 2

  /** Half a stream, less EntryOverheadReserve for the headers of the entry that holds it, so one transaction's entry
    * always fits in a stream. A larger one would kill the CRL at Stream.writeEntry's require (CRL-W2)
    */
  override def maxTransactionBytes: Long = maxSizeInBytes / 2 - EntryOverheadReserve

  private val ioQueue = new LinkedBlockingQueue[ActionRequest]()

  // Reported through the CRL thread, which dies of it: the CRL has one death path
  private val writer = StreamWriter(maxSizeInBytes, files, t => ioQueue.put(WriterDied(t)))
  private var currentStream: Int = r.activeStreamId.number
  // Replaced once the recovered records are queued, with the oldest entry they need
  private var currentLogEntry: LogEntry = new LogEntry(r.initialPreviousEntryLocation,
    r.initialNextEntrySerialNumber, r.initialOldestEntryNeeded)
  private val streams: Array[Stream] = new Array[Stream](files.length)

  private var transactions: HashMap[TxId, Tx] = new HashMap()

  /** Applied stubs: entries apart from transactions, under the same ids (StoreRecoveryState) */
  private var stubs: HashMap[TxId, Stub] = new HashMap()

  private var queueHead: Option[Record] = None
  private var queueTail: Option[Record] = None

  private var writeInProgress = false
  /** Barriers queued while the current entry was empty and a write was in progress: they run once it completes */
  private var afterInFlight: List[() => Unit] = Nil

  /** Shuts the writer down, releasing the lock once it has closed the stream files and before `completionHandler` */
  private def shutdownWriter(completionHandler: () => Unit): Unit =
    writer.shutdown: () =>
      lock.release()
      completionHandler()

  files.foreach: tpl =>
    if tpl._1 == r.activeStreamId then
      streams(tpl._1.number) = new Stream(r.activeStreamId, crlId, writer,
        r.currentStreamUUID, r.initialNextEntryOffset)
    else
      streams(tpl._1.number) = new Stream(tpl._1, crlId, writer, UUID.randomUUID(), 0)

  // The open writes nothing (crash-recovery-log F16): each recovered record keeps the locations its newest durable
  // static record names, queued by the oldest entry holding any of them, as if this instance had written them. The
  // last durable entry kept the ring's invariant (the stream after the active one holds nothing needed), so the next
  // rotation's copy-forward finds and moves them as the instance before the crash would have. A failed open shuts
  // its writer down: the caller may open the same files again
  try
    val recoveredTxs = r.txs.map: t =>
      (new Tx(t.id, t.state, Some(t.txdLocation), t.updateLocations), t.entrySerialNumber)
    val recoveredStubs = r.stubs.map: s =>
      val stub = new Stub(s.id, s.start)
      stub.anchor = Some(s.anchor)
      (stub, s.entrySerialNumber)
    (recoveredTxs ++ recoveredStubs).sortBy(_._2).foreach: (record, serial) =>
      record match
        case tx: Tx => transactions += (tx.id -> tx)
        case stub: Stub => stubs += (stub.id -> stub)
      addToQueueHead(record, serial)
    currentLogEntry = new LogEntry(r.initialPreviousEntryLocation, r.initialNextEntrySerialNumber,
      oldestEntryNeeded.max(r.initialOldestEntryNeeded))
  catch case t: Throwable =>
    val closed = new CountDownLatch(1)
    shutdownWriter(() => closed.countDown())
    closed.await()
    throw t

  override val recoveredBytesByStore: Map[StoreId, Long] =
    val byStore = transactions.valuesIterator.map(tx => tx.id.storeId -> CrlAdmission.recoveredBytes(tx.state)) ++
      stubs.keysIterator.map(id => id.storeId -> CrlAdmission.AppliedStubBytes)
    byStore.toList.groupMapReduce(_._1)(_._2)(_ + _)

  // Before the thread starts: its catch counts shutdownComplete down
  private val shutdownRequested = new AtomicBoolean(false)
  private val shutdownComplete = new CountDownLatch(1)

  // The CRL has failed: its thread is dead (of its own error or the writer's), and nothing it is asked from now on is
  // answered. `cause` is set before `dead`
  @volatile private var cause: Throwable = null
  @volatile private var dead = false
  private val death = Promise[Throwable]()

  // The promises of the Future-returning calls not answered yet. A death fails them, so no caller waits on a dead CRL
  // (crash-recovery-log F9). Handler-style completions (save, saveAppliedStub) are never run after a death: nothing
  // that waits on one, a vote above all, may proceed without the write
  private val pending = java.util.concurrent.ConcurrentHashMap.newKeySet[Promise[?]]()

  private def failPending(): Unit =
    pending.forEach(p => p.tryFailure(CrlFailed(cause)))

  /** Queues `request(p)` and returns `p`'s future, failed with CrlFailed if the CRL is or becomes dead first. Added
    * before `dead` is checked: either the dying thread's failPending finds `p`, or this check sees the death */
  private def tracked[T](request: Promise[T] => ActionRequest): Future[T] =
    val p = Promise[T]()
    pending.add(p)
    p.future.onComplete(_ => pending.remove(p))(using scala.concurrent.ExecutionContext.parasitic)
    ioQueue.put(request(p))
    if dead then
      p.tryFailure(CrlFailed(cause))
    p.future

  private val ioThread = new Thread {
    override def run(): Unit =
      try crlThread()
      catch case t: Throwable =>
        logger.error("CRL thread died", t)
        cause = t
        dead = true
        failPending()
        // Nothing takes the Shutdown that shutdown() queues now, so the shutdown is completed here: the files are
        // closed and the latch counted down once the writer finishes what it holds. Without it the device could not
        // be loaded again (its path stays in closingCrls) and the host's stop would wait forever.
        shutdownWriter(() => shutdownComplete.countDown())
        death.trySuccess(t)
        onThreadDeath(t)
  }
  ioThread.start()

  def currentEntrySerialNumber: Long = currentLogEntry.entrySerialNumber

  def currentStreamNumber: Int = currentStream

  def oldestEntryNeeded: Long = queueTail match
    case None => currentLogEntry.entrySerialNumber
    case Some(lc) => lc.entrySerialNumber

  private def startWrite(): Unit =
    if !writeInProgress && !currentLogEntry.isEmpty then
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
          onDeleteTransactions(storeId, transactionIds, resavable = false, completionHandler)

        case DropStore(storeId, completionHandler) =>
          // Read and deleted in one step: what any earlier request saved is dropped, nothing a later one saves
          // (crash-recovery-log F12). A save in this same entry cancels the deletion, as it is the store loaded again
          val ids = (transactions.keysIterator ++ stubs.keysIterator).filter(_.storeId == storeId)
            .map(_.transactionId).toList.distinct
          onDeleteTransactions(storeId, ids, resavable = true, completionHandler)

        case SaveAppliedStub(storeId, transactionId, start, completionHandler) =>
          onSaveAppliedStub(storeId, transactionId, start, completionHandler)

        case DeleteAppliedStub(storeId, transactionId) => onDeleteAppliedStub(storeId, transactionId)

        case WriterDied(t) => throw t // Nothing more can be written: the CRL dies with its writer

        case WriteComplete(serialNumber) =>
          logger.trace(s"CRL write complete for entry serial number: ${serialNumber}")

          writeInProgress = false
          val barriers = afterInFlight.reverse
          afterInFlight = Nil
          barriers.foreach(_())
          startWrite()

        case Barrier(handler) =>
          // Every earlier request is in the current entry, in the entry being written, or already durable
          if !currentLogEntry.isEmpty then
            currentLogEntry.addCompletionHandler(handler)
            startWrite()
          else if writeInProgress then
            afterInFlight = handler :: afterInFlight
          else
            handler()

        case GetRecoveryState(storeId, completion) =>
          onGetRecoveryState(storeId, completion)

        case GetStoresWithEntries(completion) =>
          completion((transactions.keysIterator ++ stubs.keysIterator).map(_.storeId).toSet)

        case RunOnCrlThread(f) => f()

        case Shutdown(completionHandler) =>
          shutdownWriter(completionHandler)
          return

  /** All in the current entry, with the handler, before its write starts: the handler then runs once every deletion is
    * durable. `resavable` deletions are cancelled by a save of the same id later in the entry
    * (LogEntry.deleteTxUnlessResaved) */
  private def onDeleteTransactions(storeId: StoreId, transactionIds: List[TransactionId], resavable: Boolean,
                                   completionHandler: () => Unit): Unit =
    if transactionIds.isEmpty then
      completionHandler()
    else
      transactionIds.foreach: transactionId =>
        val txid = TxId(storeId, transactionId)
        if resavable then
          currentLogEntry.deleteTxUnlessResaved(txid)
        else
          currentLogEntry.deleteTx(txid)
        transactions.get(txid).foreach(tx => removeFromQueue(tx))
        transactions -= txid
        currentLogEntry.deleteStub(txid)
        stubs.get(txid).foreach(stub => removeFromQueue(stub))
        stubs -= txid
      currentLogEntry.addCompletionHandler(completionHandler)
      startWrite()

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

  /** Queued by `entrySerialNumber`, the entry holding its oldest data: the current one, unless recovered */
  private def addToQueueHead(lc: Record, entrySerialNumber: Long = currentLogEntry.entrySerialNumber): Unit =
    lc.next = None
    lc.prev = queueHead
    queueHead.foreach(_.next = Some(lc))
    lc.entrySerialNumber = entrySerialNumber
    queueHead = Some(lc)
    if queueTail.isEmpty then
      queueTail = Some(lc)

  /** Leaves the transactions live: only a deletion, which is written, takes them out of the log */
  private def onGetRecoveryState(storeId: StoreId,
                                 completion: StoreRecoveryState => Unit
                                ): Unit =
    completion(StoreRecoveryState(
      transactions.valuesIterator.filter(_.id.storeId == storeId).map(_.state).toList,
      stubs.valuesIterator.filter(_.id.storeId == storeId).map(s => s.id.transactionId -> s.start).toMap))

  /** Replaces the transaction state held for the id, if any, in the same entry: its deletion and the stub are durable
    * together */
  private def onSaveAppliedStub(storeId: StoreId, transactionId: TransactionId, start: HLCTimestamp,
                                completionHandler: () => Unit): Unit =
    val txid = TxId(storeId, transactionId)
    transactions.get(txid).foreach: tx =>
      removeFromQueue(tx)
      transactions -= txid
      currentLogEntry.deleteTxUnlessResaved(txid)
    val stub = stubs.get(txid) match
      case Some(stub) => stub
      case None =>
        val stub = new Stub(txid, start)
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
    // Deleted earlier in this unwritten entry: the save is skipped but still reports success once the deletion is
    // durable (LogEntry.addTx). No record is created for it: one that is never written would pin oldestEntryNeeded
    // until the ring wraps over it, and be reported live by readStoreState (crash-recovery-log F4). Every deletion
    // removes the id from `transactions` too, so no record exists here
    if currentLogEntry.deletesFinally(txid) then
      currentLogEntry.addCompletionHandler(completionHandler)
      startWrite()
      return
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

  def getAppliedStubs(storeId: StoreId): Map[TransactionId, HLCTimestamp] = blockingRead(storeId).appliedStubs

  private def blockingRead(storeId: StoreId): StoreRecoveryState =
    scala.concurrent.Await.result(readStoreState(storeId), scala.concurrent.duration.Duration.Inf)

  def save(transactionId: TransactionId,
           state: TransactionRecoveryState,
           completionHandler: () => Unit): Unit =
    ioQueue.put(SaveTransaction(transactionId, state, completionHandler))

  def deleteTransaction(storeId: StoreId, transactionid: TransactionId): Unit =
    ioQueue.put(DeleteTransaction(storeId, transactionid))

  def dropTransactionObjectData(storeId: StoreId, transactionid: TransactionId): Unit =
    ioQueue.put(DropTransactionData(storeId, transactionid))

  def saveAppliedStub(storeId: StoreId, txid: TransactionId, start: HLCTimestamp, completionHandler: () => Unit): Unit =
    ioQueue.put(SaveAppliedStub(storeId, txid, start, completionHandler))

  override def whenDurable(storeId: StoreId, handler: () => Unit): Unit = ioQueue.put(Barrier(handler))

  def deleteAppliedStub(storeId: StoreId, txid: TransactionId): Unit =
    ioQueue.put(DeleteAppliedStub(storeId, txid))

  override def deleteTransactions(storeId: StoreId, txids: List[TransactionId]): Future[Unit] =
    tracked(p => DeleteTransactions(storeId, txids, () => p.trySuccess(())))

  override def readStoreState(storeId: StoreId): Future[StoreRecoveryState] =
    tracked(p => GetRecoveryState(storeId, state => p.trySuccess(state)))

  /** One request on the CRL thread, so it drops exactly what was saved before it (crash-recovery-log F12) */
  override def dropStore(storeId: StoreId): Future[Unit] =
    tracked(p => DropStore(storeId, () => p.trySuccess(())))

  override def storesWithEntries(): Future[Set[StoreId]] =
    tracked(p => GetStoresWithEntries(stores => p.trySuccess(stores)))

  /** As the default, with every save tracked, so a death fails the load rather than leaving it waiting */
  override def loadStore(storeId: StoreId, state: StoreRecoveryState): Future[Unit] =
    given scala.concurrent.ExecutionContext = scala.concurrent.ExecutionContext.parasitic
    // Stubs first: a stub's save replaces a transaction's state of the same id, which a later save then keeps
    Future.sequence(state.appliedStubs.toList.map: (txid, start) =>
      tracked[Unit](p => SaveAppliedStub(storeId, txid, start, () => p.trySuccess(())))
    ).flatMap: _ =>
      Future.sequence(state.transactions.map: trs =>
        tracked[Unit](p => SaveTransaction(trs.txd.transactionId, trs, () => p.trySuccess(())))
      ).map(_ => ())

  /** Tests only: runs `f` on the CRL thread once every request queued before it is processed */
  private[simple] def runOnCrlThread(f: () => Unit): Unit = ioQueue.put(RunOnCrlThread(f))

  override def failed: Future[Throwable] = death.future

  /** Idempotent: a later call waits for the first one's shutdown, as a second Shutdown would find no thread */
  override def shutdown(): Unit =
    if shutdownRequested.compareAndSet(false, true) then
      ioQueue.put(Shutdown(() => shutdownComplete.countDown()))
    shutdownComplete.await()
