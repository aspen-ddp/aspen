package org.aspen_ddp.aspen.server.crl.simple

import org.aspen_ddp.aspen.FileBasedTests
import org.aspen_ddp.aspen.common.ida.Replication
import org.aspen_ddp.aspen.common.{DataBuffer, HLCTimestamp}
import org.aspen_ddp.aspen.common.objects.{DataObjectPointer, ObjectId, ObjectRevision}
import org.aspen_ddp.aspen.common.paxos.{PersistentState, ProposalId}
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.transaction.{DataUpdate, DataUpdateOperation, ObjectUpdate, TransactionDescription, TransactionDisposition, TransactionId, TransactionStatus}
import org.aspen_ddp.aspen.server.crl.simple.{Recovery, StreamId, StreamLocation, Tx}
import org.aspen_ddp.aspen.server.FilePreallocationSuite
import org.aspen_ddp.aspen.server.{DurableFiles, FilePreallocation}
import org.aspen_ddp.aspen.server.crl.{CrashRecoveryLog, CrlAdmission, CrlCorrupt, CrlFailed, CrlLocked, CrlMissing, CrlSizingMismatch, StoreRecoveryState,
  TransactionRecoveryState}

import java.nio.ByteBuffer
import java.util.UUID
import java.util.concurrent.{CountDownLatch, LinkedBlockingQueue, TimeUnit}
import java.util.concurrent.atomic.AtomicBoolean
import scala.concurrent.Await
import scala.concurrent.duration.*
import scala.language.implicitConversions

object SimpleCRLSuite:
  val transactionId = TransactionId(new UUID(0, 1))
  val transactionId2 = TransactionId(new UUID(0, 3))
  val poolId = PoolId(new UUID(0, 2))
  val storeId = StoreId(poolId, 1)
  val storeId2 = StoreId(poolId, 2)

  val txid = TxId(storeId, transactionId)
  val txid2 = TxId(storeId2, transactionId2)

  val txdata = DataBuffer(Array[Byte](1, 2))
  val oud1 = DataBuffer(Array[Byte](3, 4))
  val oud2 = DataBuffer(Array[Byte](4))
  val ou1 = ObjectUpdate(ObjectId(new UUID(0, 3)), oud1)
  val ou2 = ObjectUpdate(ObjectId(new UUID(0, 4)), oud2)
  val disp = TransactionDisposition.VoteCommit
  val status = TransactionStatus.Unresolved
  val promise = ProposalId(1, 1)
  val accept = ProposalId(2, 2)
  val pax = PersistentState(Some(promise), Some((accept, true)))

  val timestamp = HLCTimestamp(2)

  val trs = TransactionRecoveryState(storeId, txdata, List(ou1, ou2), disp, status, pax)
  val trs2 = TransactionRecoveryState(storeId2, txdata, Nil, disp, status, pax)

  val txdLoc = StreamLocation(StreamId(0), 16, 4)
  val ou1Loc = StreamLocation(StreamId(0), 25, 2)
  val ou2Loc = StreamLocation(StreamId(0), 27, 1)

  val updateLocations = Some((ou1.objectId, ou1Loc) :: (ou2.objectId, ou2Loc) :: Nil)

  /** The CRL id of the streams the tests here write directly */
  val crlId = new UUID(0xc71, 1)

  val stream0 = StreamId(0)
  val stream1 = StreamId(1)
  val stream2 = StreamId(2)

  val oid1 = ObjectId(new UUID(0, 2))
  val ida = Replication(3, 2)
  val op1 = DataObjectPointer(oid1, poolId, Array[Byte]())
  val txd = TransactionDescription(transactionId, timestamp, op1, 1.toByte,
    List(DataUpdate(op1, ObjectRevision(transactionId), DataUpdateOperation.Overwrite)),
    List(), None, List(), List(), ida, Map(poolId -> ida))
  val trsValidTxd = TransactionRecoveryState(storeId, txd.serialize(), List(ou1, ou2), disp, status, pax)
  val trsValidTxd2 = TransactionRecoveryState(storeId2, txd.serialize(), List(ou1, ou2), disp, status, pax)

  val transactionId3 = TransactionId(new UUID(0, 5))
  val transactionId4 = TransactionId(new UUID(0, 6))
  val storeId3 = StoreId(poolId, 3)

  val paxInitial = PersistentState.initial
  val paxPromiseOnly = PersistentState(Some(promise), None)

  val trsUndetermined = TransactionRecoveryState(storeId, txd.serialize(), List(ou1, ou2),
    TransactionDisposition.Undetermined, TransactionStatus.Unresolved, paxInitial)
  val trsPromised = TransactionRecoveryState(storeId, txd.serialize(), List(ou1, ou2),
    TransactionDisposition.VoteCommit, TransactionStatus.Unresolved, paxPromiseOnly)

  val trsValidTxd3 = TransactionRecoveryState(storeId, txd.serialize(), List(ou1), disp, status, pax)
  val trsValidTxd4 = TransactionRecoveryState(storeId3, txd.serialize(), List(ou1, ou2), disp, status, pax)


class SimpleCRLSuite extends FileBasedTests {
  import SimpleCRLSuite._

  /** The start timestamp the stubs here carry: a fixed one, so recovered states compare equal */
  private val stubStart = HLCTimestamp(0x5eed0000L)

  /** The id the CRL in `dir` recorded at its creation */
  def recordedId(dir: os.Path): UUID = SimpleCRL.Sizing.decode(os.read(dir / SimpleCRL.SizingFile)).get.crlId

  def streams(numStreams: Int): List[(StreamId, os.Path)] =
    var lst: List[(StreamId, os.Path)] = Nil
    for i <- 0 until numStreams do
      lst = (StreamId(i), tdir / f"$i.log") :: lst
    lst.reverse

  /** A writer over `numStreams` stream files, preallocated as SimpleCRL creates them */
  def newWriter(maxSizeInBytes: Long, numStreams: Int): StreamWriter =
    val files = streams(numStreams)
    files.foreach((_, f) => FilePreallocation.preallocate(f, maxSizeInBytes))
    new StreamWriter(maxSizeInBytes, files)

  private def saveAndWait(crl: CrashRecoveryLog, txid: TransactionId, trs: TransactionRecoveryState): Unit =
    val q = new LinkedBlockingQueue[String]()
    crl.save(txid, trs, () => q.put(""))
    q.take()

  /** A completion handler runs on the writer thread, possibly before the CRL thread has moved on to the next entry
    * (writeCurrentLogEntry replaces currentLogEntry after handing the entry to the writer). A round trip through the
    * CRL thread orders the test's reads of its state after that */
  private def settle(crl: CrashRecoveryLog): Unit =
    Await.result(crl.storesWithEntries(), 10.seconds)

  test("Save & Recover CRL Saved State File Foo") {
    val savePath = tdir / "crl_save_file.log"
    val queue = new LinkedBlockingQueue[String]()

    def completionHandler(): Unit = queue.put("")

    val i = SimpleCRL(tdir, 3, 1024 * 1024)

    i.crl.save(transactionId, trsValidTxd, completionHandler)

    queue.take() // Block till completion handlers are run

    i.crl.save(transactionId, trsValidTxd2, completionHandler)

    queue.take() // Block till completion handlers are run

    val trs1 = i.crl.getFullRecoveryState(storeId)

    assert(trs1.size == 1)

    val t2 = i.crl.getFullRecoveryState(storeId2)

    assert(t2.size == 1)

    val t = Await.result(i.crl.readStoreState(storeId), Duration(5000, MILLISECONDS))

    // Read, not removed: only the deletion takes them out
    assert(i.crl.getFullRecoveryState(storeId).size == 1)

    Await.result(i.crl.deleteTransactions(storeId, t.entryIds), Duration(5000, MILLISECONDS))

    val t3 = i.crl.getFullRecoveryState(storeId)

    assert(t3.size == 0)

    val t4 = i.crl.getFullRecoveryState(storeId2)

    assert(t4.size == 1)

    CrashRecoveryLog.saveStoreState(storeId, t, savePath)

    val (sid, trl) = CrashRecoveryLog.loadStoreState(savePath)

    assert(sid == storeId)
    assert(trl.transactions.length == 1)

    assert(trl.transactions.head == trsValidTxd)

    Await.result(i.crl.loadStore(sid, trl), Duration(5000, MILLISECONDS))

    val t5 = i.crl.getFullRecoveryState(storeId)

    assert(t5.size == 1)

    val t6 = i.crl.getFullRecoveryState(storeId2)

    assert(t6.size == 1)

    i.crl.shutdown()
  }

  // A store rebuilt while its device is shut down is given an empty save file so an adopting
  // host accepts it. Loading that file must restore nothing and succeed.
  test("An empty CRL saved state file loads with no transactions") {
    val savePath = tdir / "crl_save_file.log"
    CrashRecoveryLog.saveStoreState(storeId, StoreRecoveryState.Empty, savePath)

    val (sid, trl) = CrashRecoveryLog.loadStoreState(savePath)
    assert(sid == storeId)
    assert(trl.isEmpty)

    val i = SimpleCRL(tdir, 3, 1024 * 1024)
    Await.result(i.crl.loadStore(sid, trl), Duration(5000, MILLISECONDS))
    assert(i.crl.getFullRecoveryState(storeId).isEmpty)
    i.crl.shutdown()
  }

  // An adopting host decides from this file whether a store's CRL state survived the move. A
  // file cut short by a crash or a pulled disk must fail to load, never load as fewer entries.
  test("A truncated CRL saved state file fails to load") {
    val savePath = tdir / "crl_save_file.log"
    CrashRecoveryLog.saveStoreState(storeId, StoreRecoveryState(List(trsValidTxd)), savePath)
    val full = os.read.bytes(savePath)
    assert(CrashRecoveryLog.loadStoreState(savePath)._2.transactions.length == 1)
    assert(os.list(tdir).map(_.last).toSet == Set("crl_save_file.log"))

    // Zero length, mid-header, mid-length-prefix and mid-entry
    for cut <- List(0, 9, 17 + 3, full.length - 1) do
      os.write.over(savePath, full.take(cut))
      assertThrows[CrashRecoveryLog.TruncatedSaveFile](CrashRecoveryLog.loadStoreState(savePath))
  }

  test("loadValidStoreState accepts only an intact file naming the store") {
    val savePath = tdir / "crl_save_file.log"
    val other = StoreId(storeId.poolId, (storeId.poolIndex + 1).toByte)

    assert(CrashRecoveryLog.loadValidStoreState(savePath, storeId).isLeft)

    CrashRecoveryLog.saveStoreState(storeId, StoreRecoveryState(List(trsValidTxd)), savePath)
    assert(CrashRecoveryLog.loadValidStoreState(savePath, storeId) == Right(StoreRecoveryState(List(trsValidTxd))))
    assert(CrashRecoveryLog.loadValidStoreState(savePath, other).isLeft)

    os.write.over(savePath, os.read.bytes(savePath).dropRight(1))
    assert(CrashRecoveryLog.loadValidStoreState(savePath, storeId).isLeft)
  }

  test("SimpleCRL Drop Transaction Data") {
    val queue = new LinkedBlockingQueue[String]()

    def completionHandler(): Unit = queue.put("")

    val i = SimpleCRL(tdir, 3, 1024 * 1024)

    i.crl.save(transactionId, trsValidTxd, completionHandler)

    queue.take() // Block till completion handlers are run

    i.crl.dropTransactionObjectData(storeId, transactionId)
    i.crl.save(transactionId, trsValidTxd, completionHandler)
    queue.take()

    i.crl.shutdown()

    val i2 = SimpleCRL(tdir, 3, 1024 * 1024)

    assert(i2.trsList.length == 1)

    assert(i2.trsList.head.storeId == txid.storeId)
    assert(i2.trsList.head.disposition == disp)
    assert(i2.trsList.head.serializedTxd == trsValidTxd.serializedTxd)
    assert(i2.trsList.head.objectUpdates == List())
    assert(i2.trsList.head.paxosAcceptorState == pax)
  }

  test("SimpleCRL Recycle Streams") {
    val queue = new LinkedBlockingQueue[String]()

    def completionHandler(): Unit = queue.put("")

    val i = SimpleCRL(tdir, 3, 4096 * 3)

    i.crl.save(transactionId, trsValidTxd, completionHandler)

    queue.take() // Block till completion handlers are run
    assert(i.crl.currentStreamNumber == 0)

    i.crl.save(transactionId, trsValidTxd, completionHandler)

    queue.take()
    assert(i.crl.currentStreamNumber == 0)

    i.crl.save(transactionId, trsValidTxd, completionHandler)

    queue.take()
    assert(i.crl.currentStreamNumber == 0)

    i.crl.save(transactionId, trsValidTxd, completionHandler)

    queue.take()
    assert(i.crl.currentStreamNumber == 1)

    i.crl.save(transactionId2, trsValidTxd, completionHandler)

    queue.take()
    assert(i.crl.currentStreamNumber == 1)

    i.crl.save(transactionId2, trsValidTxd, completionHandler)
    queue.take()
    assert(i.crl.currentStreamNumber == 1)

    // The rotation into stream 2 writes its evacuation of stream 0 (the first transaction's pieces) before the save
    i.crl.save(transactionId2, trsValidTxd, completionHandler)
    queue.take()
    assert(i.crl.currentStreamNumber == 2)

    i.crl.save(transactionId2, trsValidTxd, completionHandler)
    queue.take()
    assert(i.crl.currentStreamNumber == 2)

    // Stream 2 holds the evacuation and two saves: this one rotates into stream 0
    i.crl.save(transactionId2, trsValidTxd, completionHandler)
    queue.take()
    assert(i.crl.currentStreamNumber == 0)

    i.crl.save(transactionId2, trsValidTxd, completionHandler)
    i.crl.deleteTransaction(storeId, transactionId2)
    queue.take()
    // The deletion is in the entry after the save's: stream 0 (the evacuation and two saves) has no room for it, so it
    // rotates into stream 1. Waited for, since it is written only once the save's write completes
    i.crl.whenDurable(storeId, () => queue.put("deleted"))
    assert(queue.take() == "deleted")
    assert(i.crl.currentStreamNumber == 1)

    // The save's completion handler can fire before the delete has been written, and shutdown
    // does not flush an unwritten entry. Requests are processed in order, so a save that completes
    // after the delete proves the delete is durable.
    i.crl.save(transactionId, trsValidTxd, completionHandler)
    queue.take()

    i.crl.shutdown()

    val i2 = SimpleCRL(tdir, 3, 4096 * 3)

    assert(i2.trsList.length == 1)

    assert(i2.trsList.head.storeId == txid.storeId)
    assert(i2.trsList.head.disposition == disp)
    assert(i2.trsList.head.txd.transactionId == transactionId)
    assert(i2.trsList.head.serializedTxd == trsValidTxd.serializedTxd)
    assert(i2.trsList.head.objectUpdates == trsValidTxd.objectUpdates)
    assert(i2.trsList.head.paxosAcceptorState == pax)
  }

  test("SimpleCRL Recovery From Multiple Streams") {
    val queue = new LinkedBlockingQueue[String]()

    def completionHandler(): Unit = queue.put("")

    val i = SimpleCRL(tdir, 3, 4096 * 3)

    i.crl.save(transactionId, trsValidTxd, completionHandler)

    queue.take() // Block till completion handlers are run
    assert(i.crl.currentStreamNumber == 0)

    i.crl.save(transactionId, trsValidTxd, completionHandler)

    queue.take()
    assert(i.crl.currentStreamNumber == 0)

    i.crl.save(transactionId, trsValidTxd, completionHandler)

    queue.take()
    assert(i.crl.currentStreamNumber == 0)

    i.crl.save(transactionId, trsValidTxd, completionHandler)

    queue.take()
    assert(i.crl.currentStreamNumber == 1)

    i.crl.save(transactionId2, trsValidTxd, completionHandler)

    queue.take()
    assert(i.crl.currentStreamNumber == 1)

    // Fill stream 1 so the delete+save below must move to stream 2 whether or not the CRL thread
    // batches them into one entry. The completion handler runs before the CRL thread sees the
    // write-complete event, so the batching of back-to-back requests depends on thread timing.
    i.crl.save(transactionId, trsValidTxd, completionHandler)

    queue.take()
    assert(i.crl.currentStreamNumber == 1)

    i.crl.deleteTransaction(storeId, transactionId2)
    i.crl.save(transactionId, trsValidTxd, completionHandler)

    queue.take()
    assert(i.crl.currentStreamNumber == 2)

    i.crl.shutdown()

    val i2 = SimpleCRL(tdir, 3, 4096 * 3)

    assert(i2.trsList.length == 1)

    assert(i2.trsList.head.storeId == txid.storeId)
    assert(i2.trsList.head.disposition == disp)
    assert(i2.trsList.head.serializedTxd == trsValidTxd.serializedTxd)
    assert(i2.trsList.head.objectUpdates == trsValidTxd.objectUpdates)
    assert(i2.trsList.head.paxosAcceptorState == pax)
  }

  test("SimpleCRL Switch Streams") {
    val queue = new LinkedBlockingQueue[String]()

    def completionHandler(): Unit = queue.put("")

    val i = SimpleCRL(tdir, 3, 4096*3)

    i.crl.save(transactionId, trsValidTxd, completionHandler)

    queue.take() // Block till completion handlers are run
    assert(i.crl.currentStreamNumber == 0)

    i.crl.save(transactionId, trsValidTxd, completionHandler)

    queue.take()
    assert(i.crl.currentStreamNumber == 0)

    i.crl.save(transactionId, trsValidTxd, completionHandler)

    queue.take()
    assert(i.crl.currentStreamNumber == 0)

    i.crl.save(transactionId, trsValidTxd, completionHandler)

    queue.take()
    assert(i.crl.currentStreamNumber == 1)

    i.crl.shutdown()

    val i2 = SimpleCRL(tdir, 3, 4096*3)

    assert(i2.trsList.length == 1)

    assert(i2.trsList.head.storeId == txid.storeId)
    assert(i2.trsList.head.disposition == disp)
    assert(i2.trsList.head.serializedTxd == trsValidTxd.serializedTxd)
    assert(i2.trsList.head.objectUpdates == trsValidTxd.objectUpdates)
    assert(i2.trsList.head.paxosAcceptorState == pax)
  }

  test("SimpleCRL Basic Functionality") {
    val queue = new LinkedBlockingQueue[String]()

    def completionHandler(): Unit = queue.put("")

    val i = SimpleCRL(tdir, 3, 1024*1024)

    i.crl.save(transactionId, trsValidTxd, completionHandler)

    queue.take() // Block till completion handlers are run

    settle(i.crl)
    assert(i.crl.currentEntrySerialNumber == 1)

    i.crl.shutdown()

    val i2 = SimpleCRL(tdir, 3, 1024*1024)

    // The open writes nothing: the next entry is the one after the last written (crash-recovery-log F16)
    assert(i2.crl.currentEntrySerialNumber == 1)

    assert(i2.trsList.length == 1)

    assert(i2.trsList.head.storeId == txid.storeId)
    assert(i2.trsList.head.disposition == disp)
    assert(i2.trsList.head.serializedTxd == trsValidTxd.serializedTxd)
    assert(i2.trsList.head.objectUpdates == trsValidTxd.objectUpdates)
    assert(i2.trsList.head.paxosAcceptorState == pax)
  }

  test("Save & Recover with Tx in Multiple Log Entries") {
    val queue = new LinkedBlockingQueue[String]()

    def completionHandler(): Unit = queue.put("")

    val streamWriter = newWriter(4096 * 1000, 1)

    // One image for both entries, as SimpleCRL keeps: the second entry names the pieces the first wrote, so its
    // oldestEntryNeeded is the first
    val image = new Image(new LivePieces)
    val le = LogEntry(image)
    val stream = new Stream(stream0, crlId, streamWriter, UUID.randomUUID(), 0)

    le.addTx(txid, TxSave(TxForm(trs, keepUpdates = true), writesPieces = true), completionHandler)

    val entry1Location = stream.writeEntry(le, 0, StreamLocation.Null, () => ())

    val le2 = LogEntry(image)

    le2.addTx(txid, TxSave(TxForm(trs, keepUpdates = true), writesPieces = false), completionHandler)

    val entry2Location = stream.writeEntry(le2, 1, entry1Location, () => ())

    // Both entries durable, and the writer stopped, before recovery reads the stream: one take left the second
    // entry's write in flight while recovery read it
    queue.take()
    queue.take()
    val stopped = new java.util.concurrent.CountDownLatch(1)
    streamWriter.shutdown(() => stopped.countDown())
    stopped.await()

    val r = Recovery.recover(streams(1), crlId, Vector(stream0))

    assert(r.trsList.length == 1)
    assert(r.activeStreamId == stream0)

    assert(r.trsList.head.storeId == txid.storeId)
    assert(r.trsList.head.disposition == disp)
    assert(r.trsList.head.serializedTxd == trs.serializedTxd)
    assert(r.trsList.head.objectUpdates == trs.objectUpdates)
    assert(r.trsList.head.paxosAcceptorState == pax)
  }

  test("Tx Save & Recover with Single Transaction") {
    val queue = new LinkedBlockingQueue[String]()
    def completionHandler(): Unit = queue.put("")

    val streamWriter = newWriter(4096*1000, 1)

    val le = LogEntry(new Image(new LivePieces))
    val stream = new Stream(stream0, crlId, streamWriter, UUID.randomUUID(), 0)

    le.addTx(txid, TxSave(TxForm(trs, keepUpdates = true), writesPieces = true), completionHandler)

    val entryLocation = stream.writeEntry(le, 0, StreamLocation.Null, () => ())

    assert(entryLocation.streamId == stream0)
    assert(entryLocation.offset == 0)

    queue.take() // Block till completion handlers are run

    streamWriter.shutdown()

    val r = Recovery.recover(streams(1), crlId, Vector(stream0))

    assert(r.trsList.length == 1)
    assert(r.activeStreamId == stream0)

    assert(r.trsList.head.storeId == txid.storeId)
    assert(r.trsList.head.disposition == disp)
    assert(r.trsList.head.serializedTxd == trs.serializedTxd)
    assert(r.trsList.head.objectUpdates == trs.objectUpdates)
    assert(r.trsList.head.paxosAcceptorState == pax)
  }

  test("Recovery without log files") {
    val r = Recovery.recover(Nil, crlId, Vector())

    assert(r.trsList.length == 0)
    assert(r.activeStreamId == stream0)
  }

  test("Tx Static Save & Load with ObjectUpdate data") {
    val form = TxForm(trs.copy(status = TransactionStatus.Committed), keepUpdates = true)
    val pieces = Vector(Piece(txdLoc, 0), Piece(ou1Loc, 0), Piece(ou2Loc, 0))

    val buffer = new Array[Byte](4096 * 5)

    assert(TxSave(form, writesPieces = false).dynamicDataSize == 0)

    Tx.writeStatic(ByteBuffer.wrap(buffer), txid, form, pieces)

    val ltx = Tx.loadTx(ByteBuffer.wrap(buffer))

    assert(ltx.id == txid)
    assert(ltx.disposition == disp)
    assert(ltx.txdLocation == txdLoc)
    assert(ltx.updateLocations == updateLocations)
    assert(ltx.paxosAcceptorState == pax)
    assert(ltx.status == TransactionStatus.Committed)
  }

  test("Tx Static Save & Load without ObjectUpdate data") {
    val form = TxForm(trs, keepUpdates = false)

    val buffer = new Array[Byte](4096 * 5)

    assert(TxSave(form, writesPieces = false).dynamicDataSize == 0)

    Tx.writeStatic(ByteBuffer.wrap(buffer), txid, form, Vector(Piece(txdLoc, 0)))

    val ltx = Tx.loadTx(ByteBuffer.wrap(buffer))

    assert(ltx.id == txid)
    assert(ltx.disposition == disp)
    assert(ltx.txdLocation == txdLoc)
    assert(ltx.updateLocations.isEmpty)
    assert(ltx.paxosAcceptorState == pax)
  }

  test("Recovery after deleting oldest transaction") {
    val queue = new LinkedBlockingQueue[String]()

    def completionHandler(): Unit = queue.put("")

    val i = SimpleCRL(tdir, 3, 1024 * 1024)

    i.crl.save(transactionId, trsValidTxd, completionHandler)
    queue.take()

    i.crl.save(transactionId2, trsValidTxd2, completionHandler)
    queue.take()

    i.crl.deleteTransaction(storeId, transactionId)
    i.crl.save(transactionId2, trsValidTxd2, completionHandler)
    queue.take()

    i.crl.shutdown()

    val i2 = SimpleCRL(tdir, 3, 1024 * 1024)

    assert(i2.trsList.length == 1)
    assert(i2.trsList.head.storeId == storeId2)
    assert(i2.trsList.head.disposition == disp)
    assert(i2.trsList.head.serializedTxd == trsValidTxd2.serializedTxd)
    assert(i2.trsList.head.objectUpdates == trsValidTxd2.objectUpdates)
    assert(i2.trsList.head.paxosAcceptorState == pax)
  }

  // ===== Tier 1: Critical Crash Recovery Correctness =====

  test("Recovery after crash without clean shutdown") {
    val i = SimpleCRL(tdir, 3, 1024 * 1024)

    saveAndWait(i.crl, transactionId, trsValidTxd)
    saveAndWait(i.crl, transactionId2, trsValidTxd2)

    // Simulate a crash: the CRL stops with no further write (its lock goes with it, as with a dead process)
    i.crl.shutdown()
    val i2 = SimpleCRL(tdir, 3, 1024 * 1024)

    assert(i2.trsList.length == 2)
    val byStore = i2.trsList.groupBy(_.storeId)
    assert(byStore(storeId).head.serializedTxd == trsValidTxd.serializedTxd)
    assert(byStore(storeId).head.objectUpdates == trsValidTxd.objectUpdates)
    assert(byStore(storeId).head.paxosAcceptorState == pax)
    assert(byStore(storeId2).head.serializedTxd == trsValidTxd2.serializedTxd)

    i2.crl.shutdown()
  }

  test("A crash recovers each record's saved status, as readStoreState returns it (CRL F14)") {
    val i = SimpleCRL(tdir, 3, 1024 * 1024)

    val committed = trsValidTxd.copy(status = TransactionStatus.Committed)
    val aborted = trsValidTxd2.copy(status = TransactionStatus.Aborted,
      paxosAcceptorState = PersistentState(Some(promise), Some((accept, false))))
    val unresolved = trsValidTxd4
    saveAndWait(i.crl, transactionId, committed)
    saveAndWait(i.crl, transactionId2, aborted)
    saveAndWait(i.crl, transactionId4, unresolved)

    def statuses(crl: CrashRecoveryLog): Map[StoreId, TransactionStatus.Value] =
      List(storeId, storeId2, storeId3).map: s =>
        s -> Await.result(crl.readStoreState(s), 10.seconds).transactions.head.status
      .toMap

    val expected = Map(storeId -> TransactionStatus.Committed, storeId2 -> TransactionStatus.Aborted,
      storeId3 -> TransactionStatus.Unresolved)
    assert(statuses(i.crl) == expected)

    i.crl.shutdown()
    val i2 = SimpleCRL(tdir, 3, 1024 * 1024)
    assert(i2.trsList.map(t => t.storeId -> t.status).toMap == expected)
    assert(statuses(i2.crl) == expected)
    assert(i2.trsList.find(_.storeId == storeId2).get.paxosAcceptorState == aborted.paxosAcceptorState)
    i2.crl.shutdown()
  }

  test("A save with object data the transaction's earlier saves lacked is written and recovered") {
    val i = SimpleCRL(tdir, 3, 1024 * 1024)

    def save(state: TransactionRecoveryState): Unit =
      val q = new LinkedBlockingQueue[String]()
      i.crl.save(transactionId, state, () => q.put(""))
      assert(q.poll(10, TimeUnit.SECONDS) == "", "the CRL did not complete the save")

    save(trsValidTxd.copy(objectUpdates = Nil))
    save(trsValidTxd)

    i.crl.shutdown()

    val i2 = SimpleCRL(tdir, 3, 1024 * 1024)

    assert(i2.trsList.length == 1)
    assert(i2.trsList.head.objectUpdates.map(_.objectId) == List(ou1.objectId, ou2.objectId))
    assert(i2.trsList.head.objectUpdates.map(_.data.getByteArray.toList) ==
      List(oud1.getByteArray.toList, oud2.getByteArray.toList))

    i2.crl.shutdown()
  }

  test("A CRL holding a live transaction recovers it across repeated reopens") {
    val i = SimpleCRL(tdir, 3, 1024 * 1024)
    saveAndWait(i.crl, transactionId, trsValidTxd)
    i.crl.shutdown()

    // Each reopen recovers it from the entry that wrote it: an open writes nothing
    for n <- 1 to 3 do
      val opened = new LinkedBlockingQueue[SimpleCRL.InitialCRLState]()
      val t = new Thread(() => opened.put(SimpleCRL(tdir, 3, 1024 * 1024)))
      t.setDaemon(true)
      t.start()
      val r = opened.poll(10, TimeUnit.SECONDS)
      assert(r != null, s"reopen $n did not finish its recovery")
      assert(r.trsList.map(_.txd.transactionId) == List(transactionId), s"reopen $n")
      r.crl.shutdown()
  }

  test("A second open of a CRL that is open fails with CrlLocked and leaves the first working") {
    val i = SimpleCRL(tdir, 3, 1024 * 1024)
    saveAndWait(i.crl, transactionId, trsValidTxd)

    assertThrows[CrlLocked](SimpleCRL(tdir, 3, 1024 * 1024))

    // The refused opener wrote nothing: the first CRL still saves, and its state recovers intact
    saveAndWait(i.crl, transactionId2, trsValidTxd2)
    i.crl.shutdown()
    val i2 = SimpleCRL(tdir, 3, 1024 * 1024)
    assert(i2.trsList.map(_.storeId).toSet == Set(storeId, storeId2))
    i2.crl.shutdown()
  }

  test("A failed open releases the CRL lock") {
    SimpleCRL(tdir, 3, 1024 * 1024).crl.shutdown()
    os.write.over(tdir / "2.crl", "truncated")
    assertThrows[CrlSizingMismatch](SimpleCRL(tdir, 3, 1024 * 1024))
    // Refused for its sizing again, not for a lock still held
    assertThrows[CrlSizingMismatch](SimpleCRL(tdir, 3, 1024 * 1024))
  }

  test("A CRL that must exist but has no stream files is missing, and nothing is written") {
    // Never created
    assertThrows[CrlMissing](SimpleCRL(tdir, 3, 1024 * 1024, mustExist = true))
    assert(os.list(tdir).filter(_.last != "lock").isEmpty)
    // Its stream files removed, its sizing left
    SimpleCRL(tdir, 3, 1024 * 1024).crl.shutdown()
    os.list(tdir).filter(_.ext == "crl").foreach(os.remove(_))
    val remains = os.list(tdir).map(_.last).toSet
    assertThrows[CrlMissing](SimpleCRL(tdir, 3, 1024 * 1024, mustExist = true))
    assert(os.list(tdir).map(_.last).toSet == remains)
    // Refused for missing again, not for a lock still held
    assertThrows[CrlMissing](SimpleCRL(tdir, 3, 1024 * 1024, mustExist = true))
  }

  test("A CRL that must exist and does opens") {
    SimpleCRL(tdir, 3, 1024 * 1024).crl.shutdown()
    SimpleCRL(tdir, 3, 1024 * 1024, mustExist = true).crl.shutdown()
  }

  test("Creating a CRL records its sizing before any stream file") {
    SimpleCRL(tdir, 4, 1024 * 1024).crl.shutdown()
    val recorded = SimpleCRL.Sizing.decode(os.read(tdir / SimpleCRL.SizingFile))
    assert(recorded.map(r => (r.numStreams, r.maxSizeInBytes)) == Some((4, 1024 * 1024)))
    assert(os.list(tdir).filter(_.ext == "crl").map(_.last).toSet == Set("0.crl", "1.crl", "2.crl", "3.crl"))
  }

  test("An existing CRL opens with the sizing it recorded, whatever it is asked for, and keeps its entries") {
    val i = SimpleCRL(tdir, 3, 1024 * 1024)
    saveAndWait(i.crl, transactionId, trsValidTxd)
    i.crl.shutdown()

    for (n, size) <- List((2, 1024L * 1024), (4, 1024L * 1024), (3, 2L * 1024 * 1024)) do
      val i2 = SimpleCRL(tdir, n, size)
      assert(i2.trsList.map(_.storeId) == List(storeId))
      assert(i2.crl.admissionLimitBytes == 1024 * 1024 * 7 / 16)
      i2.crl.shutdown()
    assert(os.list(tdir).filter(_.ext == "crl").map(_.last).toSet == Set("0.crl", "1.crl", "2.crl"))
  }

  test("A sizing file without stream files is a CRL whose creation crashed: it is created with that sizing") {
    os.write(tdir / SimpleCRL.SizingFile, SimpleCRL.Sizing.inOrder(4, 1024 * 1024, UUID.randomUUID()).encode)
    val i = SimpleCRL(tdir, 3, 2 * 1024 * 1024)
    assert(i.crl.admissionLimitBytes == 1024 * 1024 * 15 / 16)
    i.crl.shutdown()
    assert(os.list(tdir).filter(_.ext == "crl").size == 4)
  }

  test("Stream files without a readable sizing file are a sizing mismatch, and nothing is written") {
    SimpleCRL(tdir, 3, 1024 * 1024).crl.shutdown()
    os.remove(tdir / SimpleCRL.SizingFile)
    assertThrows[CrlSizingMismatch](SimpleCRL(tdir, 3, 1024 * 1024))
    assert(!os.exists(tdir / SimpleCRL.SizingFile))
    os.write(tdir / SimpleCRL.SizingFile, "garbage")
    assertThrows[CrlSizingMismatch](SimpleCRL(tdir, 3, 1024 * 1024))
    os.write.over(tdir / SimpleCRL.SizingFile, SimpleCRL.Sizing.inOrder(1, 1024 * 1024, UUID.randomUUID()).encode)
    assertThrows[CrlSizingMismatch](SimpleCRL(tdir, 3, 1024 * 1024))
  }

  test("Stream files disagreeing with the recorded sizing are a sizing mismatch") {
    SimpleCRL(tdir, 3, 1024 * 1024).crl.shutdown()
    os.write.over(tdir / SimpleCRL.SizingFile, SimpleCRL.Sizing.inOrder(4, 1024 * 1024, UUID.randomUUID()).encode)
    assertThrows[CrlSizingMismatch](SimpleCRL(tdir, 3, 1024 * 1024))
    os.write.over(tdir / SimpleCRL.SizingFile, SimpleCRL.Sizing.inOrder(3, 2 * 1024 * 1024, UUID.randomUUID()).encode)
    assertThrows[CrlSizingMismatch](SimpleCRL(tdir, 3, 1024 * 1024))
  }

  test("A CRL created with a ring out of numeric order rotates and recovers in ring order") {
    val size = 4096L * 3
    val ring = Vector(0, 2, 1).map(StreamId(_))
    var i = SimpleCRL(tdir, 3, size, ringForTest = Some(ring))
    // A record saved once in stream 0 is copied forward by the rotations that follow the ring, not numeric order
    saveAndWait(i.crl, transactionId, trsValidTxd)
    def live(states: List[TransactionRecoveryState]) =
      states.map(s => (s.storeId, s.serializedTxd, s.objectUpdates, s.paxosAcceptorState)).sortBy(_._1.poolIndex)
    val expected = live(List(trsValidTxd, trsValidTxd2))
    for (from, to) <- List((0, 2), (2, 1), (1, 0)) do
      var saves = 0
      while i.crl.currentStreamNumber != to do
        assert(i.crl.currentStreamNumber == from)
        saveAndWait(i.crl, transactionId2, trsValidTxd2)
        settle(i.crl)
        saves += 1
        assert(saves < 10, s"no rotation from stream $from to $to")
      i.crl.shutdown()
      i = SimpleCRL(tdir, 3, size)
      assert(i.crl.currentStreamNumber == to)
      assert(live(i.trsList) == expected)
    i.crl.shutdown()
    assert(SimpleCRL.Sizing.decode(os.read(tdir / SimpleCRL.SizingFile)).map(_.ring) == Some(ring))
  }

  test("Sizing without a ring line is unreadable") {
    SimpleCRL(tdir, 3, 1024 * 1024).crl.shutdown()
    val text = os.read(tdir / SimpleCRL.SizingFile)
    assert(text.linesIterator.contains("ring: 0,1,2"))
    os.write.over(tdir / SimpleCRL.SizingFile, text.linesIterator.filterNot(_.startsWith("ring:")).mkString("\n"))
    assertThrows[CrlSizingMismatch](SimpleCRL(tdir, 3, 1024 * 1024))
    // Nor is a ring that is not a permutation of the streams
    for bad <- List("0,1,1", "0,1", "0,1,3", "0,1,2,3") do
      os.write.over(tdir / SimpleCRL.SizingFile, text.replace("ring: 0,1,2", s"ring: $bad"))
      assertThrows[CrlSizingMismatch](SimpleCRL(tdir, 3, 1024 * 1024))
  }

  test("A stream file numbered past the ring is an uncommitted growth: removed at open") {
    val size = 4096L * 3
    val i = SimpleCRL(tdir, 3, size)
    saveAndWait(i.crl, transactionId, trsValidTxd)
    i.crl.shutdown()
    for leftover <- List("3.crl", "3.crl.tmp") do
      os.write(tdir / leftover, new Array[Byte](size.toInt))
      val i2 = SimpleCRL(tdir, 3, size)
      assert(!os.exists(tdir / leftover))
      assert(i2.trsList.map(_.storeId) == List(storeId))
      i2.crl.shutdown()
      assert(os.list(tdir).filter(_.ext == "crl").map(_.last).toSet == Set("0.crl", "1.crl", "2.crl"))
  }

  test("Any other unlisted stream file is a sizing mismatch") {
    val size = 4096L * 3
    SimpleCRL(tdir, 3, size).crl.shutdown()
    os.write(tdir / "5.crl", new Array[Byte](size.toInt))
    assertThrows[CrlSizingMismatch](SimpleCRL(tdir, 3, size))
    assert(os.exists(tdir / "5.crl"))
  }

  test("A CRL whose thread died has released its lock once its shutdown returns") {
    val died = new LinkedBlockingQueue[Throwable]()
    val i = SimpleCRL(tdir, 3, 8192, t => died.put(t))
    val tooBig = ObjectUpdate(ou1.objectId, DataBuffer(new Array[Byte](8192)))
    i.crl.save(transactionId, trsValidTxd.copy(objectUpdates = List(tooBig)), () => ())
    assert(died.poll(10, TimeUnit.SECONDS) != null)

    i.crl.shutdown()
    SimpleCRL(tdir, 3, 8192).crl.shutdown()
  }

  /** Every Future-returning call of a failed CRL fails with CrlFailed, whether queued before the death or after */
  private def assertFailsFast(crl: CrashRecoveryLog): Unit =
    def failsFast(f: => scala.concurrent.Future[?]): Unit =
      val result = Await.ready(f, 10.seconds).value.get
      assert(result.failed.toOption.exists(_.isInstanceOf[CrlFailed]), s"expected CrlFailed, got $result")
    failsFast(crl.readStoreState(storeId))
    failsFast(crl.deleteTransactions(storeId, List(transactionId)))
    failsFast(crl.storesWithEntries())
    failsFast(crl.dropStore(storeId))
    failsFast(crl.loadStore(storeId2, StoreRecoveryState(List(trsValidTxd2), Map(transactionId2 -> stubStart))))
    assertThrows[CrlFailed](crl.getFullRecoveryState(storeId))

  test("A CRL whose writer thread died fails every call, reports the death once and recovers on reopen") {
    val died = new LinkedBlockingQueue[Throwable]()
    val i = SimpleCRL(tdir, 3, 1024 * 1024, t => died.put(t))
    val err = new RuntimeException("completion handler failed")
    i.crl.save(transactionId, trsValidTxd, () => throw err)
    // Queued behind the save: answered before the death or failed by it, never left waiting
    val pendingRead = i.crl.readStoreState(storeId)

    assert(Await.result(i.crl.failed, 10.seconds) eq err)
    Await.ready(pendingRead, 10.seconds)
    assertFailsFast(i.crl)
    assert(died.poll(10, TimeUnit.SECONDS) eq err)
    assert(died.poll(100, TimeUnit.MILLISECONDS) == null, "the death was reported twice")

    assertShutdownReturns(i.crl)
    // The entry was written before its handler threw
    val i2 = SimpleCRL(tdir, 3, 1024 * 1024)
    assert(i2.trsList.map(_.storeId) == List(storeId))
    i2.crl.shutdown()
  }

  test("A CRL whose own thread died fails every call, including those queued behind the fatal one") {
    val i = SimpleCRL(tdir, 3, 8192)
    val tooBig = ObjectUpdate(ou1.objectId, DataBuffer(new Array[Byte](8192)))
    i.crl.save(transactionId, trsValidTxd.copy(objectUpdates = List(tooBig)), () => ())
    val queuedBehind = i.crl.storesWithEntries()

    assert(Await.result(i.crl.failed, 10.seconds).isInstanceOf[IllegalArgumentException])
    val r = Await.ready(queuedBehind, 10.seconds).value.get
    assert(r.failed.toOption.exists(_.isInstanceOf[CrlFailed]))
    assertFailsFast(i.crl)
    assertShutdownReturns(i.crl)
  }

  test("An entry larger than a stream kills the CRL: the backstop admission must keep it from (CRL-W2)") {
    val i = SimpleCRL(tdir, 3, 4096 * 4)
    val tooBig = ObjectUpdate(ou1.objectId, DataBuffer(new Array[Byte](4096 * 5)))
    i.crl.save(transactionId, trsValidTxd.copy(objectUpdates = List(tooBig)), () => ())
    Await.result(i.crl.failed, 5.seconds)
    assertShutdownReturns(i.crl)
  }

  test("A SimpleCRL takes transactions of up to half a stream less the entry overhead reserve (CRL-W2)") {
    val i = SimpleCRL(tdir, 3, 1024 * 1024)
    assert(i.crl.maxTransactionBytes == 1024 * 1024 / 2 - SimpleCRL.EntryOverheadReserve)
    i.crl.shutdown()
  }

  test("A live CRL has not failed") {
    val i = SimpleCRL(tdir, 3, 1024 * 1024)
    saveAndWait(i.crl, transactionId, trsValidTxd)
    assert(!i.crl.failed.isCompleted)
    i.crl.shutdown()
  }

  test("A new CRL's stream files are fully allocated, and an interrupted creation starts over") {
    val size = 1024 * 1024L
    SimpleCRL(tdir, 3, size).crl.shutdown()
    val files = (0 until 3).map(n => tdir / s"$n.crl")
    files.foreach: f =>
      assert(os.size(f) == size)
      assert(FilePreallocationSuite.allocatedBytes(f) >= size, s"$f is sparse")
    assert(os.list(tdir).forall(!_.last.endsWith(".tmp")))

    // As a crash between the renames leaves it: one file still under its temporary name
    os.move(files(1), tdir / "1.crl.tmp")
    val i = SimpleCRL(tdir, 3, size)
    assert(i.trsList.isEmpty)
    assert(files.forall(os.exists(_)) && !os.exists(tdir / "1.crl.tmp"))
    saveAndWait(i.crl, transactionId, trsValidTxd)
    i.crl.shutdown()
    val i2 = SimpleCRL(tdir, 3, size)
    assert(i2.trsList.map(_.storeId) == List(storeId))
    i2.crl.shutdown()
  }

  test("A reopened CRL reports what it holds for each store, as admission counts it") {
    val i = SimpleCRL(tdir, 3, 1024 * 1024)
    assert(i.crl.recoveredBytesByStore.isEmpty)
    saveAndWait(i.crl, transactionId, trsValidTxd)
    saveAndWait(i.crl, transactionId2, trsValidTxd2)
    val q = new LinkedBlockingQueue[String]()
    i.crl.saveAppliedStub(storeId3, transactionId3, stubStart, () => q.put(""))
    q.take()
    i.crl.shutdown()

    val i2 = SimpleCRL(tdir, 3, 1024 * 1024)
    assert(i2.crl.recoveredBytesByStore == Map(
      storeId -> CrlAdmission.recoveredBytes(trsValidTxd),
      trsValidTxd2.storeId -> CrlAdmission.recoveredBytes(trsValidTxd2),
      storeId3 -> CrlAdmission.AppliedStubBytes))
    i2.crl.shutdown()
  }

  test("A dead CRL thread is reported") {
    val died = new LinkedBlockingQueue[Throwable]()
    val i = SimpleCRL(tdir, 3, 8192, t => died.put(t))

    // An entry larger than a whole stream cannot be written
    val tooBig = ObjectUpdate(ou1.objectId, DataBuffer(new Array[Byte](8192)))
    i.crl.save(transactionId, trsValidTxd.copy(objectUpdates = List(tooBig)), () => ())

    assert(died.poll(10, TimeUnit.SECONDS).isInstanceOf[IllegalArgumentException])
    assertShutdownReturns(i.crl)
  }

  test("A dead CRL writer thread is reported") {
    val died = new LinkedBlockingQueue[Throwable]()
    val i = SimpleCRL(tdir, 3, 1024 * 1024, t => died.put(t))
    val err = new RuntimeException("completion handler failed")

    // Completion handlers run on the writer thread
    i.crl.save(transactionId, trsValidTxd, () => throw err)

    assert(died.poll(10, TimeUnit.SECONDS) eq err)
    assertShutdownReturns(i.crl)
  }

  /** A CRL whose thread has died still completes its shutdown, so its device can be loaded again and its host can
   *  stop */
  private def assertShutdownReturns(crl: CrashRecoveryLog): Unit =
    val done = new CountDownLatch(1)
    val t = new Thread(() => { crl.shutdown(); done.countDown() })
    t.setDaemon(true)
    t.start()
    assert(done.await(10, TimeUnit.SECONDS), "the shutdown of a CRL whose thread died did not return")
    // Idempotent: a second shutdown returns too
    crl.shutdown()

  test("Recovery preserves latest paxos state after multiple saves") {
    val i = SimpleCRL(tdir, 3, 1024 * 1024)

    saveAndWait(i.crl, transactionId, trsUndetermined)
    saveAndWait(i.crl, transactionId, trsPromised)
    saveAndWait(i.crl, transactionId, trsValidTxd)

    i.crl.shutdown()

    val i2 = SimpleCRL(tdir, 3, 1024 * 1024)

    assert(i2.trsList.length == 1)
    assert(i2.trsList.head.disposition == TransactionDisposition.VoteCommit)
    assert(i2.trsList.head.paxosAcceptorState.promised == Some(promise))
    assert(i2.trsList.head.paxosAcceptorState.accepted == Some((accept, true)))

    i2.crl.shutdown()
  }

  test("Recovery after stream recycling re-writes tx data from recycled stream") {
    val queue = new LinkedBlockingQueue[String]()
    def completionHandler(): Unit = queue.put("")

    val i = SimpleCRL(tdir, 3, 4096 * 3)

    i.crl.save(transactionId, trsValidTxd, completionHandler)
    queue.take()
    assert(i.crl.currentStreamNumber == 0)

    // Fill all three streams: 3 entries per stream (each ~4k, max=12k), 4th triggers switch
    // Need 9 more saves to cycle through streams 0->1->2 and back to 0
    for _ <- 1 to 9 do
      i.crl.save(transactionId, trsValidTxd, completionHandler)
      queue.take()

    assert(i.crl.currentStreamNumber == 0)

    // Simulate a crash (the CRL stops with no further write), then recover
    i.crl.shutdown()
    val i2 = SimpleCRL(tdir, 3, 4096 * 3)

    assert(i2.trsList.length == 1)
    assert(i2.trsList.head.storeId == storeId)
    assert(i2.trsList.head.serializedTxd == trsValidTxd.serializedTxd)
    assert(i2.trsList.head.objectUpdates == trsValidTxd.objectUpdates)
    assert(i2.trsList.head.paxosAcceptorState == pax)

    i2.crl.shutdown()
  }

  test("oldestEntryNeeded advances correctly as tail transactions are deleted") {
    val queue = new LinkedBlockingQueue[String]()
    def completionHandler(): Unit = queue.put("")

    val i = SimpleCRL(tdir, 3, 1024 * 1024)

    i.crl.save(transactionId, trsValidTxd, completionHandler)
    queue.take()
    i.crl.save(transactionId2, trsValidTxd2, completionHandler)
    queue.take()
    i.crl.save(transactionId3, trsValidTxd3, completionHandler)
    queue.take()

    settle(i.crl)
    assert(i.crl.oldestEntryNeeded == 0)

    // Delete oldest (tx1), flush with a save
    i.crl.deleteTransaction(storeId, transactionId)
    i.crl.save(transactionId3, trsValidTxd3, completionHandler)
    queue.take()
    settle(i.crl)
    assert(i.crl.oldestEntryNeeded == 1)

    // Delete new oldest (tx2), flush
    i.crl.deleteTransaction(storeId2, transactionId2)
    i.crl.save(transactionId3, trsValidTxd3, completionHandler)
    queue.take()
    settle(i.crl)
    assert(i.crl.oldestEntryNeeded == 2)

    i.crl.shutdown()

    val i2 = SimpleCRL(tdir, 3, 1024 * 1024)

    assert(i2.trsList.length == 1)
    assert(i2.trsList.head.storeId == storeId)

    i2.crl.shutdown()
  }

  test("Delete newest transaction (queue head removal)") {
    val queue = new LinkedBlockingQueue[String]()
    def completionHandler(): Unit = queue.put("")

    val i = SimpleCRL(tdir, 3, 1024 * 1024)

    i.crl.save(transactionId, trsValidTxd, completionHandler)
    queue.take()
    i.crl.save(transactionId2, trsValidTxd2, completionHandler)
    queue.take()

    // Delete the newest (head of queue)
    i.crl.deleteTransaction(storeId2, transactionId2)
    i.crl.save(transactionId, trsValidTxd, completionHandler)
    queue.take()

    i.crl.shutdown()

    val i2 = SimpleCRL(tdir, 3, 1024 * 1024)

    assert(i2.trsList.length == 1)
    assert(i2.trsList.head.storeId == storeId)
    assert(i2.trsList.head.serializedTxd == trsValidTxd.serializedTxd)

    i2.crl.shutdown()
  }

  test("Delete middle transaction from queue of three") {
    val queue = new LinkedBlockingQueue[String]()
    def completionHandler(): Unit = queue.put("")

    val i = SimpleCRL(tdir, 3, 1024 * 1024)

    i.crl.save(transactionId, trsValidTxd, completionHandler)
    queue.take()
    i.crl.save(transactionId2, trsValidTxd2, completionHandler)
    queue.take()
    i.crl.save(transactionId3, trsValidTxd3, completionHandler)
    queue.take()

    settle(i.crl)
    assert(i.crl.oldestEntryNeeded == 0)

    // Delete the middle node (tx2)
    i.crl.deleteTransaction(storeId2, transactionId2)
    i.crl.save(transactionId3, trsValidTxd3, completionHandler)
    queue.take()

    // tx1 is still oldest, so oldestEntryNeeded should remain 0
    settle(i.crl)
    assert(i.crl.oldestEntryNeeded == 0)

    i.crl.shutdown()

    val i2 = SimpleCRL(tdir, 3, 1024 * 1024)

    assert(i2.trsList.length == 2)
    assert(i2.trsList.exists(_.storeId == storeId))
    assert(!i2.trsList.exists(_.storeId == storeId2))

    i2.crl.shutdown()
  }

  // ===== Tier 2: Important Functional Coverage =====

  test("Multiple independent transactions across different stores") {
    val queue = new LinkedBlockingQueue[String]()
    def completionHandler(): Unit = queue.put("")

    val i = SimpleCRL(tdir, 3, 1024 * 1024)

    // Save 4 transactions across 3 stores
    i.crl.save(transactionId, trsValidTxd, completionHandler)
    queue.take()
    i.crl.save(transactionId2, trsValidTxd2, completionHandler)
    queue.take()
    i.crl.save(transactionId3, trsValidTxd3, completionHandler)
    queue.take()
    i.crl.save(transactionId4, trsValidTxd4, completionHandler)
    queue.take()

    // Delete tx2, update tx1 with different paxos state
    i.crl.deleteTransaction(storeId2, transactionId2)
    i.crl.save(transactionId, trsPromised, completionHandler)
    queue.take()

    i.crl.shutdown()

    val i2 = SimpleCRL(tdir, 3, 1024 * 1024)

    assert(i2.trsList.length == 3)
    assert(!i2.trsList.exists(_.storeId == storeId2))

    // tx1 should have updated paxos state (promised only, no accepted)
    val tx1Recovered = i2.trsList.filter(t => t.storeId == storeId)
    assert(tx1Recovered.length == 2) // tx1 and tx3 are both on storeId
    assert(tx1Recovered.exists(_.paxosAcceptorState == paxPromiseOnly)) // tx1 updated
    assert(tx1Recovered.exists(_.paxosAcceptorState == pax)) // tx3 unchanged

    // tx4 on storeId3
    assert(i2.trsList.exists(_.storeId == storeId3))

    i2.crl.shutdown()
  }

  test("getFullRecoveryState filters correctly by storeId") {
    val i = SimpleCRL(tdir, 3, 1024 * 1024)

    saveAndWait(i.crl, transactionId, trsValidTxd)   // storeId
    saveAndWait(i.crl, transactionId2, trsValidTxd2) // storeId2
    saveAndWait(i.crl, transactionId3, trsValidTxd3) // storeId

    assert(i.crl.getFullRecoveryState(storeId).length == 2)
    assert(i.crl.getFullRecoveryState(storeId2).length == 1)
    assert(i.crl.getFullRecoveryState(storeId3).isEmpty)

    i.crl.shutdown()
  }

  test("Copy-forward during recycling preserves all transactions") {
    val queue = new LinkedBlockingQueue[String]()
    def completionHandler(): Unit = queue.put("")

    val i = SimpleCRL(tdir, 3, 4096 * 3)

    // Save two transactions — their dynamic data goes to stream 0
    i.crl.save(transactionId, trsValidTxd, completionHandler)
    queue.take()
    i.crl.save(transactionId2, trsValidTxd2, completionHandler)
    queue.take()
    assert(i.crl.currentStreamNumber == 0)

    // Fill streams to cycle back to 0. Need to advance through streams 0->1->2->0
    // Stream 0 already has 2 entries. One more fits, then switch.
    i.crl.save(transactionId, trsValidTxd, completionHandler)
    queue.take()
    assert(i.crl.currentStreamNumber == 0)

    // 4th entry on stream 0 doesn't fit, switches to stream 1
    i.crl.save(transactionId, trsValidTxd, completionHandler)
    queue.take()
    assert(i.crl.currentStreamNumber == 1)

    // Fill stream 1 (3 entries)
    for _ <- 1 to 2 do
      i.crl.save(transactionId, trsValidTxd, completionHandler)
      queue.take()
    assert(i.crl.currentStreamNumber == 1)

    i.crl.save(transactionId, trsValidTxd, completionHandler)
    queue.take()
    assert(i.crl.currentStreamNumber == 2)

    // Entering stream 2 evacuated stream 0 first: both transactions' pieces, in an entry of their own at its head
    val evacuation = headerAt(tdir / "2.crl", 0)
    assert(evacuation.numTransactions == 2)
    assert(evacuation.numDeletedTransactions == 0)
    assert(evacuation.dynamicDataSize ==
      trsValidTxd.serializedTxd.size + trsValidTxd.objectUpdates.map(_.data.size).sum +
      trsValidTxd2.serializedTxd.size + trsValidTxd2.objectUpdates.map(_.data.size).sum)

    // Wrap to stream 0, whose rotation evacuates stream 1: it holds neither
    while i.crl.currentStreamNumber != 0 do
      i.crl.save(transactionId, trsValidTxd, completionHandler)
      queue.take()

    i.crl.shutdown()

    val i2 = SimpleCRL(tdir, 3, 4096 * 3)

    assert(i2.trsList.length == 2)
    val byStore = i2.trsList.groupBy(_.storeId)
    assert(byStore(storeId).head.serializedTxd == trsValidTxd.serializedTxd)
    assert(byStore(storeId).head.objectUpdates == trsValidTxd.objectUpdates)
    assert(byStore(storeId2).head.serializedTxd == trsValidTxd2.serializedTxd)
    assert(byStore(storeId2).head.objectUpdates == trsValidTxd2.objectUpdates)

    i2.crl.shutdown()
  }

  test("dropTransactionObjectData then stream recycling preserves txd only") {
    val queue = new LinkedBlockingQueue[String]()
    def completionHandler(): Unit = queue.put("")

    val i = SimpleCRL(tdir, 3, 4096 * 3)

    i.crl.save(transactionId, trsValidTxd, completionHandler)
    queue.take()
    assert(i.crl.currentStreamNumber == 0)

    // Drop object data (simulates post-commit)
    i.crl.dropTransactionObjectData(storeId, transactionId)

    // Re-save to record the drop
    i.crl.save(transactionId, trsValidTxd, completionHandler)
    queue.take()

    // Fill streams to cycle back to 0, forcing re-write of txd (but not object updates)
    for _ <- 1 to 8 do
      i.crl.save(transactionId, trsValidTxd, completionHandler)
      queue.take()

    assert(i.crl.currentStreamNumber == 0)

    i.crl.shutdown()

    val i2 = SimpleCRL(tdir, 3, 4096 * 3)

    assert(i2.trsList.length == 1)
    assert(i2.trsList.head.serializedTxd == trsValidTxd.serializedTxd)
    assert(i2.trsList.head.objectUpdates == List())

    i2.crl.shutdown()
  }

  test("Delete and re-save same transaction ID") {
    val queue = new LinkedBlockingQueue[String]()
    def completionHandler(): Unit = queue.put("")

    val i = SimpleCRL(tdir, 3, 1024 * 1024)

    // Save tx1, then delete it
    i.crl.save(transactionId, trsValidTxd, completionHandler)
    queue.take()
    i.crl.deleteTransaction(storeId, transactionId)
    i.crl.save(transactionId2, trsValidTxd2, completionHandler)
    queue.take()

    // Re-save tx1 with the same TxId
    i.crl.save(transactionId, trsValidTxd, completionHandler)
    queue.take()

    i.crl.shutdown()

    val i2 = SimpleCRL(tdir, 3, 1024 * 1024)

    assert(i2.trsList.length == 2)
    assert(i2.trsList.exists(_.storeId == storeId))
    assert(i2.trsList.exists(_.storeId == storeId2))

    i2.crl.shutdown()
  }

  test("Re-saved transaction survives recovery when its earlier delete shares an entry with a live transaction") {
    val queue = new LinkedBlockingQueue[String]()
    def completionHandler(): Unit = queue.put("")
    val gate = new CountDownLatch(1)

    val i = SimpleCRL(tdir, 3, 1024 * 1024)

    // Completion handlers run on the writer thread before the IO thread sees the write finish.
    // Holding the writer inside tx1's handler guarantees that the delete of tx1 and the save of
    // tx2 are batched into the same log entry, and the re-save of tx1 lands in a later one.
    i.crl.save(transactionId, trsValidTxd, () => { queue.put(""); gate.await() })
    queue.take()
    i.crl.deleteTransaction(storeId, transactionId)
    i.crl.save(transactionId2, trsValidTxd2, completionHandler)
    gate.countDown()
    queue.take()

    i.crl.save(transactionId, trsValidTxd, completionHandler)
    queue.take()

    i.crl.shutdown()

    val i2 = SimpleCRL(tdir, 3, 1024 * 1024)

    assert(i2.trsList.length == 2)
    assert(i2.trsList.exists(_.storeId == storeId))
    assert(i2.trsList.exists(_.storeId == storeId2))

    i2.crl.shutdown()
  }

  test("Save after a delete in the same unwritten entry is skipped but reports success") {
    val queue = new LinkedBlockingQueue[String]()
    val gate = new CountDownLatch(1)

    val i = SimpleCRL(tdir, 3, 1024 * 1024)

    // Hold the writer so that everything below is batched into a single log entry
    i.crl.save(transactionId2, trsValidTxd2, () => { queue.put("first"); gate.await() })
    queue.take()
    i.crl.save(transactionId, trsValidTxd, () => queue.put("save"))
    i.crl.deleteTransaction(storeId, transactionId)
    i.crl.save(transactionId, trsValidTxd, () => queue.put("late save"))
    gate.countDown()

    val completed = Set(queue.poll(5, TimeUnit.SECONDS), queue.poll(5, TimeUnit.SECONDS))
    assert(completed == Set("save", "late save"))

    // The skipped save leaves no live record behind (F4): nothing is reported for the store, and once the only other
    // transaction is deleted no entry is needed any more
    assert(i.crl.getFullRecoveryState(storeId).isEmpty)
    i.crl.deleteTransaction(storeId2, transactionId2)
    saveAndWait(i.crl, transactionId3, trsValidTxd4)
    settle(i.crl)
    assert(i.crl.oldestEntryNeeded == i.crl.currentEntrySerialNumber - 1)

    i.crl.shutdown()

    val i2 = SimpleCRL(tdir, 3, 1024 * 1024)
    assert(i2.trsList.map(_.storeId) == List(storeId3))

    i2.crl.shutdown()
  }

  test("A save skipped after a delete in the same entry does not pin the log across a full wrap") {
    val queue = new LinkedBlockingQueue[String]()
    val gate = new CountDownLatch(1)

    // Three entries per stream
    val i = SimpleCRL(tdir, 3, 4096 * 3)

    // Hold the writer so that the save, delete and late save are batched into a single log entry
    i.crl.save(transactionId2, trsValidTxd2, () => { queue.put("first"); gate.await() })
    queue.take()
    i.crl.save(transactionId, trsValidTxd, () => queue.put("save"))
    i.crl.deleteTransaction(storeId, transactionId)
    i.crl.save(transactionId, trsValidTxd, () => queue.put("late save"))
    gate.countDown()
    queue.take()
    queue.take()

    i.crl.deleteTransaction(storeId2, transactionId2)

    // Wrap the ring several times with another transaction's saves
    for _ <- 0 until 14 do
      saveAndWait(i.crl, transactionId3, trsValidTxd4)

    assert(i.crl.getFullRecoveryState(storeId).isEmpty)
    i.crl.shutdown()

    val i2 = SimpleCRL(tdir, 3, 4096 * 3)
    assert(i2.trsList.map(_.storeId) == List(storeId3))
    i2.crl.shutdown()
  }

  // ===== Tier 3: Edge Cases and Corruption =====

  test("Corrupted entry detected — recovery stops at last valid entry") {
    val queue = new LinkedBlockingQueue[String]()
    def completionHandler(): Unit = queue.put("")

    val i = SimpleCRL(tdir, 3, 1024 * 1024)

    i.crl.save(transactionId, trsValidTxd, completionHandler)
    queue.take()
    i.crl.save(transactionId2, trsValidTxd2, completionHandler)
    queue.take()

    i.crl.shutdown()

    // Corrupt the trailing UUID of the second entry in stream 0
    val streamFile = tdir / "0.crl"
    val reader = StreamReader(StreamId(0), streamFile, recordedId(tdir))
    val Some(entry0Header) = reader.readEntry(0): @unchecked
    val entry1Start = entry0Header.entrySize
    val Some(entry1Header) = reader.readEntry(entry1Start): @unchecked
    val corruptOffset = entry1Start + entry1Header.trailingUUIDOffset

    os.write.over(streamFile, Array.fill(16)(0xFF.toByte), offset = corruptOffset, truncate = false)

    // Recovery should only find the first (uncorrupted) entry
    val i2 = SimpleCRL(tdir, 3, 1024 * 1024)

    assert(i2.trsList.length == 1)
    assert(i2.trsList.head.storeId == storeId)
    assert(i2.trsList.head.serializedTxd == trsValidTxd.serializedTxd)

    i2.crl.shutdown()
  }

  test("Recovery from fresh empty stream files") {
    val i = SimpleCRL(tdir, 3, 1024 * 1024)
    i.crl.shutdown()

    val i2 = SimpleCRL(tdir, 3, 1024 * 1024)
    assert(i2.trsList.isEmpty)
    i2.crl.shutdown()
  }

  test("readStoreState leaves a store's transactions live while other stores' entries move the log on") {
    val queue = new LinkedBlockingQueue[String]()
    def completionHandler(): Unit = queue.put("")

    val i = SimpleCRL(tdir, 3, 1024 * 1024)

    i.crl.save(transactionId, trsValidTxd, completionHandler)
    queue.take()
    i.crl.save(transactionId2, trsValidTxd2, completionHandler)
    queue.take()

    val read = Await.result(i.crl.readStoreState(storeId), Duration(5000, MILLISECONDS))
    assert(read.transactions.map(_.storeId) == List(storeId))
    assert(i.crl.getFullRecoveryState(storeId).length == 1)

    // A new transaction of another store, saved twice: the last entry needs nothing older than the first of these
    i.crl.save(transactionId3, trsValidTxd2, completionHandler)
    queue.take()
    i.crl.save(transactionId3, trsValidTxd2, completionHandler)
    queue.take()

    i.crl.shutdown()

    val i2 = SimpleCRL(tdir, 3, 1024 * 1024)
    assert(i2.trsList.count(_.storeId == storeId) == 1)
    i2.crl.shutdown()
  }

  test("Delete in same log entry as save") {
    val queue = new LinkedBlockingQueue[String]()

    val streamWriter = newWriter(4096 * 1000, 1)
    val stream = new Stream(stream0, crlId, streamWriter, UUID.randomUUID(), 0)

    val le = LogEntry(new Image(new LivePieces))
    le.addTx(txid, TxSave(TxForm(trs, keepUpdates = true), writesPieces = true), () => ())
    le.addTx(txid2, TxSave(TxForm(trs2, keepUpdates = true), writesPieces = true), () => ())
    le.deleteTx(txid)

    stream.writeEntry(le, 0, StreamLocation.Null, () => queue.put(""))
    queue.take()
    streamWriter.shutdown()

    val r = Recovery.recover(streams(1), crlId, Vector(stream0))

    assert(r.trsList.length == 1)
    assert(r.trsList.head.storeId == storeId2)
  }

  test("Rapid-fire saves all recovered") {
    val queue = new LinkedBlockingQueue[String]()

    val i = SimpleCRL(tdir, 3, 1024 * 1024)

    i.crl.save(transactionId, trsValidTxd, () => queue.put("a"))
    i.crl.save(transactionId2, trsValidTxd2, () => queue.put("b"))
    i.crl.save(transactionId3, trsValidTxd3, () => queue.put("c"))

    queue.take()
    queue.take()
    queue.take()

    i.crl.shutdown()

    val i2 = SimpleCRL(tdir, 3, 1024 * 1024)

    assert(i2.trsList.length == 3)
    assert(i2.trsList.exists(_.storeId == storeId))
    assert(i2.trsList.exists(_.storeId == storeId2))

    i2.crl.shutdown()
  }

  test("Recovery spanning many entries with interleaved saves and deletes") {
    val queue = new LinkedBlockingQueue[String]()
    def completionHandler(): Unit = queue.put("")

    val i = SimpleCRL(tdir, 3, 1024 * 1024)

    i.crl.save(transactionId, trsValidTxd, completionHandler)
    queue.take()
    i.crl.save(transactionId2, trsValidTxd2, completionHandler)
    queue.take()
    i.crl.save(transactionId3, trsValidTxd3, completionHandler)
    queue.take()

    // Delete tx1, flush
    i.crl.deleteTransaction(storeId, transactionId)
    i.crl.save(transactionId2, trsValidTxd2, completionHandler)
    queue.take()

    // Delete tx3, flush
    i.crl.deleteTransaction(storeId, transactionId3)
    i.crl.save(transactionId2, trsValidTxd2, completionHandler)
    queue.take()

    // Add tx4
    i.crl.save(transactionId4, trsValidTxd4, completionHandler)
    queue.take()

    i.crl.shutdown()

    val i2 = SimpleCRL(tdir, 3, 1024 * 1024)

    assert(i2.trsList.length == 2)
    assert(i2.trsList.exists(_.storeId == storeId2))
    assert(i2.trsList.exists(_.storeId == storeId3))
    assert(!i2.trsList.exists(t => t.storeId == storeId))

    i2.crl.shutdown()
  }

  private def mkTx(n: Long, numUpdates: Int): (TransactionId, TransactionRecoveryState) =
    val tid = TransactionId(new UUID(7, n))
    val d = TransactionDescription(tid, timestamp, op1, 1.toByte,
      List(DataUpdate(op1, ObjectRevision(tid), DataUpdateOperation.Overwrite)),
      List(), None, List(), List(), ida, Map(poolId -> ida))
    val ous = (0 until numUpdates).map(i => ObjectUpdate(ObjectId(new UUID(9, i)), DataBuffer(Array[Byte](1, 2, 3)))).toList
    (tid, TransactionRecoveryState(storeId, d.serialize(), ous, disp, status, pax))

  // ===== Entry checksums (crash-recovery-log F5, F19) =====

  private def headerAt(file: os.Path, offset: Int): LogEntry.EntryHeader =
    LogEntry.loadHeader(ByteBuffer.wrap(os.read.bytes(file), offset, LogEntry.StaticEntryHeaderSize).slice())

  private def flip(file: os.Path, pos: Long): Unit =
    val bytes = os.read.bytes(file)
    bytes(pos.toInt) = (bytes(pos.toInt) ^ 0x5a).toByte
    os.write.over(file, bytes)

  /** A in the entry at offset 0 and B (storeId2) in the entry at 4096 of stream 0, both live */
  private def twoEntries(dir: os.Path): Unit =
    val i = SimpleCRL(dir, 3, 1024 * 1024)
    saveAndWait(i.crl, transactionId, trsValidTxd)
    saveAndWait(i.crl, transactionId2, trsValidTxd2)
    i.crl.shutdown()
    assert(headerAt(dir / "0.crl", 4096).entrySerialNumber == headerAt(dir / "0.crl", 0).entrySerialNumber + 1)

  /** The bytes of a well-formed entry holding B, with serial number `serial`, as CRL `id` writes it under
    * `streamUUID` */
  private def entryImage(serial: Long, streamUUID: UUID, id: UUID): Array[Byte] =
    val le = LogEntry(new Image(new LivePieces))
    le.addTx(txid2, TxSave(TxForm(trs2, keepUpdates = true), writesPieces = true), () => ())
    val out = new java.io.ByteArrayOutputStream()
    le.createEntryBuffers(serial, StreamLocation.Null, 0, id, streamUUID, stream0).foreach: b =>
      val a = new Array[Byte](b.remaining())
      b.get(a)
      out.write(a)
    out.toByteArray

  test("Object data holding a well-formed foreign entry with a high serial number does not fail recovery") {
    val queue = new LinkedBlockingQueue[String]()
    val streamWriter = newWriter(4096 * 8, 1)
    val stream = new Stream(stream0, crlId, streamWriter, UUID.randomUUID(), 0)

    // A's object data places a forged entry, serial 2^62, at offset 8192 of the stream
    val forged = entryImage(1L << 62, UUID.randomUUID(), UUID.randomUUID())
    val dataStart = LogEntry.StaticEntryHeaderSize + txdata.size
    val payload = new Array[Byte](8192 - dataStart + forged.length)
    System.arraycopy(forged, 0, payload, 8192 - dataStart, forged.length)
    val a = TransactionRecoveryState(storeId, txdata,
      List(ObjectUpdate(ObjectId(new UUID(0, 9)), DataBuffer(payload))), disp, status, pax)
    val le = LogEntry(new Image(new LivePieces))
    le.addTx(txid, TxSave(TxForm(a, keepUpdates = true), writesPieces = true), () => queue.put(""))
    stream.writeEntry(le, 0, StreamLocation.Null, () => ())
    queue.take()

    // The stream's next cycle: its only entry covers the first page, leaving A's payload behind it
    stream.recycleStream()
    val le2 = LogEntry(new Image(new LivePieces))
    le2.addTx(txid2, TxSave(TxForm(trs2, keepUpdates = true), writesPieces = true), () => queue.put(""))
    stream.writeEntry(le2, 1, StreamLocation.Null, () => ())
    queue.take()
    val stopped = new CountDownLatch(1)
    streamWriter.shutdown(() => stopped.countDown())
    stopped.await()

    val r = Recovery.recover(streams(1), crlId, Vector(stream0))
    assert(r.trsList.map(_.storeId) == List(storeId2))
  }

  test("A header is an entry only if it bears the reader's CRL id") {
    val f = tdir / "image.crl"
    os.write(f, entryImage(5, UUID.randomUUID(), crlId))
    val own = StreamReader(stream0, f, crlId)
    val other = StreamReader(stream0, f, UUID.randomUUID())
    try
      assert(own.readHeader(0).map(_.entrySerialNumber) == Some(5L))
      assert(other.readHeader(0).isEmpty)
      assert(other.newerIntactEntry(0, 0).isEmpty)
    finally
      own.close()
      other.close()
  }

  test("A sizing file without a CRL id is unreadable: a sizing mismatch") {
    SimpleCRL(tdir, 3, 1024 * 1024).crl.shutdown()
    os.write.over(tdir / SimpleCRL.SizingFile, "num-streams: 3\nmax-size-in-bytes: 1048576\nring: 0,1,2\n")
    assertThrows[CrlSizingMismatch](SimpleCRL(tdir, 3, 1024 * 1024))
  }

  test("A flipped byte in a needed entry's txd, object data or static record fails the open with CrlCorrupt") {
    val h = LogEntry.StaticEntryHeaderSize
    val txdSize = trsValidTxd.serializedTxd.size
    // Offsets within the entry at 0, which holds only A: its txd comes first, then its object data, then its record,
    // whose bytes 49 to 52 are the promise number
    val positions = List("txd" -> (h + 1), "object data" -> (h + txdSize), "promise" -> -1)
    for (what, at) <- positions do
      val dir = tdir / what.replace(' ', '-')
      twoEntries(dir)
      val e0 = headerAt(dir / "0.crl", 0)
      val pos = if at >= 0 then at.toLong else h + e0.dynamicDataSize + 52
      flip(dir / "0.crl", pos)
      val r =
        try Left(SimpleCRL(dir, 3, 1024 * 1024))
        catch case t: Throwable => Right(t)
      r match
        case Left(opened) =>
          opened.crl.shutdown()
          fail(s"$what: the corrupted CRL opened")
        case Right(t) => assert(t.isInstanceOf[CrlCorrupt], s"$what: $t")
  }

  test("A torn newest entry is dropped and the state before it recovers") {
    twoEntries(tdir)
    // B's entry was being written when the host crashed: part of its body never reached the disk
    val f = tdir / "0.crl"
    val bytes = os.read.bytes(f)
    java.util.Arrays.fill(bytes, 4096 + LogEntry.StaticEntryHeaderSize, 4096 + LogEntry.StaticEntryHeaderSize + 10, 0.toByte)
    os.write.over(f, bytes)

    val i2 = SimpleCRL(tdir, 3, 1024 * 1024)
    assert(i2.trsList.map(_.storeId) == List(storeId))
    saveAndWait(i2.crl, transactionId2, trsValidTxd2)
    i2.crl.shutdown()
    val i3 = SimpleCRL(tdir, 3, 1024 * 1024)
    assert(i3.trsList.map(_.storeId).toSet == Set(storeId, storeId2))
    i3.crl.shutdown()
  }

  test("A corrupt entry followed by valid newer ones fails the open instead of rolling back to the entry before it") {
    val i = SimpleCRL(tdir, 3, 1024 * 1024)
    saveAndWait(i.crl, transactionId, trsValidTxd)
    saveAndWait(i.crl, transactionId2, trsValidTxd2)
    saveAndWait(i.crl, transactionId3, trsValidTxd4)
    i.crl.shutdown()
    // The stream UUID in the middle entry's header
    flip(tdir / "0.crl", 4096)
    assertThrows[CrlCorrupt](SimpleCRL(tdir, 3, 1024 * 1024))
  }

  test("A growth leftover beside missing or unlisted streams is a sizing mismatch, and nothing is removed") {
    val size = 4096L * 3
    SimpleCRL(tdir, 3, size).crl.shutdown()
    // The listed streams gone: removing 3.crl would leave none, and an empty CRL would be created under live stores
    (0 until 3).foreach(n => os.remove(tdir / s"$n.crl"))
    os.write(tdir / "3.crl", new Array[Byte](size.toInt))
    for mustExist <- List(true, false) do
      assertThrows[CrlSizingMismatch](SimpleCRL(tdir, 3, size, mustExist = mustExist))
      assert(os.list(tdir).filter(_.ext == "crl").map(_.last).toSet == Set("3.crl"))
    // One listed stream missing
    os.remove(tdir / "3.crl")
    os.remove(tdir / SimpleCRL.SizingFile)
    SimpleCRL(tdir, 3, size).crl.shutdown()
    os.remove(tdir / "1.crl")
    os.write(tdir / "3.crl.tmp", new Array[Byte](size.toInt))
    assertThrows[CrlSizingMismatch](SimpleCRL(tdir, 3, size))
    assert(os.exists(tdir / "3.crl.tmp"))
    // Every listed stream present, but another unlisted one beside the leftover
    os.write(tdir / "1.crl", new Array[Byte](size.toInt))
    os.write(tdir / "5.crl", new Array[Byte](size.toInt))
    assertThrows[CrlSizingMismatch](SimpleCRL(tdir, 3, size))
    assert(os.exists(tdir / "3.crl.tmp") && os.exists(tdir / "5.crl"))
  }

  test("Recovery's newer-entry check of the next stream follows the ring, not the next number") {
    // Ring 0,2,1: the stream after 0 is 2. A corrupt head of stream 2 with an intact entry after it must fail the
    // open. Checking stream 1, all zeros, would roll back silently to stream 0's entries
    val size = 4096L * 3
    val y = mkTx(2, 2)
    val i = SimpleCRL(tdir, 3, size, ringForTest = Some(Vector(0, 2, 1).map(StreamId(_))))
    saveAndWait(i.crl, transactionId, trsValidTxd)
    while i.crl.currentStreamNumber == 0 do
      saveAndWait(i.crl, y._1, y._2)
    saveAndWait(i.crl, transactionId3, trsValidTxd4)
    settle(i.crl)
    assert(i.crl.currentStreamNumber == 2)
    i.crl.shutdown()
    flip(tdir / "2.crl", 0)
    assertThrows[CrlCorrupt](SimpleCRL(tdir, 3, size))
  }

  test("A corrupt head of the active stream fails the open instead of recovering from the stream before it") {
    val size = 4096L * 3
    val y = mkTx(2, 2)
    val i = SimpleCRL(tdir, 3, size)
    saveAndWait(i.crl, transactionId, trsValidTxd)
    while i.crl.currentStreamNumber == 0 do
      saveAndWait(i.crl, y._1, y._2)
    // A second entry in stream 1 holding a transaction no earlier entry has
    saveAndWait(i.crl, transactionId3, trsValidTxd4)
    settle(i.crl)
    assert(i.crl.currentStreamNumber == 1)
    i.crl.shutdown()
    flip(tdir / "1.crl", 0)
    assertThrows[CrlCorrupt](SimpleCRL(tdir, 3, size))
  }

  test("Zero-filled stream files open as an empty CRL that writes from the start of stream 0") {
    val size = 4096L * 3
    // As a creation that recorded its sizing and crashed once its files were allocated
    os.write(tdir / SimpleCRL.SizingFile, SimpleCRL.Sizing.inOrder(3, size, UUID.randomUUID()).encode)
    for n <- 0 until 3 do
      os.write(tdir / s"$n.crl", new Array[Byte](size.toInt))
    val i = SimpleCRL(tdir, 3, size)
    assert(i.trsList.isEmpty)
    saveAndWait(i.crl, transactionId, trsValidTxd)
    settle(i.crl)
    assert(i.crl.currentStreamNumber == 0)
    i.crl.shutdown()
    val i2 = SimpleCRL(tdir, 3, size)
    assert(i2.trsList.map(_.storeId) == List(storeId))
    i2.crl.shutdown()
  }

  test("StreamWriter writes every buffer of a write with more buffers than IOV_MAX") {
    val sw = newWriter(1024 * 1024, 1)
    val buffers = (0 until 3000).map(i => ByteBuffer.wrap(Array[Byte]((i % 127).toByte, 1))).toArray
    val done = new CountDownLatch(1)
    sw.write(stream0, 10, buffers, () => done.countDown())
    assert(done.await(10, TimeUnit.SECONDS))
    val shut = new CountDownLatch(1)
    sw.shutdown(() => shut.countDown())
    assert(shut.await(10, TimeUnit.SECONDS))

    val bytes = os.read.bytes(tdir / "0.log")
    for i <- 0 until 3000 do
      assert(bytes(10 + 2 * i) == (i % 127).toByte, s"buffer $i")
      assert(bytes(10 + 2 * i + 1) == 1.toByte, s"buffer $i")
  }

  test("An entry holding more transactions than IOV_MAX buffers recovers, and saves after the reopen survive") {
    val count = 600
    val i1 = SimpleCRL(tdir, 3, 64L * 1024 * 1024)
    val done = new CountDownLatch(count)
    for n <- 1 to count do
      val t = mkTx(n, 2)
      i1.crl.save(t._1, t._2, () => done.countDown())
    assert(done.await(30, TimeUnit.SECONDS))
    i1.crl.shutdown()

    // The save acknowledged after the reopen must survive the next one
    val i2 = SimpleCRL(tdir, 3, 64L * 1024 * 1024)
    assert(i2.trsList.length == count)
    val late = mkTx(count + 1, 2)
    saveAndWait(i2.crl, late._1, late._2)
    i2.crl.shutdown()

    val i3 = SimpleCRL(tdir, 3, 64L * 1024 * 1024)
    assert(i3.trsList.length == count + 1)
    i3.crl.shutdown()
  }

  test("Saves acknowledged after a recovery that spanned two streams survive the next recoveries") {
    val size = 4096L * 3
    val x = mkTx(1, 2)
    val y = mkTx(2, 2)
    val z = mkTx(3, 2)

    val i1 = SimpleCRL(tdir, 3, size)
    saveAndWait(i1.crl, x._1, x._2)
    for _ <- 1 to 3 do saveAndWait(i1.crl, y._1, y._2)
    i1.crl.shutdown()

    val i2 = SimpleCRL(tdir, 3, size)
    assert(i2.trsList.length == 2)
    saveAndWait(i2.crl, z._1, z._2)
    i2.crl.shutdown()

    val i3 = SimpleCRL(tdir, 3, size)
    assert(i3.trsList.map(_.txd).toSet.size == 3)
    // Rotate into another stream before the next restart
    for _ <- 1 to 5 do saveAndWait(i3.crl, y._1, y._2)
    i3.crl.shutdown()

    val i4 = SimpleCRL(tdir, 3, size)
    assert(i4.trsList.length == 3)
    i4.crl.shutdown()
  }

  test("A rotation write torn at any sector leaves every earlier acknowledged save recoverable") {
    val size = 4096L * 4
    val files = (0 until 3).map(i => tdir / s"$i.crl")
    // X lives throughout with data spanning several sectors, so a rotation that copies it forward writes a
    // multi-sector entry. Y is re-saved to move the log on
    val x0 = mkTx(1, 1)
    val x = (x0._1, x0._2.copy(objectUpdates =
      List(ObjectUpdate(ObjectId(new UUID(9, 100)), DataBuffer(Array.tabulate[Byte](6000)(i => (i % 101).toByte))))))
    val y = mkTx(2, 2)

    // Each rotation: the stream it wrote into, and every file before and after its write
    var rotations: List[(Int, IndexedSeq[Array[Byte]], IndexedSeq[Array[Byte]])] = Nil
    val i1 = SimpleCRL(tdir, 3, size)
    saveAndWait(i1.crl, x._1, x._2)
    saveAndWait(i1.crl, y._1, y._2)
    for _ <- 1 to 20 do
      val before = files.map(os.read.bytes(_))
      val stream = i1.crl.currentStreamNumber
      saveAndWait(i1.crl, y._1, y._2)
      if i1.crl.currentStreamNumber != stream then
        rotations = (i1.crl.currentStreamNumber, before, files.map(os.read.bytes(_))) :: rotations
    i1.crl.shutdown()
    assert(rotations.size >= 4)

    def recoversXandY(image: IndexedSeq[Array[Byte]], what: String): Unit =
      files.zip(image).foreach((f, bytes) => os.write.over(f, bytes))
      val r = try SimpleCRL(tdir, 3, size) catch case t: Throwable => fail(s"$what: $t")
      val byTx = r.trsList.map(t => t.txd.transactionId -> t).toMap
      assert(byTx.keySet == Set(x._1, y._1), what)
      assert(byTx(x._1).objectUpdates == x._2.objectUpdates, what)
      assert(byTx(y._1).objectUpdates == y._2.objectUpdates, what)
      r.crl.shutdown()

    var largestRotation = 0
    var evacuatedFirst = false
    for (stream, before, after) <- rotations.reverse do
      val writes = tornRotationWrites(stream, before, after)
      evacuatedFirst ||= writes.size > 1
      writes.foreach: w =>
        largestRotation = largestRotation.max(w.sectors)
        w.images.foreach((what, image) => recoversXandY(image, what))
    // Some rotation evacuated X before writing the pending entry
    assert(largestRotation > 1)
    assert(evacuatedFirst)
  }

  /** One write of a rotation: the sectors it changed, and every image a crash during it can leave */
  private case class TornWrite(sectors: Int, images: List[(String, IndexedSeq[Array[Byte]])])

  /** The writes of the rotation into `stream`, which turned the files `before` into `after`. A rotation writes its
    * evacuation, then the pending entry once the evacuation is durable: each write is torn on its own, over the
    * stream as the writes before it left it. Sectors 0..n-1 of the new entry persisted and the rest not, for each n;
    * or every sector but the first (the old head intact) */
  private def tornRotationWrites(stream: Int, before: IndexedSeq[Array[Byte]],
                                 after: IndexedSeq[Array[Byte]]): List[TornWrite] =
    val sector = 4096
    val writes = currentCycle(after(stream)).map((o, h) => (o, h.entrySize.toInt))
    assert(writes.nonEmpty)
    writes.zipWithIndex.map: (extent, k) =>
      val (start, length) = extent
      val base = before(stream).clone()
      System.arraycopy(after(stream), 0, base, 0, start)
      val changed = (start / sector until (start + length) / sector).filter: s =>
        !java.util.Arrays.equals(base, s * sector, (s + 1) * sector, after(stream), s * sector, (s + 1) * sector)
      assert(changed.nonEmpty)
      def torn(newSectors: Set[Int]): IndexedSeq[Array[Byte]] =
        val bytes = base.clone()
        newSectors.foreach(s => System.arraycopy(after(stream), s * sector, bytes, s * sector, sector))
        before.updated(stream, bytes)
      val images = (0 until changed.size).toList.map(n =>
        s"rotation into $stream, write $k, first $n of ${changed.size} sectors" -> torn(changed.take(n).toSet)) :+
        (s"rotation into $stream, write $k, all but its first sector" -> torn(changed.drop(1).toSet))
      TornWrite(changed.size, images)

  test("An open of a CRL whose live set is more than a stream recovers it without writing") {
    val size = 4096L * 4
    def big(n: Int) =
      val t = mkTx(n, 1)
      (t._1, t._2.copy(objectUpdates = List(ObjectUpdate(ObjectId(new UUID(9, n)), DataBuffer(new Array[Byte](10000))))))
    val a = big(1)
    val b = big(2)
    // Without admission the two live transactions fill more than a stream. The open used to re-save both, which could
    // not fit, so the device could not open at all. It now only reads; copying them forward at the next rotation is
    // what cannot fit, Stream.writeEntry's fail-stop backstop, which admission keeps out of reach
    val i1 = SimpleCRL(tdir, 3, size)
    saveAndWait(i1.crl, a._1, a._2)
    saveAndWait(i1.crl, b._1, b._2)
    i1.crl.shutdown()

    val before = (0 until 3).map(n => os.read.bytes(tdir / s"$n.crl"))
    for _ <- 1 to 2 do
      val r = SimpleCRL(tdir, 3, size)
      assert(r.trsList.map(_.txd.transactionId).toSet == Set(a._1, b._1))
      r.crl.shutdown()
    (0 until 3).foreach(n => assert(java.util.Arrays.equals(os.read.bytes(tdir / s"$n.crl"), before(n)), s"stream $n"))
  }

  // ---- Applied stubs ----

  private def stubAndWait(crl: CrashRecoveryLog, sid: StoreId, tid: TransactionId): Unit =
    val q = new LinkedBlockingQueue[String]()
    crl.saveAppliedStub(sid, tid, stubStart, () => q.put(""))
    q.take()

  private def readState(crl: CrashRecoveryLog, sid: StoreId): StoreRecoveryState =
    Await.result(crl.readStoreState(sid), Duration(5000, MILLISECONDS))

  test("an applied stub replaces a transaction's state and is recovered as a stub, not a transaction") {
    val i = SimpleCRL(tdir, 3, 1024 * 1024)
    saveAndWait(i.crl, transactionId, trsValidTxd)
    stubAndWait(i.crl, storeId, transactionId)

    readState(i.crl, storeId) shouldBe StoreRecoveryState(Nil, Map(transactionId -> stubStart))
    i.crl.getFullRecoveryState(storeId) shouldBe Nil
    // A transaction deletion leaves the stub
    i.crl.deleteTransaction(storeId, transactionId)
    saveAndWait(i.crl, transactionId2, trsValidTxd2)
    i.crl.getAppliedStubs(storeId) shouldBe Map(transactionId -> stubStart)
    Await.result(i.crl.storesWithEntries(), Duration(5000, MILLISECONDS)) shouldBe Set(storeId, storeId2)
    i.crl.shutdown()

    val i2 = SimpleCRL(tdir, 3, 1024 * 1024)
    i2.trsList.map(_.storeId) shouldBe List(storeId2)
    i2.appliedStubs shouldBe Map(TxId(storeId, transactionId) -> stubStart)
    readState(i2.crl, storeId) shouldBe StoreRecoveryState(Nil, Map(transactionId -> stubStart))
    i2.crl.shutdown()
  }

  test("a transaction saved after its applied stub keeps the stub") {
    val i = SimpleCRL(tdir, 3, 1024 * 1024)
    stubAndWait(i.crl, storeId, transactionId)
    saveAndWait(i.crl, transactionId, trsValidTxd)
    i.crl.deleteTransaction(storeId, transactionId)
    saveAndWait(i.crl, transactionId2, trsValidTxd2)
    i.crl.shutdown()

    val i2 = SimpleCRL(tdir, 3, 1024 * 1024)
    readState(i2.crl, storeId) shouldBe StoreRecoveryState(Nil, Map(transactionId -> stubStart))
    i2.crl.shutdown()
  }

  /** Saves `transactionId2`'s state and holds the writer until the returned latch is counted down: requests queued
    * meanwhile are batched into a single log entry */
  private def holdWriter(crl: CrashRecoveryLog): CountDownLatch =
    val first = new LinkedBlockingQueue[String]()
    val gate = new CountDownLatch(1)
    crl.save(transactionId2, trsValidTxd2, () => { first.put(""); gate.await() })
    first.take()
    gate

  test("an applied stub written after its transaction's deletion in the same entry is kept") {
    val q = new LinkedBlockingQueue[String]()
    val i = SimpleCRL(tdir, 3, 1024 * 1024)
    saveAndWait(i.crl, transactionId, trsValidTxd)
    val gate = holdWriter(i.crl)
    i.crl.deleteTransaction(storeId, transactionId)
    i.crl.saveAppliedStub(storeId, transactionId, stubStart, () => q.put("stub"))
    gate.countDown()
    q.poll(5, TimeUnit.SECONDS) shouldBe "stub"
    i.crl.shutdown()

    val i2 = SimpleCRL(tdir, 3, 1024 * 1024)
    readState(i2.crl, storeId) shouldBe StoreRecoveryState(Nil, Map(transactionId -> stubStart))
    i2.crl.shutdown()
  }

  test("a transaction saved after its applied stub in the same entry is written beside it") {
    val q = new LinkedBlockingQueue[String]()
    val i = SimpleCRL(tdir, 3, 1024 * 1024)
    val gate = holdWriter(i.crl)
    // All in one entry: the stub's replacement of the state does not make the later save's deletion final
    i.crl.save(transactionId, trsUndetermined, () => q.put("first save"))
    i.crl.saveAppliedStub(storeId, transactionId, stubStart, () => q.put("stub"))
    i.crl.save(transactionId, trsValidTxd, () => q.put("save"))
    gate.countDown()
    Set(q.poll(5, TimeUnit.SECONDS), q.poll(5, TimeUnit.SECONDS), q.poll(5, TimeUnit.SECONDS)) shouldBe
      Set("first save", "stub", "save")
    readState(i.crl, storeId) shouldBe StoreRecoveryState(List(trsValidTxd), Map(transactionId -> stubStart))
    i.crl.shutdown()

    val i2 = SimpleCRL(tdir, 3, 1024 * 1024)
    readState(i2.crl, storeId) shouldBe StoreRecoveryState(List(trsValidTxd), Map(transactionId -> stubStart))
    i2.crl.shutdown()
  }

  test("a deletion after a stub and a later save in the same entry stays final") {
    val q = new LinkedBlockingQueue[String]()
    val i = SimpleCRL(tdir, 3, 1024 * 1024)
    val gate = holdWriter(i.crl)
    i.crl.save(transactionId, trsUndetermined, () => q.put("first save"))
    i.crl.saveAppliedStub(storeId, transactionId, stubStart, () => q.put("stub"))
    i.crl.save(transactionId, trsValidTxd, () => q.put("save"))
    i.crl.deleteTransaction(storeId, transactionId)
    i.crl.save(transactionId, trsValidTxd, () => q.put("late save"))
    gate.countDown()
    (1 to 4).map(_ => q.poll(5, TimeUnit.SECONDS)).toSet shouldBe Set("first save", "stub", "save", "late save")
    i.crl.shutdown()

    val i2 = SimpleCRL(tdir, 3, 1024 * 1024)
    readState(i2.crl, storeId) shouldBe StoreRecoveryState(Nil, Map(transactionId -> stubStart))
    i2.crl.shutdown()
  }

  test("an applied stub survives stream rotation") {
    val size = 4096L * 3
    val i = SimpleCRL(tdir, 3, size)
    stubAndWait(i.crl, storeId, transactionId)
    var rotations = 0
    var stream = i.crl.currentStreamNumber
    // Another store's transaction saved and deleted over and over: the ring recycles every stream several times
    for n <- 0 until 40 do
      val (tid, t) = mkTx(n, 1)
      saveAndWait(i.crl, tid, t.copy(storeId = storeId2))
      i.crl.deleteTransaction(storeId2, tid)
      if i.crl.currentStreamNumber != stream then
        rotations += 1
        stream = i.crl.currentStreamNumber
    saveAndWait(i.crl, transactionId2, trsValidTxd2)
    assert(rotations > 6)
    readState(i.crl, storeId) shouldBe StoreRecoveryState(Nil, Map(transactionId -> stubStart))
    i.crl.shutdown()

    val i2 = SimpleCRL(tdir, 3, size)
    i2.appliedStubs shouldBe Map(TxId(storeId, transactionId) -> stubStart)
    i2.crl.shutdown()
    // And again: the open's re-save carries it on
    val i3 = SimpleCRL(tdir, 3, size)
    i3.appliedStubs shouldBe Map(TxId(storeId, transactionId) -> stubStart)
    i3.crl.shutdown()
  }

  test("deleteAppliedStub removes it durably") {
    val i = SimpleCRL(tdir, 3, 1024 * 1024)
    saveAndWait(i.crl, transactionId, trsValidTxd)
    stubAndWait(i.crl, storeId, transactionId3)
    i.crl.deleteAppliedStub(storeId, transactionId3)
    // Requests are handled in order: a save completing after the deletion proves it durable
    saveAndWait(i.crl, transactionId2, trsValidTxd2)
    readState(i.crl, storeId).appliedStubs shouldBe Map()
    i.crl.shutdown()

    val i2 = SimpleCRL(tdir, 3, 1024 * 1024)
    i2.appliedStubs shouldBe Map()
    // Only the stub: the store's transaction stays
    readState(i2.crl, storeId).transactions shouldBe List(trsValidTxd)
    i2.crl.shutdown()
  }

  test("stubs travel with a store transfer and are dropped by dropStore") {
    val savePath = tdir / "crl_save_file.log"
    val i = SimpleCRL(tdir, 3, 1024 * 1024)
    saveAndWait(i.crl, transactionId, trsValidTxd)
    stubAndWait(i.crl, storeId, transactionId3)
    stubAndWait(i.crl, storeId, transactionId4)
    saveAndWait(i.crl, transactionId2, trsValidTxd2)

    // Extraction: read, saved to the file, then deleted from the CRL
    val extracted = readState(i.crl, storeId)
    extracted shouldBe StoreRecoveryState(List(trsValidTxd), Map(transactionId3 -> stubStart, transactionId4 -> stubStart))
    CrashRecoveryLog.saveStoreState(storeId, extracted, savePath)
    CrashRecoveryLog.loadValidStoreState(savePath, storeId) shouldBe Right(extracted)

    // The target's CRL loads it
    val targetDir = tdir / "target"
    os.makeDir.all(targetDir)
    val target = SimpleCRL(targetDir, 3, 1024 * 1024)
    Await.result(target.crl.loadStore(storeId, extracted), Duration(5000, MILLISECONDS))
    readState(target.crl, storeId) shouldBe extracted
    target.crl.shutdown()
    SimpleCRL(targetDir, 3, 1024 * 1024) match
      case reopened =>
        reopened.appliedStubs shouldBe Map(TxId(storeId, transactionId3) -> stubStart, TxId(storeId, transactionId4) -> stubStart)
        reopened.crl.shutdown()

    // A store with only stubs still has entries; dropStore takes them with its transactions
    i.crl.deleteTransaction(storeId, transactionId)
    Await.result(i.crl.storesWithEntries(), Duration(5000, MILLISECONDS)) shouldBe Set(storeId, storeId2)
    Await.result(i.crl.dropStore(storeId), Duration(5000, MILLISECONDS))
    readState(i.crl, storeId) shouldBe StoreRecoveryState(Nil, Map())
    i.crl.shutdown()
    val i2 = SimpleCRL(tdir, 3, 1024 * 1024)
    i2.appliedStubs shouldBe Map()
    readState(i2.crl, storeId2).transactions.size shouldBe 1
    i2.crl.shutdown()
  }

  // ---- An open writes nothing (crash-recovery-log F16) ----

  private def streamBytes(dir: os.Path, n: Int): IndexedSeq[Array[Byte]] =
    (0 until n).map(i => os.read.bytes(dir / s"$i.crl"))

  test("An open writes nothing: repeated reopens leave every stream file unchanged and recover the same state") {
    val size = 1024L * 1024
    val x = mkTx(1, 2)
    val y = mkTx(2, 2)
    val i1 = SimpleCRL(tdir, 3, size)
    // X's data is written in its first entry and named by its later saves; Y's update data comes after a data-less
    // first save (a driver's prepare), in a later entry than its txd; a stub stands beside them
    saveAndWait(i1.crl, x._1, x._2)
    saveAndWait(i1.crl, y._1, y._2.copy(objectUpdates = Nil))
    saveAndWait(i1.crl, x._1, x._2.copy(disposition = TransactionDisposition.VoteAbort))
    saveAndWait(i1.crl, y._1, y._2)
    stubAndWait(i1.crl, storeId2, transactionId3)
    settle(i1.crl)
    val serial = i1.crl.currentEntrySerialNumber
    val stream = i1.crl.currentStreamNumber
    i1.crl.shutdown()

    val before = streamBytes(tdir, 3)
    for n <- 1 to 5 do
      val r = SimpleCRL(tdir, 3, size)
      assert(r.crl.currentEntrySerialNumber == serial, s"reopen $n")
      assert(r.crl.currentStreamNumber == stream, s"reopen $n")
      val byTx = r.trsList.map(t => t.txd.transactionId -> t).toMap
      assert(byTx.keySet == Set(x._1, y._1), s"reopen $n")
      assert(byTx(x._1).disposition == TransactionDisposition.VoteAbort, s"reopen $n")
      assert(byTx(x._1).objectUpdates == x._2.objectUpdates, s"reopen $n")
      assert(byTx(y._1).objectUpdates == y._2.objectUpdates, s"reopen $n")
      assert(r.appliedStubs == Map(TxId(storeId2, transactionId3) -> stubStart), s"reopen $n")
      r.crl.shutdown()
      val after = streamBytes(tdir, 3)
      (0 until 3).foreach(i => assert(java.util.Arrays.equals(after(i), before(i)), s"reopen $n wrote stream $i"))
  }

  /** X with data spanning sectors, and a stub, recovered by each reopen; Y re-saved to drive the log through
    * rotations, which must copy X and the stub forward from where the recovery found them */
  private def rotateRecovered(reopenEvery: Int): Unit =
    val size = 4096L * 4
    val x0 = mkTx(1, 1)
    val x = (x0._1, x0._2.copy(objectUpdates =
      List(ObjectUpdate(ObjectId(new UUID(9, 100)), DataBuffer(Array.tabulate[Byte](6000)(i => (i % 101).toByte))))))
    val y = mkTx(2, 2)
    var crl = SimpleCRL(tdir, 3, size).crl
    saveAndWait(crl, x._1, x._2)
    stubAndWait(crl, storeId2, transactionId3)
    crl.shutdown()

    def check(r: SimpleCRL.InitialCRLState, what: String): Unit =
      val byTx = r.trsList.map(t => t.txd.transactionId -> t).toMap
      assert(byTx.get(x._1).map(_.objectUpdates).contains(x._2.objectUpdates), what)
      assert(r.appliedStubs == Map(TxId(storeId2, transactionId3) -> stubStart), what)

    var rotations = 0
    var r = SimpleCRL(tdir, 3, size)
    check(r, "first reopen")
    crl = r.crl
    for n <- 1 to 30 do
      val stream = crl.currentStreamNumber
      saveAndWait(crl, y._1, y._2)
      settle(crl)
      if crl.currentStreamNumber != stream then
        rotations += 1
      if n % reopenEvery == 0 then
        crl.shutdown()
        r = SimpleCRL(tdir, 3, size)
        check(r, s"reopen after save $n")
        crl = r.crl
    crl.shutdown()
    // Every stream recycled at least twice: X was copied forward from a recovered location more than once
    assert(rotations >= 6)
    val last = SimpleCRL(tdir, 3, size)
    check(last, "last reopen")
    last.crl.shutdown()

  test("Records an open recovers are copied forward by later rotations") {
    rotateRecovered(reopenEvery = 1000)
  }

  test("Records are copied forward by rotations when the CRL is reopened after every save") {
    rotateRecovered(reopenEvery = 1)
  }

  // ---- Piece relocation and the live-piece index ----

  /** Drives the log with fillers of storeId2, each saved under a fresh id once the one before it is deleted, so the
    * only live filler is the newest one and fillers pin nothing a rotation evacuates */
  private class Filler(crl: SimpleCRL):
    private var n = 0L
    private def id(k: Long) = TransactionId(new UUID(8, k))

    def next(): Unit =
      crl.deleteTransaction(storeId2, id(n))
      n += 1
      saveAndWait(crl, id(n), trs2)
      settle(crl)

    def until(stream: Int): Unit =
      while crl.currentStreamNumber != stream do
        next()

  private val pieceStreamSize = 4096L * 4

  /** A (storeId) saved data-less (its txd only, in stream 1), then with two 1500-byte updates in entry `eU` of
    * stream 2, the oldest stream once the fillers reach stream 0. Returns A and `eU` */
  private def countChanged(crl: SimpleCRL, fill: Filler): ((TransactionId, TransactionRecoveryState), Long) =
    val (aId, a0) = mkTx(1, 0)
    val a = a0.copy(objectUpdates = List(1, 2).map(k =>
      ObjectUpdate(ObjectId(new UUID(9, k)), DataBuffer(Array.fill[Byte](1500)(k.toByte)))))
    fill.until(1)
    saveAndWait(crl, aId, a0) // A's data-less prepare: its txd only, in stream 1
    fill.until(2)
    settle(crl)
    val eU = crl.currentEntrySerialNumber
    saveAndWait(crl, aId, a) // The update count changes: the updates are written, in stream 2
    fill.until(0)
    ((aId, a), eU)

  /** Recovers the CRL's files while it is open, as a crash now would: the CRL's own stores only */
  private def recoverNow(): List[TransactionRecoveryState] =
    val files = (0 until 3).map(n => (StreamId(n), tdir / s"$n.crl")).toList
    Recovery.recover(files, recordedId(tdir), files.map(_._1).toVector).trsList.filter(_.storeId == storeId)

  test("An update-count change writes the txd again beside the updates (W4)") {
    val crl = SimpleCRL(tdir, 3, pieceStreamSize).crl
    val fill = Filler(crl)
    val ((_, a), eU) = countChanged(crl, fill)
    // Every piece of A lies in the entry that built the new count: the data-less form's txd in stream 1 was not reused
    val files = (0 until 3).map(n => (StreamId(n), tdir / s"$n.crl")).toList
    val recovered = Recovery.recover(files, recordedId(tdir), files.map(_._1).toVector).txs.filter(_.id.storeId == storeId)
    assert(recovered.map(t => (t.txdPiece +: t.updatePieces).map(_.serial).toSet) == List(Set(eU)))
    assert(recovered.map(_.txdPiece.location.streamId) == List(stream2))
    // So the rotation into stream 0, which evacuated stream 1, carried nothing of A
    assert(headerAt(tdir / "0.crl", 0).dynamicDataSize < a.serializedTxd.size)
    crl.shutdown()

    val i2 = SimpleCRL(tdir, 3, pieceStreamSize)
    val reopened = i2.trsList.filter(_.storeId == storeId)
    assert(reopened.map(_.serializedTxd) == List(a.serializedTxd))
    assert(reopened.map(_.objectUpdates) == List(a.objectUpdates))
    i2.crl.shutdown()
  }

  /** A's pieces lie in entry `eU`. Dropping its object data writes nothing: its last-built form keeps the updates,
    * pinned, and an evacuation of their stream carries them (W5), until a save builds a form without them
    * (`saveAfterDrop`). Until then the last durable static record names them, and a crash recovers them */
  private def droppedPiecesStayPinnedUntilSaved(saveAfterDrop: Boolean): Unit =
    val crl = SimpleCRL(tdir, 3, pieceStreamSize).crl
    val fill = Filler(crl)
    val ((aId, a), eU) = countChanged(crl, fill)
    assert(crl.oldestEntryNeeded == eU)

    crl.dropTransactionObjectData(storeId, aId)
    fill.next()
    assert(crl.currentStreamNumber == 0)
    assert(crl.oldestEntryNeeded == eU)
    assert(recoverNow().map(_.objectUpdates) == List(a.objectUpdates))

    if saveAfterDrop then
      saveAndWait(crl, aId, a)
      settle(crl)
      // The new form leaves the updates out; it names the txd where it lies
      assert(recoverNow().map(_.objectUpdates) == List(Nil))

    // Up to the wrap: the rotation into 1 evacuates stream 2, the one into 2 evacuates stream 0, back into 0. The
    // save above may itself have rotated into 1
    val updates = if saveAfterDrop then Nil else a.objectUpdates
    var evacuated = crl.currentStreamNumber == 1
    for stream <- List(1, 2, 0) do
      while crl.currentStreamNumber != stream do
        fill.next()
        evacuated ||= crl.currentStreamNumber == 1
        if evacuated then
          assert(crl.oldestEntryNeeded > eU)
        else
          assert(crl.oldestEntryNeeded == eU)
        val now = recoverNow()
        assert(now.map(_.serializedTxd) == List(a.serializedTxd))
        assert(now.map(_.objectUpdates) == List(updates))
    crl.shutdown()

    val i2 = SimpleCRL(tdir, 3, pieceStreamSize)
    assert(i2.trsList.filter(_.storeId == storeId).map(t => (t.serializedTxd, t.objectUpdates)) ==
      List((a.serializedTxd, updates)))
    i2.crl.shutdown()

  test("Dropped pieces stay pinned until a save builds a form without them") {
    droppedPiecesStayPinnedUntilSaved(saveAfterDrop = true)
  }

  test("An evacuation carries dropped data until the record is saved again (W5)") {
    droppedPiecesStayPinnedUntilSaved(saveAfterDrop = false)
  }

  // ---- dropStore is one request (crash-recovery-log F12) ----

  /** Holds the CRL thread until the returned latch is counted down */
  private def holdCrlThread(crl: SimpleCRL): CountDownLatch =
    val held = new CountDownLatch(1)
    val release = new CountDownLatch(1)
    crl.runOnCrlThread: () =>
      held.countDown()
      release.await()
    assert(held.await(10, TimeUnit.SECONDS))
    release

  /** Saved before a drop of its store, then saved again after it (the store loaded again) while the CRL thread was
    * held, so the drop and the later save queue back to back. `writeInFlight`: another store's save is written as
    * the drop is processed, so the drop's deletion and the later save share the next entry */
  private def saveAfterDrop(writeInFlight: Boolean): Unit =
    val v1 = trsValidTxd
    val v2 = trsValidTxd.copy(objectUpdates = List(ou1), disposition = TransactionDisposition.VoteAbort)
    val i = SimpleCRL(tdir, 3, 1024 * 1024)
    saveAndWait(i.crl, transactionId, v1)
    stubAndWait(i.crl, storeId, transactionId3)

    val release = holdCrlThread(i.crl)
    val saved = new LinkedBlockingQueue[String]()
    if writeInFlight then
      i.crl.save(transactionId2, trsValidTxd2, () => saved.put("other"))
    val dropped = i.crl.dropStore(storeId)
    i.crl.save(transactionId, v2, () => saved.put("v2"))
    i.crl.saveAppliedStub(storeId, transactionId4, stubStart, () => saved.put("stub"))
    release.countDown()

    Await.result(dropped, 10.seconds)
    val expected = if writeInFlight then Set("other", "v2", "stub") else Set("v2", "stub")
    assert((1 to expected.size).map(_ => saved.poll(10, TimeUnit.SECONDS)).toSet == expected)
    // The drop took v1 and the stub saved before it, and nothing saved after it
    val after = StoreRecoveryState(List(v2), Map(transactionId4 -> stubStart))
    readState(i.crl, storeId) shouldBe after
    i.crl.shutdown()

    val i2 = SimpleCRL(tdir, 3, 1024 * 1024)
    readState(i2.crl, storeId) shouldBe after
    i2.crl.shutdown()

  test("dropStore keeps a same-id save queued after it") {
    saveAfterDrop(writeInFlight = false)
  }

  test("dropStore keeps a same-id save that lands in the entry holding its deletion") {
    saveAfterDrop(writeInFlight = true)
  }

  // ---- Durability of object data drops and barriers (transaction-recovery R-N13) ----

  test("a drop alone writes nothing: a restart recovers the object data") {
    val queue = new LinkedBlockingQueue[String]()
    val i = SimpleCRL(tdir, 3, 1024 * 1024)
    i.crl.save(transactionId, trsValidTxd, () => queue.put(""))
    queue.take()
    i.crl.dropTransactionObjectData(storeId, transactionId)
    // The in-memory state still carries it, and so would a transfer's saved state
    assert(i.crl.getFullRecoveryState(storeId).head.objectUpdates == List(ou1, ou2))
    i.crl.shutdown()

    val i2 = SimpleCRL(tdir, 3, 1024 * 1024)
    assert(i2.trsList.head.objectUpdates == List(ou1, ou2))
    i2.crl.shutdown()
  }

  test("a save without one object's update keeps the other's") {
    val queue = new LinkedBlockingQueue[String]()
    val i = SimpleCRL(tdir, 3, 1024 * 1024)
    i.crl.save(transactionId, trsValidTxd, () => queue.put(""))
    queue.take()
    i.crl.save(transactionId, trsValidTxd.copy(objectUpdates = List(ou2)), () => queue.put(""))
    queue.take()
    i.crl.shutdown()

    val i2 = SimpleCRL(tdir, 3, 1024 * 1024)
    assert(i2.trsList.map(_.objectUpdates) == List(List(ou2)))
    assert(i2.trsList.head.serializedTxd == trsValidTxd.serializedTxd)
    i2.crl.shutdown()
  }

  test("whenDurable runs once a deletion queued before it is on disk") {
    val queue = new LinkedBlockingQueue[String]()
    val i = SimpleCRL(tdir, 3, 1024 * 1024)
    i.crl.save(transactionId, trsValidTxd, () => queue.put(""))
    queue.take()
    i.crl.deleteTransaction(storeId, transactionId)
    i.crl.whenDurable(storeId, () => queue.put("barrier"))
    assert(queue.poll(10, TimeUnit.SECONDS) == "barrier")
    // Read from the files while the CRL is still open: the deletion is durable
    val files = (0 until 3).map(n => (StreamId(n), tdir / s"$n.crl")).toList
    assert(Recovery.recover(files, recordedId(tdir), files.map(_._1).toVector).trsList.isEmpty)
    i.crl.shutdown()
  }

  test("whenDurable on an idle CRL, or behind a write in flight, runs") {
    val queue = new LinkedBlockingQueue[String]()
    val i = SimpleCRL(tdir, 3, 1024 * 1024)
    i.crl.whenDurable(storeId, () => queue.put("idle"))
    assert(queue.poll(10, TimeUnit.SECONDS) == "idle")
    // Queued right behind a save: its entry may be in flight, or not yet started
    for n <- 1 to 50 do
      i.crl.save(transactionId, trsValidTxd, () => ())
      i.crl.whenDurable(storeId, () => queue.put(s"b$n"))
      assert(queue.poll(10, TimeUnit.SECONDS) == s"b$n")
    i.crl.shutdown()
  }

  // ===== Bounded entries and evacuation-first rotations (crash-recovery-log 6.1) =====

  /** The entries of a stream's current cycle, with their offsets: from offset 0, while their headers are intact and
    * carry the head's stream UUID */
  private def currentCycle(bytes: Array[Byte]): List[(Int, LogEntry.EntryHeader)] =
    def header(offset: Int) = LogEntry.loadHeader(ByteBuffer.wrap(bytes, offset, LogEntry.StaticEntryHeaderSize).slice())
    if !LogEntry.headerIntact(bytes.slice(0, LogEntry.StaticEntryHeaderSize)) then
      Nil
    else
      val uuid = header(0).streamUUID
      Iterator.iterate(0)(o => o + header(o).entrySize.toInt)
        .takeWhile: o =>
          o < bytes.length && LogEntry.headerIntact(bytes.slice(o, o + LogEntry.StaticEntryHeaderSize)) &&
            header(o).streamUUID == uuid
        .map(o => (o, header(o))).toList

  private val ringStreamSize = 64 * 1024L

  /** Transaction `n` of storeId with one update of `bytes` bytes */
  private def sized(n: Long, bytes: Int): (TransactionId, TransactionRecoveryState) =
    val (tid, t) = mkTx(n, 0)
    (tid, t.copy(objectUpdates = List(ObjectUpdate(ObjectId(new UUID(9, n)), DataBuffer(Array.fill[Byte](bytes)(n.toByte))))))

  /** A completion handler that blocks the writer thread, once it runs, until `release` is counted down: every request
    * queued meanwhile is processed into unwritten entries, and the files hold exactly the entries written so far */
  private class Hold:
    val reached = new CountDownLatch(1)
    val release = new CountDownLatch(1)
    val handler: () => Unit = () =>
      reached.countDown()
      release.await(10, TimeUnit.SECONDS)
    def awaitReached(): Unit = assert(reached.await(10, TimeUnit.SECONDS))

  /** Saves `t` with a handler that holds the writer, once that save's entry is written */
  private def holdWriter(crl: SimpleCRL, t: (TransactionId, TransactionRecoveryState)): Hold =
    val hold = new Hold
    crl.save(t._1, t._2, hold.handler)
    hold.awaitReached()
    hold

  /** `f` evaluated on the CRL thread, after every request queued before */
  private def onCrl[T](crl: SimpleCRL)(f: => T): T =
    val q = new LinkedBlockingQueue[Option[T]]()
    crl.runOnCrlThread(() => q.put(Some(f)))
    q.poll(10, TimeUnit.SECONDS).get

  /** The CRL's files in `tdir` as they are now, copied to a directory of their own: a crash at this moment */
  private def crashImage(name: String, numStreams: Int): os.Path =
    val dir = tdir / name
    os.makeDir(dir)
    (SimpleCRL.SizingFile :: (0 until numStreams).map(n => s"$n.crl").toList).foreach(f => os.copy(tdir / f, dir / f))
    dir

  /** storeId's recovered transactions (Filler's, of storeId2, carry no valid txd) */
  private def recoveredById(r: SimpleCRL.InitialCRLState): Map[TransactionId, TransactionRecoveryState] =
    r.trsList.filter(_.storeId == storeId).map(t => t.txd.transactionId -> t).toMap

  test("Pending saves larger than half a stream are cut into entries that each fit") {
    val i = SimpleCRL(tdir, 4, ringStreamSize)
    val h = sized(100, 100)
    val hold = holdWriter(i.crl, h)
    // 40 saves of 2 KiB each, as admission counts them: more than a stream in all
    val dataBytes = 2048 - CrlAdmission.TxOverheadBytes.toInt - CrlAdmission.ObjectUpdateOverheadBytes.toInt -
      mkTx(1, 0)._2.serializedTxd.size
    val saves = (1 to 40).map(n => sized(n, dataBytes))
    val done = new LinkedBlockingQueue[TransactionId]()
    saves.foreach(t => i.crl.save(t._1, t._2, () => done.put(t._1)))
    // All processed while the writer is held: queued in sealed entries behind the newest
    assert(onCrl(i.crl)(i.crl.sealedEntryCount) >= 2)
    hold.release.countDown()
    assert((1 to 40).map(_ => done.poll(10, TimeUnit.SECONDS)).toSet == saves.map(_._1).toSet)
    i.crl.shutdown()

    val entries = (0 until 4).flatMap(n => currentCycle(os.read.bytes(tdir / s"$n.crl")))
    assert(entries.size > 3)
    entries.foreach((_, e) => assert(e.entrySize <= ringStreamSize / 2))
    val r = SimpleCRL(tdir, 4, ringStreamSize)
    assert(recoveredById(r).view.mapValues(_.objectUpdates).toMap ==
      (saves :+ h).map(t => t._1 -> t._2.objectUpdates).toMap)
    r.crl.shutdown()
  }

  test("A rotation writes its evacuation before the pending saves") {
    val i = SimpleCRL(tdir, 4, ringStreamSize)
    val fill = Filler(i.crl)
    val x = sized(1, 10000)
    saveAndWait(i.crl, x._1, x._2) // In stream 0, which the rotation into stream 3 evacuates
    List(1, 2, 3).foreach(fill.until)
    val head = currentCycle(os.read.bytes(tdir / "3.crl"))
    val (_, evacuation) = head.head
    // Its own entry: X's pieces and no pending request, which follows it in the next entry
    assert(evacuation.numTransactions == 1)
    assert(evacuation.numDeletedTransactions == 0)
    assert(evacuation.dynamicDataSize == x._2.serializedTxd.size + 10000)
    assert(head.size >= 2)
    assert(head(1)._2.entrySerialNumber == evacuation.entrySerialNumber + 1)
    assert(head(1)._2.numTransactions + head(1)._2.numDeletedTransactions > 0)
    i.crl.shutdown()

    val r = SimpleCRL(tdir, 4, ringStreamSize)
    assert(recoveredById(r)(x._1).objectUpdates == x._2.objectUpdates)
    r.crl.shutdown()
  }

  test("The admission limit is (n - 2) * S/2 - S/16: 15S/16 for 4 streams, 9S - S/16 for 20") {
    for (n, expected) <- List((4, ringStreamSize * 15 / 16), (20, 9 * ringStreamSize - ringStreamSize / 16)) do
      val dir = tdir / s"streams-$n"
      val i = SimpleCRL(dir, n, ringStreamSize)
      assert(i.crl.admissionLimitBytes == expected)
      i.crl.shutdown()
  }

  test("A new CRL's streams are a multiple of 4096 bytes, and at least 3 (S1)") {
    assertThrows[IllegalArgumentException](SimpleCRL(tdir, 4, ringStreamSize + 512))
    assertThrows[IllegalArgumentException](SimpleCRL(tdir, 2, ringStreamSize))
    assert(!os.exists(tdir / SimpleCRL.SizingFile))
    SimpleCRL(tdir, 3, ringStreamSize).crl.shutdown()
  }

  test("A save after its record's form was sealed takes a form of its own: the sealed entry is unchanged (W1a)") {
    val i = SimpleCRL(tdir, 4, ringStreamSize)
    val hold = holdWriter(i.crl, sized(100, 100))
    val (aId, a0) = mkTx(1, 0) // A's data-less prepare
    val a = sized(1, 3000)._2  // and its late data
    val f1 = sized(2, 20000)
    val f2 = sized(3, 20000)
    val inE1 = new Hold
    i.crl.save(aId, a0, inE1.handler)
    i.crl.save(f1._1, f1._2, () => ())
    i.crl.save(f2._1, f2._2, () => ()) // Seals A0 and F1's entry, E1
    val done = new LinkedBlockingQueue[String]()
    i.crl.save(aId, a, () => done.put("a"))
    assert(onCrl(i.crl)(i.crl.sealedEntryCount) == 1)

    // E1 as built from the forms its own requests took: A's data-less form beside F1
    val probe = LogEntry(new Image(new LivePieces))
    probe.addTx(TxId(storeId, aId), TxSave(TxForm(a0, keepUpdates = true), writesPieces = true), () => ())
    probe.addTx(TxId(storeId, f1._1), TxSave(TxForm(f1._2, keepUpdates = true), writesPieces = true), () => ())

    hold.release.countDown()
    inE1.awaitReached()
    val crash = crashImage("after-e1", 4)
    inE1.release.countDown()
    assert(done.poll(10, TimeUnit.SECONDS) == "a")
    i.crl.shutdown()

    val stream0 = currentCycle(os.read.bytes(tdir / "0.crl"))
    val (_, e1) = stream0(1)
    assert(e1.numTransactions == 2)
    assert(e1.entrySize == probe.entrySize)

    // A crash after E1 recovers the data-less form, the end the one with the data
    val r1 = SimpleCRL(crash, 4, ringStreamSize)
    assert(recoveredById(r1)(aId).objectUpdates == Nil)
    assert(recoveredById(r1)(aId).serializedTxd == a0.serializedTxd)
    assert(recoveredById(r1)(f1._1).objectUpdates == f1._2.objectUpdates)
    r1.crl.shutdown()
    val r2 = SimpleCRL(tdir, 4, ringStreamSize)
    assert(recoveredById(r2)(aId).objectUpdates == a.objectUpdates)
    r2.crl.shutdown()
  }

  test("A save after a final deletion in an older unwritten entry is skipped and reports success (W1b)") {
    val i = SimpleCRL(tdir, 4, ringStreamSize)
    saveAndWait(i.crl, transactionId, trsValidTxd)
    val hold = holdWriter(i.crl, sized(100, 100))
    val f1 = sized(2, 20000)
    val f2 = sized(3, 20000)
    i.crl.save(f1._1, f1._2, () => ())
    i.crl.deleteTransaction(storeId, transactionId)
    i.crl.save(f2._1, f2._2, () => ()) // Seals the entry holding the deletion
    val done = new LinkedBlockingQueue[String]()
    i.crl.save(transactionId, trsValidTxd, () => done.put("skipped"))
    assert(onCrl(i.crl)(i.crl.sealedEntryCount) == 1)
    // No record was created for it
    assert(!readState(i.crl, storeId).transactions.exists(_.txd.transactionId == transactionId))

    hold.release.countDown()
    assert(done.poll(10, TimeUnit.SECONDS) == "skipped")
    assert(!readState(i.crl, storeId).transactions.exists(_.txd.transactionId == transactionId))
    i.crl.shutdown()
    val r = SimpleCRL(tdir, 4, ringStreamSize)
    assert(!recoveredById(r).contains(transactionId))
    r.crl.shutdown()
  }

  test("A save after a final deletion in the entry in flight is skipped and reports success once it completes (W1b)") {
    val i = SimpleCRL(tdir, 4, ringStreamSize)
    saveAndWait(i.crl, transactionId, trsValidTxd)
    // The deletion's entry is written, and its write held from completing by the handler of a save beside it
    val first = holdWriter(i.crl, sized(100, 100))
    val hold = new Hold
    i.crl.deleteTransaction(storeId, transactionId)
    i.crl.save(sized(101, 100)._1, sized(101, 100)._2, hold.handler)
    first.release.countDown()
    hold.awaitReached()
    val done = new LinkedBlockingQueue[String]()
    i.crl.save(transactionId, trsValidTxd, () => done.put("skipped"))
    assert(!readState(i.crl, storeId).transactions.exists(_.txd.transactionId == transactionId))
    assert(done.poll(50, TimeUnit.MILLISECONDS) == null)
    hold.release.countDown()
    assert(done.poll(10, TimeUnit.SECONDS) == "skipped")
    i.crl.shutdown()
    val r = SimpleCRL(tdir, 4, ringStreamSize)
    assert(!recoveredById(r).contains(transactionId))
    r.crl.shutdown()
  }

  test("A deleted record stays pinned until its deletion's entry is built: a crash before it recovers it (W3)") {
    val i = SimpleCRL(tdir, 4, ringStreamSize)
    saveAndWait(i.crl, transactionId, trsValidTxd) // X, in the oldest entry needed
    val h = sized(100, 100)
    val hold = holdWriter(i.crl, h)
    val f1 = sized(2, 20000)
    val f2 = sized(3, 20000)
    val inE2 = new Hold
    i.crl.save(f1._1, f1._2, inE2.handler)
    i.crl.save(f2._1, f2._2, () => ()) // Seals F1's entry
    i.crl.deleteTransaction(storeId, transactionId) // Behind it
    assert(onCrl(i.crl)(i.crl.sealedEntryCount) == 1)

    hold.release.countDown()
    // F1's entry is durable and the deletion's is not: X's state must still be recoverable
    inE2.awaitReached()
    val crash = crashImage("before-deletion", 4)
    inE2.release.countDown()
    val barrier = new LinkedBlockingQueue[String]()
    i.crl.whenDurable(storeId, () => barrier.put(""))
    assert(barrier.poll(10, TimeUnit.SECONDS) == "")
    i.crl.shutdown()

    val r1 = SimpleCRL(crash, 4, ringStreamSize)
    assert(recoveredById(r1).keySet == Set(transactionId, h._1, f1._1))
    assert(recoveredById(r1)(transactionId).objectUpdates == trsValidTxd.objectUpdates)
    r1.crl.shutdown()
    val r2 = SimpleCRL(tdir, 4, ringStreamSize)
    assert(recoveredById(r2).keySet == Set(h._1, f1._1, f2._1))
    r2.crl.shutdown()
  }

  test("A head not written within n - 1 rotations fails the CRL instead of rotating on (W6)") {
    val i = SimpleCRL(tdir, 4, ringStreamSize)
    // Saved without admission: live records of 20 KB each come to more than the ring holds
    val saves = (1 to 16).map(n => sized(n, 20000))
    saves.foreach(t => i.crl.save(t._1, t._2, () => ()))
    val cause = Await.result(i.crl.failed, 10.seconds)
    assert(cause.isInstanceOf[IllegalStateException], cause)
    assert(cause.getMessage.contains("not written after 3 rotations"))
    assertShutdownReturns(i.crl)

    // The files hold what was written, as after a crash
    val r = SimpleCRL(tdir, 4, ringStreamSize)
    assert(r.trsList.nonEmpty)
    recoveredById(r).foreach((tid, t) => assert(saves.toMap.get(tid).map(_.objectUpdates) == Some(t.objectUpdates)))
    r.crl.shutdown()
  }

  // ---- Growth ----

  private val growthStreamSize = 64 * 1024L

  /** A large device, mostly free, so growth is allowed unless a test says otherwise. The back-off is long: a test
    * whose request is refused sees a later one ignored, and one that needs a retry passes a short one */
  private def growthPolicy(maxStreams: Int = 20,
                           capacity: () => Long = () => 1L << 40,
                           freeSpace: () => Long = () => 1L << 39,
                           retryAfter: FiniteDuration = 1.hour): GrowthPolicy =
    GrowthPolicy(maxStreams, 5, capacity, freeSpace, retryAfter)

  private def openGrowing(policy: GrowthPolicy = growthPolicy(), dir: os.Path = tdir): SimpleCRL.InitialCRLState =
    SimpleCRL(dir, 4, growthStreamSize, growth = policy)

  private def recordedRing(dir: os.Path = tdir): Vector[Int] =
    SimpleCRL.Sizing.decode(os.read(dir / SimpleCRL.SizingFile)).get.ring.map(_.number)

  /** The limits `crl` reports raised, in order */
  private def limitsRaised(crl: SimpleCRL): LinkedBlockingQueue[java.lang.Long] =
    val q = new LinkedBlockingQueue[java.lang.Long]()
    crl.onAdmissionLimitRaised(limit => q.put(limit))
    q

  /** The next limit raised: a growth committed */
  private def nextRaise(q: LinkedBlockingQueue[java.lang.Long]): Long =
    Option(q.poll(10, TimeUnit.SECONDS)).map(_.longValue).getOrElse(fail("no growth was committed"))

  private def eventually(what: String)(done: => Boolean): Unit =
    val deadline = System.nanoTime() + 10.seconds.toNanos
    while !done do
      assert(System.nanoTime() < deadline, what)
      Thread.sleep(1)

  private def awaitAttemptsEnded(crl: SimpleCRL, count: Int): Unit =
    eventually(s"growth attempt $count did not end")(crl.growthAttemptsEndedForTest >= count)

  /** The streams the writer enters over the next `rotations` rotations, driven by fillers */
  private def nextStreams(crl: SimpleCRL, fill: Filler, rotations: Int): List[Int] =
    var entered = List[Int]()
    var saves = 0
    while entered.size < rotations do
      val from = crl.currentStreamNumber
      fill.next()
      saves += 1
      assert(saves < 1000, s"no rotation after $saves saves")
      if crl.currentStreamNumber != from then
        entered = crl.currentStreamNumber :: entered
    entered.reverse

  private def growthLeftovers(dir: os.Path = tdir): List[String] =
    os.list(dir).map(_.last).filter(f => f == "4.crl" || f.endsWith(".crl.tmp")).toList

  test("A growth inserts a stream after the active one and raises the limit") {
    val s = growthStreamSize
    val i = openGrowing()
    val raised = limitsRaised(i.crl)
    val x = sized(1, 3000)
    saveAndWait(i.crl, x._1, x._2)
    val fill = Filler(i.crl)
    fill.until(2)
    i.crl.requestGrowth()
    assert(nextRaise(raised) == 3 * s / 2 - s / 16)
    assert(recordedRing() == Vector(0, 1, 2, 4, 3))
    assert(i.crl.numStreams == 5)
    assert(i.crl.admissionLimitBytes == 3 * s / 2 - s / 16)
    assert(os.size(tdir / "4.crl") == s)
    assert(!os.exists(tdir / "4.crl.tmp"))
    assert(nextStreams(i.crl, fill, 1) == List(4))
    i.crl.shutdown()

    val r1 = openGrowing()
    assert(r1.crl.numStreams == 5)
    assert(r1.crl.currentStreamNumber == 4)
    assert(recoveredById(r1)(x._1).objectUpdates == x._2.objectUpdates)
    // Around the whole ring, X carried forward by each rotation that empties its stream
    assert(nextStreams(r1.crl, Filler(r1.crl), 5) == List(3, 0, 1, 2, 4))
    r1.crl.shutdown()
    val r2 = openGrowing()
    assert(recoveredById(r2)(x._1).objectUpdates == x._2.objectUpdates)
    r2.crl.shutdown()
  }

  test("Growth stops at maxStreams") {
    val i = openGrowing(growthPolicy(maxStreams = 5))
    val raised = limitsRaised(i.crl)
    i.crl.requestGrowth()
    nextRaise(raised)
    awaitAttemptsEnded(i.crl, 1)
    i.crl.requestGrowth()
    awaitAttemptsEnded(i.crl, 2)
    assert(raised.isEmpty)
    assert(i.crl.numStreams == 5)
    assert(recordedRing().size == 5)
    assert(!os.exists(tdir / "5.crl") && !os.exists(tdir / "5.crl.tmp"))
    // The refusal backs off: a request now is ignored
    i.crl.requestGrowth()
    assert(!i.crl.growthBusyForTest)
    i.crl.shutdown()

    // A ring already at its cap, or growth disabled, runs no growth thread, and a request does nothing
    for policy <- List(growthPolicy(maxStreams = 5), GrowthPolicy.Disabled) do
      val r = openGrowing(policy)
      assert(!r.crl.growthThreadAliveForTest)
      r.crl.requestGrowth()
      assert(!r.crl.growthBusyForTest)
      r.crl.shutdown()
    assert(recordedRing().size == 5)
  }

  test("Growth is refused below the free-space floor") {
    val s = growthStreamSize
    val capacity = 100 * s
    // The free space less the new stream must stay at or above 5% of the capacity: 5 S
    val i = openGrowing(growthPolicy(capacity = () => capacity, freeSpace = () => 6 * s - 1))
    i.crl.requestGrowth()
    awaitAttemptsEnded(i.crl, 1)
    assert(i.crl.numStreams == 4)
    assert(recordedRing() == Vector(0, 1, 2, 3))
    assert(growthLeftovers().isEmpty)
    // Backed off: a request now is ignored
    i.crl.requestGrowth()
    assert(!i.crl.growthBusyForTest)
    i.crl.shutdown()

    val r = openGrowing(growthPolicy(capacity = () => capacity, freeSpace = () => 6 * s))
    val raised = limitsRaised(r.crl)
    r.crl.requestGrowth()
    nextRaise(raised)
    assert(r.crl.numStreams == 5)
    r.crl.shutdown()
  }

  test("A failed preallocation leaves the CRL unchanged and alive, and growth retries after its back-off") {
    val failing = new AtomicBoolean(true)
    val i = openGrowing(growthPolicy(
      capacity = () => if failing.get then throw new java.io.IOException("statfs failed") else 1L << 40,
      retryAfter = 20.millis))
    val raised = limitsRaised(i.crl)
    val x = sized(1, 3000)
    saveAndWait(i.crl, x._1, x._2)
    i.crl.requestGrowth()
    awaitAttemptsEnded(i.crl, 1)
    assert(i.crl.numStreams == 4)
    assert(recordedRing() == Vector(0, 1, 2, 3))
    assert(growthLeftovers().isEmpty)
    assert(!i.crl.failed.isCompleted)
    val y = sized(2, 3000)
    saveAndWait(i.crl, y._1, y._2)
    assert(readState(i.crl, storeId).transactions.map(_.txd.transactionId).toSet == Set(x._1, y._1))

    failing.set(false)
    eventually("no growth after the back-off"):
      i.crl.requestGrowth()
      !raised.isEmpty
    assert(i.crl.numStreams == 5)
    i.crl.shutdown()
  }

  test("Growth crashed before its commit leaves a .tmp or an unlisted stream: removed at open") {
    for leftover <- List("4.crl.tmp", "4.crl") do
      val dir = tdir / leftover.replace('.', '-')
      val i = openGrowing(dir = dir)
      val x = sized(1, 3000)
      saveAndWait(i.crl, x._1, x._2)
      i.crl.shutdown()
      FilePreallocation.preallocate(dir / leftover, growthStreamSize)
      val r = openGrowing(dir = dir)
      assert(!os.exists(dir / leftover))
      assert(r.crl.numStreams == 4)
      assert(recoveredById(r)(x._1).objectUpdates == x._2.objectUpdates)
      r.crl.shutdown()
  }

  test("Growth committed but its stream never written: it opens, and the next rotation enters it") {
    val s = growthStreamSize
    val i = SimpleCRL(tdir, 4, s)
    val x = sized(1, 3000)
    saveAndWait(i.crl, x._1, x._2)
    Filler(i.crl).until(2)
    i.crl.shutdown()
    // The files as a crash right after the SizingFile's rewrite leaves them
    FilePreallocation.preallocate(tdir / "4.crl", s)
    val recorded = SimpleCRL.Sizing.decode(os.read(tdir / SimpleCRL.SizingFile)).get
    DurableFiles.write(tdir / SimpleCRL.SizingFile, recorded.copy(ring = Vector(0, 1, 2, 4, 3).map(StreamId(_))).encode)

    val r = SimpleCRL(tdir, 4, s)
    assert(r.crl.numStreams == 5)
    assert(r.crl.currentStreamNumber == 2)
    assert(r.crl.admissionLimitBytes == 3 * s / 2 - s / 16)
    assert(recoveredById(r)(x._1).objectUpdates == x._2.objectUpdates)
    assert(nextStreams(r.crl, Filler(r.crl), 1) == List(4))
    r.crl.shutdown()
    val r2 = SimpleCRL(tdir, 4, s)
    assert(recoveredById(r2)(x._1).objectUpdates == x._2.objectUpdates)
    r2.crl.shutdown()
  }

  test("A growth's first entry in its new stream, torn at any sector, leaves the state before it") {
    val i = openGrowing()
    val raised = limitsRaised(i.crl)
    val x = sized(1, 3000)
    saveAndWait(i.crl, x._1, x._2)
    Filler(i.crl).until(2)
    i.crl.requestGrowth()
    nextRaise(raised)

    // Saves of 20 KB each until one enters stream 4: its entry is the first written there, several sectors long
    val files = (0 until 5).map(n => tdir / s"$n.crl")
    var saved = List(x)
    var rotation: Option[(IndexedSeq[Array[Byte]], IndexedSeq[Array[Byte]])] = None
    var k = 0
    while rotation.isEmpty do
      k += 1
      assert(k < 10)
      val before = files.map(os.read.bytes(_))
      val t = sized(100 + k, 20000)
      saveAndWait(i.crl, t._1, t._2)
      settle(i.crl)
      if i.crl.currentStreamNumber == 4 then
        rotation = Some((before, files.map(os.read.bytes(_))))
      else
        saved = t :: saved
    i.crl.shutdown()

    val (before, after) = rotation.get
    val writes = tornRotationWrites(4, before, after)
    assert(writes.exists(_.sectors > 1))
    val expected = saved.map(t => t._1 -> t._2.objectUpdates).toMap
    writes.flatMap(_.images).foreach: (what, image) =>
      files.zip(image).foreach((f, bytes) => os.write.over(f, bytes))
      val r = try SimpleCRL(tdir, 4, growthStreamSize) catch case t: Throwable => fail(s"$what: $t")
      assert(recoveredById(r).view.mapValues(_.objectUpdates).toMap == expected, what)
      r.crl.shutdown()
  }

  test("A growth that arrives during a rotation is committed once the rotation's head is written") {
    val i = openGrowing()
    val fill = Filler(i.crl)
    // X lies in stream 3, which the rotation from 1 into 2 evacuates
    fill.until(3)
    val x = sized(1, 3000)
    saveAndWait(i.crl, x._1, x._2)
    fill.until(0)
    fill.until(1)
    // Stream 1 left with too little room for S's six sectors, so S's save rotates
    while onCrl(i.crl)(i.crl.activeStreamRoomForTest) >= 6 * 4096 do
      fill.next()
    assert(i.crl.currentStreamNumber == 1)
    val s = sized(2, 21000)

    val sDurable = new AtomicBoolean(false)
    val durableAtCommit = new LinkedBlockingQueue[Boolean]()
    i.crl.onAdmissionLimitRaised(_ => durableAtCommit.put(sDurable.get))
    val release = holdCrlThread(i.crl)
    i.crl.save(s._1, s._2, () => sDurable.set(true))
    i.crl.requestGrowth()
    // S, then the growth, queue behind the held CRL thread: S's rotation is under way, its evacuation of X in flight,
    // when the growth arrives
    eventually("the growth is not queued")(i.crl.queuedRequestsForTest == 2)
    release.countDown()
    assert(durableAtCommit.poll(10, TimeUnit.SECONDS) == true)
    assert(i.crl.currentStreamNumber == 2)
    assert(recordedRing() == Vector(0, 1, 2, 4, 3))
    assert(nextStreams(i.crl, fill, 1) == List(4))
    i.crl.shutdown()

    val r = openGrowing()
    assert(recoveredById(r).view.mapValues(_.objectUpdates).toMap ==
      List(x, s).map(t => t._1 -> t._2.objectUpdates).toMap)
    r.crl.shutdown()
  }

  test("A stream a crashed growth orphaned is removed, and the next growth recreates it (growthOrphanThenRegrow)") {
    val s = growthStreamSize
    val i = openGrowing()
    val x = sized(1, 3000)
    saveAndWait(i.crl, x._1, x._2)
    i.crl.shutdown()
    // Renamed into place but never committed, holding bytes of no entry
    os.write(tdir / "4.crl", Array.fill[Byte](s.toInt)(0x5a))

    val r = openGrowing()
    assert(!os.exists(tdir / "4.crl"))
    val raised = limitsRaised(r.crl)
    r.crl.requestGrowth()
    nextRaise(raised)
    assert(recordedRing() == Vector(0, 4, 1, 2, 3))
    val grown = os.read.bytes(tdir / "4.crl")
    assert(grown.length == s && grown.forall(_ == 0))
    assert(nextStreams(r.crl, Filler(r.crl), 1) == List(4))
    r.crl.shutdown()

    val r2 = openGrowing()
    assert(r2.crl.numStreams == 5)
    assert(recoveredById(r2)(x._1).objectUpdates == x._2.objectUpdates)
    r2.crl.shutdown()
  }

  test("A growth in flight at shutdown is abandoned without holding the shutdown up (growthDuringShutdown)") {
    val inPolicy = new CountDownLatch(1)
    val latch = new CountDownLatch(1)
    val i = openGrowing(growthPolicy(freeSpace = () =>
      inPolicy.countDown()
      latch.await()
      1L << 39))
    val x = sized(1, 3000)
    saveAndWait(i.crl, x._1, x._2)
    i.crl.requestGrowth()
    assert(inPolicy.await(10, TimeUnit.SECONDS))
    val shutdown = new CountDownLatch(1)
    new Thread(() => { i.crl.shutdown(); shutdown.countDown() }).start()
    assert(shutdown.await(10, TimeUnit.SECONDS), "shutdown waited for the growth")
    latch.countDown()
    eventually("the growth thread outlived the CRL")(!i.crl.growthThreadAliveForTest)
    assert(growthLeftovers().isEmpty)

    val r = openGrowing()
    assert(r.crl.numStreams == 4)
    assert(recordedRing() == Vector(0, 1, 2, 3))
    assert(recoveredById(r)(x._1).objectUpdates == x._2.objectUpdates)
    r.crl.shutdown()
  }

  test("Growth requested of a CRL that died does nothing") {
    val i = openGrowing()
    val tooBig = ObjectUpdate(ou1.objectId, DataBuffer(new Array[Byte](2 * growthStreamSize.toInt)))
    i.crl.save(transactionId, trsValidTxd.copy(objectUpdates = List(tooBig)), () => ())
    Await.result(i.crl.failed, 10.seconds)
    eventually("the growth thread outlived the CRL")(!i.crl.growthThreadAliveForTest)
    i.crl.requestGrowth()
    assert(!i.crl.growthBusyForTest)
    i.crl.shutdown()
    assert(growthLeftovers().isEmpty)
    assert(recordedRing().size == 4)
  }
}
