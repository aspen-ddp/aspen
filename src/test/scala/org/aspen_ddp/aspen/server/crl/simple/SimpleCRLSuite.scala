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
import org.aspen_ddp.aspen.server.crl.{CrashRecoveryLog, StoreRecoveryState, TransactionRecoveryState}

import java.nio.ByteBuffer
import java.util.UUID
import java.util.concurrent.{CountDownLatch, LinkedBlockingQueue, TimeUnit}
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

  def streams(numStreams: Int): List[(StreamId, os.Path)] =
    var lst: List[(StreamId, os.Path)] = Nil
    for i <- 0 until numStreams do
      lst = (StreamId(i), tdir / f"$i.log") :: lst
    lst.reverse

  private def saveAndWait(crl: CrashRecoveryLog, txid: TransactionId, trs: TransactionRecoveryState): Unit =
    val q = new LinkedBlockingQueue[String]()
    crl.save(txid, trs, () => q.put(""))
    q.take()

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

    i.crl.save(transactionId2, trsValidTxd, completionHandler)
    queue.take()
    assert(i.crl.currentStreamNumber == 2)

    i.crl.save(transactionId2, trsValidTxd, completionHandler)
    queue.take()
    assert(i.crl.currentStreamNumber == 2)

    i.crl.save(transactionId2, trsValidTxd, completionHandler)
    queue.take()
    assert(i.crl.currentStreamNumber == 2)

    i.crl.save(transactionId2, trsValidTxd, completionHandler)
    i.crl.deleteTransaction(storeId, transactionId2)
    queue.take()
    assert(i.crl.currentStreamNumber == 0)

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

    // There's a race condition here where the currentLogEntry variable is updated after
    // the completion callback is executed. It's possible for the take() operation to complete
    // before this happens which causes the currentEntrySerialNumber to still be zero when it
    // should be 1. Add a short sleep to reduce the likelihood of this happening
    Thread.sleep(50)
    
    assert(i.crl.currentEntrySerialNumber == 1)

    i.crl.shutdown()

    val i2 = SimpleCRL(tdir, 3, 1024*1024)

    assert(i2.crl.currentEntrySerialNumber == 2)

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

    val streamWriter = new StreamWriter(4096 * 1000, streams(1))

    val tx = Tx(txid, trs, None, None)

    val le = LogEntry(StreamLocation.Null, 0, 0)
    val stream = new Stream(stream0, streamWriter, UUID.randomUUID(), 0)

    le.addTx(tx, completionHandler)

    val entry1Location = stream.writeEntry(le, () => ())

    val le2 = LogEntry(entry1Location, 1, 0)

    le2.addTx(tx, completionHandler)

    val entry2Location = stream.writeEntry(le2, () => ())

    queue.take() // Block till completion handlers are run

    streamWriter.shutdown()

    val r = Recovery.recover(streams(1))

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

    val streamWriter = new StreamWriter(4096*1000, streams(1))

    val tx = Tx(txid, trs, None, None)
    val le = LogEntry(StreamLocation.Null, 0, 0)
    val stream = new Stream(stream0, streamWriter, UUID.randomUUID(), 0)

    le.addTx(tx, completionHandler)

    val entryLocation = stream.writeEntry(le, () => ())

    assert(entryLocation.streamId == stream0)
    assert(entryLocation.offset == 0)

    queue.take() // Block till completion handlers are run

    streamWriter.shutdown()

    val r = Recovery.recover(streams(1))

    assert(r.trsList.length == 1)
    assert(r.activeStreamId == stream0)

    assert(r.trsList.head.storeId == txid.storeId)
    assert(r.trsList.head.disposition == disp)
    assert(r.trsList.head.serializedTxd == trs.serializedTxd)
    assert(r.trsList.head.objectUpdates == trs.objectUpdates)
    assert(r.trsList.head.paxosAcceptorState == pax)
  }

  test("Recovery without log files") {
    val r = Recovery.recover(Nil)

    assert(r.trsList.length == 0)
    assert(r.activeStreamId == stream0)
  }

  test("Tx Static Save & Load with ObjectUpdate data") {
    val tx = Tx(txid, trs, Some(txdLoc), updateLocations)

    val buffer = new Array[Byte](4096 * 5)

    assert(tx.dynamicDataSize == 0)

    tx.writeStaticEntry(ByteBuffer.wrap(buffer))

    val ltx = Tx.loadTx(ByteBuffer.wrap(buffer))

    assert(ltx.id == txid)
    assert(ltx.disposition == disp)
    assert(ltx.txdLocation == txdLoc)
    assert(ltx.updateLocations == updateLocations)
    assert(ltx.paxosAcceptorState == pax)
  }

  test("Tx Static Save & Load without ObjectUpdate data") {
    val tx = Tx(txid, trs, Some(txdLoc), None, keepObjectUpdates = false)

    val buffer = new Array[Byte](4096 * 5)

    assert(tx.dynamicDataSize == 0)

    tx.writeStaticEntry(ByteBuffer.wrap(buffer))

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

    // Simulate crash: do NOT call shutdown
    val i2 = SimpleCRL(tdir, 3, 1024 * 1024)

    assert(i2.trsList.length == 2)
    val byStore = i2.trsList.groupBy(_.storeId)
    assert(byStore(storeId).head.serializedTxd == trsValidTxd.serializedTxd)
    assert(byStore(storeId).head.objectUpdates == trsValidTxd.objectUpdates)
    assert(byStore(storeId).head.paxosAcceptorState == pax)
    assert(byStore(storeId2).head.serializedTxd == trsValidTxd2.serializedTxd)

    i.crl.shutdown()
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

    // Each reopen writes the recovered transaction into a new entry, linked to the entries before it
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

    // Simulate crash, then recover
    val i2 = SimpleCRL(tdir, 3, 4096 * 3)

    assert(i2.trsList.length == 1)
    assert(i2.trsList.head.storeId == storeId)
    assert(i2.trsList.head.serializedTxd == trsValidTxd.serializedTxd)
    assert(i2.trsList.head.objectUpdates == trsValidTxd.objectUpdates)
    assert(i2.trsList.head.paxosAcceptorState == pax)

    i.crl.shutdown()
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

    Thread.sleep(50)
    assert(i.crl.oldestEntryNeeded == 0)

    // Delete oldest (tx1), flush with a save
    i.crl.deleteTransaction(storeId, transactionId)
    i.crl.save(transactionId3, trsValidTxd3, completionHandler)
    queue.take()
    Thread.sleep(50)
    assert(i.crl.oldestEntryNeeded == 1)

    // Delete new oldest (tx2), flush
    i.crl.deleteTransaction(storeId2, transactionId2)
    i.crl.save(transactionId3, trsValidTxd3, completionHandler)
    queue.take()
    Thread.sleep(50)
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

    Thread.sleep(50)
    assert(i.crl.oldestEntryNeeded == 0)

    // Delete the middle node (tx2)
    i.crl.deleteTransaction(storeId2, transactionId2)
    i.crl.save(transactionId3, trsValidTxd3, completionHandler)
    queue.take()

    // tx1 is still oldest, so oldestEntryNeeded should remain 0
    Thread.sleep(50)
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

  test("moveToQueueHead during recycling preserves all transactions") {
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

    // Fill stream 2 (3 entries)
    for _ <- 1 to 2 do
      i.crl.save(transactionId, trsValidTxd, completionHandler)
      queue.take()
    assert(i.crl.currentStreamNumber == 2)

    // Wrap to stream 0: triggers recycling, moveToQueueHead for both tx1 and tx2
    // (both had dynamic data in stream 0)
    i.crl.save(transactionId, trsValidTxd, completionHandler)
    queue.take()
    assert(i.crl.currentStreamNumber == 0)

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

    i.crl.shutdown()

    val i2 = SimpleCRL(tdir, 3, 1024 * 1024)

    assert(i2.trsList.length == 1)
    assert(i2.trsList.head.storeId == storeId2)

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
    val reader = StreamReader(StreamId(0), streamFile)
    val Some((entry1Start, _)) = reader.readEntry(0): @unchecked
    val Some((_, entry1Header)) = reader.readEntry(entry1Start): @unchecked
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

    val streamWriter = new StreamWriter(4096 * 1000, streams(1))
    val stream = new Stream(stream0, streamWriter, UUID.randomUUID(), 0)

    val tx1 = Tx(txid, trs, None, None)
    val tx2 = Tx(txid2, trs2, None, None)

    val le = LogEntry(StreamLocation.Null, 0, 0)
    le.addTx(tx1, () => ())
    le.addTx(tx2, () => ())
    le.deleteTx(txid)

    stream.writeEntry(le, () => queue.put(""))
    queue.take()
    streamWriter.shutdown()

    val r = Recovery.recover(streams(1))

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

  test("StreamWriter writes every buffer of a write with more buffers than IOV_MAX") {
    val sw = StreamWriter(1024 * 1024, streams(1))
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

  test("An entry holding more transactions than IOV_MAX buffers recovers, including the re-save at open") {
    val count = 600
    val i1 = SimpleCRL(tdir, 3, 64L * 1024 * 1024)
    val done = new CountDownLatch(count)
    for n <- 1 to count do
      val t = mkTx(n, 2)
      i1.crl.save(t._1, t._2, () => done.countDown())
    assert(done.await(30, TimeUnit.SECONDS))
    i1.crl.shutdown()

    // The open re-saves every recovered transaction in one entry, about three buffers each. A torn write of it hides
    // every entry written after it, so the save acknowledged next must survive the reopen
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
    val sector = 4096
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
    for (stream, before, after) <- rotations.reverse do
      val changed = (0 until (size / sector).toInt).filter: s =>
        !java.util.Arrays.equals(before(stream), s * sector, (s + 1) * sector, after(stream), s * sector, (s + 1) * sector)
      assert(changed.nonEmpty)
      largestRotation = largestRotation.max(changed.size)
      // Sectors 0..n-1 of the new entry persisted, and the rest not; or every sector but the first (old head intact)
      def torn(newSectors: Set[Int]): IndexedSeq[Array[Byte]] =
        val bytes = before(stream).clone()
        newSectors.foreach(s => System.arraycopy(after(stream), s * sector, bytes, s * sector, sector))
        before.updated(stream, bytes)
      for n <- 0 until changed.size do
        recoversXandY(torn(changed.take(n).toSet), s"rotation into $stream, first $n of ${changed.size} sectors")
      recoversXandY(torn(changed.drop(1).toSet), s"rotation into $stream, all but its first sector")
    // Some rotation copied X forward
    assert(largestRotation > 1)
  }

  test("An open whose re-save cannot fit in a stream fails, and fails the same way when retried") {
    val size = 4096L * 4
    def big(n: Int) =
      val t = mkTx(n, 1)
      (t._1, t._2.copy(objectUpdates = List(ObjectUpdate(ObjectId(new UUID(9, n)), DataBuffer(new Array[Byte](10000))))))
    val a = big(1)
    val b = big(2)
    // Without admission the two live transactions fill more than a stream, so the open's re-save of both cannot fit
    val i1 = SimpleCRL(tdir, 3, size)
    saveAndWait(i1.crl, a._1, a._2)
    saveAndWait(i1.crl, b._1, b._2)
    i1.crl.shutdown()

    for _ <- 1 to 2 do
      assertThrows[IllegalArgumentException](SimpleCRL(tdir, 3, size))
  }

  // ---- Applied stubs ----

  private def stubAndWait(crl: CrashRecoveryLog, sid: StoreId, tid: TransactionId): Unit =
    val q = new LinkedBlockingQueue[String]()
    crl.saveAppliedStub(sid, tid, () => q.put(""))
    q.take()

  private def readState(crl: CrashRecoveryLog, sid: StoreId): StoreRecoveryState =
    Await.result(crl.readStoreState(sid), Duration(5000, MILLISECONDS))

  test("an applied stub replaces a transaction's state and is recovered as a stub, not a transaction") {
    val i = SimpleCRL(tdir, 3, 1024 * 1024)
    saveAndWait(i.crl, transactionId, trsValidTxd)
    stubAndWait(i.crl, storeId, transactionId)

    readState(i.crl, storeId) shouldBe StoreRecoveryState(Nil, Set(transactionId))
    i.crl.getFullRecoveryState(storeId) shouldBe Nil
    // A transaction deletion leaves the stub
    i.crl.deleteTransaction(storeId, transactionId)
    saveAndWait(i.crl, transactionId2, trsValidTxd2)
    i.crl.getAppliedStubs(storeId) shouldBe Set(transactionId)
    Await.result(i.crl.storesWithEntries(), Duration(5000, MILLISECONDS)) shouldBe Set(storeId, storeId2)
    i.crl.shutdown()

    val i2 = SimpleCRL(tdir, 3, 1024 * 1024)
    i2.trsList.map(_.storeId) shouldBe List(storeId2)
    i2.appliedStubs shouldBe Set(TxId(storeId, transactionId))
    readState(i2.crl, storeId) shouldBe StoreRecoveryState(Nil, Set(transactionId))
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
    readState(i2.crl, storeId) shouldBe StoreRecoveryState(Nil, Set(transactionId))
    i2.crl.shutdown()
  }

  test("an applied stub written after its transaction's deletion in the same entry is kept") {
    val i = SimpleCRL(tdir, 3, 1024 * 1024)
    saveAndWait(i.crl, transactionId, trsValidTxd)
    // Both queued before the CRL thread takes either, so they normally share an entry
    i.crl.deleteTransaction(storeId, transactionId)
    stubAndWait(i.crl, storeId, transactionId)
    i.crl.shutdown()

    val i2 = SimpleCRL(tdir, 3, 1024 * 1024)
    readState(i2.crl, storeId) shouldBe StoreRecoveryState(Nil, Set(transactionId))
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
    readState(i.crl, storeId) shouldBe StoreRecoveryState(Nil, Set(transactionId))
    i.crl.shutdown()

    val i2 = SimpleCRL(tdir, 3, size)
    i2.appliedStubs shouldBe Set(TxId(storeId, transactionId))
    i2.crl.shutdown()
    // And again: the open's re-save carries it on
    val i3 = SimpleCRL(tdir, 3, size)
    i3.appliedStubs shouldBe Set(TxId(storeId, transactionId))
    i3.crl.shutdown()
  }

  test("deleteAppliedStub removes it durably") {
    val i = SimpleCRL(tdir, 3, 1024 * 1024)
    saveAndWait(i.crl, transactionId, trsValidTxd)
    stubAndWait(i.crl, storeId, transactionId3)
    i.crl.deleteAppliedStub(storeId, transactionId3)
    // Requests are handled in order: a save completing after the deletion proves it durable
    saveAndWait(i.crl, transactionId2, trsValidTxd2)
    readState(i.crl, storeId).appliedStubs shouldBe Set()
    i.crl.shutdown()

    val i2 = SimpleCRL(tdir, 3, 1024 * 1024)
    i2.appliedStubs shouldBe Set()
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
    extracted shouldBe StoreRecoveryState(List(trsValidTxd), Set(transactionId3, transactionId4))
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
        reopened.appliedStubs shouldBe Set(TxId(storeId, transactionId3), TxId(storeId, transactionId4))
        reopened.crl.shutdown()

    // A store with only stubs still has entries; dropStore takes them with its transactions
    i.crl.deleteTransaction(storeId, transactionId)
    Await.result(i.crl.storesWithEntries(), Duration(5000, MILLISECONDS)) shouldBe Set(storeId, storeId2)
    Await.result(i.crl.dropStore(storeId), Duration(5000, MILLISECONDS))
    readState(i.crl, storeId) shouldBe StoreRecoveryState(Nil, Set())
    i.crl.shutdown()
    val i2 = SimpleCRL(tdir, 3, 1024 * 1024)
    i2.appliedStubs shouldBe Set()
    readState(i2.crl, storeId2).transactions.size shouldBe 1
    i2.crl.shutdown()
  }
}
