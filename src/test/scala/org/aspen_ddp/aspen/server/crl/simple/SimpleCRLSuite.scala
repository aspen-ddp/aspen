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
import org.aspen_ddp.aspen.server.crl.{CrashRecoveryLog, TransactionRecoveryState}

import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.{Path, StandardOpenOption}
import java.util.UUID
import java.util.concurrent.LinkedBlockingQueue
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

  def streams(numStreams: Int): List[(StreamId, Path)] =
    var lst: List[(StreamId, Path)] = Nil
    for i <- 0 until numStreams do
      lst = (StreamId(i), tdir.toPath.resolve(f"$i.log")) :: lst
    lst.reverse

  private def saveAndWait(crl: CrashRecoveryLog, txid: TransactionId, trs: TransactionRecoveryState): Unit =
    val q = new LinkedBlockingQueue[String]()
    crl.save(txid, trs, () => q.put(""))
    q.take()

  test("Save & Recover CRL Saved State File Foo") {
    val savePath = tdir.toPath.resolve("crl_save_file.log")
    val queue = new LinkedBlockingQueue[String]()

    def completionHandler(): Unit = queue.put("")

    val i = SimpleCRL(tdir.toPath, 3, 1024 * 1024)

    i.crl.save(transactionId, trsValidTxd, completionHandler)

    queue.take() // Block till completion handlers are run

    i.crl.save(transactionId, trsValidTxd2, completionHandler)

    queue.take() // Block till completion handlers are run

    val trs1 = i.crl.getFullRecoveryState(storeId)

    assert(trs1.size == 1)

    val t2 = i.crl.getFullRecoveryState(storeId2)

    assert(t2.size == 1)

    val t = Await.result(i.crl.closeStore(storeId), Duration(5000, MILLISECONDS))

    val t3 = i.crl.getFullRecoveryState(storeId)

    assert(t3.size == 0)

    val t4 = i.crl.getFullRecoveryState(storeId2)

    assert(t4.size == 1)

    CrashRecoveryLog.saveStoreState(storeId, t, savePath)

    val (sid, trl) = CrashRecoveryLog.loadStoreState(savePath)

    assert(sid == storeId)
    assert(trl.length == 1)

    assert(trl.head == trsValidTxd)

    Await.result(i.crl.loadStore(sid, trl), Duration(5000, MILLISECONDS))

    val t5 = i.crl.getFullRecoveryState(storeId)

    assert(t5.size == 1)

    val t6 = i.crl.getFullRecoveryState(storeId2)

    assert(t6.size == 1)

    i.crl.shutdown()
  }

  test("SimpleCRL Drop Transaction Data") {
    val queue = new LinkedBlockingQueue[String]()

    def completionHandler(): Unit = queue.put("")

    val i = SimpleCRL(tdir.toPath, 3, 1024 * 1024)

    i.crl.save(transactionId, trsValidTxd, completionHandler)

    queue.take() // Block till completion handlers are run

    i.crl.dropTransactionObjectData(storeId, transactionId)
    i.crl.save(transactionId, trsValidTxd, completionHandler)
    queue.take()

    i.crl.shutdown()

    val i2 = SimpleCRL(tdir.toPath, 3, 1024 * 1024)

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

    val i = SimpleCRL(tdir.toPath, 3, 4096 * 3)

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

    i.crl.shutdown()

    val i2 = SimpleCRL(tdir.toPath, 3, 4096 * 3)

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

    val i = SimpleCRL(tdir.toPath, 3, 4096 * 3)

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

    i.crl.deleteTransaction(storeId, transactionId2)
    i.crl.save(transactionId, trsValidTxd, completionHandler)

    queue.take()
    assert(i.crl.currentStreamNumber == 2)

    i.crl.shutdown()

    val i2 = SimpleCRL(tdir.toPath, 3, 4096 * 3)

    i.crl.save(transactionId2, trsValidTxd, completionHandler)

    assert(i.crl.currentStreamNumber == 2)

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

    val i = SimpleCRL(tdir.toPath, 3, 4096*3)

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

    val i2 = SimpleCRL(tdir.toPath, 3, 4096*3)

    i.crl.save(transactionId2, trsValidTxd, completionHandler)

    assert(i.crl.currentStreamNumber == 1)

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

    val i = SimpleCRL(tdir.toPath, 3, 1024*1024)

    i.crl.save(transactionId, trsValidTxd, completionHandler)

    queue.take() // Block till completion handlers are run

    // There's a race condition here where the currentLogEntry variable is updated after
    // the completion callback is executed. It's possible for the take() operation to complete
    // before this happens which causes the currentEntrySerialNumber to still be zero when it
    // should be 1. Add a short sleep to reduce the likelihood of this happening
    Thread.sleep(50)
    
    assert(i.crl.currentEntrySerialNumber == 1)

    i.crl.shutdown()

    val i2 = SimpleCRL(tdir.toPath, 3, 1024*1024)

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

    val i = SimpleCRL(tdir.toPath, 3, 1024 * 1024)

    i.crl.save(transactionId, trsValidTxd, completionHandler)
    queue.take()

    i.crl.save(transactionId2, trsValidTxd2, completionHandler)
    queue.take()

    i.crl.deleteTransaction(storeId, transactionId)
    i.crl.save(transactionId2, trsValidTxd2, completionHandler)
    queue.take()

    i.crl.shutdown()

    val i2 = SimpleCRL(tdir.toPath, 3, 1024 * 1024)

    assert(i2.trsList.length == 1)
    assert(i2.trsList.head.storeId == storeId2)
    assert(i2.trsList.head.disposition == disp)
    assert(i2.trsList.head.serializedTxd == trsValidTxd2.serializedTxd)
    assert(i2.trsList.head.objectUpdates == trsValidTxd2.objectUpdates)
    assert(i2.trsList.head.paxosAcceptorState == pax)
  }

  // ===== Tier 1: Critical Crash Recovery Correctness =====

  test("Recovery after crash without clean shutdown") {
    val i = SimpleCRL(tdir.toPath, 3, 1024 * 1024)

    saveAndWait(i.crl, transactionId, trsValidTxd)
    saveAndWait(i.crl, transactionId2, trsValidTxd2)

    // Simulate crash: do NOT call shutdown
    val i2 = SimpleCRL(tdir.toPath, 3, 1024 * 1024)

    assert(i2.trsList.length == 2)
    val byStore = i2.trsList.groupBy(_.storeId)
    assert(byStore(storeId).head.serializedTxd == trsValidTxd.serializedTxd)
    assert(byStore(storeId).head.objectUpdates == trsValidTxd.objectUpdates)
    assert(byStore(storeId).head.paxosAcceptorState == pax)
    assert(byStore(storeId2).head.serializedTxd == trsValidTxd2.serializedTxd)

    i.crl.shutdown()
    i2.crl.shutdown()
  }

  test("Recovery preserves latest paxos state after multiple saves") {
    val i = SimpleCRL(tdir.toPath, 3, 1024 * 1024)

    saveAndWait(i.crl, transactionId, trsUndetermined)
    saveAndWait(i.crl, transactionId, trsPromised)
    saveAndWait(i.crl, transactionId, trsValidTxd)

    i.crl.shutdown()

    val i2 = SimpleCRL(tdir.toPath, 3, 1024 * 1024)

    assert(i2.trsList.length == 1)
    assert(i2.trsList.head.disposition == TransactionDisposition.VoteCommit)
    assert(i2.trsList.head.paxosAcceptorState.promised == Some(promise))
    assert(i2.trsList.head.paxosAcceptorState.accepted == Some((accept, true)))

    i2.crl.shutdown()
  }

  test("Recovery after stream recycling re-writes tx data from recycled stream") {
    val queue = new LinkedBlockingQueue[String]()
    def completionHandler(): Unit = queue.put("")

    val i = SimpleCRL(tdir.toPath, 3, 4096 * 3)

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
    val i2 = SimpleCRL(tdir.toPath, 3, 4096 * 3)

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

    val i = SimpleCRL(tdir.toPath, 3, 1024 * 1024)

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

    val i2 = SimpleCRL(tdir.toPath, 3, 1024 * 1024)

    assert(i2.trsList.length == 1)
    assert(i2.trsList.head.storeId == storeId)

    i2.crl.shutdown()
  }

  test("Delete newest transaction (queue head removal)") {
    val queue = new LinkedBlockingQueue[String]()
    def completionHandler(): Unit = queue.put("")

    val i = SimpleCRL(tdir.toPath, 3, 1024 * 1024)

    i.crl.save(transactionId, trsValidTxd, completionHandler)
    queue.take()
    i.crl.save(transactionId2, trsValidTxd2, completionHandler)
    queue.take()

    // Delete the newest (head of queue)
    i.crl.deleteTransaction(storeId2, transactionId2)
    i.crl.save(transactionId, trsValidTxd, completionHandler)
    queue.take()

    i.crl.shutdown()

    val i2 = SimpleCRL(tdir.toPath, 3, 1024 * 1024)

    assert(i2.trsList.length == 1)
    assert(i2.trsList.head.storeId == storeId)
    assert(i2.trsList.head.serializedTxd == trsValidTxd.serializedTxd)

    i2.crl.shutdown()
  }

  test("Delete middle transaction from queue of three") {
    val queue = new LinkedBlockingQueue[String]()
    def completionHandler(): Unit = queue.put("")

    val i = SimpleCRL(tdir.toPath, 3, 1024 * 1024)

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

    val i2 = SimpleCRL(tdir.toPath, 3, 1024 * 1024)

    assert(i2.trsList.length == 2)
    assert(i2.trsList.exists(_.storeId == storeId))
    assert(!i2.trsList.exists(_.storeId == storeId2))

    i2.crl.shutdown()
  }

  // ===== Tier 2: Important Functional Coverage =====

  test("Multiple independent transactions across different stores") {
    val queue = new LinkedBlockingQueue[String]()
    def completionHandler(): Unit = queue.put("")

    val i = SimpleCRL(tdir.toPath, 3, 1024 * 1024)

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

    val i2 = SimpleCRL(tdir.toPath, 3, 1024 * 1024)

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
    val i = SimpleCRL(tdir.toPath, 3, 1024 * 1024)

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

    val i = SimpleCRL(tdir.toPath, 3, 4096 * 3)

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

    val i2 = SimpleCRL(tdir.toPath, 3, 4096 * 3)

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

    val i = SimpleCRL(tdir.toPath, 3, 4096 * 3)

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

    val i2 = SimpleCRL(tdir.toPath, 3, 4096 * 3)

    assert(i2.trsList.length == 1)
    assert(i2.trsList.head.serializedTxd == trsValidTxd.serializedTxd)
    assert(i2.trsList.head.objectUpdates == List())

    i2.crl.shutdown()
  }

  test("Delete and re-save same transaction ID") {
    val queue = new LinkedBlockingQueue[String]()
    def completionHandler(): Unit = queue.put("")

    val i = SimpleCRL(tdir.toPath, 3, 1024 * 1024)

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

    val i2 = SimpleCRL(tdir.toPath, 3, 1024 * 1024)

    assert(i2.trsList.length == 2)
    assert(i2.trsList.exists(_.storeId == storeId))
    assert(i2.trsList.exists(_.storeId == storeId2))

    i2.crl.shutdown()
  }

  // ===== Tier 3: Edge Cases and Corruption =====

  test("Corrupted entry detected — recovery stops at last valid entry") {
    val queue = new LinkedBlockingQueue[String]()
    def completionHandler(): Unit = queue.put("")

    val i = SimpleCRL(tdir.toPath, 3, 1024 * 1024)

    i.crl.save(transactionId, trsValidTxd, completionHandler)
    queue.take()
    i.crl.save(transactionId2, trsValidTxd2, completionHandler)
    queue.take()

    i.crl.shutdown()

    // Corrupt the trailing UUID of the second entry in stream 0
    val streamFile = tdir.toPath.resolve("0.crl")
    val reader = StreamReader(StreamId(0), streamFile)
    val Some((entry1Start, _)) = reader.readEntry(0): @unchecked
    val Some((_, entry1Header)) = reader.readEntry(entry1Start): @unchecked
    val corruptOffset = entry1Start + entry1Header.trailingUUIDOffset

    val fc = FileChannel.open(streamFile, StandardOpenOption.WRITE)
    fc.position(corruptOffset)
    fc.write(ByteBuffer.wrap(Array.fill(16)(0xFF.toByte)))
    fc.close()

    // Recovery should only find the first (uncorrupted) entry
    val i2 = SimpleCRL(tdir.toPath, 3, 1024 * 1024)

    assert(i2.trsList.length == 1)
    assert(i2.trsList.head.storeId == storeId)
    assert(i2.trsList.head.serializedTxd == trsValidTxd.serializedTxd)

    i2.crl.shutdown()
  }

  test("Recovery from fresh empty stream files") {
    val i = SimpleCRL(tdir.toPath, 3, 1024 * 1024)
    i.crl.shutdown()

    val i2 = SimpleCRL(tdir.toPath, 3, 1024 * 1024)
    assert(i2.trsList.isEmpty)
    i2.crl.shutdown()
  }

  test("closeStore does not record log deletion") {
    val queue = new LinkedBlockingQueue[String]()
    def completionHandler(): Unit = queue.put("")

    val i = SimpleCRL(tdir.toPath, 3, 1024 * 1024)

    i.crl.save(transactionId, trsValidTxd, completionHandler)
    queue.take()
    i.crl.save(transactionId2, trsValidTxd2, completionHandler)
    queue.take()

    // closeStore removes from in-memory state but NOT from the log
    val closedTrs = Await.result(i.crl.closeStore(storeId), Duration(5000, MILLISECONDS))
    assert(closedTrs.length == 1)
    assert(closedTrs.head.storeId == storeId)

    assert(i.crl.getFullRecoveryState(storeId).isEmpty)
    assert(i.crl.getFullRecoveryState(storeId2).length == 1)

    // Flush to disk
    i.crl.save(transactionId2, trsValidTxd2, completionHandler)
    queue.take()

    i.crl.shutdown()

    // On recovery, tx1 reappears because closeStore never wrote a deletion to the log.
    // This is expected behavior: the migrate-out process must use deleteTransaction
    // or the store must be re-closed after recovery.
    val i2 = SimpleCRL(tdir.toPath, 3, 1024 * 1024)

    val store1Txs = i2.trsList.filter(_.storeId == storeId)
    val store2Txs = i2.trsList.filter(_.storeId == storeId2)
    assert(store1Txs.length == 1)
    assert(store2Txs.length == 1)

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

    val i = SimpleCRL(tdir.toPath, 3, 1024 * 1024)

    i.crl.save(transactionId, trsValidTxd, () => queue.put("a"))
    i.crl.save(transactionId2, trsValidTxd2, () => queue.put("b"))
    i.crl.save(transactionId3, trsValidTxd3, () => queue.put("c"))

    queue.take()
    queue.take()
    queue.take()

    i.crl.shutdown()

    val i2 = SimpleCRL(tdir.toPath, 3, 1024 * 1024)

    assert(i2.trsList.length == 3)
    assert(i2.trsList.exists(_.storeId == storeId))
    assert(i2.trsList.exists(_.storeId == storeId2))

    i2.crl.shutdown()
  }

  test("Recovery spanning many entries with interleaved saves and deletes") {
    val queue = new LinkedBlockingQueue[String]()
    def completionHandler(): Unit = queue.put("")

    val i = SimpleCRL(tdir.toPath, 3, 1024 * 1024)

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

    val i2 = SimpleCRL(tdir.toPath, 3, 1024 * 1024)

    assert(i2.trsList.length == 2)
    assert(i2.trsList.exists(_.storeId == storeId2))
    assert(i2.trsList.exists(_.storeId == storeId3))
    assert(!i2.trsList.exists(t => t.storeId == storeId))

    i2.crl.shutdown()
  }
}
