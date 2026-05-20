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
import java.nio.file.Path
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



class SimpleCRLSuite extends FileBasedTests {
  import SimpleCRLSuite._

  def streams(numStreams: Int): List[(StreamId, Path)] =
    var lst: List[(StreamId, Path)] = Nil
    for i <- 0 until numStreams do
      lst = (StreamId(i), tdir.toPath.resolve(f"$i.log")) :: lst
    lst.reverse


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
}
