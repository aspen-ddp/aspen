package org.aspen_ddp.aspen.client.internal.read

import java.util.UUID

import org.aspen_ddp.aspen.client.{CorruptedObject, InvalidObject, ObjectState}
import org.aspen_ddp.aspen.common.HLCTimestamp
import org.aspen_ddp.aspen.common.ida.Replication
import org.aspen_ddp.aspen.common.network.{ClientId, ReadResponse}
import org.aspen_ddp.aspen.common.objects.{DataObjectPointer, ObjectId, ObjectRefcount, ObjectRevision, ReadError}
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.transaction.TransactionId
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

object DataObjectReaderSuite {
  
  class TestReader(pointer: DataObjectPointer)
    extends BaseObjectReader[DataObjectPointer, DataObjectStoreState](false, pointer, new UUID(0,0)) {

    var rstates: Option[List[StoreState]] = None

    override protected def createObjectState(storeId:StoreId, readTime: HLCTimestamp, cs: ReadResponse.CurrentState): DataObjectStoreState = {
      new DataObjectStoreState(storeId, cs.revision, cs.refcount, cs.timestamp, readTime, cs.sizeOnStore, cs.objectData)
    }

    override protected def restoreObject(revision:ObjectRevision, refcount: ObjectRefcount, timestamp:HLCTimestamp,
                                         readTime: HLCTimestamp, matchingStoreStates: List[DataObjectStoreState],
                                         allStoreStates: List[DataObjectStoreState], debug: Boolean): ObjectState = {
      rstates = Some(matchingStoreStates)
      null
    }
  }

  object TestReader {
    def apply(width: Int, threshold: Int): TestReader = {

      val ida = Replication(width, threshold)

      new TestReader(DataObjectPointer(ObjectId(objUUID), pool, None, ida, Array()))
    }
  }

  val pool = PoolId(new UUID(0,1))
  val readUUID = new UUID(1,2)
  val objUUID = new UUID(1,3)
  val cliUUID = new UUID(0,4)
  val client = ClientId(cliUUID)

  val s0 = StoreId(pool, 0)
  val s1 = StoreId(pool, 1)
  val s2 = StoreId(pool, 2)
  val s3 = StoreId(pool, 3)
  val s4 = StoreId(pool, 4)

  val r0 = ObjectRevision(TransactionId(new UUID(0,0)))
  val r1 = ObjectRevision(TransactionId(new UUID(0,1)))
  val r2 = ObjectRevision(TransactionId(new UUID(0,2)))

  val t0 = HLCTimestamp(1)
  val t1 = HLCTimestamp(2)
  val t3 = HLCTimestamp(3)

  def err(store: Int, e: ReadError.Value): ReadResponse = {
    ReadResponse(client, StoreId(pool, store.asInstanceOf[Byte]), readUUID, HLCTimestamp.Zero, Left(e))
  }

  def ok(store: Int, rev: ObjectRevision, ts: HLCTimestamp): ReadResponse = {
    val cs = ReadResponse.CurrentState(rev, ObjectRefcount(0,0), ts, 0, None, Set())
    ReadResponse(client, StoreId(pool, store.asInstanceOf[Byte]), readUUID, HLCTimestamp.Zero, Right(cs))
  }


}

class DataObjectReaderSuite extends AnyFunSuite with Matchers {
  import DataObjectReaderSuite._
  /*
  final case class ReadResponse(
      fromStore: StoreId,
      readUUID: UUID,
      readTime: HLCTimestamp,
      result: Either[ReadError.Value, ReadResponse.CurrentState]) extends Message

  object ReadError extends Enumeration {
    val InvalidLocalPointer = Value("InvalidLocalPointer")
    val ObjectMismatch = Value("ObjectMismatch")
    val CorruptedObject = Value("CorruptedObject")

  object ReadResponse {
    case class CurrentState(
        revision: ObjectRevision,
        refcount: ObjectRefcount,
        timestamp: HLCTimestamp,
        sizeOnStore: Int,
        objectData: Option[DataBuffer],
        lockedWriteTransactions: Set[UUID])

        */
  test("Simple Success") {
    val r = TestReader(3,2)
    r.receiveReadResponse(ok(0, r0, t0))
    r.rstates should be (None)
    r.receiveReadResponse(ok(0, r0, t0))
    r.rstates should be (None)
    r.receiveReadResponse(ok(1, r0, t0))
    r.rstates.nonEmpty should be (true)
  }

  test("Simple InvalidLocalPointer Failure") {
    val r = TestReader(3,2)
    r.receiveReadResponse(ok(0, r0, t0))
    r.result should be (None)
    r.receiveReadResponse(err(1, ReadError.ObjectNotFound))
    r.result should be (None)
    r.receiveReadResponse(err(2, ReadError.ObjectNotFound))

    r.result match {
      case Some(Left(err: InvalidObject)) => err.pointer should be (r.pointer)
      case _ => fail("bah")
    }
  }

  test("Simple Mismatch Failure") {
    val r = TestReader(3,2)
    r.receiveReadResponse(ok(0, r0, t0))
    r.result should be (None)
    r.receiveReadResponse(err(1, ReadError.ObjectMismatch))
    r.result should be (None)
    r.receiveReadResponse(err(2, ReadError.ObjectMismatch))

    r.result match {
      case Some(Left(err: InvalidObject)) => err.pointer should be (r.pointer)
      case _ => fail("bah")
    }
  }

  test("Simple Corruption Failure") {
    val r = TestReader(3,2)
    r.receiveReadResponse(ok(0, r0, t0))
    r.result should be (None)
    r.receiveReadResponse(err(1, ReadError.CorruptedObject))
    r.result should be (None)
    r.receiveReadResponse(err(2, ReadError.CorruptedObject))

    r.result match {
      case Some(Left(err: CorruptedObject)) => err.pointer should be (r.pointer)
      case _ => fail("bah")
    }
  }

  test("Mixed error response failure") {
    val r = TestReader(3,2)
    r.receiveReadResponse(ok(0, r0, t0))
    r.result should be (None)
    r.receiveReadResponse(err(1, ReadError.ObjectMismatch))
    r.result should be (None)
    r.receiveReadResponse(err(2, ReadError.ObjectNotFound))

    r.result match {
      case Some(Left(err: InvalidObject)) => err.pointer should be (r.pointer)
      case _ => fail("bah")
    }
  }

//  test("Use highest revision") {
//    val r = TestReader(5,3)
//    r.receiveReadResponse(ok(0, r0, t0))
//    r.rstates should be (None)
//    r.receiveReadResponse(ok(1, r0, t0))
//    r.rstates should be (None)
//    r.receiveReadResponse(ok(2, r1, t1))
//    r.rstates should be (None)
//    r.rereadCandidates.keySet should be (Set(s0,s1))
//    //rereads should be (Set(0,1))
//    r.receiveReadResponse(ok(3, r0, t0))
//    r.rstates should be (None)
//    r.rereadCandidates.keySet should be (Set(s0,s1,s3))
//    r.receiveReadResponse(ok(0, r1, t1))
//    r.rstates should be (None)
//    r.receiveReadResponse(ok(1, r1, t1))
//    r.rereadCandidates.keySet should be (Set(s3))
//    r.rstates.nonEmpty should be (true)
//  }

  test("Resolve with errors") {
    val r = TestReader(5,3)
    r.receiveReadResponse(err(0, ReadError.CorruptedObject))
    r.rstates should be (None)
    r.receiveReadResponse(ok(1, r0, t0))
    r.rstates should be (None)
    r.receiveReadResponse(ok(2, r0, t0))
    r.rstates should be (None)
    r.receiveReadResponse(err(3, ReadError.CorruptedObject))
    r.rstates should be (None)
    r.receiveReadResponse(ok(4, r0, t0))
    r.rstates.nonEmpty should be (true)
  }

  test("Rereads override previous state") {
    val r = TestReader(5,3)
    r.receiveReadResponse(ok(0, r0, t0))
    r.rstates should be (None)
    r.receiveReadResponse(ok(1, r0, t0))
    r.rstates should be (None)
    r.receiveReadResponse(ok(2, r1, t1))
    r.rstates should be (None)
    //rereads should be (Set(0,1))
    r.receiveReadResponse(ok(0, r1, t1))
    r.rstates should be (None)
    r.receiveReadResponse(ok(1, r1, t1))
    r.rstates.nonEmpty should be (true)
  }
}
