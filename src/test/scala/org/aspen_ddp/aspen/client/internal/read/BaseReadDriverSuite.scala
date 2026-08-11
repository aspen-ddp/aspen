package org.aspen_ddp.aspen.client.internal.read

import java.util.UUID
import org.aspen_ddp.aspen.client.{CorruptedObject, DataObjectState, InvalidObject, ReadDriverClient}
import org.aspen_ddp.aspen.common.network.{ClientId, ReadResponse}
import org.aspen_ddp.aspen.common.{DataBuffer, HLCTimestamp}
import org.aspen_ddp.aspen.common.ida.Replication
import org.aspen_ddp.aspen.common.objects.{DataObjectPointer, KeyValueObjectPointer, ObjectId, ObjectPointer, ObjectRefcount, ObjectRevision, ReadError}
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.transaction.{TransactionDescription, TransactionId}
import org.scalatest.funsuite.AsyncFunSuite
import org.scalatest.matchers.should.Matchers

import scala.concurrent.{Await, ExecutionContext}
import scala.concurrent.duration.*
import scala.language.implicitConversions

object BaseReadDriverSuite {
  val awaitDuration = Duration(100, MILLISECONDS)

  val objId = ObjectId(new UUID(0,1))
  val poolId = PoolId(new UUID(0,2))
  val readUUID = new UUID(0,3)
  val cliUUID = new UUID(0,4)

  val ida = Replication(3,2)

  val ds0 = StoreId(poolId, 0)
  val ds1 = StoreId(poolId, 1)
  val ds2 = StoreId(poolId, 2)

  val ptr = DataObjectPointer(objId, poolId)
  val kvptr = KeyValueObjectPointer(objId, poolId)
  val rev = ObjectRevision.Null
  val ref = ObjectRefcount(1,1)

  val odata = DataBuffer(List[Byte](1,2,3,4).toArray)

  val noLocks = Some(Map[StoreId, List[TransactionDescription]]())

  val client = ClientId(cliUUID)

}

class BaseReadDriverSuite  extends AsyncFunSuite with Matchers {
  import BaseReadDriverSuite._


  def mkReader(client: ReadDriverClient,
               objectPointer: ObjectPointer = ptr,
               readUUID:UUID = readUUID,
               comment: String = "",
               disableOpportunisticRebuild: Boolean = false) = {
    new BaseReadDriver(client, objectPointer, ida, readUUID, comment, disableOpportunisticRebuild) {
      given ec: ExecutionContext = this.client.clientContext
    }
  }

  test("Fail with invalid object") {
    val m = ReadDriverClient.NoOp(client)
    val r = mkReader(m)
    val nrev = ObjectRevision(TransactionId(new UUID(0,1)))
    val nrev2 = ObjectRevision(TransactionId(new UUID(0,2)))

    val ts = HLCTimestamp.now
    val readTime = HLCTimestamp(ts.asLong - 100)

    r.receiveReadResponse(ReadResponse(client, ds0, readUUID, readTime, Right(ReadResponse.CurrentState(rev, ref, ts, 5, Some(odata), Set()))))
    r.readResult.isCompleted should be (false)
    r.receiveReadResponse(ReadResponse(client, ds1, readUUID, readTime, Left(ReadError.ObjectNotFound)))
    r.readResult.isCompleted should be (false)
    r.receiveReadResponse(ReadResponse(client, ds2, readUUID, readTime, Left(ReadError.ObjectMismatch)))

    r.readResult.isCompleted should be (true)

    recoverToSucceededIf[InvalidObject] {
      r.readResult
    }
  }

  test("Fail with corrupted object") {
    val m = ReadDriverClient.NoOp(client)
    val r = mkReader(m)
    val nrev = ObjectRevision(TransactionId(new UUID(0,1)))
    val nrev2 = ObjectRevision(TransactionId(new UUID(0,2)))

    val ts = HLCTimestamp.now

    r.receiveReadResponse(ReadResponse(client, ds0, readUUID, ts, Right(ReadResponse.CurrentState(rev, ref, ts, 5, Some(odata), Set()))))
    r.readResult.isCompleted should be (false)
    r.receiveReadResponse(ReadResponse(client, ds1, readUUID, ts, Left(ReadError.CorruptedObject)))
    r.readResult.isCompleted should be (false)
    r.receiveReadResponse(ReadResponse(client, ds2, readUUID, ts, Left(ReadError.CorruptedObject)))

    r.readResult.isCompleted should be (true)

    recoverToSucceededIf[CorruptedObject] {
      r.readResult
    }
  }

  test("Succeed with errors") {
    val m = ReadDriverClient.NoOp(client)
    val r = mkReader(m)
    val nrev = ObjectRevision(TransactionId(new UUID(0,1)))
    val nrev2 = ObjectRevision(TransactionId(new UUID(0,2)))
    val ts = HLCTimestamp.now
    val readTime = HLCTimestamp(ts.asLong - 100)

    r.receiveReadResponse(ReadResponse(client, ds0, readUUID, readTime, Right(ReadResponse.CurrentState(rev, ref, ts, 5, Some(odata), Set()))))
    r.readResult.isCompleted should be (false)
    r.receiveReadResponse(ReadResponse(client, ds1, readUUID, readTime, Left(ReadError.ObjectNotFound)))
    r.readResult.isCompleted should be (false)
    r.receiveReadResponse(ReadResponse(client, ds2, readUUID, readTime, Right(ReadResponse.CurrentState(nrev2, ref, ts, 5, Some(odata), Set()))))
    r.readResult.isCompleted should be (false)
    r.receiveReadResponse(ReadResponse(client, ds0, readUUID, readTime, Right(ReadResponse.CurrentState(nrev2, ref, ts, 5, Some(odata), Set()))))
    r.readResult.isCompleted should be (true)
    val o = Await.result(r.readResult, awaitDuration)

    //    o match {
    //      case Left(_) =>
    //      case Right((ds:DataObjectState, o)) =>
    //        println(s"ptr(${ds.pointer}), rev(${ds.revision}), ref(${ds.refcount}), ts(${ds.timestamp}), data(${com.ibm.aspen.util.db2string(ds.data)})")
    //        println(s"ptr(${ptr}), rev(${nrev2}), ref(${ref}), ts(${ts}), data(${com.ibm.aspen.util.db2string(odata)})")
    //    }

    o should be (DataObjectState(ptr, nrev2, ref, ts, readTime, ida, 5, odata))
  }

  test("Ignore old revisions") {
    val m = ReadDriverClient.NoOp(client)
    val r = mkReader(m)
    val nrev = ObjectRevision(TransactionId(new UUID(0,1)))
    val nrev2 = ObjectRevision(TransactionId(new UUID(0,2)))
    val ts = HLCTimestamp.now

    r.receiveReadResponse(ReadResponse(client, ds0, readUUID, ts, Right(ReadResponse.CurrentState(rev,   ref, ts, 5, Some(odata), Set()))))
    r.readResult.isCompleted should be (false)
    r.receiveReadResponse(ReadResponse(client, ds1, readUUID, ts, Right(ReadResponse.CurrentState(nrev,  ref, ts, 5, Some(odata), Set()))))
    r.readResult.isCompleted should be (false)
    r.receiveReadResponse(ReadResponse(client, ds2, readUUID, ts, Right(ReadResponse.CurrentState(nrev2, ref, ts, 5, Some(odata), Set()))))
    r.readResult.isCompleted should be (false)
    r.receiveReadResponse(ReadResponse(client, ds0, readUUID, ts, Right(ReadResponse.CurrentState(nrev2, ref, ts, 5, Some(odata), Set()))))
    r.readResult.isCompleted should be (true)
    val o = Await.result(r.readResult, awaitDuration)

    o should be (DataObjectState(ptr, nrev2, ref, ts, ts, ida, 5, odata))
  }

  test("Use minimum readTime") {
    val m = ReadDriverClient.NoOp(client)
    val r = mkReader(m)
    val nrev = ObjectRevision(TransactionId(new UUID(0,1)))
    val nrev2 = ObjectRevision(TransactionId(new UUID(0,2)))
    val ts = HLCTimestamp.now

    val minTs = HLCTimestamp(ts.asLong-100)

    r.receiveReadResponse(ReadResponse(client, ds0, readUUID, ts, Right(ReadResponse.CurrentState(rev,   ref, ts, 5, Some(odata), Set()))))
    r.readResult.isCompleted should be (false)
    r.receiveReadResponse(ReadResponse(client, ds1, readUUID, ts, Right(ReadResponse.CurrentState(nrev,  ref, ts, 5, Some(odata), Set()))))
    r.readResult.isCompleted should be (false)
    r.receiveReadResponse(ReadResponse(client, ds2, readUUID, minTs, Right(ReadResponse.CurrentState(nrev2, ref, ts, 5, Some(odata), Set()))))
    r.readResult.isCompleted should be (false)
    r.receiveReadResponse(ReadResponse(client, ds0, readUUID, ts, Right(ReadResponse.CurrentState(nrev2, ref, ts, 5, Some(odata), Set()))))
    r.readResult.isCompleted should be (true)
    val o = Await.result(r.readResult, awaitDuration)

    o should be (DataObjectState(ptr, nrev2, ref, ts, minTs, ida, 5, odata))
  }


  test("Successful read with data and locks") {
    val m = ReadDriverClient.NoOp(client)
    val r = mkReader(m)
    val ts = HLCTimestamp.now

    r.receiveReadResponse(ReadResponse(client, ds0, readUUID, ts, Right(ReadResponse.CurrentState(rev, ref, ts, 5, Some(odata), Set()))))
    r.readResult.isCompleted should be (false)
    r.receiveReadResponse(ReadResponse(client, ds1, readUUID, ts, Right(ReadResponse.CurrentState(rev, ref, ts, 5, Some(odata), Set()))))
    r.readResult.isCompleted should be (true)
    val o = Await.result(r.readResult, awaitDuration)

    o should be (DataObjectState(ptr, rev, ref, ts, ts, ida, 5, odata))
  }

}
