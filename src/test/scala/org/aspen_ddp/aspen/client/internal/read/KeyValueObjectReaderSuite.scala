package org.aspen_ddp.aspen.client.internal.read

import java.nio.charset.StandardCharsets
import java.util.UUID

import org.aspen_ddp.aspen.client.KeyValueObjectState
import org.aspen_ddp.aspen.client.KeyValueObjectState.ValueState
import org.aspen_ddp.aspen.common.HLCTimestamp
import org.aspen_ddp.aspen.common.ida.{IDA, Replication}
import org.aspen_ddp.aspen.common.network.{ClientId, ReadResponse}
import org.aspen_ddp.aspen.common.objects._
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.transaction.TransactionId
import org.aspen_ddp.aspen.server.store.{KVObjectState, ValueState => StoreValueState}
import org.scalatest.Assertion
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

object KeyValueObjectReaderSuite {
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

  val ida3 = Replication(3,2)
  val ida5 = Replication(5,3)

  val r0 = ObjectRevision(TransactionId(new UUID(0,0)))
  val r1 = ObjectRevision(TransactionId(new UUID(0,1)))
  val r2 = ObjectRevision(TransactionId(new UUID(0,2)))

  val t0 = HLCTimestamp(1)
  val t1 = HLCTimestamp(2)
  val t2 = HLCTimestamp(3)

  val v0: (ObjectRevision, HLCTimestamp) = (r0, t0)
  val v1: (ObjectRevision, HLCTimestamp) = (r1, t1)
  val v2: (ObjectRevision, HLCTimestamp) = (r2, t2)

  val foo: Value = Value("foo".getBytes(StandardCharsets.UTF_8))
  val bar: Value = Value("bar".getBytes(StandardCharsets.UTF_8))
  val baz: Value = Value("baz".getBytes(StandardCharsets.UTF_8))

  val kfoo = Key("foo")
  val kbar = Key("bar")
  val kbaz = Key("baz")

  sealed abstract class KVOp
  case class Insert(key: Key, value:Array[Byte], timestamp:Option[HLCTimestamp], revision:Option[ObjectRevision]) extends KVOp
  case class SetMin(value: Key, timestamp:Option[HLCTimestamp], revision:Option[ObjectRevision]) extends KVOp
  case class SetMax(value: Key, timestamp:Option[HLCTimestamp], revision:Option[ObjectRevision]) extends KVOp
  case class SetLeft(value: Array[Byte], timestamp:Option[HLCTimestamp], revision:Option[ObjectRevision]) extends KVOp
  case class SetRight(value: Array[Byte], timestamp:Option[HLCTimestamp], revision:Option[ObjectRevision]) extends KVOp

  val ka = Key("foo")
  val a0 = Insert(ka, foo.bytes, Some(t0), Some(r0))
  val a1 = Insert(ka, bar.bytes, Some(t1), Some(r1))
  val a2 = Insert(ka, baz.bytes, Some(t2), Some(r2))

  val kb = Key("bar")
  val b0 = Insert(kb, foo.bytes, Some(t0), Some(r0))
  val b1 = Insert(kb, bar.bytes, Some(t1), Some(r1))
  val b2 = Insert(kb, baz.bytes, Some(t2), Some(r2))

  val kc = Key("baz")
  val c0 = Insert(kc, foo.bytes, Some(t0), Some(r0))
  val c1 = Insert(kc, bar.bytes, Some(t1), Some(r1))
  val c2 = Insert(kc, baz.bytes, Some(t2), Some(r2))

  val min0 = SetMin(kfoo, Some(t0), Some(r0))
  val min1 = SetMin(kbar, Some(t1), Some(r1))
  val min2 = SetMin(kbaz, Some(t2), Some(r2))

  val max0 = SetMax(kfoo, Some(t0), Some(r0))
  val max1 = SetMax(kbar, Some(t1), Some(r1))
  val max2 = SetMax(kbaz, Some(t2), Some(r2))

  val left0 = SetLeft(foo.bytes, Some(t0), Some(r0))
  val left1 = SetLeft(bar.bytes, Some(t1), Some(r1))
  val left2 = SetLeft(baz.bytes, Some(t2), Some(r2))

  val right0 = SetRight(foo.bytes, Some(t0), Some(r0))
  val right1 = SetRight(bar.bytes, Some(t1), Some(r1))
  val right2 = SetRight(baz.bytes, Some(t2), Some(r2))

  class TestReader(val ida: IDA) extends KeyValueObjectReader(
    false, KeyValueObjectPointer(ObjectId(objUUID), pool, None, ida, Array()), new UUID(0,0)) {

    def err(store: Int, e: ReadError.Value): Unit = {
      receiveReadResponse(ReadResponse(client, StoreId(pool, store.asInstanceOf[Byte]), this.readUUID, HLCTimestamp.Zero, Left(e)))
    }

    def ok(store: Int,
           oversion: (ObjectRevision, HLCTimestamp),
           locks: Set[ObjectRevision],
           ops: KVOp*): Unit = {

      var min: Option[Key] = None
      var max: Option[Key] = None
      var left: Option[Value] = None
      var right: Option[Value] = None
      var contents: Map[Key, StoreValueState] = Map()

      ops.foreach {
        case o: SetMin => min = Some(o.value)
        case o: SetMax => max = Some(o.value)
        case o: SetLeft => left = Some(Value(o.value))
        case o: SetRight => right = Some(Value(o.value))
        case o: Insert => contents += o.key -> new StoreValueState(Value(o.value), o.revision.get, o.timestamp.get, None)
      }

      val odata = if (ops.isEmpty) None else {
        Some(KVObjectState.encode(min, max, left, right, contents))
      }

      val cs = ReadResponse.CurrentState(
        oversion._1,
        ObjectRefcount(0, 0),
        oversion._2,
        odata.map(_.size).getOrElse(0),
        odata,
        locks.map(r => TransactionId(r.lastUpdateTxUUID)))

      receiveReadResponse(ReadResponse(client, StoreId(pool, store.asInstanceOf[Byte]), this.readUUID, HLCTimestamp.Zero, Right(cs)))
    }
  }

  object TestReader {
    def apply(ida: IDA): TestReader = new TestReader(ida)
  }
}

class KeyValueObjectReaderSuite extends AnyFunSuite with Matchers {
  import KeyValueObjectReaderSuite._

  test("Resolve empty object") {
    val r = TestReader(ida3)
    r.ok(0, v0, Set())
    r.result should be (None)
    r.ok(1, v0, Set())
    r.result should not be (None)
    r.rereadCandidates.keySet should be (Set())
  }

  test("Resolve single kv pair, simple") {
    val r = TestReader(ida3)
    r.ok(0, v0, Set(), a0)
    r.result should be (None)
    r.ok(1, v0, Set(), a0)
    r.result should not be (None)
    r.rereadCandidates.keySet should be (Set())
    r.result.get match {
      case Left(_) => fail()
      case Right(os) => os.asInstanceOf[KeyValueObjectState].contents.head._2 should be (ValueState(foo, r0, t0))
    }
  }

  test("Resolve single kv pair, upreved") {
    val r = TestReader(ida3)
    r.ok(0, v0, Set(), a0)
    r.result should be (None)
    r.rereadCandidates.keySet should be (Set())
    r.ok(1, v1, Set(), a1)
    r.result should be (None)
    r.rereadCandidates.keySet should be (Set(s0))
    r.ok(2, v1, Set(), a1)
    r.result should not be (None)
    r.rereadCandidates.keySet should be (Set(s0))
    r.result.get match {
      case Left(_) => fail()
      case Right(os) => os.asInstanceOf[KeyValueObjectState].contents.head._2 should be (ValueState(bar, r1, t1))
    }
  }

  test("Resolve multiple kv pair, simple") {
    val r = TestReader(ida3)
    r.ok(0, v0, Set(), a0, b0)
    r.result should be (None)
    r.ok(1, v0, Set(), b0, a0)
    r.result should not be (None)
    r.rereadCandidates.keySet should be (Set())
    r.result.get match {
      case Left(_) => fail()
      case Right(os) =>
        val c = os.asInstanceOf[KeyValueObjectState].contents
        c(ka) should be (ValueState(foo, r0, t0))
        c(kb) should be (ValueState(foo, r0, t0))
    }
  }

  test("Resolve three kv pair, simple") {
    val r = TestReader(ida3)
    r.ok(0, v0, Set(), a0, b0, c1)
    r.result should be (None)
    r.ok(1, v0, Set(), c1, b0, a0)
    r.result should not be (None)
    r.rereadCandidates.keySet should be (Set())
    r.result.get match {
      case Left(_) => fail()
      case Right(os) =>
        val c = os.asInstanceOf[KeyValueObjectState].contents
        c(ka) should be (ValueState(foo, r0, t0))
        c(kb) should be (ValueState(foo, r0, t0))
        c(kc) should be (ValueState(bar, r1, t1))
    }
  }

  test("Resolve three kv pair, intermixed and partial") {
    val r = TestReader(ida3)
    r.ok(0, v1, Set(), a0, b0)
    r.result should be (None)
    r.ok(1, v0, Set(), c0, b0)
    r.result should be (None)
    r.ok(2, v1, Set(), a0, b0, c0)
    r.result should not be (None)
    r.rereadCandidates.keySet should be (Set(s1))
    r.result.get match {
      case Left(_) => fail()
      case Right(os) =>
        val c = os.asInstanceOf[KeyValueObjectState].contents
        c(ka) should be (ValueState(foo, r0, t0))
        c(kb) should be (ValueState(foo, r0, t0))
        c(kc) should be (ValueState(foo, r0, t0))
    }
  }

  test("Resolve multiple kv pair, upreved intermixed") {
    val r = TestReader(ida3)
    r.ok(0, v0, Set(), a0, b1)
    r.result should be (None)
    r.rereadCandidates.keySet should be (Set())
    r.ok(1, v0, Set(), a1, b0)
    r.result should be (None)
    r.rereadCandidates.keySet.subsetOf(Set(s0, s1)) should be (true)
    r.ok(2, v0, Set(), a1, b1)
    r.result should not be (None)
    r.rereadCandidates.keySet should be (Set(s0, s1))
    r.result.get match {
      case Left(_) => fail()
      case Right(os) =>
        val c = os.asInstanceOf[KeyValueObjectState].contents
        c(ka) should be (ValueState(bar, r1, t1))
        c(kb) should be (ValueState(bar, r1, t1))
    }
  }

  test("Resolve multiple kv pair, upreved intermixed 5-way") {
    val r = TestReader(ida5)
    r.ok(0, v0, Set(), a0, b1, c0)
    r.result should be (None)
    r.ok(1, v0, Set(), a1, b0, c1)
    r.result should be (None)
    r.ok(2, v0, Set(), a1, b1, c2)
    r.result should be (None)
    r.ok(3, v0, Set(), a1, b0, c2)
    r.result should be (None)
    r.ok(4, v0, Set(), b1, c2)
    r.result should not be (None)
    r.rereadCandidates.keySet should be (Set(s0, s1, s3))
    r.result.get match {
      case Left(_) => fail()
      case Right(os) =>
        val c = os.asInstanceOf[KeyValueObjectState].contents
        c(ka) should be (ValueState(bar, r1, t1))
        c(kb) should be (ValueState(bar, r1, t1))
        c(kc) should be (ValueState(baz, r2, t2))
    }
  }

  test("Resolve multiple kv pair, upreved intermixed 5-way with deletion") {
    val r = TestReader(ida5)
    r.ok(0, v0, Set(), a0, c0)
    r.result should be (None)
    r.ok(1, v0, Set(), a1, c1)
    r.result should be (None)
    r.ok(2, v0, Set(), a1, b1, c2)
    r.result should be (None)
    r.ok(3, v0, Set(), a1, b0, c2)
    r.result should be (None)
    r.ok(4, v0, Set(), c2)
    r.result should not be (None)
    r.result.get match {
      case Left(_) => fail()
      case Right(os) =>
        val c = os.asInstanceOf[KeyValueObjectState].contents
        c.size should be (2)
        c(ka) should be (ValueState(bar, r1, t1))
        c(kc) should be (ValueState(baz, r2, t2))
    }
  }

  test("Resolve multiple kv pair, upreved intermixed 5-way with deletion and min/max") {
    val r = TestReader(ida5)
    r.ok(0, v0, Set(), a0, c0, max1)
    r.result should be (None)
    r.ok(1, v0, Set(), a1, c1, min0, max1)
    r.result should be (None)
    r.ok(2, v0, Set(), a1, b1, c2, min0, max0)
    r.result should be (None)
    r.ok(3, v0, Set(), a1, b0, c2, min0)
    r.result should be (None)
    r.ok(4, v0, Set(), c2, max1)
    r.result should not be (None)
    r.result.get match {
      case Left(_) => fail()
      case Right(os) =>
        val kvoss = os.asInstanceOf[KeyValueObjectState]
        val c = kvoss.contents
        c.size should be (2)
        c(ka) should be (ValueState(bar, r1, t1))
        c(kc) should be (ValueState(baz, r2, t2))
        kvoss.minimum should be (Some(kfoo))
        kvoss.maximum should be (Some(kbar))

    }
  }

  def tdeleted(op0: KVOp): Assertion = {
    val r = TestReader(ida5)
    r.ok(0, v0, Set())
    r.result should be (None)
    r.ok(1, v0, Set(), op0)
    r.result should be (None)
    r.ok(2, v0, Set())
    r.result should be (None)
    r.ok(3, v0, Set())
    r.result should not be (None)
  }

  test("Resolve deleted/non-restorable kv pair") {
    tdeleted(a0)
  }

  def tdeletedWithLock(op0: KVOp): Assertion = {
    val r = TestReader(ida5)
    r.ok(0, v0, Set(r0))
    r.result should be (None)
    r.ok(1, v0, Set(), op0)
    r.result should be (None)
    r.ok(2, v0, Set())
    r.result should be (None)
    r.ok(3, v0, Set())
    r.result should be (None)
    r.ok(0, v0, Set()) // discard lock
    r.result should not be (None)
  }

  test("Resolve deleted/non-restorabler kv pair with lock") {
    tdeletedWithLock(a0)
  }


}
