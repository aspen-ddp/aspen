package org.aspen_ddp.aspen.server.store

import java.util.UUID
import org.aspen_ddp.aspen.common.HLCTimestamp
import org.aspen_ddp.aspen.common.objects.{FullObject, IntegerKeyOrdering, Key, KeyRange, KeySet, ObjectRevision, ObjectType, Value}
import org.aspen_ddp.aspen.common.transaction.TransactionId
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

object PartialReadSuite {
  def k(n: Int): Key = Key(Array[Byte](n.toByte))

  val rev = ObjectRevision(TransactionId(new UUID(0, 1)))

  val min = k(0)
  val max = k(100)
  val left = Value(Array[Byte](7, 7))
  val right = Value(Array[Byte](8, 8))

  val contents: Map[Key, ValueState] = (1 to 5).map { n =>
    k(n) -> new ValueState(Value(Array[Byte](n.toByte, 0)), rev, HLCTimestamp(n.toLong), None)
  }.toMap

  val data = KVObjectState.encode(Some(min), Some(max), Some(left), Some(right), contents)

  def keysOf(kvos: KVObjectState): Set[Int] = kvos.content.keySet.map(_.bytes(0).toInt)

  def assertBoundsKept(kvos: KVObjectState): Unit =
    assert(kvos.min.get.bytes.sameElements(min.bytes))
    assert(kvos.max.get.bytes.sameElements(max.bytes))
    assert(kvos.left.get.bytes.sameElements(left.bytes))
    assert(kvos.right.get.bytes.sameElements(right.bytes))
}

class PartialReadSuite extends AnyFunSuite with Matchers {
  import PartialReadSuite._

  test("KeySet keeps only requested keys") {
    val r = KVObjectState.decode(PartialRead.filter(ObjectType.KeyValue, data, KeySet(List(k(2), k(4), k(9)))))
    keysOf(r) shouldBe Set(2, 4)
    r.content(k(2)).value.bytes shouldBe Array[Byte](2, 0)
    assertBoundsKept(r)
  }

  test("empty KeySet keeps no keys") {
    val r = KVObjectState.decode(PartialRead.filter(ObjectType.KeyValue, data, KeySet(Nil)))
    keysOf(r) shouldBe Set()
    assertBoundsKept(r)
  }

  test("KeyRange is inclusive at both ends") {
    val r = KVObjectState.decode(PartialRead.filter(ObjectType.KeyValue, data, KeyRange(k(2), k(4), IntegerKeyOrdering)))
    keysOf(r) shouldBe Set(2, 3, 4)
    assertBoundsKept(r)
  }

  test("KeyRange with min > max is empty") {
    val r = KVObjectState.decode(PartialRead.filter(ObjectType.KeyValue, data, KeyRange(k(4), k(2), IntegerKeyOrdering)))
    keysOf(r) shouldBe Set()
  }

  test("non-partial read types and Data objects are unchanged") {
    PartialRead.filter(ObjectType.KeyValue, data, FullObject()).getByteArray shouldBe data.getByteArray
    PartialRead.filter(ObjectType.Data, data, KeySet(Nil)).getByteArray shouldBe data.getByteArray
  }
}
