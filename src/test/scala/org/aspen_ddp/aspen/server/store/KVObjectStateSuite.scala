package org.aspen_ddp.aspen.server.store

import java.util.UUID

import org.aspen_ddp.aspen.common.HLCTimestamp
import org.aspen_ddp.aspen.common.objects.{Key, ObjectRevision, Value}
import org.aspen_ddp.aspen.common.transaction.TransactionId
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.language.implicitConversions

class KVObjectStateSuite extends AnyFunSuite with Matchers {
  test("Empty Object") {
    val kv = new KVObjectState(None, None, None, None, Map(), Map())
    val db = kv.encode()
    val kv2 = KVObjectState(db)
    assert(kv2.min == kv.min)
    assert(kv2.max == kv.max)
    assert(kv2.right == kv.right)
    assert(kv2.left == kv.left)
    assert(kv2.content == kv.content)
  }

  test("Fully populated Object") {
    val kmin = Key(Array[Byte](1,2))
    val kmax = Key(Array[Byte](3,4,5))
    val vleft = Value(Array[Byte](1,2))
    val vright = Value(Array[Byte]())

    val k1 = Key(Array[Byte]())
    val v1 = new ValueState(Value(Array[Byte]()), ObjectRevision(TransactionId(new UUID(1,2))),
      HLCTimestamp(2), None)

    val k2 = Key(Array[Byte](1))
    val v2 = new ValueState(Value(Array[Byte](2)), ObjectRevision(TransactionId(new UUID(1,2))),
      HLCTimestamp(2), None)

    val content = Map(k1->v1, k2->v2)

    val kv = new KVObjectState(Some(kmin), Some(kmax), Some(vleft), Some(vright), content, Map())
    val db = kv.encode()
    val kv2 = KVObjectState(db)

    assert(java.util.Arrays.equals(kv2.min.get.bytes, kv.min.get.bytes))
    assert(java.util.Arrays.equals(kv2.max.get.bytes, kv.max.get.bytes))
    assert(java.util.Arrays.equals(kv2.left.get.bytes, kv.left.get.bytes))
    assert(java.util.Arrays.equals(kv2.right.get.bytes, kv.right.get.bytes))

    assert(kv2.content.size == 2)
    val dv1 = kv2.content(k1)
    val dv2 = kv2.content(k2)

    assert(dv1.timestamp == v1.timestamp)
    assert(dv1.revision == v1.revision)
    assert(java.util.Arrays.equals(dv1.value.bytes, v1.value.bytes))

    assert(dv2.timestamp == v2.timestamp)
    assert(dv2.revision == v2.revision)
    assert(java.util.Arrays.equals(dv2.value.bytes, v2.value.bytes))
  }

  test("Partially populated Object") {
    val kmin = Key(Array[Byte](1,2))
    val vright = Value(Array[Byte]())

    val k1 = Key(Array[Byte]())
    val v1 = new ValueState(Value(Array[Byte]()), ObjectRevision(TransactionId(new UUID(1,2))),
      HLCTimestamp(2), None)

    val content = Map(k1->v1)

    val kv = new KVObjectState(Some(kmin), None, None, Some(vright), content, Map())
    val db = kv.encode()
    val kv2 = KVObjectState(db)

    assert(java.util.Arrays.equals(kv2.min.get.bytes, kv.min.get.bytes))
    assert(kv2.max.isEmpty)
    assert(kv2.left.isEmpty)
    assert(java.util.Arrays.equals(kv2.right.get.bytes, kv.right.get.bytes))

    assert(kv2.content.size == 1)
    val dv1 = kv2.content(k1)

    assert(dv1.timestamp == v1.timestamp)
    assert(dv1.revision == v1.revision)
    assert(java.util.Arrays.equals(dv1.value.bytes, v1.value.bytes))
  }
}
