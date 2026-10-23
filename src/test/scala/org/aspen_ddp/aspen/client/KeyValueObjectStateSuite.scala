package org.aspen_ddp.aspen.client

import java.util.UUID

import org.aspen_ddp.aspen.client.KeyValueObjectState.ValueState
import org.aspen_ddp.aspen.common.HLCTimestamp
import org.aspen_ddp.aspen.common.ida.Replication
import org.aspen_ddp.aspen.common.objects.{Key, KeyValueObjectPointer, ObjectId, ObjectRefcount, ObjectRevision, Value}
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.transaction.TransactionId
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

object KeyValueObjectStateSuite {
  val pool = PoolId(new UUID(0, 1))
  val r1 = ObjectRevision(TransactionId(new UUID(0, 1)))
  val r2 = ObjectRevision(TransactionId(new UUID(0, 2)))

  def ptr(n: Int): KeyValueObjectPointer = KeyValueObjectPointer(ObjectId(new UUID(1, n)), pool)
  def bytes(n: Int): Array[Byte] = Array[Byte](n.toByte, 0)

  // Every field holds a freshly allocated array so equality must compare contents, not references
  def state(pointer: KeyValueObjectPointer = ptr(1),
            revision: ObjectRevision = r1,
            refcount: ObjectRefcount = ObjectRefcount(0, 1),
            timestamp: HLCTimestamp = HLCTimestamp(1),
            readTimestamp: HLCTimestamp = HLCTimestamp(2),
            minimum: Option[Key] = Some(Key(bytes(1))),
            maximum: Option[Key] = Some(Key(bytes(2))),
            left: Option[Value] = Some(Value(bytes(3))),
            right: Option[Value] = Some(Value(bytes(4))),
            contents: Map[Key, ValueState] = Map(Key(bytes(5)) -> ValueState(Value(bytes(6)), r1, HLCTimestamp(1))),
            partial: Boolean = false): KeyValueObjectState =
    new KeyValueObjectState(pointer, revision, refcount, timestamp, readTimestamp, Replication(3, 2),
      minimum, maximum, left, right, contents, partial)
}

class KeyValueObjectStateSuite extends AnyFunSuite with Matchers {
  import KeyValueObjectStateSuite._

  test("states built from equal field values are equal and share a hash code") {
    state() shouldBe state()
    state().hashCode shouldBe state().hashCode
  }

  test("timestamps do not affect equality or the hash code") {
    val a = state()
    val b = state(timestamp = HLCTimestamp(10), readTimestamp = HLCTimestamp(20))
    a shouldBe b
    a.hashCode shouldBe b.hashCode
  }

  test("a difference in any compared field makes states unequal") {
    val variants = List(
      "pointer" -> state(pointer = ptr(2)),
      "revision" -> state(revision = r2),
      "refcount" -> state(refcount = ObjectRefcount(1, 1)),
      "minimum" -> state(minimum = Some(Key(bytes(9)))),
      "minimum absent" -> state(minimum = None),
      "maximum" -> state(maximum = Some(Key(bytes(9)))),
      "maximum absent" -> state(maximum = None),
      "left" -> state(left = Some(Value(bytes(9)))),
      "left absent" -> state(left = None),
      "right" -> state(right = Some(Value(bytes(9)))),
      "right absent" -> state(right = None),
      "contents" -> state(contents = Map()),
      "content value" -> state(contents = Map(Key(bytes(5)) -> ValueState(Value(bytes(9)), r1, HLCTimestamp(1)))),
      "partial" -> state(partial = true))

    for (field, variant) <- variants do
      withClue(s"$field: ") {
        state() should not be variant
        variant should not be state()
      }
  }

  test("ValueStates with equal value bytes are equal and share a hash code") {
    val a = ValueState(Value(bytes(6)), r1, HLCTimestamp(1))
    val b = ValueState(Value(bytes(6)), r1, HLCTimestamp(1))
    a shouldBe b
    a.hashCode shouldBe b.hashCode
  }

  test("is not equal to other object state types") {
    state() should not be MetadataObjectState(ptr(1), r1, ObjectRefcount(0, 1), HLCTimestamp(1), HLCTimestamp(2))
  }
}
