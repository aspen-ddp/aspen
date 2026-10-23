package org.aspen_ddp.aspen.client

import java.util.UUID

import org.aspen_ddp.aspen.common.{DataBuffer, HLCTimestamp}
import org.aspen_ddp.aspen.common.ida.Replication
import org.aspen_ddp.aspen.common.objects.{DataObjectPointer, ObjectId, ObjectRefcount, ObjectRevision}
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.transaction.TransactionId
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** Equality of DataObjectState and MetadataObjectState. KeyValueObjectState has its own suite */
object ObjectStateEqualitySuite {
  val pool = PoolId(new UUID(0, 1))
  val r1 = ObjectRevision(TransactionId(new UUID(0, 1)))
  val r2 = ObjectRevision(TransactionId(new UUID(0, 2)))

  def ptr(n: Int): DataObjectPointer = DataObjectPointer(ObjectId(new UUID(1, n)), pool)

  def data(pointer: DataObjectPointer = ptr(1),
           revision: ObjectRevision = r1,
           refcount: ObjectRefcount = ObjectRefcount(0, 1),
           timestamp: HLCTimestamp = HLCTimestamp(1),
           readTimestamp: HLCTimestamp = HLCTimestamp(2),
           content: Array[Byte] = Array[Byte](1, 2, 3)): DataObjectState =
    DataObjectState(pointer, revision, refcount, timestamp, readTimestamp, Replication(3, 2), 3, DataBuffer(content.clone()))

  def metadata(pointer: DataObjectPointer = ptr(1),
               revision: ObjectRevision = r1,
               refcount: ObjectRefcount = ObjectRefcount(0, 1),
               timestamp: HLCTimestamp = HLCTimestamp(1),
               readTimestamp: HLCTimestamp = HLCTimestamp(2)): MetadataObjectState =
    MetadataObjectState(pointer, revision, refcount, timestamp, readTimestamp)
}

class ObjectStateEqualitySuite extends AnyFunSuite with Matchers {
  import ObjectStateEqualitySuite._

  test("DataObjectState: equal field values are equal and share a hash code") {
    data() shouldBe data()
    data().hashCode shouldBe data().hashCode
  }

  test("DataObjectState: timestamps do not affect equality or the hash code") {
    val a = data()
    val b = data(timestamp = HLCTimestamp(10), readTimestamp = HLCTimestamp(20))
    a shouldBe b
    a.hashCode shouldBe b.hashCode
  }

  test("DataObjectState: a difference in any compared field makes states unequal") {
    val variants = List(
      "pointer" -> data(pointer = ptr(2)),
      "revision" -> data(revision = r2),
      "refcount" -> data(refcount = ObjectRefcount(1, 1)),
      "data" -> data(content = Array[Byte](1, 2, 4)))
    for (field, variant) <- variants do
      withClue(s"$field: ") {
        data() should not be variant
        variant should not be data()
      }
  }

  test("MetadataObjectState: timestamps do not affect equality or the hash code") {
    val a = metadata()
    val b = metadata(timestamp = HLCTimestamp(10), readTimestamp = HLCTimestamp(20))
    a shouldBe b
    a.hashCode shouldBe b.hashCode
  }

  test("MetadataObjectState: a difference in any compared field makes states unequal") {
    val variants = List(
      "pointer" -> metadata(pointer = ptr(2)),
      "revision" -> metadata(revision = r2),
      "refcount" -> metadata(refcount = ObjectRefcount(1, 1)))
    for (field, variant) <- variants do
      withClue(s"$field: ") {
        metadata() should not be variant
        variant should not be metadata()
      }
  }

  test("data and metadata states are never equal to each other") {
    data() should not be metadata()
    metadata() should not be data()
  }
}
