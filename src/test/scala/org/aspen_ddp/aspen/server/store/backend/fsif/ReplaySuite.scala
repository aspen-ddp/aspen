package org.aspen_ddp.aspen.server.store.backend.fsif

import java.util.UUID

import org.aspen_ddp.aspen.FileBasedTests
import org.aspen_ddp.aspen.common.DataBuffer
import org.aspen_ddp.aspen.common.HLCTimestamp
import org.aspen_ddp.aspen.common.objects.{Metadata, ObjectId, ObjectRefcount, ObjectRevision, ObjectType}
import org.aspen_ddp.aspen.common.transaction.TransactionId

class ReplaySuite extends FileBasedTests:
  import JournalRecord.{Delta, Mapping}

  private val md = Metadata(ObjectRevision(TransactionId(new UUID(5, 6))), ObjectRefcount(3, 4), HLCTimestamp(99))
  private val Zero = ObjectId(new UUID(0, 0))
  private def oid(i: Int) = ObjectId(new UUID(i, ~i))

  private val X = 1
  private val Y = 2

  private def delta(ext: Int, slot: Int, tag: Int): Delta =
    val h = SegmentHeader.forSlice(ObjectType.Data, oid(ext), ext, md, DataBuffer(Array[Byte](tag.toByte)))
    Delta(ext, slot, h, List(DeltaRange(0, Array[Byte](tag.toByte))))

  private def map(ext: Int, slot: Int, id: ObjectId): Mapping = Mapping(ext, slot, id)

  private def startWith(entries: (Int, Int, ObjectId)*): IndexMap =
    val m = IndexMap.empty(16)
    entries.foreach: (ext, slot, id) =>
      m.slots(ext) = slot
      m.ids(ext) = id
    m

  // Deltas are identified by the tag byte in their only range
  private def tags(r: Replay.Result): Vector[Int] = r.apply.map(_.ranges.head.bytes.head.toInt)

  test("dead occupant's delta is not applied to a reused slot"):
    val r = Replay.run(IndexMap.empty(16), List(delta(X, 3, 1), map(X, -1, Zero), map(Y, 3, oid(Y))))
    tags(r) shouldBe Vector.empty

  test("pre-move deltas are not applied to the freed tail"):
    val r = Replay.run(startWith((X, 7, oid(X))), List(delta(X, 7, 1), map(X, 2, oid(X)), map(Y, 7, oid(Y))))
    tags(r) shouldBe Vector.empty
    r.index.slots(X) shouldBe 2
    r.index.slots(Y) shouldBe 7

  test("old delta does not overwrite a newer copy-on-write version"):
    val r = Replay.run(startWith((X, 1, oid(X))), List(delta(X, 1, 1), map(X, 4, oid(X))))
    tags(r) shouldBe Vector.empty

  test("deltas for the live occupant apply in order"):
    val r = Replay.run(startWith((X, 1, oid(X))), List(delta(X, 1, 1), delta(X, 1, 2)))
    tags(r) shouldBe Vector(1, 2)

  test("reallocation to the same slot applies only the new incarnation"):
    val p = oid(99)
    val r = Replay.run(startWith((X, 1, oid(X))),
      List(delta(X, 1, 1), map(X, -1, Zero), map(X, 1, p), delta(X, 1, 2)))
    tags(r) shouldBe Vector(2)
    r.index.ids(X) shouldBe p

  test("journaled allocation"):
    val r = Replay.run(IndexMap.empty(16), List(map(X, 5, oid(X)), delta(X, 5, 1)))
    tags(r) shouldBe Vector(1)

  test("copy-on-write back to an earlier slot"):
    val r = Replay.run(startWith((X, 1, oid(X))),
      List(delta(X, 1, 1), map(X, 4, oid(X)), map(X, 1, oid(X)), delta(X, 1, 2)))
    tags(r) shouldBe Vector(2)

  test("overwrite of a missed-delete occupant"):
    val p = oid(99)
    val r = Replay.run(startWith((X, 2, oid(77))), List(delta(X, 2, 1), map(X, 2, p), delta(X, 2, 2)))
    tags(r) shouldBe Vector(2)
    r.index.ids(X) shouldBe p

  test("a move during which deltas target the old slot, with the move completing"):
    val r = Replay.run(startWith((X, 7, oid(X))), List(delta(X, 7, 1), map(X, 2, oid(X)), delta(X, 2, 2)))
    tags(r) shouldBe Vector(2)

  test("a move that never completed"):
    val r = Replay.run(startWith((X, 7, oid(X))), List(delta(X, 7, 1)))
    tags(r) shouldBe Vector(1)

  test("out of range or unmapped deltas are dropped"):
    val r = Replay.run(startWith((X, 1, oid(X))),
      List(delta(X, 1, 1), delta(40, 1, 2), delta(Y, 99, 3), delta(-1, 0, 4)))
    tags(r) shouldBe Vector(1)

  test("start is not mutated"):
    val start = startWith((X, 1, oid(X)))
    Replay.run(start, List(map(X, 4, oid(55)), map(Y, 1, oid(Y))))
    start.slots(X) shouldBe 1
    start.ids(X) shouldBe oid(X)
    start.slots(Y) shouldBe -1
