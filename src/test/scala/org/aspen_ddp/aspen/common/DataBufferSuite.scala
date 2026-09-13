package org.aspen_ddp.aspen.common

import java.nio.ByteBuffer

import org.scalatest._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers


object DataBufferSuite:
  def db2s(db: DataBuffer): String = "[" + db.getByteArray.mkString(",") + "]"

  def pdb(db: DataBuffer): Unit = println(db2s(db))

  def bdb(barr: Int*): DataBuffer = DataBuffer(barr.map(_.toByte).toArray)


class DataBufferSuite extends AnyFunSuite with Matchers:
  import DataBufferSuite.*

  test("compareTo"):
    val arr = Array[Byte](1, 2, 3)
    val bb = ByteBuffer.wrap(arr)

    val da = DataBuffer(arr)
    val db = DataBuffer(bb)

    da.compareTo(db) should be(0)
    db.compareTo(da) should be(0)

  test("slice"):
    val arr = Array[Byte](1, 2, 3, 4, 5)
    val db = DataBuffer(arr)

    db.slice(0, db.size).compareTo(db) should be(0)
    db.slice(0, 2).compareTo(bdb(1, 2)) should be(0)
    db.slice(0, 2).compareTo(bdb(1, 2)) should be(0)
    db.slice(1, 2).compareTo(bdb(2, 3)) should be(0)
    db.slice(3, 2).compareTo(bdb(4, 5)) should be(0)
    db.slice(4, 1).compareTo(bdb(5)) should be(0)

  test("get is relative to the buffer content, not the backing buffer"):
    val db = bdb(1, 2, 3, 4, 5)

    db.get(0) should be(1.toByte)
    db.get(4) should be(5.toByte)

    val tail = db.slice(2)
    tail.size should be(3)
    tail.get(0) should be(3.toByte)
    tail.get(2) should be(5.toByte)

    val mid = db.slice(1, 3)
    mid.size should be(3)
    mid.get(0) should be(2.toByte)
    mid.get(2) should be(4.toByte)

  test("multi-byte gets are relative to the buffer content"):
    val bb = ByteBuffer.allocate(2 + 2 + 4 + 8 + 4 + 8)
    bb.put(0xAA.toByte)
    bb.put(0xBB.toByte) // two bytes of leading padding to be sliced away
    bb.putShort(0x1234)
    bb.putInt(0x01020304)
    bb.putLong(0x0102030405060708L)
    bb.putFloat(1.5f)
    bb.putDouble(2.5)
    bb.position(0)

    val db = DataBuffer(bb).slice(2)

    db.getShort(0) should be(0x1234.toShort)
    db.getInt(2) should be(0x01020304)
    db.getLong(6) should be(0x0102030405060708L)
    db.getFloat(14) should be(1.5f)
    db.getDouble(18) should be(2.5)

  test("gets bounds-check against the buffer content"):
    val db = bdb(1, 2, 3, 4, 5, 6, 7, 8, 9, 10).slice(2, 8)

    db.size should be(8)

    // Offsets preceding the content are rejected even though the backing buffer has data there
    an[IndexOutOfBoundsException] should be thrownBy db.get(-1)
    an[IndexOutOfBoundsException] should be thrownBy db.getInt(-1)

    // Reads running past the end of the content are rejected even though the backing buffer has data there
    an[IndexOutOfBoundsException] should be thrownBy db.get(8)
    an[IndexOutOfBoundsException] should be thrownBy db.getShort(7)
    an[IndexOutOfBoundsException] should be thrownBy db.getInt(5)
    an[IndexOutOfBoundsException] should be thrownBy db.getLong(1)

    // The largest in-bounds read of each width is accepted
    db.get(7) should be(10.toByte)
    noException should be thrownBy db.getShort(6)
    noException should be thrownBy db.getInt(4)
    noException should be thrownBy db.getLong(0)

  test("split"):
    val db = bdb(1, 2, 3, 4, 5)

    def cmp(a: (DataBuffer, DataBuffer), b: (DataBuffer, DataBuffer)): Unit =
      a._1.compareTo(b._1) should be (0)
      a._2.compareTo(b._2) should be (0)

    cmp(db.split(0), (DataBuffer.Empty, db))
    cmp(db.split(1), (bdb(1), bdb(2, 3, 4,5)))
    cmp(db.split(2), (bdb(1, 2), bdb(3, 4, 5)))
    cmp(db.split(3), (bdb(1, 2, 3), bdb(4, 5)))
    cmp(db.split(4), (bdb(1, 2, 3, 4), bdb(5)))
    cmp(db.split(5), (bdb(1, 2, 3, 4, 5), DataBuffer.Empty))

  test("append"):
    DataBuffer.Empty.append(DataBuffer.Empty).compareTo(DataBuffer.Empty) should be(0)
    DataBuffer.Empty.append(bdb(1)).compareTo(bdb(1)) should be (0)
    bdb(1).append(DataBuffer.Empty).compareTo(bdb(1)) should be (0)
    bdb(1).append(bdb(2)).compareTo(bdb(1,2)) should be (0)

  test("fill - empty input list"):
    val bb = ByteBuffer.allocate(10)
    val remaining = DataBuffer.fill(bb, Nil)
    remaining should be(Nil)
    bb.position() should be(0)

  test("fill - empty buffer (no remaining space)"):
    val bb = ByteBuffer.allocate(0)
    val buffers = List(bdb(1, 2, 3))
    val remaining = DataBuffer.fill(bb, buffers)
    remaining should be(buffers)
    bb.position() should be(0)

  test("fill - single buffer fits exactly"):
    val bb = ByteBuffer.allocate(3)
    val buffers = List(bdb(1, 2, 3))
    val remaining = DataBuffer.fill(bb, buffers)
    remaining should be(Nil)
    bb.position() should be(3)
    bb.array() should be(Array[Byte](1, 2, 3))

  test("fill - single buffer fits with space left"):
    val bb = ByteBuffer.allocate(5)
    val buffers = List(bdb(1, 2, 3))
    val remaining = DataBuffer.fill(bb, buffers)
    remaining should be(Nil)
    bb.position() should be(3)
    bb.array().take(3) should be(Array[Byte](1, 2, 3))

  test("fill - single buffer too large, needs splitting"):
    val bb = ByteBuffer.allocate(2)
    val buffers = List(bdb(1, 2, 3, 4, 5))
    val remaining = DataBuffer.fill(bb, buffers)
    remaining.size should be(1)
    remaining.head.compareTo(bdb(3, 4, 5)) should be(0)
    bb.position() should be(2)
    bb.array() should be(Array[Byte](1, 2))

  test("fill - multiple buffers all fit"):
    val bb = ByteBuffer.allocate(6)
    val buffers = List(bdb(1, 2), bdb(3, 4), bdb(5, 6))
    val remaining = DataBuffer.fill(bb, buffers)
    remaining should be(Nil)
    bb.position() should be(6)
    bb.array() should be(Array[Byte](1, 2, 3, 4, 5, 6))

  test("fill - multiple buffers, some fit, some don't"):
    val bb = ByteBuffer.allocate(5)
    val buffers = List(bdb(1, 2), bdb(3, 4), bdb(5, 6, 7))
    val remaining = DataBuffer.fill(bb, buffers)
    remaining.size should be(1)
    remaining.head.compareTo(bdb(6, 7)) should be(0)
    bb.position() should be(5)
    bb.array() should be(Array[Byte](1, 2, 3, 4, 5))

  test("fill - multiple buffers, first one needs splitting"):
    val bb = ByteBuffer.allocate(3)
    val buffers = List(bdb(1, 2, 3, 4, 5), bdb(6, 7))
    val remaining = DataBuffer.fill(bb, buffers)
    remaining.size should be(2)
    remaining.head.compareTo(bdb(4, 5)) should be(0)
    remaining.tail.head.compareTo(bdb(6, 7)) should be(0)
    bb.position() should be(3)
    bb.array() should be(Array[Byte](1, 2, 3))

  test("fill - buffer with zero remaining space"):
    val bb = ByteBuffer.allocate(3)
    bb.position(3) // Fill the buffer completely
    val buffers = List(bdb(1, 2, 3))
    val remaining = DataBuffer.fill(bb, buffers)
    remaining should be(buffers)
    bb.position() should be(3)

  test("fill - empty buffers in list"):
    val bb = ByteBuffer.allocate(5)
    val buffers = List(DataBuffer.Empty, bdb(1, 2), DataBuffer.Empty, bdb(3, 4))
    val remaining = DataBuffer.fill(bb, buffers)
    remaining should be(Nil)
    bb.position() should be(4)
    bb.array().take(4) should be(Array[Byte](1, 2, 3, 4))

  test("fill - single empty buffer"):
    val bb = ByteBuffer.allocate(5)
    val buffers = List(DataBuffer.Empty)
    val remaining = DataBuffer.fill(bb, buffers)
    remaining should be(Nil)
    bb.position() should be(0)

  test("compact - empty buffer list"):
    val (compacted, remaining) = DataBuffer.compact(10, Nil)
    compacted.compareTo(DataBuffer.Empty) should be(0)
    remaining should be(Nil)

  test("compact - single buffer fits within maxSize"):
    val buffers = List(bdb(1, 2, 3))
    val (compacted, remaining) = DataBuffer.compact(5, buffers)
    compacted.compareTo(bdb(1, 2, 3)) should be(0)
    remaining should be(Nil)

  test("compact - single buffer exactly matches maxSize"):
    val buffers = List(bdb(1, 2, 3))
    val (compacted, remaining) = DataBuffer.compact(3, buffers)
    compacted.compareTo(bdb(1, 2, 3)) should be(0)
    remaining should be(Nil)

  test("compact - single buffer larger than maxSize"):
    val buffers = List(bdb(1, 2, 3, 4, 5))
    val (compacted, remaining) = DataBuffer.compact(3, buffers)
    compacted.compareTo(bdb(1, 2, 3)) should be(0)
    remaining.size should be(1)
    remaining.head.compareTo(bdb(4, 5)) should be(0)

  test("compact - multiple buffers all fit within maxSize"):
    val buffers = List(bdb(1, 2), bdb(3, 4), bdb(5, 6))
    val (compacted, remaining) = DataBuffer.compact(10, buffers)
    compacted.compareTo(bdb(1, 2, 3, 4, 5, 6)) should be(0)
    remaining should be(Nil)

  test("compact - multiple buffers exactly fill maxSize"):
    val buffers = List(bdb(1, 2), bdb(3, 4), bdb(5, 6))
    val (compacted, remaining) = DataBuffer.compact(6, buffers)
    compacted.compareTo(bdb(1, 2, 3, 4, 5, 6)) should be(0)
    remaining should be(Nil)

  test("compact - multiple buffers exceed maxSize, some fit"):
    val buffers = List(bdb(1, 2), bdb(3, 4), bdb(5, 6, 7))
    val (compacted, remaining) = DataBuffer.compact(5, buffers)
    compacted.compareTo(bdb(1, 2, 3, 4, 5)) should be(0)
    remaining.size should be(1)
    remaining.head.compareTo(bdb(6, 7)) should be(0)

  test("compact - multiple buffers exceed maxSize, first buffer needs splitting"):
    val buffers = List(bdb(1, 2, 3, 4, 5), bdb(6, 7))
    val (compacted, remaining) = DataBuffer.compact(3, buffers)
    compacted.compareTo(bdb(1, 2, 3)) should be(0)
    remaining.size should be(2)
    remaining.head.compareTo(bdb(4, 5)) should be(0)
    remaining.tail.head.compareTo(bdb(6, 7)) should be(0)

  test("compact - maxSize zero"):
    val buffers = List(bdb(1, 2, 3))
    val (compacted, remaining) = DataBuffer.compact(0, buffers)
    compacted.compareTo(DataBuffer.Empty) should be(0)
    remaining should be(buffers)

  test("compact - maxSize larger than total size"):
    val buffers = List(bdb(1, 2), bdb(3, 4))
    val (compacted, remaining) = DataBuffer.compact(100, buffers)
    compacted.compareTo(bdb(1, 2, 3, 4)) should be(0)
    remaining should be(Nil)

  test("compact - buffers with empty buffers in list"):
    val buffers = List(DataBuffer.Empty, bdb(1, 2), DataBuffer.Empty, bdb(3, 4), DataBuffer.Empty)
    val (compacted, remaining) = DataBuffer.compact(10, buffers)
    compacted.compareTo(bdb(1, 2, 3, 4)) should be(0)
    remaining.forall(_.size == 0) should be(true)

  test("compact - only empty buffers"):
    val buffers = List(DataBuffer.Empty, DataBuffer.Empty)
    val (compacted, remaining) = DataBuffer.compact(10, buffers)
    compacted.compareTo(DataBuffer.Empty) should be(0)
    remaining.forall(_.size == 0) should be(true)

  test("compact - Long maxSize variant"):
    val buffers = List(bdb(1, 2, 3))
    val (compacted, remaining) = DataBuffer.compact(5L, buffers)
    compacted.compareTo(bdb(1, 2, 3)) should be(0)
    remaining should be(Nil)

  test("compact - single byte buffer"):
    val buffers = List(bdb(42))
    val (compacted, remaining) = DataBuffer.compact(1, buffers)
    compacted.compareTo(bdb(42)) should be(0)
    remaining should be(Nil)



