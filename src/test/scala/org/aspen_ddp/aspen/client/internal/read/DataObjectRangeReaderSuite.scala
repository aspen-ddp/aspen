package org.aspen_ddp.aspen.client.internal.read

import java.util.UUID

import org.aspen_ddp.aspen.client.DataObjectRangesState
import org.aspen_ddp.aspen.common.{DataBuffer, HLCTimestamp}
import org.aspen_ddp.aspen.common.ida.{AppendingReedSolomon, Replication}
import org.aspen_ddp.aspen.common.network.{ClientId, ReadResponse}
import org.aspen_ddp.aspen.common.objects.{ByteRanges, DataObjectPointer, FullObject, ObjectId, ObjectRefcount, ObjectRevision, ReadType}
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.transaction.TransactionId
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class DataObjectRangeReaderSuite extends AnyFunSuite with Matchers:

  // cellSize 8 and k 3 give 24 byte rows
  private val ida = AppendingReedSolomon(5, 3, 4, 8)
  private val pool = PoolId(new UUID(0, 1))
  private val pointer = DataObjectPointer(ObjectId(new UUID(1, 3)), pool, Array[Byte]())
  private val client = ClientId(new UUID(0, 4))
  private val readUUID = new UUID(1, 2)

  private val rev1 = ObjectRevision(TransactionId(new UUID(0, 1)))
  private val rev2 = ObjectRevision(TransactionId(new UUID(0, 2)))
  private val ts1 = HLCTimestamp(2)
  private val ts2 = HLCTimestamp(3)

  private def content(n: Int): Array[Byte] = Array.tabulate(n)(i => (i + 1).toByte)

  private def newReader(rt: ReadType) = new DataObjectRangeReader(pointer, ida, readUUID, rt)

  // Builds a store response for a ByteRanges read: each range clipped to the slice, then the tail, concatenated
  private def rangesResponse(store: Int, rev: ObjectRevision, ts: HLCTimestamp, slice: Array[Byte],
                             rt: ByteRanges): ReadResponse =
    val parts = rt.pieces(slice.length).map((s, e) => slice.slice(s, e))
    val cs = ReadResponse.CurrentState(rev, ObjectRefcount(0, 1), ts, slice.length,
      Some(DataBuffer(parts.flatten.toArray)), Set())
    ReadResponse(client, StoreId(pool, store.toByte), readUUID, HLCTimestamp.Zero, Right(cs))

  private def feedRanges(reader: DataObjectRangeReader, responses: List[ReadResponse]): DataObjectRangesState =
    responses.map(reader.receiveReadResponse).last match
      case Some(Right(s: DataObjectRangesState)) => s
      case other => fail(s"Unexpected result $other")

  test("a to-end range restores the tail and the object length"):
    val c = content(50)
    val slices = ida.encode(c)
    val rt = ByteRanges(List((8, Int.MaxValue)), 8)
    val s = feedRanges(newReader(rt), List(0, 2, 4).map(i => rangesResponse(i, rev1, ts1, slices(i), rt)))
    s.ranges.size should be (1)
    s.ranges.head.offset should be (24)
    s.ranges.head.data.getByteArray should be (c.drop(24))
    s.objectLength should be (Some(50))

  test("a tail of cellSize yields the object length with no data beyond L"):
    val c = content(50)
    val slices = ida.encode(c)
    val rt = ByteRanges(Nil, 8)
    val s = feedRanges(newReader(rt), List(1, 2, 3).map(i => rangesResponse(i, rev1, ts1, slices(i), rt)))
    s.ranges should be (Nil)
    s.objectLength should be (Some(50))

  test("bounded range not reaching the final row has no objectLength"):
    val c = content(100) // 108 bytes with trailer: 5 rows of 24
    val slices = ida.encode(c)
    val rt = ByteRanges(List((0, 16)), 0)
    val s = feedRanges(newReader(rt), List(0, 1, 2).map(i => rangesResponse(i, rev1, ts1, slices(i), rt)))
    s.ranges.head.offset should be (0)
    s.ranges.head.data.getByteArray should be (c.take(48))
    s.objectLength should be (None)

  test("responses at differing revisions restore only from the matching set"):
    val oldC = content(20) // 28 bytes with trailer: 2 rows, slice size 16
    val newC = content(50) // slice size 24
    val oldSlices = ida.encode(oldC)
    val newSlices = ida.encode(newC)
    val rt = ByteRanges(List((0, Int.MaxValue)), 8)
    val reader = newReader(rt)
    val responses =
      List(0, 1).map(i => rangesResponse(i, rev1, ts1, oldSlices(i), rt)) :::
        List(2, 3, 4).map(i => rangesResponse(i, rev2, ts2, newSlices(i), rt))
    val results = responses.map(reader.receiveReadResponse)
    results.dropRight(1).foreach(_ should be (None))
    results.last match
      case Some(Right(s: DataObjectRangesState)) =>
        s.revision should be (rev2)
        s.ranges.head.data.getByteArray should be (newC)
        s.objectLength should be (Some(50))
      case other => fail(s"Unexpected result $other")

  test("range results never provide rebuild data"):
    val c = content(50)
    val slices = ida.encode(c)
    val rt = ByteRanges(List((0, Int.MaxValue)), 0)
    val s = feedRanges(newReader(rt), List(0, 1, 2).map(i => rangesResponse(i, rev1, ts1, slices(i), rt)))
    (0 until 5).foreach(i => s.getRebuildDataForStore(StoreId(pool, i.toByte)) should be (None))

  test("the reader rejects read types other than ByteRanges"):
    val slices = ida.encode(content(50))
    val reader = newReader(FullObject())
    an [IllegalArgumentException] should be thrownBy
      List(0, 1, 2).foreach(i => reader.receiveReadResponse(rangesResponse(i, rev1, ts1, slices(i), ByteRanges(Nil, 0))))

  private def sliceRanges(logical: List[(Int, Int)]): List[(Int, Int)] =
    logical.map((o, l) =>
      val (start, end) = ida.sliceRangeFor(o, l)
      (start, end - start))

  test("ByteRanges restores every range and the object length from one response set"):
    val c = content(100) // slice size 40
    val slices = ida.encode(c)
    val rt = ByteRanges(sliceRanges(List((0, 16), (48, 16))), 8)
    val s = feedRanges(newReader(rt), List(0, 1, 2).map(i => rangesResponse(i, rev1, ts1, slices(i), rt)))
    s.ranges.size should be (2)
    s.ranges(0).offset should be (0)
    s.ranges(0).data.getByteArray should be (c.slice(0, 48))
    s.ranges(1).offset should be (48)
    s.ranges(1).data.getByteArray should be (c.slice(48, 96))
    s.objectLength should be (Some(100))
    s.revision should be (rev1)
    s.ranges.foreach(_.revision should be (rev1))

  test("ByteRanges with overlapping and duplicate ranges restores each independently"):
    val c = content(100)
    val slices = ida.encode(c)
    val rt = ByteRanges(sliceRanges(List((0, 30), (24, 30), (24, 30))), 8)
    val s = feedRanges(newReader(rt), List(1, 3, 4).map(i => rangesResponse(i, rev1, ts1, slices(i), rt)))
    s.ranges(0).data.getByteArray should be (c.slice(0, 72))
    s.ranges(1).offset should be (24)
    s.ranges(1).data.getByteArray should be (c.slice(24, 96))
    s.ranges(2).data.getByteArray should be (c.slice(24, 96))
    s.objectLength should be (Some(100))

  test("ByteRanges range past the end yields an empty piece"):
    val c = content(100)
    val slices = ida.encode(c)
    val rt = ByteRanges(List((400, 16), (0, 16)), 8)
    val s = feedRanges(newReader(rt), List(0, 1, 2).map(i => rangesResponse(i, rev1, ts1, slices(i), rt)))
    s.ranges(0).data.getByteArray should be (Array.emptyByteArray)
    s.ranges(1).data.getByteArray should be (c.take(48))
    s.objectLength should be (Some(100))

  test("zero-length range yields an empty piece"):
    val c = content(100)
    val slices = ida.encode(c)
    val rt = ByteRanges(List((8, 0)), 8)
    val s = feedRanges(newReader(rt), List(0, 1, 2).map(i => rangesResponse(i, rev1, ts1, slices(i), rt)))
    s.ranges.head.data.getByteArray should be (Array.emptyByteArray)
    s.objectLength should be (Some(100))

  test("ByteRanges tail-only read on Replication yields the object length"):
    val repl = Replication(3, 2)
    val c = content(37)
    val slices = repl.encode(c)
    val rt = ByteRanges(Nil, 0)
    val reader = new DataObjectRangeReader(pointer, repl, readUUID, rt)
    val s = feedRanges(reader, List(0, 1).map(i => rangesResponse(i, rev1, ts1, slices(i), rt)))
    s.ranges should be (Nil)
    s.objectLength should be (Some(slices(0).length))

  test("ByteRanges ignores responses from a minority revision"):
    val oldC = content(20)
    val newC = content(100)
    val oldSlices = ida.encode(oldC)
    val newSlices = ida.encode(newC)
    val rt = ByteRanges(List((0, 16)), 8)
    val responses =
      List(0, 1).map(i => rangesResponse(i, rev1, ts1, oldSlices(i), rt)) :::
        List(2, 3, 4).map(i => rangesResponse(i, rev2, ts2, newSlices(i), rt))
    val s = feedRanges(newReader(rt), responses)
    s.revision should be (rev2)
    s.ranges.head.data.getByteArray should be (newC.take(48))
    s.objectLength should be (Some(100))
