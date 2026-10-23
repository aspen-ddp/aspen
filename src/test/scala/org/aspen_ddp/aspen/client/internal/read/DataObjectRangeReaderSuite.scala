package org.aspen_ddp.aspen.client.internal.read

import java.util.UUID

import org.aspen_ddp.aspen.client.DataObjectRangeState
import org.aspen_ddp.aspen.common.{DataBuffer, HLCTimestamp}
import org.aspen_ddp.aspen.common.ida.AppendingReedSolomon
import org.aspen_ddp.aspen.common.network.{ClientId, ReadResponse}
import org.aspen_ddp.aspen.common.objects.{ByteRange, ByteRangeToEnd, DataObjectPointer, ObjectId, ObjectRefcount, ObjectRevision, ReadType, SliceTail}
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

  private def response(store: Int, rev: ObjectRevision, ts: HLCTimestamp, slice: Array[Byte],
                       from: Int, until: Int): ReadResponse =
    val end = math.min(until, slice.length)
    val seg = if from >= end then Array.emptyByteArray else slice.slice(from, end)
    val cs = ReadResponse.CurrentState(rev, ObjectRefcount(0, 1), ts, slice.length, Some(DataBuffer(seg)), Set())
    ReadResponse(client, StoreId(pool, store.toByte), readUUID, HLCTimestamp.Zero, Right(cs))

  private def newReader(rt: ReadType) = new DataObjectRangeReader(pointer, ida, readUUID, rt)

  private def feed(reader: DataObjectRangeReader, responses: List[ReadResponse]): DataObjectRangeState =
    val results = responses.map(reader.receiveReadResponse)
    results.last match
      case Some(Right(s: DataObjectRangeState)) => s
      case other => fail(s"Unexpected result $other")

  test("ByteRangeToEnd restores the tail and the object length"):
    val c = content(50)
    val slices = ida.encode(c)
    val reader = newReader(ByteRangeToEnd(8))
    val s = feed(reader, List(0, 2, 4).map(i => response(i, rev1, ts1, slices(i), 8, Int.MaxValue)))
    s.offset should be (24)
    s.data.getByteArray should be (c.drop(24))
    s.objectLength should be (Some(50))
    s.appendPosition.map(_.length) should be (Some(50))

  test("SliceTail of cellSize yields the object length with no data beyond L"):
    val c = content(50)
    val slices = ida.encode(c)
    val reader = newReader(SliceTail(8))
    val s = feed(reader, List(1, 2, 3).map(i => response(i, rev1, ts1, slices(i), slices(i).length - 8, Int.MaxValue)))
    s.offset should be (48)
    s.data.getByteArray should be (c.drop(48))
    s.objectLength should be (Some(50))

  test("bounded ByteRange not reaching the final row has no objectLength"):
    val c = content(100) // 108 bytes with trailer: 5 rows of 24
    val slices = ida.encode(c)
    val reader = newReader(ByteRange(0, 16))
    val s = feed(reader, List(0, 1, 2).map(i => response(i, rev1, ts1, slices(i), 0, 16)))
    s.offset should be (0)
    s.data.getByteArray should be (c.take(48))
    s.objectLength should be (None)
    s.appendPosition should be (None)

  test("responses at differing revisions restore only from the matching set"):
    val oldC = content(20) // 28 bytes with trailer: 2 rows, slice size 16
    val newC = content(50) // slice size 24
    val oldSlices = ida.encode(oldC)
    val newSlices = ida.encode(newC)
    val reader = newReader(ByteRangeToEnd(0))
    val responses =
      List(0, 1).map(i => response(i, rev1, ts1, oldSlices(i), 0, Int.MaxValue)) :::
        List(2, 3, 4).map(i => response(i, rev2, ts2, newSlices(i), 0, Int.MaxValue))
    val results = responses.map(reader.receiveReadResponse)
    results.dropRight(1).foreach(_ should be (None))
    results.last match
      case Some(Right(s: DataObjectRangeState)) =>
        s.revision should be (rev2)
        s.data.getByteArray should be (newC)
        s.objectLength should be (Some(50))
      case other => fail(s"Unexpected result $other")

  test("range results never provide rebuild data"):
    val c = content(50)
    val slices = ida.encode(c)
    val reader = newReader(ByteRangeToEnd(0))
    val s = feed(reader, List(0, 1, 2).map(i => response(i, rev1, ts1, slices(i), 0, Int.MaxValue)))
    (0 until 5).foreach(i => s.getRebuildDataForStore(StoreId(pool, i.toByte)) should be (None))
