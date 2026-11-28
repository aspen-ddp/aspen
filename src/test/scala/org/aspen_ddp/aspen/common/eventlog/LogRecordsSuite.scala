package org.aspen_ddp.aspen.common.eventlog

import scala.util.Random

import org.aspen_ddp.aspen.common.HLCTimestamp
import org.aspen_ddp.aspen.common.objects.ByteArrayKeyOrdering
import org.scalatest.funsuite.AnyFunSuite

class LogRecordsSuite extends AnyFunSuite:

  private def randomRecord(rnd: Random, incompressible: Boolean): LoggedRecord =
    val data = new Array[Byte](rnd.nextInt(200))
    if incompressible then rnd.nextBytes(data) else java.util.Arrays.fill(data, rnd.nextInt(3).toByte)
    LoggedRecord(HLCTimestamp(rnd.nextLong()), rnd.nextInt(65536).toShort, data)

  test("LogKey order matches HLCTimestamp order") {
    val rnd = new Random(1)
    val longs = Seq(Long.MinValue, -1L, 0L, Long.MaxValue, Long.MinValue + 1, Long.MaxValue - 1) ++
      Seq.fill(1000)(rnd.nextLong())
    val tss = longs.map(HLCTimestamp(_)) :+ HLCTimestamp.now
    val byKey = tss.sortBy(LogKey(_))(using ByteArrayKeyOrdering)
    assert(byKey == tss.sorted)
    tss.foreach(t => assert(LogKey.decode(LogKey(t)) == t))
  }

  test("decodeFrames reads every concatenated frame") {
    val rnd = new Random(2)
    def recs(n: Int) = Seq.fill(n)(randomRecord(rnd, rnd.nextBoolean()))
    val (a, b, c) = (recs(5), recs(1), recs(20))
    val all = LogRecords.encodeFrame(a) ++ LogRecords.encodeFrame(b) ++ LogRecords.encodeFrame(c)
    assert(LogRecords.decodeFrames(all) == (a ++ b ++ c).toVector)
    assert(LogRecords.decodeFrames(Array.empty[Byte]).isEmpty)
    assert(LogRecords.decodeFrames(LogRecords.encodeFrame(Seq.empty)).isEmpty)
  }

  test("encodeFrame never exceeds maxFrameSize") {
    val rnd = new Random(3)
    for incompressible <- Seq(false, true); n <- Seq(1, 2, 17, 300, 5000) do
      val recs = Seq.fill(n)(randomRecord(rnd, incompressible))
      val raw = recs.map(LogRecords.recordSize).sum
      val frame = LogRecords.encodeFrame(recs)
      assert(frame.length <= LogRecords.maxFrameSize(raw))
      assert(LogRecords.decodeFrames(frame) == recs.toVector)
  }

  test("FullObject round-trips with and without an upper bound") {
    val rnd = new Random(4)
    val recs = Seq.fill(50)(randomRecord(rnd, true))
    for ub <- Seq(None, Some(HLCTimestamp(Long.MinValue)), Some(HLCTimestamp(Long.MaxValue))) do
      val h = FullObjectHeader(HLCTimestamp(-12345L), ub)
      val bytes = FullObject.encode(h, recs)
      assert(FullObject.decode(bytes) == (h, recs.toVector))
    val empty = FullObject.encode(FullObjectHeader(HLCTimestamp.Zero, None), Seq.empty)
    assert(FullObject.decode(empty)._2.isEmpty)
  }
