package org.aspen_ddp.aspen.vdisk

import org.aspen_ddp.aspen.client.RangedWrite
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.util.Random

class SegmentMapSuite extends AnyFunSuite with Matchers:

  private def arr(len: Int, v: Int): Array[Byte] = Array.fill(len)(v.toByte)

  private def shape(s: SegmentSnapshot): List[(Int, List[Byte])] =
    s.pieces.toList.map((o, b) => (o, b.toList))

  test("a newer overlapping put splits the older piece"):
    val m = SegmentMap()
    m.put(0, arr(8, 1), 1)
    m.put(4, arr(8, 2), 2)
    val s = m.snapshot
    shape(s) should be (List((0, arr(4, 1).toList), (4, arr(8, 2).toList)))
    s.maxSeq should be (2L)
    s.end should be (12)
    m.bytes should be (12L)
    m.maxSeq should be (2L)

  test("removeUpTo keeps newer pieces"):
    val m = SegmentMap()
    m.put(0, arr(8, 1), 1)
    m.put(4, arr(8, 2), 2)
    m.removeUpTo(1)
    shape(m.snapshot) should be (List((4, arr(8, 2).toList)))

  test("a put inside an older piece leaves two older fragments"):
    val m = SegmentMap()
    m.put(0, arr(100, 1), 1)
    m.put(10, arr(10, 2), 2)
    m.snapshot.pieces.map(_._1) should be (Vector(0, 10, 20))
    m.removeUpTo(1)
    shape(m.snapshot) should be (List((10, arr(10, 2).toList)))
    m.bytes should be (10L)

  test("overlay copies only covered bytes"):
    val m = SegmentMap()
    m.put(4, arr(4, 7), 1)
    m.put(12, arr(2, 9), 2)
    val target = arr(8, 5)
    m.snapshot.overlay(target, 8)
    // block range [8, 16): covered [8,8)... piece [4,8) is outside, [12,14) maps to target [4,6)
    target.toList should be (List[Int](5, 5, 5, 5, 9, 9, 5, 5).map(_.toByte))
    val t2 = arr(8, 5)
    m.snapshot.overlay(t2, 2)
    // block range [2, 10): piece [4,8) -> target [2,6)
    t2.toList should be (List[Int](5, 5, 7, 7, 7, 7, 5, 5).map(_.toByte))

  test("empty map"):
    val m = SegmentMap()
    m.isEmpty should be (true)
    m.maxSeq should be (0L)
    m.bytes should be (0L)
    m.snapshot.maxSeq should be (0L)
    m.snapshot.end should be (0)
    m.snapshot.writes should be (Nil)

  test("writes yields one RangedWrite per piece"):
    val m = SegmentMap()
    m.put(0, arr(8, 1), 1)
    m.put(4, arr(8, 2), 2)
    m.snapshot.writes should be (List(RangedWrite(0, arr(4, 1)), RangedWrite(4, arr(8, 2))))

  test("snapshot is isolated from later puts and from the caller's array"):
    val m = SegmentMap()
    val in = arr(8, 1)
    m.put(0, in, 1)
    in(0) = 99
    val s = m.snapshot
    m.put(2, arr(2, 3), 2)
    m.removeUpTo(1)
    shape(s) should be (List((0, arr(8, 1).toList)))

  test("randomized against a per-byte model"):
    val rnd = Random(12345)
    val size = 64
    for _ <- 0 until 500 do
      val m = SegmentMap()
      val model = Array.fill(size)(0)        // value
      val modelSeq = Array.fill(size)(0L)    // 0 = absent
      var seq = 0L
      for _ <- 0 until 30 do
        if rnd.nextInt(4) == 0 then
          val upTo = rnd.nextLong(seq + 1)
          m.removeUpTo(upTo)
          for i <- 0 until size do
            if modelSeq(i) != 0 && modelSeq(i) <= upTo then modelSeq(i) = 0
        else
          seq += 1
          val off = rnd.nextInt(size)
          val len = 1 + rnd.nextInt(size - off)
          val v = rnd.nextInt(200) + 1
          m.put(off, arr(len, v), seq)
          for i <- off until off + len do
            model(i) = v
            modelSeq(i) = seq
        val s = m.snapshot
        val target = Array.fill[Byte](size)(0xff.toByte)
        s.overlay(target, 0)
        for i <- 0 until size do
          
          (target(i) & 0xff).toInt should be (if modelSeq(i) == 0 then 255 else model(i))
        m.bytes should be (modelSeq.count(_ != 0).toLong)
        m.isEmpty should be (modelSeq.forall(_ == 0))
        m.maxSeq should be (modelSeq.max)
        s.maxSeq should be (modelSeq.max)
        s.end should be (modelSeq.lastIndexWhere(_ != 0) + 1)
        // pieces are sorted and non-overlapping
        s.pieces.sliding(2).foreach: w =>
          if w.size == 2 then (w(0)._1 + w(0)._2.length <= w(1)._1) should be (true)
