package org.aspen_ddp.aspen.vdisk

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class CleanCacheSuite extends AnyFunSuite with Matchers:

  private def arr(len: Int, v: Int): Array[Byte] = Array.fill(len)(v.toByte)

  private def shape(b: CleanBlock): List[(Int, List[Byte])] = b.pieces.toList.map((o, d) => (o, d.toList))

  test("merge keeps pieces sorted and replaces what it overlaps"):
    val b = CleanBlock.empty.merge(20, arr(10, 1)).merge(0, arr(10, 2)).merge(5, arr(20, 3))
    shape(b) should be (List((0, arr(5, 2).toList), (5, arr(20, 3).toList), (25, arr(5, 1).toList)))
    b.bytes should be (30L)

  test("merge inside a piece splits it"):
    val b = CleanBlock.empty.merge(0, arr(100, 1)).merge(10, arr(10, 2))
    b.pieces.map(_._1) should be (Vector(0, 10, 20))
    b.bytes should be (100L)

  test("merged blocks accumulate in the cache"):
    val c = CleanCache(1024)
    c.merge(3, 0, arr(10, 1))
    c.merge(3, 10, arr(10, 2))
    c.get(3).map(_.bytes) should be (Some(20L))
    c.invalidate(3)
    c.get(3) should be (None)

  test("the cache holds no more than its byte bound"):
    val c = CleanCache(16384)
    c.merge(0, 0, arr(16384, 1))
    c.merge(1, 0, arr(16384, 2))
    (c.get(0).map(_.bytes).getOrElse(0L) + c.get(1).map(_.bytes).getOrElse(0L)) should be (16384L)
