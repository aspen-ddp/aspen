package org.aspen_ddp.aspen.common.ida

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class IDAMaxObjectSizeSuite extends AnyFunSuite with Matchers:

  private val idas: List[IDA] = List(
    Replication(3, 2),
    ReedSolomon(5, 3, 4),
    RandomAccessReedSolomon(5, 3, 4, 8),
    ReedSolomon(14, 10, 12))

  for ida <- idas do
    test(s"$ida maxObjectSize is the exact inverse of calculateEncodedSegmentLength"):
      val sizes = (ida.minSliceSize.toLong to ida.minSliceSize.toLong + 200).toList ++
        List(1000L, 4096L, 65535L, 1L << 20, 1L << 28, Int.MaxValue.toLong, Int.MaxValue.toLong * 4)

      sizes.foreach { s =>
        val n = ida.maxObjectSize(s)
        n should be >= 0
        ida.calculateEncodedSegmentLength(n).toLong should be <= s
        if n < Int.MaxValue then
          // A larger object either overflows the slice limit or exceeds the supported maximum
          val next = ida.calculateEncodedSegmentLength(n + 1).toLong
          val atCap = ida match
            case a: RandomAccessReedSolomon => n.toLong + 1 > Int.MaxValue - a.rowBytes
            case _ => false
          withClue(s"slice size $s, n=$n: ") { (next > s || atCap) should be (true) }
      }

  test("ReedSolomon(14,10,12).maxObjectSize(1000) == 9999"):
    ReedSolomon(14, 10, 12).maxObjectSize(1000) should be (9999)

  test("RandomAccessReedSolomon(5,3,4,8).maxObjectSize(16) == 2*24 - 8"):
    RandomAccessReedSolomon(5, 3, 4, 8).maxObjectSize(16) should be (2 * 24 - 8)

  test("RandomAccessReedSolomon maxObjectSize is capped at Int.MaxValue - rowBytes"):
    val ida = RandomAccessReedSolomon(5, 3, 4, 8)
    ida.maxObjectSize(Long.MaxValue) should be (Int.MaxValue - ida.rowBytes)

  test("Replication and ReedSolomon maxObjectSize are capped at Int.MaxValue"):
    Replication(3, 2).maxObjectSize(Long.MaxValue) should be (Int.MaxValue)
    ReedSolomon(5, 3, 4).maxObjectSize(Long.MaxValue) should be (Int.MaxValue)

  test("minSliceSize values"):
    Replication(3, 2).minSliceSize should be (1)
    ReedSolomon(5, 3, 4).minSliceSize should be (1)
    RandomAccessReedSolomon(5, 3, 4, 8).minSliceSize should be (8)

  test("slice sizes below minSliceSize are rejected"):
    an [IllegalArgumentException] should be thrownBy Replication(3, 2).maxObjectSize(0)
    an [IllegalArgumentException] should be thrownBy ReedSolomon(5, 3, 4).maxObjectSize(0)
    an [IllegalArgumentException] should be thrownBy RandomAccessReedSolomon(5, 3, 4, 8).maxObjectSize(0)
    an [IllegalArgumentException] should be thrownBy RandomAccessReedSolomon(5, 3, 4, 8).maxObjectSize(7)
