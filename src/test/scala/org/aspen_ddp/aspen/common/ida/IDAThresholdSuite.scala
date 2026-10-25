package org.aspen_ddp.aspen.common.ida

import java.nio.ByteBuffer

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class IDAThresholdSuite extends AnyFunSuite with Matchers:

  private def majority(width: Int): Int = width / 2 + 1

  test("Replication write threshold must be a majority of the width"):
    for width <- 1 to 9 do
      for wt <- 0 to width + 1 do
        if wt >= majority(width) && wt <= width then
          Replication(width, wt).writeThreshold should be(wt)
        else
          an[IllegalArgumentException] should be thrownBy Replication(width, wt)

  test("Replication(5,1), (5,2), (6,2) and (6,3) are rejected"):
    for (w, wt) <- List((5, 1), (5, 2), (6, 2), (6, 3)) do
      an[IllegalArgumentException] should be thrownBy Replication(w, wt)

  test("Reed-Solomon read threshold must be a majority and write at least read"):
    for width <- 1 to 9 do
      for rt <- 1 to width do
        for wt <- 1 to width do
          val valid = rt >= majority(width) && wt >= rt
          if valid then
            ReedSolomon(width, rt, wt).restoreThreshold should be(rt)
          else
            an[IllegalArgumentException] should be thrownBy ReedSolomon(width, rt, wt)

  test("Random-Access Reed-Solomon read threshold must be a majority and write at least read"):
    for width <- 1 to 9 do
      for rt <- 1 to width do
        for wt <- 1 to width do
          val valid = rt >= majority(width) && wt >= rt
          if valid then
            RandomAccessReedSolomon(width, rt, wt, 8).restoreThreshold should be(rt)
          else
            an[IllegalArgumentException] should be thrownBy RandomAccessReedSolomon(width, rt, wt, 8)

  test("deserializing an encoded invalid IDA is rejected"):
    val bb = ByteBuffer.allocate(16)
    bb.put(IDA.ReplicationCode).put(5.toByte).put(2.toByte).put(0.toByte)
    bb.flip()
    an[IllegalArgumentException] should be thrownBy IDA.deserializeIDAType(bb)
