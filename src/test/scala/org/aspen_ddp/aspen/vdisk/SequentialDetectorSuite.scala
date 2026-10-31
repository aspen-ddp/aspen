package org.aspen_ddp.aspen.vdisk

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class SequentialDetectorSuite extends AnyFunSuite with Matchers:

  private val K = 1024

  private def detector(): SequentialDetector =
    SequentialDetector(initialWindow = 16 * K, maxWindow = 128 * K)

  test("a random read returns 0"):
    val d = detector()
    d.onRead(40960, 4096) should be (0)
    d.onRead(4096, 4096) should be (0)

  test("sequential reads double the window up to maxWindow"):
    val d = detector()
    val windows = (0 until 8).map(i => d.onRead(i * 4096L, 4096))
    windows should be (Seq(0, 16 * K, 32 * K, 64 * K, 128 * K, 128 * K, 128 * K, 128 * K))

  test("two interleaved sequential streams are both detected"):
    val d = detector()
    val a = 0L
    val b = 1L << 30
    val windows = (0 until 3).flatMap: i =>
      Seq(d.onRead(a + i * 4096, 4096), d.onRead(b + i * 8192, 8192))
    windows should be (Seq(0, 0, 16 * K, 16 * K, 32 * K, 32 * K))

  test("a gap resets the stream"):
    val d = detector()
    d.onRead(0, 4096) should be (0)
    d.onRead(4096, 4096) should be (16 * K)
    d.onRead(8192, 4096) should be (32 * K)
    d.onRead(65536, 4096) should be (0)
    d.onRead(69632, 4096) should be (16 * K)

  test("the least recently used stream is replaced beyond maxStreams"):
    val d = SequentialDetector(maxStreams = 2, initialWindow = 16 * K, maxWindow = 128 * K)
    d.onRead(0, 4096)
    d.onRead(1 << 20, 4096)
    d.onRead(2 << 20, 4096) // replaces the stream at 0
    d.onRead(4096, 4096) should be (0)
    d.onRead((2 << 20) + 4096, 4096) should be (16 * K)

  test("an initial window above maxWindow is capped"):
    val d = SequentialDetector(initialWindow = 16 * K, maxWindow = 8 * K)
    d.onRead(0, 4096)
    d.onRead(4096, 4096) should be (8 * K)
