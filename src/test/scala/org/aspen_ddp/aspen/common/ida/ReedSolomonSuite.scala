package org.aspen_ddp.aspen.common.ida

import java.nio.ByteBuffer

import org.aspen_ddp.aspen.common.DataBuffer
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.language.implicitConversions
import scala.util.Random

object ReedSolomonSuite:

  /** Deliberately spans the interesting shapes: the degenerate width-1 pool, a k == 1 pool that
    * degenerates to replication, a zero-parity pool, the (5,3,4) pool used by CodecRoundTripSuite,
    * a few small enough for exhaustive subset enumeration, and (10,7,9), where the pad count
    * exceeds the shard size for small objects.
    */
  val Configs: List[ReedSolomon] = List(
    ReedSolomon(1, 1, 1),
    ReedSolomon(3, 1, 2),
    ReedSolomon(3, 2, 3),
    ReedSolomon(4, 4, 4),
    ReedSolomon(5, 3, 4),
    ReedSolomon(6, 3, 5),
    ReedSolomon(8, 4, 6),
    ReedSolomon(10, 7, 9))

  val Sizes: List[Int] = ((0 to 40) ++ List(63, 64, 65, 100, 127, 128, 129, 200, 255, 256, 257, 1000)).toList

  /** Content generators. Zeros and 0xFF runs matter because Galois arithmetic treats zero
    * specially, and the full byte cycle exercises every value in the field.
    */
  val Generators: List[(String, Int => Array[Byte])] = List(
    "cycled" -> (n => Array.tabulate(n)(i => (i % 256).toByte)),
    "zeros" -> (n => new Array[Byte](n)),
    "ones" -> (n => Array.fill(n)(0xFF.toByte)),
    "random" -> (n => randomBytes(n, 0x5EED)),
    "runs" -> (n => runsOf(n)))

  def randomBytes(nbytes: Int, seed: Int): Array[Byte] =
    val arr = new Array[Byte](nbytes)
    new Random(seed).nextBytes(arr)
    arr

  /** Alternating runs of zeros, 0xFF, and random bytes */
  def runsOf(nbytes: Int): Array[Byte] =
    val rnd = new Random(99)
    val arr = new Array[Byte](nbytes)
    var i = 0
    while i < nbytes do
      val len = math.min(nbytes - i, 1 + rnd.nextInt(8))
      val value: Byte = rnd.nextInt(3) match
        case 0 => 0.toByte
        case 1 => 0xFF.toByte
        case _ => rnd.nextInt(256).toByte
      java.util.Arrays.fill(arr, i, i + len, value)
      i += len
    arr

  def dataOf(nbytes: Int): Array[Byte] = Array.tabulate(nbytes)(i => (i % 256).toByte)

  def kSubsets(width: Int, k: Int): List[List[Int]] = (0 until width).toList.combinations(k).toList

  /** All 2^width subsets of the encoding indices */
  def allSubsets(width: Int): List[List[Int]] =
    (0 until (1 << width)).toList.map(mask => (0 until width).filter(i => (mask & (1 << i)) != 0).toList)

  def segmentsFor(shards: Array[Array[Byte]], idxs: Seq[Int]): List[(Byte, Array[Byte])] =
    idxs.map(i => (i.toByte, shards(i))).toList

  /** The padded content, which is the concatenation of the data shards */
  def concatData(ida: ReedSolomon, shards: Array[Array[Byte]]): Array[Byte] =
    shards.take(ida.restoreThreshold).flatten


class ReedSolomonSuite extends AnyFunSuite with Matchers:

  import ReedSolomonSuite.*

  /** Array comparison that reports the first mismatch rather than dumping kilobytes of bytes */
  def assertSameBytes(actual: Array[Byte], expected: Array[Byte], clue: String): Unit =
    if actual.length != expected.length then
      fail(s"$clue: length ${actual.length} does not match the expected ${expected.length}")
    var i = 0
    while i < actual.length do
      if actual(i) != expected(i) then
        fail(s"$clue: first mismatch at index $i, ${actual(i)} != ${expected(i)} (length ${actual.length})")
      i += 1

  //---------------------------------------------------------------------------------------------
  // Serialization
  //---------------------------------------------------------------------------------------------

  test("the IDA type round trips through serialization"):
    val ida = ReedSolomon(5, 3, 4)

    ida.getSerializedIDATypeLength should be(IDA.EncodedIDASize)

    val bb = ByteBuffer.allocate(ida.getSerializedIDATypeLength)
    ida.serializeIDAType(bb)
    bb.position() should be(4)
    bb.flip()
    bb.get(0) should be(IDA.ReedSolomonCode)

    IDA.deserializeIDAType(bb) should be(ida)
    bb.remaining() should be(0)

  test("replication serialization is unaffected"):
    val ida = Replication(3, 2)
    val bb = ByteBuffer.allocate(ida.getSerializedIDATypeLength)
    ida.serializeIDAType(bb)
    bb.flip()
    bb.get(0) should be(IDA.ReplicationCode)
    IDA.deserializeIDAType(bb) should be(ida)

  //---------------------------------------------------------------------------------------------
  // Length and padding contract
  //---------------------------------------------------------------------------------------------

  test("calculateEncodedSegmentLength matches the padding contract"):
    for ida <- Configs do
      val k = ida.restoreThreshold
      var previous = 0
      for n <- 0 to 300 do
        val len = ida.calculateEncodedSegmentLength(n)
        withClue(s"$ida n=$n: ") {
          len should be(n / k + 1)
          val padBytes = k * len - n
          padBytes should be >= 1
          padBytes should be <= k
          len should be >= previous
        }
        previous = len

  test("every encoded shard is exactly calculateEncodedSegmentLength bytes"):
    for ida <- Configs; n <- Sizes do
      val shards = ida.encode(dataOf(n))
      val expected = ida.calculateEncodedSegmentLength(n)
      withClue(s"$ida n=$n: ") {
        shards.length should be(ida.width)
        shards.foreach(shard => shard.length should be(expected))
      }

  test("padding is applied even when the content is already aligned"):
    for ida <- Configs do
      val k = ida.restoreThreshold
      for n <- List(0, k, 2 * k, 10 * k) do
        val padded = concatData(ida, ida.encode(dataOf(n)))
        withClue(s"$ida n=$n: ") {
          padded.length should be(n + k)
          padded.drop(n).foreach(b => b should be(k.toByte))
        }

  test("the pad count is k - n%k and fills the tail of the padded content"):
    for ida <- Configs; n <- Sizes do
      val k = ida.restoreThreshold
      val padBytes = k - (n % k)
      val padded = concatData(ida, ida.encode(dataOf(n)))
      withClue(s"$ida n=$n: ") {
        padded.length should be(n + padBytes)
        padded(padded.length - 1) should be(padBytes.toByte)
        padded.drop(n).foreach(b => b should be(padBytes.toByte))
      }

  test("padding spans several data shards when the pad count exceeds the shard size"):
    val ida = ReedSolomon(10, 7, 9)

    // shardSize is 1 and padBytes is 7, so every data shard is nothing but padding
    val empty = ida.encode(new Array[Byte](0))
    for i <- 0 until 7 do
      withClue(s"shard $i: ") { empty(i) should be(Array(7.toByte)) }
    ida.restoreArray(segmentsFor(empty, 0 until 7)).length should be(0)

    // k=5, n=7: shardSize is 2 and padBytes is 3, so the pad covers the tail of shard 3
    // and the whole of shard 4
    val small = ReedSolomon(7, 5, 6)
    val src = dataOf(7)
    val shards = small.encode(src)
    shards(3) should be(Array(6.toByte, 3.toByte))
    shards(4) should be(Array(3.toByte, 3.toByte))
    assertSameBytes(small.restoreArray(segmentsFor(shards, 0 until 5)), src, "k=5 n=7")

  //---------------------------------------------------------------------------------------------
  // Systematic code property
  //---------------------------------------------------------------------------------------------

  test("data shards are byte-for-byte slices of the padded content"):
    for ida <- Configs; (gname, gen) <- Generators; n <- List(0, 1, 7, 64, 200) do
      val src = gen(n)
      val shards = ida.encode(src)
      val shardSize = shards(0).length
      val padded = concatData(ida, shards)

      assertSameBytes(padded.take(n), src, s"$ida/$gname/$n padded prefix")

      for i <- 0 until ida.restoreThreshold do
        assertSameBytes(shards(i), padded.slice(i * shardSize, (i + 1) * shardSize),
          s"$ida/$gname/$n shard $i")

  //---------------------------------------------------------------------------------------------
  // Round trip and slice mixing
  //---------------------------------------------------------------------------------------------

  test("content of arbitrary size round trips through the data shards"):
    for ida <- Configs; (gname, gen) <- Generators; n <- Sizes do
      val src = gen(n)
      val shards = ida.encode(src)
      val restored = ida.restoreArray(segmentsFor(shards, 0 until ida.restoreThreshold))
      assertSameBytes(restored, src, s"$ida/$gname/$n")

  test("any restoreThreshold slices restore the original content"):
    for ida <- Configs.filter(_.width <= 8) do
      val k = ida.restoreThreshold
      for n <- List(0, 1, k - 1, k, k + 1, 2 * k + 1, 37, 200).filter(_ >= 0).distinct do
        val src = randomBytes(n, n + 1)
        val shards = ida.encode(src)
        for subset <- kSubsets(ida.width, k) do
          assertSameBytes(ida.restoreArray(segmentsFor(shards, subset)), src,
            s"$ida/$n/[${subset.mkString(",")}]")

  test("restore succeeds exactly when at least restoreThreshold slices are supplied"):
    // Exhaustive over every subset of the encoding indices. This subsumes the data-only,
    // parity-only, mixed, and superset cases in a single sweep and pins the MDS property.
    for ida <- Configs.filter(_.width <= 10) do
      val k = ida.restoreThreshold
      for n <- List(0, 13, 200) do
        val src = randomBytes(n, n + 7)
        val shards = ida.encode(src)
        for subset <- allSubsets(ida.width) do
          val segments = segmentsFor(shards, subset)
          if subset.size >= k then
            assertSameBytes(ida.restoreArray(segments), src, s"$ida/$n/[${subset.mkString(",")}]")
          else
            withClue(s"$ida/$n/[${subset.mkString(",")}]: ") {
              an[IDARestoreError] should be thrownBy ida.restoreArray(segments)
            }

  test("parity slices alone restore the original content"):
    val ida = ReedSolomon(6, 3, 5)
    for n <- List(0, 1, 200, 1001) do
      val src = randomBytes(n, n + 3)
      val shards = ida.encode(src)
      assertSameBytes(ida.restoreArray(segmentsFor(shards, List(3, 4, 5))), src, s"parity only n=$n")

  test("a mix of data and parity slices restores the original content"):
    val ida = ReedSolomon(8, 4, 6)
    val src = randomBytes(517, 21)
    val shards = ida.encode(src)
    for subset <- List(List(0, 1, 4, 5), List(0, 3, 6, 7), List(2, 5, 6, 7), List(1, 2, 3, 4)) do
      assertSameBytes(ida.restoreArray(segmentsFor(shards, subset)), src, s"[${subset.mkString(",")}]")

  //---------------------------------------------------------------------------------------------
  // Call site fidelity
  //---------------------------------------------------------------------------------------------

  test("restore is insensitive to segment order and tolerates extra segments"):
    val ida = ReedSolomon(8, 4, 6)
    val src = randomBytes(431, 17)
    val shards = ida.encode(src)
    val all = segmentsFor(shards, 0 until 8)

    assertSameBytes(ida.restoreArray(all), src, "every slice")

    // DataObjectReader builds its segment list by prepending, so it arrives in descending order
    assertSameBytes(ida.restoreArray(all.reverse), src, "descending order")

    assertSameBytes(ida.restoreArray(segmentsFor(shards, List(0, 1, 2, 3, 4))), src, "k+1 slices")
    assertSameBytes(ida.restoreArray(segmentsFor(shards, List(7, 2, 5, 0, 6))), src, "sparse and unordered")

    val rnd = new Random(1234)
    for i <- 0 until 20 do
      assertSameBytes(ida.restoreArray(rnd.shuffle(all)), src, s"shuffle $i")

  //---------------------------------------------------------------------------------------------
  // API agreement and determinism
  //---------------------------------------------------------------------------------------------

  test("encoding is deterministic, including across separately constructed instances"):
    val a = ReedSolomon(8, 4, 6)
    val b = ReedSolomon(8, 4, 6)
    val src = randomBytes(500, 77)

    val first = a.encode(src)
    val again = a.encode(src)
    val other = b.encode(src)

    for i <- 0 until 8 do
      assertSameBytes(again(i), first(i), s"repeat encode shard $i")
      assertSameBytes(other(i), first(i), s"second instance shard $i")

  test("encode(DataBuffer) agrees with encode(Array[Byte])"):
    for ida <- Configs; n <- List(0, 1, 17, 64, 200) do
      val src = randomBytes(n, n + 11)
      val expected = ida.encode(src)
      val actual = ida.encode(DataBuffer(src))
      actual.length should be(ida.width)
      for i <- 0 until ida.width do
        assertSameBytes(actual(i).getByteArray, expected(i), s"$ida/$n shard $i")

  test("encode handles DataBuffers backed by an offset and by a converted array"):
    val ida = ReedSolomon(5, 3, 4)

    // A slice of a larger backing array has a non-zero position, so encode must read relative
    // to that position rather than from the start of the backing array
    val backing = randomBytes(100, 11)
    val expected = ida.encode(backing.slice(10, 60))
    val actual = ida.encode(DataBuffer(backing).slice(10, 50))
    for i <- 0 until 5 do
      assertSameBytes(actual(i).getByteArray, expected(i), s"offset slice shard $i")

    // Encoding a DataBuffer obtained from the implicit conversion must leave the caller's
    // array untouched
    val src = randomBytes(37, 12)
    val snapshot = src.clone()
    val aliased: DataBuffer = src
    val shards = ida.encode(aliased)
    assertSameBytes(src, snapshot, "the source array must be left unmodified")
    val fromArray = ida.encode(snapshot)
    for i <- 0 until 5 do
      assertSameBytes(shards(i).getByteArray, fromArray(i), s"aliased shard $i")

  test("encodeInto agrees with encode"):
    for ida <- Configs; n <- List(0, 5, 64, 201) do
      val src = randomBytes(n, n + 13)
      val expected = ida.encode(src)
      val segLen = ida.calculateEncodedSegmentLength(n)
      val bbs = Array.fill(ida.width)(ByteBuffer.allocate(segLen))

      ida.encodeInto(DataBuffer(src), bbs)

      for i <- 0 until ida.width do
        bbs(i).position() should be(segLen)
        assertSameBytes(bbs(i).array(), expected(i), s"$ida/$n shard $i")

  test("encodeInto writes only the segment and advances the position by exactly the segment length"):
    val ida = ReedSolomon(5, 3, 4)
    val src = randomBytes(97, 3)
    val expected = ida.encode(src)
    val segLen = ida.calculateEncodedSegmentLength(97)

    val bbs = Array.fill(5) {
      val bb = ByteBuffer.allocate(segLen + 20)
      java.util.Arrays.fill(bb.array(), 0x7E.toByte)
      bb.position(7)
      bb
    }

    ida.encodeInto(DataBuffer(src), bbs)

    for i <- 0 until 5 do
      bbs(i).position() should be(7 + segLen)
      val arr = bbs(i).array()
      assertSameBytes(arr.slice(7, 7 + segLen), expected(i), s"segment $i")
      arr.take(7).foreach(b => b should be(0x7E.toByte))
      arr.drop(7 + segLen).foreach(b => b should be(0x7E.toByte))

  test("encodeInto rejects a wrongly sized buffer array"):
    val ida = ReedSolomon(5, 3, 4)
    an[IllegalArgumentException] should be thrownBy
      ida.encodeInto(DataBuffer(new Array[Byte](10)), Array.fill(4)(ByteBuffer.allocate(16)))
    an[IllegalArgumentException] should be thrownBy
      ida.encodeInto(DataBuffer(new Array[Byte](10)), Array.fill(6)(ByteBuffer.allocate(16)))

  test("restore(DataBuffer) agrees with restoreArray"):
    val ida = ReedSolomon(6, 3, 5)
    val src = randomBytes(333, 6)
    val shards = ida.encode(DataBuffer(src))
    val restored = ida.restore(List(1, 3, 5).map(i => (i.toByte, shards(i))))
    restored.size should be(333)
    assertSameBytes(restored.getByteArray, src, "restore(DataBuffer)")

  test("restore does not modify the caller supplied slices"):
    val ida = ReedSolomon(6, 3, 5)
    val src = randomBytes(200, 8)
    val shards = ida.encode(src)
    val snapshot = shards.map(_.clone())

    // omit data shards 1 and 2 to force the decodeMissing path
    assertSameBytes(ida.restoreArray(segmentsFor(shards, List(0, 4, 5))), src, "restored")

    for i <- 0 until 6 do
      assertSameBytes(shards(i), snapshot(i), s"shard $i must be left unmodified")

  //---------------------------------------------------------------------------------------------
  // Errors
  //---------------------------------------------------------------------------------------------

  test("restore fails when fewer than restoreThreshold slices are supplied"):
    for ida <- Configs do
      val shards = ida.encode(randomBytes(50, 5))
      an[IDARestoreError] should be thrownBy ida.restoreArray(Nil)
      for count <- 0 until ida.restoreThreshold do
        withClue(s"$ida with $count slices: ") {
          an[IDARestoreError] should be thrownBy ida.restoreArray(segmentsFor(shards, 0 until count))
        }

  test("restore rejects out of range encoding indices"):
    val ida = ReedSolomon(5, 3, 4)
    val shards = ida.encode(randomBytes(50, 5))
    val valid = segmentsFor(shards, List(0, 1, 2))

    // -56 is what a poolIndex of 200 looks like as a signed Byte
    for bad <- List(-1.toByte, 5.toByte, 127.toByte, (-56).toByte) do
      withClue(s"index $bad: ") {
        an[IDARestoreError] should be thrownBy ida.restoreArray((bad, shards(0)) :: valid)
      }

  test("restore rejects slices of differing or zero length"):
    val ida = ReedSolomon(5, 3, 4)
    val shards = ida.encode(randomBytes(50, 5))

    an[IDARestoreError] should be thrownBy ida.restoreArray(
      List((0.toByte, shards(0)), (1.toByte, shards(1).dropRight(1)), (2.toByte, shards(2))))

    an[IDARestoreError] should be thrownBy ida.restoreArray(
      List(0, 1, 2).map(i => (i.toByte, new Array[Byte](0))))

  test("restore counts duplicate encoding indices only once"):
    val ida = ReedSolomon(5, 3, 4)
    val src = randomBytes(50, 5)
    val shards = ida.encode(src)

    // three segments but only two distinct indices
    an[IDARestoreError] should be thrownBy ida.restoreArray(segmentsFor(shards, List(0, 0, 1)))

    // a repeated slice from a reader retry must not be a hard failure
    assertSameBytes(ida.restoreArray(segmentsFor(shards, List(0, 0, 1, 2))), src, "duplicate tolerated")

  test("restore rejects a corrupt pad count"):
    // Reed-Solomon provides no corruption detection. A corrupted pad byte holding a value
    // within 1..k is indistinguishable from a valid one and yields silently truncated content;
    // only values outside that range are detectable.
    val ida = ReedSolomon(5, 3, 4)
    val shards = ida.encode(randomBytes(50, 5))

    for bad <- List(0.toByte, 4.toByte, 255.toByte) do
      val corrupt = shards.map(_.clone())
      val last = corrupt(2)
      last(last.length - 1) = bad
      withClue(s"pad count $bad: ") {
        an[IDARestoreError] should be thrownBy ida.restoreArray(segmentsFor(corrupt, List(0, 1, 2)))
      }

  //---------------------------------------------------------------------------------------------
  // Configuration
  //---------------------------------------------------------------------------------------------

  test("invalid configurations are rejected at construction"):
    an[IllegalArgumentException] should be thrownBy ReedSolomon(3, 0, 2)
    an[IllegalArgumentException] should be thrownBy ReedSolomon(3, -1, 2)
    an[IllegalArgumentException] should be thrownBy ReedSolomon(3, 4, 4)
    an[IllegalArgumentException] should be thrownBy ReedSolomon(200, 3, 4)
    an[IllegalArgumentException] should be thrownBy ReedSolomon(5, 3, 2)
    an[IllegalArgumentException] should be thrownBy ReedSolomon(5, 3, 6)

    ReedSolomon(ReedSolomon.MaxWidth, 3, 4).width should be(127)

  test("the IDA attributes are as expected"):
    val ida = ReedSolomon(5, 3, 4)
    ida.name should be("reed-solomon")
    ida.consistentRestoreThreshold should be(3)
    ida.failureTolerance should be(1)

  test("a zero parity configuration round trips from the full slice set"):
    val ida = ReedSolomon(4, 4, 4)
    for n <- List(0, 3, 4, 100) do
      val src = randomBytes(n, n + 2)
      val shards = ida.encode(src)
      assertSameBytes(ida.restoreArray(segmentsFor(shards, 0 until 4)), src, s"n=$n")
      for subset <- kSubsets(4, 3) do
        withClue(s"n=$n [${subset.mkString(",")}]: ") {
          an[IDARestoreError] should be thrownBy ida.restoreArray(segmentsFor(shards, subset))
        }

  test("a single data shard configuration behaves as replication plus a pad byte"):
    val ida = ReedSolomon(3, 1, 2)
    for n <- List(0, 1, 50) do
      val src = randomBytes(n, n + 4)
      val shards = ida.encode(src)
      ida.calculateEncodedSegmentLength(n) should be(n + 1)
      for i <- 0 until 3 do
        assertSameBytes(shards(i), shards(0), s"n=$n shard $i")
        assertSameBytes(ida.restoreArray(segmentsFor(shards, List(i))), src, s"n=$n restore from $i")

  test("a width of one round trips"):
    val ida = ReedSolomon(1, 1, 1)
    val src = randomBytes(64, 1)
    val shards = ida.encode(src)
    shards.length should be(1)
    shards(0).length should be(65)
    assertSameBytes(ida.restoreArray(segmentsFor(shards, List(0))), src, "width 1")

  //---------------------------------------------------------------------------------------------
  // Scale
  //---------------------------------------------------------------------------------------------

  test("a large object round trips"):
    val ida = ReedSolomon(8, 4, 6)
    val src = randomBytes(256 * 1024, 0xBEEF)
    val shards = ida.encode(src)
    shards.foreach(shard => shard.length should be(256 * 1024 / 4 + 1))
    assertSameBytes(ida.restoreArray(segmentsFor(shards, List(1, 3, 5, 7))), src, "256 KiB")
