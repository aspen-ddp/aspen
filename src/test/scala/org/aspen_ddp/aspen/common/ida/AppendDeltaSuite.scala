package org.aspen_ddp.aspen.common.ida

import java.nio.ByteBuffer

import org.aspen_ddp.aspen.common.DataBuffer
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.language.implicitConversions
import scala.util.Random

class AppendDeltaSuite extends AnyFunSuite with Matchers:

  import ReedSolomonSuite.segmentsFor

  private val Configs: List[IDA & AppendableIDA] = List(
    AppendingReedSolomon(1, 1, 1, 8),
    AppendingReedSolomon(3, 1, 2, 8),
    AppendingReedSolomon(3, 2, 3, 8),
    AppendingReedSolomon(4, 4, 4, 16),
    AppendingReedSolomon(5, 3, 4, 64),
    AppendingReedSolomon(5, 3, 4, 8),
    AppendingReedSolomon(8, 4, 6, 64),
    Replication(3, 2))

  private def rowBytesOf(ida: IDA): Int = ida match
    case a: AppendingReedSolomon => a.rowBytes
    case _ => 24

  private def bytes(n: Int, seed: Int): Array[Byte] =
    val a = new Array[Byte](n)
    new Random(seed).nextBytes(a)
    a

  /** Encodes initial, then applies each append's deltas to every slice */
  private def appendAll(ida: IDA & AppendableIDA, initial: Array[Byte], appends: Seq[Array[Byte]]): Array[Array[Byte]] =
    var slices = ida.encode(initial)
    var len = initial.length
    appends.foreach { a =>
      val deltas = ida.appendDeltas(len, a)
      slices = Array.tabulate(ida.width)(i => deltas(i).applyTo(DataBuffer(slices(i))).getByteArray)
      len += a.length
    }
    slices

  private def sameSlices(a: Array[Array[Byte]], b: Array[Array[Byte]]): Unit =
    a.length should be (b.length)
    a.indices.foreach(i => a(i).toSeq should be (b(i).toSeq))

  test("delta-applied slices equal a fresh encode") {
    for
      ida <- Configs
      initial <- List(0, 1, 15, 16, 17, 100, 1000)
      m <- List(1, 7, 8, 9, rowBytesOf(ida), 1024)
    do
      val init = bytes(initial, initial)
      val app = bytes(m, m + 1)
      sameSlices(appendAll(ida, init, List(app)), ida.encode(init ++ app))
  }

  test("trailer boundary appends") {
    val ida = AppendingReedSolomon(5, 3, 4, 8)
    for
      initial <- List(15, 16, 17, 24)
      m <- List(1, 8, 9)
    do
      val init = bytes(initial, initial)
      val app = bytes(m, m + 1)
      sameSlices(appendAll(ida, init, List(app)), ida.encode(init ++ app))
  }

  test("long sequences of 1-byte appends") {
    val idas = List[IDA & AppendableIDA](
      AppendingReedSolomon(5, 3, 4, 8), AppendingReedSolomon(3, 1, 2, 8), Replication(3, 2))
    for ida <- idas do
      val data = bytes(300, 7)
      val slices = appendAll(ida, new Array[Byte](0), data.toSeq.map(b => Array(b)))
      ida.restoreArray(segmentsFor(slices, 0 until ida.restoreThreshold)).toSeq should be (data.toSeq)
      sameSlices(slices, ida.encode(data))
  }

  test("random append sequences") {
    val rnd = new Random(0xA99E)
    for _ <- 0 until 50 do
      val ida = Configs(rnd.nextInt(Configs.length))
      val appends = List.fill(1 + rnd.nextInt(40))(bytes(1 + rnd.nextInt(3000), rnd.nextInt()))
      val slices = appendAll(ida, new Array[Byte](0), appends)
      val idxs = rnd.shuffle((0 until ida.width).toList).take(ida.restoreThreshold)
      ida.restoreArray(segmentsFor(slices, idxs)).toSeq should be (appends.flatten.toSeq)
  }

  test("delta only touches rows from the old final row onward") {
    val ida = AppendingReedSolomon(5, 3, 4, 64)
    ida.appendDeltas(10000, bytes(1024, 3)).foreach { d =>
      d.regions.length should be (1)
      d.regions.head.offset should be ((10000 / 192) * 64)
      d.regions.head.bytes.length should be (d.newSliceLength - d.regions.head.offset)
    }
  }

  test("matches rejects wrong slice length and wrong trailer") {
    val ida = AppendingReedSolomon(5, 3, 4, 8)
    val slices = ida.encode(bytes(50, 1))
    val deltas = ida.appendDeltas(50, bytes(10, 2))
    for i <- 0 until ida.width do
      val s = slices(i)
      deltas(i).matches(DataBuffer(s)) should be (true)
      deltas(i).matches(DataBuffer(s.take(s.length - ida.cellSize))) should be (false)
      val flipped = s.clone()
      flipped(flipped.length - 1) = (flipped(flipped.length - 1) ^ 1).toByte
      deltas(i).matches(DataBuffer(flipped)) should be (i != ida.restoreThreshold - 1)
  }

  test("applyTo throws when the delta does not match") {
    val ida = AppendingReedSolomon(5, 3, 4, 8)
    val slices = ida.encode(bytes(50, 1))
    val deltas = ida.appendDeltas(50, bytes(10, 2))
    an [IllegalArgumentException] should be thrownBy deltas(0).applyTo(DataBuffer(slices(0).take(8)))
  }

  test("encode/decode round trips") {
    val withTail = AppendDelta(16, 32, 8, Array.tabulate[Byte](24)(_.toByte), Some(Array[Byte](9, 8, 7, 6, 5, 4, 3, 2)))
    val without = AppendDelta(0, 8, 0, Array.tabulate[Byte](8)(i => (i + 5).toByte), None)
    AppendDelta.decode(withTail.encode) should be (withTail)
    AppendDelta.decode(without.encode) should be (without)
    AppendDelta.decode(withTail.encode).hashCode should be (withTail.hashCode)
    withTail should not be without
  }

  test("decode rejects truncated input") {
    val d = AppendDelta(16, 32, 8, Array.tabulate[Byte](24)(_.toByte), Some(Array[Byte](9, 8, 7, 6, 5, 4, 3, 2)))
    val enc = d.encode.getByteArray
    for n <- 0 until enc.length do
      an [IllegalArgumentException] should be thrownBy AppendDelta.decode(DataBuffer(enc.take(n)))
  }

  test("decode rejects inconsistent header fields") {
    val tail = Some(Array[Byte](1, 2, 3, 4))
    val bad = List(
      (16, 8, 0, Array[Byte](1), None),            // shrinking slice
      (-1, 8, 0, Array[Byte](1), None),            // negative expected length
      (0, -8, 0, Array[Byte](), None),             // negative new length
      (0, 8, -1, Array[Byte](1), None),            // negative offset
      (0, 8, 7, Array[Byte](1, 2), None),          // xor range past new length
      (2, 8, 0, Array[Byte](1), tail),             // tail longer than expected slice
      (0, 8, 0, Array[Byte](), None))              // empty region
    bad.foreach { (e, n, o, x, t) =>
      val enc = ByteBuffer.allocate(22 + x.length + t.map(_.length).getOrElse(0))
      enc.put(1.toByte).putInt(e).putInt(n).putInt(o).putInt(x.length)
      enc.put(if t.isDefined then 1.toByte else 0.toByte).putInt(t.map(_.length).getOrElse(0))
      enc.put(x)
      t.foreach(a => enc.put(a))
      an [IllegalArgumentException] should be thrownBy AppendDelta.decode(DataBuffer(enc.array()))
    }
  }

  private def v1Payload(e: Int, n: Int, o: Int, x: Array[Byte], t: Option[Array[Byte]]): DataBuffer =
    val enc = ByteBuffer.allocate(22 + x.length + t.map(_.length).getOrElse(0))
    enc.put(1.toByte).putInt(e).putInt(n).putInt(o).putInt(x.length)
    enc.put(if t.isDefined then 1.toByte else 0.toByte).putInt(t.map(_.length).getOrElse(0))
    enc.put(x)
    t.foreach(a => enc.put(a))
    DataBuffer(enc.array())

  test("decoding a version 1 payload yields one region") {
    val x = Array.tabulate[Byte](24)(_.toByte)
    val tail = Array[Byte](9, 8, 7, 6, 5, 4, 3, 2)
    val d = AppendDelta.decode(v1Payload(16, 32, 8, x, Some(tail)))
    d.regions should be (List(XorRegion(8, x)))
    d should be (AppendDelta(16, 32, 8, x, Some(tail)))
  }

  test("multiple distant regions XOR only their own bytes") {
    val d = AppendDelta(64, 64, List(XorRegion(2, Array[Byte](1, 1)), XorRegion(50, Array[Byte](3))), None)
    val src = bytes(64, 11)
    val out = d.applyTo(DataBuffer(src)).getByteArray
    out.indices.foreach { i =>
      val x = if i == 2 || i == 3 then 1 else if i == 50 then 3 else 0
      out(i) should be ((src(i) ^ x).toByte)
    }
  }

  test("version 2 round trip with multiple regions and a region beyond the expected length") {
    val d = AppendDelta(16, 48,
      List(XorRegion(4, Array[Byte](1, 2, 3)), XorRegion(40, Array.tabulate[Byte](8)(i => (i + 1).toByte))),
      Some(Array[Byte](5, 6)))
    val dec = AppendDelta.decode(d.encode)
    dec should be (d)
    dec.hashCode should be (d.hashCode)
    val enc = d.encode.getByteArray
    for n <- 0 until enc.length do
      an [IllegalArgumentException] should be thrownBy AppendDelta.decode(DataBuffer(enc.take(n)))
    val none = AppendDelta(0, 0, Nil, None)
    AppendDelta.decode(none.encode) should be (none)
  }

  test("invalid region lists are rejected") {
    def r(o: Int, n: Int) = XorRegion(o, Array.fill[Byte](n)(1))
    an [IllegalArgumentException] should be thrownBy AppendDelta(8, 32, List(r(4, 8), r(8, 2)), None)
    an [IllegalArgumentException] should be thrownBy AppendDelta(8, 32, List(r(10, 2), r(2, 2)), None)
    an [IllegalArgumentException] should be thrownBy AppendDelta(8, 32, List(r(2, 0)), None)
    an [IllegalArgumentException] should be thrownBy AppendDelta(8, 32, List(r(30, 4)), None)
    an [IllegalArgumentException] should be thrownBy AppendDelta(8, 32, List(r(-1, 4)), None)
    AppendDelta(8, 32, List(r(2, 2), r(4, 2)), None).regions.length should be (2)
  }
