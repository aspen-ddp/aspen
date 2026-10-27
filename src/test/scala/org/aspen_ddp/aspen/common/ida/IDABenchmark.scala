package org.aspen_ddp.aspen.common.ida

import scala.util.Random

/** Single-threaded microbenchmark comparing ReedSolomon against RandomAccessReedSolomon at two cell sizes.
  * Run with: sbt 'set Test / run / fork := true' 'Test/runMain org.aspen_ddp.aspen.common.ida.IDABenchmark'
  */
object IDABenchmark:

  val Width = 16
  val K = 10
  val WriteThreshold = 12

  val idas: List[(String, IDA)] = List(
    "RS" -> ReedSolomon(Width, K, WriteThreshold),
    "RARS-16" -> RandomAccessReedSolomon(Width, K, WriteThreshold, 16),
    "RARS-64" -> RandomAccessReedSolomon(Width, K, WriteThreshold, 64))

  val rars: List[(String, RandomAccessReedSolomon)] = idas.collect { case (n, r: RandomAccessReedSolomon) => n -> r }

  var sink: Long = 0

  /** Median ns/op over several timed trials, after a warmup */
  def time(f: () => Any): Double =
    def trial(minNanos: Long): Double =
      var ops = 0L
      val start = System.nanoTime()
      var now = start
      while now - start < minNanos do
        var i = 0
        while i < 16 do
          f() match
            case a: Array[?] => sink += a.length
            case r: RestoredRange => sink += r.data.length
            case _ => sink += 1
          i += 1
        ops += 16
        now = System.nanoTime()
      (now - start).toDouble / ops
    trial(400_000_000L)
    val trials = Array.fill(7)(trial(120_000_000L)).sorted
    trials(trials.length / 2)

  def fmt(ns: Double): String =
    if ns >= 1e6 then f"${ns / 1e6}%8.2f ms"
    else if ns >= 1e3 then f"${ns / 1e3}%8.2f us"
    else f"$ns%8.0f ns"

  def fmtSize(n: Long): String =
    if n >= (1 << 20) && n % (1 << 20) == 0 then s"${n >> 20}MB"
    else if n >= 1024 && n % 1024 == 0 then s"${n >> 10}KB"
    else s"${n}B"

  def bytes(n: Int, seed: Int): Array[Byte] =
    val a = new Array[Byte](n)
    new Random(seed).nextBytes(a)
    a

  def deltaBytes(ds: Array[AppendDelta]): Long =
    ds.iterator.map(d => d.regions.map(_.bytes.length.toLong).sum + d.expectedTail.map(_.length).getOrElse(0)).sum

  def row(cols: Seq[String]): Unit =
    println(cols.head.padTo(16, ' ') + cols.tail.map(c => c.reverse.padTo(22, ' ').reverse).mkString)

  def main(args: Array[String]): Unit =
    val sizes = List(16, 64, 256, 1024, 4096, 16384, 65536, 1 << 20, 4 << 20)

    println(s"width=$Width k=$K writeThreshold=$WriteThreshold  JVM ${System.getProperty("java.version")}")

    println("\n== Stored bytes (all slices) ==")
    row("size" :: idas.map(_._1))
    sizes.foreach { n =>
      row(fmtSize(n) :: idas.map((_, ida) => (ida.calculateEncodedSegmentLength(n).toLong * Width).toString))
    }

    println("\n== encode (ns/op, median) ==")
    row("size" :: idas.map(_._1) ::: List("R16/RS", "R64/RS"))
    sizes.foreach { n =>
      val data = bytes(n, n)
      val t = idas.map((_, ida) => time(() => ida.encode(data)))
      row(fmtSize(n) :: t.map(fmt) ::: List(f"${t(1) / t(0)}%.2fx", f"${t(2) / t(0)}%.2fx"))
    }

    for (label, drop) <- List("restore, all data slices present" -> Set.empty[Int],
                              "restore, data slices 0 and 1 lost (decode)" -> Set(0, 1)) do
      println(s"\n== $label ==")
      row("size" :: idas.map(_._1) ::: List("R16/RS", "R64/RS"))
      sizes.foreach { n =>
        val data = bytes(n, n)
        val t = idas.map { (_, ida) =>
          val shards = ida.encode(data)
          val segs = (0 until Width).filterNot(drop.contains).take(K).map(i => (i.toByte, shards(i))).toList
          require(java.util.Arrays.equals(ida.restoreArray(segs), data))
          time(() => ida.restoreArray(segs))
        }
        row(fmtSize(n) :: t.map(fmt) ::: List(f"${t(1) / t(0)}%.2fx", f"${t(2) / t(0)}%.2fx"))
      }

    // Updates against a large existing object. RS has no partial update, so its cost is a full re-encode
    for objSize <- List(64 * 1024, 1 << 20) do
      println(s"\n== updates to a ${fmtSize(objSize)} object: client time / total bytes sent to stores ==")
      row("op" :: idas.map(_._1))
      val obj = bytes(objSize, 7)

      for appendSize <- List(64, 4096) do
        val data = bytes(appendSize, 9)
        val grown = obj ++ data
        val cells = idas.map {
          case (_, ida: RandomAccessReedSolomon) =>
            s"${fmt(time(() => ida.appendDeltas(objSize, data))).trim} / ${deltaBytes(ida.appendDeltas(objSize, data))}"
          case (_, ida) =>
            s"${fmt(time(() => ida.encode(grown))).trim} / ${ida.calculateEncodedSegmentLength(grown.length).toLong * Width}"
        }
        row(s"append ${fmtSize(appendSize)}" :: cells)

      for editSize <- List(16, 4096) do
        val off = objSize / 2 + 123
        val edit = RangeEdit(off, bytes(editSize, 11), obj.slice(off, off + editSize))
        val edited = obj.clone()
        System.arraycopy(edit.newBytes, 0, edited, off, editSize)
        val cells = idas.map {
          case (_, ida: RandomAccessReedSolomon) =>
            s"${fmt(time(() => ida.rangedWriteDeltas(objSize, List(edit)))).trim} / ${deltaBytes(ida.rangedWriteDeltas(objSize, List(edit)))}"
          case (_, ida) =>
            s"${fmt(time(() => ida.encode(edited))).trim} / ${ida.calculateEncodedSegmentLength(objSize).toLong * Width}"
        }
        row(s"write ${fmtSize(editSize)}" :: cells)

      // Range read of 4KB from the middle: client decode time / bytes fetched from k stores
      val readOff = objSize / 2 + 123
      val readLen = 4096
      for (label, drop) <- List("read 4KB" -> Set.empty[Int], "read 4KB dgr" -> Set(0, 1)) do
        val cells = idas.map {
          case (_, ida: RandomAccessReedSolomon) =>
            val shards = ida.encode(obj)
            val sliceSize = shards(0).length
            val (s, e0) = ida.sliceRangeFor(readOff, readLen)
            val e = math.min(e0, sliceSize)
            val segs = (0 until Width).filterNot(drop.contains).take(K)
              .map(i => (i.toByte, java.util.Arrays.copyOfRange(shards(i), s, e))).toList
            val r = ida.restoreRange(s, sliceSize, segs)
            require(java.util.Arrays.equals(r.data.slice(readOff - r.logicalOffset, readOff - r.logicalOffset + readLen),
              obj.slice(readOff, readOff + readLen)))
            s"${fmt(time(() => ida.restoreRange(s, sliceSize, segs))).trim} / ${(e - s).toLong * K}"
          case (_, ida) =>
            val shards = ida.encode(obj)
            val segs = (0 until Width).filterNot(drop.contains).take(K).map(i => (i.toByte, shards(i))).toList
            s"${fmt(time(() => ida.restoreArray(segs))).trim} / ${shards(0).length.toLong * K}"
        }
        row(label :: cells)

    println(s"\n(sink $sink)")
