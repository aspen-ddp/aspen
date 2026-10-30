package org.aspen_ddp.aspen.server.store.backend.fsif

import java.io.IOException

import scala.util.Random

import org.aspen_ddp.aspen.FileBasedTests

class SimulatedFsifFilesSuite extends FileBasedTests:

  // java.util.Random seeded with consecutive small ints yields correlated first draws; scramble them
  private def rng(seed: Int) = new Random(splitMix(seed))

  private def splitMix(seed: Int): Long =
    var z = seed * 0x9e3779b97f4a7c15L + 0x9e3779b97f4a7c15L
    z = (z ^ (z >>> 30)) * 0xbf58476d1ce4e5b9L
    z = (z ^ (z >>> 27)) * 0x94d049bb133111ebL
    z ^ (z >>> 31)

  private def filled(n: Int, v: Int): Array[Byte] = Array.fill(n)(v.toByte)

  test("forced writes survive crash"):
    for seed <- 0 until 20 do
      val fs = new SimulatedFsifFiles(rng(seed))
      val f = fs.open("a")
      f.write(0, filled(1500, 7))
      f.force()
      f.write(1500, filled(100, 9)) // not forced; must not disturb the rest
      val c = fs.crash()
      c.exists("a") shouldBe true
      val r = c.open("a").read(0, 1500)
      withClue(s"seed=$seed")(r shouldBe filled(1500, 7))

  test("reads and size see unforced writes"):
    val fs = new SimulatedFsifFiles(new Random(1))
    fs.size("a") shouldBe 0L
    fs.exists("a") shouldBe false
    val f = fs.open("a")
    f.write(10, filled(5, 3))
    f.size shouldBe 15
    fs.size("a") shouldBe 15L
    f.read(0, 20) shouldBe (Array.fill(10)(0.toByte) ++ filled(5, 3))
    f.read(12, 100) shouldBe filled(3, 3)
    f.read(100, 4) shouldBe Array.empty[Byte]

  test("unforced writes are sometimes applied, absent or torn"):
    var applied, absent, torn = 0
    for seed <- 0 until 200 do
      val fs = new SimulatedFsifFiles(rng(seed))
      val f = fs.open("a")
      f.write(0, new Array[Byte](2048))
      f.force()
      f.write(0, filled(2048, 1))
      val r = fs.crash().open("a").read(0, 2048)
      r.length shouldBe 2048
      val sectors = r.grouped(512).map(s => s.toSet).toList
      withClue(s"seed=$seed"):
        sectors.foreach(s => s.size shouldBe 1) // torn only at 512-byte boundaries
      val ones = sectors.count(_ == Set(1.toByte))
      if ones == 4 then applied += 1
      else if ones == 0 then absent += 1
      else torn += 1
    applied should be > 0
    absent should be > 0
    torn should be > 0

  test("crash is deterministic per seed"):
    def run(seed: Int): List[Byte] =
      val fs = new SimulatedFsifFiles(rng(seed))
      val f = fs.open("a")
      for i <- 0 until 8 do f.write(i * 700L, filled(700, i + 1))
      fs.crash().open("a").read(0, 10000).toList
    run(5) shouldBe run(5)

  test("pending ops apply in order and a kept write beyond the end extends the file"):
    var extended = false
    for seed <- 0 until 100 do
      val fs = new SimulatedFsifFiles(rng(seed))
      val f = fs.open("a")
      f.write(2000, filled(10, 5))
      val r = fs.crash().open("a").read(0, 3000)
      if r.nonEmpty then
        extended = true
        // the gap is zero-filled, and the file never ends before the last applied byte
        r.take(2000).forall(_ == 0) shouldBe true
    extended shouldBe true

  test("a truncate followed by force is durable"):
    for seed <- 0 until 20 do
      val fs = new SimulatedFsifFiles(rng(seed))
      val f = fs.open("a")
      f.write(0, filled(1000, 4))
      f.force()
      f.truncate(100)
      f.force()
      val c = fs.crash().open("a")
      withClue(s"seed=$seed")(c.size shouldBe 100)

  test("an unforced truncate is applied or skipped as a whole"):
    var kept, skipped = 0
    for seed <- 0 until 100 do
      val fs = new SimulatedFsifFiles(rng(seed))
      val f = fs.open("a")
      f.write(0, filled(1000, 4))
      f.force()
      f.truncate(100)
      fs.crash().open("a").size match
        case 100 => kept += 1
        case 1000 => skipped += 1
        case other => fail(s"seed=$seed size=$other")
    kept should be > 0
    skipped should be > 0

  test("growing truncate zero-fills and truncate then regrow reads zeros"):
    val fs = new SimulatedFsifFiles(new Random(1))
    val f = fs.open("a")
    f.write(0, filled(10, 9))
    f.truncate(4)
    f.truncate(8)
    f.read(0, 20) shouldBe (filled(4, 9) ++ new Array[Byte](4))

  test("failNext(OutOfSpace) makes the next write throw, once, with no effect"):
    val fs = new SimulatedFsifFiles(new Random(1))
    val f = fs.open("a")
    fs.failNext(Fault.OutOfSpace)
    val e = intercept[IOException](f.write(0, filled(10, 1)))
    FsifIo.isOutOfSpace(e) shouldBe true
    f.size shouldBe 0
    f.write(0, filled(10, 1))
    f.size shouldBe 10

  test("a faulted force discards the pending ops: reads still see them, but no later force makes them durable"):
    for (fault, seed) <- List(Fault.OutOfSpace, Fault.IoError).zipWithIndex do
      val fs = new SimulatedFsifFiles(rng(seed))
      val f = fs.open("a")
      f.write(0, filled(1024, 1))
      f.force()
      f.write(0, filled(512, 2))
      fs.failNext(fault, ops = Set(FaultOp.Force))
      intercept[IOException](f.force())
      fs.faultCount("a", FaultOp.Force) shouldBe 1
      f.read(0, 512) shouldBe filled(512, 2) // still in the page cache
      f.write(512, filled(512, 3))
      f.force() // succeeds, but covers only the write made after the failure
      val r = fs.crash().open("a").read(0, 1024)
      withClue(s"$fault: ")(r shouldBe (filled(512, 1) ++ filled(512, 3)))

  test("IoError is not out of space"):
    val fs = new SimulatedFsifFiles(new Random(1))
    val f = fs.open("a")
    fs.failNext(Fault.IoError)
    val e = intercept[IOException](f.force())
    FsifIo.isOutOfSpace(e) shouldBe false
    FsifIo.isOutOfSpace(new RuntimeException("x", new IOException("No space left on device"))) shouldBe true

  test("failWhile persists until the predicate is false"):
    val fs = new SimulatedFsifFiles(new Random(1))
    val f = fs.open("a")
    @volatile var failing = true
    fs.failWhile(Fault.IoError)(failing)
    intercept[IOException](f.write(0, filled(1, 1)))
    intercept[IOException](f.truncate(0))
    intercept[IOException](f.force())
    f.size shouldBe 0
    failing = false
    f.write(0, filled(1, 1))
    f.size shouldBe 1

  test("targeted faults skip non-matching files and ops"):
    val fs = new SimulatedFsifFiles(new Random(1))
    val journal = fs.open("journal")
    val other = fs.open("index")
    fs.failNext(Fault.IoError, file = _ == "journal", ops = Set(FaultOp.Force))
    other.write(0, filled(4, 1))
    other.force()                       // wrong file
    journal.write(0, filled(4, 1))      // wrong op
    journal.truncate(2)                 // wrong op
    intercept[IOException](journal.force())
    journal.force()                     // spent
    fs.failWhile(Fault.OutOfSpace, file = _ == "journal", ops = Set(FaultOp.Write))(true)
    other.write(4, filled(4, 2))
    intercept[IOException](journal.write(0, filled(1, 1)))
    journal.force()                     // wrong op

  test("a crashed instance cannot be used"):
    val fs = new SimulatedFsifFiles(new Random(1))
    val f = fs.open("a")
    fs.crash()
    intercept[IllegalStateException](f.write(0, filled(1, 1)))
    intercept[IllegalStateException](fs.open("b"))

  test("concurrent writers do not corrupt the simulator"):
    val fs = new SimulatedFsifFiles(new Random(1))
    val f = fs.open("a")
    val threads = (0 until 4).map: t =>
      new Thread(() => for i <- 0 until 200 do f.write((t * 200 + i) * 4L, filled(4, t + 1)))
    threads.foreach(_.start())
    threads.foreach(_.join())
    f.size shouldBe 3200
    val all = f.read(0, 3200)
    for t <- 0 until 4; i <- 0 until 200 do
      all((t * 200 + i) * 4) shouldBe (t + 1).toByte

  test("NioFsifFiles round-trips write, read, truncate and force"):
    val files = new NioFsifFiles(tdir)
    files.exists("x") shouldBe false
    files.size("x") shouldBe 0L
    files.exists("x") shouldBe false
    val f = files.open("x")
    files.exists("x") shouldBe true
    f.write(0, filled(100, 3))
    f.write(200, filled(10, 4))
    f.size shouldBe 210
    files.size("x") shouldBe 210L
    f.read(95, 10) shouldBe (filled(5, 3) ++ new Array[Byte](5))
    f.read(205, 100) shouldBe filled(5, 4)
    f.read(500, 4) shouldBe Array.empty[Byte]
    f.truncate(50)
    f.size shouldBe 50
    f.truncate(60)
    f.size shouldBe 60
    f.read(50, 10) shouldBe new Array[Byte](10)
    f.force()
    f.close()
    val g = files.open("x")
    g.size shouldBe 60
    g.read(0, 50) shouldBe filled(50, 3)
    g.close()
