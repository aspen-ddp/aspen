package org.aspen_ddp.aspen.server.store.backend.fsif

import java.io.IOException
import java.util.UUID
import java.util.concurrent.{ConcurrentLinkedQueue, CountDownLatch, Executors, TimeUnit}
import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}

import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Random, Success, Try}

import org.aspen_ddp.aspen.FileBasedTests
import org.aspen_ddp.aspen.common.objects.ObjectId

class JournalWriterSuite extends FileBasedTests:
  import JournalFormat.Scan
  import JournalWriterSuite.*

  private def rng(seed: Int) = new Random(splitMix(seed))

  private def splitMix(seed: Int): Long =
    var z = seed * 0x9e3779b97f4a7c15L + 0x9e3779b97f4a7c15L
    z = (z ^ (z >>> 30)) * 0xbf58476d1ce4e5b9L
    z = (z ^ (z >>> 27)) * 0x94d049bb133111ebL
    z ^ (z >>> 31)

  private def rec(i: Int): JournalRecord = JournalRecord.Mapping(i, math.floorMod(i, 64), ObjectId(new UUID(i, ~i)))

  /** Single-threaded harness: the serial executor runs on a manual EC and io runs only when released */
  private final class Harness(seed: Int = 0, startBatches: Int = 0):
    val rnd: Random = rng(seed)
    var files = new SimulatedFsifFiles(rnd)
    val file: FsifFile = files.open("journal.a")
    val salt: Long = FsifRecovery.writeJournalHeader(file, 3)
    // Optionally continue an existing journal
    private val prefix = (0 until startBatches).map(b => JournalFormat.encodeBatch(salt, b, List(rec(-1 - b))))
    prefix.foldLeft(JournalFormat.HeaderSize.toLong): (pos, bytes) =>
      file.write(pos, bytes)
      pos + bytes.length
    file.force()
    val prefixRecords: Vector[JournalRecord] = (0 until startBatches).map(b => rec(-1 - b)).toVector
    val startLength: Long = JournalFormat.HeaderSize + prefix.map(_.length).sum
    val serialEc = new ManualEc
    val io = new ManualEc
    val serial = new SerialExecutor(serialEc)
    val errors = mutable.ArrayBuffer[Throwable]()
    val writer = new JournalWriter(file, startLength, startBatches, salt, io, serial, t => errors += t)

    def onSerial[A](f: => A): A =
      val out = new AtomicReference[Try[A]]()
      serial.execute(() => out.set(Try(f)))
      serialEc.runAll()
      out.get().get

    def append(records: List[JournalRecord]): Future[Unit] = onSerial(writer.append(records))

    /** Runs every released io task and the serial work it posts, until nothing is left */
    def pump(): Unit =
      while io.pending > 0 || serialEc.pending > 0 do
        io.runAll()
        serialEc.runAll()

    def scan(bytes: Array[Byte] = file.read(0, file.size.toInt)): Scan = JournalFormat.scan(bytes)

  private def done(f: Future[Unit]): Boolean = f.value.contains(Success(()))

  test("SerialExecutor runs 10,000 tasks from 8 threads one at a time, in per-thread order"):
    val pool = Executors.newFixedThreadPool(8)
    try
      val serial = new SerialExecutor(ExecutionContext.fromExecutor(pool))
      val active = new AtomicInteger(0)
      val maxActive = new AtomicInteger(0)
      val ran = new AtomicInteger(0)
      val outOfOrder = new AtomicInteger(0)
      val last = Array.fill(8)(-1) // only touched inside serial tasks
      val latch = new CountDownLatch(10000)
      val start = new CountDownLatch(1)
      val submitters = (0 until 8).map: t =>
        val th = new Thread(() =>
          start.await()
          for i <- 0 until 1250 do
            serial.execute: () =>
              val n = active.incrementAndGet()
              maxActive.accumulateAndGet(n, math.max)
              if last(t) != i - 1 then outOfOrder.incrementAndGet()
              last(t) = i
              ran.incrementAndGet()
              active.decrementAndGet()
              latch.countDown()
        )
        th.start()
        th
      start.countDown()
      submitters.foreach(_.join(5000))
      latch.await(5, TimeUnit.SECONDS) shouldBe true
      ran.get shouldBe 10000
      maxActive.get shouldBe 1
      outOfOrder.get shouldBe 0
    finally pool.shutdownNow()

  test("SerialExecutor reports a throwing task and keeps running later tasks"):
    val reported = new ConcurrentLinkedQueue[Throwable]()
    val ec = new ManualEc(t => reported.add(t))
    val serial = new SerialExecutor(ec)
    val ran = mutable.ArrayBuffer[Int]()
    serial.execute(() => ran += 1)
    serial.execute(() => throw new RuntimeException("boom"))
    serial.execute(() => ran += 3)
    ec.runAll()
    serial.execute(() => ran += 4)
    ec.runAll()
    ran.toList shouldBe List(1, 3, 4)
    reported.size shouldBe 1
    reported.peek().getMessage shouldBe "boom"

  test("SerialExecutor keeps running later tasks after a task throws a fatal Throwable"):
    for fatal <- List[Throwable](new InterruptedException("stop"), new scala.util.control.ControlThrowable {}) do
      withClue(fatal.getClass.getName):
        val ec = new ManualEc()
        val serial = new SerialExecutor(ec)
        val ran = mutable.ArrayBuffer[Int]()
        serial.execute(() => ran += 1)
        serial.execute(() => throw fatal)
        serial.execute(() => ran += 3)
        val escaped = intercept[Throwable](ec.runAll())
        escaped shouldBe theSameInstanceAs(fatal)
        ec.runAll()
        serial.execute(() => ran += 4)
        ec.runAll()
        ran.toList shouldBe List(1, 3, 4)

  test("ten appends while the first batch is held produce exactly two batches"):
    val h = new Harness
    val first = h.append(List(rec(0)))
    h.io.pending shouldBe 1
    val rest = (1 to 10).map(i => h.append(List(rec(i))))
    h.io.pending shouldBe 1 // at most one batch in flight
    h.onSerial(h.writer.idle) shouldBe false
    h.onSerial(h.writer.length) shouldBe
      h.startLength + JournalFormat.encodeBatch(h.salt, 0, List(rec(0))).length +
        JournalFormat.encodeBatch(h.salt, 1, (1 to 10).map(rec).toList).length
    h.pump()
    (first +: rest).forall(done) shouldBe true
    h.scan() match
      case Scan.Valid(_, _, recs, len, next) =>
        next shouldBe 2
        recs shouldBe (0 to 10).map(rec).toVector
        len shouldBe h.file.size
        h.onSerial(h.writer.length) shouldBe len
      case other => fail(other.toString)
    h.onSerial(h.writer.idle) shouldBe true

  test("continues an existing journal with its next batch number"):
    val h = new Harness(startBatches = 3)
    val f = h.append(List(rec(7)))
    h.pump()
    done(f) shouldBe true
    h.scan() match
      case Scan.Valid(_, _, recs, _, next) =>
        next shouldBe 4
        recs shouldBe h.prefixRecords :+ rec(7)
      case other => fail(other.toString)

  test("futures complete on the serial executor"):
    val h = new Harness
    val f = h.append(List(rec(1)))
    val onSerial = new AtomicReference[Option[Boolean]](None)
    // Completion callbacks run synchronously where the promise is completed
    f.onComplete(_ => onSerial.set(Some(SerialMark.get)))(using ExecutionContext.parasitic)
    h.serialEc.wrap = r => () => { SerialMark.set(true); try r.run() finally SerialMark.set(false) }
    h.pump()
    onSerial.get shouldBe Some(true)

  test("every acknowledged record survives crash"):
    for seed <- 0 until 10 do
      val h = new Harness(seed)
      val acked = mutable.ArrayBuffer[JournalRecord]()
      var i = 0
      for round <- 0 until 20 do
        val batch = (0 until h.rnd.nextInt(4)).map(_ => { i += 1; rec(i) }).toList
        h.append(batch).foreach(_ => acked ++= batch)(using ExecutionContext.parasitic)
        if h.rnd.nextBoolean() then h.pump()
      val crashed = h.files.crash()
      val f = crashed.open("journal.a")
      withClue(s"seed=$seed"):
        h.scan(f.read(0, f.size.toInt)) match
          case Scan.Valid(_, _, recs, _, _) => recs.take(acked.size) shouldBe acked.toVector
          case other => fail(other.toString)

  test("an IoError on force truncates back to the last durable length"):
    val h = new Harness
    h.append(List(rec(0)))
    h.pump()
    val durable = h.file.size
    h.files.failNext(Fault.IoError, ops = Set(FaultOp.Force))
    val f = h.append((1 to 30).map(rec).toList)
    h.pump()
    h.errors.size shouldBe 1
    h.errors.head shouldBe a[IOException]
    h.errors.head.getMessage shouldBe "Input/output error"
    f.isCompleted shouldBe false
    h.file.size shouldBe durable
    h.onSerial(h.writer.idle) shouldBe false
    h.io.pending shouldBe 0 // nothing more is sent until retry or failAll

  for op <- List(FaultOp.Write, FaultOp.Force) do
    test(s"a faulted $op leaves no garbage tail"):
      val h = new Harness
      h.append(List(rec(0)))
      h.pump()
      val durable = h.file.size
      h.files.failNext(Fault.OutOfSpace, ops = Set(op))
      h.append((1 to 30).map(rec).toList)
      h.pump()
      h.errors.size shouldBe 1
      h.file.size shouldBe durable

  test("a failed write followed by retry completes the futures with contiguous batch numbers"):
    val h = new Harness
    val a = h.append(List(rec(0)))
    h.pump()
    h.files.failNext(Fault.OutOfSpace, ops = Set(FaultOp.Write))
    val b = h.append(List(rec(1), rec(2)))
    h.pump()
    h.errors.size shouldBe 1
    FsifIo.isOutOfSpace(h.errors.head) shouldBe true
    b.isCompleted shouldBe false
    // Buffered during the error: these go in the batch after the retried one
    val c = h.append(List(rec(3)))
    h.io.pending shouldBe 0
    h.onSerial(h.writer.retry())
    h.pump()
    List(a, b, c).forall(done) shouldBe true
    h.errors.size shouldBe 1
    h.scan() match
      case Scan.Valid(_, _, recs, len, next) =>
        next shouldBe 3
        recs shouldBe (0 to 3).map(rec).toVector
        len shouldBe h.file.size
      case other => fail(other.toString)

  test("retry truncates first when the recovery truncate also failed"):
    val h = new Harness
    h.append(List(rec(0)))
    h.pump()
    val durable = h.file.size
    var failing = true
    h.files.failWhile(Fault.IoError, ops = Set(FaultOp.Force, FaultOp.Truncate))(failing)
    val b = h.append((1 to 30).map(rec).toList)
    h.pump()
    h.errors.size shouldBe 1
    h.file.size should be > durable // the torn batch is still there
    failing = false
    // The retry's first op is the truncate: a fault on it is reported and nothing is written
    h.files.failNext(Fault.IoError, ops = Set(FaultOp.Truncate))
    h.onSerial(h.writer.retry())
    h.pump()
    h.errors.size shouldBe 2
    b.isCompleted shouldBe false
    h.onSerial(h.writer.retry())
    h.pump()
    h.file.size should be > durable
    done(b) shouldBe true
    h.scan() match
      case Scan.Valid(_, _, recs, len, next) =>
        next shouldBe 2
        recs shouldBe (0 to 30).map(rec).toVector
        len shouldBe h.file.size
      case other => fail(other.toString)

  test("failAll fails pending and later appends"):
    val h = new Harness
    h.files.failNext(Fault.IoError, ops = Set(FaultOp.Write))
    val a = h.append(List(rec(0)))
    h.pump()
    val b = h.append(List(rec(1)))
    h.errors.size shouldBe 1
    val boom = new IOException("fatal")
    h.onSerial(h.writer.failAll(boom))
    val c = h.append(List(rec(2)))
    h.pump()
    for f <- List(a, b, c) do f.value shouldBe Some(Failure(boom))
    h.io.pending shouldBe 0
    h.onSerial(h.writer.retry()) // no effect once permanently failed
    h.pump()
    h.io.pending shouldBe 0
    h.scan() match
      case Scan.Valid(_, _, recs, _, next) =>
        next shouldBe 0
        recs shouldBe empty
      case other => fail(other.toString)

  test("failAll while a batch is in flight fails it and ignores its completion"):
    val h = new Harness
    val a = h.append(List(rec(0)))
    val b = h.append(List(rec(1)))
    val boom = new IOException("fatal")
    h.onSerial(h.writer.failAll(boom))
    h.pump()
    a.value shouldBe Some(Failure(boom))
    b.value shouldBe Some(Failure(boom))
    h.io.pending shouldBe 0

  test("random faults, retries and crashes never lose an acknowledged record or corrupt the journal"):
    for seed <- 0 until 30 do
      val h = new Harness(seed, startBatches = seed % 3)
      val acked = mutable.ArrayBuffer[JournalRecord]()
      val appended = mutable.ArrayBuffer[JournalRecord]()
      var i = 0
      var handled = 0
      val steps = 20 + h.rnd.nextInt(60)
      for step <- 0 until steps do
        h.rnd.nextInt(5) match
          case 0 | 1 =>
            // Large enough batches span several sectors, so torn writes happen
            val batch = (0 until h.rnd.nextInt(40)).map(_ => { i += 1; rec(i) }).toList
            appended ++= batch
            h.append(batch).foreach(_ => acked ++= batch)(using ExecutionContext.parasitic)
          case 2 =>
            val fault = if h.rnd.nextBoolean() then Fault.OutOfSpace else Fault.IoError
            val op = FaultOp.values(h.rnd.nextInt(FaultOp.values.length))
            h.files.failNext(fault, ops = Set(op))
          case 3 =>
            h.io.runOne()
            h.serialEc.runAll()
          case _ =>
            h.pump()
        if h.errors.size > handled then
          handled = h.errors.size
          h.onSerial(h.writer.retry())
      val crashed = h.files.crash()
      val f = crashed.open("journal.a")
      withClue(s"seed=$seed"):
        h.scan(f.read(0, f.size.toInt)) match
          case Scan.Valid(_, _, recs, _, _) =>
            recs.take(h.prefixRecords.size) shouldBe h.prefixRecords
            val mine = recs.drop(h.prefixRecords.size)
            mine.take(acked.size) shouldBe acked.toVector
            appended.take(mine.size).toVector shouldBe mine
          case other => fail(other.toString)

object JournalWriterSuite:
  val SerialMark: ThreadLocal[Boolean] = ThreadLocal.withInitial(() => false)

  /** Queues runnables; they run only when the test calls runOne/runAll */
  final class ManualEc(report: Throwable => Unit = t => throw t) extends ExecutionContext:
    private val queue = new ConcurrentLinkedQueue[Runnable]()
    @volatile var wrap: Runnable => Runnable = r => r
    def execute(r: Runnable): Unit = queue.add(r)
    def reportFailure(t: Throwable): Unit = report(t)
    def pending: Int = queue.size
    def runOne(): Unit = Option(queue.poll()).foreach(r => wrap(r).run())
    /** Includes tasks queued by the tasks it runs */
    def runAll(): Unit =
      while !queue.isEmpty do runOne()
