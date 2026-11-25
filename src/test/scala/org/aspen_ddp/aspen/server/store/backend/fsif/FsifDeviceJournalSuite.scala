package org.aspen_ddp.aspen.server.store.backend.fsif

import java.io.IOException
import java.util.UUID

import scala.collection.mutable
import scala.concurrent.Future
import scala.util.{Failure, Random, Success}

import org.scalatest.funsuite.AnyFunSuite

import org.aspen_ddp.aspen.common.objects.ObjectId

class FsifDeviceJournalSuite extends AnyFunSuite:

  private val sizing = JournalSizing(new UUID(5, 6), 3, 64 * 1024)
  /** Blocks (batches of one small append) per file */
  private val perFile = (sizing.fileSize / 4096 - 1).toInt
  private val dir = os.Path("/fsif-test/store")

  private def rec(i: Int): JournalRecord = JournalRecord.Mapping(i, i & 63, ObjectId(new UUID(i, ~i)))

  /** Record bytes of `n` Mapping records */
  private def bytesOf(n: Int): Long = n.toLong * DeviceJournalFormat.taggedRecordSize(rec(0))

  /** A zero-filled ring, as DeviceJournalFiles.create leaves it */
  private def newFiles(seed: Long = 1, sz: JournalSizing = sizing): SimulatedFsifFiles =
    val files = new SimulatedFsifFiles(new Random(seed))
    for n <- 0 until sz.files do
      val f = files.open(DeviceJournalScan.fileName(n))
      f.write(0, new Array[Byte](sz.fileSize.toInt))
      f.force()
      f.close()
    files

  /** Completions run on `ec` when given, else at once */
  private final class TestOwner(ec: Option[ManualEc] = None) extends FsifDeviceJournal.Owner:
    var rooms = 0
    var checkpoints = 0
    val failures = mutable.ArrayBuffer[Throwable]()
    def complete(task: Runnable): Unit = ec match
      case Some(e) => e.execute(task)
      case None => task.run()
    def roomAvailable(): Unit = rooms += 1
    def checkpointRequested(): Unit = checkpoints += 1
    def journalFailed(t: Throwable): Unit = failures += t

  private def direct(files: FsifFiles): FsifDeviceJournal =
    new FsifDeviceJournal(files, sizing, None, r => r.run(), forceAfterWrite = true)

  private def value(f: Future[Long]): Long = f.value match
    case Some(Success(s)) => s
    case other => fail(s"not completed: $other")

  private def scanBatches(files: FsifFiles): Vector[Batch] = DeviceJournalScan.scan(files, sizing) match
    case Scan.Valid(bs, _) => bs
    case other => fail(other.toString)

  test("appends from two attachments made meanwhile share one batch"):
    val io = new ManualEc
    val store = new ManualEc
    val files = newFiles()
    val j = new FsifDeviceJournal(files, sizing, None, io.execute, forceAfterWrite = true)
    val (a, b) = (UUID.randomUUID(), UUID.randomUUID())
    j.attach(a, dir / "a", new TestOwner(Some(store)))
    j.attach(b, dir / "b", new TestOwner(Some(store)))
    assert(j.tailSerial == -1)
    val fa = j.append(a, List(rec(1)))
    val fb1 = j.append(b, List(rec(2)))
    val fb2 = j.append(b, List(rec(3)))
    io.runOne() // the batch holding fa alone
    io.runOne() // fb1 and fb2 together
    assert(io.pending == 0)
    assert(!fa.isCompleted && !fb1.isCompleted) // completions wait for the store's executor
    store.runAll()
    assert(value(fa) == 0 && value(fb1) == 1 && value(fb2) == 1)
    assert(j.tailSerial == 1)
    assert(scanBatches(files).map(_.records) == Vector(
      Vector(TaggedRecord(a, rec(1))), Vector(TaggedRecord(b, rec(2)), TaggedRecord(b, rec(3)))))

  test("serials rise by one per batch and survive a scan"):
    val io = new ManualEc
    val files = newFiles()
    val j = new FsifDeviceJournal(files, sizing, None, io.execute, forceAfterWrite = true)
    val ids = Vector.fill(2)(UUID.randomUUID())
    ids.foreach(id => j.attach(id, dir / id.toString, new TestOwner))
    val rnd = new Random(3)
    val appended = (0 until 50).map: i =>
      val id = ids(i % 2)
      val f = j.append(id, List(rec(i), rec(i + 1000)))
      if rnd.nextInt(3) == 0 then io.runOne()
      (id, i, f)
    io.runAll()
    val serials = appended.map(t => value(t._3))
    assert(serials.head == 0)
    assert(serials.sliding(2).forall(p => p(1) == p(0) || p(1) == p(0) + 1))
    val batches = scanBatches(files)
    assert(batches.map(_.serial) == (0L to serials.last).toVector)
    val expected = appended.groupBy(t => value(t._3)).toVector.sortBy(_._1).map: (_, ts) =>
      ts.sortBy(_._2).flatMap(t => Vector(TaggedRecord(t._1, rec(t._2)), TaggedRecord(t._1, rec(t._2 + 1000))))
    assert(batches.map(_.records) == expected)

  test("the writer enters the next file only when no unreleased attachment needs it"):
    val files = newFiles()
    val j = direct(files)
    val (a, b) = (UUID.randomUUID(), UUID.randomUUID())
    val (oa, ob) = (new TestOwner, new TestOwner)
    j.attach(a, dir / "a", oa)
    j.attach(b, dir / "b", ob)
    val sa = value(j.append(a, List(rec(0))))
    // B fills the rest of file 0, then files 1 and 2, covering only what lies in file 0
    for i <- 1 until 3 * perFile do
      assert(j.tryReserve(b, bytesOf(1)))
      val s = value(j.append(b, List(rec(i))))
      j.unreserve(b, bytesOf(1))
      if i < perFile then j.covered(b, s)
    assert(j.tailSerial == 3L * perFile - 1)
    assert(!j.tryReserve(b, bytesOf(1)))
    assert(oa.checkpoints >= 1 && ob.checkpoints == 0)
    // An unreserved append waits for file 0
    val waiting = j.append(b, List(rec(9999)))
    assert(!waiting.isCompleted)
    assert(ob.rooms == 0)
    j.covered(a, sa)
    assert(value(waiting) == 3L * perFile)
    assert(ob.rooms == 1)
    assert(j.tryReserve(b, bytesOf(1)))

  test("an idle attachment pins nothing"):
    val files = newFiles()
    val j = direct(files)
    val (a, b) = (UUID.randomUUID(), UUID.randomUUID())
    val oa = new TestOwner
    j.attach(a, dir / "a", oa)
    j.attach(b, dir / "b", new TestOwner)
    for i <- 0 until 3 * 3 * perFile do
      assert(j.tryReserve(b, bytesOf(1)), s"append $i")
      val s = value(j.append(b, List(rec(i))))
      j.unreserve(b, bytesOf(1))
      j.covered(b, s)
    assert(j.tailSerial == 9L * perFile - 1)
    assert(oa.checkpoints == 0)

  test("a released attachment pins nothing"):
    val files = newFiles()
    val j = direct(files)
    val (a, b) = (UUID.randomUUID(), UUID.randomUUID())
    j.attach(a, dir / "a", new TestOwner)
    j.attach(b, dir / "b", new TestOwner)
    value(j.append(a, List(rec(0))))
    j.releaseDirectory(dir / "a")
    for i <- 0 until 2 * 3 * perFile do
      val s = value(j.append(b, List(rec(i))))
      j.covered(b, s)

  test("checkpointRequested goes to the owners pinning the oldest file"):
    val files = newFiles()
    val j = direct(files)
    val (a, b, d) = (UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID())
    val (oa, ob, od) = (new TestOwner, new TestOwner, new TestOwner)
    j.attach(a, dir / "a", oa)
    j.attach(b, dir / "b", ob)
    j.attach(d, dir / "d", od)
    value(j.append(a, List(rec(0)))) // pins file 0
    for i <- 1 until perFile do // d fills file 0 and stays idle
      j.covered(d, value(j.append(d, List(rec(i)))))
    // b fills file 1 and file 2 up to just below 3/4
    val threeQuarters = (perFile * 3 + 3) / 4
    for i <- 0 until perFile + threeQuarters - 1 do
      value(j.append(b, List(rec(i))))
    assert(oa.checkpoints == 0)
    value(j.append(b, List(rec(7777))))
    assert(oa.checkpoints == 1 && ob.checkpoints == 0 && od.checkpoints == 0)
    // Once per oldest needed serial
    value(j.append(b, List(rec(7778))))
    assert(oa.checkpoints == 1)

  test("a checkpoint covering below its owner's oldest needed serial is followed by another request"):
    val files = newFiles()
    val j = direct(files)
    val (a, b) = (UUID.randomUUID(), UUID.randomUUID())
    val (oa, ob) = (new TestOwner, new TestOwner)
    j.attach(a, dir / "a", oa)
    j.attach(b, dir / "b", ob)
    value(j.append(a, List(rec(0)))) // serial 0, file 0
    val sa = value(j.append(a, List(rec(1)))) // serial 1, file 0: a's head stays here after covering 0
    for i <- 2 until perFile do
      j.covered(b, value(j.append(b, List(rec(i)))))
    for i <- 0 until perFile + (perFile * 3 + 3) / 4 do // into file 2 past 3/4
      value(j.append(b, List(rec(i))))
    assert(oa.checkpoints == 1)
    // A checkpoint whose snapshot predated the request covers only serial 0: a still pins file 0 at serial 1
    j.covered(a, sa - 1)
    assert(oa.checkpoints == 2)
    // Covering below its head again (nothing changes) still asks again
    j.covered(a, sa - 1)
    assert(oa.checkpoints == 3)
    j.covered(a, sa)
    assert(oa.checkpoints == 3 && ob.checkpoints == 0)

  test("a refused reservation asks the owners pinning the oldest file to checkpoint"):
    val files = newFiles()
    val j = direct(files)
    val (a, b) = (UUID.randomUUID(), UUID.randomUUID())
    val (oa, ob) = (new TestOwner, new TestOwner)
    j.attach(a, dir / "a", oa)
    j.attach(b, dir / "b", ob)
    value(j.append(a, List(rec(0))))
    for i <- 1 until perFile + 2 do // fill file 0, then 2 batches in file 1
      j.covered(b, value(j.append(b, List(rec(i)))))
    // File 2 is free, file 0 pinned: the room is file 1's 13 blocks and file 2's 15
    val big = bytesOf(1300) // c = 15 blocks, charged 2c - 4096 = 29 blocks
    assert(!j.tryReserve(b, big))
    assert(oa.checkpoints == 1 && ob.checkpoints == 0)
    assert(j.tryReserve(b, bytesOf(1)))
    j.unreserve(b, bytesOf(1))
    assert(ob.rooms == 0)
    j.release(a)
    assert(ob.rooms == 1)
    assert(j.tryReserve(b, big))

  test("a reservation larger than one file less its header is refused"):
    val j = direct(newFiles())
    val (a, oa) = (UUID.randomUUID(), new TestOwner)
    j.attach(a, dir / "a", oa)
    val usable = sizing.fileSize - DeviceJournalFormat.HeaderSize
    assert(!j.tryReserve(a, usable - 19))
    assert(j.tryReserve(a, usable - 20))
    j.unreserve(a, usable - 20)
    assert(oa.rooms == 0 && oa.checkpoints == 0)

  test("a fresh ring reserves one block per small append, across every file"):
    val j = direct(newFiles())
    val a = UUID.randomUUID()
    j.attach(a, dir / "a", new TestOwner)
    for _ <- 0 until 3 * perFile do
      assert(j.tryReserve(a, bytesOf(1)))
    assert(!j.tryReserve(a, bytesOf(1)))

  test("an append under a successful reservation is written without waiting for a file"):
    for seed <- 0 until 40 do
      val rnd = new Random(seed)
      val io = new ManualEc
      val files = newFiles(seed)
      val j = new FsifDeviceJournal(files, sizing, None, io.execute, forceAfterWrite = true)
      val (a, b) = (UUID.randomUUID(), UUID.randomUUID())
      j.attach(a, dir / "a", new TestOwner)
      j.attach(b, dir / "b", new TestOwner)
      j.append(a, List(rec(0))) // a never covers: file 0 stays pinned
      io.runAll()
      val made = mutable.ArrayBuffer[Future[Long]]()
      var refusals = 0
      var i = 0
      while refusals < 5 do
        val n = if rnd.nextBoolean() then 1 + rnd.nextInt(90) else 1 + rnd.nextInt(1300)
        if j.tryReserve(b, bytesOf(n)) then
          val f = j.append(b, List.tabulate(n)(k => rec(i * 2000 + k)))
          f.foreach(_ => j.unreserve(b, bytesOf(n)))(using scala.concurrent.ExecutionContext.parasitic)
          made += f
        else
          refusals += 1
        i += 1
        while rnd.nextInt(3) == 0 && io.pending > 0 do io.runOne()
      io.runAll()
      withClue(s"seed=$seed"):
        assert(made.nonEmpty)
        made.foreach(f => value(f))
        assert(j.failed.isEmpty)

  test("a write failure fails every pending append and calls journalFailed once per owner"):
    val io = new ManualEc
    val files = newFiles()
    val j = new FsifDeviceJournal(files, sizing, None, io.execute, forceAfterWrite = true)
    val (a, b) = (UUID.randomUUID(), UUID.randomUUID())
    val (oa, ob) = (new TestOwner, new TestOwner)
    j.attach(a, dir / "a", oa)
    j.attach(b, dir / "b", ob)
    val fs = List(j.append(a, List(rec(1))), j.append(b, List(rec(2))), j.append(a, List(rec(3))))
    files.failNext(Fault.IoError, ops = Set(FaultOp.Write))
    io.runAll()
    fs.foreach(f => assert(f.value.exists(_.isFailure)))
    assert(oa.failures.size == 1 && ob.failures.size == 1)
    assert(j.failed.contains(oa.failures.head))
    j.append(a, List(rec(4))).value match
      case Some(Failure(t)) => assert(t eq oa.failures.head)
      case other => fail(other.toString)
    assert(!j.tryReserve(a, bytesOf(1)))
    assertThrows[IOException](j.attach(UUID.randomUUID(), dir / "c", new TestOwner))
    assert(oa.failures.size == 1 && ob.failures.size == 1)

  test("after recovery the writer enters the next file and continues the serials"):
    val files = newFiles()
    val j = direct(files)
    val a = UUID.randomUUID()
    j.attach(a, dir / "a", new TestOwner)
    for i <- 0 until 3 do value(j.append(a, List(rec(i))))
    val Scan.Valid(_, tail) = DeviceJournalScan.scan(files, sizing): @unchecked
    val j2 = new FsifDeviceJournal(files, sizing, Some(tail), r => r.run(), forceAfterWrite = true)
    assert(j2.tailSerial == 2)
    val b = UUID.randomUUID()
    j2.attach(b, dir / "b", new TestOwner)
    // Every file is free: the room spans the whole ring
    for _ <- 0 until 3 * perFile do assert(j2.tryReserve(b, bytesOf(1)))
    assert(value(j2.append(b, List(rec(10)))) == 3)
    val Scan.Valid(bs, tail2) = DeviceJournalScan.scan(files, sizing): @unchecked
    assert(tail2.file == 1)
    assert(bs.map(_.serial) == Vector(3L))
    assert(bs.head.records == Vector(TaggedRecord(b, rec(10))))

  test("crash at every op of 20 appends and 2 file switches scans Valid and holds every completed append"):
    // 100 Mappings per append: two blocks each, 7 batches per file
    def scenario(files: SimulatedFsifFiles): Vector[(Long, List[JournalRecord])] =
      val j = direct(files)
      val a = UUID.randomUUID()
      j.attach(a, dir / "a", new TestOwner)
      val done = (0 until 20).flatMap: i =>
        val records = List.tabulate(100)(k => rec(i * 100 + k))
        j.append(a, records).value match
          case Some(Success(s)) => Some(s -> records)
          case _ => None
      done.toVector
    val probe = newFiles()
    val before = probe.opCount
    val all = scenario(probe)
    assert(all.size == 20)
    assert(all.map(_._1) == (0L until 20).toVector)
    assert(scanBatches(probe).size == 20)
    val ops = (probe.opCount - before).toInt
    for k <- 0 until ops do
      val files = newFiles(k)
      files.crashAfterOps(k)
      val done = scenario(files)
      val crashed = files.crash()
      withClue(s"k=$k"):
        DeviceJournalScan.scan(crashed, sizing) match
          case Scan.Empty => assert(done.isEmpty)
          case Scan.Valid(bs, _) =>
            assert(bs.map(_.serial) == (0L until bs.size).toVector)
            done.foreach: (s, records) =>
              assert(s < bs.size)
              assert(bs(s.toInt).records.map(_.record) == records.toVector)
          case other => fail(other.toString)

  test("needsReplay holds while an unreleased attachment has records above its covered serial"):
    val io = new ManualEc
    val files = newFiles()
    val j = new FsifDeviceJournal(files, sizing, None, io.execute, forceAfterWrite = true)
    val (a, never) = (UUID.randomUUID(), UUID.randomUUID())
    j.attach(a, dir / "a", new TestOwner)
    assert(!j.needsReplay(a) && !j.needsReplay(never))
    val f = j.append(a, List(rec(1)))
    assert(j.needsReplay(a)) // queued, not yet written
    io.runAll()
    assert(j.needsReplay(a))
    j.covered(a, value(f))
    assert(!j.needsReplay(a))
    j.append(a, List(rec(2)))
    io.runAll()
    assert(j.needsReplay(a))
    j.release(a)
    assert(!j.needsReplay(a))

  test("after recovery to a header-only tail file the writer re-enters that file"):
    // Lifetime 1 fills files 0 and 1; a crash after file 2's entry header, before its batch
    val files = newFiles()
    val j = direct(files)
    val a = UUID.randomUUID()
    j.attach(a, dir / "a", new TestOwner)
    for i <- 0 until 2 * perFile do
      j.covered(a, value(j.append(a, List(rec(i)))))
    val n = 2L * perFile
    val f2 = files.open(DeviceJournalScan.fileName(2))
    f2.write(0, DeviceJournalFormat.encodeHeader(FileHeader(sizing.journalId, 42L, n, n)))
    f2.force()
    f2.close()
    val Scan.Valid(_, tail) = DeviceJournalScan.scan(files, sizing): @unchecked
    assert(tail == Tail(2, DeviceJournalFormat.HeaderSize, n, 42L))
    // Lifetime 2: its first batch goes into file 2, not file 0 (which would also start at serial n)
    val j2 = new FsifDeviceJournal(files, sizing, Some(tail), r => r.run(), forceAfterWrite = true)
    val b = UUID.randomUUID()
    j2.attach(b, dir / "b", new TestOwner)
    assert(value(j2.append(b, List(rec(99)))) == n)
    val Scan.Valid(bs, tail2) = DeviceJournalScan.scan(files, sizing): @unchecked
    assert(tail2.file == 2 && tail2.nextSerial == n + 1 && tail2.salt != 42L)
    assert(bs.map(_.records) == Vector(Vector(TaggedRecord(b, rec(99)))))

  test("crash at every op of restarts wrapping the ring, then of the run after it, loses no acknowledged append"):
    // Three batches per file; each lifetime makes five appends, so three lifetimes wrap the ring of nine
    val small = JournalSizing(new UUID(7, 8), 3, 4 * 4096L)
    final class Chain(var needed: Map[Long, Vector[JournalRecord]], var next: Int, var headerOnlyTails: Int):
      def copy(): Chain = new Chain(needed, next, headerOnlyTails)

      /** A device load's scan: every acknowledged append its attachment has not covered is there. None when the
        * scan's own I/O hit the crash; else the writer's start (replay then covers everything) */
      def recover(files: SimulatedFsifFiles): Option[Option[Tail]] =
        val scan =
          try Some(DeviceJournalScan.scan(files, small))
          catch case _: IOException => None
        scan.map:
          case Scan.Empty =>
            assert(needed.isEmpty)
            None
          case Scan.Valid(bs, tail) =>
            assert(bs.map(_.serial) == bs.headOption.map(h => (h.serial until h.serial + bs.size).toVector)
              .getOrElse(Vector.empty))
            val bySerial = bs.map(b => b.serial -> b.records.map(_.record)).toMap
            needed.foreach((s, records) => assert(bySerial.get(s).contains(records), s"acknowledged batch $s"))
            needed = Map.empty
            if tail.nextPos == DeviceJournalFormat.HeaderSize then
              headerOnlyTails += 1
            Some(tail)
          case other => fail(other.toString)

      /** `lifetimes` device loads, each recovering then appending five times, until the crash stops the I/O */
      def run(files: SimulatedFsifFiles, lifetimes: Int): Unit =
        var go = true
        var l = 0
        while go && l < lifetimes do
          recover(files) match
            case None => go = false
            case Some(start) =>
              val j = new FsifDeviceJournal(files, small, start, r => r.run(), forceAfterWrite = true)
              val a = UUID.randomUUID()
              j.attach(a, dir / "a", new TestOwner)
              var i = 0
              while go && i < 5 do
                val records = List(rec(next))
                next += 1
                j.append(a, records).value match
                  case Some(Success(s)) =>
                    // The two newest stay uncovered, so the scan must find them
                    j.covered(a, s - 2)
                    needed = needed.filter(_._1 > s - 2) + (s -> records.toVector)
                  case _ => go = false
                i += 1
              j.close()
          l += 1

    def opsOf(files: SimulatedFsifFiles, chain: Chain): Int =
      val before = files.opCount
      chain.run(files, 3)
      (files.opCount - before).toInt
    val outerOps = opsOf(newFiles(sz = small), new Chain(Map.empty, 0, 0))
    var headerOnly = 0
    for k <- 0 to outerOps do
      val files = newFiles(k, small)
      files.crashAfterOps(k)
      val outer = new Chain(Map.empty, 0, 0)
      outer.run(files, 3)
      val base = files.crash()
      val innerOps = opsOf(base.snapshot(), outer.copy())
      for i <- 0 to innerOps do
        withClue(s"k=$k i=$i: "):
          val f = base.snapshot()
          f.crashAfterOps(i)
          val inner = outer.copy()
          inner.run(f, 3)
          assert(inner.recover(f.crash()).isDefined)
          headerOnly += inner.headerOnlyTails
    // The sweep reached the state the fix is for: recoveries to a tail file holding no batch of its pass
    assert(headerOnly > 0)

  /** Every callback throws */
  private final class ThrowingOwner extends FsifDeviceJournal.Owner:
    var calls = 0
    private def boom(): Nothing =
      calls += 1
      throw new IllegalStateException("owner callback")
    def complete(task: Runnable): Unit = boom()
    def roomAvailable(): Unit = boom()
    def checkpointRequested(): Unit = boom()
    def journalFailed(t: Throwable): Unit = boom()

  test("owner callbacks that throw stop neither the next write nor the other owners"):
    val io = new ManualEc
    val files = newFiles()
    val j = new FsifDeviceJournal(files, sizing, None, io.execute, forceAfterWrite = true)
    val (a, b) = (UUID.randomUUID(), UUID.randomUUID())
    val bad = new ThrowingOwner
    j.attach(a, dir / "a", bad)
    j.attach(b, dir / "b", new TestOwner)
    j.append(a, List(rec(1)))
    val fb = j.append(b, List(rec(2))) // queued behind the batch in flight
    io.runOne()
    assert(bad.calls == 1)
    assert(io.pending == 1) // the next write was submitted
    io.runOne()
    assert(value(fb) == 1)
    // And the journal still writes after it
    assert(value({ val f = j.append(b, List(rec(3))); io.runAll(); f }) == 2)

  test("a journalFailed that throws still calls the other owners and completes whenFailed"):
    val io = new ManualEc
    val files = newFiles()
    val j = new FsifDeviceJournal(files, sizing, None, io.execute, forceAfterWrite = true)
    val (a, b) = (UUID.randomUUID(), UUID.randomUUID())
    val bad = new ThrowingOwner
    val good = new TestOwner
    j.attach(a, dir / "a", bad)
    j.attach(b, dir / "b", good)
    val fb = j.append(b, List(rec(1)))
    j.append(a, List(rec(2)))
    files.failNext(Fault.IoError, ops = Set(FaultOp.Write))
    io.runAll()
    assert(fb.value.exists(_.isFailure))
    assert(good.failures.size == 1)
    assert(j.whenFailed.value.exists(_.isSuccess))
