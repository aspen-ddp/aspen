package org.aspen_ddp.aspen.server.store.backend.fsif

import java.io.IOException
import java.nio.ByteBuffer
import java.util.UUID
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicBoolean

import scala.collection.mutable
import scala.concurrent.{Await, ExecutionContext, Future, Promise}
import scala.concurrent.duration.*
import scala.util.{Random, Try}

import org.aspen_ddp.aspen.FileBasedTests
import org.aspen_ddp.aspen.common.{DataBuffer, HLCTimestamp}
import org.aspen_ddp.aspen.common.objects.{DataObjectPointer, Metadata, ObjectId, ObjectPointer, ObjectRefcount,
  ObjectRevision, ObjectType, ReadError}
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.{ReadState, StoreId}
import org.aspen_ddp.aspen.common.transaction.TransactionId
import org.aspen_ddp.aspen.server.store.backend.{Commit, CommitError, CommitState, Completion, CompletionHandler,
  Read, UpdateForm}

class FsifFaultSuite extends FileBasedTests:
  import FsifBackendSuite.{GateEc, InlineEc, ShuffleEc, isInPlace}
  import FsifFaultSuite.*

  private val layout = FsifLayout(1024, 64)
  private val poolId = PoolId(new UUID(7, 7))
  private val storeId = StoreId(poolId, 0)
  private val Timeout = 10.seconds
  private val retry = 10.millis

  private def rng(seed: Int) = new Random(splitMix(seed))

  private def splitMix(seed: Int): Long =
    var z = seed * 0x9e3779b97f4a7c15L + 0x9e3779b97f4a7c15L
    z = (z ^ (z >>> 30)) * 0xbf58476d1ce4e5b9L
    z = (z ^ (z >>> 27)) * 0x94d049bb133111ebL
    z ^ (z >>> 31)

  private def oid(i: Int) = ObjectId(new UUID(i, ~i))
  private def sp(x: Int): Array[Byte] = ByteBuffer.allocate(4).putInt(x).array()
  private def ptr(x: Int, id: ObjectId): ObjectPointer = DataObjectPointer(id, poolId, sp(x))
  private def md(rev: Int, refcount: Int = 1) =
    Metadata(ObjectRevision(TransactionId(new UUID(rev, 6))), ObjectRefcount(rev, refcount), HLCTimestamp(rev))
  private def bytes(n: Int, v: Int): Array[Byte] = Array.tabulate[Byte](n)(j => (v + j * 7).toByte)

  private def alloc(p: ObjectPointer, data: Array[Byte], rev: Int): CommitState =
    CommitState(p, md(rev), ObjectType.Data, DataBuffer(data), allocating = true)
  private def overwrite(p: ObjectPointer, data: Array[Byte], rev: Int): CommitState =
    CommitState(p, md(rev), ObjectType.Data, DataBuffer(data))
  private def append(p: ObjectPointer, old: Array[Byte], add: Array[Byte], rev: Int): CommitState =
    CommitState(p, md(rev), ObjectType.Data, DataBuffer(old ++ add),
      form = UpdateForm.Ranges(old.length, List((old.length, add.length))))
  private def patched(old: Array[Byte], off: Int, patch: Array[Byte]): Array[Byte] =
    val data = old.clone()
    System.arraycopy(patch, 0, data, off, patch.length)
    data
  private def writeRange(p: ObjectPointer, old: Array[Byte], off: Int, patch: Array[Byte], rev: Int): CommitState =
    CommitState(p, md(rev), ObjectType.Data, DataBuffer(patched(old, off, patch)),
      form = UpdateForm.Ranges(old.length, List((off, patch.length))))
  private def delete(p: ObjectPointer, rev: Int): CommitState =
    CommitState(p, md(rev, 0), ObjectType.Data, DataBuffer())

  type CommitResult = Either[Unit, CommitError.Value]
  type ReadResult = Either[ReadState, ReadError.Value]

  private def acked(f: Future[CommitResult]): Boolean = f.value.exists(_.toOption.contains(Left(())))

  private final class Recorder extends CompletionHandler:
    private val commits = mutable.Map[(ObjectId, TransactionId), Promise[CommitResult]]()
    private val reads = mutable.Map[ObjectId, mutable.Queue[Promise[ReadResult]]]()
    val unexpected = new ConcurrentLinkedQueue[Completion]()

    def expectCommit(id: ObjectId, tx: TransactionId): Future[CommitResult] = synchronized:
      val p = Promise[CommitResult]()
      commits((id, tx)) = p
      p.future

    def expectRead(id: ObjectId): Future[ReadResult] = synchronized:
      val p = Promise[ReadResult]()
      reads.getOrElseUpdate(id, mutable.Queue()).enqueue(p)
      p.future

    def complete(op: Completion): Unit = synchronized:
      op match
        case c: Commit =>
          commits.remove((c.objectId, c.transactionId)) match
            case Some(p) => p.success(c.result)
            case None => unexpected.add(c)
        case r: Read =>
          reads.get(r.objectId).filter(_.nonEmpty) match
            case Some(q) => q.dequeue().success(r.result)
            case None => unexpected.add(r)

  /** A backend over SimulatedFsifFiles. With `manual`, retries run only on `tick()` (a fresh ManualScheduler per
    * incarnation, so a crashed incarnation's retries are dropped with it); otherwise the shared production timer.
    */
  private final class Harness(seed: Int = 0, mkIo: () => ExecutionContext = () => InlineEc,
                              tuning: FsifTuning = FsifTuning(cowThreshold = 256, enospcRetry = 10.millis),
                              manual: Boolean = true):
    var files = new SimulatedFsifFiles(rng(seed))
    FsifRecovery.initialize(files, layout)
    var fatals = new ConcurrentLinkedQueue[Throwable]()
    var io: ExecutionContext = mkIo()
    var sched: Option[ManualScheduler] = None
    var rec = new Recorder
    var backend: FsifBackend = open()
    private var txn = 0

    private def open(): FsifBackend =
      sched = if manual then Some(new ManualScheduler) else None
      val fs = fatals
      val b = sched match
        case Some(s) => new FsifBackend(storeId, tdir / "fsif", layout, files, io, tuning, t => fs.add(t), s)
        case None => new FsifBackend(storeId, tdir / "fsif", layout, files, io, tuning, t => fs.add(t))
      b.setCompletionHandler(rec)
      b

    /** A new incarnation on the same files, as after a clean close (nothing is dropped). Rules stay in force. */
    def reopen(): Unit =
      io = mkIo()
      rec = new Recorder
      fatals = new ConcurrentLinkedQueue[Throwable]()
      backend = open()

    /** Faults are not carried over: the new instance starts without rules */
    def crash(): Unit =
      files = files.crash()
      io = mkIo()
      rec = new Recorder
      fatals = new ConcurrentLinkedQueue[Throwable]()
      backend = open()

    def pump(): Unit = io match
      case m: ShuffleEc => m.runAll()
      case _ => ()

    /** Runs one round of retries (manual scheduler only) and the io they start */
    def tick(): Unit =
      sched.foreach(_.tick())
      pump()

    def await[A](f: Future[A]): A =
      pump()
      sched.foreach: s =>
        var rounds = 0
        while !f.isCompleted && rounds < 10000 do
          s.tick()
          pump()
          rounds += 1
      Await.result(f, Timeout)

    def issueCommit(cs: CommitState): Future[CommitResult] =
      txn += 1
      val tx = TransactionId(new UUID(seed, txn))
      val f = rec.expectCommit(cs.objectId, tx)
      backend.commit(cs, tx)
      f

    def commitOk(cs: CommitState): Unit = await(issueCommit(cs)) shouldBe Left(())

    def issueRead(p: ObjectPointer): Future[ReadResult] =
      val f = rec.expectRead(p.id)
      backend.read(p)
      f

    def read(p: ObjectPointer): ReadResult =
      val f = issueRead(p)
      pump()
      Await.result(f, Timeout) // never ticks: reads must not need a retry to complete

    def readData(p: ObjectPointer): List[Byte] = read(p) match
      case Left(rs) => rs.data.getByteArray.toList
      case Right(err) => fail(s"read of ${p.id} failed: $err")

    def readOpt(p: ObjectPointer): Option[List[Byte]] = read(p) match
      case Left(rs) => Some(rs.data.getByteArray.toList)
      case Right(ReadError.ObjectNotFound) => None
      case Right(err) => fail(s"read of ${p.id} failed: $err")

    def quiesce(): Unit = await(backend.quiesce())

    def slotOf(x: Int): Int = await(backend.mapping(x))._1

    def contents(name: String): Array[Byte] =
      val f = files.open(name)
      try f.read(0, f.size.toInt)
      finally f.close()

    def indexSeq(name: String): Option[Long] = IndexFile.decode(contents(name), layout.segmentCount).map(_._1)

  /** Runs `body` on another thread and fails if it takes longer than the hang guard */
  private def guarded[A](body: => A): Try[A] =
    Try(Await.result(Future(body)(using ExecutionContext.global), Timeout))

  private def isJournal(name: String): Boolean = name.startsWith("journal")

  // ---- The brief's four tests (the production timer, retrying every 10 ms) ----

  test("ENOSPC on the journal holds the commit and freezes writes; reads still work; clearing it completes the commit"):
    val h = new Harness(manual = false)
    val p1 = ptr(1, oid(1))
    h.commitOk(alloc(p1, bytes(40, 1), 1))
    val full = new AtomicBoolean(true)
    h.files.failWhile(Fault.OutOfSpace, isJournal)(full.get)
    val p2 = ptr(2, oid(2))
    val f = h.issueCommit(alloc(p2, bytes(60, 2), 1))
    f.isCompleted shouldBe false
    h.backend.isWriteFrozen shouldBe true
    h.readData(p1) shouldBe bytes(40, 1).toList
    // A backend commit is a decided transaction, so a read may already return it (it overlays pending Deltas)
    h.readData(p2) shouldBe bytes(60, 2).toList
    f.isCompleted shouldBe false
    full.set(false)
    h.await(f) shouldBe Left(())
    h.backend.isWriteFrozen shouldBe false
    h.quiesce()
    h.fatals.isEmpty shouldBe true
    h.crash()
    h.readData(p1) shouldBe bytes(40, 1).toList
    h.readData(p2) shouldBe bytes(60, 2).toList

  test("ENOSPC on data with acknowledged deltas pending: reads overlay them and the writes land once it clears"):
    val h = new Harness(manual = false)
    val p = ptr(1, oid(1))
    val v1 = bytes(100, 1)
    h.commitOk(alloc(p, v1, 1))
    h.quiesce()
    val full = new AtomicBoolean(true)
    h.files.failWhile(Fault.OutOfSpace, _ == FsifRecovery.DataFile)(full.get)
    h.commitOk(writeRange(p, v1, 10, bytes(8, 2), 2)) // journaled and acknowledged; its in-place write is held
    val v2 = patched(v1, 10, bytes(8, 2))
    h.backend.isWriteFrozen shouldBe true
    h.commitOk(writeRange(p, v2, 50, bytes(8, 3), 3))
    val v3 = patched(v2, 50, bytes(8, 3))
    h.readData(p) shouldBe v3.toList
    val q = h.backend.quiesce()
    q.isCompleted shouldBe false // the held in-place writes are outstanding
    h.readData(p) shouldBe v3.toList
    full.set(false)
    h.await(q)
    h.backend.isWriteFrozen shouldBe false
    val slot = h.slotOf(1)
    h.contents(FsifRecovery.DataFile).slice(
      (layout.slotOffset(slot) + SegmentHeader.Size).toInt,
      (layout.slotOffset(slot) + SegmentHeader.Size).toInt + v3.length).toList shouldBe v3.toList
    h.fatals.isEmpty shouldBe true
    h.crash()
    h.readData(p) shouldBe v3.toList

  test("an I/O error on a journal force fails the commit with IOFailure, calls onFatal once, and reads fail"):
    val h = new Harness(manual = false)
    val p = ptr(1, oid(1))
    h.commitOk(alloc(p, bytes(20, 1), 1))
    h.files.failNext(Fault.IoError, isJournal, Set(FaultOp.Force))
    h.await(h.issueCommit(overwrite(p, bytes(20, 2), 2))) shouldBe Right(CommitError.IOFailure)
    h.fatals.size shouldBe 1
    h.backend.isWriteFrozen shouldBe false
    h.read(p) shouldBe Right(ReadError.CorruptedObject)
    h.await(h.issueCommit(overwrite(p, bytes(20, 3), 3))) shouldBe Right(CommitError.IOFailure)
    h.quiesce()
    h.await(h.backend.close())
    h.fatals.size shouldBe 1

  test("with unappliedBound = 2048 and data writes gated, the 3rd 1 KiB commit's completion waits for the gate"):
    val gate = new GateEc(isInPlace)
    val h = new Harness(mkIo = () => gate, manual = false,
      tuning = FsifTuning(cowThreshold = 4096, unappliedBound = 2048, enospcRetry = retry))
    val ps = (1 to 3).map(x => ptr(x, oid(x)))
    val f1 = h.issueCommit(alloc(ps(0), bytes(1024, 1), 1))
    val f2 = h.issueCommit(alloc(ps(1), bytes(1024, 2), 1))
    val f3 = h.issueCommit(alloc(ps(2), bytes(1024, 3), 1))
    h.await(f1) shouldBe Left(())
    h.await(f2) shouldBe Left(())
    gate.heldCount should be >= 2
    f3.isCompleted shouldBe false
    h.readData(ps(2)) shouldBe bytes(1024, 3).toList // reads overlay it meanwhile
    f3.isCompleted shouldBe false
    gate.release()
    h.await(f3) shouldBe Left(())
    h.quiesce()
    h.crash()
    for i <- 0 until 3 do h.readData(ps(i)) shouldBe bytes(1024, i + 1).toList

  // ---- Frozen across components ----

  test("ENOSPC on a checkpoint's index write freezes the store until it clears, then the checkpoint completes"):
    val h = new Harness
    val ps = (0 until 4).map(x => ptr(x, oid(x + 1)))
    val vs = mutable.ArrayBuffer.tabulate(4)(i => bytes(40 + i * 100, i)) // 340 bytes goes copy-on-write
    for i <- 0 until 4 do h.commitOk(alloc(ps(i), vs(i), 1))
    h.quiesce()
    val full = new AtomicBoolean(true)
    h.files.failWhile(Fault.OutOfSpace, _ == "index.b")(full.get)
    val cp = h.backend.checkpointNow()
    cp.isCompleted shouldBe false
    h.backend.isWriteFrozen shouldBe true
    for i <- 0 until 4 do h.readData(ps(i)) shouldBe vs(i).toList
    // A commit while frozen is accepted; journal.b has space, so it may be acknowledged
    val fc = h.issueCommit(writeRange(ps(0), vs(0), 5, bytes(6, 9), 2))
    vs(0) = patched(vs(0), 5, bytes(6, 9))
    for _ <- 0 until 3 do h.tick()
    cp.isCompleted shouldBe false
    h.backend.isWriteFrozen shouldBe true
    h.readData(ps(0)) shouldBe vs(0).toList
    full.set(false)
    h.await(cp)
    h.backend.isWriteFrozen shouldBe false
    h.await(fc) shouldBe Left(())
    h.backend.journalSeq shouldBe 1
    h.indexSeq("index.b") shouldBe Some(1)
    h.quiesce()
    h.fatals.isEmpty shouldBe true
    h.crash()
    for i <- 0 until 4 do h.readData(ps(i)) shouldBe vs(i).toList

  test("ENOSPC on a compaction move's force pauses the move until it clears, then the move completes"):
    val h = new Harness
    val ps = (0 until 3).map(x => ptr(x, oid(x + 1)))
    val vs = mutable.ArrayBuffer.tabulate(3)(i => bytes(60 + i * 30, i))
    for i <- 0 until 3 do h.commitOk(alloc(ps(i), vs(i), 1))
    h.quiesce()
    h.slotOf(2) shouldBe 2
    val full = new AtomicBoolean(true)
    h.files.failWhile(Fault.OutOfSpace, _ == FsifRecovery.DataFile, Set(FaultOp.Force))(full.get)
    h.commitOk(delete(ps(0), 2)) // frees slot 0: ext 2 moves from slot 2 into it, and its force fails
    h.backend.compactionStats.started shouldBe 1
    h.backend.compactionStats.mapped shouldBe 0
    h.backend.isWriteFrozen shouldBe true
    h.readData(ps(2)) shouldBe vs(2).toList
    // A commit to the moving object while the move is paused
    h.commitOk(writeRange(ps(2), vs(2), 7, bytes(5, 4), 3))
    vs(2) = patched(vs(2), 7, bytes(5, 4))
    h.readData(ps(2)) shouldBe vs(2).toList
    h.tick()
    h.backend.compactionStats.mapped shouldBe 0
    h.backend.isWriteFrozen shouldBe true
    full.set(false)
    h.quiesce()
    h.backend.isWriteFrozen shouldBe false
    h.backend.compactionStats.mapped shouldBe 1
    h.slotOf(2) shouldBe 0
    h.backend.slotCount shouldBe 2
    h.readData(ps(2)) shouldBe vs(2).toList
    h.fatals.isEmpty shouldBe true
    h.crash()
    h.read(ps(0)) shouldBe Right(ReadError.ObjectNotFound)
    h.readData(ps(1)) shouldBe vs(1).toList
    h.readData(ps(2)) shouldBe vs(2).toList

  // ---- A failed data force drops every write since the last good one (Linux marks the pages clean) ----

  private def isData(name: String): Boolean = name == FsifRecovery.DataFile

  test("ENOSPC on the data force of a checkpoint's step 2 re-issues the in-place writes before the journal is emptied"):
    val h = new Harness
    val p = ptr(1, oid(1))
    val v1 = bytes(100, 1)
    h.commitOk(alloc(p, v1, 1))
    h.quiesce()
    val v2 = patched(v1, 10, bytes(8, 2))
    h.commitOk(writeRange(p, v1, 10, bytes(8, 2), 2)) // acknowledged and written in place, not yet forced
    h.files.failNext(Fault.OutOfSpace, isData, Set(FaultOp.Force)) // drops that write
    val cp = h.backend.checkpointNow()
    h.files.faultCount(FsifRecovery.DataFile, FaultOp.Force) shouldBe 1
    cp.isCompleted shouldBe false
    h.backend.isWriteFrozen shouldBe true
    h.readData(p) shouldBe v2.toList
    h.await(cp) // the fault is spent: the retry rewrites the Delta, forces it, and only then empties journal 0
    h.backend.journalSeq shouldBe 1
    h.backend.isWriteFrozen shouldBe false
    h.quiesce()
    h.fatals.isEmpty shouldBe true
    h.crash()
    h.readData(p) shouldBe v2.toList

  test("ENOSPC on a copy-on-write's data force re-issues the segment and another object's unforced in-place write"):
    val h = new Harness
    val p = ptr(1, oid(1))
    val q = ptr(2, oid(2))
    val vq = bytes(80, 2)
    h.commitOk(alloc(p, bytes(40, 1), 1))
    h.commitOk(alloc(q, vq, 1))
    h.quiesce()
    val vq2 = patched(vq, 20, bytes(6, 3))
    h.commitOk(writeRange(q, vq, 20, bytes(6, 3), 2)) // written in place, not yet forced
    h.files.failNext(Fault.OutOfSpace, isData, Set(FaultOp.Force))
    val vp = bytes(600, 4) // larger than cowThreshold
    val f = h.issueCommit(overwrite(p, vp, 2))
    h.files.faultCount(FsifRecovery.DataFile, FaultOp.Force) shouldBe 1
    f.isCompleted shouldBe false
    h.backend.isWriteFrozen shouldBe true
    h.await(f) shouldBe Left(())
    h.await(h.backend.checkpointNow()) // empties journal 0, which holds q's Delta
    h.quiesce()
    h.fatals.isEmpty shouldBe true
    h.crash()
    h.readData(p) shouldBe vp.toList
    h.readData(q) shouldBe vq2.toList

  test("ENOSPC on a compaction move's data force re-issues its copy into f before Map(X, f)"):
    val h = new Harness
    val ps = (0 until 3).map(x => ptr(x, oid(x + 1)))
    val vs = (0 until 3).map(i => bytes(60 + i * 30, i))
    for i <- 0 until 3 do h.commitOk(alloc(ps(i), vs(i), 1))
    h.quiesce()
    h.files.failNext(Fault.OutOfSpace, isData, Set(FaultOp.Force))
    h.commitOk(delete(ps(0), 2)) // ext 2 moves from slot 2 into slot 0; the move's force drops the copy
    h.files.faultCount(FsifRecovery.DataFile, FaultOp.Force) shouldBe 1
    h.backend.compactionStats.mapped shouldBe 0
    h.quiesce()
    h.backend.compactionStats.mapped shouldBe 1
    h.slotOf(2) shouldBe 0
    h.await(h.backend.checkpointNow())
    h.quiesce()
    h.fatals.isEmpty shouldBe true
    h.crash()
    h.readData(ps(2)) shouldBe vs(2).toList
    h.readData(ps(1)) shouldBe vs(1).toList

  test("after a data force fails with a rebuild write unmapped, that incarnation refuses further rebuild work"):
    val h = new Harness
    h.backend.rebuildWrite(ptr(1, oid(1)), ObjectType.Data, md(1), DataBuffer(bytes(30, 1)))
    h.files.failNext(Fault.OutOfSpace, isData, Set(FaultOp.Force)) // drops the segment just written
    val fl = guarded(h.backend.rebuildFlush())
    FsifIo.isOutOfSpace(fl.failed.get) shouldBe true
    // Space is back, but a flush now would map a segment that is not on disk
    val w = guarded(h.backend.rebuildWrite(ptr(2, oid(2)), ObjectType.Data, md(1), DataBuffer(bytes(30, 2))))
    FsifIo.isOutOfSpace(w.failed.get) shouldBe true
    val again = guarded(h.backend.rebuildFlush())
    FsifIo.isOutOfSpace(again.failed.get) shouldBe true
    h.crash() // a new incarnation resumes the rebuild from scratch
    h.read(ptr(1, oid(1))) shouldBe Right(ReadError.ObjectNotFound)
    h.backend.rebuildWrite(ptr(1, oid(1)), ObjectType.Data, md(1), DataBuffer(bytes(30, 1)))
    guarded(h.backend.rebuildFlush()).get
    h.crash()
    h.readData(ptr(1, oid(1))) shouldBe bytes(30, 1).toList

  test("a crash while frozen recovers the acknowledged state; held commits are old or new, never torn"):
    val cases = List[String => Boolean](_ => true, isJournal, _ == FsifRecovery.DataFile)
    for (files, c) <- cases.zipWithIndex do
      withClue(s"case=$c"):
        val h = new Harness(c)
        val n = 4
        val ps = (0 until n).map(x => ptr(x, oid(x + 1)))
        val hist = Array.fill(n)(mutable.ArrayBuffer[(Option[Array[Byte]], () => Boolean)]())
        for x <- 0 until n do
          val v = bytes(50 + x * 10, x)
          h.commitOk(alloc(ps(x), v, 1))
          hist(x) += ((Some(v), () => true))
        h.quiesce()
        // Data fills first: acknowledged Deltas pile up unapplied
        h.files.failWhile(Fault.OutOfSpace, _ == FsifRecovery.DataFile)(true)
        val old0 = hist(0).last._1.get
        val f0 = h.issueCommit(writeRange(ps(0), old0, 3, bytes(4, 7), 2))
        hist(0) += ((Some(patched(old0, 3, bytes(4, 7))), () => acked(f0)))
        h.backend.isWriteFrozen shouldBe true
        // Then the chosen files fill as well
        h.files.failWhile(Fault.OutOfSpace, files)(true)
        var rev = 3
        for x <- 0 until n do
          val old = hist(x).last._1.get
          rev += 1
          val (cs, next) =
            if x == 3 then (overwrite(ps(x), bytes(500, rev), rev), bytes(500, rev)) // copy-on-write
            else if x == 2 then (append(ps(x), old, bytes(30, rev), rev), old ++ bytes(30, rev))
            else (writeRange(ps(x), old, 1, bytes(9, rev), rev), patched(old, 1, bytes(9, rev)))
          val f = h.issueCommit(cs)
          hist(x) += ((Some(next), () => acked(f)))
        h.tick()
        h.backend.isWriteFrozen shouldBe true
        // Reads work while frozen. A held journaled commit is already visible (reads overlay pending Deltas); a held
        // copy-on-write is not, as X maps to its old slot until the new segment is forced.
        for x <- 0 until n do
          withClue(s"ext=$x while frozen"):
            val k = hist(x).lastIndexWhere(_._2())
            hist(x).drop(k).map(_._1.map(_.toList)).toList should contain(Some(h.readData(ps(x))))
        h.readData(ps(3)) shouldBe hist(3)(hist(3).size - 2)._1.get.toList
        h.crash() // the fault is cleared in the recovered instance
        for x <- 0 until n do
          withClue(s"ext=$x"):
            val k = hist(x).lastIndexWhere(_._2())
            val allowed = hist(x).drop(k).map(_._1.map(_.toList)).toList
            allowed should contain(h.readOpt(ps(x)))
        h.quiesce()
        h.fatals.isEmpty shouldBe true

  test("an I/O error during a retry while frozen is fail-stop and calls onFatal once"):
    for source <- List("journal", "data") do
      withClue(source):
        val h = new Harness
        val p = ptr(1, oid(1))
        val v1 = bytes(80, 1)
        h.commitOk(alloc(p, v1, 1))
        h.quiesce()
        val full = new AtomicBoolean(true)
        val file: String => Boolean = if source == "journal" then isJournal else _ == FsifRecovery.DataFile
        h.files.failWhile(Fault.OutOfSpace, file)(full.get)
        val f = h.issueCommit(writeRange(p, v1, 4, bytes(4, 2), 2))
        h.backend.isWriteFrozen shouldBe true
        h.tick()
        h.files.failNext(Fault.IoError, file)
        full.set(false)
        h.tick()
        h.fatals.size shouldBe 1
        h.backend.isWriteFrozen shouldBe false
        if source == "journal" then
          h.await(f) shouldBe Right(CommitError.IOFailure)
        else
          h.await(f) shouldBe Left(()) // it was durable in the journal before the in-place write failed
        h.read(p) shouldBe Right(ReadError.CorruptedObject)
        h.await(h.issueCommit(overwrite(p, bytes(20, 3), 3))) shouldBe Right(CommitError.IOFailure)
        for _ <- 0 until 3 do h.tick()
        h.quiesce()
        h.await(h.backend.close())
        h.fatals.size shouldBe 1

  test("rebuildWrite and rebuildFlush throw out-of-space instead of hanging while frozen"):
    // The store is frozen by something else
    locally:
      val h = new Harness
      val full = new AtomicBoolean(true)
      h.files.failWhile(Fault.OutOfSpace, isJournal)(full.get)
      h.backend.rebuildWrite(ptr(1, oid(1)), ObjectType.Data, md(1), DataBuffer(bytes(30, 1)))
      val r = guarded(h.backend.rebuildFlush()) // its Mappings' batch is held: frozen
      r.isFailure shouldBe true
      FsifIo.isOutOfSpace(r.failed.get) shouldBe true
      h.backend.isWriteFrozen shouldBe true
      val w = guarded(h.backend.rebuildWrite(ptr(2, oid(2)), ObjectType.Data, md(1), DataBuffer(bytes(30, 2))))
      w.isFailure shouldBe true
      FsifIo.isOutOfSpace(w.failed.get) shouldBe true
      val again = guarded(h.backend.rebuildFlush())
      again.isFailure shouldBe true
      FsifIo.isOutOfSpace(again.failed.get) shouldBe true
      // Once space returns, the held Mapping lands and a flush succeeds
      full.set(false)
      h.quiesce()
      h.backend.isWriteFrozen shouldBe false
      guarded(h.backend.rebuildFlush()).get
      h.crash()
      h.readData(ptr(1, oid(1))) shouldBe bytes(30, 1).toList
      h.read(ptr(2, oid(2))) shouldBe Right(ReadError.ObjectNotFound)
    // rebuildWrite's own write hits ENOSPC
    locally:
      val h = new Harness
      val full = new AtomicBoolean(true)
      h.files.failWhile(Fault.OutOfSpace, _ == FsifRecovery.DataFile)(full.get)
      val w = guarded(h.backend.rebuildWrite(ptr(1, oid(1)), ObjectType.Data, md(1), DataBuffer(bytes(30, 1))))
      w.isFailure shouldBe true
      FsifIo.isOutOfSpace(w.failed.get) shouldBe true
      val fl = guarded(h.backend.rebuildFlush()) // data's force fails too
      fl.isFailure shouldBe true
      FsifIo.isOutOfSpace(fl.failed.get) shouldBe true
      full.set(false)
      h.backend.rebuildWrite(ptr(1, oid(1)), ObjectType.Data, md(1), DataBuffer(bytes(30, 1)))
      guarded(h.backend.rebuildFlush()).get
      h.fatals.isEmpty shouldBe true
      h.crash()
      h.readData(ptr(1, oid(1))) shouldBe bytes(30, 1).toList

  // ---- Ruling 27: closing a frozen store abandons its held I/O as a crash would ----

  /** Awaits `f` without running retry rounds: a close must not need space to return */
  private def awaitNoRetry[A](h: Harness, f: Future[A]): A =
    h.pump()
    Await.result(f, Timeout)

  test("close of a frozen store completes without space returning, and a reopen recovers the acknowledged state"):
    for (files, c) <- List[String => Boolean](_ => true, isJournal, isData).zipWithIndex do
      withClue(s"case=$c"):
        val h = new Harness(c)
        val ps = (0 until 4).map(x => ptr(x, oid(x + 1)))
        val v = (0 until 4).map(x => bytes(60 + x * 10, x))
        for x <- 0 until 4 do h.commitOk(alloc(ps(x), v(x), 1))
        h.quiesce()
        val full = new AtomicBoolean(true)
        h.files.failWhile(Fault.OutOfSpace, isData)(full.get)
        // Acknowledged (journaled) with its in-place write held, then the chosen files fill as well
        h.commitOk(writeRange(ps(0), v(0), 3, bytes(5, 9), 2))
        val v0 = patched(v(0), 3, bytes(5, 9))
        h.backend.isWriteFrozen shouldBe true
        h.files.failWhile(Fault.OutOfSpace, files)(full.get)
        val held = List(
          h.issueCommit(writeRange(ps(1), v(1), 1, bytes(4, 7), 3)),
          h.issueCommit(overwrite(ps(2), bytes(500, 8), 3)), // copy-on-write
          h.issueCommit(delete(ps(3), 3)))
        val cp = h.backend.checkpointNow()
        h.tick()
        h.backend.isWriteFrozen shouldBe true
        awaitNoRetry(h, h.backend.close())
        h.backend.isWriteFrozen shouldBe true // still frozen: the held I/O was abandoned, not finished
        for _ <- 0 until 3 do h.tick() // a retry round after the close does nothing
        h.pump()
        h.fatals.isEmpty shouldBe true
        // Held operations never complete: the host no longer routes this store's completions
        for f <- held do f.value.forall(_ == scala.util.Success(Left(()))) shouldBe true
        cp.value.forall(_.isFailure) shouldBe true
        full.set(false)
        h.reopen()
        h.readData(ps(0)) shouldBe v0.toList
        h.readData(ps(1)) should (equal(v(1).toList) or equal(patched(v(1), 1, bytes(4, 7)).toList))
        h.readData(ps(2)) should (equal(v(2).toList) or equal(bytes(500, 8).toList))
        h.readOpt(ps(3)) should (equal(Some(v(3).toList)) or equal(None))
        h.quiesce()
        h.fatals.isEmpty shouldBe true

  test("a store that freezes while its close waits for an in-place write still closes"):
    val gate = new GateEc(isInPlace)
    val h = new Harness(mkIo = () => gate)
    val p = ptr(1, oid(1))
    val v1 = bytes(50, 1)
    h.commitOk(alloc(p, v1, 1))
    gate.heldCount shouldBe 1
    val closed = h.backend.close()
    closed.isCompleted shouldBe false // the in-place write is in flight
    h.files.failWhile(Fault.OutOfSpace, isData)(true)
    gate.release() // the write hits ENOSPC: the store freezes while closing, and the close abandons it
    awaitNoRetry(h, closed)
    h.backend.isWriteFrozen shouldBe true
    h.fatals.isEmpty shouldBe true
    h.crash()
    h.readData(p) shouldBe v1.toList

  test("close after a fail-stop completes once the journal batch that was in flight settles"):
    import FsifCheckpointSuite.{HoldEc, oneShot}
    val h = new Harness(mkIo = () => new HoldEc)
    val ec = h.io.asInstanceOf[HoldEc]
    val p1 = ptr(1, oid(1))
    val p2 = ptr(2, oid(2))
    h.commitOk(alloc(p1, bytes(40, 1), 1))
    h.commitOk(alloc(p2, bytes(40, 2), 1))
    h.quiesce()
    ec.hold = oneShot(FsifBackendSuite.isKind(FsifBackend.IoKind.Journal))
    val held = h.issueCommit(writeRange(p1, bytes(40, 1), 3, bytes(4, 9), 2)) // its batch's io is held
    ec.heldCount shouldBe 1
    h.files.failNext(Fault.IoError, _ == FsifRecovery.DataFile)
    val cow = h.issueCommit(overwrite(p2, bytes(600, 3), 2)) // copy-on-write: a data I/O error, fail-stop
    h.await(cow) shouldBe Right(CommitError.IOFailure)
    h.await(held) shouldBe Right(CommitError.IOFailure)
    h.fatals.size shouldBe 1
    val closed = h.backend.close()
    closed.isCompleted shouldBe false // the journal's io still runs on its file
    ec.release()
    Await.result(closed, Timeout)
    h.fatals.size shouldBe 1

  test("EDQUOT (disk quota exceeded) counts as out of space"):
    FsifIo.isOutOfSpace(new IOException("Disk quota exceeded")) shouldBe true
    FsifIo.isOutOfSpace(new IOException("write failed", new IOException("EDQUOT"))) shouldBe true
    FsifIo.isOutOfSpace(new IOException("DISK QUOTA EXCEEDED")) shouldBe true
    FsifIo.isOutOfSpace(new IOException("Input/output error")) shouldBe false

  // ---- Seeded ----

  test("random ENOSPC windows with commits, checkpoints and compaction: every commit succeeds and survives a crash"):
    var froze = 0
    val kinds = mutable.Map[String, Int]().withDefaultValue(0)
    for seed <- 0 until 30 do
      withClue(s"seed=$seed"):
        val r = rng(seed + 9000)
        val h = new Harness(seed, mkIo = () => new ShuffleEc(r),
          tuning = FsifTuning(cowThreshold = 256, journalBound = 4096, enospcRetry = retry))
        val io = h.io.asInstanceOf[ShuffleEc]
        val n = 8
        val ps = (0 until n).map(x => ptr(x, oid(x + 1)))
        val last = Array.fill[Option[Array[Byte]]](n)(None)
        val commits = mutable.ArrayBuffer[Future[CommitResult]]()
        var rev = 1
        for x <- 0 until n do
          val v = bytes(20 + r.nextInt(400), x)
          h.commitOk(alloc(ps(x), v, rev))
          last(x) = Some(v)
        h.quiesce()
        // data is the busiest file (in-place writes, copy-on-write, moves, checkpoint forces, truncation)
        val names = Vector.fill(4)(FsifRecovery.DataFile) ++ Vector("journal.a", "journal.b", "index.a", "index.b")
        val windows = mutable.ArrayBuffer[AtomicBoolean]()
        var sawFrozen = false
        for i <- 0 until 60 do
          r.nextInt(10) match
            case 0 =>
              val w = new AtomicBoolean(true)
              windows += w
              val name = names(r.nextInt(names.size))
              val ops = FaultOp.values.filter(_ => r.nextBoolean()).toSet
              h.files.failWhile(Fault.OutOfSpace, _ == name, if ops.isEmpty then FaultOp.values.toSet else ops)(w.get)
            case 1 if windows.nonEmpty =>
              windows.remove(r.nextInt(windows.size)).set(false)
            case 2 =>
              h.backend.checkpointNow()
            case 3 =>
              h.tick()
            case _ =>
              val x = r.nextInt(n)
              rev += 1
              val (cs, next) = last(x) match
                case None =>
                  val v = bytes(r.nextInt(600), rev)
                  (alloc(ps(x), v, rev), Some(v))
                case Some(old) =>
                  r.nextInt(5) match
                    case 1 if old.length > 0 =>
                      val off = r.nextInt(old.length)
                      val patch = bytes(1 + r.nextInt(math.min(16, old.length - off)), rev)
                      (writeRange(ps(x), old, off, patch, rev), Some(patched(old, off, patch)))
                    case 2 if old.length + 40 <= layout.segmentPayloadSize =>
                      val add = bytes(1 + r.nextInt(40), rev)
                      (append(ps(x), old, add, rev), Some(old ++ add))
                    case 3 =>
                      (delete(ps(x), rev), None) // holes below the top slots start moves
                    case _ =>
                      val v = bytes(r.nextInt(900), rev) // over 256 bytes goes copy-on-write
                      (overwrite(ps(x), v, rev), Some(v))
              commits += h.issueCommit(cs)
              last(x) = next
          for _ <- 0 until r.nextInt(8) do io.runOne()
          if h.backend.isWriteFrozen then sawFrozen = true
        // Reads work whatever is held
        for x <- 0 until n do
          withClue(s"ext=$x while held"):
            h.read(ps(x)) match
              case Left(_) | Right(ReadError.ObjectNotFound) => ()
              case Right(e) => fail(s"read failed: $e")
        if sawFrozen then froze += 1
        h.backend.holdStats.keys.foreach(k => kinds(k) += 1)
        windows.foreach(_.set(false))
        h.quiesce()
        h.backend.isWriteFrozen shouldBe false
        h.fatals.isEmpty shouldBe true
        for (f, i) <- commits.zipWithIndex do
          withClue(s"commit=$i"):
            f.value.map(_.get) shouldBe Some(Left(()))
        for x <- 0 until n do withClue(s"ext=$x"):
          h.readOpt(ps(x)) shouldBe last(x).map(_.toList)
        h.crash()
        for x <- 0 until n do withClue(s"ext=$x after crash"):
          h.readOpt(ps(x)) shouldBe last(x).map(_.toList)
    info(s"seeds that froze: $froze; seeds holding each kind of I/O: ${kinds.toList.sorted}")
    froze should be >= 20
    for k <- List("journal", "in-place", "copy-on-write", "checkpoint") do
      withClue(k)(kinds(k) should be >= 3)
    withClue("move")(kinds("move-write") + kinds("move-force") should be >= 1)

object FsifFaultSuite:
  /** Runs scheduled retries only when the test calls `tick()`, ignoring their delay */
  final class ManualScheduler extends FsifBackend.RetryScheduler:
    private val queue = new ConcurrentLinkedQueue[Runnable]()
    def schedule(delay: FiniteDuration, task: Runnable): Unit = queue.add(task)
    def pending: Int = queue.size
    /** Runs the retries scheduled so far (not those they schedule) */
    def tick(): Unit =
      val n = queue.size
      for _ <- 0 until n do
        val t = queue.poll()
        if t != null then t.run()
