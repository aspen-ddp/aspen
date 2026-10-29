package org.aspen_ddp.aspen.server.store.backend.fsif

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

class FsifBackendSuite extends FileBasedTests:
  import FsifBackendSuite.*
  import JournalWriterSuite.ManualEc

  private val layout = FsifLayout(1024, 64)
  private val poolId = PoolId(new UUID(7, 7))
  private val storeId = StoreId(poolId, 0)
  private val Timeout = 10.seconds

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
  private def writeRange(p: ObjectPointer, old: Array[Byte], off: Int, patch: Array[Byte], rev: Int): CommitState =
    val data = old.clone()
    System.arraycopy(patch, 0, data, off, patch.length)
    CommitState(p, md(rev), ObjectType.Data, DataBuffer(data),
      form = UpdateForm.Ranges(old.length, List((off, patch.length))))
  private def delete(p: ObjectPointer, rev: Int): CommitState =
    CommitState(p, md(rev, 0), ObjectType.Data, DataBuffer())

  type CommitResult = Either[Unit, CommitError.Value]
  type ReadResult = Either[ReadState, ReadError.Value]

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

  /** A backend over SimulatedFsifFiles. `mkIo` gives a fresh io EC for every incarnation, so tasks queued by a
    * crashed incarnation are dropped with it.
    */
  private final class Harness(seed: Int = 0, val lay: FsifLayout = layout,
                              mkIo: () => ExecutionContext = () => InlineEc,
                              tuning: FsifTuning = FsifTuning(cowThreshold = 256),
                              wrap: FsifFiles => FsifFiles = f => f):
    var files = new SimulatedFsifFiles(rng(seed))
    FsifRecovery.initialize(files, lay)
    val fatals = new ConcurrentLinkedQueue[Throwable]()
    var io: ExecutionContext = mkIo()
    var rec = new Recorder
    var backend: FsifBackend = open()
    private var txn = 0

    private def open(): FsifBackend =
      val b = new FsifBackend(storeId, tdir / "fsif", lay, wrap(files), io, tuning, t => fatals.add(t))
      b.setCompletionHandler(rec)
      b

    def crash(): Unit =
      files = files.crash()
      io = mkIo()
      rec = new Recorder
      backend = open()

    def pump(): Unit = io match
      case m: ManualEc => m.runAll()
      case m: ShuffleEc => m.runAll()
      case _ => ()

    def await[A](f: Future[A]): A =
      pump()
      Await.result(f, Timeout)

    def issueCommit(cs: CommitState): Future[CommitResult] =
      txn += 1
      val tx = TransactionId(new UUID(seed, txn))
      val f = rec.expectCommit(cs.objectId, tx)
      backend.commit(cs, tx)
      f

    def commit(cs: CommitState): CommitResult = await(issueCommit(cs))

    def commitOk(cs: CommitState): Unit = commit(cs) shouldBe Left(())

    def issueRead(p: ObjectPointer): Future[ReadResult] =
      val f = rec.expectRead(p.id)
      backend.read(p)
      f

    def read(p: ObjectPointer): ReadResult = await(issueRead(p))

    def readData(p: ObjectPointer): Array[Byte] = read(p) match
      case Left(rs) =>
        rs.objectId shouldBe p.id
        rs.data.getByteArray
      case Right(err) => fail(s"read of ${p.id} failed: $err")

    def repair(cs: CommitState): Try[Unit] =
      val p = Promise[Unit]()
      backend.repair(cs, p)
      pump()
      Try(Await.result(p.future, Timeout))

    def repairDelete(id: ObjectId, storePointer: Array[Byte]): Try[Unit] =
      val p = Promise[Unit]()
      backend.repairDelete(id, storePointer, p)
      pump()
      Try(Await.result(p.future, Timeout))

    def quiesce(): Unit = await(backend.quiesce())

    def slotOccupant(slot: Int): Option[ObjectId] =
      FsifRecovery.readSlot(files, lay, slot).map(_._1.objectId)

  test("allocate, read, overwrite, read, delete, then ObjectNotFound"):
    val h = new Harness
    val p = ptr(3, oid(1))
    h.commitOk(alloc(p, bytes(100, 1), 1))
    h.readData(p).toList shouldBe bytes(100, 1).toList
    h.commitOk(overwrite(p, bytes(80, 2), 2))
    h.read(p) match
      case Left(rs) =>
        rs.data.getByteArray.toList shouldBe bytes(80, 2).toList
        rs.metadata shouldBe md(2)
        rs.objectType shouldBe ObjectType.Data
      case Right(e) => fail(s"read failed: $e")
    h.commitOk(delete(p, 3))
    h.read(p) shouldBe Right(ReadError.ObjectNotFound)
    h.quiesce()
    h.read(p) shouldBe Right(ReadError.ObjectNotFound)
    h.crash()
    h.read(p) shouldBe Right(ReadError.ObjectNotFound)
    h.rec.unexpected.isEmpty shouldBe true

  test("100 appends of 37 bytes read back byte-exactly, before and after a crash"):
    val h = new Harness(seed = 3, lay = FsifLayout(4096, 16))
    val p = ptr(5, oid(5))
    h.commitOk(alloc(p, Array(), 1))
    var expected = Array[Byte]()
    for i <- 0 until 100 do
      val add = bytes(37, i)
      h.commitOk(append(p, expected, add, i + 2))
      expected = expected ++ add
    h.readData(p).toList shouldBe expected.toList
    h.crash()
    h.readData(p).toList shouldBe expected.toList
    h.read(p).swap.toOption.get.metadata shouldBe md(101)

  test("appends into a reused slot never expose the previous occupant"):
    for seed <- 0 until 5 do
      withClue(s"seed=$seed"):
        val h = new Harness(seed)
        val old = ptr(5, oid(50))
        h.commitOk(alloc(old, Array.fill[Byte](1000)(0xAA.toByte), 1))
        h.slotOccupant(0) shouldBe Some(oid(50))
        h.commitOk(delete(old, 2))
        h.quiesce()
        val p = ptr(9, oid(90))
        h.commitOk(alloc(p, Array(), 3))
        h.quiesce()
        h.slotOccupant(0) shouldBe Some(oid(90))
        var expected = Array[Byte]()
        for i <- 0 until 100 do
          val add = Array.tabulate[Byte](10)(j => ((i + j) % 100).toByte)
          h.commitOk(append(p, expected, add, i + 4))
          expected = expected ++ add
          h.readData(p).toList shouldBe expected.toList
          if i == 49 then
            h.crash()
            h.readData(p).toList shouldBe expected.toList
        h.crash()
        h.readData(p).toList shouldBe expected.toList
        h.quiesce()
        h.readData(p).toList shouldBe expected.toList

  test("malformed store pointers fail cleanly"):
    val h = new Harness
    val good = ptr(0, oid(1))
    h.commitOk(alloc(good, bytes(10, 1), 1))
    val bad = List(Array[Byte](), Array[Byte](0, 0, 0), Array[Byte](0, 0, 0, 0, 1), sp(-1), sp(layout.segmentCount),
      sp(Int.MinValue))
    for (b, i) <- bad.zipWithIndex do
      withClue(s"pointer ${b.toList}: "):
        val p = DataObjectPointer(oid(100 + i), poolId, b)
        h.commit(alloc(p, bytes(10, 2), 2)) shouldBe Right(CommitError.InvalidPointer)
        h.commit(overwrite(p, bytes(10, 2), 2)) shouldBe Right(CommitError.InvalidPointer)
        h.read(p) shouldBe Right(ReadError.ObjectNotFound)
        h.repair(overwrite(p, bytes(10, 2), 2)).isFailure shouldBe true
        h.repairDelete(p.id, b).isSuccess shouldBe true
        an[IllegalArgumentException] should be thrownBy
          h.backend.rebuildWrite(p, ObjectType.Data, md(2), DataBuffer(bytes(10, 2)))
    h.readData(good).toList shouldBe bytes(10, 1).toList
    h.fatals.isEmpty shouldBe true

  test("oversized slices are rejected on every entry point"):
    val h = new Harness
    val a = ptr(0, oid(1))
    val b = ptr(1, oid(2))
    val aData = Array.fill[Byte](1024)(0x11)
    val bData = Array.fill[Byte](1024)(0x22)
    h.commitOk(alloc(a, aData, 1))
    h.commitOk(alloc(b, bData, 2))
    h.quiesce()
    h.slotOccupant(0) shouldBe Some(oid(1))
    h.slotOccupant(1) shouldBe Some(oid(2))
    val big = Array.fill[Byte](1025)(0x33)
    h.commit(overwrite(a, big, 3)) shouldBe Right(CommitError.SliceTooLarge)
    h.commit(alloc(ptr(2, oid(3)), big, 3)) shouldBe Right(CommitError.SliceTooLarge)
    h.commit(append(a, aData, Array[Byte](1), 3)) shouldBe Right(CommitError.SliceTooLarge)
    h.repair(overwrite(a, big, 3)).isFailure shouldBe true
    an[IllegalArgumentException] should be thrownBy
      h.backend.rebuildWrite(a, ObjectType.Data, md(3), DataBuffer(big))
    an[IllegalArgumentException] should be thrownBy
      h.backend.rebuildWrite(ptr(2, oid(3)), ObjectType.Data, md(3), DataBuffer(big))
    h.backend.rebuildFlush()
    h.quiesce()
    h.readData(a).toList shouldBe aData.toList
    h.readData(b).toList shouldBe bData.toList
    h.crash()
    h.readData(a).toList shouldBe aData.toList
    h.readData(b).toList shouldBe bData.toList
    h.read(ptr(2, oid(3))) shouldBe Right(ReadError.ObjectNotFound)

  test("a non-allocating commit to an unmapped or differently occupied ext is ObjectMismatch"):
    val h = new Harness
    h.commit(overwrite(ptr(4, oid(4)), bytes(10, 1), 1)) shouldBe Right(CommitError.ObjectMismatch)
    h.commit(append(ptr(4, oid(4)), Array(), bytes(10, 1), 1)) shouldBe Right(CommitError.ObjectMismatch)
    h.commitOk(alloc(ptr(4, oid(4)), bytes(10, 1), 1))
    h.commit(overwrite(ptr(4, oid(5)), bytes(10, 2), 2)) shouldBe Right(CommitError.ObjectMismatch)
    h.commit(delete(ptr(4, oid(5)), 2)) shouldBe Right(CommitError.ObjectMismatch)
    h.readData(ptr(4, oid(4))).toList shouldBe bytes(10, 1).toList
    h.read(ptr(4, oid(5))) shouldBe Right(ReadError.ObjectNotFound)

  test("allocating over a missed delete replaces the occupant, journaled or copy-on-write"):
    for (size, label) <- List((40, "journaled"), (700, "copy-on-write")) do
      withClue(label):
        val h = new Harness
        val o = ptr(6, oid(60))
        val p = ptr(6, oid(61))
        h.commitOk(alloc(o, bytes(500, 1), 1))
        h.commitOk(alloc(p, bytes(size, 2), 2))
        h.readData(p).toList shouldBe bytes(size, 2).toList
        h.read(o) shouldBe Right(ReadError.ObjectNotFound)
        h.crash()
        h.readData(p).toList shouldBe bytes(size, 2).toList
        h.read(o) shouldBe Right(ReadError.ObjectNotFound)

  test("repairDelete of a stale ObjectId leaves the new occupant readable"):
    val h = new Harness
    val o = ptr(2, oid(20))
    val p = ptr(2, oid(21))
    h.commitOk(alloc(o, bytes(30, 1), 1))
    // This store missed O's delete; X was reallocated to P
    h.commitOk(alloc(p, bytes(30, 2), 2))
    h.repairDelete(o.id, sp(2)).isSuccess shouldBe true
    h.repairDelete(o.id, Array()).isSuccess shouldBe true
    h.readData(p).toList shouldBe bytes(30, 2).toList
    h.crash()
    h.readData(p).toList shouldBe bytes(30, 2).toList
    h.repairDelete(p.id, sp(2)).isSuccess shouldBe true
    h.read(p) shouldBe Right(ReadError.ObjectNotFound)
    h.crash()
    h.read(p) shouldBe Right(ReadError.ObjectNotFound)

  test("metadata-only commits and repair update the object durably"):
    val h = new Harness
    val p = ptr(1, oid(1))
    h.commitOk(alloc(p, bytes(50, 1), 1))
    h.commitOk(CommitState(p, md(2, 3), ObjectType.Data, DataBuffer(bytes(50, 1)), form = UpdateForm.MetadataOnly))
    h.read(p).swap.toOption.get.metadata shouldBe md(2, 3)
    h.repair(overwrite(p, bytes(60, 4), 4)).isSuccess shouldBe true
    h.crash()
    val rs = h.read(p).swap.toOption.get
    rs.metadata shouldBe md(4)
    rs.data.getByteArray.toList shouldBe bytes(60, 4).toList

  test("repair of a never-allocated object allocates it (Ruling 18)"):
    for (size, label) <- List((40, "journaled"), (700, "copy-on-write")) do
      withClue(label):
        val h = new Harness
        val p = ptr(9, oid(9))
        h.read(p) shouldBe Right(ReadError.ObjectNotFound)
        h.repair(overwrite(p, bytes(size, 1), 5)).isSuccess shouldBe true
        h.readData(p).toList shouldBe bytes(size, 1).toList
        h.crash()
        val rs = h.read(p).swap.toOption.get
        rs.metadata shouldBe md(5)
        rs.data.getByteArray.toList shouldBe bytes(size, 1).toList
        // A plain commit to an unmapped X is still a mismatch
        h.commit(overwrite(ptr(10, oid(10)), bytes(10, 2), 2)) shouldBe Right(CommitError.ObjectMismatch)

  test("repair after a CorruptedObject read restores the object"):
    val h = new Harness
    val p = ptr(3, oid(3))
    h.commitOk(alloc(p, bytes(100, 1), 1))
    h.quiesce()
    val f = h.files.open("data")
    f.write(layout.slotOffset(0) + SegmentHeader.Size + 10, Array.fill[Byte](8)(0x5A))
    h.read(p) shouldBe Right(ReadError.CorruptedObject)
    h.repair(overwrite(p, bytes(100, 1), 2)).isSuccess shouldBe true
    h.readData(p).toList shouldBe bytes(100, 1).toList
    h.quiesce()
    h.readData(p).toList shouldBe bytes(100, 1).toList
    h.crash()
    h.readData(p).toList shouldBe bytes(100, 1).toList

  test("repair at an ext held by a different object fails with the mismatch"):
    val h = new Harness
    h.commitOk(alloc(ptr(4, oid(40)), bytes(30, 1), 1))
    h.repair(overwrite(ptr(4, oid(41)), bytes(30, 2), 2)).isFailure shouldBe true
    h.repair(overwrite(ptr(4, oid(41)), bytes(700, 2), 2)).isFailure shouldBe true
    h.readData(ptr(4, oid(40))).toList shouldBe bytes(30, 1).toList
    h.read(ptr(4, oid(41))) shouldBe Right(ReadError.ObjectNotFound)

  test("deleting an unmapped ext appends nothing and never completes before an earlier delete"):
    val h = new Harness(mkIo = () => new ManualEc)
    val p = ptr(2, oid(2))
    h.commitOk(alloc(p, bytes(30, 1), 1))
    h.quiesce()
    val f1 = h.issueCommit(delete(p, 2))
    val f2 = h.issueCommit(delete(p, 3))
    val io = h.io.asInstanceOf[ManualEc]
    while io.pending > 0 do
      (f2.isCompleted && !f1.isCompleted) shouldBe false
      io.runOne()
    h.await(f1) shouldBe Left(())
    h.await(f2) shouldBe Left(())
    h.quiesce()
    def journalSize = h.files.open("journal.a").size
    val before = journalSize
    h.commitOk(delete(p, 4))
    h.commitOk(delete(ptr(30, oid(30)), 1))
    h.quiesce()
    journalSize shouldBe before

  test("a read whose header read tears while the slot changes is retried, not reported corrupt"):
    val gate = new GateEc(isKind(FsifBackend.IoKind.Read))
    val tearing = new AtomicBoolean(false)
    val h = new Harness(mkIo = () => gate, wrap = f => new TearingFiles(f, tearing))
    val p = ptr(1, oid(1))
    val v1 = bytes(100, 1)
    h.commitOk(alloc(p, v1, 1))
    h.quiesce() // nothing pending, so the read takes its length from the disk header
    val held = h.issueRead(p)
    h.commitOk(writeRange(p, v1, 5, bytes(4, 9), 2)) // its in-place write runs while the read is held
    tearing.set(true)
    gate.release()
    val v2 = v1.clone()
    System.arraycopy(bytes(4, 9), 0, v2, 5, 4)
    h.await(held) match
      case Left(rs) => rs.data.getByteArray.toList shouldBe v2.toList
      case Right(err) => fail(s"read failed: $err")
    tearing.get shouldBe false

  test("a rejected io task fails the commit and still lets quiesce complete"):
    for kind <- List(FsifBackend.IoKind.Cow, FsifBackend.IoKind.InPlace) do
      withClue(kind.toString):
        val h = new Harness(mkIo = () => new RejectingEc(isKind(kind)))
        val p = ptr(1, oid(1))
        val size = if kind == FsifBackend.IoKind.Cow then 700 else 40
        val r = h.commit(alloc(p, bytes(size, 1), 1))
        if kind == FsifBackend.IoKind.Cow then r shouldBe Right(CommitError.IOFailure)
        h.quiesce()
        h.fatals.size shouldBe 1
        h.commit(overwrite(p, bytes(size, 2), 2)) shouldBe Right(CommitError.IOFailure)
        h.quiesce()

  test("a commit larger than cowThreshold goes through copy-on-write and frees its old slot"):
    val h = new Harness
    val p = ptr(7, oid(7))
    h.commitOk(alloc(p, bytes(100, 1), 1))
    h.quiesce()
    h.slotOccupant(0) shouldBe Some(oid(7))
    h.commitOk(overwrite(p, bytes(700, 2), 2))
    h.quiesce()
    // The copy went to slot 1 and freed slot 0, so compaction then moved it down into slot 0
    h.backend.compactionStats.mapped shouldBe 1
    h.backend.slotCount shouldBe 1
    h.slotOccupant(0) shouldBe Some(oid(7))
    h.readData(p).toList shouldBe bytes(700, 2).toList
    val q = ptr(8, oid(8))
    h.commitOk(alloc(q, bytes(20, 3), 3))
    h.quiesce()
    h.slotOccupant(1) shouldBe Some(oid(8))
    h.crash()
    h.readData(p).toList shouldBe bytes(700, 2).toList
    h.readData(q).toList shouldBe bytes(20, 3).toList

  test("a copy-on-write overwrite cut short by a crash is old or new, never torn"):
    for seed <- 0 until 20 do
      withClue(s"seed=$seed"):
        val steps = rng(seed + 1000)
        val h = new Harness(seed, mkIo = () => new ShuffleEc(steps))
        val p = ptr(7, oid(7))
        h.commitOk(alloc(p, bytes(100, 1), 1))
        h.quiesce()
        val f = h.issueCommit(overwrite(p, bytes(900, 2), 2))
        val io = h.io.asInstanceOf[ShuffleEc]
        for _ <- 0 until steps.nextInt(10) do io.runOne()
        val acked = f.value.exists(_.toOption.contains(Left(())))
        h.crash()
        val got = h.readData(p).toList
        if acked then
          got shouldBe bytes(900, 2).toList
        else
          List(bytes(100, 1).toList, bytes(900, 2).toList) should contain(got)

  test("a small write right behind a copy-on-write survives a crash once acknowledged (Ruling 14)"):
    for seed <- 0 until 30 do
      withClue(s"seed=$seed"):
        val steps = rng(seed + 2000)
        val h = new Harness(seed, mkIo = () => new ShuffleEc(steps))
        val p = ptr(7, oid(7))
        val v1 = bytes(100, 1)
        val v2 = bytes(900, 2)
        val patch = bytes(5, 9)
        h.commitOk(alloc(p, v1, 1))
        h.quiesce()
        val f2 = h.issueCommit(overwrite(p, v2, 2))
        val f3 = h.issueCommit(writeRange(p, v2, 10, patch, 3))
        val v3 = v2.clone()
        System.arraycopy(patch, 0, v3, 10, patch.length)
        val io = h.io.asInstanceOf[ShuffleEc]
        val n = if seed < 5 then 1000 else steps.nextInt(16)
        var i = 0
        while i < n && io.pending > 0 do
          io.runOne()
          i += 1
        def ok(f: Future[CommitResult]) = f.value.exists(_.toOption.contains(Left(())))
        val (a2, a3) = (ok(f2), ok(f3))
        if seed < 5 then a3 shouldBe true
        h.crash()
        val got = h.readData(p).toList
        if a3 then
          got shouldBe v3.toList
        else if a2 then
          List(v2.toList, v3.toList) should contain(got)
        else
          List(v1.toList, v2.toList, v3.toList) should contain(got)

  test("a slot freed by a delete is not reused before the delete is durable"):
    for seed <- 0 until 40 do
      withClue(s"seed=$seed"):
        val order = rng(seed + 3000)
        val h = new Harness(seed, mkIo = () => new ShuffleEc(order))
        val x = ptr(1, oid(1))
        val y = ptr(2, oid(2))
        h.commitOk(alloc(x, bytes(300, 1), 1))
        h.quiesce()
        val fd = h.issueCommit(delete(x, 2))
        val fy = h.issueCommit(alloc(y, bytes(700, 3), 3)) // copy-on-write into the lowest free slot
        val io = h.io.asInstanceOf[ShuffleEc]
        for _ <- 0 until order.nextInt(10) do io.runOne()
        def ok(f: Future[CommitResult]) = f.value.exists(_.toOption.contains(Left(())))
        val (deleted, allocated) = (ok(fd), ok(fy))
        h.crash()
        val (slot, id) = h.await(h.backend.mapping(1))
        if deleted then slot shouldBe -1
        if slot >= 0 then
          id shouldBe oid(1)
          h.readData(x).toList shouldBe bytes(300, 1).toList
        else
          h.read(x) shouldBe Right(ReadError.ObjectNotFound)
        if allocated then
          h.readData(y).toList shouldBe bytes(700, 3).toList

  test("allocations above in-flight copy-on-write tail slots never make recovery lose the store"):
    for seed <- 0 until 40 do
      withClue(s"seed=$seed"):
        val order = rng(seed + 4000)
        val h = new Harness(seed, mkIo = () => new ShuffleEc(order))
        val objs = List((1, 700), (2, 700), (3, 30), (4, 30), (5, 700), (6, 30)).map: (x, n) =>
          (ptr(x, oid(x)), bytes(n, x))
        val fs = objs.map((p, d) => h.issueCommit(alloc(p, d, 1)))
        val io = h.io.asInstanceOf[ShuffleEc]
        for _ <- 0 until order.nextInt(30) do io.runOne()
        val acked = fs.map(_.value.exists(_.toOption.contains(Left(()))))
        h.crash()
        for ((p, d), a) <- objs.zip(acked) do
          h.read(p) match
            case Left(rs) => rs.data.getByteArray.toList shouldBe d.toList
            case Right(err) =>
              a shouldBe false
              err shouldBe ReadError.ObjectNotFound

  test("two commits to one object issued back to back leave the second's data"):
    for (s1, s2, label) <- List((40, 60, "journaled"), (600, 800, "copy-on-write"), (40, 800, "mixed"),
      (800, 40, "cow then small")) do
      withClue(label):
        val h = new Harness(mkIo = () => new ManualEc)
        val p = ptr(3, oid(3))
        h.commitOk(alloc(p, bytes(100, 1), 1))
        h.quiesce()
        val f1 = h.issueCommit(overwrite(p, bytes(s1, 2), 2))
        val f2 = h.issueCommit(overwrite(p, bytes(s2, 3), 3))
        f1.isCompleted shouldBe false
        h.await(f1) shouldBe Left(())
        h.await(f2) shouldBe Left(())
        h.quiesce()
        h.readData(p).toList shouldBe bytes(s2, 3).toList
        h.crash()
        h.readData(p).toList shouldBe bytes(s2, 3).toList
        h.read(p).swap.toOption.get.metadata shouldBe md(3)

  test("a read issued while an in-place write is pending returns the new data"):
    val gate = new GateEc(isInPlace)
    val h = new Harness(mkIo = () => gate)
    val p = ptr(4, oid(4))
    val v1 = bytes(100, 1)
    h.commitOk(alloc(p, v1, 1))
    gate.heldCount should be > 0
    h.slotOccupant(0) shouldBe None // nothing written in place yet
    h.readData(p).toList shouldBe v1.toList
    h.commitOk(writeRange(p, v1, 20, bytes(10, 5), 2))
    val v2 = v1.clone()
    System.arraycopy(bytes(10, 5), 0, v2, 20, 10)
    h.readData(p).toList shouldBe v2.toList
    h.commitOk(append(p, v2, bytes(30, 6), 3))
    h.readData(p).toList shouldBe (v2 ++ bytes(30, 6)).toList
    gate.release()
    h.quiesce()
    h.slotOccupant(0) shouldBe Some(oid(4))
    h.readData(p).toList shouldBe (v2 ++ bytes(30, 6)).toList

  test("a slot with a read in flight is not reused, and the read completes cleanly"):
    val gate = new GateEc(isKind(FsifBackend.IoKind.Read))
    val h = new Harness(mkIo = () => gate)
    val x = ptr(1, oid(1))
    h.commitOk(alloc(x, bytes(100, 1), 1))
    h.quiesce()
    val held = h.issueRead(x)
    gate.heldCount shouldBe 1
    h.commitOk(delete(x, 2))
    h.commitOk(alloc(ptr(2, oid(2)), bytes(50, 3), 3))
    h.await(h.backend.mapping(2))._1 shouldBe 1 // slot 0 still has a read in flight
    gate.release()
    h.await(held) match
      case Left(rs) => rs.data.getByteArray.toList shouldBe bytes(100, 1).toList
      case Right(err) => err shouldBe ReadError.ObjectNotFound
    h.quiesce()
    // Slot 0 was freed once the read ended, and compaction moved ext 2 down into it
    h.await(h.backend.mapping(2))._1 shouldBe 0
    h.commitOk(alloc(ptr(3, oid(3)), bytes(20, 4), 4))
    h.await(h.backend.mapping(3))._1 shouldBe 1
    h.readData(ptr(2, oid(2))).toList shouldBe bytes(50, 3).toList
    h.readData(ptr(3, oid(3))).toList shouldBe bytes(20, 4).toList

  test("reads that overlap in-place writes on a real thread pool always see the latest acknowledged data"):
    val pool = java.util.concurrent.Executors.newFixedThreadPool(4)
    try
      val ec = ExecutionContext.fromExecutor(pool)
      val h = new Harness(mkIo = () => ec)
      val p = ptr(1, oid(1))
      var cur = bytes(200, 1)
      h.commitOk(alloc(p, cur, 1))
      for i <- 0 until 200 do
        val patch = bytes(16, i)
        val off = (i * 13) % 180
        val fc = h.issueCommit(writeRange(p, cur, off, patch, i + 2))
        System.arraycopy(patch, 0, cur, off, patch.length)
        h.await(fc) shouldBe Left(())
        val reads = (0 until 3).map(_ => h.issueRead(p))
        reads.foreach: r =>
          h.await(r) match
            case Left(rs) => rs.data.getByteArray.toList shouldBe cur.toList
            case Right(e) => fail(s"read $i failed: $e")
      h.quiesce()
      h.readData(p).toList shouldBe cur.toList
    finally pool.shutdownNow()

  test("rebuildWrite and rebuildFlush make objects durable"):
    val h = new Harness
    val a = ptr(10, oid(10))
    val b = ptr(11, oid(11))
    h.backend.rebuildWrite(a, ObjectType.Data, md(1), DataBuffer(bytes(300, 1)))
    h.backend.rebuildWrite(b, ObjectType.KeyValue, md(2), DataBuffer(bytes(30, 2)))
    // A resumed walk revisits a key: it overwrites the same slot
    h.backend.rebuildWrite(a, ObjectType.Data, md(3), DataBuffer(bytes(250, 3)))
    h.backend.rebuildFlush()
    h.slotOccupant(0) shouldBe Some(oid(10))
    h.slotOccupant(1) shouldBe Some(oid(11))
    h.slotOccupant(2) shouldBe None
    h.crash()
    h.readData(a).toList shouldBe bytes(250, 3).toList
    val rb = h.read(b).swap.toOption.get
    rb.objectType shouldBe ObjectType.KeyValue
    rb.data.getByteArray.toList shouldBe bytes(30, 2).toList

  test("bootstrap entry points are unsupported and estimateSize counts the files"):
    val h = new Harness
    an[UnsupportedOperationException] should be thrownBy
      h.backend.bootstrapAllocate(oid(1), ObjectType.Data, md(1), DataBuffer(bytes(5, 1)))
    an[UnsupportedOperationException] should be thrownBy h.backend.bootstrapOverwrite(oid(1), DataBuffer())
    val before = h.backend.estimateSize()
    h.commitOk(alloc(ptr(1, oid(1)), bytes(500, 1), 1))
    h.quiesce()
    h.backend.estimateSize() should be > before
    FsifBackend.decodeStorePointer(sp(3), 64) shouldBe Some(3)
    FsifBackend.decodeStorePointer(sp(64), 64) shouldBe None
    FsifBackend.decodeStorePointer(Array(0, 0, 3), 64) shouldBe None

  test("a journal I/O error fails the commit, fail-stops the store and calls onFatal once"):
    val h = new Harness
    val p = ptr(1, oid(1))
    h.commitOk(alloc(p, bytes(20, 1), 1))
    h.files.failNext(Fault.IoError, _ == "journal.a", Set(FaultOp.Force))
    h.commit(overwrite(p, bytes(20, 2), 2)) shouldBe Right(CommitError.IOFailure)
    h.fatals.size shouldBe 1
    h.read(p) shouldBe Right(ReadError.CorruptedObject)
    h.commit(overwrite(p, bytes(20, 3), 3)) shouldBe Right(CommitError.IOFailure)
    h.repair(overwrite(p, bytes(20, 3), 3)).isFailure shouldBe true
    h.quiesce()
    h.fatals.size shouldBe 1
    h.crash()
    h.readData(p).toList shouldBe bytes(20, 1).toList

object FsifBackendSuite:
  object InlineEc extends ExecutionContext:
    def execute(r: Runnable): Unit = r.run()
    def reportFailure(t: Throwable): Unit = throw t

  /** Queues every task; runOne runs a random queued one, modelling a pool that runs queued tasks in any order */
  final class ShuffleEc(rnd: Random) extends ExecutionContext:
    private val queue = mutable.ArrayBuffer[Runnable]()
    def execute(r: Runnable): Unit = synchronized(queue += r)
    def reportFailure(t: Throwable): Unit = throw t
    def pending: Int = synchronized(queue.size)
    def runOne(): Unit =
      val next = synchronized:
        if queue.isEmpty then None else Some(queue.remove(rnd.nextInt(queue.size)))
      next.foreach(_.run())
    def runAll(): Unit =
      while pending > 0 do runOne()

  /** Runs tasks inline but rejects those matching `reject`, as a shut-down pool would */
  final class RejectingEc(reject: Runnable => Boolean) extends ExecutionContext:
    def execute(r: Runnable): Unit =
      if reject(r) then throw new java.util.concurrent.RejectedExecutionException("rejected") else r.run()
    def reportFailure(t: Throwable): Unit = throw t

  /** While `tear` is set, the next header-sized read of `data` returns garbage (then clears `tear`), as a pread
    * racing a pwrite of the header can
    */
  final class TearingFiles(underlying: FsifFiles, tear: AtomicBoolean) extends FsifFiles:
    def exists(name: String): Boolean = underlying.exists(name)
    def open(name: String): FsifFile =
      val f = underlying.open(name)
      if name != FsifRecovery.DataFile then
        f
      else
        new FsifFile:
          def size: Long = f.size
          def read(position: Long, length: Int): Array[Byte] =
            if length == SegmentHeader.Size && tear.compareAndSet(true, false) then
              Array.fill[Byte](length)(0x7F)
            else
              f.read(position, length)
          def write(position: Long, bytes: Array[Byte]): Unit = f.write(position, bytes)
          def truncate(size: Long): Unit = f.truncate(size)
          def force(): Unit = f.force()
          def close(): Unit = f.close()

  def isKind(kind: FsifBackend.IoKind)(r: Runnable): Boolean = r match
    case t: FsifBackend.IoTask => t.kind == kind
    case _ => false

  def isInPlace(r: Runnable): Boolean = isKind(FsifBackend.IoKind.InPlace)(r)

  /** Runs every task inline, except that tasks matching `hold` are queued until `release()` */
  final class GateEc(hold: Runnable => Boolean) extends ExecutionContext:
    private val held = new ConcurrentLinkedQueue[Runnable]()
    @volatile private var open = false
    def execute(r: Runnable): Unit =
      if !open && hold(r) then held.add(r) else r.run()
    def reportFailure(t: Throwable): Unit = throw t
    def heldCount: Int = held.size
    def release(): Unit =
      open = true
      while !held.isEmpty do held.poll().run()
