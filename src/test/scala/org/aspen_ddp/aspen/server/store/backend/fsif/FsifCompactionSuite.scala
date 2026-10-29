package org.aspen_ddp.aspen.server.store.backend.fsif

import java.nio.ByteBuffer
import java.util.UUID
import java.util.concurrent.ConcurrentLinkedQueue

import scala.collection.mutable
import scala.concurrent.{Await, ExecutionContext, Future, Promise}
import scala.concurrent.duration.*
import scala.util.Random

import org.aspen_ddp.aspen.FileBasedTests
import org.aspen_ddp.aspen.common.{DataBuffer, HLCTimestamp}
import org.aspen_ddp.aspen.common.objects.{DataObjectPointer, Metadata, ObjectId, ObjectPointer, ObjectRefcount,
  ObjectRevision, ObjectType, ReadError}
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.{ReadState, StoreId}
import org.aspen_ddp.aspen.common.transaction.TransactionId
import org.aspen_ddp.aspen.server.store.backend.{Commit, CommitError, CommitState, Completion, CompletionHandler,
  Read, UpdateForm}

class FsifCompactionSuite extends FileBasedTests:
  import FsifBackendSuite.{InlineEc, ShuffleEc, isKind}
  import FsifCheckpointSuite.{HoldEc, oneShot}
  import FsifBackend.IoKind

  private val layout = FsifLayout(1024, 64)
  private val stride = layout.stride
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

  /** A backend over SimulatedFsifFiles; `mkIo` gives every incarnation a fresh io EC */
  private final class Harness(seed: Int = 0, mkIo: () => ExecutionContext = () => InlineEc,
                              tuning: FsifTuning = FsifTuning(cowThreshold = 256)):
    var files = new SimulatedFsifFiles(rng(seed))
    FsifRecovery.initialize(files, layout)
    val fatals = new ConcurrentLinkedQueue[Throwable]()
    var io: ExecutionContext = mkIo()
    var rec = new Recorder
    var backend: FsifBackend = open()
    private var txn = 0

    private def open(): FsifBackend =
      val b = new FsifBackend(storeId, tdir / "fsif", layout, files, io, tuning, t => fatals.add(t))
      b.setCompletionHandler(rec)
      b

    def crash(): Unit =
      files = files.crash()
      io = mkIo()
      rec = new Recorder
      fatals.clear()
      backend = open()

    def pump(): Unit = io match
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

    def commitOk(cs: CommitState): Unit = await(issueCommit(cs)) shouldBe Left(())

    def issueRead(p: ObjectPointer): Future[ReadResult] =
      val f = rec.expectRead(p.id)
      backend.read(p)
      f

    def read(p: ObjectPointer): ReadResult = await(issueRead(p))

    def readData(p: ObjectPointer): List[Byte] = read(p) match
      case Left(rs) => rs.data.getByteArray.toList
      case Right(err) => fail(s"read of ${p.id} failed: $err")

    def readOpt(p: ObjectPointer): Option[List[Byte]] = read(p) match
      case Left(rs) => Some(rs.data.getByteArray.toList)
      case Right(ReadError.ObjectNotFound) => None
      case Right(err) => fail(s"read of ${p.id} failed: $err")

    def slotOf(x: Int): Int = await(backend.mapping(x))._1

    def quiesce(): Unit = await(backend.quiesce())

    def size(name: String): Long =
      val f = files.open(name)
      try f.size
      finally f.close()

    /** The live exts' slots are exactly 0 until (live count) and data ends inside the last of them. (Without a
      * crash it is exactly that many strides long; after one, its last slot may end early, which is harmless.)
      */
    def assertDense(exts: Range): Unit =
      val slots = exts.map(slotOf).filter(_ >= 0).sorted
      val n = slots.size
      withClue(s"slots=$slots"):
        slots shouldBe (0 until n)
        backend.slotCount shouldBe n
        val dataSize = size(FsifRecovery.DataFile)
        dataSize should be <= n * stride
        if n > 0 then dataSize should be > (n - 1) * stride

  private def ec(h: Harness): HoldEc = h.io.asInstanceOf[HoldEc]

  test("deleting 9 of 10 objects compacts the store to one slot of exactly one stride"):
    val h = new Harness
    val ps = (0 until 10).map(x => ptr(x, oid(x + 1)))
    for i <- 0 until 10 do h.commitOk(alloc(ps(i), bytes(100, i), 1))
    h.quiesce()
    h.backend.slotCount shouldBe 10
    for i <- 0 until 9 do h.commitOk(delete(ps(i), 2))
    h.quiesce()
    h.backend.slotCount shouldBe 1
    h.size("data") shouldBe stride
    h.slotOf(9) shouldBe 0
    h.readData(ps(9)) shouldBe bytes(100, 9).toList
    h.crash()
    h.readData(ps(9)) shouldBe bytes(100, 9).toList
    h.quiesce()
    h.backend.slotCount shouldBe 1
    // Deleting the last object empties the file
    h.commitOk(delete(ps(9), 3))
    h.quiesce()
    h.backend.slotCount shouldBe 0
    h.size("data") shouldBe 0
    h.crash()
    h.read(ps(9)) shouldBe Right(ReadError.ObjectNotFound)

  private enum Park:
    /** Reading slot t for the copy */
    case CopyRead
    /** Writing the copy into f */
    case CopyWrite
    /** Writing a Delta that arrived after the copy into f */
    case DeltaWrite
    /** Forcing data before Map(X, f) */
    case Force
    /** Map(X, f) appended, its batch not yet durable */
    case MapBatch

  private final class Parked(val h: Harness, val ps: IndexedSeq[ObjectPointer],
                             val vs: mutable.ArrayBuffer[Array[Byte]]):
    var rev = 10

  /** Objects 1..4 at exts 0..3 in slots 0..3. Deleting ext 1 frees slot 1, which starts a move of ext 3 from slot 3
    * (t) to slot 1 (f), parked at `park` with exactly one io task held.
    */
  private def parkMove(park: Park, seed: Int = 0): Parked =
    val h = new Harness(seed, mkIo = () => new HoldEc)
    val ps = (0 until 4).map(x => ptr(x, oid(x + 1)))
    val vs = mutable.ArrayBuffer.tabulate(4)(i => bytes(100 + i, i))
    for i <- 0 until 4 do h.commitOk(alloc(ps(i), vs(i), 1))
    // After a checkpoint the objects live only in data, so a slot lost to compaction can't be replayed back
    h.await(h.backend.checkpointNow())
    h.quiesce()
    val io = ec(h)
    io.hold = park match
      case Park.CopyRead => oneShot(isKind(IoKind.MoveRead))
      case Park.CopyWrite => oneShot(isKind(IoKind.MoveWrite))
      case Park.DeltaWrite => isKind(IoKind.MoveWrite)
      case Park.Force | Park.MapBatch => oneShot(isKind(IoKind.MoveForce))
    h.commitOk(delete(ps(1), 2))
    io.heldCount shouldBe 1
    val st = new Parked(h, ps, vs)
    park match
      case Park.DeltaWrite =>
        // Arrives after the copy was assembled, so its bytes are written to f separately
        val patch = bytes(8, 50)
        h.commitOk(writeRange(ps(3), vs(3), 5, patch, 3))
        vs(3) = patched(vs(3), 5, patch)
        io.release() // the copy write completes; the Delta's write into f is held
        io.heldCount shouldBe 1
      case Park.MapBatch =>
        io.hold = oneShot(isKind(IoKind.Journal))
        io.release() // the force completes and Map(X, f) is appended; its batch is held
        io.heldCount shouldBe 1
        h.slotOf(3) shouldBe 1
        h.backend.slotCount shouldBe 4 // slot t is not freed, let alone truncated, before the Map is durable
      case _ => ()
    h.backend.compactionStats.started shouldBe 1
    st

  private def releaseAll(h: Harness): Unit =
    ec(h).hold = _ => false
    ec(h).release()

  test("reads during a move or truncate see correct data"):
    for park <- Park.values do
      withClue(s"park=$park"):
        val m = parkMove(park)
        val h = m.h
        h.readData(m.ps(3)) shouldBe m.vs(3).toList // the moving object
        h.readData(m.ps(2)) shouldBe m.vs(2).toList // the next tail object
        h.readData(m.ps(0)) shouldBe m.vs(0).toList
        h.read(m.ps(1)) shouldBe Right(ReadError.ObjectNotFound)
        releaseAll(h)
        h.quiesce()
        h.slotOf(3) shouldBe 1
        h.readData(m.ps(3)) shouldBe m.vs(3).toList
        h.readData(m.ps(2)) shouldBe m.vs(2).toList
        h.assertDense(0 until 4)

    // A read of the moving object that is in flight on slot t when Map(X, f) becomes durable holds t: the file is not
    // truncated under it, and the read completes with the right data (retried on f)
    val h = new Harness(mkIo = () => new HoldEc)
    val ps = (0 until 4).map(x => ptr(x, oid(x + 1)))
    for i <- 0 until 4 do h.commitOk(alloc(ps(i), bytes(100 + i, i), 1))
    h.quiesce()
    ec(h).hold = oneShot(isKind(IoKind.Read))
    val held = h.issueRead(ps(3))
    ec(h).heldCount shouldBe 1
    h.commitOk(delete(ps(1), 2)) // the whole move runs inline
    h.backend.compactionStats.mapped shouldBe 1
    h.slotOf(3) shouldBe 1
    h.backend.slotCount shouldBe 4 // slot 3 still has a read in flight
    h.size("data") should be > 3 * stride
    releaseAll(h)
    h.await(held).left.map(_.data.getByteArray.toList) shouldBe Left(bytes(103, 3).toList)
    h.quiesce()
    h.assertDense(0 until 4)

  test("small writes to the moving object at each parked step survive quiesce and crash"):
    for park <- Park.values; crashParked <- List(false, true); seed <- 0 until 3 do
      withClue(s"park=$park crashParked=$crashParked seed=$seed"):
        val m = parkMove(park, seed)
        val h = m.h
        val p = m.ps(3)
        val before = m.vs(3)
        val patch = bytes(6, 70)
        val f1 = h.issueCommit(writeRange(p, before, 30, patch, 20))
        val v1 = patched(before, 30, patch)
        val add = bytes(25, 71)
        val f2 = h.issueCommit(append(p, v1, add, 21)) // grows the slice
        val v2 = v1 ++ add
        if park != Park.MapBatch then
          acked(f1) shouldBe true
          acked(f2) shouldBe true
          h.readData(p) shouldBe v2.toList
        if crashParked then
          h.crash()
          val got = h.readData(p)
          if park == Park.MapBatch then
            List(before.toList, v1.toList, v2.toList) should contain(got)
          else
            got shouldBe v2.toList
          h.quiesce()
          h.readData(p) shouldBe got
        else
          releaseAll(h)
          h.await(f1) shouldBe Left(())
          h.await(f2) shouldBe Left(())
          h.quiesce()
          h.readData(p) shouldBe v2.toList
          h.backend.compactionStats.mapped shouldBe 1
          h.assertDense(0 until 4)
          h.crash()
          h.readData(p) shouldBe v2.toList
        h.readData(m.ps(2)) shouldBe m.vs(2).toList
        h.readData(m.ps(0)) shouldBe m.vs(0).toList
        h.quiesce()
        h.assertDense(0 until 4)

  test("a commit whose write into f runs after the force before Map(X, f) forces data again first"):
    for crashBeforeRelease <- List(false, true) do
      withClue(s"crashBeforeRelease=$crashBeforeRelease"):
        val m = parkMove(Park.Force)
        val h = m.h
        val p = m.ps(3)
        ec(h).hold = isKind(IoKind.MoveWrite)
        val patch = bytes(9, 90)
        h.commitOk(writeRange(p, m.vs(3), 40, patch, 20)) // its write into f is held
        val v = patched(m.vs(3), 40, patch)
        ec(h).release(isKind(IoKind.MoveForce)) // that force ran before the write, so it does not cover it
        h.backend.compactionStats.mapped shouldBe 0
        h.slotOf(3) shouldBe 3
        if crashBeforeRelease then
          h.crash()
          h.readData(p) shouldBe v.toList
        else
          ec(h).hold = isKind(IoKind.MoveForce)
          ec(h).release(isKind(IoKind.MoveWrite)) // the write completes: a second force must precede the Map
          ec(h).heldCount shouldBe 1
          h.backend.compactionStats.mapped shouldBe 0
          releaseAll(h)
          h.quiesce()
          h.backend.compactionStats.mapped shouldBe 1
          h.slotOf(3) shouldBe 1
          h.crash()
          h.readData(p) shouldBe v.toList
        h.quiesce()
        h.readData(p) shouldBe v.toList
        h.assertDense(0 until 4)

  test("a copy-on-write overwrite or a delete of the moving object cancels the move and frees the reserved slot"):
    for cow <- List(true, false) do
      withClue(s"cow=$cow"):
        val m = parkMove(Park.CopyWrite)
        val h = m.h
        val p = m.ps(3)
        val big = bytes(700, 80)
        if cow then h.commitOk(overwrite(p, big, 20)) else h.commitOk(delete(p, 20))
        h.backend.compactionStats.cancelled shouldBe 1
        // The reserved slot 1 still has the copy write in flight, so a new allocation must not take it
        val z = ptr(5, oid(9))
        h.commitOk(alloc(z, bytes(40, 81), 21))
        h.slotOf(5) should not be 1
        releaseAll(h)
        h.quiesce()
        if cow then h.readData(p) shouldBe big.toList else h.read(p) shouldBe Right(ReadError.ObjectNotFound)
        h.readData(z) shouldBe bytes(40, 81).toList
        h.readData(m.ps(2)) shouldBe m.vs(2).toList
        h.assertDense(0 until 6)
        h.backend.compactionStats.started should be >= 2
        h.crash()
        if cow then h.readData(p) shouldBe big.toList else h.read(p) shouldBe Right(ReadError.ObjectNotFound)
        h.readData(z) shouldBe bytes(40, 81).toList
        h.readData(m.ps(0)) shouldBe m.vs(0).toList

  test("a crash at any file op during compaction with commits in flight keeps every acknowledged write"):
    var midMove = 0
    var completed = 0
    for seed <- 0 until 50 do
      withClue(s"seed=$seed"):
        val r = rng(seed + 7000)
        val h = new Harness(seed, mkIo = () => new ShuffleEc(r))
        val n = 8
        val ps = (0 until n).map(x => ptr(x, oid(x + 1)))
        // Per ext, every issued version in order (None = deleted), with whether it was acknowledged
        val hist = Array.fill(n)(mutable.ArrayBuffer[(Option[Array[Byte]], () => Boolean)]())
        var rev = 1
        for x <- 0 until n do
          val v = bytes(20 + r.nextInt(400), x)
          h.commitOk(alloc(ps(x), v, rev))
          hist(x) += ((Some(v), () => true))
        h.quiesce()
        val io = h.io.asInstanceOf[ShuffleEc]
        h.files.crashAfterOps(r.nextInt(70))
        def issue(x: Int, cs: CommitState, next: Option[Array[Byte]]): Unit =
          val f = h.issueCommit(cs)
          hist(x) += ((next, () => acked(f)))
        // Holes low in the file start moves of the objects at the top
        for x <- r.shuffle((0 until n / 2).toList).take(1 + r.nextInt(3)) do
          rev += 1
          issue(x, delete(ps(x), rev), None)
          for _ <- 0 until r.nextInt(4) do io.runOne()
        val cpAt = if r.nextBoolean() then r.nextInt(12) else -1
        for i <- 0 until 12 do
          if i == cpAt then h.backend.checkpointNow()
          // Half the commits go to the objects at the top: the moving ones and the next to move
          val x = if r.nextBoolean() then n - 1 - r.nextInt(3) else r.nextInt(n)
          rev += 1
          hist(x).last._1 match
            case None =>
              val v = bytes(r.nextInt(600), rev)
              issue(x, alloc(ps(x), v, rev), Some(v))
            case Some(old) =>
              r.nextInt(5) match
                case 1 | 2 if old.length > 0 =>
                  val off = r.nextInt(old.length)
                  val patch = bytes(1 + r.nextInt(math.min(16, old.length - off)), rev)
                  issue(x, writeRange(ps(x), old, off, patch, rev), Some(patched(old, off, patch)))
                case 3 if old.length + 40 <= layout.segmentPayloadSize =>
                  val add = bytes(1 + r.nextInt(40), rev)
                  issue(x, append(ps(x), old, add, rev), Some(old ++ add))
                case 4 =>
                  issue(x, delete(ps(x), rev), None)
                case _ =>
                  val v = bytes(r.nextInt(900), rev) // over 256 bytes goes copy-on-write
                  issue(x, overwrite(ps(x), v, rev), Some(v))
          for _ <- 0 until r.nextInt(6) do io.runOne()
        io.runAll()
        val stats = h.backend.compactionStats
        if h.fatals.isEmpty then
          if stats.mapped > 0 then completed += 1
          if cpAt >= 0 then h.backend.journalSeq shouldBe 1
          h.assertDense(0 until n)
        else if stats.started > stats.mapped + stats.cancelled then
          midMove += 1
        h.crash()
        val got = (0 until n).map: x =>
          withClue(s"ext=$x"):
            val k = hist(x).lastIndexWhere(_._2())
            val allowed = hist(x).drop(k).map(_._1.map(_.toList)).toList
            val g = h.readOpt(ps(x))
            allowed should contain(g)
            g
        // Compaction resumes after recovery, and the result is durable
        h.quiesce()
        h.assertDense(0 until n)
        h.crash()
        for x <- 0 until n do withClue(s"ext=$x after compaction"):
          h.readOpt(ps(x)) shouldBe got(x)
    info(s"seeds crashed mid-move: $midMove, completed with a move: $completed")
    withClue(s"midMove=$midMove completed=$completed"):
      midMove should be >= 5
      completed should be >= 5

  test("a crash at every file op of one move with an in-flight Delta keeps the acknowledged state"):
    final case class Run(h: Harness, deleted: Future[CommitResult], written: Future[CommitResult], ops: Long)
    val a = ptr(0, oid(1))
    val b = ptr(1, oid(2))
    val va = bytes(100, 1)
    val vb = bytes(120, 2)
    val patch = bytes(8, 3)
    val vb2 = patched(vb, 10, patch)
    def scenario(seed: Int, crashAt: Option[Int]): Run =
      val h = new Harness(seed, mkIo = () => new HoldEc)
      h.commitOk(alloc(a, va, 1))
      h.commitOk(alloc(b, vb, 2))
      h.quiesce()
      val start = h.files.opCount
      crashAt.foreach(h.files.crashAfterOps)
      ec(h).hold = oneShot(isKind(IoKind.MoveWrite))
      val fd = h.issueCommit(delete(a, 3)) // frees slot 0: b moves from slot 1, its copy write held
      val fw = h.issueCommit(writeRange(b, vb, 10, patch, 4)) // a Delta after the copy: written to t and to f
      releaseAll(h)
      h.quiesce()
      Run(h, fd, fw, h.files.opCount - start)

    val clean = scenario(0, None)
    clean.h.fatals.isEmpty shouldBe true
    acked(clean.deleted) shouldBe true
    acked(clean.written) shouldBe true
    clean.h.backend.compactionStats.mapped shouldBe 1
    clean.h.backend.slotCount shouldBe 1
    clean.ops should be >= 10L
    info(s"one move with an in-flight Delta takes ${clean.ops} file ops")
    for k <- 0 to clean.ops.toInt; seed <- 0 until 4 do
      withClue(s"k=$k seed=$seed"):
        val run = scenario(seed, Some(k))
        val h = run.h
        if k < clean.ops then h.fatals.isEmpty shouldBe false else h.fatals.isEmpty shouldBe true
        h.crash()
        val gotA = h.readOpt(a)
        if acked(run.deleted) then gotA shouldBe None else List(None, Some(va.toList)) should contain(gotA)
        val gotB = h.readData(b)
        if acked(run.written) then gotB shouldBe vb2.toList else List(vb.toList, vb2.toList) should contain(gotB)
        h.quiesce()
        h.assertDense(0 until 2)
        h.crash()
        h.readOpt(a) shouldBe gotA
        h.readData(b) shouldBe gotB

  test("compaction resumes after a crash and reopen"):
    val h = new Harness(mkIo = () => new HoldEc)
    val ps = (0 until 6).map(x => ptr(x, oid(x + 1)))
    for i <- 0 until 6 do h.commitOk(alloc(ps(i), bytes(90 + i, i), 1))
    h.quiesce()
    ec(h).hold = isKind(IoKind.MoveRead) // no move gets past its read
    for i <- 0 until 4 do h.commitOk(delete(ps(i), 2))
    h.backend.slotCount shouldBe 6
    val before = h.size("data")
    before should be > 5 * stride
    h.crash() // the new incarnation's io holds nothing
    h.quiesce()
    h.backend.slotCount shouldBe 2
    h.size("data") shouldBe 2 * stride
    for i <- 4 until 6 do h.readData(ps(i)) shouldBe bytes(90 + i, i).toList
    for i <- 0 until 4 do h.read(ps(i)) shouldBe Right(ReadError.ObjectNotFound)
    h.crash()
    h.backend.slotCount shouldBe 2
    for i <- 4 until 6 do h.readData(ps(i)) shouldBe bytes(90 + i, i).toList
