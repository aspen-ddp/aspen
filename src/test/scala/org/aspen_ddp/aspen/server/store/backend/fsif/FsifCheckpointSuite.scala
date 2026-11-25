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

class FsifCheckpointSuite extends FileBasedTests:
  import FsifBackendSuite.{InlineEc, ShuffleEc, isKind, journalOn}
  import FsifCheckpointSuite.*

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

  /** Store `idx` on a SharedJournal (a fresh one unless `sharedWith`); `mkIo` gives every incarnation a fresh io EC,
    * on which the device journal's writes run too when this harness builds it (IoKind.Journal tasks)
    */
  private final class Harness(seed: Int = 0, mkIo: () => ExecutionContext = () => InlineEc,
                              tuning: FsifTuning = FsifTuning(cowThreshold = 256),
                              sharedWith: Option[SharedJournal] = None, idx: Int = 0,
                              sizing: JournalSizing = SharedJournal.TestSizing):
    val shared: SharedJournal = sharedWith.getOrElse(new SharedJournal(rng(seed), sizing))
    shared.initialize(idx, layout)
    def files: SharedJournal.StoreFiles = shared.storeFiles(idx)
    val fatals = new ConcurrentLinkedQueue[Throwable]()
    var io: ExecutionContext = mkIo()
    var rec = new Recorder
    var backend: FsifBackend = open()
    private var txn = 0

    private def open(): FsifBackend =
      shared.run = journalOn(io)
      val b = shared.open(idx, layout, io, tuning, t => fatals.add(t), storeId = StoreId(poolId, idx.toByte))
      b.setCompletionHandler(rec)
      b

    /** The whole machine crashes (every store on `shared`); this harness reopens its store */
    def crash(): Unit =
      shared.crash()
      reopen()

    /** After another harness on `shared` crashed the machine */
    def reopen(): Unit =
      io = mkIo()
      rec = new Recorder
      backend = open()

    /** Harnesses on the same SharedJournal whose io may hold this one's journal writes, pumped with it */
    var peers: Seq[Harness] = Nil

    private def shuffle: Seq[ShuffleEc] = (this +: peers).map(_.io).collect { case m: ShuffleEc => m }

    def pump(): Unit =
      while shuffle.exists(_.pending > 0) do shuffle.foreach(_.runAll())

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

    def read(p: ObjectPointer): ReadResult =
      val f = rec.expectRead(p.id)
      backend.read(p)
      await(f)

    def readData(p: ObjectPointer): List[Byte] = read(p) match
      case Left(rs) => rs.data.getByteArray.toList
      case Right(err) => fail(s"read of ${p.id} failed: $err")

    def quiesce(): Unit = await(backend.quiesce())

    def size(name: String): Long =
      val f = files.open(name)
      try f.size
      finally f.close()

    def contents(name: String): Array[Byte] =
      val f = files.open(name)
      try f.read(0, f.size.toInt)
      finally f.close()

    def indexSeq(name: String): Option[Long] = IndexFile.decode(contents(name), layout.segmentCount).map(_._1)

    def meta(name: String): Option[IndexMeta] = IndexFile.decode(contents(name), layout.segmentCount).map(_._2)

  test("checkpointNow advances indexSeq, empties the old index, and keeps every object"):
    val h = new Harness
    val ps = (0 until 5).map(x => ptr(x, oid(x + 1)))
    val vs = mutable.ArrayBuffer.tabulate(5)(i => bytes(50 + i * 100, i)) // 450 bytes goes copy-on-write
    for i <- 0 until 5 do h.commitOk(alloc(ps(i), vs(i), 1))
    h.commitOk(writeRange(ps(0), vs(0), 3, bytes(4, 9), 2))
    vs(0) = patched(vs(0), 3, bytes(4, 9))
    // initialize wrote index.a (seq 0), the attach index.b (seq 1)
    h.backend.indexSeq shouldBe 1
    h.size("index.a") shouldBe 0
    h.await(h.backend.checkpointNow())
    h.backend.indexSeq shouldBe 2
    h.size("index.b") shouldBe 0
    h.indexSeq("index.a") shouldBe Some(2)
    val m = h.meta("index.a").get
    (m.journalId, m.attachment, m.detached) shouldBe ((h.shared.sizing.journalId, h.backend.attachmentId, false))
    m.covered shouldBe h.shared.journal.tailSerial
    for i <- 0 until 5 do h.readData(ps(i)) shouldBe vs(i).toList
    // Later commits are above the covered serial
    h.commitOk(writeRange(ps(1), vs(1), 7, bytes(3, 4), 3))
    vs(1) = patched(vs(1), 7, bytes(3, 4))
    h.shared.journal.tailSerial should be > m.covered
    h.quiesce()
    h.crash()
    h.backend.indexSeq shouldBe 4 // device-load replay wrote seq 3, the attach seq 4
    for i <- 0 until 5 do h.readData(ps(i)) shouldBe vs(i).toList

  test("two back-to-back checkpoints (a to b to a) read back correctly after a crash"):
    val h = new Harness
    val ps = (0 until 4).map(x => ptr(x, oid(x + 1)))
    val vs = mutable.ArrayBuffer.tabulate(4)(i => bytes(40 + i * 120, i))
    for i <- 0 until 4 do h.commitOk(alloc(ps(i), vs(i), 1))
    h.await(h.backend.checkpointNow())
    h.commitOk(writeRange(ps(2), vs(2), 11, bytes(9, 3), 2))
    vs(2) = patched(vs(2), 11, bytes(9, 3))
    h.commitOk(delete(ps(3), 3))
    h.await(h.backend.checkpointNow())
    h.backend.indexSeq shouldBe 3
    h.size("index.a") shouldBe 0
    h.indexSeq("index.b") shouldBe Some(3)
    h.commitOk(append(ps(0), vs(0), bytes(30, 5), 4))
    vs(0) = vs(0) ++ bytes(30, 5)
    h.crash()
    h.backend.indexSeq shouldBe 5
    for i <- 0 until 3 do h.readData(ps(i)) shouldBe vs(i).toList
    h.read(ps(3)) shouldBe Right(ReadError.ObjectNotFound)

  test("with a small device journal, 200 small writes trigger checkpoints on their own (journal pressure)"):
    val h = new Harness(sizing = JournalSizing(new UUID(3, 3), 3, 16 * 1024))
    val p = ptr(9, oid(9))
    var v = bytes(200, 1)
    h.commitOk(alloc(p, v, 1))
    for i <- 0 until 200 do
      val patch = bytes(8, i)
      h.commitOk(writeRange(p, v, (i * 13) % 190, patch, i + 2))
      v = patched(v, (i * 13) % 190, patch)
    h.quiesce()
    h.backend.indexSeq should be >= 10L // 3 files of 3 batches each: the ring wrapped many times
    h.readData(p) shouldBe v.toList
    h.crash()
    h.readData(p) shouldBe v.toList

  test("rebuildFlush runs a checkpoint"):
    val h = new Harness
    h.await(h.backend.rebuildWrite(ptr(3, oid(3)), ObjectType.Data, md(1), DataBuffer(bytes(70, 3))))
    h.await(h.backend.rebuildFlush())
    h.backend.indexSeq shouldBe 2
    h.indexSeq("index.a") shouldBe Some(2)
    h.crash()
    h.readData(ptr(3, oid(3))) shouldBe bytes(70, 3).toList

  test("quiesce waits for a running checkpoint"):
    val h = new Harness(mkIo = () => new HoldEc)
    val ec = h.io.asInstanceOf[HoldEc]
    h.commitOk(alloc(ptr(1, oid(1)), bytes(30, 1), 1))
    ec.hold = isKind(FsifBackend.IoKind.Checkpoint)
    val cp = h.backend.checkpointNow()
    val q = h.backend.quiesce()
    ec.heldCount shouldBe 1
    q.isCompleted shouldBe false
    val again = h.backend.checkpointNow() // the running one's future, not a second checkpoint
    again.isCompleted shouldBe false
    ec.hold = _ => false
    ec.release()
    h.await(cp)
    h.await(again)
    h.await(q)
    h.backend.indexSeq shouldBe 2

  test("Ruling R8: a commit made while a checkpoint drains shares no batch with the snapshot's last append"):
    // Store y's batch is in flight; store x's delete (A, the snapshot's last append) queues behind it; x's checkpoint
    // snapshots; x's commit b follows. Were b in A's batch S, index s+1 would cover it unapplied, and a crash before
    // b's in-place write would lose it.
    val shared = new SharedJournal(rng(3))
    val ec = new HoldEc
    val x = new Harness(mkIo = () => ec, sharedWith = Some(shared), idx = 0)
    val y = new Harness(mkIo = () => ec, sharedWith = Some(shared), idx = 1)
    val p = ptr(1, oid(1))
    val q = ptr(2, oid(2))
    val v1 = bytes(100, 1)
    x.commitOk(alloc(p, v1, 1))
    x.commitOk(alloc(q, bytes(30, 2), 1))
    y.commitOk(alloc(ptr(5, oid(5)), bytes(30, 5), 1))
    x.quiesce()
    ec.hold = r => isKind(FsifBackend.IoKind.InPlace)(r) || isKind(FsifBackend.IoKind.Journal)(r)
    val fy = y.issueCommit(writeRange(ptr(5, oid(5)), bytes(30, 5), 0, bytes(4, 6), 2)) // in flight, held
    ec.heldCount shouldBe 1
    val fa = x.issueCommit(delete(q, 2)) // A: queued behind y's batch
    val cp = x.backend.checkpointNow()
    val fb = x.issueCommit(writeRange(p, v1, 10, bytes(8, 3), 3)) // b
    val v2 = patched(v1, 10, bytes(8, 3))
    // Journal writes run; in-place writes stay held, so b is never applied before the crash
    ec.hold = isKind(FsifBackend.IoKind.InPlace)
    ec.release(isKind(FsifBackend.IoKind.Journal))
    y.await(fy) shouldBe Left(())
    x.await(fa) shouldBe Left(())
    x.await(fb) shouldBe Left(())
    x.await(cp)
    x.backend.indexSeq shouldBe 2
    shared.crash()
    x.reopen()
    y.reopen()
    x.readData(p) shouldBe v2.toList
    x.read(q) shouldBe Right(ReadError.ObjectNotFound)

  test("a journal-s Delta cancelled by a later delete holds the checkpoint until that delete is durable"):
    for crashBeforeRelease <- List(false, true) do
      withClue(s"crashBeforeRelease=$crashBeforeRelease"):
        val h = new Harness(mkIo = () => new HoldEc)
        val ec = h.io.asInstanceOf[HoldEc]
        val p = ptr(1, oid(1))
        val v1 = bytes(100, 1)
        h.commitOk(alloc(p, v1, 1))
        h.quiesce()
        ec.hold = isKind(FsifBackend.IoKind.InPlace)
        h.commitOk(writeRange(p, v1, 10, bytes(8, 2), 2)) // in-place write held (in flight)
        val v2 = patched(v1, 10, bytes(8, 2))
        h.commitOk(writeRange(p, v2, 40, bytes(8, 3), 3)) // queued behind it
        val v3 = patched(v2, 40, bytes(8, 3))
        val cp = h.backend.checkpointNow()
        val journalHold = oneShot(isKind(FsifBackend.IoKind.Journal))
        ec.hold = r => isKind(FsifBackend.IoKind.InPlace)(r) || journalHold(r)
        val fd = h.issueCommit(delete(p, 4)) // cancels the queued Delta; its Map(X, -1) batch is held
        ec.release(isKind(FsifBackend.IoKind.InPlace)) // the first Delta is applied: no journal-s Delta is left
        fd.isCompleted shouldBe false
        cp.isCompleted shouldBe false
        if crashBeforeRelease then
          h.crash()
          h.readData(p) shouldBe v3.toList
        else
          ec.hold = _ => false
          ec.release()
          h.await(fd) shouldBe Left(())
          h.await(cp)
          h.read(p) shouldBe Right(ReadError.ObjectNotFound)
          h.crash()
          h.read(p) shouldBe Right(ReadError.ObjectNotFound)

  test("a crash at any file op during a checkpoint with commits in flight keeps every acknowledged write"):
    // Two stores on one machine and one device journal; the checkpoint runs on store 0 while both commit.
    // Coverage: seeds whose crash cut the checkpoint short, and seeds whose checkpoint completed first
    var cutShort = 0
    var completed = 0
    for seed <- 0 until 50 do
      withClue(s"seed=$seed"):
        val r = rng(seed + 5000)
        val shared = new SharedJournal(rng(seed))
        val hs = Vector.tabulate(2)(i => new Harness(seed, mkIo = () => new ShuffleEc(r), sharedWith = Some(shared),
          idx = i))
        hs(0).peers = Seq(hs(1))
        hs(1).peers = Seq(hs(0))
        val n = 6
        val ps = (0 until n).map(x => ptr(x, oid(x + 1)))
        // Per store and ext, every issued version in order (None = deleted), with whether it was acknowledged
        val hist = Array.fill(2, n)(mutable.ArrayBuffer[(Option[Array[Byte]], () => Boolean)]())
        var rev = 1
        for s <- 0 until 2; x <- 0 until n do
          val v = bytes(20 + r.nextInt(400), x + s)
          hs(s).commitOk(alloc(ps(x), v, rev))
          hist(s)(x) += ((Some(v), () => true))
        hs.foreach(_.quiesce())
        val ios = hs.map(_.io.asInstanceOf[ShuffleEc])
        def runAll(): Unit =
          while ios.exists(_.pending > 0) do ios.foreach(_.runAll())
        val startSeq = hs(0).backend.indexSeq
        val opCount = 14
        val cpAt = r.nextInt(opCount)
        for i <- 0 until opCount do
          if i == cpAt then
            // Counted from the checkpoint's start: the crash lands inside it or just after it (a few ops plus those of
            // the commits in flight)
            shared.machine.crashAfterOps(r.nextInt(50))
            hs(0).backend.checkpointNow()
          val s = r.nextInt(2)
          val x = r.nextInt(n)
          rev += 1
          val (cs, next) = hist(s)(x).last._1 match
            case None =>
              val v = bytes(r.nextInt(600), rev)
              (alloc(ps(x), v, rev), Some(v))
            case Some(old) =>
              r.nextInt(4) match
                case 1 if old.length > 0 =>
                  val off = r.nextInt(old.length)
                  val patch = bytes(1 + r.nextInt(math.min(16, old.length - off)), rev)
                  (writeRange(ps(x), old, off, patch, rev), Some(patched(old, off, patch)))
                case 2 if old.length + 40 <= layout.segmentPayloadSize =>
                  val add = bytes(1 + r.nextInt(40), rev)
                  (append(ps(x), old, add, rev), Some(old ++ add))
                case 3 =>
                  (delete(ps(x), rev), None)
                case _ =>
                  val v = bytes(r.nextInt(900), rev) // over 256 bytes goes copy-on-write
                  (overwrite(ps(x), v, rev), Some(v))
          val f = hs(s).issueCommit(cs)
          hist(s)(x) += ((next, () => acked(f)))
          for _ <- 0 until r.nextInt(6) do ios(r.nextInt(2)).runOne()
        runAll()
        val done = hs(0).backend.indexSeq == startSeq + 1
        if hs(0).fatals.isEmpty then done shouldBe true
        // The I/O stop was armed as the checkpoint started, so a stop before its index is durable landed inside it
        if done then completed += 1
        else if !hs(0).fatals.isEmpty then cutShort += 1
        shared.crash()
        hs.foreach(_.reopen())
        for s <- 0 until 2; x <- 0 until n do
          withClue(s"store=$s ext=$x"):
            val k = hist(s)(x).lastIndexWhere(_._2())
            val allowed = hist(s)(x).drop(k).map(_._1.map(_.toList)).toList
            val got = hs(s).read(ps(x)) match
              case Left(rs) => Some(rs.data.getByteArray.toList)
              case Right(ReadError.ObjectNotFound) => None
              case Right(e) => fail(s"read failed: $e")
            allowed should contain(got)
    info(s"checkpoints cut short by the crash: $cutShort, completed before it: $completed")
    cutShort should be >= 10
    completed should be >= 10

object FsifCheckpointSuite:
  /** Runs every task inline, except that tasks matching `hold` are queued until released */
  final class HoldEc extends ExecutionContext:
    @volatile var hold: Runnable => Boolean = _ => false
    private val held = mutable.ArrayBuffer[Runnable]()
    def execute(r: Runnable): Unit =
      val queued = synchronized:
        val h = hold(r)
        if h then held += r
        h
      if !queued then r.run()
    def reportFailure(t: Throwable): Unit = throw t
    def heldCount: Int = synchronized(held.size)
    /** Runs the held tasks matching `p` (tasks they queue are subject to `hold` again) */
    def release(p: Runnable => Boolean = _ => true): Unit =
      val ready = synchronized:
        val (run, keep) = held.partition(p)
        held.clear()
        held ++= keep
        run.toList
      ready.foreach(_.run())

  /** Matches only the first runnable that satisfies `p` */
  def oneShot(p: Runnable => Boolean): Runnable => Boolean =
    var spent = false
    r =>
      if !spent && p(r) then
        spent = true
        true
      else
        false
