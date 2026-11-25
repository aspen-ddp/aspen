package org.aspen_ddp.aspen.server.store.backend.fsif

import java.io.IOException
import java.nio.ByteBuffer
import java.util.UUID
import java.util.concurrent.atomic.AtomicBoolean

import scala.collection.mutable
import scala.concurrent.Future
import scala.util.Random
import scala.util.control.NonFatal

import org.scalatest.exceptions.TestFailedException

import org.aspen_ddp.aspen.FileBasedTests
import org.aspen_ddp.aspen.common.{DataBuffer, HLCTimestamp}
import org.aspen_ddp.aspen.common.ida.{AppendDelta, XorRegion}
import org.aspen_ddp.aspen.common.objects.{DataObjectPointer, Metadata, ObjectId, ObjectPointer, ObjectRefcount,
  ObjectRevision, ObjectType, ReadError}
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.{ReadState, StoreId}
import org.aspen_ddp.aspen.common.transaction.{DataUpdate, DataUpdateOperation, RefcountUpdate, TransactionId,
  VersionBump}
import org.aspen_ddp.aspen.server.store.backend.{Commit, CommitError, CommitState, Completion, CompletionHandler,
  Read, UpdateForm}

/** Model-based randomized crash test (Task 13).
  *
  * Three stores share one device journal on one simulated machine, so every crash covers all of them and each
  * recovery is a device load (the journal's replay into every store) followed by each store's open. Each store's
  * model is a plain map ext -> history of the states issued to it since the last recovery, each with its commit
  * result, checked separately. After every crash, each ext must read back as the state of its last acknowledged
  * commit or of a later issued one (any prefix of its commit order), byte for byte including metadata and type;
  * every other candidate ObjectId must give ObjectNotFound, and nothing may ever read as CorruptedObject except after
  * the crash's I/O stop has fail-stopped the store.
  *
  * Everything runs on the test thread: io (and so the serial executor) is a seeded ShuffleEc and ENOSPC retries run
  * on a ManualScheduler, so a seed's run is reproducible.
  */
class FsifModelSuite extends FileBasedTests:
  import FsifBackendSuite.{ShuffleEc, journalOn}
  import FsifFaultSuite.ManualScheduler
  import FsifModelSuite.*

  private val layout = FsifLayout(1024, 16)
  private val tuning = FsifTuning(cowThreshold = 256, unappliedBound = 4096)
  /** A small device journal: its pressure starts checkpoints, as a small journalBound did */
  private val sizing = JournalSizing(new UUID(9, 9), 3, 16 * 1024)
  private val poolId = PoolId(new UUID(7, 7))
  /** Stores sharing the device journal, one machine */
  private val Stores = 3
  private val Exts = 16
  private val Ops = 400
  private val CoverageFloor = 3

  private def splitMix(seed: Long): Long =
    var z = seed * 0x9e3779b97f4a7c15L + 0x9e3779b97f4a7c15L
    z = (z ^ (z >>> 30)) * 0xbf58476d1ce4e5b9L
    z = (z ^ (z >>> 27)) * 0x94d049bb133111ebL
    z ^ (z >>> 31)

  private def sp(x: Int): Array[Byte] = ByteBuffer.allocate(4).putInt(x).array()
  private def ptr(x: Int, id: ObjectId): ObjectPointer = DataObjectPointer(id, poolId, sp(x))

  private def isSimCrash(t: Throwable): Boolean =
    t.isInstanceOf[IOException] && Option(t.getMessage).exists(_.contains("simulated crash"))

  // ---- Model ----

  private final case class Obj(id: ObjectId, md: Metadata, tpe: ObjectType.Value, data: Array[Byte]):
    def same(o: Obj): Boolean =
      id == o.id && md == o.md && tpe == o.tpe && java.util.Arrays.equals(data, o.data)
    override def toString: String =
      s"Obj(${id.uuid.getLeastSignificantBits}, rev=${md.revision}, rc=${md.refcount}, $tpe, len=${data.length}, " +
        s"hash=${java.util.Arrays.hashCode(data)})"

  private def sameState(a: Option[Obj], b: Option[Obj]): Boolean = (a, b) match
    case (None, None) => true
    case (Some(x), Some(y)) => x.same(y)
    case _ => false

  /** One issued commit (or the recovered baseline, which counts as acknowledged) */
  private final class Entry(val state: Option[Obj], val what: String, baseline: Boolean = false):
    var result: Option[CommitResult] = if baseline then Some(Left(())) else None
    def acked: Boolean = result.contains(Left(()))
    override def toString: String = s"$what -> $state [${result.getOrElse("pending")}]"

  private final class ReadRec(val x: Int, val id: ObjectId, val from: Int):
    var result: Option[ReadResult] = None
    /** History size at completion: the read may see any state issued before it completed */
    var end = 0
    var firedAt = false

  /** Routes completions to the issued Entry / ReadRec. Runs on serial, so it only records. */
  private final class Handler(endOf: Int => Int, firedNow: () => Boolean) extends CompletionHandler:
    val commits = mutable.HashMap[TransactionId, Entry]()
    val reads = mutable.HashMap[ObjectId, mutable.Queue[ReadRec]]()
    val errors = mutable.ArrayBuffer[String]()
    def complete(op: Completion): Unit = op match
      case c: Commit =>
        commits.remove(c.transactionId) match
          case Some(e) =>
            e.result = Some(c.result)
            if c.result.isRight && !(c.result == Right(CommitError.IOFailure) && firedNow()) then
              errors += s"commit $e completed with ${c.result} without a crash"
          case None => errors += s"completion of an unknown commit $c"
      case r: Read =>
        reads.get(r.objectId).filter(_.nonEmpty) match
          case Some(q) =>
            val rr = q.dequeue()
            rr.result = Some(r.result)
            rr.end = endOf(rr.x)
            rr.firedAt = firedNow()
          case None => errors += s"completion of an unknown read $r"

  /** Calls `onCrashIo` when an I/O fails because the simulated machine stopped (crashAfterOps), before the failure
    * reaches the backend, so the backend's state at that moment can be sampled
    */
  private final class ProbeFiles(under: FsifFiles, onCrashIo: () => Unit) extends FsifFiles:
    def exists(name: String): Boolean = under.exists(name)
    def size(name: String): Long = under.size(name)
    def open(name: String): FsifFile =
      val f = under.open(name)
      new FsifFile:
        def size: Long = f.size
        def read(position: Long, length: Int): Array[Byte] = f.read(position, length)
        def write(position: Long, bytes: Array[Byte]): Unit = probe(f.write(position, bytes))
        def truncate(size: Long): Unit = probe(f.truncate(size))
        def force(): Unit = probe(f.force())
        def close(): Unit = f.close()
    private def probe(op: => Unit): Unit =
      try op
      catch
        case e: IOException if isSimCrash(e) =>
          onCrashIo()
          throw e

  /** One seed's run over `Stores` stores sharing one device journal (one machine, one io, one crash). Each store has
    * its own model, checked separately after every crash. `interruptRecovery`: every recovery is first cut off by
    * crashAfterOps, possibly repeatedly, and the final recovery must then yield exactly the state an uninterrupted
    * recovery of the same image yields.
    */
  private final class Run(seed: Int, interruptRecovery: Boolean):
    private val scrambled = splitMix(seed.toLong)
    private val rnd = new Random(scrambled)
    private val ioRnd = new Random(splitMix(scrambled ^ 0x10))
    var shared = new SharedJournal(new Random(splitMix(scrambled ^ 0x20)), sizing)
    for s <- 0 until Stores do shared.initialize(s, layout)

    val log = mutable.ArrayBuffer[String]()
    private var nextId = 0
    private var nextMeta = 0
    private var txn = 0
    private var op = 0

    // Per incarnation, shared by the stores
    var io: ShuffleEc = new ShuffleEc(ioRnd)
    var sched = new ManualScheduler
    /** The simulated machine has stopped (crashAfterOps ran out) */
    private var fired = false
    private var firedSample: Option[Sample] = None
    private var armed = false
    private var opened = false
    /** The device load's replay is running (the journal is being built) */
    private var replaying = false
    /** The open ENOSPC window: its store and flag */
    private var window: Option[(Int, AtomicBoolean)] = None
    private var windowEnd = 0

    // Counters
    val opCounts = mutable.TreeMap[String, Int]()
    val cov = new Coverage
    private var checkpoints = 0L
    private var moves = 0
    private var ackedCommits = 0
    private var unackedSurvived = 0
    /** Commits not yet acknowledged when a crash hit */
    private var unackedAtCrash = 0

    private def count(k: String): Unit = opCounts(k) = opCounts.getOrElse(k, 0) + 1
    private def note(s: String): Unit = log += s"#$op $s"

    /** One store: its model and its current incarnation */
    private final class St(val idx: Int):
      val sid: StoreId = StoreId(poolId, idx.toByte)
      def files: SharedJournal.StoreFiles = shared.storeFiles(idx)
      val history = Array.fill(Exts)(mutable.ArrayBuffer(new Entry(None, "initial", baseline = true)))
      val pendingReads = mutable.ArrayBuffer[ReadRec]()
      var handler: Handler = null
      var backend: FsifBackend = null
      val fatals = mutable.ArrayBuffer[Throwable]()
      var startSeq = 0L

      def note(s: String): Unit = Run.this.note(s"s$idx $s")

      def open(): Unit =
        fatals.clear()
        val h = new Handler(x => history(x).size, () => fired)
        handler = h
        val b = shared.open(idx, layout, io, tuning, t => fatals += t, sched,
          wrap = f => new ProbeFiles(f, onCrashIo), storeId = sid)
        b.setCompletionHandler(h)
        backend = b
        startSeq = b.indexSeq

      def checkErrors(): Unit =
        if handler.errors.nonEmpty then fail(s"store $idx: " + handler.errors.mkString("; "))
        if fatals.nonEmpty && !fired then fail(s"store $idx fail-stopped without a crash: ${fatals.head}")

      // ---- Commits ----

      private def issued(x: Int): Option[Obj] = history(x).last.state

      def lastAcked(x: Int): Int = history(x).lastIndexWhere(_.acked)

      private def issue(x: Int, cs: CommitState, next: Option[Obj], what: String): Unit =
        txn += 1
        val tx = TransactionId(new UUID(seed.toLong + 2000, txn))
        val e = new Entry(next, s"#$op s$idx $what")
        history(x) += e
        handler.commits(tx) = e
        note(s"commit x=$x $what -> $next")
        count(what.takeWhile(_ != '('))
        backend.commit(cs, tx)

      private def form(id: ObjectId, reqs: List[org.aspen_ddp.aspen.common.transaction.TransactionRequirement],
                       updates: Map[ObjectId, DataBuffer]): UpdateForm =
        UpdateForm.of(id, reqs, updates, allocating = false)

      def commitOp(x: Int): Unit =
        issued(x) match
          case None =>
            val id = newId()
            val large = rnd.nextInt(10) < 3
            val size = if large then 257 + rnd.nextInt(768) else rnd.nextInt(257)
            val tpe = if rnd.nextInt(8) == 0 then ObjectType.KeyValue else ObjectType.Data
            val o = Obj(id, meta(1), tpe, randomBytes(size))
            val cs = CommitState(ptr(x, id), o.md, tpe, DataBuffer(o.data), allocating = true,
              form = UpdateForm.of(id, Nil, Map(), allocating = true))
            issue(x, cs, Some(o), if large then "allocLarge" else "allocSmall")
          case Some(o) =>
            val len = o.data.length
            val isData = o.tpe == ObjectType.Data
            val r = rnd.nextInt(100)
            if r < 25 && isData && len > 0 then smallWrite(x, o)
            else if r < 45 && isData && len < layout.segmentPayloadSize then appendOp(x, o)
            else if r < 55 then overwrite(x, o, large = false)
            else if r < 70 then overwrite(x, o, large = true)
            else if r < 85 then metadataOnly(x, o)
            else deleteOp(x, o)

      /** WriteRange: 1-3 sorted regions of an AppendDelta, as a RARS ranged write sends them */
      private def smallWrite(x: Int, o: Obj): Unit =
        val len = o.data.length
        val nu = o.data.clone()
        val regions = mutable.ArrayBuffer[XorRegion]()
        var pos = 0
        for _ <- 0 until 1 + rnd.nextInt(3) do
          if pos < len then
            val off = pos + rnd.nextInt(math.max(1, (len - pos) / 2))
            if off < len then
              val l = 1 + rnd.nextInt(math.min(64, len - off))
              val patch = randomBytes(l)
              System.arraycopy(patch, 0, nu, off, l)
              regions += XorRegion(off, Array.tabulate[Byte](l)(i => (o.data(off + i) ^ patch(i)).toByte))
              pos = off + l
        val delta = AppendDelta(len, len, regions.toList, None)
        deltaCommit(x, o, delta, nu, DataUpdateOperation.WriteRange, "smallWrite")

      /** Append: grows the slice, optionally rewriting the last few old bytes (a RARS trailer) in the same region */
      private def appendOp(x: Int, o: Obj): Unit =
        val len = o.data.length
        val add = 1 + rnd.nextInt(math.min(200, layout.segmentPayloadSize - len))
        val rewrite = if rnd.nextInt(3) == 0 then rnd.nextInt(math.min(8, len) + 1) else 0
        val nu = java.util.Arrays.copyOf(o.data, len + add)
        val fresh = randomBytes(rewrite + add)
        System.arraycopy(fresh, 0, nu, len - rewrite, fresh.length)
        val start = len - rewrite
        val xor = Array.tabulate[Byte](rewrite + add): i =>
          val old = if start + i < len then o.data(start + i) else 0.toByte
          (old ^ nu(start + i)).toByte
        val delta = AppendDelta(len, len + add, List(XorRegion(start, xor)), None)
        deltaCommit(x, o, delta, nu, DataUpdateOperation.Append, "append")

      private def deltaCommit(x: Int, o: Obj, delta: AppendDelta, expected: Array[Byte],
                              operation: DataUpdateOperation.Value, what: String): Unit =
        val data = delta.applyTo(DataBuffer(o.data)).getByteArray
        assert(java.util.Arrays.equals(data, expected), "test bug: AppendDelta does not produce the model's bytes")
        val p = ptr(x, o.id)
        val f = form(o.id, List(DataUpdate(p, o.md.revision, operation)), Map(o.id -> delta.encode))
        val n = o.copy(md = meta(o.md.refcount.count), data = data)
        issue(x, CommitState(p, n.md, o.tpe, DataBuffer(data), form = f), Some(n), what)

      private def overwrite(x: Int, o: Obj, large: Boolean): Unit =
        val size = if large then 257 + rnd.nextInt(768) else rnd.nextInt(257)
        val p = ptr(x, o.id)
        val n = o.copy(md = meta(o.md.refcount.count), data = randomBytes(size))
        val f = form(o.id, List(DataUpdate(p, o.md.revision, DataUpdateOperation.Overwrite)), Map())
        issue(x, CommitState(p, n.md, o.tpe, DataBuffer(n.data), form = f), Some(n),
          if large then "overwriteLarge" else "overwriteSmall")

      private def metadataOnly(x: Int, o: Obj): Unit =
        val p = ptr(x, o.id)
        val (n, req) =
          if rnd.nextBoolean() then
            val n = o.copy(md = meta(1 + rnd.nextInt(3)))
            (n, RefcountUpdate(p, o.md.refcount, n.md.refcount))
          else
            (o.copy(md = meta(o.md.refcount.count)), VersionBump(p, o.md.revision))
        val f = form(o.id, List(req), Map())
        assert(f == UpdateForm.MetadataOnly)
        issue(x, CommitState(p, n.md, o.tpe, DataBuffer(o.data), form = f), Some(n), "metadataOnly")

      private def deleteOp(x: Int, o: Obj): Unit =
        val p = ptr(x, o.id)
        val md = meta(0)
        val f = form(o.id, List(RefcountUpdate(p, o.md.refcount, md.refcount)), Map())
        issue(x, CommitState(p, md, o.tpe, DataBuffer(o.data), form = f), None, "delete")

      // ---- Reads ----

      /** The id a read of x uses: the newest issued object, else the newest one ever issued there, else one never
        * allocated
        */
      private def readId(x: Int): ObjectId =
        history(x).reverseIterator.flatMap(_.state).nextOption().map(_.id).getOrElse(ObjectId(new UUID(-1, x)))

      private def issueRead(x: Int, id: ObjectId, from: Int): ReadRec =
        val rr = new ReadRec(x, id, from)
        handler.reads.getOrElseUpdate(id, mutable.Queue()).enqueue(rr)
        backend.read(ptr(x, id))
        rr

      def readOp(x: Int): Unit =
        val id = readId(x)
        note(s"read x=$x id=${id.uuid.getLeastSignificantBits}")
        count("read")
        pendingReads += issueRead(x, id, lastAcked(x))

      /** Checks completed reads: the result is the state of a commit between the last acknowledged one at issue and
        * the last issued one at completion
        */
      def checkReads(): Unit =
        val done = pendingReads.filter(_.result.isDefined)
        pendingReads.filterInPlace(_.result.isEmpty)
        for rr <- done do
          val cands = history(rr.x).slice(rr.from, rr.end).map(_.state)
          def clue = s"store $idx: read of x=${rr.x} id=${rr.id.uuid.getLeastSignificantBits} gave " +
            s"${rr.result.get}; candidates ${history(rr.x).slice(rr.from, rr.end).mkString(", ")}"
          rr.result.get match
            case Left(rs) =>
              val got = Obj(rs.objectId, rs.metadata, rs.objectType, rs.data.getByteArray)
              if !cands.exists(c => sameState(c, Some(got))) then fail(clue)
            case Right(ReadError.ObjectNotFound) =>
              if !cands.exists(c => !c.exists(_.id == rr.id)) then fail(clue)
            case Right(ReadError.CorruptedObject) if rr.firedAt => ()
            case Right(_) => fail(clue)

      /** Before a crash: what the crash's counters need */
      def beforeCrash(): Unit =
        checkReads()
        checkErrors()
        pendingReads.clear()
        checkpoints += backend.indexSeq - startSeq
        moves += backend.compactionStats.mapped
        for x <- 0 until Exts do
          ackedCommits += history(x).drop(1).count(_.acked)
          unackedAtCrash += history(x).count(!_.acked)
        // Rules (and so faults) live only in the incarnation's files, which this crash replaces
        cov.dataForceFaults += files.faultCount(FsifRecovery.DataFile, FaultOp.Force)

      /** Every ext matches this store's model; the recovered state becomes each ext's acknowledged baseline */
      def verifyRecovered(): Unit =
        for x <- 0 until Exts do
          val got = stateOf(backend, handler, io, x, s"store $idx, recovered")
          val k = lastAcked(x)
          val cands = history(x).drop(k)
          if !cands.exists(e => sameState(e.state, got)) then
            fail(s"store $idx x=$x recovered as $got; allowed (last acknowledged first): ${cands.mkString(", ")}")
          if !sameState(cands.head.state, got) then unackedSurvived += 1
          // Every id ever issued at x since the last recovery, not only the candidates: none but the recovered one
          // may read back
          for id <- history(x).flatMap(_.state).map(_.id).distinct if !got.exists(_.id == id) do
            val r = syncRead(backend, handler, io, x, id)
            if r != Right(ReadError.ObjectNotFound) then
              fail(s"store $idx x=$x recovered as $got, but a read of candidate " +
                s"${id.uuid.getLeastSignificantBits} gives $r")
          history(x).clear()
          history(x) += new Entry(got, s"#$op recovered", baseline = true)
        checkErrors()

      /** The newest index is attached and not detached: the device load replays the store's records into it */
      def midCheckpointImage: Boolean =
        try !FsifRecovery.newestIndex(files, layout)._3.detached
        catch case _: FsifRecovery.StoreLost => false

      /** The seqs of the valid index files in the image */
      def indexSeqs: Vector[Long] =
        FsifRecovery.IndexFiles.flatMap: name =>
          if !files.exists(name) then
            None
          else
            val f = files.open(name)
            try IndexFile.decode(f.read(0, f.size.toInt), layout.segmentCount).map(_._1)
            finally f.close()

    private val stores = Vector.tabulate(Stores)(new St(_))

    private def sample(): Sample =
      Sample(stores.exists(_.backend.checkpointRunning), stores.exists(_.backend.moveRunning),
        stores.exists(_.backend.isWriteFrozen))

    /** A device load (the journal's build replays every store), then every store opens; per-incarnation state starts
      * fresh
      */
    private def openAll(): Unit =
      io = new ShuffleEc(ioRnd)
      sched = new ManualScheduler
      fired = false
      firedSample = None
      armed = false
      opened = false
      window = None
      shared.run = journalOn(io)
      shared.wrapJournal = f => new ProbeFiles(f, onCrashIo)
      replaying = true
      shared.journal
      replaying = false
      stores.foreach(_.open())
      opened = true

    private def onCrashIo(): Unit =
      if !fired then
        fired = true
        if opened then firedSample = Some(sample())

    def checkErrors(): Unit = stores.foreach(_.checkErrors())

    private def checkReads(): Unit = stores.foreach(_.checkReads())

    // ---- Pumping ----

    private def steps(n: Int): Unit =
      var i = 0
      while i < n && io.pending > 0 do
        io.runOne()
        i += 1

    private def anyFrozen: Boolean = stores.exists(_.backend.isWriteFrozen)

    /** Retries held I/O until every store unfreezes; the fault window must already be closed */
    private def unfreeze(): Unit =
      var i = 0
      while anyFrozen && !fired && i < 100 do
        sched.tick()
        io.runAll()
        i += 1
      if anyFrozen && !fired then fail("a store stays frozen after space returned")

    private def settle[A](f: Future[A], what: String): A =
      io.runAll()
      if !f.isCompleted && window.isEmpty then
        unfreeze()
        io.runAll()
      f.value match
        case Some(r) => r.get
        case None => fail(s"$what never completed")

    // ---- Model helpers ----

    private def newId(): ObjectId =
      nextId += 1
      ObjectId(new UUID(seed.toLong, nextId))

    private def meta(refcount: Int): Metadata =
      nextMeta += 1
      Metadata(ObjectRevision(TransactionId(new UUID(seed.toLong + 1000, nextMeta))),
        ObjectRefcount(nextMeta, refcount), HLCTimestamp(nextMeta.toLong))

    private def randomBytes(n: Int): Array[Byte] =
      val a = new Array[Byte](n)
      rnd.nextBytes(a)
      a

    /** A read on `b` run to completion (verification only) */
    private def syncRead(b: FsifBackend, h: Handler, ec: ShuffleEc, x: Int, id: ObjectId): ReadResult =
      val rr = new ReadRec(x, id, 0)
      h.reads.getOrElseUpdate(id, mutable.Queue()).enqueue(rr)
      b.read(ptr(x, id))
      ec.runAll()
      rr.result.getOrElse(fail(s"verification read of x=$x never completed"))

    /** The state x holds in a freshly recovered backend: its mapping, then a read of the mapped id */
    private def stateOf(b: FsifBackend, h: Handler, ec: ShuffleEc, x: Int, who: String): Option[Obj] =
      val m = b.mapping(x)
      ec.runAll()
      val (slot, id) = m.value.getOrElse(fail(s"$who: mapping($x) never completed")).get
      if slot < 0 then
        None
      else
        syncRead(b, h, ec, x, id) match
          case Left(rs) => Some(Obj(rs.objectId, rs.metadata, rs.objectType, rs.data.getByteArray))
          case Right(err) =>
            fail(s"$who: x=$x maps to slot $slot as ${id.uuid.getLeastSignificantBits} but reads $err")

    // ---- Crashes and recovery ----

    private def crash(reason: String): Unit =
      val s = firedSample.getOrElse(sample())
      note(s"crash ($reason, fired=$fired, $s)")
      cov.crashes += 1
      if s.checkpoint then cov.inCheckpoint += 1
      if s.move then cov.inMove += 1
      if s.frozen then cov.frozen += 1
      stores.foreach(_.beforeCrash())
      shared.crash()
      recover()
      stores.foreach(_.verifyRecovered())

    private def countIndexes(): Unit =
      for st <- stores do
        val seqs = st.indexSeqs
        if seqs.size == 2 then cov.staleIndexImages += 1 // index m-1 beside index m: step 4 had not emptied it
        if seqs.exists(_ > 0) then cov.laterIndexImages += 1

    /** Loads the crashed image. With `interruptRecovery`, recovery is first cut off one to three times, each at a
      * uniformly chosen one of the I/O ops an uninterrupted recovery of the image performs (the device-load replay
      * and every store's open; or just after its last, so the recovered stores' own I/O stops), and the final
      * recovery must give exactly the state the uninterrupted one gives.
      */
    private def recover(): Unit =
      val midCheckpoint = stores.exists(_.midCheckpointImage)
      if midCheckpoint then cov.midCheckpointImages += 1
      countIndexes()
      val reference =
        if !interruptRecovery then
          None
        else
          val ref = shared.snapshot()
          val ec = new ShuffleEc(new Random(splitMix(scrambled ^ op)))
          ref.run = journalOn(ec)
          val states = stores.map: st =>
            val h = new Handler(_ => 0, () => false)
            val b = ref.open(st.idx, layout, ec, tuning, _ => (), new ManualScheduler, storeId = st.sid)
            b.setCompletionHandler(h)
            (b, h)
          val recoveryOps = ref.machine.opCount.toInt
          Some((states.zipWithIndex.map { case ((b, h), i) =>
            (0 until Exts).map(x => stateOf(b, h, ec, x, s"store $i, reference recovery"))
          }, recoveryOps))
      reference.foreach: (_, recoveryOps) =>
        var again = true
        var depth = 0
        while again do
          val n = rnd.nextInt(recoveryOps + 2)
          shared.machine.crashAfterOps(n)
          opened = false
          try
            openAll()
            // Recovery finished within n ops; the stores run on until their I/O stops
            cov.recoveryCompleted += 1
            note(s"recovery within $n of $recoveryOps ops completed")
            steps(rnd.nextInt(20))
          catch
            case e: IOException if isSimCrash(e) =>
              cov.inRecovery += 1
              if replaying then cov.inReplay += 1
              if midCheckpoint then cov.inRecoveryCheckpoint += 1
              note(s"recovery cut off after $n of $recoveryOps ops (in device-load replay: $replaying, " +
                s"mid-checkpoint image: $midCheckpoint)")
            case NonFatal(t) =>
              fail(s"recovery cut off after $n ops threw $t (a crash during recovery must stay recoverable)", t)
          shared.crash()
          countIndexes()
          depth += 1
          again = depth < 3 && rnd.nextInt(3) == 0
      try openAll()
      catch case NonFatal(t) => fail(s"recovery after a crash threw $t", t)
      reference.foreach: (ref, _) =>
        for st <- stores; x <- 0 until Exts do
          val got = stateOf(st.backend, st.handler, io, x, s"store ${st.idx}, after interrupted recoveries")
          if !sameState(ref(st.idx)(x), got) then
            fail(s"store ${st.idx} x=$x: recovery after interrupted recoveries gives $got, an uninterrupted one " +
              s"gives ${ref(st.idx)(x)}")

    // ---- Out-of-space windows ----

    private def openWindow(): Unit =
      val s = rnd.nextInt(Stores)
      val flag = new AtomicBoolean(true)
      val (fileName, file) = rnd.nextInt(5) match
        case 0 => ("all", (_: String) => true)
        case 1 => ("indexes", (n: String) => n.startsWith("index")) // the device journal never runs out of space
        case 2 | 3 => ("data", (n: String) => n == FsifRecovery.DataFile)
        case _ => ("indexes", (n: String) => n.startsWith("index"))
      // ENOSPC on a data force drops every write since the last good force (the simulator discards them), so it is
      // the most demanding fault: data windows are forces only half the time
      val (opsName, ops) = if fileName == "data" && rnd.nextBoolean() then ("force", Set(FaultOp.Force)) else
        rnd.nextInt(3) match
          case 0 => ("all", FaultOp.values.toSet)
          case 1 => ("write", Set(FaultOp.Write))
          case _ => ("force", Set(FaultOp.Force))
      stores(s).files.failWhile(Fault.OutOfSpace, file, ops)(flag.get)
      window = Some((s, flag))
      windowEnd = op + 1 + rnd.nextInt(10)
      note(s"ENOSPC window on s$s $fileName/$opsName until #$windowEnd")
      count("enospcWindow")

    private def closeWindow(): Unit =
      window.foreach: (_, flag) =>
        flag.set(false)
        window = None
        note("ENOSPC window closed")
        unfreeze()

    // ---- The run ----

    private def randomOp(): Unit =
      val st = stores(rnd.nextInt(Stores))
      val x = rnd.nextInt(Exts)
      val r = rnd.nextInt(100)
      if r < 50 then
        st.commitOp(x)
      else if r < 56 then
        // Overlapping commits to one ext, as the object cache may send them
        for _ <- 0 until 2 + rnd.nextInt(2) do st.commitOp(x)
      else if r < 78 then
        st.readOp(x)
      else if r < 85 then
        st.note("checkpointNow")
        count("checkpointNow")
        st.backend.checkpointNow()
      else if r < 92 then
        if window.isEmpty then
          st.note("quiesce")
          count("quiesce")
          settle(st.backend.quiesce(), "quiesce")
      else if r < 96 then
        if window.isEmpty && !fired then openWindow()
      else
        note("pump")
        io.runAll()

    def run(): Result =
      openAll()
      var crashesLeft = 3 + rnd.nextInt(3)
      var crashAt = 0
      var armAt = -1
      def plan(): Unit =
        val seg = (Ops - op) / (crashesLeft + 1)
        crashAt = op + 1 + rnd.nextInt(math.max(1, 2 * seg))
        armAt = if rnd.nextInt(4) == 0 then -1 else op + rnd.nextInt(crashAt - op)
      plan()
      while op < Ops do
        if crashesLeft > 0 && op == armAt && !armed then
          val n = rnd.nextInt(200)
          shared.machine.crashAfterOps(n)
          armed = true
          note(s"crashAfterOps($n)")
        val frozenCrash = window.exists((s, _) => stores(s).backend.isWriteFrozen) && rnd.nextInt(5) == 0
        if crashesLeft > 0 && (fired || op >= crashAt || frozenCrash) then
          if fired then
            steps(rnd.nextInt(10))
          else if rnd.nextBoolean() then
            // Land the crash with several commits part way through their I/O
            for _ <- 0 until 2 + rnd.nextInt(7) do stores(rnd.nextInt(Stores)).commitOp(rnd.nextInt(Exts))
            steps(rnd.nextInt(30))
          crash(if fired then "I/O stopped" else if frozenCrash then "while frozen" else "scheduled")
          crashesLeft -= 1
          plan()
        else
          if window.isDefined && op >= windowEnd then closeWindow()
          randomOp()
          if window.isDefined && rnd.nextInt(3) == 0 then sched.tick()
          steps(rnd.nextInt(12))
          checkReads()
          checkErrors()
          op += 1
      // The final state: a stopped or still-armed machine crashes first, then everything settles and is acknowledged
      if fired || armed then crash("final")
      closeWindow()
      for st <- stores do settle(st.backend.quiesce(), s"store ${st.idx}'s final quiesce")
      checkReads()
      checkErrors()
      for st <- stores; x <- 0 until Exts do
        for e <- st.history(x) if !e.acked do fail(s"store ${st.idx} x=$x: $e is not acknowledged after quiesce")
        val got = stateOf(st.backend, st.handler, io, x, s"store ${st.idx}, final")
        if !sameState(st.history(x).last.state, got) then
          fail(s"store ${st.idx} x=$x reads $got, expected ${st.history(x).last}")
      crash("clean")
      val finalHash = stores.map(st => (0 until Exts).map(x => st.history(x).last.state.map(o =>
        (o.id, o.md, o.tpe, java.util.Arrays.hashCode(o.data))).hashCode).hashCode).hashCode
      val digest = s"ops=${opCounts.mkString(",")} crashes=${cov.crashes} (cp ${cov.inCheckpoint}, move " +
        s"${cov.inMove}, frozen ${cov.frozen}) recovery cut/completed=${cov.inRecovery}/${cov.recoveryCompleted} " +
        s"replay cut=${cov.inReplay} midCheckpoint images/cut=${cov.midCheckpointImages}/" +
        s"${cov.inRecoveryCheckpoint} checkpoints=$checkpoints moves=$moves acked=$ackedCommits " +
        s"unackedAtCrash=$unackedAtCrash unackedSurvived=$unackedSurvived hash=$finalHash"
      Result(seed, digest, cov)

  private def runSeed(seed: Int, interruptRecovery: Boolean): Result =
    val run = new Run(seed, interruptRecovery)
    val clue = new Object:
      override def toString: String =
        s"seed=$seed (interruptRecovery=$interruptRecovery); last ops:\n  " + run.log.takeRight(40).mkString("\n  ")
    withClue(clue):
      try run.run()
      catch
        case e: TestFailedException => throw e
        case NonFatal(t) => fail(s"unexpected exception: $t", t)

  private def runSuite(seeds: Seq[Int], interruptRecovery: Boolean): Unit =
    val results = seeds.map(s => runSeed(s, interruptRecovery))
    results.foreach(r => info(s"seed ${r.seed}: ${r.digest}"))
    // Determinism: a seed's run is reproducible
    for r <- results.take(2) do
      withClue(s"seed=${r.seed} rerun: ")(runSeed(r.seed, interruptRecovery).digest shouldBe r.digest)
    val total = results.map(_.cov).reduce(_ + _)
    info(s"coverage: $total")
    withClue(s"coverage $total: "):
      total.inCheckpoint should be >= CoverageFloor
      total.inMove should be >= CoverageFloor
      total.frozen should be >= CoverageFloor
      total.midCheckpointImages should be >= CoverageFloor
      // A stale index lives only between a checkpoint's index force and its emptying of the old one
      total.staleIndexImages should be >= 1
      total.laterIndexImages should be >= CoverageFloor
      total.dataForceFaults should be >= CoverageFloor
      if interruptRecovery then
        total.inRecovery should be >= CoverageFloor
        total.inRecoveryCheckpoint should be >= CoverageFloor
        total.inReplay should be >= CoverageFloor

  test("random operations match the model across crashes"):
    runSuite(0 until 30, interruptRecovery = false)

  test("crash during recovery"):
    runSuite(1000 until 1030, interruptRecovery = true)

object FsifModelSuite:
  type CommitResult = Either[Unit, CommitError.Value]
  type ReadResult = Either[ReadState, ReadError.Value]

  final case class Sample(checkpoint: Boolean, move: Boolean, frozen: Boolean)

  final class Coverage:
    var crashes = 0
    var inCheckpoint = 0
    var inMove = 0
    var frozen = 0
    var inRecovery = 0
    var inRecoveryCheckpoint = 0
    /** Recoveries cut off inside the device-load replay (DeviceJournalRecovery) */
    var inReplay = 0
    var recoveryCompleted = 0
    var midCheckpointImages = 0
    var staleIndexImages = 0
    var laterIndexImages = 0
    /** Data forces that failed with ENOSPC (each dropping the writes since the last good one) */
    var dataForceFaults = 0
    def +(o: Coverage): Coverage =
      val c = new Coverage
      c.crashes = crashes + o.crashes
      c.inCheckpoint = inCheckpoint + o.inCheckpoint
      c.inMove = inMove + o.inMove
      c.frozen = frozen + o.frozen
      c.inRecovery = inRecovery + o.inRecovery
      c.recoveryCompleted = recoveryCompleted + o.recoveryCompleted
      c.inRecoveryCheckpoint = inRecoveryCheckpoint + o.inRecoveryCheckpoint
      c.inReplay = inReplay + o.inReplay
      c.midCheckpointImages = midCheckpointImages + o.midCheckpointImages
      c.staleIndexImages = staleIndexImages + o.staleIndexImages
      c.laterIndexImages = laterIndexImages + o.laterIndexImages
      c.dataForceFaults = dataForceFaults + o.dataForceFaults
      c
    override def toString: String =
      s"crashes=$crashes inCheckpoint=$inCheckpoint inMove=$inMove frozen=$frozen " +
        s"midCheckpointImages=$midCheckpointImages staleIndexImages=$staleIndexImages " +
        s"laterIndexImages=$laterIndexImages dataForceFaults=$dataForceFaults inRecovery=$inRecovery " +
        s"inRecoveryCheckpoint=$inRecoveryCheckpoint inReplay=$inReplay recoveryCompleted=$recoveryCompleted"

  final case class Result(seed: Int, digest: String, cov: Coverage)
