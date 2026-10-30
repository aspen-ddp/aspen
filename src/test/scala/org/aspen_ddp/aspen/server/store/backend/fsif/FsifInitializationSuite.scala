package org.aspen_ddp.aspen.server.store.backend.fsif

import java.util.UUID

import scala.collection.concurrent.TrieMap
import scala.concurrent.{Await, ExecutionContext, Promise}
import scala.concurrent.duration.*
import scala.util.{Failure, Random, Success, Try}

import org.aspen_ddp.aspen.FileBasedTests
import org.aspen_ddp.aspen.common.{DataBuffer, HLCTimestamp}
import org.aspen_ddp.aspen.common.ida.{ReedSolomon, Replication}
import org.aspen_ddp.aspen.common.objects.{DataObjectPointer, Metadata, ObjectId, ObjectPointer, ObjectRefcount,
  ObjectRevision, ObjectType, ReadError}
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.{ReadState, StoreId}
import org.aspen_ddp.aspen.common.transaction.TransactionId
import org.aspen_ddp.aspen.server.store.backend.{Commit, CommitError, CommitState, Completion, CompletionHandler,
  FsifConfig, Read, UpdateForm}

class FsifInitializationSuite extends FileBasedTests:

  private val layout = FsifLayout(1024, 64)
  private val storeId = StoreId(PoolId(new UUID(7, 7)), 0)
  private val io = ExecutionContext.global
  private val Timeout = 10.seconds
  private val Unmapped = (-1, ObjectId(new UUID(0, 0)))

  private def close(b: FsifBackend): Unit = Await.result(b.close(), Timeout)

  test("create writes the fsif-initialized marker"):
    val dir = tdir / "store"
    val b = FsifBackend.create(storeId, dir, layout, io, _ => ())
    try
      os.exists(dir / FsifBackend.InitializedMarkerFile) shouldBe true
    finally close(b)

  private def sp(x: Int): Array[Byte] = java.nio.ByteBuffer.allocate(4).putInt(x).array()
  private def oid(i: Int) = ObjectId(new UUID(i, ~i))
  private def ptr(x: Int): ObjectPointer = DataObjectPointer(oid(x), storeId.poolId, sp(x))
  private def md(rev: Int) =
    Metadata(ObjectRevision(TransactionId(new UUID(rev, 6))), ObjectRefcount(rev, 1), HLCTimestamp(rev))
  private def bytes(n: Int, v: Int): Array[Byte] = Array.tabulate[Byte](n)(j => (v + j * 7).toByte)

  /** Completes one promise per object id: each test has at most one commit or read per object outstanding */
  private final class Recorder extends CompletionHandler:
    private val commits = TrieMap[ObjectId, Promise[Either[Unit, CommitError.Value]]]()
    private val reads = TrieMap[ObjectId, Promise[Either[ReadState, ReadError.Value]]]()
    def complete(op: Completion): Unit = op match
      case c: Commit => commits.remove(c.objectId).foreach(_.success(c.result))
      case r: Read => reads.remove(r.objectId).foreach(_.success(r.result))
    def commit(b: FsifBackend, cs: CommitState): Unit =
      val p = Promise[Either[Unit, CommitError.Value]]()
      commits(cs.objectId) = p
      b.commit(cs, TransactionId(UUID.randomUUID()))
      Await.result(p.future, Timeout) shouldBe Left(())
    def read(b: FsifBackend, p: ObjectPointer): Option[List[Byte]] =
      val pr = Promise[Either[ReadState, ReadError.Value]]()
      reads(p.id) = pr
      b.read(p)
      Await.result(pr.future, Timeout) match
        case Left(rs) => Some(rs.data.getByteArray.toList)
        case Right(ReadError.ObjectNotFound) => None
        case Right(e) => fail(s"read of ${p.id} failed: $e")

  /** java.util.Random's first draws are correlated across consecutive seeds: mix the seed first */
  private def scrambled(seed: Int): Random =
    var z = seed * 0x9e3779b97f4a7c15L + 0x9e3779b97f4a7c15L
    z = (z ^ (z >>> 30)) * 0xbf58476d1ce4e5b9L
    z = (z ^ (z >>> 27)) * 0x94d049bb133111ebL
    new Random(z ^ (z >>> 31))

  private def isUnmarked(dir: os.Path): Boolean = !os.exists(dir / FsifBackend.InitializedMarkerFile)

  test("without its marker, a store holding objects is recovered, not re-initialized, and gains the marker"):
    val dir = tdir / "store"
    val rec = new Recorder
    val b0 = FsifBackend.create(storeId, dir, layout, io, _ => ())
    b0.setCompletionHandler(rec)
    val v1 = bytes(100, 1)
    val v2 = bytes(300, 2)
    rec.commit(b0, CommitState(ptr(1), md(1), ObjectType.Data, DataBuffer(v1), allocating = true))
    Await.result(b0.checkpointNow(), Timeout) // object 1 is in the index and data file, not only the journal
    rec.commit(b0, CommitState(ptr(2), md(1), ObjectType.Data, DataBuffer(v2), allocating = true))
    val v1b = v1.take(20) ++ bytes(30, 9) ++ v1.drop(50)
    rec.commit(b0, CommitState(ptr(1), md(2), ObjectType.Data, DataBuffer(v1b),
      form = UpdateForm.Ranges(v1.length, List((20, 30)))))
    close(b0)
    os.remove(dir / FsifBackend.InitializedMarkerFile)

    val b = FsifBackend.open(storeId, dir, layout, io, _ => ())
    try
      os.exists(dir / FsifBackend.InitializedMarkerFile) shouldBe true
      b.setCompletionHandler(rec)
      b.journalSeq shouldBe 1
      rec.read(b, ptr(1)) shouldBe Some(v1b.toList)
      rec.read(b, ptr(2)) shouldBe Some(v2.toList)
      rec.read(b, ptr(3)) shouldBe None
    finally close(b)

  test("without its marker, a directory whose FSIF files are all absent or empty is initialized"):
    val dir = tdir / "store"
    os.makeDir.all(dir)
    // As a crash early in initialize leaves it: data and journal.a created but empty, no index files
    os.write(dir / FsifRecovery.DataFile, Array[Byte]())
    os.write(dir / FsifRecovery.JournalFiles(0), Array[Byte]())
    FsifRecovery.isBlank(new NioFsifFiles(dir)) shouldBe true
    val b = FsifBackend.open(storeId, dir, layout, io, _ => ())
    try
      os.exists(dir / FsifBackend.InitializedMarkerFile) shouldBe true
      b.slotCount shouldBe 0
      Await.result(b.mapping(0), Timeout) shouldBe Unmapped
      IndexFile.decode(os.read.bytes(dir / FsifRecovery.IndexFiles(0)), layout.segmentCount).map(_._1) shouldBe
        Some(0L)
    finally close(b)

  test("without its marker, a new directory is initialized"):
    val dir = tdir / "fresh"
    val b = FsifBackend.open(storeId, dir, layout, io, _ => ())
    try
      os.exists(dir / FsifBackend.InitializedMarkerFile) shouldBe true
      b.slotCount shouldBe 0
    finally close(b)

  test("without its marker, a directory with non-empty files and no valid index is lost and left untouched"):
    val dir = tdir / "store"
    os.makeDir.all(dir)
    os.write(dir / FsifRecovery.DataFile, Array.fill[Byte](100)(5))
    os.write(dir / FsifRecovery.JournalFiles(0), Array.fill[Byte](10)(9))
    an[FsifRecovery.StoreLost] should be thrownBy FsifBackend.open(storeId, dir, layout, io, _ => ())
    isUnmarked(dir) shouldBe true
    os.read.bytes(dir / FsifRecovery.DataFile).toList shouldBe List.fill[Byte](100)(5)
    os.read.bytes(dir / FsifRecovery.JournalFiles(0)).toList shouldBe List.fill[Byte](10)(9)

  test("a fully initialized store without its marker is recovered as the empty store, not re-initialized"):
    val dir = tdir / "store"
    os.makeDir.all(dir)
    FsifRecovery.initialize(new NioFsifFiles(dir), layout)
    // initialize gives journal.a a fresh random salt: an unchanged header shows it was not run again
    val header = os.read.bytes(dir / FsifRecovery.JournalFiles(0)).toList
    val b = FsifBackend.open(storeId, dir, layout, io, _ => ())
    try
      os.exists(dir / FsifBackend.InitializedMarkerFile) shouldBe true
      b.slotCount shouldBe 0
      os.read.bytes(dir / FsifRecovery.JournalFiles(0)).toList shouldBe header
    finally close(b)

  /** Fails op `failAt` (counting writes, forces and truncates) with ENOSPC if it is a write or force; every other op
    * succeeds, so the failure is live, not a crash
    */
  private final class FailOneOp(under: FsifFiles, failAt: Int) extends FsifFiles:
    private var n = 0
    private def op[A](spaceNeeded: Boolean)(body: => A): A =
      n += 1
      if spaceNeeded && n == failAt + 1 then throw new java.io.IOException("No space left on device")
      body
    def exists(name: String): Boolean = under.exists(name)
    def size(name: String): Long = under.size(name)
    def open(name: String): FsifFile =
      val f = under.open(name)
      new FsifFile:
        def size: Long = f.size
        def read(position: Long, length: Int): Array[Byte] = f.read(position, length)
        def write(position: Long, bytes: Array[Byte]): Unit = op(true)(f.write(position, bytes))
        def truncate(size: Long): Unit = op(false)(f.truncate(size))
        def force(): Unit = op(true)(f.force())
        def close(): Unit = f.close()

  test("an initialize that fails with ENOSPC leaves the directory blank, so the next open initializes it"):
    val probe = new SimulatedFsifFiles(new Random(0))
    FsifRecovery.initialize(probe, layout)
    val ops = probe.opCount.toInt
    var failed = 0
    for failAt <- 0 until ops do withClue(s"failAt=$failAt"):
      val files = new SimulatedFsifFiles(new Random(failAt))
      Try(FsifRecovery.initialize(new FailOneOp(files, failAt), layout)) match
        case Failure(_: java.io.IOException) =>
          failed += 1
          FsifRecovery.isBlank(files) shouldBe true
        case other => other shouldBe Success(()) // a truncate at failAt cannot fail
    failed should be > 0

  test("a crash at any op of initialize on a fresh directory opens as the empty store or StoreLost, nothing else"):
    val probe = new SimulatedFsifFiles(new Random(0))
    FsifRecovery.initialize(probe, layout)
    val ops = probe.opCount.toInt
    var empty = 0
    var lost = 0
    val lostAt = scala.collection.mutable.SortedMap[Int, Int]()
    for crashAt <- 0 to ops; seed <- 0 until 32 do withClue(s"crashAt=$crashAt seed=$seed"):
      val files = new SimulatedFsifFiles(scrambled(crashAt * 1000 + seed))
      files.crashAfterOps(crashAt)
      Try(FsifRecovery.initialize(files, layout))
      val after = files.crash()
      var marked = false
      Try(FsifBackend.openUnmarked(storeId, tdir / "sim", layout, after, FsifBackendSuite.InlineEc, FsifTuning(),
          _ => (), () => marked = true)) match
        case Success(b) =>
          marked shouldBe true
          b.slotCount shouldBe 0
          Await.result(b.mapping(0), Timeout) shouldBe Unmapped
          Await.result(b.mapping(63), Timeout) shouldBe Unmapped
          b.journalSeq shouldBe 0
          close(b)
          empty += 1
        case Failure(_: FsifRecovery.StoreLost) =>
          marked shouldBe false
          lost += 1
          lostAt(crashAt) = lostAt.getOrElse(crashAt, 0) + 1
        case Failure(t) => fail(s"unexpected failure: $t")
    info(s"initialize has $ops ops; of ${(ops + 1) * 32} crashes, $empty opened empty and $lost were lost " +
      s"(crash point -> lost: ${lostAt.mkString(", ")})")
    empty should be > 0

  test("a marked store with no valid index is lost, not re-initialized"):
    val dir = tdir / "store"
    close(FsifBackend.create(storeId, dir, layout, io, _ => ()))
    os.remove(dir / FsifRecovery.IndexFiles(0))
    an[FsifRecovery.StoreLost] should be thrownBy FsifBackend.open(storeId, dir, layout, io, _ => ())
    os.exists(dir / FsifRecovery.IndexFiles(0)) shouldBe false

  test("a marked store reopens through open"):
    val dir = tdir / "store"
    close(FsifBackend.create(storeId, dir, layout, io, _ => ()))
    val b = FsifBackend.open(storeId, dir, layout, io, _ => ())
    close(b)

  test("FsifConfig.forObjectSize sizes segments with ida.minSliceSizeFor"):
    val r = Replication(3, 2)
    r.minSliceSizeFor(10 << 20) shouldBe (10L << 20)
    FsifConfig.forObjectSize(r, 10 << 20, 10) shouldBe FsifConfig(10 << 20, 10)
    FsifConfig.forObjectSize(r, 10 << 20, 10).maxSliceSize shouldBe Some(10 << 20)

    val rs = ReedSolomon(5, 3, 4)
    val cfg = FsifConfig.forObjectSize(rs, 1 << 20, 1000)
    cfg.segmentPayloadSize.toLong shouldBe rs.minSliceSizeFor(1 << 20)
    cfg.segmentCount shouldBe 1000
    rs.maxObjectSize(cfg.segmentPayloadSize) should be >= (1 << 20)

  test("segmentCount is bounded so the index file stays below 2 GiB"):
    val max = FsifLayout.MaxSegmentCount
    FsifRecovery.MaxLoadSize shouldBe (Int.MaxValue - 16).toLong
    IndexFile.sizeFor(max) should be <= FsifRecovery.MaxLoadSize
    IndexFile.sizeFor(max + 1) should be > FsifRecovery.MaxLoadSize
    FsifLayout(1024, max).segmentCount shouldBe max
    FsifConfig(1024, max).segmentCount shouldBe max
    an[IllegalArgumentException] should be thrownBy FsifLayout(1024, max + 1)
    an[IllegalArgumentException] should be thrownBy FsifLayout(1024, 0)
    an[IllegalArgumentException] should be thrownBy FsifConfig(1024, max + 1)
    an[IllegalArgumentException] should be thrownBy FsifConfig(1024, Int.MaxValue)

  test("journalHardLimit plus the largest append stays below what recovery can load, for every layout"):
    val t = FsifTuning()
    t.journalHardLimit shouldBe (1L << 30)
    // appendBound is the encoded size of the largest batch one commit appends: a Mapping and a one-range Delta
    for n <- Seq(0, 1, 1000, 1 << 20) do
      val d = JournalRecord.Delta(0, 0, SegmentHeader.forSlice(ObjectType.Data, oid(1), 0, md(1), DataBuffer(bytes(n, 1))),
        List(DeltaRange(0, bytes(n, 1))))
      val batch = JournalFormat.encodeBatch(1L, 0L, List(JournalRecord.Mapping(0, 0, oid(1)), d))
      FsifBackend.appendBound(n) shouldBe batch.length.toLong
    FsifBackend.MappingAppendSize shouldBe
      JournalFormat.encodeBatch(1L, 0L, List(JournalRecord.Mapping(0, -1, oid(1)))).length.toLong
    // The default limit plus a batch of a 512 MiB slice (and a compaction Mapping) is far below the load limit
    t.journalHardLimit + FsifBackend.appendBound(512 << 20) + FsifBackend.MappingAppendSize should be <
      FsifRecovery.MaxLoadSize
    for payload <- Seq(1024, 64 << 20, 1 << 30, FsifLayout.MaxSegmentPayloadSize) do withClue(s"payload=$payload"):
      FsifBackend.journalLengthBound(FsifLayout(payload, 1), t) should be <= FsifRecovery.MaxLoadSize
    FsifBackend.journalLengthBound(FsifLayout(1024, 1), t) shouldBe t.journalHardLimit
    an[IllegalArgumentException] should be thrownBy FsifLayout(FsifLayout.MaxSegmentPayloadSize + 1, 1)
    an[IllegalArgumentException] should be thrownBy FsifTuning(journalHardLimit = FsifRecovery.MaxLoadSize + 1)
    an[IllegalArgumentException] should be thrownBy FsifTuning(journalHardLimit = 0)

  test("estimateSize sizes the files it does not hold without opening them"):
    val dir = tdir / "store"
    close(FsifBackend.create(storeId, dir, layout, io, _ => ()))
    val opens = new java.util.concurrent.atomic.AtomicInteger(0)
    val nio = new NioFsifFiles(dir)
    val counting = new FsifFiles:
      def open(name: String): FsifFile =
        opens.incrementAndGet()
        nio.open(name)
      def exists(name: String): Boolean = nio.exists(name)
      def size(name: String): Long = nio.size(name)
    val b = new FsifBackend(storeId, dir, layout, counting, io)
    try
      val before = opens.get
      val expected = Seq(FsifRecovery.DataFile) ++ FsifRecovery.IndexFiles ++ FsifRecovery.JournalFiles
      b.estimateSize() shouldBe expected.map(n => nio.size(n)).sum
      opens.get shouldBe before
    finally close(b)
