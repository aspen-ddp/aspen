package org.aspen_ddp.aspen.server.store.backend.fsif

import java.util.UUID

import scala.collection.concurrent.TrieMap
import scala.concurrent.{Await, ExecutionContext, Promise}
import scala.concurrent.duration.*

import org.aspen_ddp.aspen.FileBasedTests
import org.aspen_ddp.aspen.common.{DataBuffer, HLCTimestamp}
import org.aspen_ddp.aspen.common.ida.{ReedSolomon, Replication}
import org.aspen_ddp.aspen.common.objects.{DataObjectPointer, Metadata, ObjectId, ObjectPointer, ObjectRefcount,
  ObjectRevision, ObjectType, ReadError}
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.{ReadState, StoreId}
import org.aspen_ddp.aspen.common.transaction.TransactionId
import org.aspen_ddp.aspen.server.StoreConfig
import org.aspen_ddp.aspen.server.store.backend.{Commit, CommitError, CommitState, Completion, CompletionHandler,
  DefaultBackendOpener, FsifConfig, Read, StoreCreation, UpdateForm}

class FsifInitializationSuite extends FileBasedTests:

  private val layout = FsifLayout(1024, 64)
  private val storeId = StoreId(PoolId(new UUID(7, 7)), 0)
  private val io = ExecutionContext.global
  private val Timeout = 10.seconds
  private val Unmapped = (-1, ObjectId(new UUID(0, 0)))

  private def close(b: FsifBackend): Unit = Await.result(b.close(), Timeout)

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

  test("a store holding objects is recovered on open"):
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

    val b = FsifBackend.open(storeId, dir, layout, io, _ => ())
    try
      b.setCompletionHandler(rec)
      b.journalSeq shouldBe 1
      rec.read(b, ptr(1)) shouldBe Some(v1b.toList)
      rec.read(b, ptr(2)) shouldBe Some(v2.toList)
      rec.read(b, ptr(3)) shouldBe None
    finally close(b)

  private val fsifCfg = StoreConfig(storeId, StoreConfig.Fsif(layout.segmentPayloadSize, layout.segmentCount), None,
    Replication(3, 2))

  test("a store created atomically opens as the empty store"):
    val dir = tdir / "store"
    DefaultBackendOpener.createStore(fsifCfg, dir, tdir / StoreCreation.CreatingDirectory)
    os.exists(dir / StoreConfig.configFilename) shouldBe true
    val b = FsifBackend.open(storeId, dir, layout, io, _ => ())
    try
      b.slotCount shouldBe 0
      Await.result(b.mapping(0), Timeout) shouldBe Unmapped
      b.journalSeq shouldBe 0
    finally close(b)

  test("a missing directory is lost, and open does not create it"):
    val dir = tdir / "missing"
    an[FsifRecovery.StoreLost] should be thrownBy FsifBackend.open(storeId, dir, layout, io, _ => ())
    os.exists(dir) shouldBe false

  test("a directory whose FSIF files are all absent or empty is lost, and nothing is written (SL X14)"):
    val dir = tdir / "store"
    os.makeDir.all(dir)
    // A store whose FSIF files were lost or emptied (an incomplete restore): its config is all that is left
    os.write(dir / StoreConfig.configFilename, fsifCfg.yamlConfig)
    os.write(dir / FsifRecovery.DataFile, Array[Byte]())
    FsifRecovery.isBlank(new NioFsifFiles(dir)) shouldBe true
    an[FsifRecovery.StoreLost] should be thrownBy FsifBackend.open(storeId, dir, layout, io, _ => ())
    os.list(dir).map(_.last).toSet shouldBe Set(StoreConfig.configFilename, FsifRecovery.DataFile)
    os.size(dir / FsifRecovery.DataFile) shouldBe 0

  test("a directory with non-empty files and no valid index is lost and left untouched"):
    val dir = tdir / "store"
    os.makeDir.all(dir)
    os.write(dir / FsifRecovery.DataFile, Array.fill[Byte](100)(5))
    os.write(dir / FsifRecovery.JournalFiles(0), Array.fill[Byte](10)(9))
    an[FsifRecovery.StoreLost] should be thrownBy FsifBackend.open(storeId, dir, layout, io, _ => ())
    os.read.bytes(dir / FsifRecovery.DataFile).toList shouldBe List.fill[Byte](100)(5)
    os.read.bytes(dir / FsifRecovery.JournalFiles(0)).toList shouldBe List.fill[Byte](10)(9)

  test("an initialized store is recovered as the empty store, not re-initialized"):
    val dir = tdir / "store"
    os.makeDir.all(dir)
    FsifRecovery.initialize(new NioFsifFiles(dir), layout)
    // initialize gives journal.a a fresh random salt: an unchanged header shows it was not run again
    val header = os.read.bytes(dir / FsifRecovery.JournalFiles(0)).toList
    val b = FsifBackend.open(storeId, dir, layout, io, _ => ())
    try
      b.slotCount shouldBe 0
      os.read.bytes(dir / FsifRecovery.JournalFiles(0)).toList shouldBe header
    finally close(b)

  test("a store with no valid index is lost, not re-initialized"):
    val dir = tdir / "store"
    close(FsifBackend.create(storeId, dir, layout, io, _ => ()))
    os.remove(dir / FsifRecovery.IndexFiles(0))
    an[FsifRecovery.StoreLost] should be thrownBy FsifBackend.open(storeId, dir, layout, io, _ => ())
    os.exists(dir / FsifRecovery.IndexFiles(0)) shouldBe false

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
