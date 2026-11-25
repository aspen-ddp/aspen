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

  /** Each test's device journal, over real files beside its stores */
  private var journal: FsifDeviceJournal = scala.compiletime.uninitialized

  override def preTest(): Unit =
    journal = SharedJournal.nio(tdir / "fj", JournalSizing(UUID.randomUUID(), 3, 64 * 1024))

  override def preTempDirDeletion(): Unit = journal.close()

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
    val b0 = FsifBackend.create(storeId, dir, layout, io, journal, _ => ())
    b0.setCompletionHandler(rec)
    val v1 = bytes(100, 1)
    val v2 = bytes(300, 2)
    rec.commit(b0, CommitState(ptr(1), md(1), ObjectType.Data, DataBuffer(v1), allocating = true))
    Await.result(b0.checkpointNow(), Timeout) // object 1 is in the index and data file, not only the journal
    rec.commit(b0, CommitState(ptr(2), md(1), ObjectType.Data, DataBuffer(v2), allocating = true))
    val v1b = v1.take(20) ++ bytes(30, 9) ++ v1.drop(50)
    rec.commit(b0, CommitState(ptr(1), md(2), ObjectType.Data, DataBuffer(v1b),
      form = UpdateForm.Ranges(v1.length, List((20, 30)))))
    close(b0) // detaches: the reopen needs no replay
    b0.detachedForTest shouldBe true

    val b = FsifBackend.open(storeId, dir, layout, io, journal, _ => ())
    try
      b.setCompletionHandler(rec)
      b.indexSeq shouldBe 4 // initialize 0, attach 1, checkpoint 2, detach 3, attach 4
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
    val b = FsifBackend.open(storeId, dir, layout, io, journal, _ => ())
    try
      b.slotCount shouldBe 0
      Await.result(b.mapping(0), Timeout) shouldBe Unmapped
      b.indexSeq shouldBe 1 // the attach index over the initial one
    finally close(b)

  test("a missing directory is lost, and open does not create it"):
    val dir = tdir / "missing"
    an[FsifRecovery.StoreLost] should be thrownBy FsifBackend.open(storeId, dir, layout, io, journal, _ => ())
    os.exists(dir) shouldBe false

  test("a directory whose FSIF files are all absent or empty is lost, and nothing is written (SL D5)"):
    val dir = tdir / "store"
    os.makeDir.all(dir)
    // A store whose FSIF files were lost or emptied (an incomplete restore): its config is all that is left
    os.write(dir / StoreConfig.configFilename, fsifCfg.yamlConfig)
    os.write(dir / FsifRecovery.DataFile, Array[Byte]())
    FsifRecovery.isBlank(new NioFsifFiles(dir)) shouldBe true
    an[FsifRecovery.StoreLost] should be thrownBy FsifBackend.open(storeId, dir, layout, io, journal, _ => ())
    os.list(dir).map(_.last).toSet shouldBe Set(StoreConfig.configFilename, FsifRecovery.DataFile)
    os.size(dir / FsifRecovery.DataFile) shouldBe 0

  /** `link` made a link to `target` in `locked`, a directory then left without search permission, so every stat of
   *  `link` fails (EACCES) while the target is intact; `f` runs, and the permission is restored */
  private def withUnstatable(locked: os.Path)(f: => Unit): Unit =
    os.perms.set(locked, "rw-------")
    try f
    finally os.perms.set(locked, "rwx------")

  test("a store directory whose stat fails is an I/O error at open, never StoreLost (SL-W23)"):
    val locked = tdir / "locked"
    os.makeDir.all(locked)
    DefaultBackendOpener.createStore(fsifCfg, locked / "store", tdir / StoreCreation.CreatingDirectory)
    os.symlink(tdir / "store", locked / "store")
    withUnstatable(locked):
      val t = intercept[Throwable](FsifBackend.open(storeId, tdir / "store", layout, io, journal, _ => ()))
      t shouldBe a[java.io.IOException]
      t should not be a[FsifRecovery.StoreLost]
    close(FsifBackend.open(storeId, tdir / "store", layout, io, journal, _ => ()))

  test("an FSIF file whose stat fails is neither absent nor empty to recovery (SL-W22)"):
    val dir = tdir / "store"
    val locked = tdir / "locked"
    os.makeDir.all(dir)
    os.makeDir.all(locked)
    // index s+1, present after a checkpoint
    os.write(locked / "index", Array.fill[Byte](100)(3))
    os.symlink(dir / FsifRecovery.IndexFiles(1), locked / "index")
    val files = new NioFsifFiles(dir)
    withUnstatable(locked):
      an[java.io.IOException] should be thrownBy files.exists(FsifRecovery.IndexFiles(1))
      an[java.io.IOException] should be thrownBy files.size(FsifRecovery.IndexFiles(1))
      an[java.io.IOException] should be thrownBy FsifRecovery.isBlank(files)
    files.exists(FsifRecovery.IndexFiles(1)) shouldBe true
    files.size(FsifRecovery.IndexFiles(1)) shouldBe 100
    files.exists(FsifRecovery.IndexFiles(0)) shouldBe false
    files.size(FsifRecovery.IndexFiles(0)) shouldBe 0

  test("a directory with non-empty files and no valid index is lost and left untouched"):
    val dir = tdir / "store"
    os.makeDir.all(dir)
    os.write(dir / FsifRecovery.DataFile, Array.fill[Byte](100)(5))
    os.write(dir / FsifRecovery.IndexFiles(0), Array.fill[Byte](10)(9))
    an[FsifRecovery.StoreLost] should be thrownBy FsifBackend.open(storeId, dir, layout, io, journal, _ => ())
    os.read.bytes(dir / FsifRecovery.DataFile).toList shouldBe List.fill[Byte](100)(5)
    os.read.bytes(dir / FsifRecovery.IndexFiles(0)).toList shouldBe List.fill[Byte](10)(9)

  test("an initialized store is recovered as the empty store, not re-initialized"):
    val dir = tdir / "store"
    os.makeDir.all(dir)
    FsifRecovery.initialize(new NioFsifFiles(dir), layout)
    // initialize would empty data; an open writes only its index files (the attach index, then index s emptied)
    val nio = new NioFsifFiles(dir)
    val mutated = java.util.concurrent.ConcurrentHashMap.newKeySet[String]()
    val recording = new FsifFiles:
      def open(name: String): FsifFile =
        val f = nio.open(name)
        new FsifFile:
          def size: Long = f.size
          def read(position: Long, length: Int): Array[Byte] = f.read(position, length)
          def write(position: Long, bytes: Array[Byte]): Unit =
            mutated.add(name)
            f.write(position, bytes)
          def truncate(size: Long): Unit =
            mutated.add(name)
            f.truncate(size)
          def force(): Unit = f.force()
          def close(): Unit = f.close()
      def exists(name: String): Boolean = nio.exists(name)
      def size(name: String): Long = nio.size(name)
    val b = new FsifBackend(storeId, dir, layout, recording, io, journal)
    try
      b.slotCount shouldBe 0
      Await.result(b.quiesce(), Timeout)
      mutated.toArray.toSet shouldBe FsifRecovery.IndexFiles.toSet
    finally close(b)

  test("a store with no valid index is lost, not re-initialized"):
    val dir = tdir / "store"
    close(FsifBackend.create(storeId, dir, layout, io, journal, _ => ()))
    os.remove(dir / FsifRecovery.IndexFiles(0))
    an[FsifRecovery.StoreLost] should be thrownBy FsifBackend.open(storeId, dir, layout, io, journal, _ => ())
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

  test("appendBound bounds one commit's device journal bytes, and a layout fits a journal only with room for it"):
    // appendBound is the tagged size of the largest append one commit makes: a Mapping and a one-range Delta
    for n <- Seq(0, 1, 1000, 1 << 20) do
      val d = JournalRecord.Delta(0, 0, SegmentHeader.forSlice(ObjectType.Data, oid(1), 0, md(1), DataBuffer(bytes(n, 1))),
        List(DeltaRange(0, bytes(n, 1))))
      val records = List(JournalRecord.Mapping(0, 0, oid(1)), d)
      FsifBackend.appendBound(n) shouldBe records.map(r => DeviceJournalFormat.taggedRecordSize(r).toLong).sum
    FsifBackend.MappingAppendSize shouldBe
      DeviceJournalFormat.taggedRecordSize(JournalRecord.Mapping(0, -1, oid(1))).toLong
    // A layout fits when its largest commit plus a compaction Mapping fits one file less its header
    val sizing = JournalSizing(new UUID(1, 1), 3, 64 * 1024)
    val room = sizing.fileSize - DeviceJournalFormat.HeaderSize
    val largest = (room - FsifBackend.appendBound(0) - FsifBackend.MappingAppendSize).toInt
    FsifBackend.fitsJournal(FsifLayout(largest, 1), sizing) shouldBe true
    FsifBackend.fitsJournal(FsifLayout(largest + 1, 1), sizing) shouldBe false
    // The payload limit keeps every sum an Int
    FsifBackend.appendBound(FsifLayout.MaxSegmentPayloadSize) + FsifBackend.MappingAppendSize + 16 shouldBe
      Int.MaxValue.toLong
    an[IllegalArgumentException] should be thrownBy FsifLayout(FsifLayout.MaxSegmentPayloadSize + 1, 1)

  test("an open whose layout does not fit the journal's file size is an IOException naming both, never StoreLost"):
    val dir = tdir / "store"
    val big = FsifLayout(64 * 1024, 4)
    FsifBackend.initializeStore(dir, big)
    val e = the[java.io.IOException] thrownBy FsifBackend.open(storeId, dir, big, io, journal, _ => ())
    e should not be a[FsifRecovery.StoreLost]
    e.getMessage should include((64 * 1024 - DeviceJournalFormat.HeaderSize).toString)
    e.getMessage should include(FsifBackend.appendBound(64 * 1024).+(FsifBackend.MappingAppendSize).toString)

  test("estimateSize matches the files after writes and compaction"):
    val dir = tdir / "store"
    close(FsifBackend.create(storeId, dir, layout, io, journal, _ => ()))
    val stats = new java.util.concurrent.atomic.AtomicInteger(0)
    val nio = new NioFsifFiles(dir)
    // Counts every size query, of a held handle or by name: estimateSize must answer from memory
    val counting = new FsifFiles:
      def open(name: String): FsifFile =
        val f = nio.open(name)
        new FsifFile:
          def size: Long =
            stats.incrementAndGet()
            f.size
          def read(position: Long, length: Int): Array[Byte] = f.read(position, length)
          def write(position: Long, bytes: Array[Byte]): Unit = f.write(position, bytes)
          def truncate(size: Long): Unit = f.truncate(size)
          def force(): Unit = f.force()
          def close(): Unit = f.close()
      def exists(name: String): Boolean = nio.exists(name)
      def size(name: String): Long =
        stats.incrementAndGet()
        nio.size(name)
    val rec = new Recorder
    val b = new FsifBackend(storeId, dir, layout, counting, io, journal)
    try
      b.setCompletionHandler(rec)
      def onDisk: Long =
        FsifRecovery.AllFiles
          .map(n => if os.exists(dir / n) then os.size(dir / n) else 0L).sum
      def check(): Unit =
        Await.result(b.quiesce(), Timeout)
        val before = stats.get
        b.estimateSize() shouldBe onDisk
        stats.get shouldBe before
      check()
      for i <- 1 to 10 do
        rec.commit(b, CommitState(ptr(i), md(1), ObjectType.Data, DataBuffer(bytes(300, i)), allocating = true))
      check()
      Await.result(b.checkpointNow(), Timeout) // rewrites an index and empties the other
      check()
      // Deleting the tail objects lets compaction truncate the data file
      for i <- 6 to 10 do
        rec.commit(b, CommitState(ptr(i), Metadata(md(2).revision, ObjectRefcount(2, 0), md(2).timestamp), ObjectType.Data,
          DataBuffer()))
      check()
      Await.result(b.checkpointNow(), Timeout)
      check()
    finally close(b)

  test("estimateSize sizes the files it does not hold without opening them"):
    val dir = tdir / "store"
    close(FsifBackend.create(storeId, dir, layout, io, journal, _ => ()))
    val opens = new java.util.concurrent.atomic.AtomicInteger(0)
    val nio = new NioFsifFiles(dir)
    val counting = new FsifFiles:
      def open(name: String): FsifFile =
        opens.incrementAndGet()
        nio.open(name)
      def exists(name: String): Boolean = nio.exists(name)
      def size(name: String): Long = nio.size(name)
    val b = new FsifBackend(storeId, dir, layout, counting, io, journal)
    try
      val before = opens.get
      val expected = FsifRecovery.AllFiles
      b.estimateSize() shouldBe expected.map(n => nio.size(n)).sum
      opens.get shouldBe before
    finally close(b)
