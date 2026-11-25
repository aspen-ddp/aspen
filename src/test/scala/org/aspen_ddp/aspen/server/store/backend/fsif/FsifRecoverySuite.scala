package org.aspen_ddp.aspen.server.store.backend.fsif

import java.io.IOException
import java.util.UUID

import scala.collection.mutable
import scala.util.Random

import org.aspen_ddp.aspen.FileBasedTests
import org.aspen_ddp.aspen.common.DataBuffer
import org.aspen_ddp.aspen.common.HLCTimestamp
import org.aspen_ddp.aspen.common.objects.{Metadata, ObjectId, ObjectRefcount, ObjectRevision, ObjectType}
import org.aspen_ddp.aspen.common.transaction.TransactionId

class FsifRecoverySuite extends FileBasedTests:
  import FsifRecovery.{Opened, Recovered, StoreLost}
  import JournalRecord.{Delta, Mapping}

  private val layout = FsifLayout(1024, 64)
  private val Zero = ObjectId(new UUID(0, 0))
  private def oid(i: Int) = ObjectId(new UUID(i, ~i))
  private def md(rev: Int) =
    Metadata(ObjectRevision(TransactionId(new UUID(rev, 6))), ObjectRefcount(1, 1), HLCTimestamp(rev))

  // java.util.Random seeded with consecutive small ints yields correlated first draws; scramble them
  private def rng(seed: Int) = new Random(splitMix(seed))

  private def splitMix(seed: Int): Long =
    var z = seed * 0x9e3779b97f4a7c15L + 0x9e3779b97f4a7c15L
    z = (z ^ (z >>> 30)) * 0xbf58476d1ce4e5b9L
    z = (z ^ (z >>> 27)) * 0x94d049bb133111ebL
    z ^ (z >>> 31)

  /** Test-side model of one object's segment */
  private final case class Seg(slot: Int, id: ObjectId, header: SegmentHeader, slice: Array[Byte])

  private def header(ext: Int, id: ObjectId, slice: Array[Byte], rev: Int): SegmentHeader =
    SegmentHeader.forSlice(ObjectType.Data, id, ext, md(rev), DataBuffer(slice))

  /** Map + full Delta, as a journaled allocation writes them */
  private def alloc(ext: Int, slot: Int, id: ObjectId, slice: Array[Byte], rev: Int): (List[JournalRecord], Seg) =
    val h = header(ext, id, slice, rev)
    (List(Mapping(ext, slot, id), Delta(ext, slot, h, List(DeltaRange(0, slice)))), Seg(slot, id, h, slice))

  /** In-place update of [off, off + bytes.length) with absolute bytes */
  private def update(ext: Int, s: Seg, off: Int, bytes: Array[Byte], rev: Int): (JournalRecord, Seg) =
    val slice = s.slice.clone()
    System.arraycopy(bytes, 0, slice, off, bytes.length)
    val h = header(ext, s.id, slice, rev)
    (Delta(ext, s.slot, h, List(DeltaRange(off, bytes))), s.copy(header = h, slice = slice))

  private def bytes(n: Int, v: Int): Array[Byte] = Array.tabulate[Byte](n)(j => (v + j * 7).toByte)

  private def saltOf(files: FsifFiles, name: String): Long =
    val f = files.open(name)
    JournalFormat.scan(f.read(0, f.size.toInt)) match
      case JournalFormat.Scan.Valid(_, salt, _, _, _) => salt
      case other => fail(s"$name does not scan as valid: $other")

  /** Appends forced batches to a journal; returns the next batch number */
  private def appendBatches(files: FsifFiles, name: String, first: Long, batches: List[List[JournalRecord]]): Long =
    val salt = saltOf(files, name)
    val f = files.open(name)
    var n = first
    batches.foreach: b =>
      f.write(f.size, JournalFormat.encodeBatch(salt, n, b))
      n += 1
    f.force()
    n

  private def writeIndex(files: FsifFiles, name: String, seq: Long, map: IndexMap): Unit =
    val f = files.open(name)
    f.truncate(0)
    f.write(0, IndexFile.encode(seq, map))
    f.force()

  private def writeIndex(files: FsifFiles, name: String, seq: Long, meta: IndexMeta, map: IndexMap): Unit =
    val f = files.open(name)
    f.truncate(0)
    f.write(0, IndexFile.encode(seq, meta, map))
    f.force()

  private def readIndexMeta(files: FsifFiles, name: String): Option[(Long, IndexMeta, IndexMap)] =
    val f = files.open(name)
    IndexFile.decode(f.read(0, f.size.toInt), layout.segmentCount)

  private def readIndex(files: FsifFiles, name: String): Option[(Long, IndexMap)] =
    readIndexMeta(files, name).map((seq, _, map) => (seq, map))

  private def mapOf(segs: Map[Int, Seg]): IndexMap =
    val m = IndexMap.empty(layout.segmentCount)
    segs.foreach: (ext, s) =>
      m.slots(ext) = s.slot
      m.ids(ext) = s.id
    m

  private def checkObjects(files: FsifFiles, r: Recovered, expected: Map[Int, Seg]): Unit =
    checkObjects(files, r.index, expected)

  private def checkObjects(files: FsifFiles, index: IndexMap, expected: Map[Int, Seg]): Unit =
    for ext <- 0 until layout.segmentCount do
      expected.get(ext) match
        case Some(s) =>
          withClue(s"ext $ext: ") {
            index.slots(ext) shouldBe s.slot
            index.ids(ext) shouldBe s.id
            val (h, slice) = FsifRecovery.readSlot(files, layout, s.slot).getOrElse(fail(s"slot ${s.slot} unreadable"))
            h shouldBe s.header
            slice.toList shouldBe s.slice.toList
          }
        case None =>
          withClue(s"ext $ext: ") { index.slots(ext) shouldBe -1 }

  private def fresh(seed: Int = 1): SimulatedFsifFiles =
    val files = new SimulatedFsifFiles(rng(seed))
    FsifRecovery.initialize(files, layout)
    files

  /** Three objects allocated in journal.a (seq 0) */
  private def threeObjects(files: FsifFiles): Map[Int, Seg] =
    val (r1, s1) = alloc(3, 0, oid(3), bytes(100, 1), 1)
    val (r2, s2) = alloc(7, 1, oid(7), bytes(1024, 2), 2)
    val (r3, s3) = alloc(9, 2, oid(9), bytes(0, 3), 3)
    val (u1, s1b) = update(3, s1, 10, bytes(20, 50), 4)
    appendBatches(files, "journal.a", 0, List(r1 ++ r2, r3, List(u1)))
    Map(3 -> s1b, 7 -> s2, 9 -> s3)

  // `recover` over per-store journals: the shim FsifBackend opens with until the device journal replaces it

  test("initialize then recover gives an empty map with seq 0"):
    val files = fresh()
    val r = FsifRecovery.recover(files, layout)
    r.seq shouldBe 0
    r.activeIndex shouldBe "index.a"
    r.activeJournal shouldBe "journal.a"
    r.journalLength shouldBe JournalFormat.HeaderSize
    r.nextBatch shouldBe 0
    r.slotCount shouldBe 0
    r.index.slots.forall(_ == -1) shouldBe true
    r.salt shouldBe saltOf(files, "journal.a")

  test("recover works with index.b and journal.b absent or empty"):
    val files = fresh()
    files.exists("index.b") shouldBe false
    files.exists("journal.b") shouldBe false
    FsifRecovery.recover(files, layout).seq shouldBe 0
    files.open("index.b")
    files.open("journal.b")
    FsifRecovery.recover(files, layout).seq shouldBe 0

  test("journal records for 3 objects recover byte-exactly"):
    val files = fresh()
    val expected = threeObjects(files)
    val r = FsifRecovery.recover(files, layout)
    r.seq shouldBe 0
    r.nextBatch shouldBe 3
    r.slotCount shouldBe 3
    checkObjects(files, r, expected)
    // And again from the crashed image: recovery forced everything it relied on
    val again = files.crash()
    checkObjects(again, FsifRecovery.recover(again, layout), expected)

  test("index s valid, index s+1 torn, journals s and s+1 recover the union and finish the checkpoint"):
    val files = fresh()
    val first = threeObjects(files)
    // Interrupted checkpoint 0 -> 1: journal.b got seq 1 and records, index.b was torn mid-write
    FsifRecovery.writeJournalHeader(files.open("journal.b"), 1)
    val (r4, s4) = alloc(11, 3, oid(11), bytes(300, 4), 5)
    val (u7, s7) = update(7, first(7), 1000, bytes(24, 60), 6)
    appendBatches(files, "journal.b", 0, List(r4, List(u7, Mapping(9, -1, Zero))))
    val full = IndexFile.encode(1, mapOf(first))
    val ib = files.open("index.b")
    ib.write(0, full.take(full.length / 2))
    ib.force()

    val expected = first - 9 + (11 -> s4) + (7 -> s7)
    val r = FsifRecovery.recover(files, layout)
    r.seq shouldBe 1
    r.activeIndex shouldBe "index.b"
    r.activeJournal shouldBe "journal.b"
    r.nextBatch shouldBe 2
    r.salt shouldBe saltOf(files, "journal.b")
    checkObjects(files, r, expected)

    // index.b now holds the map at the end of journal 0; journal 0 and index 0 are empty
    val (seq, m) = readIndex(files, "index.b").getOrElse(fail("index.b invalid"))
    seq shouldBe 1
    m.slots.toList shouldBe mapOf(first).slots.toList
    m.ids.toList shouldBe mapOf(first).ids.toList
    files.open("journal.a").size shouldBe 0
    files.open("index.a").size shouldBe 0

    val again = files.crash()
    val r2 = FsifRecovery.recover(again, layout)
    r2.seq shouldBe 1
    r2.activeIndex shouldBe "index.b"
    checkObjects(again, r2, expected)

  test("both index files invalid -> StoreLost"):
    val files = fresh()
    val ia = files.open("index.a")
    val b = ia.read(20, 1)
    ia.write(20, Array((b(0) ^ 1).toByte))
    ia.force()
    files.open("index.b").write(0, Array[Byte](1, 2, 3))
    a[StoreLost] should be thrownBy FsifRecovery.recover(files, layout)

  test("corruption in the middle of the active journal -> StoreLost"):
    val files = fresh()
    threeObjects(files)
    val j = files.open("journal.a")
    val pos = JournalFormat.HeaderSize + 30L
    j.write(pos, Array((j.read(pos, 1)(0) ^ 0x40).toByte))
    j.force()
    a[StoreLost] should be thrownBy FsifRecovery.recover(files, layout)

  test("a torn tail is truncated to the valid length"):
    val files = fresh()
    val expected = threeObjects(files)
    val j = files.open("journal.a")
    val valid = j.size
    val (r4, _) = alloc(11, 3, oid(11), bytes(300, 4), 5)
    val torn = JournalFormat.encodeBatch(saltOf(files, "journal.a"), 3, r4)
    j.write(valid, torn.take(torn.length - 9))
    j.force()
    val r = FsifRecovery.recover(files, layout)
    r.journalLength shouldBe valid
    files.open("journal.a").size shouldBe valid
    r.nextBatch shouldBe 3
    checkObjects(files, r, expected)
    files.crash().open("journal.a").size shouldBe valid

  test("a slot mapped twice in the index -> StoreLost"):
    val files = fresh()
    val m = IndexMap.empty(layout.segmentCount)
    m.slots(1) = 0
    m.ids(1) = oid(1)
    m.slots(2) = 0
    m.ids(2) = oid(2)
    writeIndex(files, "index.a", 0, m)
    files.open("data").truncate(layout.stride)
    a[StoreLost] should be thrownBy FsifRecovery.recover(files, layout)

  test("a mapped slot beyond the data file -> StoreLost"):
    val files = fresh()
    val m = IndexMap.empty(layout.segmentCount)
    m.slots(1) = 2
    m.ids(1) = oid(1)
    writeIndex(files, "index.a", 0, m)
    files.open("data").truncate(2 * layout.stride)
    a[StoreLost] should be thrownBy FsifRecovery.recover(files, layout)

  test("a Delta with sliceLength > segmentPayloadSize -> StoreLost"):
    val files = fresh()
    val big = bytes(layout.segmentPayloadSize + 1, 1)
    val h = header(1, oid(1), big, 1)
    appendBatches(files, "journal.a", 0, List(List(Mapping(1, 0, oid(1)), Delta(1, 0, h, List(DeltaRange(0, big))))))
    a[StoreLost] should be thrownBy FsifRecovery.recover(files, layout)
    files.open("data").size shouldBe 0

  test("index and journal letters that disagree still recover, naming the active index"):
    val files = fresh()
    writeIndex(files, "index.b", 1, IndexMap.empty(layout.segmentCount))
    FsifRecovery.writeJournalHeader(files.open("journal.a"), 1)
    val (rs, s) = alloc(4, 0, oid(4), bytes(50, 1), 1)
    appendBatches(files, "journal.a", 0, List(rs))
    val r = FsifRecovery.recover(files, layout)
    r.seq shouldBe 1
    r.activeIndex shouldBe "index.b"
    r.activeJournal shouldBe "journal.a"
    checkObjects(files, r, Map(4 -> s))

  test("a Mapping beyond the reachable slot bound -> StoreLost"):
    val files = fresh()
    // Empty data and index, one Mapping: bound = 0 + 1
    val (rs, _) = alloc(1, 2, oid(1), bytes(10, 1), 1)
    appendBatches(files, "journal.a", 0, List(rs))
    a[StoreLost] should be thrownBy FsifRecovery.recover(files, layout)
    files.open("data").size shouldBe 0

  test("a chain of allocations each taking the next new top slot is accepted"):
    val files = fresh()
    val allocs = (0 until 6).map(i => (i + 10, alloc(i + 10, i, oid(i + 10), bytes(20, i), i + 1)))
    appendBatches(files, "journal.a", 0, allocs.map(_._2._1).toList)
    val r = FsifRecovery.recover(files, layout)
    r.slotCount shouldBe 6
    checkObjects(files, r, allocs.map((ext, a) => ext -> a._2).toMap)
    // A further chain continues from the top; the data file now also covers the first six slots
    val more = (6 until 9).map(i => (i + 10, alloc(i + 10, i, oid(i + 10), bytes(20, i), i + 1)))
    appendBatches(files, "journal.a", 6, more.map(_._2._1).toList)
    val r2 = FsifRecovery.recover(files, layout)
    r2.slotCount shouldBe 9
    checkObjects(files, r2, (allocs ++ more).map((ext, a) => ext -> a._2).toMap)

  test("initialize on a reused directory empties the old index.a before anything else"):
    val files = fresh()
    threeObjects(files)
    FsifRecovery.recover(files, layout)
    var ops = List[String]()
    files.failWhile(Fault.IoError, name => { ops = ops :+ name; false })(true)
    FsifRecovery.initialize(files, layout)
    ops.head shouldBe "index.a"
    ops.last shouldBe "index.a"
    val r = FsifRecovery.recover(files, layout)
    r.index.slots.forall(_ == -1) shouldBe true
    r.slotCount shouldBe 0

  test("a record with an ext outside [0, segmentCount) -> StoreLost"):
    val files = fresh()
    appendBatches(files, "journal.a", 0, List(List(Mapping(layout.segmentCount, 0, oid(1)))))
    a[StoreLost] should be thrownBy FsifRecovery.recover(files, layout)

  test("missing journal m -> StoreLost"):
    val files = fresh()
    files.open("journal.a").truncate(0)
    a[StoreLost] should be thrownBy FsifRecovery.recover(files, layout)

  test("a journal with seq > m+1 -> StoreLost"):
    val files = fresh()
    FsifRecovery.writeJournalHeader(files.open("journal.b"), 2)
    a[StoreLost] should be thrownBy FsifRecovery.recover(files, layout)

  test("an invalid header on a long journal -> StoreLost"):
    val files = fresh()
    val j = files.open("journal.b")
    j.write(0, bytes(JournalFormat.HeaderSize + 1, 9))
    j.force()
    a[StoreLost] should be thrownBy FsifRecovery.recover(files, layout)

  test("an invalid header no longer than a header is an empty journal"):
    val files = fresh()
    val j = files.open("journal.b")
    j.write(0, bytes(JournalFormat.HeaderSize, 9))
    j.force()
    FsifRecovery.recover(files, layout).activeJournal shouldBe "journal.a"

  /** Completed checkpoint 0 -> 1 whose journal 0 and index 0 were never truncated */
  private def staleSetup(files: FsifFiles): Map[Int, Seg] =
    val first = threeObjects(files)
    FsifRecovery.recover(files, layout) // applies journal 0 in place
    FsifRecovery.writeJournalHeader(files.open("journal.b"), 1)
    writeIndex(files, "index.b", 1, mapOf(first))
    val (u3, s3) = update(3, first(3), 0, bytes(5, 77), 9)
    appendBatches(files, "journal.b", 0, List(List(u3)))
    first + (3 -> s3)

  test("a non-empty journal with seq < m is stale and ignored"):
    val files = fresh()
    val expected = staleSetup(files)
    // A record in the stale journal that would be visible if it were replayed
    val (r20, _) = alloc(20, 5, oid(20), bytes(10, 1), 1)
    appendBatches(files, "journal.a", 3, List(r20))
    val r = FsifRecovery.recover(files, layout)
    r.seq shouldBe 1
    r.activeJournal shouldBe "journal.b"
    checkObjects(files, r, expected)

  test("a corrupt journal with a valid header and seq < m is stale and ignored"):
    val files = fresh()
    val expected = staleSetup(files)
    val j = files.open("journal.a")
    val pos = JournalFormat.HeaderSize + 30L
    j.write(pos, Array((j.read(pos, 1)(0) ^ 0x40).toByte))
    j.force()
    val r = FsifRecovery.recover(files, layout)
    r.seq shouldBe 1
    checkObjects(files, r, expected)

  /** Durable store with records in journal m and m+1 (m = 0) and an interrupted checkpoint. Returns the expected
    * objects. Uses reallocation of a freed slot, moves, deletes and in-place updates.
    */
  private def randomStore(files: SimulatedFsifFiles, rnd: Random): Map[Int, Seg] =
    var segs = Map[Int, Seg]()
    var rev = 1
    var nextId = 1000
    def freeSlot(): Int =
      val used = segs.values.map(_.slot).toSet
      Iterator.from(0).find(s => !used.contains(s)).get
    def freeExt(): Int =
      Iterator.continually(rnd.nextInt(layout.segmentCount)).find(e => !segs.contains(e)).get
    def randomOps(n: Int): List[List[JournalRecord]] =
      List.fill(n):
        val batch = mutable.ListBuffer[JournalRecord]()
        for _ <- 0 until 1 + rnd.nextInt(3) do
          rev += 1
          rnd.nextInt(5) match
            case 0 | 1 if segs.size < 20 =>
              val ext = freeExt()
              nextId += 1
              val (rs, s) = alloc(ext, freeSlot(), oid(nextId), bytes(rnd.nextInt(1025), rnd.nextInt()), rev)
              batch ++= rs
              segs += ext -> s
            case 2 if segs.nonEmpty =>
              val ext = segs.keys.toSeq(rnd.nextInt(segs.size))
              val s = segs(ext)
              if s.slice.nonEmpty then
                val off = rnd.nextInt(s.slice.length)
                val len = rnd.nextInt(s.slice.length - off + 1)
                val (r, s2) = update(ext, s, off, bytes(len, rnd.nextInt()), rev)
                batch += r
                segs += ext -> s2
            case 3 if segs.nonEmpty =>
              val ext = segs.keys.toSeq(rnd.nextInt(segs.size))
              batch += Mapping(ext, -1, Zero)
              segs -= ext
            case _ if segs.nonEmpty =>
              // Copy-on-write style move to a new slot
              val ext = segs.keys.toSeq(rnd.nextInt(segs.size))
              val s = segs(ext)
              val slot = freeSlot()
              val (rs, s2) = alloc(ext, slot, s.id, bytes(rnd.nextInt(1025), rnd.nextInt()), rev)
              batch ++= rs
              segs += ext -> s2
            case _ =>
        batch.toList
    appendBatches(files, "journal.a", 0, randomOps(1 + rnd.nextInt(4)))
    val atEndOf0 = segs
    FsifRecovery.writeJournalHeader(files.open("journal.b"), 1)
    appendBatches(files, "journal.b", 0, randomOps(1 + rnd.nextInt(4)))
    // index.b: absent, empty or torn
    rnd.nextInt(3) match
      case 0 =>
      case 1 =>
        files.open("index.b").force()
      case _ =>
        val full = IndexFile.encode(1, mapOf(atEndOf0))
        val ib = files.open("index.b")
        ib.write(0, full.take(rnd.nextInt(full.length)))
        ib.force()
    // Optionally a torn, unacknowledged batch at the tail of journal.b
    if rnd.nextBoolean() then
      val jb = files.open("journal.b")
      val (rs, _) = alloc(freeExt(), freeSlot(), oid(1), bytes(500, 1), 1)
      val b = JournalFormat.encodeBatch(saltOf(files, "journal.b"), 99, rs)
      jb.write(jb.size, b.take(1 + rnd.nextInt(b.length - 1)))
      jb.force()
    segs

  /** Makes op number k (0-based, counting writes, forces and truncates) and every later op fail */
  private def failFromOp(files: SimulatedFsifFiles, k: Int): Unit =
    var n = 0
    files.failWhile(Fault.IoError, _ => { n += 1; n > k })(true)

  /** The same durable pre-recovery store for every call with this seed; `crashSeed` drives only crash() draws */
  private def build(seed: Int, crashSeed: Int): (SimulatedFsifFiles, Map[Int, Seg]) =
    val files = new SimulatedFsifFiles(rng(crashSeed))
    FsifRecovery.initialize(files, layout)
    val expected = randomStore(files, rng(seed))
    (files, expected)

  test("recover is idempotent across crashes inside recover (30 seeds)"):
    for seed <- 1 to 30 do
      withClue(s"seed $seed: ") {
        // Count the ops a full recover performs
        val (probe, _) = build(seed, 0)
        var ops = 0
        probe.failWhile(Fault.IoError, _ => { ops += 1; false })(true)
        FsifRecovery.recover(probe, layout)
        ops should be > 0

        val pick = rng(-seed)
        for k <- 0 to ops do
          withClue(s"crash at op $k: ") {
            val (base, expected) = build(seed, seed * 1000 + k)
            val first = base.crash() // everything is forced, so this is exact
            failFromOp(first, k)
            try FsifRecovery.recover(first, layout)
            catch case _: IOException => ()
            // A second crash inside the retried recover, then a clean one
            val second = first.crash()
            failFromOp(second, pick.nextInt(ops + 1))
            try FsifRecovery.recover(second, layout)
            catch case _: IOException => ()
            val last = second.crash()
            checkObjects(last, FsifRecovery.recover(last, layout), expected)
            val after = last.crash()
            checkObjects(after, FsifRecovery.recover(after, layout), expected)
          }
      }

  // Device-journal recovery: newestIndex, replayDirectory and openDirectory

  private val J = new UUID(77, 1)
  private val Att = new UUID(77, 2)

  private def otherIndex(name: String): String = FsifRecovery.IndexFiles.find(_ != name).get

  /** What an open against journal J writes before its first append: index s+1 naming J and Att, not detached */
  private def attach(files: FsifFiles, covered: Long = -1): Unit =
    val (name, seq, _, map) = FsifRecovery.newestIndex(files, layout)
    writeIndex(files, otherIndex(name), seq + 1, IndexMeta(J, Att, covered, detached = false), map)

  private def open(files: FsifFiles, expected: Map[Int, Seg]): Opened =
    val o = FsifRecovery.openDirectory(files, layout, J)
    checkObjects(files, o.index, expected)
    o

  /** The records of threeObjects, as one attachment's records in the device journal */
  private def threeObjectRecords(): (Vector[JournalRecord], Map[Int, Seg]) =
    val (r1, s1) = alloc(3, 0, oid(3), bytes(100, 1), 1)
    val (r2, s2) = alloc(7, 1, oid(7), bytes(1024, 2), 2)
    val (r3, s3) = alloc(9, 2, oid(9), bytes(0, 3), 3)
    val (u1, s1b) = update(3, s1, 10, bytes(20, 50), 4)
    ((r1 ++ r2 ++ r3 :+ u1).toVector, Map(3 -> s1b, 7 -> s2, 9 -> s3))

  test("initialize then openDirectory gives an empty map with seq 0"):
    val files = fresh()
    val (name, seq, meta, _) = FsifRecovery.newestIndex(files, layout)
    (name, seq, meta) shouldBe ("index.a", 0L, IndexMeta.None)
    val o = open(files, Map())
    o.seq shouldBe 0
    o.activeIndex shouldBe "index.a"
    o.slotCount shouldBe 0
    o.index.slots.forall(_ == -1) shouldBe true

  test("records for 3 objects replay byte-exactly"):
    val files = fresh()
    attach(files)
    val (recs, expected) = threeObjectRecords()
    FsifRecovery.replayDirectory(files, layout, recs, J, 5)
    val o = open(files, expected)
    o.slotCount shouldBe 3
    // And again from the crashed image: replay forced everything it relied on
    open(files.crash(), expected)

  test("replayDirectory writes a detached index covering tailSerial"):
    val files = fresh()
    attach(files, covered = 3)
    val (recs, expected) = threeObjectRecords()
    FsifRecovery.replayDirectory(files, layout, recs, J, 17)
    val (seq, meta, map) = readIndexMeta(files, "index.a").getOrElse(fail("index.a invalid"))
    seq shouldBe 2
    meta shouldBe IndexMeta(J, Att, 17, detached = true)
    checkObjects(files, map, expected)
    files.open("index.b").size shouldBe 0
    val again = files.crash()
    FsifRecovery.newestIndex(again, layout)._3 shouldBe IndexMeta(J, Att, 17, detached = true)
    open(again, expected).activeIndex shouldBe "index.a"

  test("replayDirectory with no records still writes the detached index"):
    val files = fresh()
    attach(files, covered = 8)
    FsifRecovery.replayDirectory(files, layout, Vector.empty, J, 8)
    FsifRecovery.newestIndex(files, layout) match
      case (name, seq, meta, _) =>
        (name, seq, meta) shouldBe ("index.a", 2L, IndexMeta(J, Att, 8, detached = true))
    open(files, Map()).slotCount shouldBe 0

  test("a torn index s+1 is ignored and replay starts from index s"):
    val files = fresh()
    val (recs, first) = threeObjectRecords()
    FsifRecovery.replayDirectory(files, layout, recs, J, 3) // index.b seq 1
    attach(files, covered = 3) // index.a seq 2
    val torn = IndexFile.encode(3, IndexMeta(J, Att, 99, detached = true), mapOf(first))
    val ib = files.open("index.b")
    ib.truncate(0)
    ib.write(0, torn.take(torn.length / 2))
    ib.force()
    val (r4, s4) = alloc(11, 3, oid(11), bytes(300, 4), 5)
    val (u7, s7) = update(7, first(7), 1000, bytes(24, 60), 6)
    FsifRecovery.replayDirectory(files, layout, (r4 ++ List(u7, Mapping(9, -1, Zero))).toVector, J, 9)
    val expected = first - 9 + (11 -> s4) + (7 -> s7)
    readIndexMeta(files, "index.b").map(t => (t._1, t._2)) shouldBe Some((3L, IndexMeta(J, Att, 9, detached = true)))
    files.open("index.a").size shouldBe 0
    open(files, expected).activeIndex shouldBe "index.b"
    open(files.crash(), expected)

  test("no valid index file -> StoreLost from every entry point"):
    val files = fresh()
    val ia = files.open("index.a")
    val b = ia.read(20, 1)
    ia.write(20, Array((b(0) ^ 1).toByte))
    ia.force()
    files.open("index.b").write(0, Array[Byte](1, 2, 3))
    a[StoreLost] should be thrownBy FsifRecovery.newestIndex(files, layout)
    a[StoreLost] should be thrownBy FsifRecovery.replayDirectory(files, layout, Vector.empty, J, 0)
    a[StoreLost] should be thrownBy FsifRecovery.openDirectory(files, layout, J)

  test("replay: a slot mapped twice in the index -> StoreLost"):
    val files = fresh()
    val m = IndexMap.empty(layout.segmentCount)
    m.slots(1) = 0
    m.ids(1) = oid(1)
    m.slots(2) = 0
    m.ids(2) = oid(2)
    writeIndex(files, "index.a", 0, IndexMeta(J, Att, -1, detached = false), m)
    files.open("data").truncate(layout.stride)
    a[StoreLost] should be thrownBy FsifRecovery.replayDirectory(files, layout, Vector.empty, J, 0)

  test("a mapped slot beyond the data file -> StoreLost at replay and at open"):
    val files = fresh()
    val m = IndexMap.empty(layout.segmentCount)
    m.slots(1) = 2
    m.ids(1) = oid(1)
    writeIndex(files, "index.a", 0, IndexMeta(J, Att, -1, detached = false), m)
    files.open("data").truncate(2 * layout.stride)
    a[StoreLost] should be thrownBy FsifRecovery.replayDirectory(files, layout, Vector.empty, J, 0)
    writeIndex(files, "index.a", 0, IndexMeta(J, Att, -1, detached = true), m)
    a[StoreLost] should be thrownBy FsifRecovery.openDirectory(files, layout, J)

  test("replay: a Delta with sliceLength > segmentPayloadSize -> StoreLost"):
    val files = fresh()
    attach(files)
    val big = bytes(layout.segmentPayloadSize + 1, 1)
    val h = header(1, oid(1), big, 1)
    val recs = Vector(Mapping(1, 0, oid(1)), Delta(1, 0, h, List(DeltaRange(0, big))))
    a[StoreLost] should be thrownBy FsifRecovery.replayDirectory(files, layout, recs, J, 0)
    files.open("data").size shouldBe 0

  test("replay: a record with an ext outside [0, segmentCount) -> StoreLost"):
    val files = fresh()
    attach(files)
    val recs = Vector(Mapping(layout.segmentCount, 0, oid(1)))
    a[StoreLost] should be thrownBy FsifRecovery.replayDirectory(files, layout, recs, J, 0)

  test("replay: a Mapping beyond the reachable slot bound -> StoreLost"):
    val files = fresh()
    attach(files)
    // Empty data and index, one Mapping: bound = 0 + 1
    val (rs, _) = alloc(1, 2, oid(1), bytes(10, 1), 1)
    a[StoreLost] should be thrownBy FsifRecovery.replayDirectory(files, layout, rs.toVector, J, 0)
    files.open("data").size shouldBe 0

  test("replay: a chain of allocations each taking the next new top slot is accepted"):
    val files = fresh()
    attach(files)
    val allocs = (0 until 6).map(i => (i + 10, alloc(i + 10, i, oid(i + 10), bytes(20, i), i + 1)))
    FsifRecovery.replayDirectory(files, layout, allocs.flatMap(_._2._1).toVector, J, 5)
    open(files, allocs.map((ext, a) => ext -> a._2).toMap).slotCount shouldBe 6
    // A further chain continues from the top; the data file now also covers the first six slots
    attach(files, covered = 5)
    val more = (6 until 9).map(i => (i + 10, alloc(i + 10, i, oid(i + 10), bytes(20, i), i + 1)))
    FsifRecovery.replayDirectory(files, layout, more.flatMap(_._2._1).toVector, J, 8)
    open(files, (allocs ++ more).map((ext, a) => ext -> a._2).toMap).slotCount shouldBe 9

  test("initialize on a replayed directory empties the old index first and leaves an empty store"):
    val files = fresh()
    val (recs, _) = threeObjectRecords()
    FsifRecovery.replayDirectory(files, layout, recs, J, 3) // the store's index is now index.b
    attach(files, covered = 3) // and index.a again
    var ops = List[String]()
    files.failWhile(Fault.IoError, name => { ops = ops :+ name; false })(true)
    FsifRecovery.initialize(files, layout)
    ops.head shouldBe "index.a"
    ops.last shouldBe "index.a"
    files.open("index.b").size shouldBe 0
    FsifRecovery.newestIndex(files, layout)._3 shouldBe IndexMeta.None
    open(files, Map()).slotCount shouldBe 0

  test("openDirectory refuses an undetached index of its own journal with an IOException"):
    val files = fresh()
    attach(files)
    val e = the[IOException] thrownBy FsifRecovery.openDirectory(files, layout, J)
    e.getMessage should include("not replayed")

  test("openDirectory accepts an index naming another journal"):
    val files = fresh()
    val (recs, expected) = threeObjectRecords()
    FsifRecovery.replayDirectory(files, layout, recs, new UUID(5, 5), 3)
    // Attached elsewhere and shipped (or moved) without a detach: nothing of it is in J
    val (name, seq, _, map) = FsifRecovery.newestIndex(files, layout)
    writeIndex(files, otherIndex(name), seq + 1, IndexMeta(new UUID(5, 5), new UUID(5, 6), 3, detached = false), map)
    val o = open(files, expected)
    o.seq shouldBe seq + 1
    o.activeIndex shouldBe otherIndex(name)
    o.slotCount shouldBe 3

  /** One attachment's records in the device journal after a durable earlier state: phase 1 records replayed into
    * index.b (seq 1), then the attach index (index.a, seq 2), then phase 2 records not yet replayed. Uses
    * reallocation of a freed slot, moves, deletes and in-place updates. Returns the records and the expected objects.
    */
  private def randomAttached(files: SimulatedFsifFiles, rnd: Random): (Vector[JournalRecord], Map[Int, Seg]) =
    var segs = Map[Int, Seg]()
    var rev = 1
    var nextId = 1000
    def freeSlot(): Int =
      val used = segs.values.map(_.slot).toSet
      Iterator.from(0).find(s => !used.contains(s)).get
    def freeExt(): Int =
      Iterator.continually(rnd.nextInt(layout.segmentCount)).find(e => !segs.contains(e)).get
    def randomOps(n: Int): Vector[JournalRecord] =
      val batch = mutable.ArrayBuffer[JournalRecord]()
      for _ <- 0 until n do
        rev += 1
        rnd.nextInt(5) match
          case 0 | 1 if segs.size < 20 =>
            val ext = freeExt()
            nextId += 1
            val (rs, s) = alloc(ext, freeSlot(), oid(nextId), bytes(rnd.nextInt(1025), rnd.nextInt()), rev)
            batch ++= rs
            segs += ext -> s
          case 2 if segs.nonEmpty =>
            val ext = segs.keys.toSeq(rnd.nextInt(segs.size))
            val s = segs(ext)
            if s.slice.nonEmpty then
              val off = rnd.nextInt(s.slice.length)
              val len = rnd.nextInt(s.slice.length - off + 1)
              val (r, s2) = update(ext, s, off, bytes(len, rnd.nextInt()), rev)
              batch += r
              segs += ext -> s2
          case 3 if segs.nonEmpty =>
            val ext = segs.keys.toSeq(rnd.nextInt(segs.size))
            batch += Mapping(ext, -1, Zero)
            segs -= ext
          case _ if segs.nonEmpty =>
            // Copy-on-write style move to a new slot
            val ext = segs.keys.toSeq(rnd.nextInt(segs.size))
            val s = segs(ext)
            val (rs, s2) = alloc(ext, freeSlot(), s.id, bytes(rnd.nextInt(1025), rnd.nextInt()), rev)
            batch ++= rs
            segs += ext -> s2
          case _ =>
      batch.toVector
    FsifRecovery.replayDirectory(files, layout, randomOps(1 + rnd.nextInt(8)), J, 10)
    attach(files, covered = 10)
    // The older index.b: valid, emptied or torn; the newest (index.a) is what replay starts from
    rnd.nextInt(3) match
      case 0 =>
      case 1 =>
        val ib = files.open("index.b")
        ib.truncate(0)
        ib.force()
      case _ =>
        val ib = files.open("index.b")
        ib.truncate(ib.size / 2)
        ib.force()
    (randomOps(1 + rnd.nextInt(8)), segs)

  /** The same durable store and records for every call with this seed; `crashSeed` drives only crash() draws */
  private def buildAttached(seed: Int, crashSeed: Int): (SimulatedFsifFiles, Vector[JournalRecord], Map[Int, Seg]) =
    val files = new SimulatedFsifFiles(rng(crashSeed))
    FsifRecovery.initialize(files, layout)
    val (recs, expected) = randomAttached(files, rng(seed))
    (files, recs, expected)

  /** A device load: replay the directory unless its newest index is already detached */
  private def deviceLoad(files: FsifFiles, recs: Vector[JournalRecord]): Unit =
    if !FsifRecovery.newestIndex(files, layout)._3.detached then
      FsifRecovery.replayDirectory(files, layout, recs, J, 20)

  test("replayDirectory is idempotent across crashes inside replayDirectory (30 seeds)"):
    for seed <- 1 to 30 do
      withClue(s"seed $seed: ") {
        // Count the ops a full replay performs
        val (probe, probeRecs, _) = buildAttached(seed, 0)
        var ops = 0
        probe.failWhile(Fault.IoError, _ => { ops += 1; false })(true)
        FsifRecovery.replayDirectory(probe, layout, probeRecs, J, 20)
        ops should be > 0

        val pick = rng(-seed)
        for k <- 0 to ops do
          withClue(s"crash at op $k: ") {
            val (base, recs, expected) = buildAttached(seed, seed * 1000 + k)
            val first = base.crash() // everything is forced, so this is exact
            failFromOp(first, k)
            try FsifRecovery.replayDirectory(first, layout, recs, J, 20)
            catch case _: IOException => ()
            // A second crash inside the retried device load, then a clean one
            val second = first.crash()
            failFromOp(second, pick.nextInt(ops + 1))
            try deviceLoad(second, recs)
            catch case _: IOException => ()
            val last = second.crash()
            deviceLoad(last, recs)
            open(last, expected)
            FsifRecovery.newestIndex(last, layout)._3 shouldBe IndexMeta(J, Att, 20, detached = true)
            open(last.crash(), expected)
          }
      }
