package org.aspen_ddp.aspen.server.store.backend.fsif

import java.io.IOException
import java.util.UUID

import scala.concurrent.Future
import scala.util.{Random, Success}

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import org.aspen_ddp.aspen.common.DataBuffer
import org.aspen_ddp.aspen.common.HLCTimestamp
import org.aspen_ddp.aspen.common.objects.{Metadata, ObjectId, ObjectRefcount, ObjectRevision, ObjectType}
import org.aspen_ddp.aspen.common.transaction.TransactionId

class DeviceJournalRecoverySuite extends AnyFunSuite with Matchers:
  import DeviceJournalRecovery.{Outcome, Target}
  import JournalRecord.{Delta, Mapping}

  private val jid = new UUID(5, 6)
  private val sizing = JournalSizing(jid, 3, 64 * 1024)
  /** Blocks (batches of one small append) per file */
  private val perFile = (sizing.fileSize / 4096 - 1).toInt
  private val layout = FsifLayout(1024, 64)

  // Objects

  private def oid(i: Int) = ObjectId(new UUID(i, ~i))
  private def md(rev: Int) =
    Metadata(ObjectRevision(TransactionId(new UUID(rev, 6))), ObjectRefcount(1, 1), HLCTimestamp(rev))
  private def bytes(n: Int, v: Int): Array[Byte] = Array.tabulate[Byte](n)(j => (v + j * 7).toByte)

  /** Test-side model of one object's segment */
  private final case class Seg(slot: Int, id: ObjectId, header: SegmentHeader, slice: Array[Byte])

  private def header(ext: Int, id: ObjectId, slice: Array[Byte], rev: Int): SegmentHeader =
    SegmentHeader.forSlice(ObjectType.Data, id, ext, md(rev), DataBuffer(slice))

  /** Map + full Delta, as a journaled allocation writes them */
  private def alloc(ext: Int, slot: Int, id: ObjectId, slice: Array[Byte], rev: Int): (List[JournalRecord], Seg) =
    val h = header(ext, id, slice, rev)
    (List(Mapping(ext, slot, id), Delta(ext, slot, h, List(DeltaRange(0, slice)))), Seg(slot, id, h, slice))

  /** In-place update of [off, off + bytes.length) with absolute bytes */
  private def update(ext: Int, s: Seg, off: Int, b: Array[Byte], rev: Int): (JournalRecord, Seg) =
    val slice = s.slice.clone()
    System.arraycopy(b, 0, slice, off, b.length)
    val h = header(ext, s.id, slice, rev)
    (Delta(ext, s.slot, h, List(DeltaRange(off, b))), s.copy(header = h, slice = slice))

  // One machine: the ring under fj/ and store i under s<i>/

  private def newMachine(seed: Long): SimulatedFsifFiles =
    val sim = new SimulatedFsifFiles(new Random(seed))
    for n <- 0 until sizing.files do
      val f = ringFiles(sim).open(DeviceJournalScan.fileName(n))
      f.write(0, new Array[Byte](sizing.fileSize.toInt))
      f.force()
      f.close()
    sim

  private def ringFiles(sim: FsifFiles): FsifFiles = new PrefixedFsifFiles(sim, "fj/")
  private def storeFiles(sim: FsifFiles, i: Int): FsifFiles = new PrefixedFsifFiles(sim, s"s$i/")
  private def key(i: Int): os.Path = os.Path(s"/dev0/copies/s$i")
  private def target(sim: FsifFiles, i: Int): Target = Target(key(i), storeFiles(sim, i), layout)

  private def journal(sim: FsifFiles): FsifDeviceJournal =
    new FsifDeviceJournal(ringFiles(sim), sizing, None, r => r.run(), forceAfterWrite = true)

  private object NoOwner extends FsifDeviceJournal.Owner:
    def complete(task: Runnable): Unit = task.run()
    def roomAvailable(): Unit = ()
    def checkpointRequested(): Unit = ()
    def journalFailed(t: Throwable): Unit = ()

  private def writeIndex(files: FsifFiles, seq: Long, meta: IndexMeta, map: IndexMap): Unit =
    val (name, _, _, _) = FsifRecovery.newestIndex(files, layout)
    val f = files.open(FsifRecovery.IndexFiles.find(_ != name).get)
    f.truncate(0)
    f.write(0, IndexFile.encode(seq, meta, map))
    f.force()
    f.close()

  /** What a backend open writes before its first append: index s+1 naming `journalId`, `att` and `covered` */
  private def attachIndex(files: FsifFiles, att: UUID, covered: Long, journalId: UUID = jid): Unit =
    val (_, seq, _, map) = FsifRecovery.newestIndex(files, layout)
    writeIndex(files, seq + 1, IndexMeta(journalId, att, covered, detached = false), map)

  /** Store i created and opened against `j` as attachment `att` */
  private def openStore(sim: FsifFiles, j: FsifDeviceJournal, i: Int, att: UUID): FsifFiles =
    val f = storeFiles(sim, i)
    FsifRecovery.initialize(f, layout)
    attachIndex(f, att, j.tailSerial)
    j.attach(att, key(i), NoOwner)
    f

  private def appended(f: Future[Long]): Long = f.value match
    case Some(Success(s)) => s
    case other => fail(s"append not completed: $other")

  private def append(j: FsifDeviceJournal, att: UUID, recs: List[JournalRecord]): Long =
    appended(j.append(att, recs))

  /** A checkpoint of store `files` covering `recs` (its records since its last one) up to `serial`: data and a new
    * attached index reflect them, and the journal is told
    */
  private def checkpoint(files: FsifFiles, j: FsifDeviceJournal, att: UUID, recs: Vector[JournalRecord],
                         serial: Long): Unit =
    FsifRecovery.replayDirectory(files, layout, recs, jid, serial)
    attachIndex(files, att, serial)
    j.covered(att, serial)

  private def checkObjects(files: FsifFiles, expected: Map[Int, Seg]): Unit =
    val o = FsifRecovery.openDirectory(files, layout, jid)
    for ext <- 0 until layout.segmentCount do
      withClue(s"ext $ext: ") {
        expected.get(ext) match
          case Some(s) =>
            o.index.slots(ext) shouldBe s.slot
            o.index.ids(ext) shouldBe s.id
            val (h, slice) = FsifRecovery.readSlot(files, layout, s.slot).getOrElse(fail(s"slot ${s.slot} unreadable"))
            h shouldBe s.header
            slice.toList shouldBe s.slice.toList
          case None =>
            o.index.slots(ext) shouldBe -1
      }

  private def metaOf(files: FsifFiles): IndexMeta = FsifRecovery.newestIndex(files, layout)._3

  /** Every file of the machine with its current bytes */
  private def image(sim: SimulatedFsifFiles): Map[String, List[Byte]] =
    val names = (0 until sizing.files).map(n => "fj/" + DeviceJournalScan.fileName(n)) ++
      (0 until 3).flatMap(i => FsifRecovery.AllFiles.map(n => s"s$i/$n"))
    def read(n: String): List[Byte] =
      val f = sim.open(n)
      try f.read(0, f.size.toInt).toList
      finally f.close()
    names.filter(sim.exists).map(n => n -> read(n)).toMap

  private def recover(sim: FsifFiles, stores: Seq[Int]): DeviceJournalRecovery.Result =
    DeviceJournalRecovery.recover(ringFiles(sim), sizing, stores.map(target(sim, _)))

  test("replay applies each attachment's records only to its own directory"):
    val sim = newMachine(1)
    val j = journal(sim)
    // Two copies of one store (the same objects), attached separately, plus a released attachment's records
    val (a0, a1, gone) = (new UUID(1, 0), new UUID(1, 1), new UUID(1, 2))
    val s0 = openStore(sim, j, 0, a0)
    val s1 = openStore(sim, j, 1, a1)
    j.attach(gone, key(9), NoOwner)
    val (r0, o0) = alloc(3, 0, oid(3), bytes(100, 1), 1)
    val (r1, o1) = alloc(3, 0, oid(3), bytes(200, 2), 2)
    val (r1b, o1b) = alloc(7, 1, oid(7), bytes(1024, 3), 3)
    val (u0, o0b) = update(3, o0, 10, bytes(20, 9), 4)
    append(j, a0, r0)
    append(j, a1, r1)
    append(j, gone, List(Mapping(5, 1000, oid(5)))) // fatal if replayed into either directory
    append(j, a1, r1b)
    append(j, a0, List(u0))
    j.release(gone)
    val r = recover(sim, Seq(0, 1))
    r.outcomes shouldBe Map(key(0) -> Outcome.Replayed(3), key(1) -> Outcome.Replayed(4))
    r.start.map(_.nextSerial) shouldBe Some(j.tailSerial + 1)
    checkObjects(s0, Map(3 -> o0b))
    checkObjects(s1, Map(3 -> o1, 7 -> o1b))
    for f <- Seq(s0, s1) do
      metaOf(f).detached shouldBe true
      metaOf(f).covered shouldBe j.tailSerial

  test("records at or below a directory's covered serial are not replayed"):
    val sim = newMachine(2)
    val j = journal(sim)
    val att = new UUID(2, 0)
    val s = openStore(sim, j, 0, att)
    val (r1, o1) = alloc(3, 0, oid(3), bytes(100, 1), 1)
    val (r2, o2) = alloc(4, 1, oid(4), bytes(50, 2), 2)
    append(j, att, r1)
    val done = append(j, att, r2)
    checkpoint(s, j, att, (r1 ++ r2).toVector, done)
    // Delete ext 3 and reuse its slot for ext 5
    val (r3, o3) = alloc(5, 0, oid(5), bytes(70, 3), 3)
    append(j, att, Mapping(3, -1, oid(0)) :: r3)
    recover(sim, Seq(0)).outcomes shouldBe Map(key(0) -> Outcome.Replayed(3))
    checkObjects(s, Map(4 -> o2, 5 -> o3))

  test("Review Focus 1: an attach index with no records replays nothing and becomes detached"):
    val sim = newMachine(3)
    val j = journal(sim)
    val (a0, a1) = (new UUID(3, 0), new UUID(3, 1))
    val s1 = openStore(sim, j, 1, a1)
    val (r1, o1) = alloc(2, 0, oid(2), bytes(10, 1), 1)
    append(j, a1, r1)
    // Store 0's attach index is durable, then the crash comes before its first append
    val s0 = openStore(sim, j, 0, a0)
    append(j, a1, List(Mapping(2, -1, oid(0))))
    val after = sim.crash()
    val r = recover(after, Seq(0, 1))
    r.outcomes shouldBe Map(key(0) -> Outcome.Replayed(0), key(1) -> Outcome.Replayed(3))
    val f0 = storeFiles(after, 0)
    metaOf(f0) shouldBe IndexMeta(jid, a0, j.tailSerial, detached = true)
    checkObjects(f0, Map())
    checkObjects(storeFiles(after, 1), Map())
    // A second load finds nothing to do
    recover(after.crash(), Seq(0, 1)).outcomes.values.toSet shouldBe Set(Outcome.NotAttached)

  test("an attached directory over an empty ring replays nothing and the ring starts fresh"):
    val sim = newMachine(4)
    val s = storeFiles(sim, 0)
    FsifRecovery.initialize(s, layout)
    attachIndex(s, new UUID(4, 0), -1)
    val r = recover(sim, Seq(0))
    r.start shouldBe None
    r.outcomes shouldBe Map(key(0) -> Outcome.Replayed(0))
    metaOf(s) shouldBe IndexMeta(jid, new UUID(4, 0), -1, detached = true)

  test("Review Focus 4: an idle directory whose covered serial is three laps behind is not Corrupt"):
    val sim = newMachine(5)
    val j = journal(sim)
    val (idle, busy) = (new UUID(5, 0), new UUID(5, 1))
    val s = openStore(sim, j, 0, idle)
    val (r1, o1) = alloc(3, 0, oid(3), bytes(300, 1), 1)
    val serial = append(j, idle, r1)
    checkpoint(s, j, idle, r1.toVector, serial)
    // The busy store (no listed directory) checkpoints after every append; the idle one stays attached
    j.attach(busy, key(1), NoOwner)
    val laps = 3 * sizing.files * perFile + 5
    for i <- 0 until laps do
      j.covered(busy, append(j, busy, List(Mapping(i % 64, i % 64, oid(i)))))
    j.tailSerial should be > serial + 3L * sizing.files * perFile
    val r = recover(sim.crash(), Seq(0))
    r.outcomes shouldBe Map(key(0) -> Outcome.Replayed(0))
    r.start.map(_.nextSerial) shouldBe Some(j.tailSerial + 1)

  test("one directory's bad Mapping is Lost, the others Replayed"):
    val sim = newMachine(6)
    val j = journal(sim)
    val atts = (0 until 3).map(i => new UUID(6, i))
    val files = (0 until 3).map(i => openStore(sim, j, i, atts(i)))
    val (r0, o0) = alloc(1, 0, oid(1), bytes(40, 1), 1)
    val (r2, o2) = alloc(2, 0, oid(2), bytes(40, 2), 2)
    append(j, atts(0), r0)
    append(j, atts(1), List(Mapping(5, 1000, oid(5)))) // past the reachable slot bound
    append(j, atts(2), r2)
    val before = metaOf(files(1))
    val r = recover(sim, Seq(0, 1, 2))
    r.outcomes(key(0)) shouldBe Outcome.Replayed(2)
    r.outcomes(key(2)) shouldBe Outcome.Replayed(2)
    r.outcomes(key(1)) shouldBe a[Outcome.Lost]
    checkObjects(files(0), Map(1 -> o0))
    checkObjects(files(2), Map(2 -> o2))
    metaOf(files(1)) shouldBe before

  test("a directory with no valid index is Lost"):
    val sim = newMachine(7)
    storeFiles(sim, 0).open("data").close()
    recover(sim, Seq(0)).outcomes(key(0)) shouldBe a[Outcome.Lost]

  test("detached and foreign directories are untouched"):
    val sim = newMachine(8)
    val j = journal(sim)
    val s0 = storeFiles(sim, 0)
    FsifRecovery.initialize(s0, layout) // IndexMeta.None
    val s1 = openStore(sim, j, 1, new UUID(8, 1))
    val (r1, o1) = alloc(1, 0, oid(1), bytes(40, 1), 1)
    val serial = append(j, new UUID(8, 1), r1)
    FsifRecovery.replayDirectory(s1, layout, r1.toVector, jid, serial) // a detach
    val s2 = storeFiles(sim, 2)
    FsifRecovery.initialize(s2, layout)
    attachIndex(s2, new UUID(8, 2), 0, journalId = new UUID(9, 9))
    val before = image(sim)
    val r = recover(sim, Seq(0, 1, 2))
    r.outcomes shouldBe Map(key(0) -> Outcome.NotAttached, key(1) -> Outcome.NotAttached,
      key(2) -> Outcome.ForeignUndetached)
    image(sim) shouldBe before
    checkObjects(s1, Map(1 -> o1))

  test("an IOException from one directory propagates"):
    val sim = newMachine(9)
    val j = journal(sim)
    openStore(sim, j, 0, new UUID(9, 0))
    sim.failNext(Fault.IoError, _ == "s0/data")
    an[IOException] should be thrownBy recover(sim, Seq(0))

  test("a corrupt ring throws JournalCorrupt and writes nothing"):
    val sim = newMachine(10)
    val j = journal(sim)
    val att = new UUID(10, 0)
    val s = openStore(sim, j, 0, att)
    // Nothing covered: every batch is needed. Fill file 0 and go on into file 1
    for i <- 0 until perFile + 2 do
      append(j, att, List(Mapping(i % 64, -1, oid(0))))
    val f = ringFiles(sim).open(DeviceJournalScan.fileName(0))
    f.write(DeviceJournalFormat.HeaderSize + 2 * 4096L, Array.fill[Byte](64)(0x5a))
    f.force()
    f.close()
    val before = image(sim)
    a[JournalCorrupt] should be thrownBy recover(sim, Seq(0))
    image(sim) shouldBe before
    metaOf(s).detached shouldBe false

  /** Three stores with random allocations, updates, deletes and checkpoints, interleaved in one ring, plus a released
    * attachment's records. The same durable machine for every call with this seed; `crashSeed` drives only crash()
    * draws. Returns the expected objects of each store.
    */
  private def build(seed: Int, crashSeed: Int): (SimulatedFsifFiles, Seq[Map[Int, Seg]]) =
    val sim = newMachine(crashSeed)
    val j = journal(sim)
    val rnd = new Random(seed * 7919L + 13)
    val atts = (0 until 3).map(i => new UUID(seed, i))
    val files = (0 until 3).map(i => openStore(sim, j, i, atts(i)))
    val segs = Array.fill(3)(Map[Int, Seg]())
    val sinceCheckpoint = Array.fill(3)(Vector[JournalRecord]())
    val last = Array.fill(3)(-1L)
    val gone = new UUID(seed, 99)
    j.attach(gone, key(9), NoOwner)
    var rev = 0
    for step <- 0 until 20 do
      val i = rnd.nextInt(3)
      rev += 1
      val s = segs(i)
      val used = s.values.map(_.slot).toSet
      val recs: List[JournalRecord] = rnd.nextInt(4) match
        case 0 | 1 if s.size < 20 =>
          val ext = Iterator.continually(rnd.nextInt(layout.segmentCount)).find(e => !s.contains(e)).get
          val slot = Iterator.from(0).find(x => !used.contains(x)).get
          val (rs, seg) = alloc(ext, slot, oid(seed * 1000 + rev), bytes(rnd.nextInt(1025), rnd.nextInt()), rev)
          segs(i) = s + (ext -> seg)
          rs
        case 2 if s.nonEmpty && s.values.exists(_.slice.nonEmpty) =>
          val (ext, seg) = s.filter(_._2.slice.nonEmpty).toSeq(rnd.nextInt(s.count(_._2.slice.nonEmpty)))
          val off = rnd.nextInt(seg.slice.length)
          val (r, seg2) = update(ext, seg, off, bytes(rnd.nextInt(seg.slice.length - off + 1), rnd.nextInt()), rev)
          segs(i) = s + (ext -> seg2)
          List(r)
        case 3 if s.nonEmpty =>
          val ext = s.keys.toSeq(rnd.nextInt(s.size))
          segs(i) = s - ext
          List(Mapping(ext, -1, oid(0)))
        case _ =>
          Nil
      if recs.nonEmpty then
        last(i) = append(j, atts(i), recs)
        sinceCheckpoint(i) = sinceCheckpoint(i) ++ recs
      if rnd.nextInt(4) == 0 then
        append(j, gone, List(Mapping(5, 1000, oid(5))))
      if rnd.nextInt(5) == 0 && sinceCheckpoint(i).nonEmpty then
        checkpoint(files(i), j, atts(i), sinceCheckpoint(i), last(i))
        sinceCheckpoint(i) = Vector()
    (sim, segs.toSeq)

  /** Makes op number k (0-based, counting writes, forces and truncates) and every later op fail */
  private def failFromOp(sim: SimulatedFsifFiles, k: Int): Unit =
    var n = 0
    sim.failWhile(Fault.IoError, _ => { n += 1; n > k })(true)

  test("crash at every op of recover, then recover again, yields the same objects (20 seeds)"):
    for seed <- 1 to 20 do
      withClue(s"seed $seed: ") {
        val (probe, _) = build(seed, 0)
        val p = probe.crash()
        var ops = 0
        p.failWhile(Fault.IoError, _ => { ops += 1; false })(true)
        recover(p, 0 until 3).outcomes.values.foreach(_ shouldBe a[Outcome.Replayed])
        ops should be > 0
        for k <- 0 to ops do
          withClue(s"crash at op $k: ") {
            val (base, expected) = build(seed, seed * 1000 + k)
            val first = base.crash() // everything is forced, so this is exact
            failFromOp(first, k)
            try recover(first, 0 until 3)
            catch case _: IOException => ()
            val second = first.crash()
            val r = recover(second, 0 until 3)
            r.outcomes.values.foreach: o =>
              o should (be(Outcome.NotAttached) or be(a[Outcome.Replayed]))
            for i <- 0 until 3 do
              withClue(s"store $i: ") {
                metaOf(storeFiles(second, i)).detached shouldBe true
                checkObjects(storeFiles(second, i), expected(i))
              }
            val third = second.crash()
            for i <- 0 until 3 do
              checkObjects(storeFiles(third, i), expected(i))
          }
      }
