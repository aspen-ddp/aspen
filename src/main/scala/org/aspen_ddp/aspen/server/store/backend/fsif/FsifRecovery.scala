package org.aspen_ddp.aspen.server.store.backend.fsif

import java.io.IOException
import java.security.SecureRandom
import java.util.UUID

import scala.util.control.NonFatal

/** Store initialization and crash recovery.
  *
  * Device-journal recovery (spec 4.7) is split in two: `replayDirectory` runs at device load for each directory
  * whose newest index is attached to the device's journal and not detached, and `openDirectory` runs at every store
  * open. `recover` is the per-store-journal recovery FsifBackend still opens with: it picks the newest valid index,
  * replays the journals that follow it, applies the surviving Deltas in place, and finishes an interrupted checkpoint.
  *
  * Every step of `recover` either only reads, rewrites bytes that a repeated run rewrites identically (absolute-byte
  * Deltas, the torn-tail truncation), or moves the store to a state whose own recovery yields the same objects
  * (finishing a checkpoint). A crash anywhere inside it followed by another `recover` therefore yields the same
  * acknowledged state.
  */
object FsifRecovery:
  final class StoreLost(msg: String) extends org.aspen_ddp.aspen.server.store.backend.BackendStateLost(msg)

  /** `activeIndex` holds index `seq`; `salt` is the active journal's salt; new batches continue at
    * `journalLength` with number `nextBatch`
    */
  final case class Recovered(index: IndexMap, seq: Long, salt: Long, activeIndex: String, activeJournal: String,
                             journalLength: Long, nextBatch: Long, slotCount: Int)

  /** A store directory ready to attach: `activeIndex` holds index `seq` */
  final case class Opened(index: IndexMap, seq: Long, activeIndex: String, slotCount: Int)

  val DataFile = "data"
  val IndexFiles: Vector[String] = Vector("index.a", "index.b")
  val JournalFiles: Vector[String] = Vector("journal.a", "journal.b")
  /** Every FSIF file of a store directory, indexes first, including the per-store journals FsifBackend still uses */
  val AllFiles: Vector[String] = IndexFiles ++ JournalFiles :+ DataFile
  /** The FSIF files of a store directory once the device journal holds its records, indexes first */
  val StoreFiles: Vector[String] = IndexFiles :+ DataFile

  /** `load` reads a file whole into one array, so a larger index or journal loses the store (StoreLost). No file the
    * backend produces is larger: an index is at most IndexFile.sizeFor(FsifLayout.MaxSegmentCount), and a journal at
    * most FsifBackend.journalLengthBound (FsifTuning.journalHardLimit).
    */
  val MaxLoadSize: Long = Int.MaxValue - 16L

  /** Every FSIF file is absent or zero-length: the directory holds no FSIF store. FsifBackend.open treats it as lost,
    * since a store directory exists only once its store was created (StoreCreation)
    */
  def isBlank(files: FsifFiles): Boolean =
    AllFiles.forall(name => !files.exists(name) || files.size(name) == 0)

  private val random = new SecureRandom

  /** (Re)writes a journal header: truncate(0), force, write the header, force. The first force guarantees that an
    * invalid header on a file longer than a header is never a crash artifact. Returns the new salt.
    */
  def writeJournalHeader(file: FsifFile, seq: Long): Long =
    val salt = random.nextLong()
    file.truncate(0)
    file.force()
    file.write(0, JournalFormat.encodeHeader(seq, salt))
    file.force()
    salt

  private def withFile[A](files: FsifFiles, name: String)(f: FsifFile => A): A =
    val file = files.open(name)
    try f(file)
    finally file.close()

  private def truncateToZero(files: FsifFiles, name: String): Unit =
    withFile(files, name): f =>
      f.truncate(0)
      f.force()

  /** Fresh store: empty data, journal.a = header(seq 0) and index.a = (seq 0, IndexMeta.None, empty map), each
    * forced. journal.a is written only for FsifBackend's per-store journal and goes with it. The index is
    * written last, so a valid index implies the rest of the store exists. On a reused directory an old index.a is
    * emptied first, so a crash part way through can never pair it with emptied or rewritten files. index.b and
    * journal.b are left absent (or emptied, if a previous store left them behind). Run in StoreCreation's scratch
    * directory, which a failure or crash discards, so nothing ever opens a partly initialized store.
    */
  def initialize(files: FsifFiles, layout: FsifLayout): Unit =
    writeInitial(files, layout)

  private def writeInitial(files: FsifFiles, layout: FsifLayout): Unit =
    if files.exists(IndexFiles(0)) then
      truncateToZero(files, IndexFiles(0))
    for name <- Seq(IndexFiles(1), JournalFiles(1)) do
      if files.exists(name) then
        truncateToZero(files, name)
    truncateToZero(files, DataFile)
    withFile(files, JournalFiles(0))(f => writeJournalHeader(f, 0))
    withFile(files, IndexFiles(0)): f =>
      f.truncate(0)
      f.write(0, IndexFile.encode(0, IndexMeta.None, IndexMap.empty(layout.segmentCount)))
      f.force()

  /** Reads the whole file after forcing it, so nothing recovery relies on lives only in the page cache. None if the
    * file is missing.
    */
  private def load(files: FsifFiles, name: String): Option[Array[Byte]] =
    if !files.exists(name) then
      None
    else
      withFile(files, name): f =>
        val size = f.size
        if size > MaxLoadSize then
          throw new StoreLost(s"$name is too large to load ($size bytes)")
        f.force()
        Some(f.read(0, size.toInt))

  private final case class Journal(name: String, scan: JournalFormat.Scan.Valid)

  /** Reads the segment in `slot`: its header plus `sliceLength` bytes. None if the header does not decode, its
    * length is outside [0, segmentPayloadSize], or the file ends early. The CRC is not checked; see
    * SegmentHeader.verify.
    */
  private[fsif] def readSlot(file: FsifFile, layout: FsifLayout, slot: Int): Option[(SegmentHeader, Array[Byte])] =
    val base = layout.slotOffset(slot)
    SegmentHeader.decode(file.read(base, SegmentHeader.Size)).flatMap: h =>
      if h.sliceLength < 0 || h.sliceLength > layout.segmentPayloadSize then
        None
      else
        val slice = file.read(base + SegmentHeader.Size, h.sliceLength)
        if slice.length == h.sliceLength then Some((h, slice)) else None

  private[fsif] def readSlot(files: FsifFiles, layout: FsifLayout, slot: Int): Option[(SegmentHeader, Array[Byte])] =
    withFile(files, DataFile)(f => readSlot(f, layout, slot))

  private def replay(start: IndexMap, records: Iterable[JournalRecord]): Replay.Result =
    try Replay.run(start, records)
    catch case e: Replay.InvalidRecord => throw new StoreLost(s"invalid journal record: ${e.getMessage}")

  /** The valid index with the highest seq: (file, seq, meta, map). Each index file is forced before it is read.
    * Throws StoreLost (L1) when neither is valid.
    */
  def newestIndex(files: FsifFiles, layout: FsifLayout): (String, Long, IndexMeta, IndexMap) =
    val indexes = IndexFiles.flatMap: name =>
      load(files, name).flatMap(b => IndexFile.decode(b, layout.segmentCount)).map((seq, meta, map) =>
        (name, seq, meta, map))
    if indexes.isEmpty then
      throw new StoreLost("no valid index file")
    indexes.maxBy(_._2)

  private def forcedDataSize(files: FsifFiles): Long =
    withFile(files, DataFile): data =>
      data.force()
      data.size

  /** Replays `records` over `start` and validates the result before writing anything (L7-L10): every kept Delta's
    * length, that no slot is mapped twice, and the bound on every kept Delta's slot and every final mapped slot.
    * Allocation takes the lowest free slot, so each Mapping to a slot >= 0 grows the slot count by at most one: no
    * legitimate slot exceeds max(D0 / stride, 1 + highest slot in `start`) + that Mapping count, where D0 is the
    * forced data size before applying. This stops a CRC-valid but bogus slot from growing data hugely.
    */
  private def replayChecked(files: FsifFiles, layout: FsifLayout, start: IndexMap,
                            records: Vector[JournalRecord]): Replay.Result =
    val n = layout.segmentCount
    val result = replay(start, records)
    result.apply.foreach: d =>
      if d.header.sliceLength > layout.segmentPayloadSize then
        throw new StoreLost(s"Delta for ext ${d.ext} has slice length ${d.header.sliceLength} > " +
          s"${layout.segmentPayloadSize}")
    val owner = scala.collection.mutable.HashMap[Int, Int]()
    for ext <- 0 until n do
      val slot = result.index.slots(ext)
      if slot >= 0 then
        owner.put(slot, ext).foreach: prev =>
          throw new StoreLost(s"slot $slot is mapped by ext $prev and ext $ext")
    val d0 = forcedDataSize(files)
    val startTop = start.slots.foldLeft(-1)(math.max)
    val mappings = records.count:
      case JournalRecord.Mapping(_, slot, _) => slot >= 0
      case _ => false
    val bound = math.max(d0 / layout.stride, startTop + 1L) + mappings
    def checkBound(slot: Int, what: String): Unit =
      if slot > bound then
        throw new StoreLost(s"$what targets slot $slot beyond the reachable bound $bound")
    result.apply.foreach(d => checkBound(d.slot, s"Delta for ext ${d.ext}"))
    for ext <- 0 until n do
      checkBound(result.index.slots(ext), s"the mapping of ext $ext")
    result

  /** Writes the kept Deltas in place and forces data; returns data's size */
  private def applyDeltas(files: FsifFiles, layout: FsifLayout, deltas: Vector[JournalRecord.Delta]): Long =
    withFile(files, DataFile): data =>
      deltas.foreach: d =>
        val base = layout.slotOffset(d.slot)
        data.write(base, SegmentHeader.encode(d.header))
        d.ranges.foreach(r => data.write(base + SegmentHeader.Size + r.offset, r.bytes))
      data.force()
      data.size

  /** L11 (every mapped slot starts inside data) and L12 (the slot count fits an Int); returns the slot count */
  private def checkedSlotCount(layout: FsifLayout, index: IndexMap, dataSize: Long): Int =
    var highest = -1
    for ext <- 0 until layout.segmentCount do
      val slot = index.slots(ext)
      if slot >= 0 then
        if layout.slotOffset(slot) >= dataSize then
          throw new StoreLost(s"ext $ext maps to slot $slot beyond the data file ($dataSize bytes)")
        highest = math.max(highest, slot)
    val slotCount = math.max(dataSize / layout.stride, highest + 1L)
    if slotCount > Int.MaxValue then
      throw new StoreLost(s"slot count $slotCount is too large")
    slotCount.toInt

  /** Device-load replay of one directory: `records` are its attachment's records above its index's covered serial,
    * in journal order. The caller runs it only for a directory whose newest index names `journalId` and is not
    * detached. As a guard, a newest index that is already detached makes it a no-op (a rerun after the detached
    * index became durable), and an undetached one naming another journal throws an IOException (a caller bug: no
    * record of this journal belongs to it).
    *
    *  1. Load the newest valid index s (L1).
    *  2. Replay the records from it and validate before writing anything (L7-L10, `replayChecked`).
    *  3. Write the kept Deltas in place, force data, then check every mapped slot against data's size (L11) and the
    *     slot count (L12).
    *  4. Write index s+1 into the other index file with IndexMeta(journalId, the attachment, tailSerial, detached),
    *     force it, then empty index s.
    *
    * Steps 1-2 only read. Step 3 writes absolute bytes, so repeating it is harmless, and until step 4 makes index
    * s+1 valid every rerun sees the same inputs. Once index s+1 is valid it is detached, so a device load no longer
    * replays the directory, and data already holds every kept Delta (forced in step 3). Emptying index s only
    * removes a file a rerun ignores. Throws StoreLost when the acknowledged state cannot be reconstructed.
    */
  def replayDirectory(files: FsifFiles, layout: FsifLayout, records: Vector[JournalRecord], journalId: UUID,
                      tailSerial: Long): Unit =
    val (indexName, s, meta, start) = newestIndex(files, layout)
    if meta.detached then
      ()
    else if meta.journalId != journalId then
      throw new IOException(s"$indexName (seq $s) is attached to journal ${meta.journalId}, not $journalId")
    else
      val result = replayChecked(files, layout, start, records)
      val dataSize = applyDeltas(files, layout, result.apply)
      checkedSlotCount(layout, result.index, dataSize)
      val otherIndex = IndexFiles.find(_ != indexName).get
      withFile(files, otherIndex): f =>
        f.truncate(0)
        f.write(0, IndexFile.encode(s + 1, IndexMeta(journalId, meta.attachment, tailSerial, detached = true),
          result.index))
        f.force()
      truncateToZero(files, indexName)

  /** Store open against the device journal `journalId`: loads the newest valid index (L1) and runs the checks that
    * need no journal (L11, L12) against data's forced size. The index must be detached or name another journal (a
    * transferred-in or newly created directory has no records here). One that names `journalId` and is not
    * detached was not replayed at device load, which is a bug, not a loss: it throws an IOException so the load
    * retries.
    */
  def openDirectory(files: FsifFiles, layout: FsifLayout, journalId: UUID): Opened =
    val (indexName, seq, meta, index) = newestIndex(files, layout)
    if !meta.detached && meta.journalId == journalId then
      throw new IOException(s"$indexName (seq $seq) is attached to journal $journalId (attachment " +
        s"${meta.attachment}) but was not replayed at device load")
    val slotCount = checkedSlotCount(layout, index, forcedDataSize(files))
    Opened(index, seq, indexName, slotCount)

  /** Per-store-journal recovery: the shim FsifBackend opens with until the device journal replaces it (then
    * `replayDirectory` and `openDirectory` are the only recovery). Recovers the store, or throws StoreLost when its
    * acknowledged state cannot be reconstructed.
    *
    * Order of steps:
    *  1. Load (and force) both index files and both journals. Pick the valid index with the highest seq m.
    *     Classify the journals: seq < m is stale (also when Corrupt with a valid header), seq m is required,
    *     seq m+1 is optional, anything else that is not Empty loses the store.
    *  2. Replay journal m then journal m+1 from index m. Check every kept Delta's length and that no slot is
    *     mapped twice, and bound every kept Delta's slot and every final mapped slot, before writing anything.
    *     Allocation takes the lowest free slot, so each Mapping to a slot >= 0 grows the slot count by at most one:
    *     no legitimate slot exceeds max(D0 / stride, 1 + highest slot in index m) + that Mapping count, where D0
    *     is the forced data size before applying. This stops a CRC-valid but bogus slot from growing data hugely.
    *  3. Write the kept Deltas in place and force data. Check that every mapped slot starts inside data and that
    *     the slot count fits an Int.
    *  4. Truncate the active journal's torn tail and force it.
    *  5. With journal m+1: write the map at the end of journal m as index m+1 into the other index file and force
    *     it; then empty journal m and index m. Without it: empty any stale journal or index file, so that one of
    *     each is free for the next checkpoint.
    *
    * Steps 1-2 only read. Step 3 writes absolute bytes, so repeating it is harmless, and until step 5 makes index
    * m+1 valid every rerun sees the same inputs. Step 4 removes only bytes that no rerun replays. Once index m+1 is
    * valid (step 5), a rerun starts from it: data already holds every kept Delta of journal m (forced in step 3), which
    * is exactly what a normal checkpoint guarantees before writing index m+1, and journal m becomes stale. The
    * files emptied in step 5 are ones a rerun ignores.
    */
  def recover(files: FsifFiles, layout: FsifLayout): Recovered =
    // Step 1
    val (indexName, m, _, start) = newestIndex(files, layout)

    val journals = JournalFiles.flatMap: name =>
      load(files, name).flatMap: bytes =>
        JournalFormat.scan(bytes) match
          case JournalFormat.Scan.Empty => None
          case v: JournalFormat.Scan.Valid =>
            if v.seq < m then
              None
            else if v.seq > m + 1 then
              throw new StoreLost(s"$name has seq ${v.seq} but the newest index has seq $m")
            else
              Some(Journal(name, v))
          case JournalFormat.Scan.Corrupt(reason) =>
            JournalFormat.decodeHeader(bytes) match
              case Some((seq, _)) if seq < m => None
              case _ => throw new StoreLost(s"$name is corrupt: $reason")

    val current = journals.filter(_.scan.seq == m) match
      case Vector(j) => j
      case Vector() => throw new StoreLost(s"no journal with seq $m")
      case _ => throw new StoreLost(s"both journals have seq $m")
    val next = journals.find(_.scan.seq == m + 1)

    // Step 2
    val replayed = current.scan.records ++ next.map(_.scan.records).getOrElse(Vector.empty)
    val result = replayChecked(files, layout, start, replayed)
    val checkpointMap = next.map(_ => replay(start, current.scan.records).index)

    // Step 3
    val dataSize = applyDeltas(files, layout, result.apply)
    val slotCount = checkedSlotCount(layout, result.index, dataSize)

    // Step 4
    val active = next.getOrElse(current)
    withFile(files, active.name): f =>
      if f.size > active.scan.validLength then
        f.truncate(active.scan.validLength)
        f.force()

    // Step 5
    val otherIndex = IndexFiles.find(_ != indexName).get
    checkpointMap match
      case Some(map) =>
        withFile(files, otherIndex): f =>
          f.truncate(0)
          f.write(0, IndexFile.encode(m + 1, map))
          f.force()
        truncateToZero(files, current.name)
        truncateToZero(files, indexName)
      case None =>
        val otherJournal = JournalFiles.find(_ != current.name).get
        for name <- Seq(otherJournal, otherIndex) do
          if files.exists(name) && withFile(files, name)(_.size) > 0 then
            truncateToZero(files, name)

    val activeIndex = if checkpointMap.isDefined then otherIndex else indexName
    Recovered(result.index, active.scan.seq, active.scan.salt, activeIndex, active.name, active.scan.validLength,
      active.scan.nextBatch, slotCount)
