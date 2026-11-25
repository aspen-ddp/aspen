package org.aspen_ddp.aspen.server.store.backend.fsif

import java.io.IOException
import java.util.UUID

/** Store initialization and crash recovery against the device journal (spec 4.7), in two parts: `replayDirectory` runs
  * at device load for each directory whose newest index is attached to the device's journal and not detached, and
  * `openDirectory` runs at every store open.
  */
object FsifRecovery:
  final class StoreLost(msg: String) extends org.aspen_ddp.aspen.server.store.backend.BackendStateLost(msg)

  /** A store directory ready to attach: `activeIndex` holds index `seq` */
  final case class Opened(index: IndexMap, seq: Long, activeIndex: String, slotCount: Int)

  val DataFile = "data"
  val IndexFiles: Vector[String] = Vector("index.a", "index.b")
  /** Every FSIF file of a store directory, indexes first (the device journal holds its records) */
  val AllFiles: Vector[String] = IndexFiles :+ DataFile

  /** `load` reads a file whole into one array, so a larger index loses the store (StoreLost). No index the backend
    * writes is larger: at most IndexFile.sizeFor(FsifLayout.MaxSegmentCount).
    */
  val MaxLoadSize: Long = Int.MaxValue - 16L

  /** Every FSIF file is absent or zero-length: the directory holds no FSIF store. FsifBackend.open treats it as lost,
    * since a store directory exists only once its store was created (StoreCreation)
    */
  def isBlank(files: FsifFiles): Boolean =
    AllFiles.forall(name => !files.exists(name) || files.size(name) == 0)

  private def withFile[A](files: FsifFiles, name: String)(f: FsifFile => A): A =
    val file = files.open(name)
    try f(file)
    finally file.close()

  private def truncateToZero(files: FsifFiles, name: String): Unit =
    withFile(files, name): f =>
      f.truncate(0)
      f.force()

  /** Fresh store: empty data and index.a = (seq 0, IndexMeta.None, empty map), each forced. The index is written
    * last, so a valid index implies the rest of the store exists. On a reused directory an old index.a is emptied
    * first, so a crash part way through can never pair it with emptied or rewritten files. index.b is left absent (or
    * emptied, if a previous store left it behind). Run in StoreCreation's scratch directory, which a failure or crash
    * discards, so nothing ever opens a partly initialized store.
    */
  def initialize(files: FsifFiles, layout: FsifLayout): Unit =
    if files.exists(IndexFiles(0)) then
      truncateToZero(files, IndexFiles(0))
    if files.exists(IndexFiles(1)) then
      truncateToZero(files, IndexFiles(1))
    truncateToZero(files, DataFile)
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
    * transferred-in or newly created directory has no records here), or be attached to `journalId` under an
    * attachment for which `replayFree` holds: one the running journal knows needs no replay (FsifDeviceJournal's
    * `needsReplay` is false: an earlier open of this lifetime wrote its attach index and failed before or without
    * appending). Any other undetached index naming `journalId` was not replayed at device load, which is a bug, not a
    * loss: it throws an IOException so the load retries.
    */
  def openDirectory(files: FsifFiles, layout: FsifLayout, journalId: UUID,
                    replayFree: UUID => Boolean = _ => false): Opened =
    val (indexName, seq, meta, index) = newestIndex(files, layout)
    if !meta.detached && meta.journalId == journalId && !replayFree(meta.attachment) then
      throw new IOException(s"$indexName (seq $seq) is attached to journal $journalId (attachment " +
        s"${meta.attachment}) but was not replayed at device load")
    val slotCount = checkedSlotCount(layout, index, forcedDataSize(files))
    Opened(index, seq, indexName, slotCount)
