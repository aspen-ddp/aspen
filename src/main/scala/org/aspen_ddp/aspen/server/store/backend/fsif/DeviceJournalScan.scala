package org.aspen_ddp.aspen.server.store.backend.fsif

/** Where the next batch goes: `file`, at `nextPos`, with serial `nextSerial` under the file's `salt` */
final case class Tail(file: Int, nextPos: Long, nextSerial: Long, salt: Long)

enum Scan:
  /** No valid file header anywhere */
  case Empty
  /** The batches at or above the newest header's oldestNeeded, in serial order, and where the journal continues */
  case Valid(batches: Vector[Batch], tail: Tail)
  case Corrupt(reason: String)

/** Recovery read side of the device journal (spec 4.1 "What a file holds", 4.7 step 1) */
object DeviceJournalScan:
  def fileName(n: Int): String = s"$n.fj"

  private final case class Found(index: Int, header: FileHeader, bytes: Array[Byte])

  private def load(files: FsifFiles, sizing: JournalSizing, n: Int): Option[Found] =
    val name = fileName(n)
    if !files.exists(name) || files.size(name) < DeviceJournalFormat.HeaderSize then
      None
    else
      val f = files.open(name)
      try
        f.force()
        val bytes = f.read(0, f.size.toInt)
        DeviceJournalFormat.decodeHeader(bytes, sizing.journalId).map(h => Found(n, h, bytes))
      finally f.close()

  def scan(files: FsifFiles, sizing: JournalSizing): Scan =
    val found = (0 until sizing.files).flatMap(load(files, sizing, _)).sortBy(_.header.first).toVector
    if found.isEmpty then
      Scan.Empty
    else
      val newest = found.last
      val oldestNeeded = newest.header.oldestNeeded
      if oldestNeeded < found.head.header.first then
        Scan.Corrupt(s"oldest needed batch $oldestNeeded is below the oldest valid file's first ${found.head.header.first}")
      else
        val out = Vector.newBuilder[Batch]
        var failure: Option[String] = None
        var i = 0
        while failure.isEmpty && i < found.size - 1 do
          val f = found(i)
          val last = found(i + 1).header.first - 1
          if last >= oldestNeeded then
            failure = readClosed(f, last, oldestNeeded, sizing, out)
          i += 1
        failure match
          case Some(r) => Scan.Corrupt(r)
          case None => readNewest(newest, oldestNeeded, sizing, out)

  /** A non-newest file: every batch from its first to `last` must be Ok. One below oldestNeeded that cannot be read
    * leaves the position of the needed ones unknown, so it fails too.
    */
  private def readClosed(f: Found, last: Long, oldestNeeded: Long, sizing: JournalSizing,
                         out: scala.collection.mutable.Builder[Batch, Vector[Batch]]): Option[String] =
    var pos = DeviceJournalFormat.HeaderSize
    var serial = f.header.first
    var failure: Option[String] = None
    while failure.isEmpty && serial <= last do
      DeviceJournalFormat.decodeBatchAt(f.bytes, pos, sizing.journalId, f.header.salt) match
        case BatchRead.Ok(b, next) if b.serial == serial =>
          if serial >= oldestNeeded then
            out += b
          pos = next
          serial += 1
        case BatchRead.Ok(b, _) =>
          failure = Some(s"${fileName(f.index)} holds batch ${b.serial} where $serial belongs")
        case BatchRead.Invalid =>
          failure = Some(s"${fileName(f.index)} has no valid batch $serial at $pos")
        case BatchRead.Unparseable(r) =>
          failure = Some(s"${fileName(f.index)} batch at $pos is unparseable: $r")
    failure

  private def readNewest(f: Found, oldestNeeded: Long, sizing: JournalSizing,
                         out: scala.collection.mutable.Builder[Batch, Vector[Batch]]): Scan =
    var pos = DeviceJournalFormat.HeaderSize
    var serial = f.header.first
    var result: Option[Scan] = None
    while result.isEmpty do
      DeviceJournalFormat.decodeBatchAt(f.bytes, pos, sizing.journalId, f.header.salt) match
        case BatchRead.Ok(b, next) if b.serial == serial =>
          if serial >= oldestNeeded then
            out += b
          pos = next
          serial += 1
        case BatchRead.Unparseable(r) =>
          result = Some(Scan.Corrupt(s"${fileName(f.index)} batch at $pos is unparseable: $r"))
        case _ =>
          if laterBatchExists(f, pos, serial, sizing) then
            result = Some(Scan.Corrupt(s"${fileName(f.index)} has batch $serial after an invalid one at $pos"))
          else
            result = Some(Scan.Valid(out.result(), Tail(f.index, pos, serial, f.header.salt)))
    result.get

  private def laterBatchExists(f: Found, pos: Int, serial: Long, sizing: JournalSizing): Boolean =
    var p = pos + 4096
    var found = false
    while !found && p < f.bytes.length do
      DeviceJournalFormat.decodeBatchAt(f.bytes, p, sizing.journalId, f.header.salt) match
        case BatchRead.Ok(b, _) if b.serial == serial => found = true
        case _ => ()
      p += 4096
    found
