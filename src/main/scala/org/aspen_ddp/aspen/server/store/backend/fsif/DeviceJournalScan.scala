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

  private final case class Found(index: Int, header: FileHeader)

  /** Reads only the header (4096 bytes) */
  private def loadHeader(files: FsifFiles, sizing: JournalSizing, n: Int): Option[Found] =
    val name = fileName(n)
    if !files.exists(name) || files.size(name) < DeviceJournalFormat.HeaderSize then
      None
    else
      val f = files.open(name)
      try
        f.force()
        DeviceJournalFormat.decodeHeader(f.read(0, DeviceJournalFormat.HeaderSize), sizing.journalId)
          .map(h => Found(n, h))
      finally f.close()

  /** The whole file, read only while it is walked */
  private def loadBytes(files: FsifFiles, f: Found): Array[Byte] =
    val h = files.open(fileName(f.index))
    try
      h.force()
      h.read(0, h.size.toInt)
    finally h.close()

  def scan(files: FsifFiles, sizing: JournalSizing): Scan =
    val found = (0 until sizing.files).flatMap(loadHeader(files, sizing, _)).sortBy(_.header.first).toVector
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
            failure = walk(f, loadBytes(files, f), Some(last), oldestNeeded, sizing, out).left.toOption
          i += 1
        failure match
          case Some(r) => Scan.Corrupt(r)
          case None =>
            walk(newest, loadBytes(files, newest), None, oldestNeeded, sizing, out) match
              case Left(r) => Scan.Corrupt(r)
              case Right((pos, serial)) =>
                Scan.Valid(out.result(), Tail(newest.index, pos, serial, newest.header.salt))

  /** The position of an Ok batch with serial `target` at a 4096 boundary at or after `from` */
  private def findLater(bytes: Array[Byte], from: Int, target: Long, sizing: JournalSizing, salt: Long): Option[Int] =
    var p = from
    var found: Option[Int] = None
    while found.isEmpty && p < bytes.length do
      DeviceJournalFormat.decodeBatchAt(bytes, p, sizing.journalId, salt) match
        case BatchRead.Ok(b, _) if b.serial == target => found = Some(p)
        case _ => ()
      p += 4096
    found

  /** Walks a file's batches from its first. `last` is the last serial a closed file must hold; None for the newest
    * file, which ends at the first position that is not an Ok batch of the next serial. A batch below oldestNeeded is
    * not read: where one fails, the walk resumes at the later boundary holding oldestNeeded. Otherwise a failed batch
    * of a closed file is Corrupt, as is a failed one in the newest file when the next serial exists after it, or an
    * Unparseable one. Right is the position and serial after the last batch.
    */
  private def walk(f: Found, bytes: Array[Byte], last: Option[Long], oldestNeeded: Long, sizing: JournalSizing,
                   out: scala.collection.mutable.Builder[Batch, Vector[Batch]]): Either[String, (Int, Long)] =
    val salt = f.header.salt
    val name = fileName(f.index)
    var pos = DeviceJournalFormat.HeaderSize
    var serial = f.header.first
    var result: Option[Either[String, (Int, Long)]] = None
    while result.isEmpty do
      if last.exists(serial > _) then
        result = Some(Right((pos, serial)))
      else
        DeviceJournalFormat.decodeBatchAt(bytes, pos, sizing.journalId, salt) match
          case BatchRead.Ok(b, next) if b.serial == serial =>
            if serial >= oldestNeeded then
              out += b
            pos = next
            serial += 1
          case read =>
            if serial < oldestNeeded then
              findLater(bytes, pos + 4096, oldestNeeded, sizing, salt) match
                case Some(p) =>
                  pos = p
                  serial = oldestNeeded
                case None =>
                  result = Some(Left(s"$name has no readable batch $oldestNeeded after the invalid one at $pos"))
            else read match
              case BatchRead.Unparseable(r) =>
                result = Some(Left(s"$name batch at $pos is unparseable: $r"))
              case _ if last.isDefined =>
                result = Some(Left(s"$name has no valid batch $serial at $pos"))
              case _ =>
                result =
                  if findLater(bytes, pos + 4096, serial + 1, sizing, salt).isDefined then
                    Some(Left(s"$name has batch ${serial + 1} after an invalid one at $pos"))
                  else
                    Some(Right((pos, serial)))
    result.get
