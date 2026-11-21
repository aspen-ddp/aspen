package org.aspen_ddp.aspen.server.crl.simple

import org.aspen_ddp.aspen.server.{DurableFiles, FilePreallocation}
import org.aspen_ddp.aspen.server.crl.{CrlSizingMismatch}
import scribe.Logging

import java.util.UUID
import java.util.concurrent.atomic.AtomicBoolean

/** The files of a CRL directory: the SizingFile and the stream files. Their creation, the check that an existing
  * directory is the one its SizingFile records, and the removal of a growth that crashed before its commit. All
  * stateless: SimpleCRL.apply and SimpleCRL.create drive them. SimpleCRL re-exports `Sizing` and `SizingFile` */
object CrlDirectory extends Logging:

  /** The CRL's own record of its sizing and id, written durably when it is created, before any stream file. The
   *  sizing is fixed then: an existing CRL is always opened with the sizing it recorded, whatever it is asked for, so
   *  a change of the device config or of the code defaults cannot make it unopenable */
  val SizingFile = "sizing"

  /** The sizing a CRL was created with, and its id: a random UUID chosen then, which every entry header bears
   *  (LogEntry). Recovery takes only headers bearing it for entries, so object data in the streams, which nobody
   *  outside the device can make bear an id it never sees, cannot pass for one.
   *
   *  `ring` is the order the streams are written in: rotation and recovery's next-stream check follow it, not the
   *  stream numbers. Stream `<n>.crl` exists for each `n` in `0 until ring.size`; a growth inserts a new number after
   *  the active stream, so the ring need not be in numeric order. Rewriting this file is a growth's commit point */
  case class Sizing(ring: Vector[StreamId], maxSizeInBytes: Long, crlId: UUID):
    def numStreams: Int = ring.size

    /** The stream written after `s` */
    def successor(s: StreamId): StreamId = StreamId.successor(ring, s)

    def encode: String = s"num-streams: $numStreams\nmax-size-in-bytes: $maxSizeInBytes\ncrl-id: $crlId\n" +
      s"ring: ${ring.map(_.number).mkString(",")}\n"

  object Sizing:
    /** A sizing whose ring is in numeric order, as a CRL is created */
    def inOrder(numStreams: Int, maxSizeInBytes: Long, crlId: UUID): Sizing =
      Sizing(Vector.tabulate(numStreams)(StreamId(_)), maxSizeInBytes, crlId)

    /** None if `text` is not a sizing file's content, or its ring is not a permutation of `0 until num-streams` */
    def decode(text: String): Option[Sizing] =
      val fields = text.linesIterator.map(_.split(":", 2)).collect:
        case Array(k, v) => k.trim -> v.trim
      .toMap
      for
        n <- fields.get("num-streams").flatMap(_.toIntOption)
        m <- fields.get("max-size-in-bytes").flatMap(_.toLongOption)
        id <- fields.get("crl-id").flatMap(v => scala.util.Try(UUID.fromString(v)).toOption)
        numbers <- fields.get("ring").map(_.split(",", -1).toVector.map(_.trim.toIntOption))
        if numbers.forall(_.isDefined) && numbers.flatten.sorted == (0 until n)
      yield Sizing(numbers.flatten.map(StreamId(_)), m, id)


  private[simple] def streamFiles(streamsDir: os.Path): Set[os.Path] =
    if os.isDir(streamsDir) then os.list(streamsDir).filter(_.ext == "crl").toSet else Set[os.Path]()

  /** The sizing `streamsDir`'s CRL recorded. A directory with neither a SizingFile nor stream files is a new CRL: it
    * records the requested sizing and a new random id first. A SizingFile with no stream files is a creation that
    * crashed before them: its sizing and id stand. Stream files without a readable SizingFile throw CrlSizingMismatch */
  private[simple] def recordedSizing(streamsDir: os.Path, numStreams: Int, maxSizeInBytes: Long,
                             ringForTest: Option[Vector[StreamId]]): Sizing =
    val file = streamsDir / SizingFile
    if os.exists(file) then
      val recorded = Sizing.decode(os.read(file)).filter(s => s.numStreams >= 3 && s.maxSizeInBytes % 4096 == 0)
        .getOrElse:
          throw CrlSizingMismatch(streamsDir, s"its $SizingFile file is unreadable")
      // A ring grown past the configured count is normal (requestGrowth): only a smaller ring or another stream
      // size departs from the config
      if recorded.numStreams < numStreams || recorded.maxSizeInBytes != maxSizeInBytes then
        logger.warn(s"The CRL in $streamsDir was created with ${recorded.numStreams} streams of " +
                    s"${recorded.maxSizeInBytes} bytes, not the $numStreams of $maxSizeInBytes configured. " +
                    s"Opening it with the sizing it was created with")
      recorded
    else if streamFiles(streamsDir).nonEmpty then
      throw CrlSizingMismatch(streamsDir, s"it holds stream files but no $SizingFile file")
    else
      // The admission limit, (n − 2) · S/2 − S/16 − H · (n − 1), needs a third stream (crash-recovery-log 6.1). The proof that every
      // entry is written rests on whole 4 KiB pages: S and every entry are multiples of 4096 (S1)
      require(numStreams >= 3, s"A CRL needs at least 3 streams, not $numStreams")
      require(maxSizeInBytes % 4096 == 0, s"A CRL's stream size must be a multiple of 4096, not $maxSizeInBytes")
      val created = ringForTest match
        case None => Sizing.inOrder(numStreams, maxSizeInBytes, UUID.randomUUID())
        case Some(ring) =>
          require(ring.map(_.number).sorted == (0 until numStreams), s"$ring is not a ring of $numStreams streams")
          Sizing(ring, maxSizeInBytes, UUID.randomUUID())
      DurableFiles.write(file, created.encode)
      created

  private val warnedCopyOnWrite = new AtomicBoolean(false)

  /** Creates the stream files of a new CRL, preallocated at their full size (FilePreallocation), so no CRL write ever
    * allocates a block and a full device cannot fail one with ENOSPC (crash-recovery-log 5.1). Each is written as
    * `<name>.tmp` and renamed into place, and the directory is forced once all are: only then does the writer start.
    * So a crashed creation leaves each listed stream at most once, as `<n>.crl` or as `<n>.crl.tmp`, and no other
    * `.crl.tmp`; it wrote no entry, and it starts over. It never leaves one under `mustExist`: a CRL is created in
    * place only on a device holding no stores, and the replacement of a lost CRL on a device that holds stores is
    * created apart (create, in CrashRecoveryLog.replacementCrlDir) and renamed into place once finished. Any other
    * `.crl.tmp` (a stray beside its stream, an unlisted name, or any under `mustExist`) is damage beside a CRL that
    * may hold entries: CrlSizingMismatch, and nothing is removed.
    * A copy-on-write file system allocates on every overwrite, which no preallocation prevents: warned once. */
  private[simple] def createStreamsIfNew(streamsDir: os.Path, streams: List[os.Path], maxSizeInBytes: Long,
                                 mustExist: Boolean): Unit =
    def tmpOf(f: os.Path): os.Path = f / os.up / s"${f.last}.tmp"
    val leftoverTmps = os.list(streamsDir).filter(_.last.endsWith(".crl.tmp"))
    if leftoverTmps.nonEmpty then
      val present = streamFiles(streamsDir)
      val unfinishedCreation = !mustExist && leftoverTmps.forall: t =>
        val stream = streamsDir / t.last.stripSuffix(".tmp")
        streams.contains(stream) && !present.contains(stream)
      if !unfinishedCreation then
        throw CrlSizingMismatch(streamsDir, s"it holds ${leftoverTmps.map(_.last).sorted.mkString(", ")} beside " +
          s"${present.map(_.last).toList.sorted.mkString(", ")}: not an unfinished creation " +
          s"${if mustExist then "(the device holds stores)" else s"of its ${streams.size} streams"}")
      logger.warn(s"The creation of the CRL in $streamsDir did not finish. Creating its stream files again")
      (leftoverTmps ++ present).foreach(os.remove(_))
    if streamFiles(streamsDir).isEmpty then
      if FilePreallocation.isCopyOnWrite(streamsDir) && warnedCopyOnWrite.compareAndSet(false, true) then
        logger.warn(s"The CRL in $streamsDir is on a copy-on-write file system: its writes allocate, so a full " +
                    s"device can stop it. Use a file system that overwrites in place, or disable copy-on-write " +
                    s"for the directory (chattr +C)")
      streams.foreach(f => FilePreallocation.preallocate(tmpOf(f), maxSizeInBytes))
      streams.foreach(f => os.move(tmpOf(f), f, atomicMove = true))
      DurableFiles.syncDirectory(streamsDir)

  /** A growth creates stream `<ringSize>` as `<ringSize>.crl.tmp`, renames it to `<ringSize>.crl`, and only then
    * commits it by rewriting the SizingFile with the new ring. Either file beside a ring that does not list it is a
    * growth that crashed before its commit: it holds no entry, and is removed. A growth runs only on a CRL whose
    * `listed` streams all exist, so a leftover beside any other set of files is damage, not a crash: it is
    * CrlSizingMismatch, and nothing is removed. Removing it there could leave no stream files, and a new, empty CRL
    * would be created under the device's stores */
  private[simple] def removeUncommittedGrowth(streamsDir: os.Path, listed: Set[os.Path]): Unit =
    val ringSize = listed.size
    val leftovers = List(s"$ringSize.crl", s"$ringSize.crl.tmp").map(streamsDir / _).filter(os.exists(_))
    if leftovers.nonEmpty then
      val others = os.list(streamsDir).filter(f => f.ext == "crl" || f.last.endsWith(".crl.tmp")).toSet --
        leftovers -- listed
      if others.nonEmpty || !listed.forall(os.exists(_)) then
        throw CrlSizingMismatch(streamsDir, s"it holds ${leftovers.map(_.last).mkString(", ")}, an uncommitted " +
          s"growth, beside ${(streamFiles(streamsDir) -- leftovers).map(_.last).toList.sorted.mkString(", ")}, " +
          s"not the $ringSize streams its ring lists")
      leftovers.foreach: f =>
        logger.warn(s"The CRL in $streamsDir holds ${f.last}, a growth that never committed. Removing it")
        os.remove(f)
      DurableFiles.syncDirectory(streamsDir)

  /** A directory with no stream files is a new CRL. Otherwise its files must be exactly `expected`, each
    * `maxSizeInBytes` long (createStreamsIfNew creates them at that size) */
  private[simple] def checkSizing(streamsDir: os.Path, expected: Set[os.Path], maxSizeInBytes: Long): Unit =
    val present = streamFiles(streamsDir)
    if present.nonEmpty then
      if present != expected then
        throw CrlSizingMismatch(streamsDir, s"found ${present.map(_.last).toList.sorted.mkString(", ")}, expected " +
          s"${expected.size} streams")
      present.foreach: f =>
        if os.size(f) != maxSizeInBytes then
          throw CrlSizingMismatch(streamsDir, s"${f.last} is ${os.size(f)} bytes, expected $maxSizeInBytes")
