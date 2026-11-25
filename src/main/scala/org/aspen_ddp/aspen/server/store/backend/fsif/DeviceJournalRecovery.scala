package org.aspen_ddp.aspen.server.store.backend.fsif

import java.util.UUID

/** The device journal fails recovery (spec 4.7 step 1): the device load treats the journal as lost */
final class JournalCorrupt(msg: String) extends Exception(msg)

/** Device-load recovery (spec 4.7 steps 1-2): recovers the ring, then replays into every attached directory.
  *
  * The ring is scanned before any directory is touched, so a corrupt ring writes nothing. Each target's newest index
  * decides it: detached (IndexMeta.None included) needs nothing; undetached and naming another journal has no records
  * here and is left for the caller; undetached and naming this journal is replayed with its attachment's records
  * above its covered serial, in journal order, which writes a detached index covering the ring's tail. Records are
  * matched to a directory only by attachment id; those of no listed directory are ignored. The scan never consults
  * covered serials, so an idle directory whose covered serial is laps behind the ring is simply replayed with
  * nothing.
  *
  * A rerun after a crash is safe: the ring is not written, a replayed directory is detached and so NotAttached, and
  * `FsifRecovery.replayDirectory` is idempotent across crashes inside it.
  */
object DeviceJournalRecovery:
  final case class Target(key: os.Path, files: FsifFiles, layout: FsifLayout)

  enum Outcome:
    /** Replayed this many records (also 0: nothing above its covered serial); its index is now detached */
    case Replayed(records: Int)
    /** The newest index is detached: untouched */
    case NotAttached
    /** The newest index is not detached but names another journal: untouched */
    case ForeignUndetached
    case Lost(err: FsifRecovery.StoreLost)

  /** `start` is where the writer continues (FsifDeviceJournal's `start`): None for a ring with no valid header */
  final case class Result(start: Option[Tail], outcomes: Map[os.Path, Outcome])

  /** @throws JournalCorrupt when the scan is Corrupt; IOExceptions propagate (transient: the device load retries) */
  def recover(journalFiles: FsifFiles, sizing: JournalSizing, targets: Seq[Target]): Result =
    require(targets.map(_.key).distinct.size == targets.size, "duplicate target keys")
    val (batches, start) = DeviceJournalScan.scan(journalFiles, sizing) match
      case Scan.Empty => (Vector.empty, None)
      case Scan.Valid(bs, tail) => (bs, Some(tail))
      case Scan.Corrupt(reason) =>
        throw new JournalCorrupt(s"device journal ${sizing.journalId} is corrupt: $reason")
    val tailSerial = start.map(_.nextSerial - 1).getOrElse(-1L)
    val byAttachment: Map[UUID, Vector[(Long, JournalRecord)]] =
      batches.flatMap(b => b.records.map(r => (r.attachment, (b.serial, r.record)))).groupMap(_._1)(_._2)
    val outcomes = targets.map: t =>
      t.key -> recoverOne(t, byAttachment, sizing.journalId, tailSerial)
    Result(start, outcomes.toMap)

  private def recoverOne(t: Target, byAttachment: Map[UUID, Vector[(Long, JournalRecord)]], journalId: UUID,
                         tailSerial: Long): Outcome =
    try
      val (_, _, meta, _) = FsifRecovery.newestIndex(t.files, t.layout)
      if meta.detached then
        Outcome.NotAttached
      else if meta.journalId != journalId then
        Outcome.ForeignUndetached
      else
        val records = byAttachment.getOrElse(meta.attachment, Vector.empty).collect:
          case (serial, r) if serial > meta.covered => r
        FsifRecovery.replayDirectory(t.files, t.layout, records, journalId, tailSerial)
        Outcome.Replayed(records.size)
    catch case e: FsifRecovery.StoreLost => Outcome.Lost(e)
