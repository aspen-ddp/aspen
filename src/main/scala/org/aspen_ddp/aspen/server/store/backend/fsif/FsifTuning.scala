package org.aspen_ddp.aspen.server.store.backend.fsif

import scala.concurrent.duration.*

import org.aspen_ddp.aspen.server.store.backend.RetryScheduler

/** @param cowThreshold   Whole-object writes larger than this many bytes go through copy-on-write instead of the
  *                       journal
  * @param journalBound   The active journal is checkpointed once it exceeds max(indexFileSize / 2, journalBound)
  * @param unappliedBound Journaled delta bytes awaiting their in-place write above which commit completions wait
  * @param enospcRetry    Interval between retries of I/O held while writes are frozen by a full device
  * @param journalHardLimit The active journal never grows past this (FsifBackend.journalLengthBound): an operation
  *                       whose records could take it further is held, neither completed nor failed, until a
  *                       checkpoint switches journals. Well below FsifRecovery.MaxLoadSize, which recovery cannot load
  * @param readRetryDelays A read failing without evidence of damage (an I/O error or internal exception, not a CRC
  *                       mismatch or invalid header) is retried after each of these before it reports CorruptedObject
  */
final case class FsifTuning(cowThreshold: Int = 64 * 1024,
                            journalBound: Long = 64L << 20,
                            unappliedBound: Long = 64L << 20,
                            enospcRetry: FiniteDuration = 1.second,
                            journalHardLimit: Long = 1L << 30,
                            readRetryDelays: Seq[FiniteDuration] = RetryScheduler.ReadRetryDelays):
  require(journalHardLimit > 0 && journalHardLimit <= FsifRecovery.MaxLoadSize,
    s"journalHardLimit must be in [1, ${FsifRecovery.MaxLoadSize}]: $journalHardLimit")
