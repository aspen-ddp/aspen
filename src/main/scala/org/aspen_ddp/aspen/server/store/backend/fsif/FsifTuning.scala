package org.aspen_ddp.aspen.server.store.backend.fsif

import scala.concurrent.duration.*

import org.aspen_ddp.aspen.server.store.backend.RetryScheduler

/** @param cowThreshold   Whole-object writes larger than this many bytes go through copy-on-write instead of the
  *                       journal
  * @param unappliedBound Journaled delta bytes awaiting their in-place write above which commit completions wait
  * @param enospcRetry    Interval between retries of I/O held while writes are frozen by a full device
  * @param readRetryDelays A read failing without evidence of damage (an I/O error or internal exception, not a CRC
  *                       mismatch or invalid header) is retried after each of these before it reports CorruptedObject
  * @param moveRetryDelays After a compaction move's read fails, the move is abandoned and no move starts until the
  *                       next of these has passed; consecutive failures step through them (the last repeating), a
  *                       successful move read starts over
  */
final case class FsifTuning(cowThreshold: Int = 64 * 1024,
                            unappliedBound: Long = 64L << 20,
                            enospcRetry: FiniteDuration = 1.second,
                            readRetryDelays: Seq[FiniteDuration] = RetryScheduler.ReadRetryDelays,
                            moveRetryDelays: Seq[FiniteDuration] = RetryScheduler.ReadRetryDelays :+ 10.seconds):
  require(moveRetryDelays.nonEmpty, "moveRetryDelays must not be empty")
