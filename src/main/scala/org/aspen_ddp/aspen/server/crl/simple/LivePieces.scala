package org.aspen_ddp.aspen.server.crl.simple

import scala.collection.mutable

/** The serial numbers of the entries holding the pieces (a txd, an object update, a stub's anchor) that live records'
  * newest static records name, counted per serial. Its oldest serial is the CRL's oldestEntryNeeded: recovery walks
  * back to it and reads pieces only from the entries it walked (Recovery.entryHolding), so no piece a durable static
  * record names may lie in an older entry. Owned by the CRL thread.
  *
  * A serial is added when the entry writing its piece builds its buffers (LogEntry.createEntryBuffers), and removed
  * when the entry holding the record's next static record is built (the piece was relocated or dropped) or the record
  * is deleted (SimpleCRL) */
final class LivePieces:
  private val counts = mutable.TreeMap[Long, Int]()

  def add(serial: Long): Unit =
    counts.update(serial, counts.getOrElse(serial, 0) + 1)

  /** A serial no piece holds is a fault in the accounting, which could unpin another piece: the CRL dies of it */
  def remove(serial: Long): Unit =
    counts.get(serial) match
      case None => throw IllegalArgumentException(s"No live piece is in entry $serial")
      case Some(1) => counts.remove(serial)
      case Some(n) => counts.update(serial, n - 1)

  def oldest: Option[Long] = counts.headOption.map(_._1)
