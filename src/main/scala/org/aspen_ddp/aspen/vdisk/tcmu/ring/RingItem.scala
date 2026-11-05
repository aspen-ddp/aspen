package org.aspen_ddp.aspen.vdisk.tcmu.ring

/** A data-area region: `offset` is from the start of the mapping. */
final case class Iovec(offset: Long, length: Long)

sealed trait RingItem

object RingItem:
  final case class Command(cmdId: Int, cdb: Array[Byte], iovecs: Vector[Iovec]) extends RingItem

  final case class Tmr(tmrType: Int, cmdIds: Vector[Int]) extends RingItem

  /** The entry header is sane but its body is unusable. `completable` is true for a CMD entry, which must still be
    * completed by `cmdId`; a TMR-derived one has no meaningful id and is never completed.
    */
  final case class Malformed(cmdId: Int, reason: String, dump: String, completable: Boolean) extends RingItem

/** The mailbox or an entry length is unusable: the ring cannot be walked further. */
final case class RingCorrupt(reason: String, dump: String) extends Exception(reason)
