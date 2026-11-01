package org.aspen_ddp.aspen.vdisk.tcmu.ring

/** A data-area region: `offset` is from the start of the mapping. */
final case class Iovec(offset: Long, length: Long)

sealed trait RingItem

object RingItem:
  final case class Command(cmdId: Int, cdb: Array[Byte], iovecs: Vector[Iovec]) extends RingItem

  final case class Tmr(tmrType: Int, cmdIds: Vector[Int]) extends RingItem

  /** The entry header is sane, so a CMD entry can still be completed by id, but its body is unusable. */
  final case class Malformed(cmdId: Int, reason: String, dump: String) extends RingItem

/** The mailbox or an entry length is unusable: the ring cannot be walked further. */
final case class RingCorrupt(reason: String, dump: String) extends Exception(reason)
