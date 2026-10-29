package org.aspen_ddp.aspen.server.store.backend.fsif

final case class FsifLayout(segmentPayloadSize: Int, segmentCount: Int):
  /** Distance in bytes between consecutive segment starts, rounded up to 4096 */
  val stride: Long =
    val raw = SegmentHeader.Size.toLong + segmentPayloadSize
    (raw + 4095L) / 4096L * 4096L

  def slotOffset(slot: Int): Long = slot.toLong * stride
