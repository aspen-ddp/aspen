package org.aspen_ddp.aspen.server.store.backend.fsif

final case class FsifLayout(segmentPayloadSize: Int, segmentCount: Int):
  require(segmentCount > 0 && segmentCount <= FsifLayout.MaxSegmentCount,
    s"segmentCount must be in [1, ${FsifLayout.MaxSegmentCount}]: $segmentCount")

  /** Distance in bytes between consecutive segment starts, rounded up to 4096 */
  val stride: Long =
    val raw = SegmentHeader.Size.toLong + segmentPayloadSize
    (raw + 4095L) / 4096L * 4096L

  def slotOffset(slot: Int): Long = slot.toLong * stride

object FsifLayout:
  /** The most segments whose index file (IndexFile.sizeFor) stays below 2 GiB, so recovery can read it whole into
    * one array: sizeFor(n) <= Int.MaxValue - 16
    */
  val MaxSegmentCount: Int = ((Int.MaxValue.toLong - 16 - 20) / 20).toInt
