package org.aspen_ddp.aspen.server.store.backend.fsif

final case class FsifLayout(segmentPayloadSize: Int, segmentCount: Int):
  require(segmentCount > 0 && segmentCount <= FsifLayout.MaxSegmentCount,
    s"segmentCount must be in [1, ${FsifLayout.MaxSegmentCount}]: $segmentCount")
  require(segmentPayloadSize >= 0 && segmentPayloadSize <= FsifLayout.MaxSegmentPayloadSize,
    s"segmentPayloadSize must be in [0, ${FsifLayout.MaxSegmentPayloadSize}]: $segmentPayloadSize")

  /** Distance in bytes between consecutive segment starts, rounded up to 4096 */
  val stride: Long =
    val raw = SegmentHeader.Size.toLong + segmentPayloadSize
    (raw + 4095L) / 4096L * 4096L

  def slotOffset(slot: Int): Long = slot.toLong * stride

object FsifLayout:
  /** The most segments whose index file (IndexFile.sizeFor) stays below 2 GiB, so recovery can read it whole into
    * one array: sizeFor(n) <= Int.MaxValue - 16
    */
  val MaxSegmentCount: Int = ((FsifRecovery.MaxLoadSize - IndexFile.sizeFor(0)) / 20).toInt

  /** The largest payload whose commit (FsifBackend.appendBound) plus a compaction Mapping keeps the sum an Int, with
    * room for a batch's framing. A store's layout must also fit its device journal's file size
    * (FsifBackend.fitsJournal), which is far smaller.
    */
  val MaxSegmentPayloadSize: Int =
    (Int.MaxValue - 16L - FsifBackend.appendBound(0) - FsifBackend.MappingAppendSize).toInt
