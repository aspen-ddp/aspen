package org.aspen_ddp.aspen.client.internal.read

import java.util.UUID

import org.aspen_ddp.aspen.client.{DataObjectRangeState, DataObjectRangesState, ObjectState}
import org.aspen_ddp.aspen.common.{DataBuffer, HLCTimestamp}
import org.aspen_ddp.aspen.common.ida.{AppendableIDA, IDA, IDARestoreError}
import org.aspen_ddp.aspen.common.network.ReadResponse
import org.aspen_ddp.aspen.common.objects.{ByteRange, ByteRanges, ByteRangeToEnd, DataObjectPointer, ObjectRefcount, ObjectRevision, ReadType, SliceTail}
import org.aspen_ddp.aspen.common.store.StoreId

/** Restores the part of a data object returned by a ByteRange, ByteRanges, ByteRangeToEnd or SliceTail read. The restored
  * window is row-aligned and may extend beyond what the caller asked for, so callers trim it.
  */
class DataObjectRangeReader(pointer: DataObjectPointer, ida: IDA, readUUID: UUID, readType: ReadType)
  extends BaseObjectReader[DataObjectPointer, DataObjectStoreState](false, pointer, ida, readUUID):

  private val appendable: AppendableIDA = ida match
    case a: AppendableIDA => a
    case _ => throw new IllegalArgumentException(s"IDA $ida does not support range reads")

  override protected def createObjectState(storeId: StoreId, readTime: HLCTimestamp, cs: ReadResponse.CurrentState): DataObjectStoreState =
    new DataObjectStoreState(storeId, cs.revision, cs.refcount, cs.timestamp, readTime, cs.sizeOnStore, cs.objectData)

  override protected def restoreObject(revision: ObjectRevision, refcount: ObjectRefcount, timestamp: HLCTimestamp,
                                       readTime: HLCTimestamp, matchingStoreStates: List[DataObjectStoreState],
                                       allStoreStates: List[DataObjectStoreState], debug: Boolean): ObjectState =

    val sliceSize = matchingStoreStates.head.sizeOnStore

    readType match
      case ByteRanges(ranges, tail) => restoreRanges(revision, refcount, timestamp, readTime, matchingStoreStates, sliceSize, ranges, tail)
      case _ => restoreSingleRange(revision, refcount, timestamp, readTime, matchingStoreStates, sliceSize)

  /** Mirrors the store-side clipping of a ByteRanges read: [start, end) clamped to [0, sliceSize] */
  private def clipped(start: Long, end: Long, sliceSize: Int): (Int, Int) =
    val s = math.min(math.max(start, 0L), sliceSize.toLong)
    val e = math.max(s, math.min(end, sliceSize.toLong))
    (s.toInt, e.toInt)

  private def restoreRanges(revision: ObjectRevision, refcount: ObjectRefcount, timestamp: HLCTimestamp,
                            readTime: HLCTimestamp, matchingStoreStates: List[DataObjectStoreState],
                            sliceSize: Int, ranges: List[(Int, Int)], tail: Int): ObjectState =
    // The tail piece is always present, even when empty, so Replication still reports the object length
    val pieces = ranges.map((offset, length) => clipped(offset.toLong, offset.toLong + length.toLong, sliceSize)) :+
      clipped(sliceSize.toLong - tail.toLong, sliceSize.toLong, sliceSize)

    // Split each store's concatenated buffer into per-piece segments
    val perStore = matchingStoreStates.flatMap: ss =>
      ss.objectData.map: db =>
        val arr = db.getByteArray
        val splits = pieces.scanLeft(0)((pos, p) => pos + (p._2 - p._1))
        if splits.last != arr.length then
          throw BaseObjectReader.NotRestorable(s"Range restore failed: unexpected response length ${arr.length}")
        ss.storeId.poolIndex -> pieces.indices.map(i => arr.slice(splits(i), splits(i + 1))).toList

    if perStore.size < threshold then
      throw BaseObjectReader.NotRestorable("Below Threshold")

    try
      val restored = pieces.zipWithIndex.map: (p, i) =>
        val segments = perStore.map((idx, parts) => idx -> parts(i))
        appendable.restoreRange(p._1, sliceSize, segments)
      val states = restored.map: range =>
        new DataObjectRangeState(pointer, revision, refcount, timestamp, readTime, ida,
          range.logicalOffset, DataBuffer(range.data), range.objectLength)
      new DataObjectRangesState(pointer, revision, refcount, timestamp, readTime, ida,
        states.init, restored.last.objectLength)
    catch
      case e: IDARestoreError => throw BaseObjectReader.NotRestorable(s"Range restore failed: ${e.getMessage}")

  private def restoreSingleRange(revision: ObjectRevision, refcount: ObjectRefcount, timestamp: HLCTimestamp,
                                 readTime: HLCTimestamp, matchingStoreStates: List[DataObjectStoreState],
                                 sliceSize: Int): ObjectState =
    val sliceStart = readType match
      case SliceTail(n) => sliceSize - math.min(n, sliceSize)
      case ByteRange(offset, _) => math.min(offset, sliceSize)
      case ByteRangeToEnd(offset) => math.min(offset, sliceSize)
      case rt => throw new IllegalArgumentException(s"Read type $rt is not a byte range read")

    val segments = matchingStoreStates.flatMap: ss =>
      ss.objectData.map(db => ss.storeId.poolIndex -> db.getByteArray)

    if segments.size < threshold then
      throw BaseObjectReader.NotRestorable("Below Threshold")

    try
      val range = appendable.restoreRange(sliceStart, sliceSize, segments)
      new DataObjectRangeState(pointer, revision, refcount, timestamp, readTime, ida,
        range.logicalOffset, DataBuffer(range.data), range.objectLength)
    catch
      case e: IDARestoreError => throw BaseObjectReader.NotRestorable(s"Range restore failed: ${e.getMessage}")
