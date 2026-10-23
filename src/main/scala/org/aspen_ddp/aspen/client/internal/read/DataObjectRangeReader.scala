package org.aspen_ddp.aspen.client.internal.read

import java.util.UUID

import org.aspen_ddp.aspen.client.{DataObjectRangeState, ObjectState}
import org.aspen_ddp.aspen.common.{DataBuffer, HLCTimestamp}
import org.aspen_ddp.aspen.common.ida.{AppendableIDA, IDA, IDARestoreError}
import org.aspen_ddp.aspen.common.network.ReadResponse
import org.aspen_ddp.aspen.common.objects.{ByteRange, ByteRangeToEnd, DataObjectPointer, ObjectRefcount, ObjectRevision, ReadType, SliceTail}
import org.aspen_ddp.aspen.common.store.StoreId

/** Restores the part of a data object returned by a ByteRange, ByteRangeToEnd or SliceTail read. The restored
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
