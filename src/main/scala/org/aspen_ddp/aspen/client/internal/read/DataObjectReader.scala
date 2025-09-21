package org.aspen_ddp.aspen.client.internal.read

import java.util.UUID

import org.aspen_ddp.aspen.client.{DataObjectState, ObjectState}
import org.aspen_ddp.aspen.common.{DataBuffer, HLCTimestamp}
import org.aspen_ddp.aspen.common.network.ReadResponse
import org.aspen_ddp.aspen.common.objects.{DataObjectPointer, ObjectRefcount, ObjectRevision}
import org.aspen_ddp.aspen.common.store.StoreId

class DataObjectReader(metadataOnly: Boolean, pointer: DataObjectPointer, readUUID: UUID)
  extends BaseObjectReader[DataObjectPointer, DataObjectStoreState](metadataOnly, pointer, readUUID) {

  override protected def createObjectState(storeId:StoreId, readTime: HLCTimestamp, cs: ReadResponse.CurrentState): DataObjectStoreState = {
    new DataObjectStoreState(storeId, cs.revision, cs.refcount, cs.timestamp, readTime, cs.sizeOnStore, cs.objectData)
  }

  override protected def restoreObject(revision:ObjectRevision, refcount: ObjectRefcount, timestamp:HLCTimestamp,
                                       readTime: HLCTimestamp, matchingStoreStates: List[DataObjectStoreState],
                                       allStoreStates: List[DataObjectStoreState], debug: Boolean): ObjectState = {
    
    val sizeOnStore = matchingStoreStates.head.sizeOnStore

    val segments = matchingStoreStates.foldLeft(List[(Byte,DataBuffer)]()) { (l, ss) => ss.objectData match {
      case None => l
      case Some(db) => (ss.storeId.poolIndex -> db) :: l
    }}
    
    if (segments.size >= threshold) {
      val data = pointer.ida.restore(segments)
      DataObjectState(pointer, revision, refcount, timestamp, readTime, sizeOnStore, data)
    }
    else
      throw BaseObjectReader.NotRestorable(s"Below Threshold")
  }
}
