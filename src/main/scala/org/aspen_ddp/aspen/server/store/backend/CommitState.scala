package org.aspen_ddp.aspen.server.store.backend

import org.aspen_ddp.aspen.common.DataBuffer
import org.aspen_ddp.aspen.common.objects.{Metadata, ObjectId, ObjectType}
import org.aspen_ddp.aspen.common.store.StorePointer

case class CommitState(objectId: ObjectId,
                       storePointer: StorePointer,
                       metadata: Metadata,
                       objectType: ObjectType.Value,
                       data: DataBuffer,
                       maxSize: Option[Int])
