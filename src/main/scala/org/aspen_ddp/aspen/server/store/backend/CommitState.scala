package org.aspen_ddp.aspen.server.store.backend

import org.aspen_ddp.aspen.common.DataBuffer
import org.aspen_ddp.aspen.common.objects.{Metadata, ObjectId, ObjectType}

case class CommitState(objectId: ObjectId,
                       metadata: Metadata,
                       objectType: ObjectType.Value,
                       data: DataBuffer)
