package org.aspen_ddp.aspen.common.store

import org.aspen_ddp.aspen.common.DataBuffer
import org.aspen_ddp.aspen.common.objects.{Metadata, ObjectId, ObjectType}
import org.aspen_ddp.aspen.common.transaction.TransactionId

case class ReadState(objectId: ObjectId,
                     metadata: Metadata,
                     objectType: ObjectType.Value,
                     data: DataBuffer,
                     lockedWriteTransactions: Set[TransactionId])
