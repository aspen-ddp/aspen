package org.aspen_ddp.aspen.common.transaction

import org.aspen_ddp.aspen.common.DataBuffer
import org.aspen_ddp.aspen.common.objects.{Metadata, ObjectId}

case class PreTransactionOpportunisticRebuild(objectId: ObjectId,
                                              requiredMetadata: Metadata,
                                              data: DataBuffer)
