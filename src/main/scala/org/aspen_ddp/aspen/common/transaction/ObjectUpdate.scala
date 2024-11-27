package org.aspen_ddp.aspen.common.transaction

import org.aspen_ddp.aspen.common.DataBuffer
import org.aspen_ddp.aspen.common.objects.ObjectId

case class ObjectUpdate(objectId: ObjectId, data: DataBuffer)
