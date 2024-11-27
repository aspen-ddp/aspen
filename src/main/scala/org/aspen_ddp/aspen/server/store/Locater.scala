package org.aspen_ddp.aspen.server.store

import org.aspen_ddp.aspen.common.objects.ObjectId
import org.aspen_ddp.aspen.common.store.StorePointer

case class Locater(objectId: ObjectId, storePointer: StorePointer)
