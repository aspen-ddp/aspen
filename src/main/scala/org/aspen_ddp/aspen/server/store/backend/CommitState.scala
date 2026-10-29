package org.aspen_ddp.aspen.server.store.backend

import org.aspen_ddp.aspen.common.DataBuffer
import org.aspen_ddp.aspen.common.objects.{Metadata, ObjectId, ObjectPointer, ObjectType}

/** The state to commit. data is always the full new slice; form says which parts of it changed. */
case class CommitState(pointer: ObjectPointer,
                       metadata: Metadata,
                       objectType: ObjectType.Value,
                       data: DataBuffer,
                       allocating: Boolean = false,
                       form: UpdateForm = UpdateForm.Full):
  def objectId: ObjectId = pointer.id
