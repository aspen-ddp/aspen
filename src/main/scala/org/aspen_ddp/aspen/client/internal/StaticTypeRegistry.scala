package org.aspen_ddp.aspen.client.internal

import java.util.UUID

import org.aspen_ddp.aspen.client.RegisteredTypeFactory
import org.aspen_ddp.aspen.client.internal.allocation.{AllocationFinalizationAction, DeletionFinalizationAction}
import org.aspen_ddp.aspen.client.internal.transaction.MissedUpdateFinalizationAction
import org.aspen_ddp.aspen.client.tkvl.{JoinFinalizationAction, KVObjectRootManager, SplitFinalizationAction}

object StaticTypeRegistry {

  private val registry: List[RegisteredTypeFactory] = List(
    KVObjectRootManager,
    SplitFinalizationAction,
    JoinFinalizationAction,
    AllocationFinalizationAction,
    DeletionFinalizationAction,
    MissedUpdateFinalizationAction
  )

  val types: List[(UUID, RegisteredTypeFactory)] = registry.map(t => t.typeUUID -> t)
}
