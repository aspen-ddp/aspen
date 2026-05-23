package org.aspen_ddp.aspen.client

import org.aspen_ddp.aspen.client.RegisteredTypeFactory
import org.aspen_ddp.aspen.client.internal.allocation.{AllocationFinalizationAction, DeletionFinalizationAction}
import org.aspen_ddp.aspen.client.internal.transaction.MissedUpdateFinalizationAction
import org.aspen_ddp.aspen.client.tkvl.{JoinFinalizationAction, KVObjectRootManager, SplitFinalizationAction}

object TypeFactories:

  val factories: List[RegisteredTypeFactory] = List(
    KVObjectRootManager,
    SplitFinalizationAction,
    JoinFinalizationAction,
    AllocationFinalizationAction,
    DeletionFinalizationAction,
    MissedUpdateFinalizationAction,
  )
  
