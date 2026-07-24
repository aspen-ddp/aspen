package org.aspen_ddp.aspen.compute.systemtask

import org.aspen_ddp.aspen.common.objects.Key

import java.util.UUID

/** System-level distributed DurableTask execution service.
 *
 *  This object currently holds only the fixed identifiers and state-object layout used by
 *  Bootstrap. The DurableServiceFactory and service class are added in a later task. */
object SystemTaskExecutorService:

  val ServiceTypeUUID: UUID = UUID.fromString("a1e5c9d4-3b7f-4a21-9c8e-2d6f0b13e7a4")
  val ServiceUUID: UUID     = UUID.fromString("c4d8f2b6-1a09-4e73-8b5d-7f3c9e0a24d1")

  /** Key in the service state object whose value is the encoded Root of the task TKVL
   *  (Key = task UUID -> Value = encoded KeyValueObjectPointer of the task state object). */
  val TaskTreeKey: Key = Key(0)
