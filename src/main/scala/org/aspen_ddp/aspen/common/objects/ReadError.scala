package org.aspen_ddp.aspen.common.objects

object ReadError extends Enumeration {
  val ObjectMismatch: Value = Value
  val ObjectNotFound: Value = Value
  val StoreNotFound: Value = Value
  val CorruptedObject: Value = Value
  /** The store voted commit on a transaction allocating the object and has not applied it yet. Not an absence: a
    * committed allocation's voters answer this until they apply it, so `InvalidObject.provesDeletion` does not count it
    */
  val AllocationPending: Value = Value

}
