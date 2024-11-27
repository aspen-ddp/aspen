package org.aspen_ddp.aspen.common.objects

object ReadError extends Enumeration {
  val ObjectMismatch: Value = Value
  val ObjectNotFound: Value = Value
  val StoreNotFound: Value = Value
  val CorruptedObject: Value = Value

}
