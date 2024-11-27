package org.aspen_ddp.aspen.common.transaction

object DataUpdateOperation extends Enumeration {
  val Append: Value    = Value("Append")
  val Overwrite: Value = Value("Overwrite")
}
