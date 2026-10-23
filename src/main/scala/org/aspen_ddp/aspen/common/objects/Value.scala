package org.aspen_ddp.aspen.common.objects

import scala.language.implicitConversions

/** Not an AnyVal: value classes cannot override equals and hashCode, and Value must compare by content */
final case class Value(bytes: Array[Byte]) {

  override def equals(other: Any): Boolean = other match
    case that: Value => java.util.Arrays.equals(bytes, that.bytes)
    case _ => false

  override def hashCode: Int = java.util.Arrays.hashCode(bytes)
}
