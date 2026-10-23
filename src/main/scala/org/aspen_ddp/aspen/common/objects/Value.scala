package org.aspen_ddp.aspen.common.objects

import scala.language.implicitConversions

/** An AnyVal to avoid an allocation per value. Value classes cannot override equals or hashCode, so `==` compares
  * the arrays by reference: compare `bytes` with java.util.Arrays.equals instead.
  */
final case class Value(bytes: Array[Byte]) extends AnyVal
