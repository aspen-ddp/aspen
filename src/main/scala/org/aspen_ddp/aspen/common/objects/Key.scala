package org.aspen_ddp.aspen.common.objects

import java.math.BigInteger
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.util.UUID
import scala.language.implicitConversions
import scala.Conversion

final case class Key(bytes: Array[Byte]) {

  override def equals(other: Any): Boolean = other match
    case that: Key => java.util.Arrays.equals(bytes, that.bytes)
    case _ => false

  override def hashCode: Int = java.util.Arrays.hashCode(bytes)

  override def toString(): String = {
    if (bytes.length == 1)
      s"Key(${bytes(0)})"
    else if (bytes.length == 16) {
      val bb = ByteBuffer.wrap(bytes)
      val m = bb.getLong()
      val l = bb.getLong()
      val uuid = new UUID(m,l)
      s"Key($uuid)"
    }
    else
      s"Key(${org.aspen_ddp.aspen.common.util.printableArray(bytes)})"
  }

  def intValue: Int = new BigInteger(bytes).intValue()
  def longValue: Long = new BigInteger(bytes).longValue()
  def stringValue: String = new String(bytes, StandardCharsets.UTF_8)
}

object Key {
  val AbsoluteMinimum = Key(new Array[Byte](0))

  def apply(number: Long): Key = Key(BigInteger.valueOf(number).toByteArray)
  
  def apply(str: String): Key = Key(str.getBytes(StandardCharsets.UTF_8))
  
  def apply(uuid: UUID): Key = {
    val arr = new Array[Byte](16)
    val bb = ByteBuffer.wrap(arr)
    bb.putLong(uuid.getMostSignificantBits)
    bb.putLong(uuid.getLeastSignificantBits)
    Key(arr)
  }

  given Conversion[String, Key] = str => Key(str.getBytes(StandardCharsets.UTF_8))

  given Conversion[UUID, Key] = uuid => {
    val arr = new Array[Byte](16)
    val bb = ByteBuffer.wrap(arr)
    bb.putLong(uuid.getMostSignificantBits)
    bb.putLong(uuid.getLeastSignificantBits)
    Key(arr)
  }

  given Conversion[Long, Key] = number => Key(BigInteger.valueOf(number).toByteArray())
}
