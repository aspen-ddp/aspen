package org.aspen_ddp.aspen.common.objects

import java.nio.ByteBuffer

case class ObjectRefcount(updateSerial: Int, count: Int) {
  def update(newCount: Int) = ObjectRefcount(updateSerial+1, newCount)

  def increment(): ObjectRefcount = ObjectRefcount(updateSerial+1, count+1)
  def decrement(): ObjectRefcount = ObjectRefcount(updateSerial+1, count-1)
  def setCount(newCount: Int): ObjectRefcount = ObjectRefcount(updateSerial+1, newCount)

  def toArray: Array[Byte] = {
    val arr = new Array[Byte](8)
    encodeInto(ByteBuffer.wrap(arr))
    arr
  }

  def encodeInto(bb: ByteBuffer): ByteBuffer = {
    bb.putInt(updateSerial)
    bb.putInt(count)
  }
}

object ObjectRefcount {
  val EncodedSize: Int = 8

  def apply(arr: Array[Byte]): ObjectRefcount = ObjectRefcount(ByteBuffer.wrap(arr))

  def apply(bb: ByteBuffer): ObjectRefcount = {
    val updateSerial = bb.getInt()
    val count = bb.getInt()
    new ObjectRefcount(updateSerial, count)
  }
}
