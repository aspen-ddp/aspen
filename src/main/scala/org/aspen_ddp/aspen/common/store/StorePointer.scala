package org.aspen_ddp.aspen.common.store

import java.nio.ByteBuffer

object StorePointer {
  def decode(encoded: Array[Byte]): StorePointer = {
    val data = new Array[Byte](encoded.length-1)
    val bb = ByteBuffer.wrap(encoded)
    val poolIndex = bb.get()
    bb.get(data)
    StorePointer(poolIndex, data)
  }
}

final case class StorePointer(poolIndex: Byte, data: Array[Byte]) {

  override def toString: String = {
    if (data.length == 0)
      s"($poolIndex,)"
    else if(data.length == 1)
      s"($poolIndex,${ByteBuffer.wrap(data).get()})"
    else if(data.length == 2)
      s"($poolIndex,${ByteBuffer.wrap(data).getShort()})"
    else if(data.length == 4)
      s"($poolIndex,${ByteBuffer.wrap(data).getInt()})"
    else
      super.toString
  }

  def encode(): Array[Byte] = {
    val arr = new Array[Byte](1 + data.length)
    val bb = ByteBuffer.wrap(arr)
    bb.put(poolIndex)
    bb.put(data)
    arr
  }

  def encodedSize(): Int = 1 + data.length

  override def equals(other: Any): Boolean = other match {
    case rhs: StorePointer => poolIndex == rhs.poolIndex && java.util.Arrays.equals(data, rhs.data)
    case _ => false
  }

  override def hashCode(): Int = java.util.Arrays.hashCode(data) ^ poolIndex
}