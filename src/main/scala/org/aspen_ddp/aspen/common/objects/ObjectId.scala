package org.aspen_ddp.aspen.common.objects

import java.util.UUID
import java.nio.{ByteBuffer, ByteOrder}

object ObjectId:
  def apply(bytes: Array[Byte]): ObjectId =
    val bb = ByteBuffer.wrap(bytes)
    bb.order(ByteOrder.BIG_ENDIAN)
    val msb = bb.getLong()
    val lsb = bb.getLong()
    new ObjectId(UUID(msb, lsb))
    
    
case class ObjectId(uuid: UUID) extends AnyVal:
  def toBytes: Array[Byte] =
    val arr = new Array[Byte](16)
    val bb = ByteBuffer.wrap(arr)
    bb.order(ByteOrder.BIG_ENDIAN)
    bb.putLong(uuid.getMostSignificantBits)
    bb.putLong(uuid.getLeastSignificantBits)
    arr


