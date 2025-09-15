package org.aspen_ddp.aspen.common.objects

import java.nio.{ByteBuffer, ByteOrder}
import java.util.UUID

import org.aspen_ddp.aspen.common.DataBuffer
import org.aspen_ddp.aspen.common.transaction.TransactionId
import scala.language.implicitConversions

object AllocationRevisionGuard {
  def deserialize(db: DataBuffer): AllocationRevisionGuard = {
    val bb = db.asReadOnlyBuffer()
    bb.get() match {
      case 0 =>
        val ptr = ObjectPointer.fromByteBuffer(bb)
        val msb = bb.getLong()
        val lsb = bb.getLong()
        val rev = ObjectRevision(TransactionId(new UUID(msb, lsb)))

        ObjectRevisionGuard(ptr, rev)
      case 1 =>
        val ptr = KeyValueObjectPointer(bb)
        val msb = bb.getLong()
        val lsb = bb.getLong()
        val rev = ObjectRevision(TransactionId(new UUID(msb, lsb)))
        val klen = bb.remaining()
        val karr = new Array[Byte](klen)
        bb.get(karr)

        KeyRevisionGuard(ptr, Key(karr), rev)

      case _ => throw new Exception("Unexpected Error")
    }
  }
}

sealed abstract class AllocationRevisionGuard {
  def serialize(): DataBuffer
  val pointer: ObjectPointer
}

case class ObjectRevisionGuard( pointer: ObjectPointer,
                                requiredRevision: ObjectRevision) extends AllocationRevisionGuard {
  def serialize(): DataBuffer = {
    val arr = new Array[Byte](1 + pointer.encodedSize + 16)
    val bb = ByteBuffer.wrap(arr)
    bb.put(0.asInstanceOf[Byte])
    pointer.encodeInto(bb)
    bb.putLong(requiredRevision.lastUpdateTxUUID.getMostSignificantBits)
    bb.putLong(requiredRevision.lastUpdateTxUUID.getLeastSignificantBits)
    arr
  }
}

case class KeyRevisionGuard(
                           pointer: KeyValueObjectPointer,
                           key: Key,
                           keyRevision: ObjectRevision
                           ) extends AllocationRevisionGuard {

  def serialize(): DataBuffer = {
    val arr = new Array[Byte](1 + pointer.encodedSize + 16 + key.bytes.length)
    val bb = ByteBuffer.wrap(arr)
    bb.order(ByteOrder.BIG_ENDIAN)
    bb.put(1.asInstanceOf[Byte])
    pointer.encodeInto(bb)
    bb.putLong(keyRevision.lastUpdateTxUUID.getMostSignificantBits)
    bb.putLong(keyRevision.lastUpdateTxUUID.getLeastSignificantBits)
    bb.put(key.bytes)
  }
}
