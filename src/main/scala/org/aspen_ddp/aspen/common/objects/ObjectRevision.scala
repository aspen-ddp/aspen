package org.aspen_ddp.aspen.common.objects

import java.nio.ByteBuffer
import java.util.UUID

import org.aspen_ddp.aspen.common.transaction.TransactionId

/** Object Revisions are set to the UUID of the transaction that last updated them
  *
  */
final class ObjectRevision(val lastUpdateTxUUID: UUID) extends AnyVal {
  override def toString: String = lastUpdateTxUUID.toString

  def toArray: Array[Byte] = {
    val arr = new Array[Byte](16)
    encodeInto(ByteBuffer.wrap(arr))
    arr
  }

  def encodeInto(bb: ByteBuffer): ByteBuffer = {
    bb.putLong(lastUpdateTxUUID.getMostSignificantBits)
    bb.putLong(lastUpdateTxUUID.getLeastSignificantBits)
  }
}

object ObjectRevision {
  def apply(lastUpdateTxId: TransactionId): ObjectRevision = new ObjectRevision(lastUpdateTxId.uuid)

  def apply(arr: Array[Byte]): ObjectRevision = {
    val bb = ByteBuffer.wrap(arr)
    ObjectRevision(bb)
  }

  def apply(bb: ByteBuffer): ObjectRevision = {
    val msb = bb.getLong()
    val lsb = bb.getLong()
    new ObjectRevision(new UUID(msb, lsb))
  }

  val Null = ObjectRevision(TransactionId(new UUID(0,0)))

  val EncodedSize: Int = 16
}
