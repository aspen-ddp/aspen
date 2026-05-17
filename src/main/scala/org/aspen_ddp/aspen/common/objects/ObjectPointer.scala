package org.aspen_ddp.aspen.common.objects

import java.nio.{ByteBuffer, ByteOrder}
import java.util.UUID

import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.util.Varint

sealed abstract class ObjectPointer(
                                     val id: ObjectId,
                                     val poolId: PoolId,
                                     val storePointer: Array[Byte]) {

  import ObjectPointer._

  def toArray: Array[Byte] = encodeToByteArray(this)

  def encodedSize: Int = numBytesNeededToEncode(this)

  def encodeInto(bb: ByteBuffer): Unit = ObjectPointer.encodeInto(bb, this)

  final override def equals(other: Any): Boolean = other match {
    case rhs: ObjectPointer => id == rhs.id && poolId == rhs.poolId &&
      java.util.Arrays.equals(storePointer, rhs.storePointer)
    case _ => false
  }

  final override def hashCode: Int = id.hashCode()

  def objectType: ObjectType.Value

  protected def addExtraToStringContent(sb: StringBuilder): Unit = {}

  def shortString: String = s"$objectType($id)"

  override def toString: String = {
    val sb = new StringBuilder
    sb.append(objectType.toString)
    sb.append("ObjectPointer(")
    sb.append(id.toString)
    sb.append(',')
    sb.append(poolId.toString)
    if (storePointer.nonEmpty) {
      sb.append(",sp=[")
      storePointer.foreach(b => sb.append(f"$b%02x"))
      sb.append(']')
    }
    sb.append(')')
    sb.toString()
  }
}

object ObjectPointer {
  protected val DataObjectPointerCode: Byte = 0
  protected val KeyValueObjectPointerCode: Byte = 1

  val EmptyArray = new Array[Byte](0)

  def apply(arr: Array[Byte]): ObjectPointer = fromArray(arr)

  def fromArray(arr: Array[Byte]): ObjectPointer = fromByteBuffer(ByteBuffer.wrap(arr))

  def fromByteBuffer(bb: ByteBuffer): ObjectPointer = {
    val origOrder = bb.order()
    bb.order(ByteOrder.BIG_ENDIAN)

    val baseSize = Varint.getUnsignedInt(bb)
    val endPos = bb.position() + baseSize
    val typeCode = bb.get()

    def getUUID: UUID = {
      val msb = bb.getLong()
      val lsb = bb.getLong()
      new UUID(msb, lsb)
    }

    val objectUUID = getUUID
    val poolUUID = getUUID

    val spLen = endPos - bb.position()
    val sp = if (spLen > 0) {
      val arr = new Array[Byte](spLen)
      bb.get(arr)
      arr
    } else EmptyArray

    bb.order(origOrder)

    typeCode match {
      case DataObjectPointerCode => new DataObjectPointer(ObjectId(objectUUID), PoolId(poolUUID), sp)
      case KeyValueObjectPointerCode => new KeyValueObjectPointer(ObjectId(objectUUID), PoolId(poolUUID), sp)
    }
  }

  def numBytesNeededToEncode(o: ObjectPointer): Int = {
    val baseLen = 1 + 32 + o.storePointer.length
    Varint.getUnsignedIntEncodingLength(baseLen) + baseLen
  }

  def encodeToByteArray(o: ObjectPointer, numPaddingBytes: Option[Int] = None): Array[Byte] = {
    val totalSize = numBytesNeededToEncode(o) + numPaddingBytes.getOrElse(0)
    val arr = new Array[Byte](totalSize)
    val bb = ByteBuffer.wrap(arr)
    bb.order(ByteOrder.BIG_ENDIAN)
    encodeInto(bb, o)
    arr
  }

  def encodeInto(bb: ByteBuffer, o: ObjectPointer): Unit = {
    val baseSize = 1 + 32 + o.storePointer.length
    val typeCode = o match {
      case _: DataObjectPointer => DataObjectPointerCode
      case _: KeyValueObjectPointer => KeyValueObjectPointerCode
    }

    Varint.putUnsignedInt(bb, baseSize)
    bb.put(typeCode)
    bb.putLong(o.id.uuid.getMostSignificantBits)
    bb.putLong(o.id.uuid.getLeastSignificantBits)
    bb.putLong(o.poolId.uuid.getMostSignificantBits)
    bb.putLong(o.poolId.uuid.getLeastSignificantBits)
    if (o.storePointer.nonEmpty) {
      bb.put(o.storePointer)
    }
  }
}

class DataObjectPointer(id: ObjectId,
                        poolId: PoolId,
                        storePointer: Array[Byte]) extends ObjectPointer(id, poolId, storePointer) {

  override def objectType: ObjectType.Value = ObjectType.Data
}

object DataObjectPointer {

  def apply(arr: Array[Byte]): DataObjectPointer = ObjectPointer.fromArray(arr).asInstanceOf[DataObjectPointer]

  def apply(bb: ByteBuffer): DataObjectPointer = ObjectPointer.fromByteBuffer(bb).asInstanceOf[DataObjectPointer]

  def apply(id: ObjectId,
            poolId: PoolId,
            storePointer: Array[Byte] = ObjectPointer.EmptyArray): DataObjectPointer =
    new DataObjectPointer(id, poolId, storePointer)
}

class KeyValueObjectPointer(
                             id: ObjectId,
                             poolId: PoolId,
                             storePointer: Array[Byte]) extends ObjectPointer(id, poolId, storePointer) {

  override def objectType: ObjectType.Value = ObjectType.KeyValue
}

object KeyValueObjectPointer {

  def apply(arr: Array[Byte]): KeyValueObjectPointer = ObjectPointer.fromArray(arr).asInstanceOf[KeyValueObjectPointer]

  def apply(bb: ByteBuffer): KeyValueObjectPointer = ObjectPointer.fromByteBuffer(bb).asInstanceOf[KeyValueObjectPointer]

  def apply(id: ObjectId,
            poolId: PoolId,
            storePointer: Array[Byte] = ObjectPointer.EmptyArray): KeyValueObjectPointer =
    new KeyValueObjectPointer(id, poolId, storePointer)
}
