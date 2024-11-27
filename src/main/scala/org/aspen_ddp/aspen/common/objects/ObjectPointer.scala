package org.aspen_ddp.aspen.common.objects

import java.nio.{ByteBuffer, ByteOrder}
import java.util.UUID

import org.aspen_ddp.aspen.common.ida.IDA
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.{StoreId, StorePointer}
import org.aspen_ddp.aspen.common.util.Varint
import org.aspen_ddp.aspen.server.store.Locater

sealed abstract class ObjectPointer(
                                     val id: ObjectId,
                                     val poolId: PoolId,
                                     val size: Option[Int],
                                     val ida: IDA,
                                     val storePointers: Array[StorePointer]) {

  import ObjectPointer._

  // Require that storePointers is a sorted by pool index
  require(storePointers.zip(storePointers.sortBy(sp => sp.poolIndex)).forall(t => t._1 == t._2))

  /** ObjectPointers are self-describing in terms of size. When given a buffer/array to decode a pointer from, they will consume
    *  exactly the number of bytes necessary to decode one pointer. Separate size encodings are not required.
    */
  def toArray: Array[Byte] = encodeToByteArray(this)

  /** ObjectPointers are self-describing in terms of size. When given a buffer/array to decode a pointer from, they will consume
    *  exactly the number of bytes necessary to decode one pointer. Separate size encodings are not required.
    */
  def encodedSize: Int = numBytesNeededToEncode(this)

  /** ObjectPointers are self-describing in terms of size. When given a buffer/array to decode a pointer from, they will consume
    *  exactly the number of bytes necessary to decode one pointer. Separate size encodings are not required.
    */
  def encodeInto(bb: ByteBuffer): Unit = ObjectPointer.encodeInto(bb, this)

  final override def equals(other: Any): Boolean = other match {
    case rhs: ObjectPointer => id == rhs.id && poolId == rhs.poolId && size == rhs.size &&
      ida == rhs.ida && java.util.Arrays.equals(storePointers.asInstanceOf[Array[Object]], rhs.storePointers.asInstanceOf[Array[Object]])
    case _ => false
  }

  final override def hashCode: Int = id.hashCode()

  def getStorePointer(storeId: StoreId): Option[StorePointer] = if (storeId.poolId == poolId) {
    storePointers.find(sp => sp.poolIndex == storeId.poolIndex)
  } else
    None

  def getStoreLocater(storeId: StoreId): Option[Locater] = getStorePointer(storeId).map { sp =>
    Locater(id, sp)
  }

  def hostingStores: List[StoreId] = storePointers.iterator.map(sp => StoreId(poolId, sp.poolIndex)).toList

  def getEncodedDataIndexForStore(storeId: StoreId): Option[Int] = {
    if (storeId.poolId != poolId)
      return None

    var result: Option[Int] = None

    for (i <- storePointers.indices)
      if (storePointers(i).poolIndex == storeId.poolIndex)
        result = Some(i)

    result
  }

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
    sb.append(',')
    sb.append(size.toString)
    sb.append(',')
    sb.append(ida.toString)
    sb.append(',')
    addExtraToStringContent(sb)
    sb.append('[')
    storePointers.foreach { sp =>
      sb.append(sp.toString)
      sb.append(',')
    }
    sb.append(']')
    sb.toString()
  }
}

object ObjectPointer {
  protected val DataObjectPointerCode: Byte = 0
  protected val KeyValueObjectPointerCode: Byte = 1

  def apply(arr: Array[Byte]): ObjectPointer = fromArray(arr)

  val EmptyArray = new Array[Byte](0)

  def bytesNeededForBits(numBits: Int): Int = if (numBits <= 8) 1 else {
    if ( numBits % 8 == 0 )
      numBits / 8
    else
      (numBits / 8) + 1
  }

  def fromArray(arr: Array[Byte]): ObjectPointer = fromByteBuffer(ByteBuffer.wrap(arr))

  def fromByteBuffer(bb: ByteBuffer): ObjectPointer = {
    val origOrder = bb.order()
    bb.order(ByteOrder.BIG_ENDIAN) // ensure big-endian

    val baseSize = Varint.getUnsignedInt(bb)
    val endPos = bb.position() + baseSize
    val typeCode = bb.get()

    def getUUID: UUID = {
      val msb = bb.getLong()
      val lsb = bb.getLong()
      new UUID(msb,lsb)
    }
    val objectUUID = getUUID
    val poolUUID = getUUID
    val rawSize = Varint.getUnsignedInt(bb)
    val ida = IDA.deserializeIDAType(bb)
    val indexMaskLen = bb.get()
    val indexMask = new Array[Byte](indexMaskLen)
    bb.get(indexMask)

    val spList = Range(0, indexMaskLen*8).foldLeft(List[StorePointer]()) { (l, idx) =>
      val byte = idx / 8
      val bit = idx % 8
      val thisStore = indexMask(byte) & (1 << bit).asInstanceOf[Byte]

      if (thisStore != 0) {
        val spArr = if (bb.position() < endPos) {
          val len = Varint.getUnsignedInt(bb)
          val spArr = new Array[Byte](len)
          bb.get(spArr)
          spArr
        } else
          EmptyArray

        StorePointer(idx.asInstanceOf[Byte], spArr) :: l
      } else
        l
    }

    val spArray = spList.reverse.toArray

    val size = if (rawSize == 0) None else Some(rawSize)

    // reset order to whatever it was originally
    bb.order(origOrder)

    typeCode match {
      case DataObjectPointerCode => new DataObjectPointer(ObjectId(objectUUID), PoolId(poolUUID), size, ida, spArray)
      case KeyValueObjectPointerCode => KeyValueObjectPointer(ObjectId(objectUUID), PoolId(poolUUID), size, ida, spArray)
    }
  }

  def baseNumBytesNeededToEncode(o: ObjectPointer): Int = {
    val sizeLen = Varint.getUnsignedIntEncodingLength(o.size.getOrElse(0))

    val idaLen = o.ida.getSerializedIDATypeLength

    val indexMaskLen = bytesNeededForBits(o.storePointers(o.storePointers.length-1).poolIndex)

    val pointerDataLen = if (o.storePointers.forall( sp => sp.data.length == 0 )) 0 else {
      o.storePointers.foldLeft(0)( (accum, sp) => accum + Varint.getUnsignedIntEncodingLength(sp.data.length) + sp.data.length)
    }

    1 + 16*2 + sizeLen + idaLen + 1 + indexMaskLen + pointerDataLen
  }

  def numBytesNeededToEncode(o: ObjectPointer): Int = {

    val baseLen = baseNumBytesNeededToEncode(o)

    Varint.getUnsignedIntEncodingLength(baseLen) + baseLen
  }

  /** Creates a new array containing the encoded representation of the object pointer.
    *
    * If numPaddingBytes is provided, that many extra bytes will be allocated for the array and
    * left unused after the object is encoded. This is primarily intended to allow extra data
    * to be easily saved alongside the encoded object pointer.
    */
  def encodeToByteArray(o: ObjectPointer, numPaddingBytes: Option[Int]=None): Array[Byte] = {

    val totalSize = numBytesNeededToEncode(o) + numPaddingBytes.getOrElse(0)
    val arr = new Array[Byte](totalSize)
    val bb = ByteBuffer.wrap(arr)
    bb.order(ByteOrder.BIG_ENDIAN) // ensure big-endian

    encodeInto(bb, o)

    arr
  }

  def encodeInto(bb: ByteBuffer, o: ObjectPointer): Unit = {
    val baseSize = baseNumBytesNeededToEncode(o)
    val indexMaskLen = bytesNeededForBits(o.storePointers(o.storePointers.length-1).poolIndex)

    val indexMask = new Array[Byte](indexMaskLen)

    o.storePointers.foreach { sp =>
      val byte = sp.poolIndex / 8
      val bit = sp.poolIndex % 8
      indexMask(byte) = (indexMask(byte) | 1 << bit).asInstanceOf[Byte]
    }

    val pointerDataLen = if (o.storePointers.forall( sp => sp.data.length == 0 )) 0 else {
      o.storePointers.foldLeft(0)( (accum, sp) => accum + Varint.getUnsignedIntEncodingLength(sp.data.length) + sp.data.length)
    }

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
    Varint.putUnsignedInt(bb, o.size.getOrElse(0))
    o.ida.serializeIDAType(bb)
    bb.put(indexMaskLen.asInstanceOf[Byte])
    bb.put(indexMask)
    if (pointerDataLen != 0) {
      o.storePointers.foreach { sp =>
        Varint.putUnsignedInt(bb, sp.data.length)
        bb.put(sp.data)
      }
    }
  }
}

class DataObjectPointer( id: ObjectId,
                         poolId: PoolId,
                         size: Option[Int],
                         ida: IDA,
                         storePointers: Array[StorePointer]) extends ObjectPointer(id, poolId, size, ida, storePointers) {

  override def objectType: ObjectType.Value = ObjectType.Data
}

object DataObjectPointer {

  def apply(arr: Array[Byte]): DataObjectPointer = ObjectPointer.fromArray(arr).asInstanceOf[DataObjectPointer]

  def apply(bb: ByteBuffer): DataObjectPointer = ObjectPointer.fromByteBuffer(bb).asInstanceOf[DataObjectPointer]

  def apply( id: ObjectId,
             poolId: PoolId,
             size: Option[Int],
             ida: IDA,
             storePointers: Array[StorePointer]): DataObjectPointer = new DataObjectPointer(id, poolId, size, ida, storePointers)
}

class KeyValueObjectPointer(
                             id: ObjectId,
                             poolId: PoolId,
                             size: Option[Int],
                             ida: IDA,
                             storePointers: Array[StorePointer]) extends ObjectPointer(id, poolId, size, ida, storePointers) {

  override def objectType: ObjectType.Value = ObjectType.KeyValue
}

object KeyValueObjectPointer {

  def apply(arr: Array[Byte]): KeyValueObjectPointer = ObjectPointer.fromArray(arr).asInstanceOf[KeyValueObjectPointer]

  def apply(bb: ByteBuffer): KeyValueObjectPointer = ObjectPointer.fromByteBuffer(bb).asInstanceOf[KeyValueObjectPointer]

  def apply( id: ObjectId,
             poolId: PoolId,
             size: Option[Int],
             ida: IDA,
             storePointers: Array[StorePointer]): KeyValueObjectPointer = new KeyValueObjectPointer(id, poolId, size, ida, storePointers)

}
