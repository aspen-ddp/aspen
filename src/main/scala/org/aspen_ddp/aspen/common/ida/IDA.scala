package org.aspen_ddp.aspen.common.ida

import java.nio.ByteBuffer

import org.aspen_ddp.aspen.common.DataBuffer

object IDA {
  val ReplicationCode: Byte = 0
  val ReedSolomonCode: Byte = 1

  val EncodedIDASize: Int = 4

  /** Deserializes the IDA type and returns the matching IDA instance */
  def deserializeIDAType(bb: ByteBuffer): IDA = {
    val typeCode = bb.get()

    typeCode match {
      case ReplicationCode =>
        val width = bb.get()
        val writeThreshold = bb.get()
        bb.get() // discard empty byte
        Replication(width, writeThreshold)

      case ReedSolomonCode =>
        val width = bb.get()
        val readThreshold = bb.get()
        val writeThreshold = bb.get()
        ReedSolomon(width, readThreshold, writeThreshold)

      case _ => throw new IDAEncodingError
    }
  }
}

sealed abstract class IDA extends Ordered[IDA] {

  /** Number of slices/replicas */
  def width: Int

  /** Minimum number of slices needed to restore the object to a readable state.
    * This value may be less than consistentRestoreThreshold.
    */
  def restoreThreshold: Int

  /** Minimum number of slices/replicas that must agree on the current object revision in order to guarantee
    * consistency.
    */
  def consistentRestoreThreshold: Int

  /** Minimum number of slices/replicas that must be successfully written for a successful update transaction */
  def writeThreshold: Int

  /** Restores the data or throws an Exception if the restore operation fails.
    *  Accepts a list of (EncodingIndex, Option[DataBuffer]).
    *  Where the encoding index is the index of this data within the corresponding encode() call
    *
    *  Note that any padding required to achieve alignment requirements must be transparently handled by
    *  the encode/restore methods.
    */
  def restore(segments: List[(Byte,DataBuffer)]): DataBuffer

  /** Restores the data or throws an Exception if the restore operation fails.
    *  Accepts a list of (EncodingIndex, Option[DataBuffer]).
    *  Where the encoding index is the index of this data within the corresponding encode() call
    *
    *  Note that any padding required to achieve alignment requirements must be transparently handled by
    *  the encode/restore methods.
    */
  def restoreArray(segments: List[(Byte, Array[Byte])]): Array[Byte]

  /** Encodes the object into an array of ByteBuffers
    *
    *  Note that the indices of this array are known as the EncodingIndex and are significant to the
    *  corresponding decode operation. The correct index in this array must be used during the decoding
    *  process.
    *
    *  Note that any padding required to achieve alignment requirements must be transparently handled by
    *  the encode/decode methods.
    */
  def encode(objectContent: DataBuffer): Array[DataBuffer]

  def encode(objectContent: Array[Byte]): Array[Array[Byte]]

  /** Encodes the object into the provided array of ByteBuffers. The the size of the array must match the
    *  IDA width and each buffer must support writing at least calculateEncodedSegmentLength bytes.
    */
  def encodeInto(objectContent: DataBuffer, bbArray: Array[ByteBuffer]): Unit

  def failureTolerance: Int = width - writeThreshold

  def compare(that: IDA): Int = failureTolerance - that.failureTolerance

  /** Returns length of the DataBuffers that would be returned by calling encode() on the provided number of bytes */
  def calculateEncodedSegmentLength(nbytes: Int): Int

  /** Returns the size of the restored object given the size of the object segment on a data store */
  def calculateRestoredObjectSize(objectSizeOnDataStore: Int): Int

  /** Returns the number of bytes needed to serialize the IDA type. */
  def getSerializedIDATypeLength: Int

  /** Serializes the IDA type into the ByteBuffer such that when the IDA type is decoded, an identical copy
    *  of this class is returned.
    */
  def serializeIDAType(bb: ByteBuffer): Unit
}

case class Replication(width: Int, writeThreshold: Int) extends IDA {

  def restoreThreshold: Int = 1

  def consistentRestoreThreshold: Int = width / 2 + 1

  def restore(segments: List[(Byte,DataBuffer)]): DataBuffer = if (segments.size < restoreThreshold) throw new IDARestoreError else segments.head._2

  def restoreArray(segments: List[(Byte, Array[Byte])]): Array[Byte] = if (segments.size < restoreThreshold) throw new IDARestoreError else segments.head._2

  def encode(objectContent: DataBuffer): Array[DataBuffer] = {
    val arr = new Array[DataBuffer](width)
    for (i <- 0 until width)
      arr(i) = objectContent
    arr
  }

  def encode(objectContent: Array[Byte]): Array[Array[Byte]] = {
    val arr = new Array[Array[Byte]](width)
    for (i <- 0 until width)
      arr(i) = objectContent
    arr
  }

  /** Terribly inefficient. Avoid the use of this method when using replication */
  def encodeInto(objectContent: DataBuffer, bbArray: Array[ByteBuffer]): Unit = {
    for (bb <- bbArray)
      bb.put(objectContent)
  }

  def calculateEncodedSegmentLength(nbytes: Int): Int = nbytes

  def calculateRestoredObjectSize(objectSizeOnDataStore: Int): Int = objectSizeOnDataStore

  def getSerializedIDATypeLength: Int = 4 // <type><width><writeThreshold>

  def serializeIDAType(bb: ByteBuffer): Unit = {
    bb.put(IDA.ReplicationCode)
    bb.put(width.asInstanceOf[Byte])
    bb.put(writeThreshold.asInstanceOf[Byte])
    bb.put(0.asInstanceOf[Byte]) // reserved
  }
}

case class ReedSolomon(width: Int, restoreThreshold: Int, writeThreshold: Int)
  extends IDA {

  def consistentRestoreThreshold: Int = restoreThreshold

  def restore(segments: List[(Byte,DataBuffer)]): DataBuffer = throw new IDANotSupportedError

  def restoreArray(segments: List[(Byte, Array[Byte])]): Array[Byte] = throw new IDANotSupportedError

  def encode(objectContent: DataBuffer): Array[DataBuffer] = throw new IDANotSupportedError

  def encode(objectContent: Array[Byte]): Array[Array[Byte]] = throw new IDANotSupportedError

  def calculateEncodedSegmentLength(nbytes: Int): Int = throw new IDANotSupportedError

  def calculateRestoredObjectSize(objectSizeOnDataStore: Int): Int = throw new IDANotSupportedError

  def encodeInto(objectContent: DataBuffer, bbArray: Array[ByteBuffer]): Unit = throw new IDANotSupportedError

  def getSerializedIDATypeLength: Int = 4 // <type><width><restoreThreshold><writeThreshold>

  def serializeIDAType(bb: ByteBuffer): Unit = {
    bb.put(IDA.ReplicationCode)
    bb.put(width.asInstanceOf[Byte])
    bb.put(restoreThreshold.asInstanceOf[Byte])
    bb.put(writeThreshold.asInstanceOf[Byte])
  }
}
