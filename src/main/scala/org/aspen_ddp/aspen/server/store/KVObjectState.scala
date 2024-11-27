package org.aspen_ddp.aspen.server.store

import java.nio.ByteBuffer
import java.util.UUID

import org.aspen_ddp.aspen.common.ida.IDA
import org.aspen_ddp.aspen.common.{DataBuffer, HLCTimestamp}
import org.aspen_ddp.aspen.common.objects.{Key, ObjectRevision, Value}
import org.aspen_ddp.aspen.common.transaction.TransactionId
import org.aspen_ddp.aspen.common.util.Varint

class KVObjectState(
  var min: Option[Key] = None,
  var max: Option[Key] = None,
  var left: Option[Value] = None,
  var right: Option[Value] = None,
  var content: Map[Key, ValueState] = Map(),
  var noExistenceLocks: Map[Key, TransactionId] = Map(),
  var contentLocked: Option[TransactionId] = None,
  var rangeLocks: Set[TransactionId] = Set() // Read locks on the object revision
) {

  def encode(): DataBuffer = {
    KVObjectState.encode(min, max, left, right, content)
  }
}

object KVObjectState {

  def apply(dataBuffer: DataBuffer): KVObjectState = decode(dataBuffer)

  // <mask byte> min 0 max << 1 left << 2 right << 3
  // <vartint len><nbytes>
  // <varint_num_content_entries>
  // <16-byte-revision><8-byte-timestamp><varint_key_len><key><varint_value><value>
  def encodedSize( min: Option[Key],
                   max: Option[Key],
                   left: Option[Value],
                   right: Option[Value],
                   contents: Map[Key, ValueState]): Int = {
    var size = 1 // mask byte
    min.foreach(key => size += Varint.getUnsignedIntEncodingLength(key.bytes.length) + key.bytes.length)
    max.foreach(key => size += Varint.getUnsignedIntEncodingLength(key.bytes.length) + key.bytes.length)
    left.foreach(value => size += Varint.getUnsignedIntEncodingLength(value.bytes.length) + value.bytes.length)
    right.foreach(value => size += Varint.getUnsignedIntEncodingLength(value.bytes.length) + value.bytes.length)
    size += Varint.getUnsignedIntEncodingLength(contents.size)
    contents.foreach { t =>
      val (key, vs) = t
      size += 16 + 8
      size += Varint.getUnsignedIntEncodingLength(key.bytes.length) + key.bytes.length
      size += Varint.getUnsignedIntEncodingLength(vs.value.bytes.length) + vs.value.bytes.length
    }
    size
  }

  def encode( min: Option[Key],
              max: Option[Key],
              left: Option[Value],
              right: Option[Value],
              contents: Map[Key, ValueState]): DataBuffer = {
    val bb = ByteBuffer.allocate(encodedSize(min, max, left, right, contents))

    var mask: Byte = 0
    if (min.nonEmpty)   mask = (mask | 1 << 0).asInstanceOf[Byte]
    if (max.nonEmpty)   mask = (mask | 1 << 1).asInstanceOf[Byte]
    if (left.nonEmpty)  mask = (mask | 1 << 2).asInstanceOf[Byte]
    if (right.nonEmpty) mask = (mask | 1 << 3).asInstanceOf[Byte]
    bb.put(mask)
    min.foreach { key =>
      Varint.putUnsignedInt(bb, key.bytes.length)
      bb.put(key.bytes)
    }
    max.foreach { key =>
      Varint.putUnsignedInt(bb, key.bytes.length)
      bb.put(key.bytes)
    }
    left.foreach { value =>
      Varint.putUnsignedInt(bb, value.bytes.length)
      bb.put(value.bytes)
    }
    right.foreach { value =>
      Varint.putUnsignedInt(bb, value.bytes.length)
      bb.put(value.bytes)
    }
    Varint.putUnsignedInt(bb, contents.size)
    contents.foreach { t =>
      val (key, vs) = t
      bb.putLong(vs.revision.lastUpdateTxUUID.getMostSignificantBits)
      bb.putLong(vs.revision.lastUpdateTxUUID.getLeastSignificantBits)
      bb.putLong(vs.timestamp.asLong)
      Varint.putUnsignedInt(bb, key.bytes.length)
      bb.put(key.bytes)
      Varint.putUnsignedInt(bb, vs.value.bytes.length)
      bb.put(vs.value.bytes)
    }

    bb.position(0)

    bb
  }

  def idaEncodedPairSize(ida: IDA, key: Key, value: Value): Int = {
    var size = 16 + 8

    size += Varint.getUnsignedIntEncodingLength(key.bytes.length) + key.bytes.length

    val encodedSize = ida.calculateEncodedSegmentLength(value.bytes.length)

    size += Varint.getUnsignedIntEncodingLength(encodedSize) + encodedSize

    size
  }

  def encodedSizeIDA( ida: IDA,
                      min: Option[Key],
                      max: Option[Key],
                      left: Option[Value],
                      right: Option[Value],
                      contents: Map[Key, ValueState]): Int = {
    var size = 1 // mask byte
    min.foreach(key => size += Varint.getUnsignedIntEncodingLength(key.bytes.length) + key.bytes.length)
    max.foreach(key => size += Varint.getUnsignedIntEncodingLength(key.bytes.length) + key.bytes.length)

    left.foreach { value =>
      val encodedSize = ida.calculateEncodedSegmentLength(value.bytes.length)
      size += Varint.getUnsignedIntEncodingLength(encodedSize) + encodedSize
    }
    right.foreach { value =>
      val encodedSize = ida.calculateEncodedSegmentLength(value.bytes.length)
      size += Varint.getUnsignedIntEncodingLength(encodedSize) + encodedSize
    }
    size += Varint.getUnsignedIntEncodingLength(contents.size)
    contents.foreach { t =>
      val (key, vs) = t
      size += 16 + 8
      size += Varint.getUnsignedIntEncodingLength(key.bytes.length) + key.bytes.length

      val encodedSize = ida.calculateEncodedSegmentLength(vs.value.bytes.length)

      size += Varint.getUnsignedIntEncodingLength(encodedSize) + encodedSize
    }
    size
  }

  def encodeIDA( ida: IDA,
                 min: Option[Key],
                 max: Option[Key],
                 left: Option[Value],
                 right: Option[Value],
                 contents: Map[Key, ValueState]): Array[DataBuffer] = {

    var mask: Byte = 0
    if (min.nonEmpty) mask = (mask | 1 << 0).asInstanceOf[Byte]
    if (max.nonEmpty) mask = (mask | 1 << 1).asInstanceOf[Byte]
    if (left.nonEmpty) mask = (mask | 1 << 2).asInstanceOf[Byte]
    if (right.nonEmpty) mask = (mask | 1 << 3).asInstanceOf[Byte]

    val bbArray = new Array[ByteBuffer](ida.width)

    for (i <- 0 until ida.width) {
      bbArray(i) = ByteBuffer.allocate(encodedSizeIDA(ida, min, max, left, right, contents))

      bbArray(i).put(mask)

      min.foreach { key =>
        Varint.putUnsignedInt(bbArray(i), key.bytes.length)
        bbArray(i).put(key.bytes)
      }
      max.foreach { key =>
        Varint.putUnsignedInt(bbArray(i), key.bytes.length)
        bbArray(i).put(key.bytes)
      }
    }

    left.foreach { value =>
      val sz = ida.calculateEncodedSegmentLength(value.bytes.length)
      val enc = ida.encode(value.bytes)
      for (i <- 0 until ida.width) {
        Varint.putUnsignedInt(bbArray(i), sz)
        bbArray(i).put(enc(i))
      }
    }
    right.foreach { value =>
      val sz = ida.calculateEncodedSegmentLength(value.bytes.length)
      val enc = ida.encode(value.bytes)
      for (i <- 0 until ida.width) {
        Varint.putUnsignedInt(bbArray(i), sz)
        bbArray(i).put(enc(i))
      }
    }

    for (i <- 0 until ida.width) {
      Varint.putUnsignedInt(bbArray(i), contents.size)
    }

    contents.foreach { t =>
      val (key, vs) = t
      val sz = ida.calculateEncodedSegmentLength(vs.value.bytes.length)
      val enc = ida.encode(vs.value.bytes)
      for (i <- 0 until ida.width) {
        bbArray(i).putLong(vs.revision.lastUpdateTxUUID.getMostSignificantBits)
        bbArray(i).putLong(vs.revision.lastUpdateTxUUID.getLeastSignificantBits)
        bbArray(i).putLong(vs.timestamp.asLong)
        Varint.putUnsignedInt(bbArray(i), key.bytes.length)
        bbArray(i).put(key.bytes)
        Varint.putUnsignedInt(bbArray(i), sz)
        bbArray(i).put(enc(i))
      }
    }

    for (i <- 0 until ida.width)
      bbArray(i).position(0)

    bbArray.map(DataBuffer(_))
  }

  def decode(db: DataBuffer): KVObjectState = {
    if (db.size == 0)
      new KVObjectState(None, None, None, None, Map(), Map())
    else {
      val bb = db.asReadOnlyBuffer()

      def getBytes: Array[Byte] = {
        val len = Varint.getUnsignedInt(bb)
        val arr = new Array[Byte](len)
        bb.get(arr)
        arr
      }

      def getKey: Key = Key(getBytes)

      def getValue: Value = Value(getBytes)

      def getContent: (Key, ValueState) = {
        val msb = bb.getLong
        val lsb = bb.getLong
        val revision = ObjectRevision(TransactionId(new UUID(msb, lsb)))
        val timestamp = HLCTimestamp(bb.getLong)
        val key = getKey
        val value = getValue
        key -> new ValueState(value, revision, timestamp, None)
      }

      val mask = bb.get()

      val min = if ((mask & 1 << 0) != 0) Some(getKey) else None
      val max = if ((mask & 1 << 1) != 0) Some(getKey) else None
      val left = if ((mask & 1 << 2) != 0) Some(getValue) else None
      val right = if ((mask & 1 << 3) != 0) Some(getValue) else None

      val ncontents = Varint.getUnsignedInt(bb)

      var content: Map[Key, ValueState] = Map()

      for (_ <- 0 until ncontents) {
        content += getContent
      }

      new KVObjectState(min, max, left, right, content, Map())
    }
  }
}
