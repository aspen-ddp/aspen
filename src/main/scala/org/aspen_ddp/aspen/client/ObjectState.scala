package org.aspen_ddp.aspen.client

import java.util.UUID

import org.aspen_ddp.aspen.common.{DataBuffer, HLCTimestamp}
import org.aspen_ddp.aspen.common.objects.{DataObjectPointer, Key, KeyOrdering, KeyValueObjectPointer, ObjectId, ObjectPointer, ObjectRefcount, ObjectRevision, Value}
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.transaction.TransactionId
import org.aspen_ddp.aspen.common.util.Varint

sealed abstract class ObjectState(
                                   val pointer: ObjectPointer,
                                   val revision: ObjectRevision,
                                   val refcount: ObjectRefcount,
                                   val timestamp: HLCTimestamp,
                                   val readTimestamp: HLCTimestamp) {

  def id: ObjectId = pointer.id

  def canEqual(other: Any): Boolean

  def getRebuildDataForStore(storeId: StoreId): Option[DataBuffer]

  def lastUpdateTimestamp: HLCTimestamp
}

class MetadataObjectState(
                           pointer: ObjectPointer,
                           revision:ObjectRevision,
                           refcount:ObjectRefcount,
                           timestamp: HLCTimestamp,
                           readTimestamp: HLCTimestamp) extends ObjectState(pointer, revision, refcount, timestamp, readTimestamp) {

  def lastUpdateTimestamp: HLCTimestamp = timestamp

  def canEqual(other: Any): Boolean = other.isInstanceOf[MetadataObjectState]

  override def equals(other: Any): Boolean =
    other match
      case that: MetadataObjectState => (that canEqual this) && pointer == that.pointer && revision == that.revision && refcount == that.refcount
      case _ => false

  override def hashCode: Int =
    val hashCodes = List(pointer.hashCode, revision.hashCode, refcount.hashCode, timestamp.hashCode)
    hashCodes.reduce( (a,b) => a ^ b )

  def getRebuildDataForStore(storeId: StoreId): Option[DataBuffer] = None
}

object MetadataObjectState {
  def apply(
             pointer: ObjectPointer,
             revision:ObjectRevision,
             refcount:ObjectRefcount,
             timestamp: HLCTimestamp,
             readTimestamp: HLCTimestamp): MetadataObjectState = new MetadataObjectState(pointer, revision, refcount, timestamp, readTimestamp)
}

class DataObjectState(
                       override val pointer: DataObjectPointer,
                       revision: ObjectRevision,
                       refcount: ObjectRefcount,
                       timestamp: HLCTimestamp,
                       readTimestamp: HLCTimestamp,
                       val sizeOnStore: Int,
                       val data: DataBuffer) extends ObjectState(pointer, revision, refcount, timestamp, readTimestamp) {

  def lastUpdateTimestamp: HLCTimestamp = timestamp

  def size: Int = pointer.ida.calculateRestoredObjectSize(sizeOnStore)

  def canEqual(other: Any): Boolean = other.isInstanceOf[DataObjectState]

  override def equals(other: Any): Boolean =
    other match
      case that: DataObjectState => (that canEqual this) && pointer == that.pointer && revision == that.revision && refcount == that.refcount && data == that.data
      case _ => false

  override def hashCode: Int =
    val hashCodes = List(pointer.hashCode, revision.hashCode, refcount.hashCode, timestamp.hashCode, data.hashCode)
    hashCodes.reduce( (a,b) => a ^ b )

  def getRebuildDataForStore(storeId: StoreId): Option[DataBuffer] = pointer.getEncodedDataIndexForStore(storeId).map { idx =>
    pointer.ida.encode(data)(idx)
  }
}

object DataObjectState {
  def apply(
             pointer: DataObjectPointer,
             revision:ObjectRevision,
             refcount:ObjectRefcount,
             timestamp: HLCTimestamp,
             readTimestamp: HLCTimestamp,
             sizeOnStore: Int,
             data: DataBuffer): DataObjectState = new DataObjectState(pointer, revision, refcount, timestamp, readTimestamp, sizeOnStore, data)
}

class KeyValueObjectState(
                           override val pointer: KeyValueObjectPointer,
                           revision:ObjectRevision,
                           refcount:ObjectRefcount,
                           timestamp: HLCTimestamp,
                           readTimestamp: HLCTimestamp,
                           val minimum: Option[Key],
                           val maximum: Option[Key],
                           val left: Option[Value],
                           val right: Option[Value],
                           val contents: Map[Key, KeyValueObjectState.ValueState]
                         ) extends ObjectState(pointer, revision, refcount, timestamp, readTimestamp) {

  def sizeOnStore: Int =
    def encodedValueSize(v: Value): Int =
      val sz = pointer.ida.calculateEncodedSegmentLength(v.bytes.length)
      Varint.getUnsignedIntEncodingLength(sz) + sz
    var size = 1 // mask byte
    minimum.foreach(key => size += Varint.getUnsignedIntEncodingLength(key.bytes.length) + key.bytes.length)
    maximum.foreach(key => size += Varint.getUnsignedIntEncodingLength(key.bytes.length) + key.bytes.length)
    left.foreach(value => size += encodedValueSize(value))
    right.foreach(value => size += encodedValueSize(value))
    size += Varint.getUnsignedIntEncodingLength(contents.size)
    contents.foreach { t =>
      val (key, vs) = t
      size += 16 + 8
      size += Varint.getUnsignedIntEncodingLength(key.bytes.length) + key.bytes.length
      size += encodedValueSize(vs.value)
    }
    size

  def guessSizeOnStoreAfterUpdate(inserts: List[(Key,Array[Byte])], deletes: List[Key]): Int =
    def encodedKeySize(k: Key): Int = Varint.getUnsignedIntEncodingLength(k.bytes.length) + k.bytes.length

    def encodedValueSize(v: Value): Int =
      val sz = pointer.ida.calculateEncodedSegmentLength(v.bytes.length)
      Varint.getUnsignedIntEncodingLength(sz) + sz

    val adds = inserts.foldLeft(0){ (sz, t) =>
      16 + 8 + encodedKeySize(t._1) + encodedValueSize(Value(t._2)) + sz
    }

    val dels = deletes.foldLeft(0) { (sz, k) =>
      val x = contents.get(k) match
        case None => 0
        case Some(vs) => 16 + 8 + encodedKeySize(k) + encodedValueSize(vs.value)
      sz + x
    }

    val guess = sizeOnStore + adds - dels

    if guess < 0 then 0 else guess

  /** Rough approximation. Only considers restored sizes */
  def size: Int = contents.foldLeft(0)((sz, t) => sz + t._1.bytes.length + t._2.value.bytes.length) +
    minimum.map(_.bytes.length).getOrElse(0) +
    maximum.map(_.bytes.length).getOrElse(0) +
    left.map(_.bytes.length).getOrElse(0) +
    right.map(_.bytes.length).getOrElse(0)

  def keyInRange(key: Key, ordering: KeyOrdering): Boolean =
    val minOk = minimum match
      case None => true
      case Some(min) => ordering.compare(key, min) >= 0
    val maxOk = maximum match
      case None => true
      case Some(max) => ordering.compare(key, max) < 0
    minOk && maxOk

  def canEqual(other: Any): Boolean = other.isInstanceOf[KeyValueObjectState]

  override def equals(other: Any): Boolean =
    other match
      case that: KeyValueObjectState =>
        val minEq = (minimum, that.minimum) match
          case (None, None) => true
          case (Some(_), None) => false
          case (None, Some(_)) => false
          case (Some(x), Some(y)) => java.util.Arrays.equals(x.bytes, y.bytes)
        val maxEq = (maximum, that.maximum) match
          case (None, None) => true
          case (Some(_), None) => false
          case (None, Some(_)) => false
          case (Some(x), Some(y)) => java.util.Arrays.equals(x.bytes, y.bytes)
        val leftEq = (left, that.left) match
          case (None, None) => true
          case (Some(_), None) => false
          case (None, Some(_)) => false
          case (Some(x), Some(y)) => java.util.Arrays.equals(x.bytes, y.bytes)
        val rightEq = (left, that.left) match
          case (None, None) => true
          case (Some(_), None) => false
          case (None, Some(_)) => false
          case (Some(x), Some(y)) => java.util.Arrays.equals(x.bytes, y.bytes)

        (that canEqual this) && pointer == that.pointer && revision == that.revision && refcount == that.refcount &&
          minEq && maxEq && leftEq && rightEq && contents == that.contents
      case _ => false
  override def hashCode: Int =
    def hk(o: Option[Key]): Int = o match
      case None => 0
      case Some(key) => java.util.Arrays.hashCode(key.bytes)
    def ha(o: Option[Array[Byte]]): Int = o match
      case None => 0
      case Some(arr) => java.util.Arrays.hashCode(arr)
    val hashCodes = List(pointer.hashCode, revision.hashCode, refcount.hashCode, timestamp.hashCode,
      hk(minimum), hk(maximum), ha(left.map(_.bytes)), ha(right.map(_.bytes)), contents.hashCode)
    hashCodes.reduce( (a,b) => a ^ b )

  def allUpdates: Set[ObjectRevision] = contents.iterator.map(_._2.revision).toSet + revision


  def lastUpdateTimestamp: HLCTimestamp =
    val i = contents.iterator.map(_._2.timestamp)

    val maxContentTs = i.foldLeft(timestamp)( (maxts, ts) =>  if ts > maxts then ts else maxts)

    if maxContentTs > timestamp then maxContentTs else timestamp

  override def toString: String =
    /*
    def p(o:Option[Array[Byte]]): String = o match
      case None => ""
      case Some(arr) => com.ibm.aspen.util.printableArray(arr)
    */
    val min = minimum.map(m => org.aspen_ddp.aspen.common.util.printableArray(m.bytes)).getOrElse("")
    val max = maximum.map(m => org.aspen_ddp.aspen.common.util.printableArray(m.bytes)).getOrElse("")
    val l = left.map(l => org.aspen_ddp.aspen.common.util.printableArray(l.bytes)).getOrElse("")
    val r = right.map(r => org.aspen_ddp.aspen.common.util.printableArray(r.bytes)).getOrElse("")

    s"KVObjectState(object: ${pointer.id}, revision: $revision, refcount: $refcount, min: $min, max: $max, left: $l, right: $r, contents: $contents"

  def getRebuildDataForStore(storeId: StoreId): Option[DataBuffer] = pointer.getEncodedDataIndexForStore(storeId).map { idaIndex =>

    import org.aspen_ddp.aspen.server.store.KVObjectState
    import org.aspen_ddp.aspen.server.store.ValueState

    var storeContent: Map[Key, ValueState] = Map()

    contents.foreach { t =>
      val (key, vs) = t
      val idaEncodedValue = Value(pointer.ida.encode(vs.value.bytes)(idaIndex))
      storeContent += (key -> new ValueState(idaEncodedValue, vs.revision, vs.timestamp, None))
    }

    val kvos = new KVObjectState(minimum, maximum, left, right, storeContent, Map())

    kvos.encode()
  }
}

object KeyValueObjectState {

  final case class ValueState(value: Value, revision: ObjectRevision, timestamp: HLCTimestamp) {
    override def hashCode = (value, revision, timestamp).##

    override def equals(other: Any) = other match
      case that: ValueState =>
        java.util.Arrays.equals(value.bytes, that.value.bytes) && revision == that.revision && timestamp == that.timestamp
      case _ => false
  }

  def compare(
               pointer: KeyValueObjectPointer,
               revision:ObjectRevision,
               refcount:ObjectRefcount,
               timestamp: HLCTimestamp,
               readTimestamp: HLCTimestamp,
               minimum: Option[Key],
               maximum: Option[Key],
               left: Option[Value],
               right: Option[Value],
               contents: Map[Key, KeyValueObjectState.ValueState]): KeyValueObjectState =
    KeyValueObjectState(pointer, revision, refcount, timestamp, readTimestamp, minimum, maximum, left, right, contents)

  def cmp(a: Option[Array[Byte]], b: Option[Array[Byte]]): Boolean = (a,b) match
    case (None, None) => true
    case (Some(_), None) => false
    case (None, Some(_)) => false
    case (Some(x), Some(y)) => java.util.Arrays.equals(x, y)

  def cmpk(a: Option[Key], b: Option[Key]): Boolean = (a,b) match
    case (None, None) => true
    case (Some(_), None) => false
    case (None, Some(_)) => false
    case (Some(x), Some(y)) => java.util.Arrays.equals(x.bytes, y.bytes)
}
