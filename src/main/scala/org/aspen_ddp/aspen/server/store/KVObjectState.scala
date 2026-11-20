package org.aspen_ddp.aspen.server.store

import java.nio.ByteBuffer
import java.util.UUID

import org.aspen_ddp.aspen.common.ida.IDA
import org.aspen_ddp.aspen.common.{DataBuffer, HLCTimestamp}
import org.aspen_ddp.aspen.common.objects.{Delete, DeleteLeft, DeleteMax, DeleteMin, DeleteRight, Insert, Key, KeyValueOperation, ObjectRevision, SetLeft, SetMax, SetMin, SetRight, Value}
import org.aspen_ddp.aspen.common.transaction.TransactionId
import org.aspen_ddp.aspen.common.util.Varint
import scala.language.implicitConversions

class KVObjectState(
  var min: Option[Key] = None,
  var max: Option[Key] = None,
  var left: Option[Value] = None,
  var right: Option[Value] = None,
  var content: Map[Key, ValueState] = Map(),
  /** Exclusive locks on absent keys: DoesNotExist and MayExist requirements, and Inserts or Deletes of the key */
  var absentKeyLocks: Map[Key, TransactionId] = Map(),
  var contentLocked: Option[TransactionId] = None,
  var rangeLocks: Set[TransactionId] = Set() // Read locks on the object revision
) {

  def encode(): DataBuffer = {
    KVObjectState.encode(min, max, left, right, content)
  }

  /** Size in bytes of encode(): the serialized slice this store holds for the object. Lock state is not counted */
  def encodedSize: Int = KVObjectState.encodedSize(min, max, left, right, content)

  /** encodedSize of the state that applying ops by the transaction writing `revision` at `timestamp` (as
    * RequirementsApplyer does) would produce. This state is not modified. They decide only which Inserts win
    * (`KVObjectState.insertWins`): every Insert counts the fixed 16-byte revision and 8-byte timestamp.
    */
  def encodedSizeAfter(ops: List[KeyValueOperation], revision: ObjectRevision, timestamp: HLCTimestamp): Int =
    val (nmin, nmax, nleft, nright, ncontent) =
      KVObjectState.applyOps(min, max, left, right, content, ops, Some((revision, timestamp)))
    KVObjectState.encodedSize(nmin, nmax, nleft, nright, ncontent)

  /** What applying ops by the transaction writing `revision` at `timestamp` (as RequirementsApplyer does) changes in
    * encodedSize, apart from the entry-count varint: see SliceGrowth. Walks only the keys `ops` name. This state is
    * not modified
    */
  def growth(ops: List[KeyValueOperation], revision: ObjectRevision, timestamp: HLCTimestamp): SliceGrowth =
    var nmin = min.map(_.bytes)
    var nmax = max.map(_.bytes)
    var nleft = left.map(_.bytes)
    var nright = right.map(_.bytes)
    var touched = Map[Key, Option[ValueState]]()
    ops.foreach {
      case o: SetMin => nmin = Some(o.value)
      case o: SetMax => nmax = Some(o.value)
      case o: SetLeft => nleft = Some(o.value)
      case o: SetRight => nright = Some(o.value)
      case _: DeleteMin => nmin = None
      case _: DeleteMax => nmax = None
      case _: DeleteLeft => nleft = None
      case _: DeleteRight => nright = None
      case o: Insert if KVObjectState.insertWins(touched.getOrElse(o.key, content.get(o.key)), revision, timestamp) =>
        touched += o.key -> Some(new ValueState(Value(o.value), revision, timestamp, None))
      case _: Insert =>
      case o: Delete => touched += o.key -> None
    }
    def field(b: Option[Array[Byte]]): Long = b.fold(0L)(a => Varint.getUnsignedIntEncodingLength(a.length) + a.length)
    def entry(k: Key, vs: Option[ValueState]): Long = vs.fold(0L)(v => KVObjectState.entrySize(k, v.value))
    var bytes = field(nmin) - field(min.map(_.bytes)) + field(nmax) - field(max.map(_.bytes)) +
      field(nleft) - field(left.map(_.bytes)) + field(nright) - field(right.map(_.bytes))
    var entries = 0
    touched.foreach { (k, after) =>
      val before = content.get(k)
      bytes += entry(k, after) - entry(k, before)
      entries += after.size - before.size
    }
    SliceGrowth(bytes, entries)
}

/** The change one KeyValue update makes to its object's encoded slice (KVObjectState.encodedSize): `bytes` apart from
  * the entry-count varint, and `entries` added to the count (negative for a net removal). Updates of disjoint keys
  * change disjoint entries, so their combined change is the sum of theirs, the count varint aside. A commit vote that
  * grows the object keeps its growth reserved on it while it holds its locks (ObjectState.sliceReservations), so the
  * size check counts the votes held beside it (RequirementsChecker.checkKVSliceSize)
  */
final case class SliceGrowth(bytes: Long, entries: Int):
  /** The update may grow the slice: it adds bytes or entries */
  def grows: Boolean = bytes > 0 || entries > 0

object KVObjectState {

  def apply(dataBuffer: DataBuffer): KVObjectState = decode(dataBuffer)

  /** Whether an Insert by the transaction writing `revision` at `timestamp` replaces `existing`: last writer wins on
    * (timestamp, revision). Equal is the same transaction applying again, which writes the same value. An Insert a
    * later one has superseded is a no-op, so a store that applies sequential writes of a key out of order (a lagging
    * store, or a crash re-apply of a recovered vote) converges with the others
    * (transaction-requirements 9.3).
    * A checked Insert (KeyRevision, DoesNotExist, a required object revision) always wins: its check rejects a key
    * written later, and the client's HLC puts the transaction after the state it read.
    */
  def insertWins(existing: Option[ValueState], revision: ObjectRevision, timestamp: HLCTimestamp): Boolean =
    existing match
      case None => true
      case Some(vs) =>
        val byTime = timestamp.compare(vs.timestamp)
        byTime > 0 || (byTime == 0 && revision.lastUpdateTxUUID.compareTo(vs.revision.lastUpdateTxUUID) >= 0)

  /** The min, max, left, right and content that applying `ops` in order (as RequirementsApplyer does) to the given
    * ones produces. With `writer` (the transaction's revision and timestamp) an Insert applies only where it wins
    * (`insertWins`); without it every Insert applies, as for an empty state, and inserted entries get a null revision
    * and zero timestamp: only their sizes matter then.
    */
  def applyOps(min: Option[Key],
               max: Option[Key],
               left: Option[Value],
               right: Option[Value],
               content: Map[Key, ValueState],
               ops: List[KeyValueOperation],
               writer: Option[(ObjectRevision, HLCTimestamp)] = None)
      : (Option[Key], Option[Key], Option[Value], Option[Value], Map[Key, ValueState]) =
    var nmin = min
    var nmax = max
    var nleft = left
    var nright = right
    var ncontent = content
    ops.foreach {
      case o: SetMin => nmin = Some(Key(o.value))
      case o: SetMax => nmax = Some(Key(o.value))
      case o: SetLeft => nleft = Some(Value(o.value))
      case o: SetRight => nright = Some(Value(o.value))
      case _: DeleteMin => nmin = None
      case _: DeleteMax => nmax = None
      case _: DeleteLeft => nleft = None
      case _: DeleteRight => nright = None
      case o: Insert if writer.forall((rev, ts) => insertWins(ncontent.get(o.key), rev, ts)) =>
        val (rev, ts) = writer.getOrElse((ObjectRevision.Null, HLCTimestamp.Zero))
        ncontent += (o.key -> new ValueState(Value(o.value), rev, ts, None))
      case _: Insert =>
      case o: Delete => ncontent -= o.key
    }
    (nmin, nmax, nleft, nright, ncontent)

  /** encodedSize of an empty state: the slice a store holds for a newly allocated key-value object before its
    * initial content is applied (mask byte plus a zero entry count)
    */
  val EmptyEncodedSize: Int = encodedSize(None, None, None, None, Map())

  /** Size in bytes of the slice each store of a pool using `ida` holds for a new key-value object whose initial
    * content is `ops` (given with full, unencoded values). This is exactly the measure (`encodedSizeAfter`, `growth`) stores
    * apply to an allocation: SetMin/SetMax keys and entry keys are replicated, while Insert, SetLeft and SetRight
    * values are IDA-encoded to `ida.calculateEncodedSegmentLength` bytes per store.
    */
  def allocationSliceSize(ida: IDA, ops: List[KeyValueOperation]): Int =
    val (min, max, left, right, content) = applyOps(None, None, None, None, Map(), ops)
    encodedSizeIDA(ida, min, max, left, right, content)

  // <mask byte> min 0 max << 1 left << 2 right << 3
  // <vartint len><nbytes>
  // <varint_num_content_entries>
  // <16-byte-revision><8-byte-timestamp><varint_key_len><key><varint_value><value>
  /** Encoded size of one content entry: 16-byte revision, 8-byte timestamp, then the length-prefixed key and value */
  def entrySize(key: Key, value: Value): Int =
    16 + 8 + Varint.getUnsignedIntEncodingLength(key.bytes.length) + key.bytes.length +
      Varint.getUnsignedIntEncodingLength(value.bytes.length) + value.bytes.length

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
    if (min.nonEmpty)   mask = (mask | 1 << 0).toByte
    if (max.nonEmpty)   mask = (mask | 1 << 1).toByte
    if (left.nonEmpty)  mask = (mask | 1 << 2).toByte
    if (right.nonEmpty) mask = (mask | 1 << 3).toByte
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
    if (min.nonEmpty) mask = (mask | 1 << 0).toByte
    if (max.nonEmpty) mask = (mask | 1 << 1).toByte
    if (left.nonEmpty) mask = (mask | 1 << 2).toByte
    if (right.nonEmpty) mask = (mask | 1 << 3).toByte

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
