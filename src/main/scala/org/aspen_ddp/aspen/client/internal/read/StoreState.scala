package org.aspen_ddp.aspen.client.internal.read

import java.util.UUID

import org.aspen_ddp.aspen.client.KeyValueObjectState
import org.aspen_ddp.aspen.client.KeyValueObjectState.ValueState
import org.aspen_ddp.aspen.common.{DataBuffer, HLCTimestamp}
import org.aspen_ddp.aspen.common.objects.{Key, ObjectRefcount, ObjectRevision, Value}
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.transaction.TransactionId
import org.aspen_ddp.aspen.common.util.Varint

sealed abstract class StoreState(
                                  val storeId: StoreId,
                                  val revision: ObjectRevision,
                                  val refcount: ObjectRefcount,
                                  val timestamp: HLCTimestamp,
                                  val readTimestamp: HLCTimestamp) {

  var rereadRequired: Boolean = false

  def lastUpdateTimestamp: HLCTimestamp

  def debugLogStatus(log: String => Unit): Unit
}

class DataObjectStoreState(
                            storeId: StoreId,
                            revision: ObjectRevision,
                            refcount: ObjectRefcount,
                            timestamp: HLCTimestamp,
                            readTimestamp: HLCTimestamp,
                            val sizeOnStore: Int,
                            val objectData: Option[DataBuffer]) extends StoreState(storeId, revision, refcount, timestamp, readTimestamp) {

  def lastUpdateTimestamp: HLCTimestamp = timestamp

  def debugLogStatus(log: String => Unit): Unit = log(s"  DOSS ${storeId.poolIndex} Rev $revision Ref $refcount $timestamp")

}

class KeyValueObjectStoreState(
                                storeId: StoreId,
                                revision: ObjectRevision,
                                refcount: ObjectRefcount,
                                timestamp: HLCTimestamp,
                                readTimestamp: HLCTimestamp,
                                objectData: Option[DataBuffer],
                                val lockedWriteTransactions: Set[TransactionId]) extends StoreState(storeId, revision, refcount, timestamp, readTimestamp) {

  val kvoss: KeyValueObjectStoreState.StoreKeyValueObjectContent = objectData match {
    case None => new KeyValueObjectStoreState.StoreKeyValueObjectContent(None, None, None, None, Map())
    case Some(db) => KeyValueObjectStoreState.StoreKeyValueObjectContent(db)
  }

  def lastUpdateTimestamp: HLCTimestamp = {
    val i = kvoss.contents.iterator.map(_._2.timestamp)

    val maxContentTs = i.foldLeft(timestamp)( (maxts, ts) =>  if (ts > maxts) ts else maxts)

    if (maxContentTs > timestamp) maxContentTs else timestamp
  }

  def debugLogStatus(log: String => Unit): Unit = {
    log(s"  KVOSS ${storeId.poolIndex} Rev $revision Ref $refcount TS $timestamp")
    kvoss.contents.foreach { t =>
      val (key, vs) = t
      log(s"    $key revision ${vs.revision} ${vs.timestamp}")
    }
    //kvoss.debugLogStatus(log)
  }
}

object KeyValueObjectStoreState {
  class StoreKeyValueObjectContent(val minimum: Option[Key],
                                   val maximum: Option[Key],
                                   val left: Option[Value],
                                   val right: Option[Value],
                                   val contents: Map[Key, KeyValueObjectState.ValueState])

  object StoreKeyValueObjectContent {
    def apply(db: DataBuffer): StoreKeyValueObjectContent = {
      // Note - content of this method is copied from server.KVObjectState.decode()
      //        copy used instead of re-using the code to avoid temporaries since the server code
      //        adds extra variables to value state to track transaction info

      val bb = db.asReadOnlyBuffer()

      if (bb.limit() == 0) {
        return new StoreKeyValueObjectContent(None, None, None, None, Map())
      }

      if (bb.limit() < 2)
        throw ObjectEncodingError()

      def getBytes: Array[Byte] = {
        if (bb.limit() == 0)
          throw ObjectEncodingError()
        val len = Varint.getUnsignedInt(bb)
        val arr = new Array[Byte](len)
        if (bb.limit() < arr.length)
          throw ObjectEncodingError()
        bb.get(arr)
        arr
      }

      def getKey: Key = Key(getBytes)
      def getValue: Value = Value(getBytes)

      def getContent: (Key, ValueState) = {
        if (bb.limit() < 16 + 8)
          throw ObjectEncodingError()
        val msb = bb.getLong
        val lsb = bb.getLong
        val revision = ObjectRevision(TransactionId(new UUID(msb, lsb)))
        val timestamp = HLCTimestamp(bb.getLong)
        val key = getKey
        val value = getValue
        key -> ValueState(value, revision, timestamp)
      }
      val mask = bb.get()

      val min   = if ((mask & 1 << 0) != 0) Some(getKey) else None
      val max   = if ((mask & 1 << 1) != 0) Some(getKey) else None
      val left  = if ((mask & 1 << 2) != 0) Some(getValue) else None
      val right = if ((mask & 1 << 3) != 0) Some(getValue) else None

      val ncontents = Varint.getUnsignedInt(bb)

      var content: Map[Key, ValueState] = Map()

      for (_ <- 0 until ncontents) {
        content += getContent
      }

      new StoreKeyValueObjectContent(min, max, left, right, content)
    }
  }
}