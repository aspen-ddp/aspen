package org.aspen_ddp.aspen.client.internal.transaction

import java.nio.{ByteBuffer, ByteOrder}
import java.util.UUID

import org.aspen_ddp.aspen.common.HLCTimestamp
import org.aspen_ddp.aspen.common.objects.{Key, ObjectPointer}
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.transaction.TransactionId

/** A TracelessCommits tree value: a store missed a transaction whose change to `objects` leaves nothing for
  * repair to compare (a refcount change, or a key delete), so the transaction's start `timestamp` and
  * the pointers, which keep their store pointers, are recorded for the store's repair.
  */
final case class TracelessCommitEntry(timestamp: HLCTimestamp, objects: List[ObjectPointer]):
  def encode: Array[Byte] =
    val ptrs = objects.map(_.toArray)
    val bb = ByteBuffer.allocate(HLCTimestamp.EncodedSize + 4 + ptrs.map(_.length + 4).sum)
    bb.order(ByteOrder.BIG_ENDIAN)
    bb.putLong(timestamp.asLong)
    bb.putInt(ptrs.size)
    ptrs.foreach: p =>
      bb.putInt(p.length)
      bb.put(p)
    bb.array()

object TracelessCommitEntry:
  def decode(bytes: Array[Byte]): TracelessCommitEntry =
    val bb = ByteBuffer.wrap(bytes)
    bb.order(ByteOrder.BIG_ENDIAN)
    val ts = HLCTimestamp(bb.getLong())
    val count = bb.getInt()
    val objects = List.fill(count):
      val p = new Array[Byte](bb.getInt())
      bb.get(p)
      ObjectPointer.fromArray(p)
    TracelessCommitEntry(ts, objects)

  /** poolIndex byte then the transaction UUID, big-endian: 17 bytes, like MissedUpdateMarker.errorKey */
  def key(store: StoreId, txid: TransactionId): Key =
    val bb = ByteBuffer.allocate(17)
    bb.order(ByteOrder.BIG_ENDIAN)
    bb.put(store.poolIndex)
    bb.putLong(txid.uuid.getMostSignificantBits)
    bb.putLong(txid.uuid.getLeastSignificantBits)
    Key(bb.array())

  def decodeTransactionId(key: Key): TransactionId =
    val bb = ByteBuffer.wrap(key.bytes)
    bb.order(ByteOrder.BIG_ENDIAN)
    bb.get()
    TransactionId(new UUID(bb.getLong(), bb.getLong()))
