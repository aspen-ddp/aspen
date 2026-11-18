package org.aspen_ddp.aspen.client.internal.transaction

import java.nio.{ByteBuffer, ByteOrder}
import java.util.UUID

import org.aspen_ddp.aspen.common.HLCTimestamp
import org.aspen_ddp.aspen.common.objects.{Key, ObjectPointer}
import org.aspen_ddp.aspen.common.store.{StoreId, StoreIncarnation}
import org.aspen_ddp.aspen.common.transaction.TransactionId

/** A UnackedCommits tree value: a store cast a commit vote on a committed transaction and had not acknowledged
  * applying it when the transaction was finalized. `objects` lists every object of the store the transaction
  * changes, traced or traceless, with their store pointers; with the transaction's start `timestamp` they are
  * recorded for the store's repair and for a recovered commit vote's lookup. `incarnation` is the incarnation of
  * the store whose vote counted, None when unknown (encoded as the zero UUID).
  */
final case class UnackedCommitEntry(timestamp: HLCTimestamp, incarnation: Option[StoreIncarnation],
                                    objects: List[ObjectPointer]):
  def encodedSize: Int = HLCTimestamp.EncodedSize + UnackedCommitEntry.IncarnationSize + 4 + objects.map(_.encodedSize + 4).sum

  /** This entry as consecutive entries of the same timestamp, each encoding to at most `maxEncodedSize` bytes and
    * holding at least one object, together holding every object once, in order. Just this entry if it fits (or
    * holds no object). The
    * split depends only on the objects and the limit, so a re-run marker writes the same chunks
    */
  def chunks(maxEncodedSize: Int): List[UnackedCommitEntry] =
    val empty = HLCTimestamp.EncodedSize + UnackedCommitEntry.IncarnationSize + 4
    val (done, last, _) = objects.foldLeft((List[List[ObjectPointer]](), List[ObjectPointer](), empty)):
      case ((done, current, size), ptr) =>
        val ptrSize = ptr.encodedSize + 4
        if current.nonEmpty && size + ptrSize > maxEncodedSize then
          (current.reverse :: done, List(ptr), empty + ptrSize)
        else
          (done, ptr :: current, size + ptrSize)
    if done.isEmpty then List(this) else (last.reverse :: done).reverse.map(UnackedCommitEntry(timestamp, incarnation, _))

  def encode: Array[Byte] =
    val ptrs = objects.map(_.toArray)
    val bb = ByteBuffer.allocate(encodedSize)
    bb.order(ByteOrder.BIG_ENDIAN)
    bb.putLong(timestamp.asLong)
    val inc = incarnation.map(_.uuid).getOrElse(UnackedCommitEntry.ZeroUUID)
    bb.putLong(inc.getMostSignificantBits)
    bb.putLong(inc.getLeastSignificantBits)
    bb.putInt(ptrs.size)
    ptrs.foreach: p =>
      bb.putInt(p.length)
      bb.put(p)
    bb.array()

object UnackedCommitEntry:
  private[transaction] val IncarnationSize = 16
  private[transaction] val ZeroUUID = new UUID(0L, 0L)

  def decode(bytes: Array[Byte]): UnackedCommitEntry =
    val bb = ByteBuffer.wrap(bytes)
    bb.order(ByteOrder.BIG_ENDIAN)
    val ts = HLCTimestamp(bb.getLong())
    val inc = new UUID(bb.getLong(), bb.getLong())
    val count = bb.getInt()
    val objects = List.fill(count):
      val p = new Array[Byte](bb.getInt())
      bb.get(p)
      ObjectPointer.fromArray(p)
    UnackedCommitEntry(ts, if inc == ZeroUUID then None else Some(StoreIncarnation(inc)), objects)

  /** The bytes a pair's key, revision, timestamp and length varints may add to an entry's value in a tree node, at
    * most. Covers the 19-byte chunk key
    */
  private val PairOverheadBound = 64

  /** Largest encoded entry the marker writes into a tree whose leaf nodes are `leafNodeSize` bytes: half a node, less
    * the pair's own overhead, leaving the other half for the node's minimum, maximum and tail pointer and for whatever
    * the node already holds, so an entry always fits a node of its own. A larger one is written as `chunks`
    */
  def maxEncodedSize(leafNodeSize: Int): Int = leafNodeSize / 2 - PairOverheadBound

  /** poolIndex byte then the transaction UUID, big-endian: 17 bytes, like MissedUpdateMarker.errorKey. The key of an
    * entry, or of its first chunk: the one a recovered vote's lookup and the stub logic name
    */
  def key(store: StoreId, txid: TransactionId): Key = key(store, txid, 0)

  /** The key of chunk `chunk` of an entry split by `chunks`: chunk 0 is `key(store, txid)`; a later chunk appends
    * its index as two big-endian bytes, so a transaction's chunks sort together, head first
    */
  def key(store: StoreId, txid: TransactionId, chunk: Int): Key =
    require(chunk >= 0 && chunk <= 0xFFFF, s"chunk index out of range: $chunk")
    val bb = ByteBuffer.allocate(if chunk == 0 then 17 else 19)
    bb.order(ByteOrder.BIG_ENDIAN)
    bb.put(store.poolIndex)
    bb.putLong(txid.uuid.getMostSignificantBits)
    bb.putLong(txid.uuid.getLeastSignificantBits)
    if chunk != 0 then bb.putShort(chunk.toShort)
    Key(bb.array())

  /** Whether `key` is an entry's head: its only chunk, or chunk 0 */
  def isHeadKey(key: Key): Boolean = key.bytes.length == 17

  def decodeTransactionId(key: Key): TransactionId =
    val bb = ByteBuffer.wrap(key.bytes)
    bb.order(ByteOrder.BIG_ENDIAN)
    bb.get()
    TransactionId(new UUID(bb.getLong(), bb.getLong()))
