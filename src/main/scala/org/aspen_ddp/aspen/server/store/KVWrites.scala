package org.aspen_ddp.aspen.server.store

import org.aspen_ddp.aspen.common.{DataBuffer, HLCTimestamp}
import org.aspen_ddp.aspen.common.objects.{Delete, Insert, Key, KeyValueOperation, ObjectId, ObjectRevision}
import org.aspen_ddp.aspen.common.transaction.{KeyValueUpdate, TransactionId, TransactionRequirement}
import scribe.Logging

/** What a KeyValueUpdate's operations change on one object. A store locks these like requirements: a transaction's
  * operations are applied in each store's own commit order, so two committed transactions writing the same key, or
  * the same object-level field, could otherwise leave the stores with different contents and nothing to repair.
  *
  * @param keys        Keys named by an Insert or Delete
  * @param objectLevel Whether any operation changes the object's minimum, maximum, left or right
  */
final case class KVWrites(keys: Set[Key], objectLevel: Boolean)

object KVWrites extends Logging {

  /** Only the operations' kinds and keys are used, so the revision and timestamp they are decoded with don't matter */
  private val decodeRevision = ObjectRevision(TransactionId(new java.util.UUID(0, 0)))

  def of(ops: List[KeyValueOperation]): KVWrites =
    var keys = Set[Key]()
    var objectLevel = false
    ops.foreach {
      case o: Insert => keys += o.key
      case o: Delete => keys += o.key
      case _ => objectLevel = true
    }
    KVWrites(keys, objectLevel)

  def decode(data: DataBuffer): KVWrites = of(KeyValueOperation.decode(data.asReadOnlyBuffer(), decodeRevision, HLCTimestamp(0)))

  /** The writes of each KeyValueUpdate whose object data is present. An undecodable update is skipped: its check
    * fails, so its transaction never votes commit or takes locks
    */
  def from(requirements: List[TransactionRequirement],
           objectUpdates: Map[ObjectId, DataBuffer]): Map[ObjectId, KVWrites] =
    requirements.iterator.collect { case r: KeyValueUpdate => r.objectPointer.id }.flatMap { oid =>
      objectUpdates.get(oid).flatMap { data =>
        try Some(oid -> decode(data))
        catch
          case e: Throwable =>
            logger.error(s"Undecodable KeyValue operations for object $oid: $e")
            None
      }
    }.toMap
}
