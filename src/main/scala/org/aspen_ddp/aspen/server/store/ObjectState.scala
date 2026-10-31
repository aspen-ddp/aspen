package org.aspen_ddp.aspen.server.store

import org.aspen_ddp.aspen.common.DataBuffer
import org.aspen_ddp.aspen.common.objects.{Metadata, ObjectId, ObjectType}
import org.aspen_ddp.aspen.common.transaction.TransactionId

class ObjectState(val objectId: ObjectId,
                  var metadata: Metadata,
                  val objectType: ObjectType.Value,
                  var data: DataBuffer) {

  /** Used to track the number of references currently working on this object.
    * The object is not allowed to exit the cache until this number drops to
    * zero.
    */
  var transactionReferences: Int = 0

  var lockedToTransaction: Option[TransactionId] = None

  /** Transactions holding a SharedRevisionLock on this object */
  var sharedLocks: Set[TransactionId] = Set()

  /** System.nanoTime deadline before which new SharedRevisionLocks are refused, 0 = none. Set when an exclusive
    * requirement collides with held shared locks. See SharedRevisionLock.ExclusivePreferenceWindow
    */
  var exclusiveWaitUntil: Long = 0

  var kvState: Option[KVObjectState] = objectType match {
    case ObjectType.KeyValue => Some(KVObjectState(data))
    case _ => None
  }

  def lockedWriteTransactions: Set[TransactionId] = {
    var s = Set[TransactionId]()
    lockedToTransaction.foreach { t => s += t }
    s ++= sharedLocks
    kvState.foreach { kvs =>
      kvs.content.values.foreach { vs =>
        vs.lockedToTransaction.foreach { txid =>
          s += txid
        }
      }
      s ++= kvs.noExistenceLocks.values
      kvs.contentLocked.foreach { t => s += t }
      s ++= kvs.rangeLocks
    }
    s
  }
}
