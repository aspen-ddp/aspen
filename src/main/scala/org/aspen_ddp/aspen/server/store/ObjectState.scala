package org.aspen_ddp.aspen.server.store

import org.aspen_ddp.aspen.common.DataBuffer
import org.aspen_ddp.aspen.common.objects.{Metadata, ObjectId, ObjectType}
import org.aspen_ddp.aspen.common.transaction.TransactionId

class ObjectState(val objectId: ObjectId,
                  var metadata: Metadata,
                  val objectType: ObjectType.Value,
                  var data: DataBuffer) {

  /** Transactions and in-flight backend commits holding this state. Touched only on the owning store's executor.
    * See Frontend.retain
    */
  var references: Int = 0

  var lockedToTransaction: Option[TransactionId] = None

  /** This in-memory state is no longer served: the object was deleted, or its state may differ from the backend's.
    * Requirement checks fail on it. It is kept only while transactions that hold locks on it reference it. See
    * Frontend.referenced
    */
  var reloadRequired: Boolean = false

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

  /** Transactions holding a lock that lets them change this object. Reported to readers, which wait for a store
    * to apply such a transaction once they know it committed. SharedRevisionLock holders are excluded: they never
    * change the object
    */
  def lockedWriteTransactions: Set[TransactionId] = {
    var s = Set[TransactionId]()
    lockedToTransaction.foreach { t => s += t }
    kvState.foreach { kvs =>
      kvs.content.values.foreach { vs =>
        vs.lockedToTransaction.foreach { txid =>
          s += txid
        }
      }
      s ++= kvs.absentKeyLocks.values
      kvs.contentLocked.foreach { t => s += t }
      s ++= kvs.rangeLocks
    }
    s
  }

  /** Every transaction holding any lock on this object, shared locks included */
  def lockHolders: Set[TransactionId] = lockedWriteTransactions ++ sharedLocks
}
