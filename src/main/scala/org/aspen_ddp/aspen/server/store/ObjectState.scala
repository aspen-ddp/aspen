package org.aspen_ddp.aspen.server.store

import org.aspen_ddp.aspen.common.DataBuffer
import org.aspen_ddp.aspen.common.objects.{Key, Metadata, ObjectId, ObjectType}
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

  /** Holders of a single-holder lock slot that another transaction took over without a check: only recovered commit
    * votes do, as two of them may legitimately share a slot (transaction-requirements.md, recovered transactions
    * re-acquiring locks). The newest entry of a slot is restored when the slot's current holder releases it, so every
    * holder keeps the slot locked until it releases (RequirementsLocker). Empty outside recovery
    */
  var displacedLocks: List[(LockSlot, TransactionId)] = Nil

  /** This in-memory state is no longer served: the object was deleted, or its state may differ from the backend's.
    * Requirement checks fail on it. It is kept only while transactions that hold locks on it reference it. See
    * Frontend.referenced
    */
  var reloadRequired: Boolean = false

  /** The slice growth each lock-holding commit vote that grows this KeyValue object has yet to apply, taken with its
    * locks and released with them (RequirementsLocker). The size check of every other vote, and of every committed
    * apply, counts it (RequirementsChecker.checkKVSliceSize), so the commit votes a store holds together fit the slice
    * limit together and none fails its apply re-check
    */
  var sliceReservations: Map[TransactionId, SliceGrowth] = Map()

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
    displacedLocks.foreach((_, t) => s += t)
    s
  }

  /** Keys a write lock covers: present keys locked to a transaction plus absent-key locks */
  def lockedKeys: Set[Key] =
    kvState match
      case None => Set()
      case Some(kvs) =>
        kvs.content.iterator.collect { case (k, vs) if vs.lockedToTransaction.nonEmpty => k }.toSet ++
          kvs.absentKeyLocks.keySet ++
          displacedLocks.iterator.collect {
            case (LockSlot.Key(k), _) => k
            case (LockSlot.AbsentKey(k), _) => k
          }

  /** True when a write lock covers the whole object */
  def objectLevelLocked: Boolean =
    lockedToTransaction.nonEmpty || kvState.exists(kvs => kvs.contentLocked.nonEmpty || kvs.rangeLocks.nonEmpty) ||
      displacedLocks.exists((slot, _) => slot == LockSlot.Object || slot == LockSlot.Content)

  /** Every transaction holding any lock on this object, shared locks included */
  def lockHolders: Set[TransactionId] = lockedWriteTransactions ++ sharedLocks
}

/** A single-holder lock slot of an object: its exclusive lock (X), a KeyValue object's content lock (C), a present
  * key's lock (K) or an absent key's lock (N)
  */
enum LockSlot:
  case Object
  case Content
  case Key(key: org.aspen_ddp.aspen.common.objects.Key)
  case AbsentKey(key: org.aspen_ddp.aspen.common.objects.Key)
