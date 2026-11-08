package org.aspen_ddp.aspen.server.store

import org.aspen_ddp.aspen.common.objects.{Key, ObjectId, ObjectRevision}
import org.aspen_ddp.aspen.common.transaction.KeyValueUpdate.FullContentLock
import org.aspen_ddp.aspen.common.transaction._
import org.aspen_ddp.aspen.server.store.RequirementsChecker.ObjectErr
import scribe.Logging

object RequirementsLocker extends Logging {

  /** Takes the object's exclusive lock. A passing check means no other transaction holds it, and single-writer
    * guarantees such as the vdisk fence depend on that, so finding one is logged as an error. The lock is still
    * taken
    */
  private def lockExclusive(state: ObjectState, transactionId: TransactionId): Unit = {
    state.lockedToTransaction.foreach { other =>
      if (other != transactionId)
        logger.error(s"Transaction $transactionId takes the exclusive lock on object ${state.objectId} held by $other")
    }
    state.lockedToTransaction = Some(transactionId)
  }

  /** Locks all objects hosted by this store to the transaction. This method may be
    * called ONLY after a call to RequirementsChecker.check returns ZERO errors
    *
    * @param kvWrites What the transaction's KeyValue operations change (KVWrites.from). Written keys and
    *                 object-level fields are locked like requirements on them. unlock must be given the same value
    */
  def lock(transactionId: TransactionId,
           requirements: List[TransactionRequirement],
           objects: Map[ObjectId, ObjectState],
           kvWrites: Map[ObjectId, KVWrites]): Unit = {

    def getState(oid: ObjectId): ObjectState = {
      objects.get(oid) match {
        case None => throw ObjectErr(oid, MissingObject())
        case Some(os) => os
      }
    }

    // A recovered commit vote is kept even when an object failed to load: there is nothing to lock on it
    for (req <- requirements if isLoaded(req, objects)) {
      try {
        req match {
          case r: LocalTimeRequirement =>

          case r: DataUpdate => lockExclusive(getState(r.objectPointer.id), transactionId)

          case r: KeyValueUpdate =>
            val state = getState(r.objectPointer.id)
            lockKV(state, transactionId, r.requiredRevision, r.contentLock, r.requirements)
            kvWrites.get(r.objectPointer.id).foreach(lockWrites(state, transactionId, _))

          case r: RefcountUpdate => lockExclusive(getState(r.objectPointer.id), transactionId)

          case r: VersionBump => lockExclusive(getState(r.objectPointer.id), transactionId)

          case r: RevisionLock => lockExclusive(getState(r.objectPointer.id), transactionId)

          case r: SharedRevisionLock => getState(r.objectPointer.id).sharedLocks += transactionId
        }
      } catch {
        case e: Throwable => logger.error(s"Unexpected error in transaction lock of $transactionId: $e")
      }
    }
  }

  def lockKV(state: ObjectState,
             transactionId: TransactionId,
             requiredRevision: Option[ObjectRevision],
             contentLock: Option[FullContentLock],
             keyRequirements: List[KeyValueUpdate.KeyRequirement]): Unit = {

    requiredRevision.foreach { _ => lockExclusive(state, transactionId) }

    state.kvState match {
      case None => throw ObjectErr(state.objectId, ObjectTypeError())
      case Some(kvs) =>

        contentLock.foreach { _ => kvs.contentLocked = Some(transactionId) }

        for (req <- keyRequirements) {
          req match {

            case _: KeyValueUpdate.WithinRange => kvs.rangeLocks += transactionId

            case r: KeyValueUpdate.KeyRevision => kvs.content.get(r.key).foreach { vs =>
              vs.lockedToTransaction = Some(transactionId)
            }

            case r: KeyValueUpdate.KeyObjectRevision => state.lockedToTransaction = Some(transactionId)

            case r: KeyValueUpdate.Exists => kvs.content.get(r.key).foreach { vs =>
              vs.lockedToTransaction = Some(transactionId)
            }

            case r: KeyValueUpdate.MayExist => lockKey(kvs, r.key, transactionId)

            case r: KeyValueUpdate.DoesNotExist => kvs.absentKeyLocks += (r.key -> transactionId)

            case r: KeyValueUpdate.TimestampEquals => kvs.content.get(r.key).foreach { vs =>
              vs.lockedToTransaction = Some(transactionId)
            }

            case r: KeyValueUpdate.TimestampLessThan => kvs.content.get(r.key).foreach { vs =>
              vs.lockedToTransaction = Some(transactionId)
            }

            case r: KeyValueUpdate.TimestampGreaterThan => kvs.content.get(r.key).foreach { vs =>
              vs.lockedToTransaction = Some(transactionId)
            }
          }
        }
    }
  }

  /** Locks an existing key's value, or an absent key's absence */
  private def lockKey(kvs: KVObjectState, key: Key, transactionId: TransactionId): Unit =
    kvs.content.get(key) match
      case Some(vs) => vs.lockedToTransaction = Some(transactionId)
      case None => kvs.absentKeyLocks += (key -> transactionId)

  /** Releases this transaction's lock on a key, whether the key was present or absent when it was locked: applying
    * the transaction may have inserted or deleted it since
    */
  private def releaseKey(kvs: KVObjectState, key: Key, transactionId: TransactionId): Unit =
    kvs.content.get(key).foreach(vs => releaseValue(vs, transactionId))
    if kvs.absentKeyLocks.get(key).contains(transactionId) then
      kvs.absentKeyLocks -= key

  private def lockWrites(state: ObjectState, transactionId: TransactionId, writes: KVWrites): Unit =
    if writes.objectLevel then
      lockExclusive(state, transactionId)
    state.kvState.foreach(kvs => writes.keys.foreach(lockKey(kvs, _, transactionId)))

  private def unlockWrites(state: ObjectState, transactionId: TransactionId, writes: KVWrites): Unit =
    if writes.objectLevel then
      release(state, transactionId)
    state.kvState.foreach(kvs => writes.keys.foreach(releaseKey(kvs, _, transactionId)))

  private def isLoaded(req: TransactionRequirement, objects: Map[ObjectId, ObjectState]): Boolean = req match
    case r: TransactionObjectRequirement => objects.contains(r.objectPointer.id)
    case _ => true

  private def release(os: ObjectState, transactionId: TransactionId): Unit =
    if os.lockedToTransaction.contains(transactionId) then
      os.lockedToTransaction = None

  private def releaseValue(vs: ValueState, transactionId: TransactionId): Unit =
    if vs.lockedToTransaction.contains(transactionId) then
      vs.lockedToTransaction = None

  /** Releases the locks taken by lock(). Only locks still held by this transaction are released. A committed
    * transaction is applied regardless of other transactions' locks, and its Insert replaces a key's ValueState,
    * dropping the lock on it, so another transaction may since have locked the key.
    */
  def unlock(transactionId: TransactionId,
             requirements: List[TransactionRequirement],
             objects: Map[ObjectId, ObjectState],
             kvWrites: Map[ObjectId, KVWrites]): Unit = {

    def getState(oid: ObjectId): ObjectState = {
      objects.get(oid) match {
        case None => throw ObjectErr(oid, MissingObject())
        case Some(os) => os
      }
    }

    for (req <- requirements if isLoaded(req, objects)) {
      try {
        req match {
          case r: LocalTimeRequirement =>

          case r: DataUpdate => release(getState(r.objectPointer.id), transactionId)

          case r: KeyValueUpdate =>
            val state = getState(r.objectPointer.id)
            unlockKV(state, transactionId, r.requiredRevision, r.contentLock, r.requirements)
            kvWrites.get(r.objectPointer.id).foreach(unlockWrites(state, transactionId, _))

          case r: RefcountUpdate => release(getState(r.objectPointer.id), transactionId)

          case r: VersionBump => release(getState(r.objectPointer.id), transactionId)

          case r: RevisionLock => release(getState(r.objectPointer.id), transactionId)

          case r: SharedRevisionLock => getState(r.objectPointer.id).sharedLocks -= transactionId
        }
      } catch {
        case e: Throwable => logger.error(s"Unexpected error in transaction lock of $transactionId: $e")
      }
    }
  }

  def unlockKV(state: ObjectState,
               transactionId: TransactionId,
               requiredRevision: Option[ObjectRevision],
               contentLock: Option[FullContentLock],
               keyRequirements: List[KeyValueUpdate.KeyRequirement]): Unit = {

    requiredRevision.foreach { rev => release(state, transactionId) }

    state.kvState match {
      case None => throw ObjectErr(state.objectId, ObjectTypeError())
      case Some(kvs) =>

        if contentLock.nonEmpty && kvs.contentLocked.contains(transactionId) then
          kvs.contentLocked = None

        for (req <- keyRequirements) {
          req match {

            case _: KeyValueUpdate.WithinRange => kvs.rangeLocks -= transactionId

            case r: KeyValueUpdate.KeyRevision => kvs.content.get(r.key).foreach(vs => releaseValue(vs, transactionId))

            case r: KeyValueUpdate.KeyObjectRevision => release(state, transactionId)

            case r: KeyValueUpdate.Exists => kvs.content.get(r.key).foreach(vs => releaseValue(vs, transactionId))

            case r: KeyValueUpdate.MayExist => releaseKey(kvs, r.key, transactionId)

            case r: KeyValueUpdate.DoesNotExist =>
              if kvs.absentKeyLocks.get(r.key).contains(transactionId) then
                kvs.absentKeyLocks -= r.key

            case r: KeyValueUpdate.TimestampEquals => kvs.content.get(r.key).foreach(vs => releaseValue(vs, transactionId))

            case r: KeyValueUpdate.TimestampLessThan => kvs.content.get(r.key).foreach(vs => releaseValue(vs, transactionId))

            case r: KeyValueUpdate.TimestampGreaterThan => kvs.content.get(r.key).foreach(vs => releaseValue(vs, transactionId))
          }
        }
    }
  }
}
