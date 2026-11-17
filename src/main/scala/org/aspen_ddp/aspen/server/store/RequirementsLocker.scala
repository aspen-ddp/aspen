package org.aspen_ddp.aspen.server.store

import org.aspen_ddp.aspen.common.objects.{Key, ObjectId, ObjectRevision}
import org.aspen_ddp.aspen.common.transaction.KeyValueUpdate.FullContentLock
import org.aspen_ddp.aspen.common.transaction._
import org.aspen_ddp.aspen.server.store.RequirementsChecker.ObjectErr
import scribe.Logging

object RequirementsLocker extends Logging {

  /** The current holder of a single-holder lock slot. A Key slot of an absent key has none */
  private def holder(state: ObjectState, slot: LockSlot): Option[TransactionId] = slot match
    case LockSlot.Object => state.lockedToTransaction
    case LockSlot.Content => state.kvState.flatMap(_.contentLocked)
    case LockSlot.Key(k) => state.kvState.flatMap(_.content.get(k)).flatMap(_.lockedToTransaction)
    case LockSlot.AbsentKey(k) => state.kvState.flatMap(_.absentKeyLocks.get(k))

  private def setHolder(state: ObjectState, slot: LockSlot, h: Option[TransactionId]): Unit = slot match
    case LockSlot.Object => state.lockedToTransaction = h
    case LockSlot.Content => state.kvState.foreach(_.contentLocked = h)
    case LockSlot.Key(k) => state.kvState.flatMap(_.content.get(k)).foreach(_.lockedToTransaction = h)
    case LockSlot.AbsentKey(k) => state.kvState.foreach { kvs =>
      h match
        case Some(t) => kvs.absentKeyLocks += (k -> t)
        case None => kvs.absentKeyLocks -= k
    }

  /** Takes a single-holder lock slot. For a live commit vote the preceding check guarantees no other holder, and
    * single-writer guarantees such as the vdisk fence depend on that, so finding one on the exclusive lock is logged
    * as an error. A recovered commit vote takes its locks without a check, and two recovered votes may share a slot
    * (transaction-requirements.md, recovered transactions re-acquiring locks). Either way the other holder is kept in
    * `ObjectState.displacedLocks` and gets the slot back when this transaction releases it (`give`), so the slot
    * stays locked until every holder has released it
    */
  private def take(state: ObjectState, slot: LockSlot, transactionId: TransactionId, recovered: Boolean): Unit =
    holder(state, slot).foreach { other =>
      if other != transactionId then
        if slot == LockSlot.Object && !recovered then
          logger.error(s"Transaction $transactionId takes the exclusive lock on object ${state.objectId} held by $other")
        else
          logger.info(s"Transaction $transactionId takes lock $slot on object ${state.objectId} also held by $other")
        state.displacedLocks = (slot, other) :: state.displacedLocks
    }
    setHolder(state, slot, Some(transactionId))

  /** Releases this transaction's hold on a single-holder slot. The current holder passes the slot to the newest
    * holder it displaced (`take`), if any; a displaced holder only drops its entry
    */
  private def give(state: ObjectState, slot: LockSlot, transactionId: TransactionId): Unit =
    if holder(state, slot).contains(transactionId) then
      state.displacedLocks.find(_._1 == slot) match
        case Some(entry @ (_, next)) =>
          state.displacedLocks = state.displacedLocks.diff(List(entry))
          setHolder(state, slot, Some(next))
        case None => setHolder(state, slot, None)
    else if state.displacedLocks.nonEmpty then
      state.displacedLocks = state.displacedLocks.filterNot(_ == (slot, transactionId))

  /** Locks all objects hosted by this store to the transaction. This method may be
    * called ONLY after a call to RequirementsChecker.check returns ZERO errors
    *
    * @param kvWrites What the transaction's KeyValue operations change (KVWrites.from). Written keys and
    *                 object-level fields are locked like requirements on them. unlock must be given the same value
    */
  def lock(transactionId: TransactionId,
           requirements: List[TransactionRequirement],
           objects: Map[ObjectId, ObjectState],
           kvWrites: Map[ObjectId, KVWrites],
           recovered: Boolean = false): Unit = {

    def lockExclusive(state: ObjectState, transactionId: TransactionId): Unit =
      take(state, LockSlot.Object, transactionId, recovered)

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
            lockKV(state, transactionId, r.requiredRevision, r.contentLock, r.requirements, recovered)
            kvWrites.get(r.objectPointer.id).foreach(lockWrites(state, transactionId, _, recovered))

          case r: RefcountUpdate => lockExclusive(getState(r.objectPointer.id), transactionId)

          case r: VersionBump => lockExclusive(getState(r.objectPointer.id), transactionId)

          case r: ExclusiveRevisionLock => lockExclusive(getState(r.objectPointer.id), transactionId)

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
             keyRequirements: List[KeyValueUpdate.KeyRequirement],
             recovered: Boolean = false): Unit = {

    def takeSlot(slot: LockSlot): Unit = take(state, slot, transactionId, recovered)

    requiredRevision.foreach { _ => takeSlot(LockSlot.Object) }

    state.kvState match {
      case None => throw ObjectErr(state.objectId, ObjectTypeError())
      case Some(kvs) =>

        contentLock.foreach { _ => takeSlot(LockSlot.Content) }

        for (req <- keyRequirements) {
          req match {

            case _: KeyValueUpdate.WithinRange => kvs.rangeLocks += transactionId

            // A Key slot of an absent key has no holder to take: nothing is locked, as for any present-key requirement
            case r: KeyValueUpdate.KeyRevision => takeSlot(LockSlot.Key(r.key))

            case r: KeyValueUpdate.KeyObjectRevision => takeSlot(LockSlot.Object)

            case r: KeyValueUpdate.Exists => takeSlot(LockSlot.Key(r.key))

            case r: KeyValueUpdate.MayExist => lockKey(state, r.key, transactionId, recovered)

            case r: KeyValueUpdate.DoesNotExist => takeSlot(LockSlot.AbsentKey(r.key))

            case r: KeyValueUpdate.TimestampEquals => takeSlot(LockSlot.Key(r.key))

            case r: KeyValueUpdate.TimestampLessThan => takeSlot(LockSlot.Key(r.key))

            case r: KeyValueUpdate.TimestampGreaterThan => takeSlot(LockSlot.Key(r.key))
          }
        }
    }
  }

  /** Locks an existing key's value, or an absent key's absence */
  private def lockKey(state: ObjectState, key: Key, transactionId: TransactionId, recovered: Boolean): Unit =
    state.kvState.foreach { kvs =>
      val slot = if kvs.content.contains(key) then LockSlot.Key(key) else LockSlot.AbsentKey(key)
      take(state, slot, transactionId, recovered)
    }

  /** Releases this transaction's lock on a key, whether the key was present or absent when it was locked: applying
    * the transaction may have inserted or deleted it since
    */
  private def releaseKey(state: ObjectState, key: Key, transactionId: TransactionId): Unit =
    give(state, LockSlot.Key(key), transactionId)
    give(state, LockSlot.AbsentKey(key), transactionId)

  private def lockWrites(state: ObjectState, transactionId: TransactionId, writes: KVWrites, recovered: Boolean): Unit =
    if writes.objectLevel then
      take(state, LockSlot.Object, transactionId, recovered)
    writes.keys.foreach(lockKey(state, _, transactionId, recovered))

  private def unlockWrites(state: ObjectState, transactionId: TransactionId, writes: KVWrites): Unit =
    if writes.objectLevel then
      release(state, transactionId)
    writes.keys.foreach(releaseKey(state, _, transactionId))

  private def isLoaded(req: TransactionRequirement, objects: Map[ObjectId, ObjectState]): Boolean = req match
    case r: TransactionObjectRequirement => objects.contains(r.objectPointer.id)
    case _ => true

  private def release(os: ObjectState, transactionId: TransactionId): Unit =
    give(os, LockSlot.Object, transactionId)

  /** Releases the locks taken by lock(). Only locks still held by this transaction are released, and a slot it
    * took from another holder goes back to that holder (`give`). A committed transaction is applied regardless of
    * other transactions' locks, and its Insert replaces a key's ValueState, dropping the lock on it, so another
    * transaction may since have locked the key.
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

          case r: ExclusiveRevisionLock => release(getState(r.objectPointer.id), transactionId)

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

        if contentLock.nonEmpty then
          give(state, LockSlot.Content, transactionId)

        for (req <- keyRequirements) {
          req match {

            case _: KeyValueUpdate.WithinRange => kvs.rangeLocks -= transactionId

            case r: KeyValueUpdate.KeyRevision => give(state, LockSlot.Key(r.key), transactionId)

            case r: KeyValueUpdate.KeyObjectRevision => release(state, transactionId)

            case r: KeyValueUpdate.Exists => give(state, LockSlot.Key(r.key), transactionId)

            case r: KeyValueUpdate.MayExist => releaseKey(state, r.key, transactionId)

            case r: KeyValueUpdate.DoesNotExist => give(state, LockSlot.AbsentKey(r.key), transactionId)

            case r: KeyValueUpdate.TimestampEquals => give(state, LockSlot.Key(r.key), transactionId)

            case r: KeyValueUpdate.TimestampLessThan => give(state, LockSlot.Key(r.key), transactionId)

            case r: KeyValueUpdate.TimestampGreaterThan => give(state, LockSlot.Key(r.key), transactionId)
          }
        }
    }
  }
}
