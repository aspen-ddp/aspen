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

  /** The locks a commit vote takes (lock): a single-holder slot, a SharedRevisionLock or a KeyValue range lock */
  enum PlannedLock:
    case Slot(slot: LockSlot)
    case Shared
    case Range

  /** The locks lock takes, by object, in the order it takes them. A key requirement or write whose lock depends on
    * whether the key is present takes the slot the state's current content selects. A KeyValue requirement on an
    * object that is not one takes only its required revision's lock, and is passed to `notKV`
    */
  def plannedLocks(requirements: List[TransactionRequirement],
                   objects: Map[ObjectId, ObjectState],
                   kvWrites: Map[ObjectId, KVWrites],
                   notKV: ObjectId => Unit = _ => ()): List[(ObjectId, PlannedLock)] =
    val planned = List.newBuilder[(ObjectId, PlannedLock)]
    def slot(oid: ObjectId, s: LockSlot): Unit = planned += oid -> PlannedLock.Slot(s)
    def keySlot(os: ObjectState, key: Key): Unit =
      os.kvState.foreach { kvs =>
        slot(os.objectId, if kvs.content.contains(key) then LockSlot.Key(key) else LockSlot.AbsentKey(key))
      }

    // A recovered commit vote is kept even when an object failed to load: there is nothing to lock on it
    for req <- requirements do
      req match
        case _: LocalTimeRequirement =>
        case r: TransactionObjectRequirement =>
          objects.get(r.objectPointer.id).foreach { os =>
            val oid = os.objectId
            r match
              case r: KeyValueUpdate =>
                r.requiredRevision.foreach(_ => slot(oid, LockSlot.Object))
                if os.kvState.isEmpty then
                  notKV(oid)
                else
                  r.contentLock.foreach(_ => slot(oid, LockSlot.Content))
                  r.requirements.foreach {
                    case _: KeyValueUpdate.WithinRange => planned += oid -> PlannedLock.Range
                    // A Key slot of an absent key has no holder to take: nothing is locked, as for any present-key
                    // requirement
                    case kr: KeyValueUpdate.KeyRevision => slot(oid, LockSlot.Key(kr.key))
                    case _: KeyValueUpdate.KeyObjectRevision => slot(oid, LockSlot.Object)
                    case kr: KeyValueUpdate.Exists => slot(oid, LockSlot.Key(kr.key))
                    case kr: KeyValueUpdate.MayExist => keySlot(os, kr.key)
                    case kr: KeyValueUpdate.DoesNotExist => slot(oid, LockSlot.AbsentKey(kr.key))
                    case kr: KeyValueUpdate.TimestampEquals => slot(oid, LockSlot.Key(kr.key))
                    case kr: KeyValueUpdate.TimestampLessThan => slot(oid, LockSlot.Key(kr.key))
                    case kr: KeyValueUpdate.TimestampGreaterThan => slot(oid, LockSlot.Key(kr.key))
                  }
                  kvWrites.get(oid).foreach { w =>
                    if w.objectLevel then slot(oid, LockSlot.Object)
                    w.keys.foreach(keySlot(os, _))
                  }
              case _: SharedRevisionLock => planned += oid -> PlannedLock.Shared
              case _ => slot(oid, LockSlot.Object)
          }
    planned.result()

  /** Locks all objects hosted by this store to the transaction. This method may be
    * called ONLY after a call to RequirementsChecker.check returns ZERO errors
    *
    * @param kvWrites What the transaction's KeyValue operations change (KVWrites.from). Written keys and
    *                 object-level fields are locked like requirements on them. unlock must be given the same value
    * @param growth   The slice growth the transaction's KeyValue updates have yet to apply
    *                 (RequirementsChecker.sliceGrowth), reserved on each object until unlock
    *                 (ObjectState.sliceReservations)
    */
  def lock(transactionId: TransactionId,
           requirements: List[TransactionRequirement],
           objects: Map[ObjectId, ObjectState],
           kvWrites: Map[ObjectId, KVWrites],
           recovered: Boolean = false,
           growth: Map[ObjectId, SliceGrowth] = Map()): Unit =
    val notKV = (oid: ObjectId) =>
      logger.error(s"Unexpected error in transaction lock of $transactionId: ${ObjectErr(oid, ObjectTypeError())}")
    plannedLocks(requirements, objects, kvWrites, notKV).foreach { (oid, planned) =>
      val state = objects(oid)
      planned match
        case PlannedLock.Slot(s) => take(state, s, transactionId, recovered)
        case PlannedLock.Shared => state.sharedLocks += transactionId
        case PlannedLock.Range => state.kvState.foreach(_.rangeLocks += transactionId)
    }
    growth.foreach((oid, g) => objects.get(oid).foreach(os => os.sliceReservations += transactionId -> g))

  /** Releases this transaction's lock on a key, whether the key was present or absent when it was locked: applying
    * the transaction may have inserted or deleted it since
    */
  private def releaseKey(state: ObjectState, key: Key, transactionId: TransactionId): Unit =
    give(state, LockSlot.Key(key), transactionId)
    give(state, LockSlot.AbsentKey(key), transactionId)

  private def unlockWrites(state: ObjectState, transactionId: TransactionId, writes: KVWrites): Unit =
    if writes.objectLevel then
      release(state, transactionId)
    writes.keys.foreach(releaseKey(state, _, transactionId))

  private def isLoaded(req: TransactionRequirement, objects: Map[ObjectId, ObjectState]): Boolean = req match
    case r: TransactionObjectRequirement => objects.contains(r.objectPointer.id)
    case _ => true

  private def release(os: ObjectState, transactionId: TransactionId): Unit =
    give(os, LockSlot.Object, transactionId)

  /** Releases the locks taken by lock(), and the slice growth reserved with them. Only locks still held by this transaction are released, and a slot it
    * took from another holder goes back to that holder (`give`). A committed transaction is applied regardless of
    * other transactions' locks, and its Delete removes a key with the lock on it, so another transaction may since
    * have locked the key. An applied Insert keeps the replaced ValueState's lock (RequirementsApplyer.apply).
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
            state.sliceReservations -= transactionId
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
