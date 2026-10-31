package org.aspen_ddp.aspen.server.store

import org.aspen_ddp.aspen.common.objects.{ObjectId, ObjectRevision}
import org.aspen_ddp.aspen.common.transaction.KeyValueUpdate.FullContentLock
import org.aspen_ddp.aspen.common.transaction._
import org.aspen_ddp.aspen.server.store.RequirementsChecker.ObjectErr

object RequirementsLocker {

  /** Locks all objects hosted by this store to the transaction. This method may be
    * called ONLY after a call to RequirementsChecker.check returns ZERO errors
    */
  def lock(transactionId: TransactionId,
           requirements: List[TransactionRequirement],
           objects: Map[ObjectId, ObjectState]): Unit = {

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

          case r: DataUpdate => getState(r.objectPointer.id).lockedToTransaction = Some(transactionId)

          case r: KeyValueUpdate =>
            lockKV(getState(r.objectPointer.id), transactionId, r.requiredRevision, r.contentLock, r.requirements)

          case r: RefcountUpdate => getState(r.objectPointer.id).lockedToTransaction = Some(transactionId)

          case r: VersionBump => getState(r.objectPointer.id).lockedToTransaction = Some(transactionId)

          case r: RevisionLock => getState(r.objectPointer.id).lockedToTransaction = Some(transactionId)

          case r: SharedRevisionLock => getState(r.objectPointer.id).sharedLocks += transactionId
        }
      } catch {
        case e: Throwable => println(s"UNEXPECTED ERROR IN TX Lock: $e")
      }
    }
  }

  def lockKV(state: ObjectState,
             transactionId: TransactionId,
             requiredRevision: Option[ObjectRevision],
             contentLock: Option[FullContentLock],
             keyRequirements: List[KeyValueUpdate.KeyRequirement]): Unit = {

    requiredRevision.foreach { rev => state.lockedToTransaction = Some(transactionId) }

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

            case r: KeyValueUpdate.MayExist => kvs.content.get(r.key).foreach { vs =>
              vs.lockedToTransaction = Some(transactionId)
            }

            case r: KeyValueUpdate.DoesNotExist => kvs.noExistenceLocks += (r.key -> transactionId)

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
             objects: Map[ObjectId, ObjectState]): Unit = {

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
            unlockKV(getState(r.objectPointer.id), transactionId, r.requiredRevision, r.contentLock, r.requirements)

          case r: RefcountUpdate => release(getState(r.objectPointer.id), transactionId)

          case r: VersionBump => release(getState(r.objectPointer.id), transactionId)

          case r: RevisionLock => release(getState(r.objectPointer.id), transactionId)

          case r: SharedRevisionLock => getState(r.objectPointer.id).sharedLocks -= transactionId
        }
      } catch {
        case e: Throwable => println(s"UNEXPECTED ERROR IN TX Lock: $e")
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

            case r: KeyValueUpdate.MayExist => kvs.content.get(r.key).foreach(vs => releaseValue(vs, transactionId))

            case r: KeyValueUpdate.DoesNotExist =>
              if kvs.noExistenceLocks.get(r.key).contains(transactionId) then
                kvs.noExistenceLocks -= r.key

            case r: KeyValueUpdate.TimestampEquals => kvs.content.get(r.key).foreach(vs => releaseValue(vs, transactionId))

            case r: KeyValueUpdate.TimestampLessThan => kvs.content.get(r.key).foreach(vs => releaseValue(vs, transactionId))

            case r: KeyValueUpdate.TimestampGreaterThan => kvs.content.get(r.key).foreach(vs => releaseValue(vs, transactionId))
          }
        }
    }
  }
}
