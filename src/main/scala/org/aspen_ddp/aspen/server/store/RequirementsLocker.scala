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

    for (req <- requirements) {
      try {
        req match {
          case r: LocalTimeRequirement =>

          case r: DataUpdate => getState(r.objectPointer.id).lockedToTransaction = Some(transactionId)

          case r: KeyValueUpdate =>
            lockKV(getState(r.objectPointer.id), transactionId, r.requiredRevision, r.contentLock, r.requirements)

          case r: RefcountUpdate => getState(r.objectPointer.id).lockedToTransaction = Some(transactionId)

          case r: VersionBump => getState(r.objectPointer.id).lockedToTransaction = Some(transactionId)

          case r: RevisionLock => getState(r.objectPointer.id).lockedToTransaction = Some(transactionId)
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

  def unlock(transactionId: TransactionId,
             requirements: List[TransactionRequirement],
             objects: Map[ObjectId, ObjectState]): Unit = {

    def getState(oid: ObjectId): ObjectState = {
      objects.get(oid) match {
        case None => throw ObjectErr(oid, MissingObject())
        case Some(os) => os
      }
    }

    for (req <- requirements) {
      try {
        req match {
          case r: LocalTimeRequirement =>

          case r: DataUpdate => getState(r.objectPointer.id).lockedToTransaction = None

          case r: KeyValueUpdate =>
            unlockKV(getState(r.objectPointer.id), transactionId, r.requiredRevision, r.contentLock, r.requirements)

          case r: RefcountUpdate => getState(r.objectPointer.id).lockedToTransaction = None

          case r: VersionBump => getState(r.objectPointer.id).lockedToTransaction = None

          case r: RevisionLock => getState(r.objectPointer.id).lockedToTransaction = None
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

    requiredRevision.foreach { rev => state.lockedToTransaction = None }

    state.kvState match {
      case None => throw ObjectErr(state.objectId, ObjectTypeError())
      case Some(kvs) =>

        contentLock.foreach { _ => kvs.contentLocked = None }

        for (req <- keyRequirements) {
          req match {

            case _: KeyValueUpdate.WithinRange => kvs.rangeLocks -= transactionId

            case r: KeyValueUpdate.KeyRevision => kvs.content.get(r.key).foreach { vs =>
              vs.lockedToTransaction = None
            }

            case r: KeyValueUpdate.KeyObjectRevision => state.lockedToTransaction = None

            case r: KeyValueUpdate.Exists => kvs.content.get(r.key).foreach { vs =>
              vs.lockedToTransaction = None
            }

            case r: KeyValueUpdate.MayExist => kvs.content.get(r.key).foreach { vs =>
              vs.lockedToTransaction = None
            }

            case r: KeyValueUpdate.DoesNotExist => kvs.noExistenceLocks -= r.key

            case r: KeyValueUpdate.TimestampEquals => kvs.content.get(r.key).foreach { vs =>
              vs.lockedToTransaction = None
            }

            case r: KeyValueUpdate.TimestampLessThan => kvs.content.get(r.key).foreach { vs =>
              vs.lockedToTransaction = None
            }

            case r: KeyValueUpdate.TimestampGreaterThan => kvs.content.get(r.key).foreach { vs =>
              vs.lockedToTransaction = None
            }
          }
        }
    }
  }
}
