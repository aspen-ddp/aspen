package org.aspen_ddp.aspen.client

import org.aspen_ddp.aspen.common.{DataBuffer, HLCTimestamp}
import org.aspen_ddp.aspen.common.objects.{DataObjectPointer, KeyValueObjectPointer, KeyValueOperation, ObjectPointer, ObjectRefcount, ObjectRevision}
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.transaction.KeyValueUpdate.FullContentLock
import org.aspen_ddp.aspen.common.transaction.{FinalizationActionId, KeyValueUpdate, TransactionId}

import scala.concurrent.Future

trait Transaction {

  def client: AspenClient

  val id: TransactionId

  def revision: ObjectRevision = ObjectRevision(id)

  // All returns are what the new object revision/refcount will be if the transaction completes successfully
  def append(objectPointer: DataObjectPointer,
             requiredRevision: ObjectRevision,
             data: DataBuffer): Unit

  def overwrite(objectPointer: DataObjectPointer,
                requiredRevision: ObjectRevision,
                data: DataBuffer): Unit

  def update(pointer: KeyValueObjectPointer,
             requiredRevision: Option[ObjectRevision],
             contentLock: Option[FullContentLock],
             requirements: List[KeyValueUpdate.KeyRequirement],
             operations: List[KeyValueOperation]): Unit

  def setRefcount(objectPointer: ObjectPointer,
                  requiredRefcount: ObjectRefcount,
                  refcount: ObjectRefcount): Unit

  /** Ensures the object revision matches the specified value and blocks other transactions attempting to modify the
    *  revision while this transaction is being executed.
    */
  def lockRevision(objectPointer: ObjectPointer, requiredRevision: ObjectRevision): Unit

  /** Increments the overwrite count on the object revision by 1 but leaves the object data untouched */
  def bumpVersion(objectPointer: ObjectPointer, requiredRevision: ObjectRevision): Unit

  def ensureHappensAfter(timestamp: HLCTimestamp): Unit

  def addFinalizationAction(finalizationActionId: FinalizationActionId,
                            serializedContent: Option[Array[Byte]]): Unit



  /** Adds a human-readable note that may be used for debugging transactions */
  def note(note: String): Unit

  /** Only the first error will be propagated should multiple attempts are made to invalidate the transaction
   *
   */
  def invalidateTransaction(reason: Throwable): Unit

  /** True if one or more updates have been added to the transaction and it has not been invalidated */
  def valid: Boolean

  def result: Future[HLCTimestamp]

  /** Begins the transaction commit process and returns a Future to its completion. This is the same future as
    *  returned by 'result'
    *
    *  The future successfully completes if the transaction commits. Otherwise it will fail with a TransactionError subclass.
    */
  def commit(): Future[HLCTimestamp]

  /** Used by allocations to ensure they receive notice of the allocating transaction result
    */
  protected[client] def addNotifyOnResolution(storesToNotify: Set[StoreId]): Unit

  /** Used by MissedUpdateFinalizationActions to prevent circular loops when marking objects as having missed update transactions.
    *  This method should NOT be used for any other purposes.
    *
    */
  protected[client] def disableMissedUpdateTracking(): Unit

  /** Sets the delay in Milliseconds after which peers will be marked as having missed the commit of the transaction
    */
  protected[client] def setMissedCommitDelayInMs(msec: Int): Unit

}
