package org.aspen_ddp.aspen.client

import org.aspen_ddp.aspen.common.{DataBuffer, HLCTimestamp}
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.objects.{DataObjectPointer, KeyValueObjectPointer, KeyValueOperation, ObjectId, ObjectPointer, ObjectRefcount, ObjectRevision}
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.transaction.KeyValueUpdate.FullContentLock
import org.aspen_ddp.aspen.common.transaction.{FinalizationActionId, KeyValueUpdate, TransactionId}

import scala.concurrent.Future

trait Transaction {

  def client: AspenClient

  val id: TransactionId

  def revision: ObjectRevision = ObjectRevision(id)

  // All returns are what the new object revision/refcount will be if the transaction completes successfully

  /** Appends data to the object at the given position. Returns the position that holds once this transaction
    * commits. Throws IllegalArgumentException if data is empty. If the object's pool IDA does not support
    * appends, building the transaction fails with AppendNotSupported.
    */
  def append(position: AppendPosition, data: DataBuffer): AppendPosition

  /** Overwrites byte ranges of the object described by `state`, sending only the changed bytes. The update
    * requires the state's revision, so a stale state aborts the transaction. Writes may be given in any order.
    * A write may extend the object. One starting beyond its current end requires `zeroFill`, which fills the gap
    * with zeros.
    *
    * Throws IllegalArgumentException if `writes` is empty, any write is empty or has a negative offset, writes
    * overlap (touching is fine), or `state` lacks the current bytes under a write. Building the transaction fails
    * with RangeWriteNotSupported if the pool's IDA does not support ranged writes (classic ReedSolomon),
    * RangeWriteBeyondEnd for a gap without `zeroFill`, and ObjectSizeLimitExceeded if the result would exceed the
    * pool's derived maxObjectSize (which never exceeds the IDA's length limit).
    *
    * The caller's arrays (`RangedWrite.data` and any state pieces) are held by reference until the transaction
    * commits and must not be modified.
    */
  def rangedWrites(state: RangedWriteState, writes: List[RangedWrite], zeroFill: Boolean = false): Unit

  def rangedWrite(state: RangedWriteState, write: RangedWrite, zeroFill: Boolean = false): Unit =
    rangedWrites(state, List(write), zeroFill)

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



  /** Asks the allocation strategies of the pools this transaction deletes from to release what they reserved for the
   *  deleted objects after the transaction commits, instead of within it. Intended for bulk deletes, where releasing
   *  in-transaction (e.g. clearing many bits of a shared allocation index) would risk collisions with concurrent
   *  allocations. A no-op for ordinary pools. Applies to every deletion in the transaction.
   *
   *  FSIF pools support deferred deletions in only one FSIF pool per transaction: deferred deletions in a second
   *  FSIF pool fail the transaction with IllegalArgumentException.
   *
   *  Throws PostCommitTransactionModification if commit() has already been called.
   */
  def deferAllocationRelease(): Unit

  /** True if deferAllocationRelease() has been called. Passed to each allocation strategy's prepareCommit */
  private[aspen] def allocationReleaseDeferred: Boolean

  /** Per-transaction state for allocation strategies. Returns the value stored under `key`, first storing `init` if
   *  there is none; `init` runs at most once per key. Synchronized, and usable both before commit() and while the
   *  commit hooks run.
   */
  private[aspen] def attachment[T <: AnyRef](key: AnyRef)(init: => T): T

  /** Adds a human-readable note that may be used for debugging transactions */
  def note(note: String): Unit

  /** Permanently marks the transaction as unable to commit and throws `reason`.
   *
   *  Use this when a precondition check fails while building a transaction. Throwing unwinds
   *  the builder so no further work -- allocations in particular -- is performed against a
   *  transaction that can never commit. Only the first reason is propagated: if the
   *  transaction is already aborted, this always throws its own `reason`, not the earlier one.
   *
   *  Throws AbortAfterCommit instead if commit() has already been called.
   */
  def abortAndThrow(reason: Throwable): Nothing

  /** Permanently marks the transaction as unable to commit, without throwing.
   *
   *  For recording a failure that has already been raised elsewhere, such as from a callback
   *  on an already-failed Future. Prefer abortAndThrow when this is the site that detects the
   *  failure. Only the first reason is propagated.
   *
   *  Throws AbortAfterCommit if commit() has already been called: once the commit process has
   *  begun the outcome is decided by the stores, so calling abort() at that point is a logic error.
   */
  def abort(reason: Throwable): Unit

  /** True if no updates have been added to the transaction. Says nothing about whether the
   *  transaction has been aborted -- inspect `result` for that.
   */
  def isEmpty: Boolean


  def result: Future[HLCTimestamp]

  /** Begins the transaction commit process and returns a Future to its completion. This is the same future as
    *  returned by 'result'
    *
    *  The future successfully completes if the transaction commits. Otherwise it will fail with a TransactionError subclass.
    */
  def commit(): Future[HLCTimestamp]

  protected[client] def addAllocatingObject(objectPtr: ObjectPointer): Unit

  /** Used by allocations to ensure they receive notice of the allocating transaction result
    */
  protected[client] def addNotifyOnResolution(storesToNotify: Set[StoreId]): Unit

  /** Used by AspenClient.newRepairTransaction and, via MissedUpdateFinalizationAction.limitTracking,
    *  by ErrorTree writes that reference the pool the entry is about. Such a write could miss an
    *  update on the very tree it writes, a circular loop. ErrorTree writes for other pools keep
    *  tracking since their trees live in other pools.
    *  This method should NOT be used for any other purposes.
    *
    */
  protected[client] def disableMissedUpdateTracking(): Unit

  /** True unless disableMissedUpdateTracking() has been called. Exists so the repair service's
    *  transaction factory is testable; the flag is otherwise write-only.
    */
  private[aspen] def missedUpdateTrackingEnabled: Boolean

  /** The ids of every pool holding an object this transaction requires, updates or allocates */
  private[aspen] def referencedPoolIds: Set[PoolId]

  /** Sets the delay in Milliseconds after which peers will be marked as having missed the commit of the transaction
    */
  protected[client] def setMissedCommitDelayInMs(msec: Int): Unit

}
