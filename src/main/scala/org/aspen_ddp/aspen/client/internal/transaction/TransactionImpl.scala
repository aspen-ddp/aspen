package org.aspen_ddp.aspen.client.internal.transaction

import java.util.UUID

import org.aspen_ddp.aspen.client.{AbortAfterCommit, AppendPosition, AspenClient, PostCommitTransactionModification, RangedWrite, RangedWriteState, StoragePool, Transaction, TransactionAborted}
import org.aspen_ddp.aspen.common.objects._
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.transaction.KeyValueUpdate.FullContentLock
import org.aspen_ddp.aspen.common.transaction.{FinalizationActionId, KeyValueUpdate, TransactionId}
import org.aspen_ddp.aspen.common.{DataBuffer, HLCTimestamp}
import org.aspen_ddp.aspen.common.ida.IDA
import org.aspen_ddp.aspen.common.pool.PoolId
import scribe.Logging

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.control.NonFatal
import scala.util.{Failure, Success}

object TransactionImpl {
  def Factory(client: AspenClient,
              txManager: TransactionManager,
              chooseDesignatedLeader: (ObjectPointer, IDA) => Byte, // Uses peer online/offline knowledge to select designated leaders for transactions
              transactionDriverStrategy: Option[ClientTransactionDriver.Factory]) = new TransactionImpl(client, txManager, chooseDesignatedLeader, transactionDriverStrategy)
}

class TransactionImpl(val client: AspenClient,
                      txManager: TransactionManager,
                      chooseDesignatedLeader: (ObjectPointer, IDA) => Byte, // Uses peer online/offline knowledge to select designated leaders for transactions
                      transactionDriverStrategy: Option[ClientTransactionDriver.Factory]) extends Transaction with Logging {

  val id: TransactionId = TransactionId(UUID.randomUUID())
  private  val promise = Promise[HLCTimestamp]()
  private  val bldr = new TransactionBuilder(id, chooseDesignatedLeader, client.clientId)
  private  var commitStarted = false
  private  var havePendingUpdates = false
  private  var hooksActive = false // True while the allocation strategies' commit hooks may modify the transaction
  private  var releaseDeferred = false
  private  val attachments = scala.collection.mutable.HashMap[AnyRef, AnyRef]()

  private  val stack = org.aspen_ddp.aspen.common.util.getStack // for debugging

  def isEmpty: Boolean = synchronized { !havePendingUpdates }

  /** Runs `f` against the builder while holding the monitor. Once commit() has been called the
    *  builder is frozen to the caller; it is read without the monitor while the transaction is built.
    */
  private def modify[T](f: TransactionBuilder => T): T = synchronized {
    if commitStarted then
      throw PostCommitTransactionModification()
    f(bldr)
  }

  /** As modify, for the commit hooks via hookView. Permitted only while the hooks run */
  private def hookModify[T](f: TransactionBuilder => T): T = synchronized {
    if !hooksActive then
      throw new IllegalStateException(s"Transaction ${id.uuid} modified by an allocation strategy after its commit hooks finished")
    f(bldr)
  }

  private def access[T](byHook: Boolean)(f: TransactionBuilder => T): T =
    if byHook then hookModify(f) else modify(f)

  // Each operation is shared by this transaction (byHook = false) and hookView (byHook = true)

  private def doAppend(byHook: Boolean, position: AppendPosition, data: DataBuffer): AppendPosition =
    require(data.size > 0, "Cannot append empty data")
    access(byHook) { bldr =>
      havePendingUpdates = true
      bldr.append(position, data)
    }
    AppendPosition(position.pointer, revision, position.length + data.size)

  private def doRangedWrites(byHook: Boolean, state: RangedWriteState, writes: List[RangedWrite],
                             zeroFill: Boolean): Unit = access(byHook) { bldr =>
    bldr.rangedWrites(state, writes, zeroFill)
    havePendingUpdates = true
  }

  private def doOverwrite(byHook: Boolean, objectPointer: DataObjectPointer, requiredRevision: ObjectRevision,
                          data: DataBuffer): Unit = access(byHook) { bldr =>
    havePendingUpdates = true
    bldr.overwrite(objectPointer, requiredRevision, data)
  }

  private def doUpdate(byHook: Boolean,
                       pointer: KeyValueObjectPointer,
                       requiredRevision: Option[ObjectRevision],
                       contentLock: Option[FullContentLock],
                       requirements: List[KeyValueUpdate.KeyRequirement],
                       operations: List[KeyValueOperation]): Unit = access(byHook) { bldr =>
    havePendingUpdates = true
    bldr.update(pointer, requiredRevision, contentLock, requirements, operations)
  }

  private def doSetRefcount(byHook: Boolean, objectPointer: ObjectPointer, requiredRefcount: ObjectRefcount,
                            refcount: ObjectRefcount): Unit = access(byHook) { bldr =>
    havePendingUpdates = true
    bldr.setRefcount(objectPointer, requiredRefcount, refcount)
  }

  private def doBumpVersion(byHook: Boolean, objectPointer: ObjectPointer,
                            requiredRevision: ObjectRevision): Unit = access(byHook) { bldr =>
    havePendingUpdates = true
    bldr.bumpVersion(objectPointer, requiredRevision)
  }

  private def doAddFinalizationAction(byHook: Boolean, finalizationActionId: FinalizationActionId,
                                      serializedContent: Option[Array[Byte]]): Unit = access(byHook) { bldr =>
    serializedContent match {
      case Some(content) => bldr.addFinalizationAction(finalizationActionId, content)
      case None => bldr.addFinalizationAction(finalizationActionId)
    }
  }

  def missedUpdateTrackingEnabled: Boolean = modify(_.missedUpdateTrackingEnabled)

  def referencedPoolIds: Set[PoolId] = modify(_.allReferencedPoolIds)

  def disableMissedUpdateTracking(): Unit = modify(_.disableMissedUpdateTracking())

  def setMissedCommitDelayInMs(msec: Int): Unit = modify(_.setMissedCommitDelayInMs(msec))

  def append(position: AppendPosition, data: DataBuffer): AppendPosition = doAppend(false, position, data)

  def rangedWrites(state: RangedWriteState, writes: List[RangedWrite], zeroFill: Boolean = false): Unit =
    doRangedWrites(false, state, writes, zeroFill)

  def overwrite(objectPointer: DataObjectPointer, requiredRevision: ObjectRevision, data: DataBuffer): Unit =
    doOverwrite(false, objectPointer, requiredRevision, data)

  def update(
              pointer: KeyValueObjectPointer,
              requiredRevision: Option[ObjectRevision],
              contentLock: Option[FullContentLock],
              requirements: List[KeyValueUpdate.KeyRequirement],
              operations: List[KeyValueOperation]): Unit =
    doUpdate(false, pointer, requiredRevision, contentLock, requirements, operations)

  def setRefcount(objectPointer: ObjectPointer, requiredRefcount: ObjectRefcount, refcount: ObjectRefcount): Unit =
    doSetRefcount(false, objectPointer, requiredRefcount, refcount)

  def bumpVersion(objectPointer: ObjectPointer, requiredRevision: ObjectRevision): Unit =
    doBumpVersion(false, objectPointer, requiredRevision)

  def lockRevision(objectPointer: ObjectPointer, requiredRevision: ObjectRevision): Unit =
    modify(_.lockRevision(objectPointer, requiredRevision))

  def ensureHappensAfter(timestamp: HLCTimestamp): Unit = modify(_.ensureHappensAfter(timestamp))

  def addFinalizationAction(finalizationActionId: FinalizationActionId, serializedContent: Option[Array[Byte]]): Unit =
    doAddFinalizationAction(false, finalizationActionId, serializedContent)

  def addAllocatingObject(objectPtr: ObjectPointer): Unit = modify(_.addAllocatingObject(objectPtr))

  def addNotifyOnResolution(storesToNotify: Set[StoreId]): Unit = modify(_.addNotifyOnResolution(storesToNotify))

  def note(note: String): Unit = modify(_.note(note))

  def deferAllocationRelease(): Unit = modify(_ => releaseDeferred = true)

  def allocationReleaseDeferred: Boolean = synchronized { releaseDeferred }

  def attachment[T <: AnyRef](key: AnyRef)(init: => T): T = synchronized {
    attachments.getOrElseUpdate(key, init).asInstanceOf[T]
  }

  def abort(reason: Throwable): Unit = synchronized {
    if commitStarted then
      throw AbortAfterCommit(reason)
    if (!promise.isCompleted)
      promise.failure(reason)
  }

  def abortAndThrow(reason: Throwable): Nothing = {
    abort(reason)
    throw reason
  }

  def result: Future[HLCTimestamp] = promise.future

  /** The transaction as the allocation strategies' commit hooks see it. It may be modified only while the hooks
    *  run, and may not be committed. abort and abortAndThrow throw AbortAfterCommit, as they do for the caller.
    */
  private object hookView extends Transaction:
    def client: AspenClient = TransactionImpl.this.client
    val id: TransactionId = TransactionImpl.this.id

    def append(position: AppendPosition, data: DataBuffer): AppendPosition = doAppend(true, position, data)

    def rangedWrites(state: RangedWriteState, writes: List[RangedWrite], zeroFill: Boolean = false): Unit =
      doRangedWrites(true, state, writes, zeroFill)

    def overwrite(objectPointer: DataObjectPointer, requiredRevision: ObjectRevision, data: DataBuffer): Unit =
      doOverwrite(true, objectPointer, requiredRevision, data)

    def update(pointer: KeyValueObjectPointer,
               requiredRevision: Option[ObjectRevision],
               contentLock: Option[FullContentLock],
               requirements: List[KeyValueUpdate.KeyRequirement],
               operations: List[KeyValueOperation]): Unit =
      doUpdate(true, pointer, requiredRevision, contentLock, requirements, operations)

    def setRefcount(objectPointer: ObjectPointer, requiredRefcount: ObjectRefcount, refcount: ObjectRefcount): Unit =
      doSetRefcount(true, objectPointer, requiredRefcount, refcount)

    def lockRevision(objectPointer: ObjectPointer, requiredRevision: ObjectRevision): Unit =
      hookModify(_.lockRevision(objectPointer, requiredRevision))

    def bumpVersion(objectPointer: ObjectPointer, requiredRevision: ObjectRevision): Unit =
      doBumpVersion(true, objectPointer, requiredRevision)

    def ensureHappensAfter(timestamp: HLCTimestamp): Unit = hookModify(_.ensureHappensAfter(timestamp))

    def addFinalizationAction(finalizationActionId: FinalizationActionId,
                              serializedContent: Option[Array[Byte]]): Unit =
      doAddFinalizationAction(true, finalizationActionId, serializedContent)

    def note(note: String): Unit = hookModify(_.note(note))

    def deferAllocationRelease(): Unit = hookModify(_ => releaseDeferred = true)

    private[aspen] def allocationReleaseDeferred: Boolean = TransactionImpl.this.allocationReleaseDeferred

    private[aspen] def attachment[T <: AnyRef](key: AnyRef)(init: => T): T = TransactionImpl.this.attachment(key)(init)

    def abortAndThrow(reason: Throwable): Nothing = TransactionImpl.this.abortAndThrow(reason)

    def abort(reason: Throwable): Unit = TransactionImpl.this.abort(reason)

    def isEmpty: Boolean = TransactionImpl.this.isEmpty

    def result: Future[HLCTimestamp] = TransactionImpl.this.result

    def commit(): Future[HLCTimestamp] =
      throw new IllegalStateException(s"Transaction ${id.uuid} committed by an allocation strategy's commit hook")

    protected[client] def addAllocatingObject(objectPtr: ObjectPointer): Unit =
      hookModify(_.addAllocatingObject(objectPtr))

    protected[client] def addNotifyOnResolution(storesToNotify: Set[StoreId]): Unit =
      hookModify(_.addNotifyOnResolution(storesToNotify))

    protected[client] def disableMissedUpdateTracking(): Unit = hookModify(_.disableMissedUpdateTracking())

    private[aspen] def missedUpdateTrackingEnabled: Boolean = hookModify(_.missedUpdateTrackingEnabled)

    private[aspen] def referencedPoolIds: Set[PoolId] = hookModify(_.allReferencedPoolIds)

    protected[client] def setMissedCommitDelayInMs(msec: Int): Unit = hookModify(_.setMissedCommitDelayInMs(msec))

  /** Adds any pools referenced by the builder that are missing from `known` */
  private def resolvePools(known: Map[PoolId, StoragePool])(using ExecutionContext): Future[Map[PoolId, StoragePool]] =
    val missing = bldr.allReferencedPoolIds -- known.keySet
    if missing.isEmpty then
      Future.successful(known)
    else
      Future.sequence(missing.toList.map(poolId => client.getStoragePool(poolId).map(pool => poolId -> pool)))
        .map(known ++ _)

  /** The objects this transaction allocates and deletes, grouped by pool */
  private def objectsByPool(): Map[PoolId, (Set[ObjectPointer], Set[ObjectPointer])] =
    val allocated = bldr.allocatedObjects.groupBy(_.poolId)
    val deleted = bldr.deletedObjects.toSet.groupBy(_.poolId)
    (allocated.keySet ++ deleted.keySet).iterator.map: poolId =>
      poolId -> (allocated.getOrElse(poolId, Set.empty), deleted.getOrElse(poolId, Set.empty))
    .toMap

  /** Calls the allocation strategy's prepareCommit for each pool in which the transaction allocates or deletes
    *  objects, then resolves every referenced pool. `prepared` holds what each pool's hook was given.
    *
    *  The hooks may add to the transaction, including objects in pools not yet resolved or prepared, so this repeats
    *  until a pass finds no pool with work its hook hasn't been given. A pool whose hook already ran must not gain
    *  allocations or deletions: that fails with IllegalStateException. Pools whose strategy's `needsCommitHook` is
    *  false are skipped entirely: never called, never recorded, never reported as grown. The hooks run one at a time, through
    *  hookView, so the caller cannot modify the transaction after commit().
    */
  private def runCommitHooks(pools: Map[PoolId, StoragePool],
                             prepared: Map[PoolId, (Set[ObjectPointer], Set[ObjectPointer])])
                            (using ExecutionContext): Future[Map[PoolId, StoragePool]] =
    resolvePools(pools).flatMap: pools =>
      // Pools whose strategy needs no hook are neither called nor recorded, so they can never be reported as grown
      val byPool = objectsByPool().filter((poolId, _) => pools(poolId).allocationStrategy.needsCommitHook)
      val grown = prepared.keys.filter(poolId => byPool(poolId) != prepared(poolId))
      val fresh = byPool.keys.filterNot(prepared.contains).toList
      if grown.nonEmpty then
        Future.failed(new IllegalStateException(s"Transaction ${id.uuid} gained allocations or deletions in " +
          s"pool(s) ${grown.mkString(", ")} after their allocation strategy's prepareCommit ran"))
      else if fresh.isEmpty then
        Future.successful(pools)
      else
        // Sequentially, and each pool's objects read just before its call: an earlier hook may add to a later pool
        fresh.foldLeft(Future.successful(prepared)): (prev, poolId) =>
          prev.flatMap: prepared =>
            val (allocated, deleted) = objectsByPool()(poolId)
            val deletedInOrder = bldr.deletedObjects.filter(deleted.contains)
            val hook =
              try
                pools(poolId).allocationStrategy.prepareCommit(poolId, allocated.toList, deletedInOrder,
                  allocationReleaseDeferred)(using hookView)
              catch
                case NonFatal(e) => Future.failed(e)
            hook.map(_ => prepared + (poolId -> (allocated, deleted)))
        .flatMap(prepared => runCommitHooks(pools, prepared))

  /** Begins the transaction commit process and returns a Future to its completion. This is the same future as
    *  returned by 'result'
    *
    *  The future successfully completes if the transaction commits. Otherwise it will fail with a TransactionError subclass.
    */
  def commit(): Future[HLCTimestamp] = synchronized {

    given ExecutionContext = client.clientContext

    if (!promise.isCompleted && !commitStarted) {
      commitStarted = true

      if bldr.isEmpty then {
        // Completely empty transactions succeed immediately
        promise.success(HLCTimestamp.now)
      } else
        hooksActive = true
        val poolsFuture = runCommitHooks(Map.empty, Map.empty)

        poolsFuture.failed.foreach { cause =>
          synchronized { hooksActive = false }
          promise.tryFailure(cause)
        }

        poolsFuture.foreach { pools =>
          synchronized { hooksActive = false }
          try
            val poolIDAMap = pools.map((poolId, pool) => poolId -> pool.ida)
            val poolMaxObjectSizeMap = pools.map((poolId, pool) => poolId -> pool.maxObjectSize)
            val poolMaxSliceSizeMap = pools.map((poolId, pool) => poolId -> pool.maxSliceSize)
            val (txd, transactionData, _) = bldr.buildTranaction(client.opportunisticRebuildManager, poolIDAMap,
              poolMaxObjectSizeMap, poolMaxSliceSizeMap)

            //---- Tx Debugging ----
            result.onComplete {
              case Success(_) =>
                logger.info(s"TX SUCCESS: ${txd.shortString}\n${txd.shortString}")
              case Failure(e) =>
                logger.info(s"TX FAILURE: ${txd.shortString}: $e\n$stack")
            }
            //---------------------

            if (txd.requirements.isEmpty)
              promise.success(txd.startTimestamp)
            else

              txManager.runTransaction(txd, transactionData, transactionDriverStrategy) onComplete {
                case Failure(cause) =>
                  client.txStatusCache.transactionAborted(txd.transactionId)
                  promise.failure(cause)
                case Success(committed) =>

                  if (committed) {
                    val ts = txd.startTimestamp
                    HLCTimestamp.update(ts)
                    client.txStatusCache.transactionCommitted(txd.transactionId)
                    promise.success(ts)
                  } else {
                    client.txStatusCache.transactionAborted(txd.transactionId)
                    promise.failure(TransactionAborted(txd))
                  }
              }
          catch
            case e: Throwable => promise.tryFailure(e)
        }
    }
    result
  }
}
