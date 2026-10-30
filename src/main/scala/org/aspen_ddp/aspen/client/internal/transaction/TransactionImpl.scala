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
  private  var runningCommitHooks = false // Permits modification by allocation strategies after commit()

  private  val stack = org.aspen_ddp.aspen.common.util.getStack // for debugging

  def isEmpty: Boolean = synchronized { !havePendingUpdates }

  /** Runs `f` against the builder while holding the monitor. Once commit() has been called the
    *  builder is frozen, except to the commit hooks; it is read without the monitor while the transaction is built.
    */
  private def modify[T](f: TransactionBuilder => T): T = synchronized {
    if commitStarted && !runningCommitHooks then
      throw PostCommitTransactionModification()
    f(bldr)
  }

  def missedUpdateTrackingEnabled: Boolean = modify { bldr =>
    bldr.missedUpdateTrackingEnabled
  }

  def referencedPoolIds: Set[PoolId] = modify { bldr =>
    bldr.allReferencedPoolIds
  }

  def disableMissedUpdateTracking(): Unit = modify { bldr =>
    bldr.disableMissedUpdateTracking()
  }

  def setMissedCommitDelayInMs(msec: Int): Unit = modify { bldr =>
    bldr.setMissedCommitDelayInMs(msec)
  }

  def append(position: AppendPosition, data: DataBuffer): AppendPosition =
    require(data.size > 0, "Cannot append empty data")
    modify { bldr =>
      havePendingUpdates = true
      bldr.append(position, data)
    }
    AppendPosition(position.pointer, revision, position.length + data.size)

  def rangedWrites(state: RangedWriteState, writes: List[RangedWrite], zeroFill: Boolean = false): Unit = modify { bldr =>
    bldr.rangedWrites(state, writes, zeroFill)
    havePendingUpdates = true
  }

  def overwrite(objectPointer: DataObjectPointer, requiredRevision: ObjectRevision, data: DataBuffer): Unit = modify { bldr =>
    havePendingUpdates = true
    bldr.overwrite(objectPointer, requiredRevision, data)
  }

  def update(
              pointer: KeyValueObjectPointer,
              requiredRevision: Option[ObjectRevision],
              contentLock: Option[FullContentLock],
              requirements: List[KeyValueUpdate.KeyRequirement],
              operations: List[KeyValueOperation]): Unit = modify { bldr =>
    havePendingUpdates = true
    bldr.update(pointer, requiredRevision, contentLock, requirements, operations)
  }

  def setRefcount(objectPointer: ObjectPointer, requiredRefcount: ObjectRefcount, refcount: ObjectRefcount): Unit = modify { bldr =>
    havePendingUpdates = true
    bldr.setRefcount(objectPointer, requiredRefcount, refcount)
  }

  def bumpVersion(objectPointer: ObjectPointer, requiredRevision: ObjectRevision): Unit = modify { bldr =>
    havePendingUpdates = true
    bldr.bumpVersion(objectPointer, requiredRevision)
  }

  def lockRevision(objectPointer: ObjectPointer, requiredRevision: ObjectRevision): Unit = modify { bldr =>
    bldr.lockRevision(objectPointer, requiredRevision)
  }

  def ensureHappensAfter(timestamp: HLCTimestamp): Unit = modify { bldr =>
    bldr.ensureHappensAfter(timestamp)
  }

  def addFinalizationAction(finalizationActionId: FinalizationActionId, serializedContent: Option[Array[Byte]]): Unit = modify { bldr =>
    serializedContent match {
      case Some(content) => bldr.addFinalizationAction(finalizationActionId, content)
      case None => bldr.addFinalizationAction(finalizationActionId)
    }
  }

  def addAllocatingObject(objectPtr: ObjectPointer): Unit = modify { bldr =>
    bldr.addAllocatingObject(objectPtr)
  }

  def addNotifyOnResolution(storesToNotify: Set[StoreId]): Unit = modify { bldr =>
    bldr.addNotifyOnResolution(storesToNotify)
  }

  def note(note: String): Unit = modify { bldr =>
    bldr.note(note)
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

  /** Adds any pools referenced by the builder that are missing from `known` */
  private def resolvePools(known: Map[PoolId, StoragePool])(using ExecutionContext): Future[Map[PoolId, StoragePool]] =
    val missing = bldr.allReferencedPoolIds -- known.keySet
    if missing.isEmpty then
      Future.successful(known)
    else
      Future.sequence(missing.toList.map(poolId => client.getStoragePool(poolId).map(pool => poolId -> pool)))
        .map(known ++ _)

  /** Tells each deleted object's allocation strategy of the deletion, then resolves every referenced pool.
    *
    *  The strategies may add to the transaction, including further deletions and objects in pools not yet
    *  resolved, so this repeats until a pass adds neither. The monitor is held while they run, so the caller
    *  cannot slip in modifications after commit().
    */
  private def runCommitHooks(pools: Map[PoolId, StoragePool],
                             notified: Set[ObjectPointer])(using ExecutionContext): Future[Map[PoolId, StoragePool]] =
    resolvePools(pools).flatMap: pools =>
      val pending = bldr.deletedObjects.filterNot(notified.contains)
      if pending.isEmpty then
        Future.successful(pools)
      else
        synchronized {
          runningCommitHooks = true
          try
            pending.foreach(ptr => pools(ptr.poolId).allocationStrategy.deleteObject(ptr)(using this))
          finally
            runningCommitHooks = false
        }
        runCommitHooks(pools, notified ++ pending)

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
        val poolsFuture = runCommitHooks(Map.empty, Set.empty)

        poolsFuture.failed.foreach { cause =>
          promise.tryFailure(cause)
        }

        poolsFuture.foreach { pools =>
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
