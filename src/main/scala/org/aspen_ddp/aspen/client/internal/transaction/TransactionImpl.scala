package org.aspen_ddp.aspen.client.internal.transaction

import java.util.UUID

import org.aspen_ddp.aspen.client.{AspenClient, PostCommitTransactionModification, Transaction, TransactionAborted}
import org.aspen_ddp.aspen.common.objects._
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.transaction.KeyValueUpdate.FullContentLock
import org.aspen_ddp.aspen.common.transaction.{FinalizationActionId, KeyValueUpdate, TransactionId}
import org.aspen_ddp.aspen.common.{DataBuffer, HLCTimestamp}
import org.apache.logging.log4j.scala.Logging

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success}

object TransactionImpl {
  def Factory(client: AspenClient,
              txManager: TransactionManager,
              chooseDesignatedLeader: ObjectPointer => Byte, // Uses peer online/offline knowledge to select designated leaders for transactions)
              transactionDriverStrategy: Option[ClientTransactionDriver.Factory]) = new TransactionImpl(client, txManager, chooseDesignatedLeader, transactionDriverStrategy)
}

class TransactionImpl(val client: AspenClient,
                      txManager: TransactionManager,
                      chooseDesignatedLeader: ObjectPointer => Byte, // Uses peer online/offline knowledge to select designated leaders for transactions)
                      transactionDriverStrategy: Option[ClientTransactionDriver.Factory]) extends Transaction with Logging {

  val id: TransactionId = TransactionId(UUID.randomUUID())
  private  val promise = Promise[HLCTimestamp]()
  private  var state: Either[HLCTimestamp, TransactionBuilder] = Right(new TransactionBuilder(id, chooseDesignatedLeader, client.clientId))
  private  var invalidated = false
  private  var havePendingUpdates = false

  private  val stack = org.aspen_ddp.aspen.common.util.getStack // for debugging

  def valid: Boolean = synchronized { !invalidated && havePendingUpdates }

  def disableMissedUpdateTracking(): Unit = synchronized { state } match {
    case Right(bldr) => bldr.disableMissedUpdateTracking()
    case Left(_) => throw PostCommitTransactionModification()
  }

  def setMissedCommitDelayInMs(msec: Int): Unit = synchronized { state } match {
    case Right(bldr) => bldr.setMissedCommitDelayInMs(msec)
    case Left(_) => throw PostCommitTransactionModification()
  }

  def append(objectPointer: DataObjectPointer, requiredRevision: ObjectRevision, data: DataBuffer): Unit = synchronized { state } match {
    case Right(bldr) =>
      havePendingUpdates = true
      bldr.append(objectPointer, requiredRevision, data)
    case Left(_) => throw PostCommitTransactionModification()
  }
  def overwrite(objectPointer: DataObjectPointer, requiredRevision: ObjectRevision, data: DataBuffer): Unit = synchronized { state } match {
    case Right(bldr) =>
      havePendingUpdates = true
      bldr.overwrite(objectPointer, requiredRevision, data)
    case Left(_) => throw PostCommitTransactionModification()
  }

  def update(
              pointer: KeyValueObjectPointer,
              requiredRevision: Option[ObjectRevision],
              contentLock: Option[FullContentLock],
              requirements: List[KeyValueUpdate.KeyRequirement],
              operations: List[KeyValueOperation]): Unit = synchronized { state } match {
    case Right(bldr) =>
      havePendingUpdates = true
      bldr.update(pointer, requiredRevision, contentLock, requirements, operations)
    case Left(_) => throw PostCommitTransactionModification()
  }

  def setRefcount(objectPointer: ObjectPointer, requiredRefcount: ObjectRefcount, refcount: ObjectRefcount): Unit = synchronized { state } match {
    case Right(bldr) =>
      havePendingUpdates = true
      bldr.setRefcount(objectPointer, requiredRefcount, refcount)
    case Left(_) => throw PostCommitTransactionModification()
  }

  def bumpVersion(objectPointer: ObjectPointer, requiredRevision: ObjectRevision): Unit = synchronized { state } match {
    case Right(bldr) =>
      havePendingUpdates = true
      bldr.bumpVersion(objectPointer, requiredRevision)
    case Left(_) => throw PostCommitTransactionModification()
  }

  def lockRevision(objectPointer: ObjectPointer, requiredRevision: ObjectRevision): Unit = synchronized { state } match {
    case Right(bldr) => bldr.lockRevision(objectPointer, requiredRevision)
    case Left(_) => throw PostCommitTransactionModification()
  }

  def ensureHappensAfter(timestamp: HLCTimestamp): Unit = synchronized { state } match {
    case Right(bldr) => bldr.ensureHappensAfter(timestamp)
    case Left(_) => throw PostCommitTransactionModification()
  }

  def addFinalizationAction(finalizationActionId: FinalizationActionId, serializedContent: Option[Array[Byte]]): Unit = synchronized { state } match {
    case Right(bldr) => serializedContent match {
      case Some(content) => bldr.addFinalizationAction(finalizationActionId, content)
      case None => bldr.addFinalizationAction(finalizationActionId)
    }

    case Left(_) => throw PostCommitTransactionModification()
  }

  def addNotifyOnResolution(storesToNotify: Set[StoreId]): Unit = synchronized { state } match {
    case Right(bldr) => bldr.addNotifyOnResolution(storesToNotify)
    case Left(_) => throw PostCommitTransactionModification()
  }

  def note(note: String): Unit = synchronized { state } match {
    case Right(bldr) => bldr.note(note)
    case Left(_) => throw PostCommitTransactionModification()
  }

  def invalidateTransaction(reason: Throwable): Unit = synchronized {
    invalidated = true
    if (!promise.isCompleted)
      promise.failure(reason)
  }

  def result: Future[HLCTimestamp] = promise.future

  /** Begins the transaction commit process and returns a Future to its completion. This is the same future as
    *  returned by 'result'
    *
    *  The future successfully completes if the transaction commits. Otherwise it will fail with a TransactionError subclass.
    */
  def commit(): Future[HLCTimestamp] = synchronized {

    given ExecutionContext = client.clientContext

    if (!promise.isCompleted) {

      state.foreach { bldr =>
        val (txd, transactionData, timestamp) = bldr.buildTranaction(client.opportunisticRebuildManager)

        //---- Tx Debugging ----
        result.onComplete {
          case Success(_) =>
            logger.info(s"TX SUCCESS: ${txd.shortString}\n${txd.shortString}")
          case Failure(e) =>
            logger.info(s"TX FAILURE: ${txd.shortString}: $e\n$stack")
        }
        //---------------------

        state = Left(timestamp)
        if (txd.requirements.isEmpty)
          promise.success(txd.startTimestamp)
        else {

          txManager.runTransaction(txd, transactionData, transactionDriverStrategy) onComplete {
            case Failure(cause) =>
              // TODO Catch transaction timeout from lower layer and convert to TransactionError.TransactionTimedOut
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
        }
      }
    }
    result
  }
}
