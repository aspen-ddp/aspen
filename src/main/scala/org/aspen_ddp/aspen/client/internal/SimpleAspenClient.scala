package org.aspen_ddp.aspen.client.internal

import org.aspen_ddp.aspen.client.internal.network.Messenger as ClientMessenger
import org.aspen_ddp.aspen.client.internal.read.SimpleReadDriver
import org.aspen_ddp.aspen.client.internal.transaction.SimpleClientTransactionDriver
import org.aspen_ddp.aspen.client.*
import org.aspen_ddp.aspen.common.ida.IDA
import org.aspen_ddp.aspen.common.network.ClientId
import org.aspen_ddp.aspen.common.objects.{KeyValueObjectPointer, ObjectPointer}

import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.concurrent.{ExecutionContext, Future}

class SimpleAspenClient(val msngr: ClientMessenger,
                        clientId: ClientId,
                        executionContext: ExecutionContext,
                        radicle: KeyValueObjectPointer,
                        txStatusCacheDuration: FiniteDuration,
                        initialReadDelay: Duration,
                        maxReadDelay: Duration,
                        txRetransmitDelay: Duration,
                        allocationRetransmitDelay: Duration,
                        userTypeFactories: List[RegisteredTypeFactory] = Nil)
  extends BaseAspenClient(
    executionContext,
    radicle,
    clientId,
    msngr,
    new TransactionStatusCache(txStatusCacheDuration),
    new SimpleObjectCache,
    new SimpleReadDriver.Factory(initialReadDelay, maxReadDelay).apply,
    SimpleClientTransactionDriver.factory(txRetransmitDelay),
    userTypeFactories):

  // Created exactly once, with thread-safe (lazy val) initialization.
  override lazy val opportunisticRebuildManager: OpportunisticRebuildManager =
    new SimpleOpportunisticRebuildManager(this)

  override protected def resolveIda(pointer: ObjectPointer): Future[IDA] =
    getStoragePool(pointer.poolId).map(_.ida)

  override protected def runCreate[T](onCommitFailure: Throwable => Future[Unit])(prepare: Transaction => Future[T]): Future[T] =
    transactUntilSuccessfulWithRecovery(onCommitFailure)(prepare)

  override def getCachedAllocator(allocatorId: ObjectAllocatorId): Option[ObjectAllocator] =
    allocatorManager.get(allocatorId)

  override def cacheAllocator(allocator: ObjectAllocator): Unit =
    allocatorManager.put(allocator)
