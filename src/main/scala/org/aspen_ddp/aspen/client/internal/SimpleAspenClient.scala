package org.aspen_ddp.aspen.client.internal

import org.aspen_ddp.aspen.client.internal.network.Messenger as ClientMessenger
import org.aspen_ddp.aspen.client.internal.read.SimpleReadDriver
import org.aspen_ddp.aspen.client.internal.transaction.SimpleClientTransactionDriver
import org.aspen_ddp.aspen.client.*
import org.aspen_ddp.aspen.common.ida.IDA
import org.aspen_ddp.aspen.common.network.ClientId
import org.aspen_ddp.aspen.common.objects.{KeyValueObjectPointer, ObjectPointer}
import org.aspen_ddp.aspen.common.pool.PoolId

import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.concurrent.{ExecutionContext, Future}

class SimpleAspenClient(val msngr: ClientMessenger,
                        clientId: ClientId,
                        executionContext: ExecutionContext,
                        radicle: KeyValueObjectPointer,
                        bootstrapIda: IDA,
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

  // The storage-pools tree lives in the bootstrap pool, so resolving that pool's IDA by reading
  // its pool object would recurse forever: read -> resolveIda -> getStoragePool -> read. The
  // bootstrap IDA is supplied offline by the bootstrap config file instead. Bootstrap.initialize
  // writes the pool object's IDA and the config file's bootstrap-ida from the same value, so the
  // two agree unless the config file has been hand-edited.
  override protected def resolveIda(pointer: ObjectPointer): Future[IDA] =
    if pointer.poolId == PoolId.BootstrapPoolId then
      Future.successful(bootstrapIda)
    else
      getStoragePool(pointer.poolId).map(_.ida)

  override protected def runCreate[T](onCommitFailure: Throwable => Future[Unit])(prepare: Transaction => Future[T]): Future[T] =
    transactUntilSuccessfulWithRecovery(onCommitFailure)(prepare)

  override def getCachedAllocator(allocatorId: ObjectAllocatorId): Option[ObjectAllocator] =
    allocatorManager.get(allocatorId)

  override def cacheAllocator(allocator: ObjectAllocator): Unit =
    allocatorManager.put(allocator)
