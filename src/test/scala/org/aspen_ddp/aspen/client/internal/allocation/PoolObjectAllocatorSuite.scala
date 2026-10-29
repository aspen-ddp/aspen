package org.aspen_ddp.aspen.client.internal.allocation

import org.aspen_ddp.aspen.IntegrationTestSuite
import org.aspen_ddp.aspen.client.{AspenClient, FinalizationAction, FinalizationActionFactory, RegisteredTypeFactory, StoragePool, StoragePoolAllocationStrategy, Transaction}
import org.aspen_ddp.aspen.common.metadata.StoragePoolState
import org.aspen_ddp.aspen.common.objects.{DataObjectPointer, Insert, Key, KeyValueObjectPointer, ObjectId, ObjectPointer, Value}
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.transaction.{FinalizationActionId, TransactionDescription}
import org.aspen_ddp.aspen.common.util.uuid2byte
import org.aspen_ddp.aspen.common.{DataBuffer, Radicle}

import java.util.UUID
import scala.collection.concurrent.TrieMap
import scala.concurrent.{ExecutionContext, Future}

object PoolObjectAllocatorSuite:

  val StrategyTypeUUID: UUID = UUID.fromString("6f0e5a4c-1d3b-4e77-9a21-0c8d5b7e4f10")

  /** Random ObjectId, but the configured bytes verbatim as the storePointer. The non-empty
   *  storePointer is what distinguishes this from StoragePoolAllocationStrategy.Default and
   *  proves the pool's config data reached the strategy. */
  class TestStrategy(configData: Option[Array[Byte]]) extends StoragePoolAllocationStrategy:
    private def storePointer: Array[Byte] = configData.getOrElse(Array.empty)

    @volatile var deleted: List[ObjectPointer] = Nil

    override def deleteObject(pointer: ObjectPointer)(using t: Transaction): Unit =
      synchronized:
        deleted = pointer :: deleted
      t.addFinalizationAction(FinalizationActionId(DeletionRecorder.typeUUID), Some(pointer.id.toBytes))

    override def createDataObjectPointer(poolId: PoolId)(using t: Transaction): Future[DataObjectPointer] =
      Future.successful(new DataObjectPointer(ObjectId(UUID.randomUUID()), poolId, storePointer))

    override def createKeyValueObjectPointer(poolId: PoolId)(using t: Transaction): Future[KeyValueObjectPointer] =
      Future.successful(new KeyValueObjectPointer(ObjectId(UUID.randomUUID()), poolId, storePointer))

  object TestStrategyFactory extends StoragePoolAllocationStrategy.Factory with RegisteredTypeFactory:
    override val typeUUID: UUID = StrategyTypeUUID

    // Keyed by pool identity: suites run in parallel and each test builds its own pool instance
    private val created = TrieMap[StoragePool, Int]()

    def createdFor(pool: StoragePool): Int = created.getOrElse(pool, 0)

    override def createStrategy(client: AspenClient,
                                pool: StoragePool,
                                configData: Option[Array[Byte]]): StoragePoolAllocationStrategy =
      created.updateWith(pool)(n => Some(n.getOrElse(0) + 1))
      new TestStrategy(configData)

  /** The finalization action TestStrategy adds on deletion. Counts executions per object. */
  object DeletionRecorder extends FinalizationActionFactory with RegisteredTypeFactory:
    override val typeUUID: UUID = UUID.fromString("2b9d4c61-8f3e-4a05-b7d2-5e1a90c3f6e8")

    private val executions = TrieMap[ObjectId, Int]()

    def executionsFor(id: ObjectId): Int = executions.getOrElse(id, 0)

    override def createFinalizationAction(client: AspenClient,
                                          txd: TransactionDescription,
                                          data: Array[Byte]): FinalizationAction =
      val id = ObjectId(data)
      new FinalizationAction:
        def complete: Future[Unit] = Future.unit
        def execute(): Unit = executions.updateWith(id)(n => Some(n.getOrElse(0) + 1))


class PoolObjectAllocatorSuite extends IntegrationTestSuite:
  import PoolObjectAllocatorSuite.*

  override def userTypeFactories: List[RegisteredTypeFactory] = List(TestStrategyFactory, DeletionRecorder)

  private val strategyConfig = Array[Byte](10, 11, 12)

  /** Point the bootstrap pool at TestStrategy and return the client's StoragePool for it.
   *
   *  Nothing in src/main writes AllocationStrategyKey, so the test writes it directly. The cached
   *  StoragePool is dropped so the client, and therefore commit(), sees the strategy. Allocators
   *  are built directly because the client's cached allocator holds the old pool instance.
   */
  private def customStrategyPool(): Future[StoragePool] =
    given ExecutionContext = executionContext
    for
      poolPtr <- client.getStoragePoolPointer(Radicle.poolId)
      kvos <- client.read(poolPtr)

      tx = client.newTransaction()
      _ = tx.update(poolPtr, Some(kvos.revision), None, Nil, List(
            Insert(StoragePoolState.AllocationStrategyKey, uuid2byte(StrategyTypeUUID)),
            Insert(StoragePoolState.AllocationStrategyConfigKey, strategyConfig)))
      _ <- tx.commit()
      _ <- waitForTransactionsToComplete()

      _ = client.dropCachedStoragePool(Radicle.poolId)
      pool <- client.getStoragePool(Radicle.poolId)
    yield pool

  private def customStrategyAllocator(): Future[PoolObjectAllocator] =
    given ExecutionContext = executionContext
    customStrategyPool().map(pool => new PoolObjectAllocator(client, pool))

  atest("custom allocation strategy supplies KeyValueObject pointers"):
    given ExecutionContext = executionContext
    val key = Key(Array[Byte](42))
    val value = Value(Array[Byte](7, 8, 9))
    for
      allocator <- customStrategyAllocator()

      tx = client.newTransaction()
      ptr <- allocator.allocateKeyValueObject(Map(key -> value))(using tx)
      _ <- tx.commit()
      _ <- waitForTransactionsToComplete()

      kvos <- client.read(ptr)
    yield
      ptr.poolId should be(Radicle.poolId)
      ptr.storePointer should be(strategyConfig)
      kvos.contents(key).value.bytes should be(value.bytes)

  atest("custom allocation strategy supplies DataObject pointers"):
    given ExecutionContext = executionContext
    for
      allocator <- customStrategyAllocator()

      tx = client.newTransaction()
      ptr <- allocator.allocateDataObject(DataBuffer(Array[Byte](1, 2, 3)))(using tx)
      _ <- tx.commit()
      _ <- waitForTransactionsToComplete()

      dos <- client.read(ptr)
    yield
      ptr.poolId should be(Radicle.poolId)
      ptr.storePointer should be(strategyConfig)
      dos.data.size should be(3)

  atest("pool without an allocation strategy uses the default"):
    given ExecutionContext = executionContext
    for
      pool <- client.getStoragePool(Radicle.poolId)
      allocator = new PoolObjectAllocator(client, pool)

      tx = client.newTransaction()
      ptr <- allocator.allocateDataObject(DataBuffer(Array[Byte](1)))(using tx)
      _ <- tx.commit()
      _ <- waitForTransactionsToComplete()
    yield
      ptr.storePointer.isEmpty should be(true)

  atest("custom allocation strategy is created once, on first use, and told of deletions"):
    given ExecutionContext = executionContext
    for
      pool <- customStrategyPool()
      createdBeforeUse = TestStrategyFactory.createdFor(pool)
      allocator = new PoolObjectAllocator(client, pool)

      tx = client.newTransaction()
      ptr <- allocator.allocateDataObject(DataBuffer(Array[Byte](1, 2, 3)))(using tx)
      _ <- tx.commit()
      _ <- waitForTransactionsToComplete()
      strategy = pool.allocationStrategy.asInstanceOf[TestStrategy]
      deletedAfterAllocation = strategy.deleted

      meta <- client.readMetadata(ptr)
      deleteTx = client.newTransaction()
      _ = deleteTx.setRefcount(ptr, meta.refcount, meta.refcount.decrement())
      _ <- deleteTx.commit()
      _ <- waitForTransactionsToComplete()
      remaining <- client.readMetadataOptional(ptr)
    yield
      createdBeforeUse should be(0)
      deletedAfterAllocation should be(Nil)
      strategy.deleted should be(List(ptr))
      DeletionRecorder.executionsFor(ptr.id) should be >= 1
      TestStrategyFactory.createdFor(pool) should be(1)
      remaining should be(None)

  atest("refcount changes that do not delete are not reported to the strategy"):
    given ExecutionContext = executionContext
    for
      pool <- customStrategyPool()
      allocator = new PoolObjectAllocator(client, pool)

      tx = client.newTransaction()
      ptr <- allocator.allocateDataObject(DataBuffer(Array[Byte](1)))(using tx)
      _ <- tx.commit()

      meta <- client.readMetadata(ptr)
      incTx = client.newTransaction()
      _ = incTx.setRefcount(ptr, meta.refcount, meta.refcount.increment())
      _ <- incTx.commit()
      _ <- waitForTransactionsToComplete()
    yield
      pool.allocationStrategy.asInstanceOf[TestStrategy].deleted should be(Nil)
      DeletionRecorder.executionsFor(ptr.id) should be(0)
