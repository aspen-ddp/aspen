package org.aspen_ddp.aspen.client.internal.allocation

import org.aspen_ddp.aspen.IntegrationTestSuite
import org.aspen_ddp.aspen.client.internal.pool.SimpleStoragePool
import org.aspen_ddp.aspen.client.{AspenClient, RegisteredTypeFactory, StoragePool, StoragePoolAllocationStrategy, Transaction}
import org.aspen_ddp.aspen.common.metadata.StoragePoolState
import org.aspen_ddp.aspen.common.objects.{DataObjectPointer, Insert, Key, KeyValueObjectPointer, ObjectId, Value}
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.util.uuid2byte
import org.aspen_ddp.aspen.common.{DataBuffer, Radicle}

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

object PoolObjectAllocatorSuite:

  val StrategyTypeUUID: UUID = UUID.fromString("6f0e5a4c-1d3b-4e77-9a21-0c8d5b7e4f10")

  /** Random ObjectId, but the configured bytes verbatim as the storePointer. The non-empty
   *  storePointer is what distinguishes this from StoragePoolAllocationStrategy.Default and
   *  proves the pool's config data reached the strategy. */
  class TestStrategy(configData: Option[Array[Byte]]) extends StoragePoolAllocationStrategy:
    private def storePointer: Array[Byte] = configData.getOrElse(Array.empty)

    override def createDataObjectPointer(poolId: PoolId)(using t: Transaction): Future[DataObjectPointer] =
      Future.successful(new DataObjectPointer(ObjectId(UUID.randomUUID()), poolId, storePointer))

    override def createKeyValueObjectPointer(poolId: PoolId)(using t: Transaction): Future[KeyValueObjectPointer] =
      Future.successful(new KeyValueObjectPointer(ObjectId(UUID.randomUUID()), poolId, storePointer))

  object TestStrategyFactory extends StoragePoolAllocationStrategy.Factory with RegisteredTypeFactory:
    override val typeUUID: UUID = StrategyTypeUUID

    override def createStrategy(client: AspenClient,
                                pool: StoragePool,
                                configData: Option[Array[Byte]]): StoragePoolAllocationStrategy =
      new TestStrategy(configData)


class PoolObjectAllocatorSuite extends IntegrationTestSuite:
  import PoolObjectAllocatorSuite.*

  override def userTypeFactories: List[RegisteredTypeFactory] = List(TestStrategyFactory)

  private val strategyConfig = Array[Byte](10, 11, 12)

  /** Point the bootstrap pool at TestStrategy and return an allocator for it.
   *
   *  Nothing in src/main writes AllocationStrategyKey, so the test writes it directly. The pool
   *  is rebuilt from a fresh read rather than fetched via client.getStoragePool because the
   *  client caches StoragePool instances and has no way to invalidate one.
   */
  private def customStrategyAllocator(): Future[PoolObjectAllocator] =
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

      updated <- client.read(poolPtr)
    yield new PoolObjectAllocator(client, SimpleStoragePool(client, updated))

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
