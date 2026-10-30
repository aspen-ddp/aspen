package org.aspen_ddp.aspen

import org.aspen_ddp.aspen.client.{AspenClient, RegisteredTypeFactory, StoragePool, StoragePoolAllocationStrategy, Transaction}
import org.aspen_ddp.aspen.common.objects.{DataObjectPointer, KeyValueObjectPointer, ObjectId, ObjectPointer}
import org.aspen_ddp.aspen.common.pool.PoolId

import java.nio.ByteBuffer
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.Future

/** Test-only allocation strategy for FSIF pools: each pointer's storePointer is the next external index of its pool,
  * as the 4-byte big-endian Int the FSIF storePointer contract requires. Indices are never reused, and prepareCommit
  * releases nothing. The real allocation strategy (a bitmap with DurableTask-based clearing) is a separate design.
  *
  * The counter is kept per pool in the factory, not in the strategy: a StoragePool instance creates its strategy on
  * first use, and a client that drops and rereads its cached pool must not hand out an index a second time.
  */
object FsifTestIndexStrategy extends StoragePoolAllocationStrategy.Factory with RegisteredTypeFactory:
  override val typeUUID: UUID = UUID.fromString("5f1c2b7e-8d34-4c0a-9e61-3a7f0d2c4b95")

  private val nextIndex = new ConcurrentHashMap[PoolId, AtomicInteger]()

  def storePointer(index: Int): Array[Byte] = ByteBuffer.allocate(4).putInt(index).array()

  override def createStrategy(client: AspenClient,
                              pool: StoragePool,
                              configData: Option[Array[Byte]]): StoragePoolAllocationStrategy =
    new Strategy(nextIndex.computeIfAbsent(pool.poolId, _ => new AtomicInteger(0)))

  private class Strategy(counter: AtomicInteger) extends StoragePoolAllocationStrategy:
    private def next(): Array[Byte] = storePointer(counter.getAndIncrement())

    override def createDataObjectPointer(poolId: PoolId)(using t: Transaction): Future[DataObjectPointer] =
      Future.successful(new DataObjectPointer(ObjectId(UUID.randomUUID()), poolId, next()))

    override def createKeyValueObjectPointer(poolId: PoolId)(using t: Transaction): Future[KeyValueObjectPointer] =
      Future.successful(new KeyValueObjectPointer(ObjectId(UUID.randomUUID()), poolId, next()))

    override def prepareCommit(poolId: PoolId, allocated: List[ObjectPointer], deleted: List[ObjectPointer],
                               deferRelease: Boolean)(using t: Transaction): Future[Unit] = Future.unit
