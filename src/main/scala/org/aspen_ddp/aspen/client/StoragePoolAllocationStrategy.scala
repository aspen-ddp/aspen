package org.aspen_ddp.aspen.client

import org.aspen_ddp.aspen.common.objects.{DataObjectPointer, KeyValueObjectPointer, ObjectId, ObjectPointer}
import org.aspen_ddp.aspen.common.pool.PoolId

import java.util.UUID
import scala.concurrent.Future

object StoragePoolAllocationStrategy:
  trait Factory:
    def createStrategy(client: AspenClient,
                       pool: StoragePool,
                       configData: Option[Array[Byte]]): StoragePoolAllocationStrategy

  object Default extends StoragePoolAllocationStrategy:
    override def createDataObjectPointer(poolId: PoolId)(using t: Transaction): Future[DataObjectPointer] =
      Future.successful(new DataObjectPointer(ObjectId(UUID.randomUUID()), poolId, Array.empty))

    override def createKeyValueObjectPointer(poolId: PoolId)(using t: Transaction): Future[KeyValueObjectPointer] =
      Future.successful(new KeyValueObjectPointer(ObjectId(UUID.randomUUID()), poolId, Array.empty))

    override def deleteObject(pointer: ObjectPointer)(using t: Transaction): Unit = ()

/**
 * Allows for user-defined strategies for creating ObjectPointers during object allocation.
 * For example, a flat-file storage backend designed bulk, fixed-sized content could embed
 * an index number in the ObjectPointer that would allow the backed to calculate the byte
 * offset of the object within the file without having to do any lookups.
 *
 * The Default strategy is to generate a random UUID for the object with no additional
 * data. This requires that the storage backend be capable of looking up the object within
 * the storage backed by the object UUID alone.
 *
 * A pool creates its strategy on first use and keeps it for the lifetime of the StoragePool
 * instance.
 * */
trait StoragePoolAllocationStrategy:
  def createDataObjectPointer(poolId: PoolId)(using t: Transaction): Future[DataObjectPointer]

  def createKeyValueObjectPointer(poolId: PoolId)(using t: Transaction): Future[KeyValueObjectPointer]

  /** Called while `t` is being committed, once for each object in this pool whose refcount `t` sets to
   *  zero. The strategy may add to `t`, typically finalization actions that release whatever it reserved
   *  for the object. Runs after the caller's commit() but before the transaction is built, so `t` must not
   *  be committed or aborted from here; throwing aborts it with the thrown exception.
   */
  def deleteObject(pointer: ObjectPointer)(using t: Transaction): Unit
