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
    override def needsCommitHook: Boolean = false

    override def createDataObjectPointer(poolId: PoolId)(using t: Transaction): Future[DataObjectPointer] =
      Future.successful(new DataObjectPointer(ObjectId(UUID.randomUUID()), poolId, Array.empty))

    override def createKeyValueObjectPointer(poolId: PoolId)(using t: Transaction): Future[KeyValueObjectPointer] =
      Future.successful(new KeyValueObjectPointer(ObjectId(UUID.randomUUID()), poolId, Array.empty))

    override def prepareCommit(poolId: PoolId, allocated: List[ObjectPointer], deleted: List[ObjectPointer],
                               deferRelease: Boolean)(using t: Transaction): Future[Unit] = Future.unit

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

  /** False means prepareCommit is never called for this pool and the pool is exempt from the commit hooks' growth
   *  check, so other pools' hooks may add allocations and deletions to it.
   */
  def needsCommitHook: Boolean = true

  /** Called while `t` is being committed, once for each pool in which `t` allocates or deletes objects, after every
   *  object the caller added. `allocated` holds only staged allocations; `deferRelease` is
   *  `t.allocationReleaseDeferred`. May add to `t` (including in other pools); must not commit or abort it; a
   *  failure aborts it with that failure. Adding an allocation or deletion in a pool whose hook already ran fails
   *  the transaction with IllegalStateException.
   */
  def prepareCommit(poolId: PoolId, allocated: List[ObjectPointer], deleted: List[ObjectPointer],
                    deferRelease: Boolean)(using t: Transaction): Future[Unit]
