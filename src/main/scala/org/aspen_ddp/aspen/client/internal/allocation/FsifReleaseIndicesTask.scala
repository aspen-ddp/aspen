package org.aspen_ddp.aspen.client.internal.allocation

import org.aspen_ddp.aspen.client.{AspenClient, KeyValueObjectState, StopRetrying, Transaction}
import org.aspen_ddp.aspen.common.objects.{Key, ObjectRevision}
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.util.{byte2uuid, uuid2byte}
import org.aspen_ddp.aspen.compute.{DurableTask, DurableTaskFactory, DurableTaskPointer, SteppedDurableTask, TaskExecutor}

import java.util.UUID
import scala.concurrent.Future

/** A system DurableTask that clears the bitmap bits of indices freed by an FSIF pool deletion whose transaction
  * called `deferAllocationRelease()`. It is enrolled in the deleting transaction, so it exists iff the delete
  * committed.
  */
object FsifReleaseIndicesTask extends DurableTaskFactory:
  val typeUUID: UUID = UUID.fromString("5e2b7c91-0d4a-4f63-b8e5-a17c3f9d2e48")

  private val PoolKey = "pool"
  private val IndicesKey = "indices"

  def createTask(client: AspenClient,
                 pointer: DurableTaskPointer,
                 revision: ObjectRevision,
                 state: Map[Key, KeyValueObjectState.ValueState],
                 taskExecutor: TaskExecutor): DurableTask =
    new FsifReleaseIndicesTask(pointer, client)

  /** Enrolls a task, within the transaction, that clears the bits of `indices` in `poolId`'s bitmap */
  def prepare(client: AspenClient, poolId: PoolId, indices: Seq[Int])(using Transaction): Future[Unit] =
    val initial = SteppedDurableTask.getInitialContent(Map(
      PoolKey -> uuid2byte(poolId.uuid),
      IndicesKey -> FsifIndexBitmap.encodeIndices(indices)))._2.bytes
    client.prepareSystemDurableTask(typeUUID, Map(SteppedDurableTask.StepStateKey -> initial))

class FsifReleaseIndicesTask(taskPointer: DurableTaskPointer, client: AspenClient)
  extends SteppedDurableTask(taskPointer, client):

  import FsifReleaseIndicesTask.*

  private def release(tx: Transaction, state: Map[String, Array[Byte]],
                      stepRevision: ObjectRevision): Future[Map[String, Array[Byte]]] =
    given Transaction = tx
    val poolId = PoolId(byte2uuid(state(PoolKey)))
    val indices = FsifIndexBitmap.decodeIndices(state(IndicesKey))
    client.getStoragePool(poolId).flatMap: pool =>
      pool.allocationStrategy match
        case strategy: FsifIndexAllocationStrategy.Strategy =>
          strategy.prepareRelease(poolId, indices).map(_ => state)
        case other =>
          Future.failed(StopRetrying(new IllegalStateException(
            s"FSIF index release: pool $poolId uses ${other.getClass.getName}, not the FSIF allocation strategy")))

  val steps: Array[(Transaction, Map[String, Array[Byte]], ObjectRevision) => Future[Map[String, Array[Byte]]]] =
    Array(release)
