package org.aspen_ddp.aspen.client.internal.allocation

import org.aspen_ddp.aspen.client.{AspenClient, KeyValueObjectState, StopRetrying, Transaction}
import org.aspen_ddp.aspen.common.objects.{Key, ObjectRevision}
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.compute.{DurableTask, DurableTaskFactory, DurableTaskPointer, SteppedDurableTask, TaskExecutor}

import java.util.UUID
import scala.concurrent.Future

/** A system DurableTask that clears the bitmap bits of indices freed by FSIF pool deletions whose transaction
  * called `deferAllocationRelease()`. One task covers every FSIF pool the transaction deleted from. It is enrolled in
  * the deleting transaction, so it exists iff the delete committed.
  */
object FsifReleaseIndicesTask extends DurableTaskFactory:
  val typeUUID: UUID = UUID.fromString("5e2b7c91-0d4a-4f63-b8e5-a17c3f9d2e48")

  /** One state entry per pool: this prefix and the pool's UUID, mapping to its encoded indices */
  private val PoolPrefix = "pool:"

  def createTask(client: AspenClient,
                 pointer: DurableTaskPointer,
                 revision: ObjectRevision,
                 state: Map[Key, KeyValueObjectState.ValueState],
                 taskExecutor: TaskExecutor): DurableTask =
    new FsifReleaseIndicesTask(pointer, client)

  /** Enrolls a task, within the transaction, that clears the bits of each pool's indices in that pool's bitmap */
  def prepare(client: AspenClient, release: Map[PoolId, Seq[Int]])(using Transaction): Future[Unit] =
    val initial = SteppedDurableTask.getInitialContent(release.map: (poolId, indices) =>
      s"$PoolPrefix${poolId.uuid}" -> FsifIndexBitmap.encodeIndices(indices))._2.bytes
    client.prepareSystemDurableTask(typeUUID, Map(SteppedDurableTask.StepStateKey -> initial))

class FsifReleaseIndicesTask(taskPointer: DurableTaskPointer, client: AspenClient)
  extends SteppedDurableTask(taskPointer, client):

  import FsifReleaseIndicesTask.*

  private def release(tx: Transaction, state: Map[String, Array[Byte]],
                      stepRevision: ObjectRevision): Future[Map[String, Array[Byte]]] =
    given Transaction = tx
    val pools = state.toList.collect:
      case (key, bytes) if key.startsWith(PoolPrefix) =>
        PoolId(UUID.fromString(key.drop(PoolPrefix.length))) -> FsifIndexBitmap.decodeIndices(bytes)
    // Every pool's bitmap in this one step's transaction, one pool at a time
    pools.foldLeft(Future.unit): (prev, entry) =>
      val (poolId, indices) = entry
      prev.flatMap: _ =>
        client.getStoragePool(poolId).flatMap: pool =>
          pool.allocationStrategy match
            case strategy: FsifIndexAllocationStrategy.Strategy =>
              strategy.prepareRelease(poolId, indices)
            case other =>
              Future.failed(StopRetrying(new IllegalStateException(
                s"FSIF index release: pool $poolId uses ${other.getClass.getName}, not the FSIF allocation strategy")))
    .map(_ => state)

  val steps: Array[(Transaction, Map[String, Array[Byte]], ObjectRevision) => Future[Map[String, Array[Byte]]]] =
    Array(release)
