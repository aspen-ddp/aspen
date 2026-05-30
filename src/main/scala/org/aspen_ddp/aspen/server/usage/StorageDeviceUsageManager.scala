package org.aspen_ddp.aspen.server.usage

import org.aspen_ddp.aspen.client.{AspenClient, FatalReadError, StopRetrying}
import org.aspen_ddp.aspen.common.metadata.{StorageDeviceId, StorageDeviceState}
import org.aspen_ddp.aspen.common.objects.{Insert, KeyValueObjectPointer}
import org.aspen_ddp.aspen.common.transaction.KeyValueUpdate.KeyRevision
import org.apache.logging.log4j.scala.Logging

import scala.concurrent.{ExecutionContext, Future}

class StorageDeviceUsageManager(client: AspenClient) extends Logging:

  given ExecutionContext = client.clientContext

  private var cachedPointers: Map[StorageDeviceId, KeyValueObjectPointer] = Map()
  private var cachedUsage: Map[StorageDeviceId, Long] = Map()
  private var cachedTotalSize: Map[StorageDeviceId, Long] = Map()

  private def exceedsThreshold(newValue: Long, oldValue: Long): Boolean =
    math.abs(newValue - oldValue).toDouble / math.max(oldValue, 1) > 0.005

  private def getDevicePointer(storageDeviceId: StorageDeviceId): Future[KeyValueObjectPointer] =
    val cached = synchronized(cachedPointers.get(storageDeviceId))
    cached match
      case Some(ptr) => Future.successful(ptr)
      case None =>
        client.getStorageDevicePointer(storageDeviceId).map: ptr =>
          synchronized(cachedPointers += storageDeviceId -> ptr)
          ptr

  def updateDeviceUsage(storageDeviceId: StorageDeviceId, currentUsage: Long, totalSize: Long): Future[Unit] =
    val (lastUsage, lastTotalSize) = synchronized:
      (cachedUsage.getOrElse(storageDeviceId, 0L), cachedTotalSize.getOrElse(storageDeviceId, 0L))

    if !exceedsThreshold(currentUsage, lastUsage) && !exceedsThreshold(totalSize, lastTotalSize) then
      Future.unit
    else
      def onFail(err: Throwable): Future[Unit] = err match
        case _: FatalReadError => throw StopRetrying(err)
        case _ => Future.unit

      client.transactUntilSuccessfulWithRecovery(onFail): tx =>
        for
          devPtr <- getDevicePointer(storageDeviceId)
          devKvos <- client.read(devPtr)
          state = StorageDeviceState(devKvos)
          updatedState = state.copy(currentUsage = currentUsage, totalSize = totalSize)
          requirements = List(KeyRevision(StorageDeviceState.StateKey, devKvos.contents(StorageDeviceState.StateKey).revision))
          operations = List(Insert(StorageDeviceState.StateKey, updatedState.encode()))
        yield
          tx.update(devPtr, None, None, requirements, operations)

      .map: _ =>
        synchronized:
          cachedUsage += storageDeviceId -> currentUsage
          cachedTotalSize += storageDeviceId -> totalSize
