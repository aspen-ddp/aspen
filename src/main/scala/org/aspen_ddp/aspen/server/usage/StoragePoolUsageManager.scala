package org.aspen_ddp.aspen.server.usage

import org.aspen_ddp.aspen.client.{AspenClient, FatalReadError, StopRetrying}
import org.aspen_ddp.aspen.common.HLCTimestamp
import org.aspen_ddp.aspen.common.metadata.StoragePoolState
import org.aspen_ddp.aspen.common.objects.{Insert, KeyValueObjectPointer}
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.transaction.KeyValueUpdate.{DoesNotExist, KeyRevision}
import org.aspen_ddp.aspen.common.util.{byte2long, long2byte}
import org.aspen_ddp.aspen.compute.TaskExecutor
import org.apache.logging.log4j.scala.Logging
import org.aspen_ddp.aspen.common.metadata.management.UpdateAllocationGroupUsageTask

import scala.concurrent.{ExecutionContext, Future}

class StoragePoolUsageManager(client: AspenClient) extends Logging:

  given ExecutionContext = client.clientContext

  private var taskExecutor: Option[TaskExecutor] = None
  private var cachedPointers: Map[PoolId, KeyValueObjectPointer] = Map()
  private var cachedSizes: Map[StoreId, Long] = Map()

  def setTaskExecutor(executor: TaskExecutor): Unit = synchronized:
    taskExecutor = Some(executor)

  private def exceedsThreshold(newValue: Long, oldValue: Long): Boolean =
    math.abs(newValue - oldValue).toDouble / math.max(oldValue, 1) > 0.005

  private def getPoolPointer(poolId: PoolId): Future[KeyValueObjectPointer] =
    val cached = synchronized(cachedPointers.get(poolId))
    cached match
      case Some(ptr) => Future.successful(ptr)
      case None =>
        client.getStoragePoolPointer(poolId).map: ptr =>
          synchronized(cachedPointers += poolId -> ptr)
          ptr

  def updateStoreSize(storeId: StoreId, currentEstimatedUsage: Long): Future[Unit] =
    val lastSize = synchronized(cachedSizes.getOrElse(storeId, 0L))

    if !exceedsThreshold(currentEstimatedUsage, lastSize) then
      Future.unit
    else
      def onFail(err: Throwable): Future[Unit] = err match
        case _: FatalReadError => throw StopRetrying(err)
        case _ => Future.unit

      client.transactUntilSuccessfulWithRecovery(onFail): tx =>
        for
          poolPtr <- getPoolPointer(storeId.poolId)
          poolKvos <- client.read(poolPtr)

          sizeKey = StoragePoolState.getStoreUsageKey(storeId.poolIndex)
          sizeBytes = long2byte(currentEstimatedUsage)

          sizeRequirement = poolKvos.contents.get(sizeKey) match
            case Some(vs) => KeyRevision(sizeKey, vs.revision)
            case None => DoesNotExist(sizeKey)

          poolState = StoragePoolState(poolKvos)
          numStores = poolState.stores.length

          totalUsage = (0 until numStores).foldLeft(0L): (acc, i) =>
            if i == storeId.poolIndex then
              acc + currentEstimatedUsage
            else
              val key = StoragePoolState.getStoreUsageKey(i.toByte)
              poolKvos.contents.get(key) match
                case Some(vs) => acc + byte2long(vs.value.bytes)
                case None => acc

          meanUsage = if numStores > 0 then totalUsage / numStores else 0L

          updateConfig = exceedsThreshold(meanUsage, poolState.currentUsage)

          configRequirements =
            if updateConfig then
              val configVs = poolKvos.contents(StoragePoolState.ConfigKey)
              List(KeyRevision(StoragePoolState.ConfigKey, configVs.revision))
            else Nil

          configOperations =
            if updateConfig then
              val updatedState = poolState.copy(currentUsage = meanUsage)
              List(Insert(StoragePoolState.ConfigKey, updatedState.encode()))
            else Nil

          requirements = sizeRequirement :: configRequirements
          operations = Insert(sizeKey, sizeBytes) :: configOperations

          needTask = updateConfig && poolState.allocationGroups.nonEmpty
          executor = synchronized(taskExecutor)

          _ <-
            if needTask && executor.isDefined then
              UpdateAllocationGroupUsageTask.prepareTask(
                poolState.poolId.uuid,
                meanUsage,
                poolState.maximumStoreSize,
                poolState.allocationGroups,
                executor.get
              )(using tx).map(_ => ())
            else
              Future.unit
        yield
          tx.update(poolPtr, None, None, requirements, operations)

      .map: _ =>
        synchronized:
          cachedSizes += storeId -> currentEstimatedUsage
