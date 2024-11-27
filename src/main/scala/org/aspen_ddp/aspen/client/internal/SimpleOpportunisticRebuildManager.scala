package org.aspen_ddp.aspen.client.internal

import com.github.blemale.scaffeine.Scaffeine
import org.aspen_ddp.aspen.client.{AspenClient, ObjectState}
import org.aspen_ddp.aspen.common.objects.{Metadata, ObjectId, ObjectPointer}
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.transaction.PreTransactionOpportunisticRebuild

import scala.concurrent.duration._
import scala.concurrent.duration.Duration

class SimpleOpportunisticRebuildManager(system: AspenClient) extends OpportunisticRebuildManager {

  private[this] val repairCache = Scaffeine().expireAfterWrite(Duration(10, SECONDS))
    .maximumSize(5000)
    .build[ObjectId, Set[Byte]]()

  val slowReadReplyDuration: Duration = Duration(5, SECONDS)

  def markRepairNeeded(os: ObjectState, repairNeeded: Set[Byte]): Unit = repairCache.put(os.pointer.id, repairNeeded)

  def getPreTransactionOpportunisticRebuild(pointer: ObjectPointer): Map[Byte, PreTransactionOpportunisticRebuild] = {
    repairCache.getIfPresent(pointer.id) match {
      case None => Map()
      case Some(set) => system.objectCache.get(pointer) match {
        case None => Map()
        case Some(os) =>
          set.foldLeft(Map[Byte, PreTransactionOpportunisticRebuild]()){ (m, i) =>
            os.getRebuildDataForStore(StoreId(pointer.poolId, i)) match {
              case None => m
              case Some(db) =>
                val p = PreTransactionOpportunisticRebuild(pointer.id, Metadata(os.revision, os.refcount, os.timestamp), db)
                m + (i -> p)
            }
          }
      }
    }
  }
}
