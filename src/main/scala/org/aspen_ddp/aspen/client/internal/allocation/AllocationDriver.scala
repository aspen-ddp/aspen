package org.aspen_ddp.aspen.client.internal.allocation

import org.aspen_ddp.aspen.client.AspenClient
import org.aspen_ddp.aspen.common.ida.IDA
import org.aspen_ddp.aspen.common.objects.{AllocationRevisionGuard, ObjectId, ObjectPointer, ObjectRefcount, ObjectType}
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.{StoreId, StorePointer}
import org.aspen_ddp.aspen.common.transaction.TransactionId
import org.aspen_ddp.aspen.common.{DataBuffer, HLCTimestamp}

import scala.concurrent.Future

trait AllocationDriver {


  def futureResult: Future[ObjectPointer]

  /** Immediately cancels all future activity scheduled for execution */
  def shutdown(): Unit

  /** Initiates the allocation process */
  def start(): Unit

  def receiveAllocationResult(fromStoreId: StoreId,
                              result: Option[StorePointer]): Unit
}

object AllocationDriver {
  trait Factory {
    def create(client: AspenClient,
               poolId: PoolId,
               newObjectId: ObjectId,
               objectSize: Option[Int],
               objectIDA: IDA,
               objectData: Map[Byte,DataBuffer], // Map DataStore pool index -> store-specific ObjectData
               objectType: ObjectType.Value,
               timestamp: HLCTimestamp,
               initialRefcount: ObjectRefcount,
               allocationTransactionId: TransactionId,
               revisionGuard: AllocationRevisionGuard): AllocationDriver
  }

}
