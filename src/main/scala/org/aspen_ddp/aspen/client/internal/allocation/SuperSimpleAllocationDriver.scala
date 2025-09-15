package org.aspen_ddp.aspen.client.internal.allocation

import org.aspen_ddp.aspen.client.AspenClient
import org.aspen_ddp.aspen.common.{DataBuffer, HLCTimestamp}
import org.aspen_ddp.aspen.common.ida.IDA
import org.aspen_ddp.aspen.common.objects.{AllocationRevisionGuard, ObjectId, ObjectRefcount, ObjectType}
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.transaction.TransactionId
import org.apache.logging.log4j.scala.Logging

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.Duration

object SuperSimpleAllocationDriver {

  def factory(retransmitDelay: Duration): AllocationDriver.Factory = {
    new AllocationDriver.Factory {
      def create(client: AspenClient,
                 poolId: PoolId,
                 newObjectId: ObjectId,
                 objectSize: Option[Int],
                 objectIDA: IDA,
                 objectData: Map[Byte, DataBuffer], // Map DataStore pool index -> store-specific ObjectData
                 objectType: ObjectType.Value,
                 timestamp: HLCTimestamp,
                 initialRefcount: ObjectRefcount,
                 allocationTransactionId: TransactionId,
                 revisionGuard: AllocationRevisionGuard): BaseAllocationDriver = {
        new SuperSimpleAllocationDriver(retransmitDelay, client, poolId, newObjectId, objectSize, objectIDA, objectData,
          objectType, timestamp, initialRefcount, allocationTransactionId, revisionGuard)
      }
    }
  }

}

class SuperSimpleAllocationDriver(retransmitDelay: Duration,
                                  client: AspenClient,
                                  poolId: PoolId,
                                  newObjectId: ObjectId,
                                  objectSize: Option[Int],
                                  objectIDA: IDA,
                                  objectData: Map[Byte, DataBuffer], // Map DataStore pool index -> store-specific ObjectData
                                  objectType: ObjectType.Value,
                                  timestamp: HLCTimestamp,
                                  initialRefcount: ObjectRefcount,
                                  allocationTransactionId: TransactionId,
                                  revisionGuard: AllocationRevisionGuard) extends BaseAllocationDriver(client, poolId,
  newObjectId, objectSize, objectIDA, objectData, objectType, timestamp, initialRefcount, allocationTransactionId,
  revisionGuard) with Logging {

  given ExecutionContext = client.clientContext

  private var retries = 0
  private val retryTask = client.backgroundTasks.schedulePeriodic(period=retransmitDelay) {
    synchronized {
      retries += 1
      if (retries % 3 == 0)
        logger.info(s"***** HUNG Allocation with Transaction $allocationTransactionId")
    }
    sendAllocationMessages()
  }

  futureResult.onComplete { _ => retryTask.cancel() }
}
