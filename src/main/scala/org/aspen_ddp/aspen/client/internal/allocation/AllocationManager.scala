package org.aspen_ddp.aspen.client.internal.allocation

import java.util.UUID

import org.aspen_ddp.aspen.client.{AllocationError, AspenClient, StoragePool, Transaction}
import org.aspen_ddp.aspen.common.{DataBuffer, HLCTimestamp}
import org.aspen_ddp.aspen.common.ida.IDA
import org.aspen_ddp.aspen.common.network.AllocateResponse
import org.aspen_ddp.aspen.common.objects.{AllocationRevisionGuard, DataObjectPointer, Key, KeyValueObjectPointer, ObjectId, ObjectPointer, ObjectRefcount, ObjectRevision, ObjectType, Value}
import org.aspen_ddp.aspen.server.store.{KVObjectState, ValueState}

import scala.concurrent.{ExecutionContext, Future}

class AllocationManager(val client: AspenClient,
                        val driverFactory: AllocationDriver.Factory) {

  given ExecutionContext = client.clientContext

  // Maps newObjectUUID -> driver
  private var outstandingAllocations = Map[ObjectId, AllocationDriver]()

  def shutdown(): Unit = outstandingAllocations.foreach( t => t._2.shutdown() )

  def receive(m: AllocateResponse): Unit = {
    synchronized { outstandingAllocations.get(m.newObjectId) } foreach {
      driver => driver.receiveAllocationResult(m.fromStore, m.result)
    }
  }

  private def allocate[PointerType <: ObjectPointer](
                                                      client: AspenClient,
                                                      transaction: Transaction,
                                                      pool: StoragePool,
                                                      objectSize: Option[Int],
                                                      objectIDA: IDA,
                                                      encodedContent: Array[DataBuffer],
                                                      objectType: ObjectType.Value,
                                                      initialRefcount: ObjectRefcount,
                                                      revisionGuard: AllocationRevisionGuard
                                                    ): Future[PointerType] = {

    val hosts = pool.selectStoresForAllocation(objectIDA)
    val objectData = hosts.map(_.asInstanceOf[Byte]).zip(encodedContent).toMap
    val newObjectId = ObjectId(UUID.randomUUID())

    val timestamp = HLCTimestamp.now

    val driver = driverFactory.create(client, pool.poolId, newObjectId, objectSize, objectIDA, objectData, objectType,
      timestamp, initialRefcount, transaction.id, revisionGuard)

    synchronized { outstandingAllocations += (newObjectId -> driver) }

    driver.futureResult onComplete {
      _ => synchronized { outstandingAllocations -= newObjectId }
    }

    val r = driver.futureResult map { newObjectPtr =>
      AllocationFinalizationAction.addToTransaction(newObjectPtr, transaction)
      newObjectPtr.asInstanceOf[PointerType]
    }

    driver.start()

    r
  }

  def allocateDataObject(
                          client: AspenClient,
                          transaction: Transaction,
                          pool: StoragePool,
                          objectSize: Option[Int],
                          objectIDA: IDA,
                          initialRefcount: ObjectRefcount,
                          revisionGuard: AllocationRevisionGuard,
                          initialContent: DataBuffer): Future[DataObjectPointer] = {

    val encodedContent = objectIDA.encode(initialContent)

    if (objectSize.exists(encodedContent(0).size > _))
      Future.failed(AllocationError(pool.poolId))

    allocate(client, transaction, pool, objectSize, objectIDA, encodedContent, ObjectType.Data,
      initialRefcount, revisionGuard)
  }

  def allocateKeyValueObject(
                              client: AspenClient,
                              transaction: Transaction,
                              pool: StoragePool,
                              objectSize: Option[Int],
                              objectIDA: IDA,
                              initialRefcount: ObjectRefcount,
                              revisionGuard: AllocationRevisionGuard,
                              initialContent: Map[Key, Value],
                              minimum: Option[Key],
                              maximum: Option[Key],
                              left: Option[Value],
                              right: Option[Value]): Future[KeyValueObjectPointer] = {

    val now = HLCTimestamp.now
    val rev = ObjectRevision(transaction.id)
    val contents = initialContent.map(t => t._1 -> new ValueState(t._2, rev, now, None))

    val encodedContent = KVObjectState.encodeIDA(objectIDA, minimum, maximum, left, right, contents)

    if (objectSize.exists(encodedContent(0).size > _))
      return Future.failed(AllocationError(pool.poolId))

    allocate(client, transaction, pool, objectSize, objectIDA, encodedContent, ObjectType.KeyValue,
      initialRefcount, revisionGuard)
  }
}
