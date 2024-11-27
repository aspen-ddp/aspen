package org.aspen_ddp.aspen.server.crl

import org.aspen_ddp.aspen.common.{DataBuffer, HLCTimestamp}
import org.aspen_ddp.aspen.common.objects.{ObjectId, ObjectRefcount, ObjectType}
import org.aspen_ddp.aspen.common.store.{StoreId, StorePointer}
import org.aspen_ddp.aspen.common.transaction.TransactionId

case class AllocationRecoveryState(
                                    storeId: StoreId,
                                    storePointer: StorePointer,
                                    newObjectId: ObjectId,
                                    objectType: ObjectType.Value,
                                    objectSize: Option[Int],
                                    objectData: DataBuffer,
                                    initialRefcount: ObjectRefcount,
                                    timestamp: HLCTimestamp,
                                    allocationTransactionId: TransactionId,
                                    serializedRevisionGuard: DataBuffer
                                  )
