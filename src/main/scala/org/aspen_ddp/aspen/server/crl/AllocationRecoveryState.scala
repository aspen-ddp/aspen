package org.aspen_ddp.aspen.server.crl

import org.aspen_ddp.aspen.common.{DataBuffer, HLCTimestamp}
import org.aspen_ddp.aspen.common.objects.{ObjectId, ObjectRefcount, ObjectType}
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.transaction.TransactionId

case class AllocationRecoveryState(
                                    storeId: StoreId,
                                    newObjectId: ObjectId,
                                    objectType: ObjectType.Value,
                                    objectData: DataBuffer,
                                    initialRefcount: ObjectRefcount,
                                    timestamp: HLCTimestamp,
                                    allocationTransactionId: TransactionId,
                                    serializedRevisionGuard: DataBuffer
                                  )
