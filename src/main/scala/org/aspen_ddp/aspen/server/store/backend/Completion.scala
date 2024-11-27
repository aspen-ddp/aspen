package org.aspen_ddp.aspen.server.store.backend

import org.aspen_ddp.aspen.common.objects.{ObjectId, ReadError}
import org.aspen_ddp.aspen.common.store.{ReadState, StoreId, StorePointer}
import org.aspen_ddp.aspen.common.transaction.TransactionId

sealed abstract class Completion {
  val storeId: StoreId
}

case class Read(storeId: StoreId,
                objectId: ObjectId,
                storePointer: StorePointer,
                result: Either[ReadState, ReadError.Value]) extends Completion

case class Commit(storeId: StoreId,
                  objectId: ObjectId,
                  transactionId: TransactionId,
                  result: Either[Unit, CommitError.Value]) extends Completion