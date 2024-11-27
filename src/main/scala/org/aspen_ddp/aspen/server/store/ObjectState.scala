package org.aspen_ddp.aspen.server.store

import org.aspen_ddp.aspen.common.DataBuffer
import org.aspen_ddp.aspen.common.objects.{Metadata, ObjectId, ObjectType}
import org.aspen_ddp.aspen.common.store.StorePointer
import org.aspen_ddp.aspen.common.transaction.TransactionId

class ObjectState(val objectId: ObjectId,
                  val storePointer: StorePointer,
                  var metadata: Metadata,
                  val objectType: ObjectType.Value,
                  var data: DataBuffer,
                  val maxSize: Option[Int]) {

  /** Used to track the number of references currently working on this object.
    * The object is not allowed to exit the cache until this number drops to
    * zero.
    */
  var transactionReferences: Int = 0

  var lockedToTransaction: Option[TransactionId] = None

  var kvState: Option[KVObjectState] = objectType match {
    case ObjectType.KeyValue => Some(KVObjectState(data))
    case _ => None
  }

  def lockedWriteTransactions: Set[TransactionId] = {
    var s = Set[TransactionId]()
    lockedToTransaction.foreach { t => s += t }
    kvState.foreach { kvs =>
      kvs.content.values.foreach { vs =>
        vs.lockedToTransaction.foreach { txid =>
          s += txid
        }
      }
    }
    s
  }
}
