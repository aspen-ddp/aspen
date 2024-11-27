package org.aspen_ddp.aspen.server.crl.simple

import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.transaction.TransactionId

final case class TxId(storeId: StoreId, transactionId: TransactionId):

  override def equals(other: Any): Boolean = {
    other match {
      case that: TxId => that.storeId == this.storeId && that.transactionId == this.transactionId
      case _ => false
    }
  }

  override def hashCode: Int = (storeId, transactionId).##

object TxId:
  /// store::Id + UUID
  val StaticSize: Int = 17 + 16

