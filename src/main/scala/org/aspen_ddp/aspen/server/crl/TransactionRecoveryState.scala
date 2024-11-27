package org.aspen_ddp.aspen.server.crl

import org.aspen_ddp.aspen.common.DataBuffer
import org.aspen_ddp.aspen.common.paxos.PersistentState
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.transaction.{ObjectUpdate, TransactionDescription, TransactionDisposition, TransactionStatus}

case class TransactionRecoveryState( storeId: StoreId,
                                     serializedTxd: DataBuffer,
                                     objectUpdates: List[ObjectUpdate],
                                     disposition: TransactionDisposition.Value,
                                     status: TransactionStatus.Value,
                                     paxosAcceptorState: PersistentState):
  def txd: TransactionDescription = TransactionDescription.deserialize(serializedTxd)

object TransactionRecoveryState {
  def initial(storeId: StoreId,
              txd: TransactionDescription,
              objectUpdates: List[ObjectUpdate]): TransactionRecoveryState = {


    val stxd = txd.serialize()

    TransactionRecoveryState(storeId, stxd, objectUpdates, TransactionDisposition.Undetermined,
      TransactionStatus.Unresolved, PersistentState.initial)
  }
}
