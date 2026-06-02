package org.aspen_ddp.aspen.server.network

import org.aspen_ddp.aspen.common.metadata.HostId
import org.aspen_ddp.aspen.common.network.{ClientResponse, TxMessage}
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.server.cnc.CnCFrontend

import scala.concurrent.Future

trait Messenger {

  def sendClientResponse(msg: ClientResponse): Unit

  def sendTransactionMessage(msg: TxMessage): Unit

  def sendTransactionMessages(msg: List[TxMessage]): Unit

  // Used in response to receiving an error message indicating that a host
  // received a message for a store it does not know about. Usually this
  // means that the store has been transferred to a hew host but the network
  // layer is still using the old host. This method will cause the network
  // layer to drop its cached value and lookup the new store location
  def dropCacheForStore(storeId: StoreId): Unit
}
