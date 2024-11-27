package org.aspen_ddp.aspen.server.network

import org.aspen_ddp.aspen.common.network.{ClientResponse, TxMessage}

trait Messenger {

  def sendClientResponse(msg: ClientResponse): Unit

  def sendTransactionMessage(msg: TxMessage): Unit

  def sendTransactionMessages(msg: List[TxMessage]): Unit

}
