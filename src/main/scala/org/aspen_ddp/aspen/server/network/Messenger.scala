package org.aspen_ddp.aspen.server.network

import org.aspen_ddp.aspen.client.HostId
import org.aspen_ddp.aspen.common.network.{ClientResponse, TxMessage}
import org.aspen_ddp.aspen.server.cnc.CnCFrontend

import scala.concurrent.Future

trait Messenger {

  def sendClientResponse(msg: ClientResponse): Unit

  def sendTransactionMessage(msg: TxMessage): Unit

  def sendTransactionMessages(msg: List[TxMessage]): Unit

  //def getCnCFrontendForHost(hostId: HostId): Future[CnCFrontend]
}
