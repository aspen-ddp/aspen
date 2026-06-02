package org.aspen_ddp.aspen.common.network

trait MessageHandler:
  def onClientResponseReceived(msg: ClientResponse): Unit
  def onClientRequestReceived(msg: ClientRequest): Unit
  def onTransactionMessageReceived(msg: TxMessage): Unit
  def onHostMessageReceived(msg: HostMessage): Unit
