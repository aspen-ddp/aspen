package org.aspen_ddp.aspen.client.internal.network

import org.aspen_ddp.aspen.common.network.{ClientRequest, HostMessage, TxMessage}

trait Messenger:

  def sendClientRequest(msg: ClientRequest): Unit

  def sendTransactionMessage(msg: TxMessage): Unit

  def sendTransactionMessages(msg: List[TxMessage]): Unit

  def sendHostMessage(msg: HostMessage): Unit


object Messenger:
  object None extends Messenger:
    def sendClientRequest(msg: ClientRequest): Unit = ()

    def sendTransactionMessage(msg: TxMessage): Unit = ()

    def sendTransactionMessages(msg: List[TxMessage]): Unit = ()

    def sendHostMessage(msg: HostMessage): Unit = ()
