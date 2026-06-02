package org.aspen_ddp.aspen.client.internal.network

import org.aspen_ddp.aspen.common.network.{ClientRequest, HostMessage, TxMessage}
import org.aspen_ddp.aspen.common.store.StoreId

trait Messenger:

  def sendClientRequest(msg: ClientRequest): Unit

  def sendTransactionMessage(msg: TxMessage): Unit

  def sendTransactionMessages(msg: List[TxMessage]): Unit

  def sendHostMessage(msg: HostMessage): Unit
  
  // Used in response to receiving an error message indicating that a host
  // received a message for a store it does not know about. Usually this
  // means that the store has been transferred to a hew hostState but the network
  // layer is still using the old host. This method will cause the network
  // layer to drop its cached value and lookup the new store location
  def dropCacheForStore(storeId: StoreId): Unit


object Messenger:
  object None extends Messenger:
    def sendClientRequest(msg: ClientRequest): Unit = ()

    def sendTransactionMessage(msg: TxMessage): Unit = ()

    def sendTransactionMessages(msg: List[TxMessage]): Unit = ()

    def sendHostMessage(msg: HostMessage): Unit = ()

    def dropCacheForStore(storeId: StoreId): Unit = ()
