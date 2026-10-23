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
  // means that the store has been transferred to a hew host but the network
  // layer is still using the old host. This method will cause the network
  // layer to drop its cached value and lookup the new store location
  def dropCacheForStore(storeId: StoreId): Unit

  /** True if `storeId` is believed offline for a transfer or rebuild. Advisory, used to choose designated leaders.
   *  Networks without gossip know of no offline stores.
   */
  def isStoreOffline(storeId: StoreId): Boolean = false

  /** True if the host `storeId` is mapped to is known to be online. Advisory, used to choose designated leaders.
   *  Never starts a lookup, so a store not yet mapped is not known online. Networks without liveness tracking
   *  report every host online.
   */
  def isStoreHostOnline(storeId: StoreId): Boolean = true


object Messenger:
  object None extends Messenger:
    def sendClientRequest(msg: ClientRequest): Unit = ()

    def sendTransactionMessage(msg: TxMessage): Unit = ()

    def sendTransactionMessages(msg: List[TxMessage]): Unit = ()

    def sendHostMessage(msg: HostMessage): Unit = ()

    def dropCacheForStore(storeId: StoreId): Unit = ()
