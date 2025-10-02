package org.aspen_ddp.aspen.client

import org.aspen_ddp.aspen.codec
import org.aspen_ddp.aspen.common.network.Codec
import org.aspen_ddp.aspen.common.store.StoreId

import java.util.UUID

final case class StorageDeviceId(uuid: UUID) extends AnyVal

object StorageDevice:

  def apply(buff: Array[Byte]): StorageDevice = Codec.decode(codec.StorageDevice.parseFrom(buff))
  
  enum StoreStatus:
    case Initializing, Active, TransferringIn, TransferringOut, Rebuilding

  case class StoreEntry(status: StoreStatus,
                        transferDevice: Option[StorageDeviceId])


case class StorageDevice(storageDeviceId: StorageDeviceId,
                         ohostId: Option[HostId],
                         stores: Map[StoreId, StorageDevice.StoreEntry]):
  
  def encode(): Array[Byte] = Codec.encode(this).toByteArray
  
  def setHost(ohostId: Option[HostId]): StorageDevice =
    this.copy(ohostId=ohostId)

  def setStoreEntry(storeId: StoreId, 
                    status: StorageDevice.StoreStatus,
                    transferDevice: Option[StorageDeviceId]): StorageDevice =
    this.copy(stores=stores + (storeId -> StorageDevice.StoreEntry(status, transferDevice)))

  def removeStore(storeId: StoreId): StorageDevice =
    this.copy(stores=stores - storeId)