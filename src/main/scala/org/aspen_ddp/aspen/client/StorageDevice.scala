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

  case class StoreEntry(storeId: StoreId,
                        status: StoreStatus,
                        transferDevice: Option[StorageDeviceId])


class StorageDevice(val storageDeviceId: StorageDeviceId, 
                    val stores: Set[StorageDevice.StoreEntry]):
  
  def encode(): Array[Byte] = Codec.encode(this).toByteArray
