package org.aspen_ddp.aspen.client

import org.aspen_ddp.aspen.common.store.StoreId

import java.util.UUID

final case class StorageDeviceId(uuid: UUID) extends AnyVal

object StorageDevice:
  enum Status:
    case Initializing, Active, TransferringIn, TransferringOut, Rebuilding
    
  case class StoreEntry(storeId: StoreId, 
                        status: Status, 
                        transferDevice: Option[StorageDeviceId])

class StorageDevice(val storageDeviceId: StorageDeviceId, 
                    val stores: Set[StorageDevice.StoreEntry])
