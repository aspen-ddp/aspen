package org.aspen_ddp.aspen.client

import org.aspen_ddp.aspen.codec
import org.aspen_ddp.aspen.common.network.Codec
import org.aspen_ddp.aspen.common.objects.Key
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.util.YamlFormat.{Format, FormatError}

import java.util.UUID

final case class StorageDeviceId(uuid: UUID) extends AnyVal

object StorageDeviceId:
  object YStorageDeviceId extends Format[StorageDeviceId]:
    override def format(o: Object): StorageDeviceId = o match
      case v: java.lang.String =>
        try
          StorageDeviceId(UUID.fromString(v))
        catch
          case t: Throwable => throw new FormatError(s"Invalid UUID: $t")
      case _ => throw new FormatError(s"String Required")


object StorageDevice:

  private [aspen] val StateKey = Key(Array[Byte](0))

  def apply(buff: Array[Byte]): StorageDevice = Codec.decode(codec.StorageDevice.parseFrom(buff))
  
  enum StoreStatus:
    case Initializing, Active, TransferringIn, TransferringOut, Rebuilding

  case class StoreEntry(status: StoreStatus,
                        transferDevice: Option[StorageDeviceId])


case class StorageDevice(storageDeviceId: StorageDeviceId,
                         hostId: HostId,
                         stores: Map[StoreId, StorageDevice.StoreEntry]):
  
  def encode(): Array[Byte] = Codec.encode(this).toByteArray
  
  def setHost(hostId: HostId): StorageDevice =
    this.copy(hostId=hostId)

  def setStoreEntry(storeId: StoreId, 
                    status: StorageDevice.StoreStatus,
                    transferDevice: Option[StorageDeviceId]): StorageDevice =
    this.copy(stores=stores + (storeId -> StorageDevice.StoreEntry(status, transferDevice)))

  def removeStore(storeId: StoreId): StorageDevice =
    this.copy(stores=stores - storeId)