package org.aspen_ddp.aspen.common.metadata

import org.aspen_ddp.aspen.client.KeyValueObjectState
import org.aspen_ddp.aspen.codec
import org.aspen_ddp.aspen.common.metadata.HostId
import org.aspen_ddp.aspen.common.network.Codec
import org.aspen_ddp.aspen.common.objects.Key
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.util.YamlFormat.{Format, FormatError}

import java.util.UUID

final case class StorageDeviceId(uuid: UUID) extends AnyVal

object StorageDeviceId:
  val BootstrapStorageDeviceId: StorageDeviceId = fixed_ids.BootstrapStorageDeviceId

  object YStorageDeviceId extends Format[StorageDeviceId]:
    override def format(o: Object): StorageDeviceId = o match
      case v: java.lang.String =>
        try
          StorageDeviceId(UUID.fromString(v))
        catch
          case t: Throwable => throw new FormatError(s"Invalid UUID: $t")
      case _ => throw new FormatError(s"String Required")


object StorageDeviceState:

  private [aspen] val StateKey = Key(Array[Byte](0))

  def apply(buff: Array[Byte]): StorageDeviceState = Codec.decode(codec.StorageDeviceState.parseFrom(buff))
  
  def apply(kvos: KeyValueObjectState): StorageDeviceState = 
    StorageDeviceState(kvos.contents(StateKey).value.bytes)
  
  enum StoreStatus:
    case Initializing, Active, TransferringIn, TransferringOut, Rebuilding

  case class StoreEntry(status: StoreStatus,
                        transferDevice: Option[StorageDeviceId])


case class StorageDeviceState(storageDeviceId: StorageDeviceId,
                              hostId: HostId,
                              currentUsage: Long,
                              totalSize: Long,
                              stores: Map[StoreId, StorageDeviceState.StoreEntry],
                              storageDeviceSet: StorageDeviceSetId):
  
  def encode(): Array[Byte] = Codec.encode(this).toByteArray
  
  def setHost(hostId: HostId): StorageDeviceState =
    this.copy(hostId=hostId)

  def setStoreEntry(storeId: StoreId,
                    status: StorageDeviceState.StoreStatus,
                    transferDevice: Option[StorageDeviceId]): StorageDeviceState =
    this.copy(stores=stores + (storeId -> StorageDeviceState.StoreEntry(status, transferDevice)))

  def removeStore(storeId: StoreId): StorageDeviceState =
    this.copy(stores=stores - storeId)