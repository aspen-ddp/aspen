package org.aspen_ddp.aspen.client

import org.aspen_ddp.aspen.codec
import org.aspen_ddp.aspen.common.network.Codec
import org.aspen_ddp.aspen.common.util.YamlFormat.{Format, FormatError}

import java.util.UUID

final case class HostId(uuid: UUID) extends AnyVal

object HostId:

  object YHostId extends Format[HostId]:
    override def format(o: Object): HostId = o match
      case v: java.lang.String =>
        try
          HostId(UUID.fromString(v))
        catch
          case t: Throwable => throw new FormatError(s"Invalid UUID: $t")
      case _ => throw new FormatError(s"String Required")


object Host:
  def apply(buff: Array[Byte]): Host = Codec.decode(codec.Host.parseFrom(buff))


case class Host(hostId: HostId,
                name: String,
                address: String,
                dataPort: Int,
                cncPort: Int,
                storeTransferPort: Int,
                storageDevices: Set[StorageDeviceId]):
  
  def encode(): Array[Byte] = Codec.encode(this).toByteArray
  
  def addStorageDevice(deviceId: StorageDeviceId): Host =
    this.copy(storageDevices = storageDevices + deviceId)

  def removeStorageDevice(deviceId: StorageDeviceId): Host =
    this.copy(storageDevices = storageDevices + deviceId)