package org.aspen_ddp.aspen.server

import org.aspen_ddp.aspen.common.metadata.StorageDeviceId
import org.aspen_ddp.aspen.common.util.YamlFormat.*

import java.io.File
import java.util.UUID

/*
storage-device-id: 00000000-0000-0000-0000-000000000000
aspen-system-id: 00000000-0000-0000-0000-000000000000
*/
case class StorageDeviceConfig(storageDeviceId: StorageDeviceId,
                               aspenSystemId: UUID):
  def yamlConfig: String =
    s"""storage-device-id: ${storageDeviceId.uuid}
       |aspen-system-id: $aspenSystemId
       |""".stripMargin


object StorageDeviceConfig extends YObject[StorageDeviceConfig]:

  val configFilename = "aspen-storage-device-config.yaml"
  
  val storageDeviceId: Required[StorageDeviceId] = Required("storage-device-id", StorageDeviceId.YStorageDeviceId)
  val aspenSystemId: Required[UUID]              = Required("aspen-system-id", YUUID)

  val attrs: List[Attr] = storageDeviceId :: aspenSystemId :: Nil

  def create(o: Object): StorageDeviceConfig = StorageDeviceConfig(
    storageDeviceId.get(o),
    aspenSystemId.get(o)
  )

  def loadHostConfig(file: File): StorageDeviceConfig =
    create(loadYamlFile(file))
