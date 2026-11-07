package org.aspen_ddp.aspen.server

import org.aspen_ddp.aspen.common.metadata.StorageDeviceId
import org.aspen_ddp.aspen.common.util.YamlFormat.*

import java.util.UUID

/*
storage-device-id: 00000000-0000-0000-0000-000000000000
aspen-system-id: 00000000-0000-0000-0000-000000000000
crl-num-streams: 3          # optional. Files in the device's CRL (<device>/crl). default: 3
crl-max-file-size-mb: 300   # optional. Size of each. default: 300
*/
case class StorageDeviceConfig(storageDeviceId: StorageDeviceId,
                               aspenSystemId: UUID,
                               crlNumStreams: Int = StorageDeviceConfig.DefaultCrlNumStreams,
                               crlFileSizeMb: Int = StorageDeviceConfig.DefaultCrlFileSizeMb):
  def yamlConfig: String =
    s"""storage-device-id: ${storageDeviceId.uuid}
       |aspen-system-id: $aspenSystemId
       |crl-num-streams: $crlNumStreams
       |crl-max-file-size-mb: $crlFileSizeMb
       |""".stripMargin


object StorageDeviceConfig extends YObject[StorageDeviceConfig]:

  val configFilename = "aspen-storage-device-config.yaml"

  val DefaultCrlNumStreams: Int = 3
  val DefaultCrlFileSizeMb: Int = 300

  val storageDeviceId: Required[StorageDeviceId] = Required("storage-device-id", StorageDeviceId.YStorageDeviceId)
  val aspenSystemId: Required[UUID]              = Required("aspen-system-id", YUUID)
  val crlNumStreams: Optional[Int]               = Optional("crl-num-streams", YInt)
  val crlFileSizeMb: Optional[Int]               = Optional("crl-max-file-size-mb", YInt)

  val attrs: List[Attr] = storageDeviceId :: aspenSystemId :: crlNumStreams :: crlFileSizeMb :: Nil

  def create(o: Object): StorageDeviceConfig = StorageDeviceConfig(
    storageDeviceId.get(o),
    aspenSystemId.get(o),
    crlNumStreams.get(o).getOrElse(DefaultCrlNumStreams),
    crlFileSizeMb.get(o).getOrElse(DefaultCrlFileSizeMb)
  )

  def loadStorageDeviceConfig(file: os.Path): StorageDeviceConfig =
    create(loadYamlFile(file))
