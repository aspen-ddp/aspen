package org.aspen_ddp.aspen.server

import org.aspen_ddp.aspen.common.metadata.StorageDeviceId
import org.aspen_ddp.aspen.common.util.YamlFormat.*

import java.util.UUID

/*
storage-device-id: 00000000-0000-0000-0000-000000000000
aspen-system-id: 00000000-0000-0000-0000-000000000000
# Optional CRL sizing (CrlSizingDefaults). Each key left out takes its default, derived from the device's capacity:
# crl-num-streams: 4           # Files in the device's CRL (<device>/crl). Default and minimum 4
# crl-max-file-size-mb: 512    # Size of each. Default capacity / 2048 (4 KiB multiple), at least 64 MiB
# crl-max-streams: 20          # Most streams the CRL may grow to
# crl-min-free-percent: 5      # Free space the CRL leaves on the device when growing
*/

/** The CRL sizing keys are optional: a missing one takes the default `CrlSizingDefaults.resolve` computes when a CRL
 *  is created. Stream count and size apply only then; an existing CRL keeps the sizing it recorded
 *  (SimpleCRL.SizingFile), so neither editing them nor changing the defaults makes a CRL unopenable
 *  (systemic-loss-protection.md S6, SC1). The growth bounds are read at each open. `yamlConfig` writes only the keys
 *  that are set. */
case class StorageDeviceConfig(storageDeviceId: StorageDeviceId,
                               aspenSystemId: UUID,
                               crlNumStreams: Option[Int] = None,
                               crlFileSizeMb: Option[Int] = None,
                               crlMaxStreams: Option[Int] = None,
                               crlMinFreePercent: Option[Int] = None):
  def yamlConfig: String =
    def key(name: String, v: Option[Int]): String = v.fold("")(n => s"$name: $n\n")
    s"""storage-device-id: ${storageDeviceId.uuid}
       |aspen-system-id: $aspenSystemId
       |""".stripMargin +
      key("crl-num-streams", crlNumStreams) +
      key("crl-max-file-size-mb", crlFileSizeMb) +
      key("crl-max-streams", crlMaxStreams) +
      key("crl-min-free-percent", crlMinFreePercent)


object StorageDeviceConfig extends YObject[StorageDeviceConfig]:

  val configFilename = "aspen-storage-device-config.yaml"

  val storageDeviceId: Required[StorageDeviceId] = Required("storage-device-id", StorageDeviceId.YStorageDeviceId)
  val aspenSystemId: Required[UUID]              = Required("aspen-system-id", YUUID)
  val crlNumStreams: Optional[Int]               = Optional("crl-num-streams", YInt)
  val crlFileSizeMb: Optional[Int]               = Optional("crl-max-file-size-mb", YInt)
  val crlMaxStreams: Optional[Int]               = Optional("crl-max-streams", YInt)
  val crlMinFreePercent: Optional[Int]           = Optional("crl-min-free-percent", YInt)

  val attrs: List[Attr] = storageDeviceId :: aspenSystemId :: crlNumStreams :: crlFileSizeMb :: crlMaxStreams ::
    crlMinFreePercent :: Nil

  def create(o: Object): StorageDeviceConfig = StorageDeviceConfig(
    storageDeviceId.get(o),
    aspenSystemId.get(o),
    crlNumStreams.get(o),
    crlFileSizeMb.get(o),
    crlMaxStreams.get(o),
    crlMinFreePercent.get(o)
  )

  def loadStorageDeviceConfig(file: os.Path): StorageDeviceConfig =
    create(loadYamlFile(file))
