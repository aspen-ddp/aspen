package org.aspen_ddp.aspen.server

import org.aspen_ddp.aspen.client.HostId
import org.aspen_ddp.aspen.common.util.YamlFormat.*
import org.aspen_ddp.aspen.server.HostConfig
import org.yaml.snakeyaml.Yaml
import org.yaml.snakeyaml.constructor.SafeConstructor

import java.io.{File, FileInputStream}
import java.util.UUID

/*
host-id: 00000000-0000-0000-0000-000000000000
name: bootstrap-host
aspen-system-id: 00000000-0000-0000-0000-000000000000
data-port: 4750
cnc-port: 4751
store-transfer-port: 4752
*/
case class HostConfig(hostId: HostId,
                      aspenSystemId: UUID,
                      name: String,
                      address: String,
                      dataPort: Int,
                      cncPort: Int,
                      storeTransferPort: Int,
                      log4jConfigFile: File,
                      crl: HostConfig.CRLBackend):
  def yamlConfig: String =
    val base = s"""host-id: $hostId
       |aspen-system-id: $aspenSystemId
       |name: $name
       |address: $address
       |data-port: $dataPort
       |cnc-port: $cncPort
       |store-transfer-port: $storeTransferPort
       |log4j-config: $log4jConfigFile
       |crl:
       |""".stripMargin

    val crlCfg = crl match
      case s: HostConfig.SimpleCRL =>
        s"""  storage-engine: simple-crl
           |  num-streams: ${s.numStreams}
           |  max-file-size-mb: ${s.fileSizeMb}
           |""".stripMargin

    base + crlCfg


object HostConfig extends YObject[HostConfig]:

  val configFilename = "aspen-host-config.yaml"

  sealed abstract class CRLBackend

  case class SimpleCRL(numStreams: Int,
                       fileSizeMb: Int) extends CRLBackend

  object SimpleCRL extends YObject[SimpleCRL]:
    val numStreams: Optional[Int] = Optional("num-streams", YInt)
    val fileSize: Optional[Int] = Optional("max-file-size-mb", YInt)
    val attrs: List[Attr] = numStreams :: fileSize :: Nil

    def create(o: Object): SimpleCRL = SimpleCRL(
      numStreams.get(o).getOrElse(3),
      fileSize.get(o).getOrElse(300))

  val hostId: Required[HostId]         = Required("host-id", HostId.YHostId)
  val aspenSystemId: Required[UUID]    = Required("aspen-system-id", YUUID)
  val name: Required[String]           = Required("name", YString)
  val address: Required[String]        = Required("address", YString)
  val dataPort: Required[Int]          = Required("data-port", YInt)
  val cncPort: Required[Int]           = Required("cnc-port", YInt)
  val storeTransferPort: Required[Int] = Required("store-transfer-port", YInt)
  val log4jConf: Required[File]        = Required("log4j-config", YFile)
  val crl: Required[SimpleCRL]         = Required("crl", Choice("storage-engine", Map("simple-crl" -> SimpleCRL)))

  val attrs: List[Attr] = List(
    hostId,
    aspenSystemId,
    name,
    address,
    dataPort,
    cncPort,
    storeTransferPort,
    log4jConf,
    crl
  )

  def create(o: Object): HostConfig = HostConfig(
    hostId.get(o),
    aspenSystemId.get(o),
    name.get(o),
    address.get(o),
    dataPort.get(o),
    cncPort.get(o),
    storeTransferPort.get(o),
    log4jConf.get(o),
    crl.get(o)
  )

  def loadHostConfig(file: File): HostConfig =
    val yaml = new Yaml(new SafeConstructor)
    val y = yaml.load[java.util.AbstractMap[Object, Object]](new FileInputStream(file))
    create(y)
