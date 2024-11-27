package org.aspen_ddp.aspen.demo

import java.io.{File, FileInputStream}
import java.util.UUID
import org.aspen_ddp.aspen.common.util.YamlFormat.*
import org.yaml.snakeyaml.Yaml
import org.yaml.snakeyaml.constructor.SafeConstructor

import java.nio.file.Path

/*
name: node_a
uuid: 00000000-0000-0000-0000-000000000000
root-dir: local/node_a
endpoint:
  host: 127.0.0.1
  data-port: 5000
  cnc-port: 5001
  store-transfer-port: 5002
log4j-config: local/log4j-conf.xml
crl:
  storage-engine: simple-crl
*/

object StorageNodeConfig {

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


  case class Endpoint(host: String, dataPort: Int, cncPort: Int, storeTransferPort: Int)

  object Endpoint extends YObject[Endpoint]:
    val host: Required[String] = Required("host", YString)
    val dataPort: Required[Int] = Required("data-port", YInt)
    val cncPort: Required[Int] = Required("cnc-port", YInt)
    val storeTransferPort: Required[Int] = Required("store-transfer-port", YInt)

    val attrs: List[Attr] = host :: dataPort :: cncPort :: storeTransferPort :: Nil

    def create(o: Object): Endpoint = Endpoint(host.get(o), dataPort.get(o), cncPort.get(o), storeTransferPort.get(o))


  case class StorageNode(name: String,
                         uuid: UUID,
                         rootDir: Path,
                         endpoint: Endpoint,
                         log4jConfigFile: File,
                         crl: CRLBackend)

  object StorageNode extends YObject[StorageNode]:
    val name: Required[String] = Required("name", YString)
    val uuid: Required[UUID] = Required("uuid", YUUID)
    val rootDir: Required[String] = Required("root-dir", YString)
    val endpoint: Required[Endpoint] = Required("endpoint", Endpoint)
    val log4jConf: Required[File] = Required("log4j-config", YFile)
    val crl: Required[SimpleCRL] = Required("crl", Choice("storage-engine", Map("simple-crl" -> SimpleCRL)))

    val attrs: List[Attr] = name :: uuid :: rootDir :: endpoint :: log4jConf :: crl :: Nil

    def create(o: Object): StorageNode = StorageNode(name.get(o), uuid.get(o), 
      Path.of(rootDir.get(o)), endpoint.get(o), log4jConf.get(o), crl.get(o))


  def loadStorageNode(file: File): StorageNode =
    val yaml = new Yaml(new SafeConstructor)
    val y = yaml.load[java.util.AbstractMap[Object, Object]](new FileInputStream(file))
    StorageNode.create(y)
}
