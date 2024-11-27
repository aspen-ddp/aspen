package org.aspen_ddp.aspen.demo

import java.io.{File, FileInputStream}
import java.util.UUID

import org.aspen_ddp.aspen.common.ida.{IDA, Replication}
import org.aspen_ddp.aspen.common.util.YamlFormat._
import org.yaml.snakeyaml.Yaml
import org.yaml.snakeyaml.constructor.SafeConstructor

/*
bootstrap-ida:
  type: replication
  width: 3
  write-threshold: 2

bootstrap-storage-nodes:
  - name: node_a
    host: 127.0.0.1
    data-port: 5000
    cnc-port: 5001
    store-transfer-port: 5002

  - name: node_b
    host: 127.0.0.1
    data-port: 5010
    cnc-port: 5011
    store-transfer-port: 5012

  - name: node_c
    host: 127.0.0.1
    data-port: 5020
    cnc-port: 5021
    store-transfer-port: 5022
*/

object BootstrapConfig {

  object ReplicationFormat extends YObject[IDA]:
    val width: Required[Int]          = Required("width",            YInt)
    val writeThreshold: Required[Int] = Required("write-threshold",  YInt)

    val attrs: List[Attr] = width :: writeThreshold :: Nil

    def create(o: Object): IDA = Replication(width.get(o), writeThreshold.get(o))

  val IDAOptions =  Map("replication" -> ReplicationFormat)


  case class BootstrapIDA(ida: IDA, maxObjectSize: Option[Int])

  object BootstrapIDA extends YObject[BootstrapIDA]:
    val ida: Required[IDA]           = Required("ida",  Choice("type", IDAOptions))
    val maxObjectSize: Optional[Int] = Optional("max-object-size", YInt)

    val attrs: List[Attr] = ida :: maxObjectSize :: Nil

    def create(o: Object): BootstrapIDA = BootstrapIDA(ida.get(o), maxObjectSize.get(o))


  case class StorageNode(name: String, host: String, dataPort: Int, cncPort: Int, storeTransferPort: Int)

  object StorageNode extends YObject[StorageNode]:
    val name: Required[String]  = Required("name", YString)
    val host: Required[String]  = Required("host", YString)
    val dataPort: Required[Int] = Required("data-port", YInt)
    val cncPort: Required[Int]  = Required("cnc-port", YInt)
    val storeTransferPort: Required[Int]  = Required("store-transfer-port", YInt)


    val attrs: List[Attr] = name :: host :: dataPort :: cncPort :: storeTransferPort :: Nil

    def create(o: Object): StorageNode = StorageNode(name.get(o), host.get(o), dataPort.get(o), cncPort.get(o), storeTransferPort.get(o))


  case class Config(bootstrapIDA: IDA, nodes: List[StorageNode]):
    // Validate config
    if nodes.length != bootstrapIDA.width then
      throw new FormatError("Number of nodes must exactly match the Bootstrap IDA width")

  object Config extends YObject[Config]:
    val bootstrapIDA: Required[IDA]        = Required("bootstrap-ida",           Choice("type", Map("replication" -> ReplicationFormat)))
    val nodes: Required[List[StorageNode]] = Required("bootstrap-storage-nodes", YList(StorageNode))

    val attrs: List[Attr] = bootstrapIDA :: nodes :: Nil

    def create(o: Object): Config = Config( bootstrapIDA.get(o), nodes.get(o) )


  def loadBootstrapConfig(file: File): Config = {
    val yaml = new Yaml(new SafeConstructor)
    val y = yaml.load[java.util.AbstractMap[Object,Object]](new FileInputStream(file))
    Config.create(y)
  }
}