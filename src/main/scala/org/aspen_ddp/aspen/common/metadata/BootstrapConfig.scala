package org.aspen_ddp.aspen.common.metadata

import org.aspen_ddp.aspen.common.ida.{IDA, ReedSolomon, Replication}
import org.aspen_ddp.aspen.common.metadata.{HostId, HostState}
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.util.YamlFormat.*

import java.io.File
import java.util.UUID

/*
aspen-system-id:  BF1049AD-D2A8-4D17-8080-E01A4678C8B3
bootstrap-ida:
  type: replication
  write-threshold: 1
  width: 1
bootstrap-hosts:
  - host-id: AA1049AD-D2A8-4D17-8080-E01A4678C8B3
    name: node_a
    address: 127.0.0.1
    data-port: 5000
    cnc-port: 5001
    store-transfer-port: 5002
    stores:
      - 00000000-0000-0000-0000-000000000000:0
*/

object BootstrapConfig:

  val configFilename = "aspen-bootstrap-config.yaml"

  object ReplicationFormat extends YObject[IDA]:
    val readThreshold: Optional[Int]  = Optional("read-threshold", YInt)
    val writeThreshold: Required[Int] = Required("write-threshold", YInt)
    val width: Required[Int]          = Required("width", YInt)

    val attrs: List[Attr] = readThreshold :: writeThreshold :: width :: Nil

    def create(o: Object): IDA = Replication(width.get(o), writeThreshold.get(o))

  object ReedSolomonFormat extends YObject[IDA]:
    val readThreshold: Required[Int]  = Required("read-threshold", YInt)
    val writeThreshold: Required[Int] = Required("write-threshold", YInt)
    val width: Required[Int]          = Required("width", YInt)

    val attrs: List[Attr] = readThreshold :: writeThreshold :: width :: Nil

    def create(o: Object): IDA = ReedSolomon(width.get(o), readThreshold.get(o), writeThreshold.get(o))

  val IDAOptions =  Map(
    "replication" -> ReplicationFormat,
    "reed-solomon" -> ReedSolomonFormat
  )

  case class BootstrapIDA(ida: IDA, maxObjectSize: Option[Int])

  object BootstrapIDA extends YObject[BootstrapIDA]:
    val ida: Required[IDA]           = Required("ida",  Choice("type", IDAOptions))
    val maxObjectSize: Optional[Int] = Optional("max-object-size", YInt)

    val attrs: List[Attr] = ida :: maxObjectSize :: Nil

    def create(o: Object): BootstrapIDA = BootstrapIDA(ida.get(o), maxObjectSize.get(o))

  case class BootstrapHost(hostId: HostId,
                           name: String,
                           address: String,
                           dataPort: Int,
                           cncPort: Int,
                           storeTransferPort: Int,
                           stores: List[StoreId])

  object BootstrapHost extends YObject[BootstrapHost]:
    val hostId: Required[HostId]         = Required("host-id", HostId.YHostId)
    val name: Required[String]           = Required("name", YString)
    val address: Required[String]        = Required("address", YString)
    val dataPort: Required[Int]          = Required("data-port", YInt)
    val cncPort: Required[Int]           = Required("cnc-port", YInt)
    val storeTransferPort: Required[Int] = Required("store-transfer-port", YInt)
    val stores: Required[List[StoreId]]  = Required("stores", YList(StoreId.YStoreId))

    val attrs: List[Attr] = hostId :: name :: address :: dataPort :: cncPort :: storeTransferPort :: stores :: Nil

    def create(o: Object): BootstrapHost = BootstrapHost(
      hostId.get(o),
      name.get(o),
      address.get(o),
      dataPort.get(o),
      cncPort.get(o),
      storeTransferPort.get(o),
      stores.get(o)
    )

  case class Config(aspenSystemId: UUID, bootstrapIDA: IDA, hosts: List[BootstrapHost]):
    // Validate config. The bootstrap pool has exactly one store per IDA element, but those
    // stores may sit on any number of hosts: a freshly bootstrapped system puts all of them
    // on the single bootstrap host and they spread out from there as hosts are added. So it
    // is the store count, not the host count, that must match the width.
    private val storeCount: Int = hosts.map(_.stores.length).sum

    if storeCount != bootstrapIDA.width then
      throw new FormatError(s"Number of bootstrap stores ($storeCount) must exactly match " +
                            s"the Bootstrap IDA width (${bootstrapIDA.width})")

  object Config extends YObject[Config]:
    val aspenSystemId: Required[UUID]        = Required("aspen-system-id", YUUID)
    val bootstrapIDA: Required[IDA]          = Required("bootstrap-ida",   Choice("type", Map("replication" -> ReplicationFormat)))
    val hosts: Required[List[BootstrapHost]] = Required("bootstrap-hosts", YList(BootstrapHost))

    val attrs: List[Attr] = aspenSystemId :: bootstrapIDA :: hosts :: Nil

    def create(o: Object): Config = Config( aspenSystemId.get(o), bootstrapIDA.get(o), hosts.get(o) )


  def loadBootstrapConfig(file: File): Config =
    Config.create(loadYamlFile(file))

  /** Parses a bootstrap config held in memory, applying exactly the validation
   *  loadBootstrapConfig applies to a file. */
  def parseBootstrapConfig(yaml: String): Config =
    Config.create(loadYamlString(yaml))


  def generateBootstrapConfig(aspenSystemId: UUID,
                              ida: IDA,
                              hostStates: List[HostState],
                              storeMap: List[(StoreId, HostId)]): String =
    val hostIdSet = hostStates.map(_.hostId).toSet

    storeMap.foreach: (_, hostId) =>
      require(hostIdSet.contains(hostId))

    val sb = StringBuilder()

    sb.append(s"aspen-system-id: $aspenSystemId\n")
    sb.append(s"bootstrap-ida:\n")
    sb.append(s"  type: ${ida.name}\n")
    ida match
      case _:Replication =>
      case _:ReedSolomon =>
        sb.append(s"  read-threshold: ${ida.consistentRestoreThreshold}\n")
    sb.append(s"  write-threshold: ${ida.writeThreshold}\n")
    sb.append(s"  width: ${ida.width}\n")
    sb.append("bootstrap-hosts:\n")
    hostStates.foreach: host =>
      val storesOnHost = storeMap.filter(t => t._2 == host.hostId).map(t => t._1)

      require(storesOnHost.nonEmpty)

      sb.append(f"  - host-id: ${host.hostId.uuid}\n")
      sb.append(f"    name: ${host.name}\n")
      sb.append(f"    address: ${host.address}\n")
      sb.append(f"    data-port: ${host.dataPort}\n")
      sb.append(f"    cnc-port: ${host.cncPort}\n")
      sb.append(f"    store-transfer-port: ${host.storeTransferPort}\n")
      sb.append(f"    stores:\n")
      storesOnHost.foreach: storeId =>
        sb.append(f"      - $storeId\n")

    sb.toString
