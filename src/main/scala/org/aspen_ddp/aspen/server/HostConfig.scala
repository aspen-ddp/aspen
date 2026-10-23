package org.aspen_ddp.aspen.server

import org.aspen_ddp.aspen.common.metadata.HostId
import org.aspen_ddp.aspen.common.util.YamlFormat.*
import org.aspen_ddp.aspen.common.util.physicalMemory
import org.aspen_ddp.aspen.server.HostConfig
import org.aspen_ddp.aspen.server.store.backend.RocksDBBackend

import java.io.File
import java.util.UUID

/*
host-id: 00000000-0000-0000-0000-000000000000
aspen-system-id: 00000000-0000-0000-0000-000000000000
name: bootstrap-host
address: 127.0.0.1
data-port: 4750
http-port: 4751
store-transfer-port: 4752
crl:
  storage-engine: simple-crl
  num-streams: 3
  max-file-size-mb: 300
object-cache-mb: 8192         # optional. Shared by all stores on the host. default: 25% of physical RAM
rocksdb:                      # optional, as is each entry. Shared by all RocksDB stores on the host
  memory-mb: 4096             # default: 25% of physical RAM
  write-buffer-mb: 1024       # default: 25% of memory-mb
  max-background-threads: 8   # default: number of cores
tls-certificate: |
  -----BEGIN CERTIFICATE-----
  ...
  -----END CERTIFICATE-----
tls-private-key: |
  -----BEGIN PRIVATE KEY-----
  ...
  -----END PRIVATE KEY-----
*/
case class HostConfig(hostId: HostId,
                      aspenSystemId: UUID,
                      name: String,
                      address: String,
                      dataPort: Int,
                      httpPort: Int,
                      storeTransferPort: Int,
                      crl: HostConfig.CRLBackend,
                      tlsCertificate: String,
                      tlsPrivateKey: String,
                      rocksdb: HostConfig.RocksDBResources = HostConfig.RocksDBResources(),
                      objectCacheMb: Option[Int] = None):

  /** Capacity of the object cache shared by all stores on the host */
  def objectCacheBytes: Long = objectCacheMb match
    case Some(mb) => mb.toLong * 1024 * 1024
    case None => (physicalMemory() * HostConfig.DefaultObjectCacheFraction).toLong

  def yamlConfig: String =
    val base = s"""host-id: ${hostId.uuid}
       |aspen-system-id: $aspenSystemId
       |name: $name
       |address: $address
       |data-port: $dataPort
       |http-port: $httpPort
       |store-transfer-port: $storeTransferPort
       |crl:
       |""".stripMargin

    val crlCfg = crl match
      case s: HostConfig.SimpleCRL =>
        s"""  storage-engine: simple-crl
           |  num-streams: ${s.numStreams}
           |  max-file-size-mb: ${s.fileSizeMb}
           |""".stripMargin

    val rocksdbCfg =
      val entries = List(
        rocksdb.memoryMb.map(v => s"  memory-mb: $v\n"),
        rocksdb.writeBufferMb.map(v => s"  write-buffer-mb: $v\n"),
        rocksdb.maxBackgroundThreads.map(v => s"  max-background-threads: $v\n")).flatten
      if entries.isEmpty then
        ""
      else
        "rocksdb:\n" + entries.mkString

    val objectCacheCfg = objectCacheMb.map(v => s"object-cache-mb: $v\n").getOrElse("")

    base + crlCfg + objectCacheCfg + rocksdbCfg +
      yamlBlockScalar("tls-certificate", tlsCertificate) +
      yamlBlockScalar("tls-private-key", tlsPrivateKey)


object HostConfig extends YObject[HostConfig]:

  val configFilename = "aspen-host-config.yaml"

  /** Share of physical RAM given to the object cache when the host config does not specify it */
  val DefaultObjectCacheFraction = 0.25

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

  /** Host-wide RocksDB limits. Unspecified values are derived from the machine. See RocksDBBackend.ResourceConfig */
  case class RocksDBResources(memoryMb: Option[Int] = None,
                              writeBufferMb: Option[Int] = None,
                              maxBackgroundThreads: Option[Int] = None):
    def resourceConfig: RocksDBBackend.ResourceConfig = RocksDBBackend.ResourceConfig.resolve(
      memoryMb.map(_.toLong * 1024 * 1024),
      writeBufferMb.map(_.toLong * 1024 * 1024),
      maxBackgroundThreads)

  object RocksDBResources extends YObject[RocksDBResources]:
    val memoryMb: Optional[Int]             = Optional("memory-mb", YInt)
    val writeBufferMb: Optional[Int]        = Optional("write-buffer-mb", YInt)
    val maxBackgroundThreads: Optional[Int] = Optional("max-background-threads", YInt)
    val attrs: List[Attr] = memoryMb :: writeBufferMb :: maxBackgroundThreads :: Nil

    def create(o: Object): RocksDBResources = RocksDBResources(
      memoryMb.get(o),
      writeBufferMb.get(o),
      maxBackgroundThreads.get(o))

  val hostId: Required[HostId]         = Required("host-id", HostId.YHostId)
  val aspenSystemId: Required[UUID]    = Required("aspen-system-id", YUUID)
  val name: Required[String]           = Required("name", YString)
  val address: Required[String]        = Required("address", YString)
  val dataPort: Required[Int]          = Required("data-port", YInt)
  val httpPort: Required[Int]           = Required("http-port", YInt)
  val storeTransferPort: Required[Int] = Required("store-transfer-port", YInt)
  val crl: Required[SimpleCRL]         = Required("crl", Choice("storage-engine", Map("simple-crl" -> SimpleCRL)))

  val tlsCertificate: Required[String] = Required("tls-certificate", YString)
  val tlsPrivateKey: Required[String]  = Required("tls-private-key", YString)
  val rocksdb: Optional[RocksDBResources] = Optional("rocksdb", RocksDBResources)
  val objectCacheMb: Optional[Int]     = Optional("object-cache-mb", YInt)

  val attrs: List[Attr] = List(
    hostId,
    aspenSystemId,
    name,
    address,
    dataPort,
    httpPort,
    storeTransferPort,
    crl,
    tlsCertificate,
    tlsPrivateKey,
    rocksdb,
    objectCacheMb
  )

  def create(o: Object): HostConfig = HostConfig(
    hostId.get(o),
    aspenSystemId.get(o),
    name.get(o),
    address.get(o),
    dataPort.get(o),
    httpPort.get(o),
    storeTransferPort.get(o),
    crl.get(o),
    tlsCertificate.get(o),
    tlsPrivateKey.get(o),
    rocksdb.get(o).getOrElse(RocksDBResources()),
    objectCacheMb.get(o)
  )

  def loadHostConfig(file: File): HostConfig =
    create(loadYamlFile(file))
