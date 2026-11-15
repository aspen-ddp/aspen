package org.aspen_ddp.aspen.server

import org.aspen_ddp.aspen.common.config.BootstrapConfig
import org.aspen_ddp.aspen.common.ida.IDA
import org.aspen_ddp.aspen.common.metadata.StoragePoolState
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.util.YamlFormat.*
import org.aspen_ddp.aspen.server.crl.AdmissionTier
import org.aspen_ddp.aspen.server.store.backend.{BackendConfig, FsifConfig, RocksDBConfig}

import java.util.UUID

/*
store-id: 00000000-0000-0000-0000-000000000000:0
backend:
  storage-engine: rocksdb
  max-slice-size: 2048      # optional, backend limit
pool-max-slice-size: 4096   # optional, pool limit
admission-tier: metadata    # optional: data (the default), metadata or bootstrap (AdmissionTier)
ida:                        # required, the pool's IDA (BootstrapConfig.IDAOptions)
  type: replication
  write-threshold: 2
  width: 3

or, for an FSIF store:
backend:
  storage-engine: fsif
  segment-payload-size: 4096  # required, the backend limit
  segment-count: 1000         # required
*/
/** @param poolMaxSliceSize The pool's maxSliceSize, copied from the StoragePoolState when the store was created.
  *                         None means the pool has no limit.
  * @param ida              The pool's IDA, copied from the StoragePoolState when the store was created (a pool's IDA
  *                         never changes). Stores refuse a transaction description that names another IDA for their
  *                         pool (Frontend.poolIDA, DescriptionValidator)
  * @param admissionTier    The CRL admission lane of the store's new transactions (CrlAdmission), from the pool:
  *                         `AdmissionTier.forPool`, fixed when the pool is created
  */
case class StoreConfig(storeId: StoreId,
                       backend: StoreConfig.StorageBackend,
                       poolMaxSliceSize: Option[Int],
                       ida: IDA,
                       admissionTier: AdmissionTier = AdmissionTier.Data):

  /** The limit the store enforces: the smaller of the pool limit and the backend limit when both are present, the
    * only one present otherwise, or None when the store is unlimited.
    */
  def effectiveMaxSliceSize: Option[Int] =
    (poolMaxSliceSize, backend.maxSliceSize) match
      case (Some(p), Some(b)) => Some(math.min(p, b))
      case (p, None) => p
      case (None, b) => b

  def yamlConfig: String =
    val backendAttrs = backend match
      case b: StoreConfig.RocksDB => b.maxSliceSize.map(v => s"\n  max-slice-size: $v").getOrElse("")
      case b: StoreConfig.Fsif =>
        s"\n  segment-payload-size: ${b.segmentPayloadSize}\n  segment-count: ${b.segmentCount}"
    val poolLimit = poolMaxSliceSize.map(v => s"\npool-max-slice-size: $v").getOrElse("")
    // The default, Data, is left out
    val tier =
      if admissionTier == AdmissionTier.Data then
        ""
      else
        s"\nadmission-tier: ${StoreConfig.tierName(admissionTier)}"
    s"""store-id: $storeId
       |backend:
       |  storage-engine: ${backend.name}$backendAttrs$poolLimit$tier
       |ida:
       |${BootstrapConfig.idaYaml(ida)}""".stripMargin


object StoreConfig extends YObject[StoreConfig]:

  val configFilename = "aspen-store-config.yaml"
  
  sealed abstract class StorageBackend:
    def name: String

    /** The backend's own slice size limit, chosen at pool creation (RocksDBConfig.maxSliceSize). Combined with the
      * pool limit as min(pool, backend) to give the effective limit.
      */
    def maxSliceSize: Option[Int]

  case class RocksDB(maxSliceSize: Option[Int]) extends StorageBackend:
    def name: String = "rocksdb"

  object RocksDB extends YObject[RocksDB]:
    val maxSliceSize: Optional[Int] = Optional("max-slice-size", YInt)

    val attrs: List[Attr] = maxSliceSize :: Nil

    def create(o: Object): RocksDB = RocksDB(positiveLimit("backend.max-slice-size", maxSliceSize.get(o)))

  /** An FSIF store: see FsifConfig. Its segment payload size is its slice limit. */
  case class Fsif(segmentPayloadSize: Int, segmentCount: Int) extends StorageBackend:
    def name: String = "fsif"
    def maxSliceSize: Option[Int] = Some(segmentPayloadSize)

  object Fsif extends YObject[Fsif]:
    val segmentPayloadSize: Required[Int] = Required("segment-payload-size", YInt)
    val segmentCount: Required[Int] = Required("segment-count", YInt)

    val attrs: List[Attr] = segmentPayloadSize :: segmentCount :: Nil

    def create(o: Object): Fsif =
      def positive(key: String, v: Int): Int =
        if v <= 0 then
          throw new FormatError(s"backend.$key must be positive, got $v")
        v
      Fsif(positive("segment-payload-size", segmentPayloadSize.get(o)),
           positive("segment-count", segmentCount.get(o)))


  /** A new store's config, from its pool's state */
  def forPool(storeId: StoreId, pstate: StoragePoolState): StoreConfig =
    StoreConfig(storeId, storageBackendFor(pstate.backendConfig), pstate.maxSliceSize, pstate.ida,
      AdmissionTier.forPool(pstate.poolId, pstate.isMetadataPool))

  /** The store backend a pool's BackendConfig names */
  def storageBackendFor(cfg: BackendConfig): StorageBackend = cfg match
    case c: RocksDBConfig => RocksDB(c.maxSliceSize)
    case c: FsifConfig => Fsif(c.segmentPayloadSize, c.segmentCount)

  val storeId: Required[StoreId] = Required("store-id", StoreId.YStoreId)
  /** Format is invariant, so each backend's YObject is wrapped to produce the common StorageBackend */
  private def anyBackend(f: Format[? <: StorageBackend]): Format[StorageBackend] = o => f.format(o)

  val backend: Required[StorageBackend] = Required("backend",
    Choice("storage-engine", Map("rocksdb" -> anyBackend(RocksDB), "fsif" -> anyBackend(Fsif))))

  val poolMaxSliceSize: Optional[Int] = Optional("pool-max-slice-size", YInt)

  val ida: Required[IDA] = Required("ida", Choice("type", BootstrapConfig.IDAOptions))

  val admissionTier: Optional[String] = Optional("admission-tier", YString)

  val attrs: List[Attr] = storeId :: backend :: poolMaxSliceSize :: ida :: admissionTier :: Nil

  private val tierNames: Map[String, AdmissionTier] = Map("data" -> AdmissionTier.Data,
    "metadata" -> AdmissionTier.Metadata, "bootstrap" -> AdmissionTier.Bootstrap)

  def tierName(tier: AdmissionTier): String = tierNames.collectFirst { case (n, t) if t == tier => n }.get

  def create(o: Object): StoreConfig =
    val tier = admissionTier.get(o) match
      case None => AdmissionTier.Data
      case Some(n) => tierNames.getOrElse(n, throw new FormatError(s"admission-tier must be one of " +
        s"${tierNames.keys.mkString(", ")}, got $n"))
    StoreConfig(storeId.get(o), backend.get(o), positiveLimit("pool-max-slice-size", poolMaxSliceSize.get(o)),
      ida.get(o), tier)

  /** `limit` unchanged, or a FormatError if it is present but not positive: a slice limit of zero or less admits
    * nothing, so such a file is corrupt rather than a deliberate setting
    */
  private def positiveLimit(key: String, limit: Option[Int]): Option[Int] =
    limit.foreach: v =>
      if v <= 0 then
        throw new FormatError(s"$key must be positive, got $v")
    limit
  
  def loadStoreConfig(file: os.Path): StoreConfig =
    create(loadYamlFile(file))

