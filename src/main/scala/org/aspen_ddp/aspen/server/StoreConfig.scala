package org.aspen_ddp.aspen.server

import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.util.YamlFormat.*

import java.util.UUID

/*
store-id: 00000000-0000-0000-0000-000000000000:0
backend:
  storage-engine: rocksdb
  max-slice-size: 2048      # optional, backend limit
pool-max-slice-size: 4096   # optional, pool limit
*/
/** @param poolMaxSliceSize The pool's maxSliceSize, copied from the StoragePoolState when the store was created.
  *                         None means the pool has no limit.
  */
case class StoreConfig(storeId: StoreId,
                       backend: StoreConfig.StorageBackend,
                       poolMaxSliceSize: Option[Int]):

  /** The limit the store enforces: the smaller of the pool limit and the backend limit when both are present, the
    * only one present otherwise, or None when the store is unlimited.
    */
  def effectiveMaxSliceSize: Option[Int] =
    (poolMaxSliceSize, backend.maxSliceSize) match
      case (Some(p), Some(b)) => Some(math.min(p, b))
      case (p, None) => p
      case (None, b) => b

  def yamlConfig: String =
    val backendLimit = backend.maxSliceSize.map(v => s"\n  max-slice-size: $v").getOrElse("")
    val poolLimit = poolMaxSliceSize.map(v => s"\npool-max-slice-size: $v").getOrElse("")
    s"""store-id: $storeId
       |backend:
       |  storage-engine: ${backend.name}$backendLimit$poolLimit
       |""".stripMargin


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

    def create(o: Object): RocksDB = RocksDB(maxSliceSize.get(o))


  val storeId: Required[StoreId] = Required("store-id", StoreId.YStoreId)
  val backend: Required[RocksDB] = Required("backend", Choice("storage-engine", Map("rocksdb" -> RocksDB)))

  val poolMaxSliceSize: Optional[Int] = Optional("pool-max-slice-size", YInt)

  val attrs: List[Attr] = storeId :: backend :: poolMaxSliceSize :: Nil

  def create(o: Object): StoreConfig = StoreConfig(storeId.get(o), backend.get(o), poolMaxSliceSize.get(o))
  
  def loadStoreConfig(file: os.Path): StoreConfig =
    create(loadYamlFile(file))

