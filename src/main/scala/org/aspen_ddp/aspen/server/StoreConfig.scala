package org.aspen_ddp.aspen.server

import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.util.YamlFormat.*

import java.io.File
import java.util.UUID

/*
store-id: 00000000-0000-0000-0000-000000000000:0
backend:
  storage-engine: rocksdb
*/
case class StoreConfig(storeId: StoreId, backend: StoreConfig.StorageBackend):
  def yamlConfig: String =
    s"""store-id: $storeId
       |backend:
       |  storage-engine: ${backend.name}
       |""".stripMargin


object StoreConfig extends YObject[StoreConfig]:

  val configFilename = "aspen-store-config.yaml"
  
  sealed abstract class StorageBackend:
    def name: String

  case class RocksDB() extends StorageBackend:
    def name: String = "rocksdb"

  object RocksDB extends YObject[RocksDB]:
    val attrs: List[Attr] = Nil

    def create(o: Object): RocksDB = RocksDB()


  val storeId: Required[StoreId] = Required("store-id", StoreId.YStoreId)
  val backend: Required[RocksDB] = Required("backend", Choice("storage-engine", Map("rocksdb" -> RocksDB)))

  val attrs: List[Attr] = storeId :: backend :: Nil

  def create(o: Object): StoreConfig = StoreConfig(storeId.get(o), backend.get(o))
  
  def loadStoreConfig(file: File): StoreConfig =
    create(loadYamlFile(file))

