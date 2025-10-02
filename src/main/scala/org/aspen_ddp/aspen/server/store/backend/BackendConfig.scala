package org.aspen_ddp.aspen.server.store.backend

sealed abstract class BackendConfig:
  val backendName: String

case class RocksDBConfig() extends BackendConfig:
  val backendName: String = "rocksdb"