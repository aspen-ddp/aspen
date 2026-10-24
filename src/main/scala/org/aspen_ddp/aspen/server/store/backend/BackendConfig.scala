package org.aspen_ddp.aspen.server.store.backend

sealed abstract class BackendConfig:
  val backendName: String

/** @param maxSliceSize Optional limit, in bytes, on the slice a store using this backend will accept. Chosen at pool
  *                     creation and combined with the pool's own maxSliceSize as min(pool, backend) to give each
  *                     store's effective limit. Most code should use the derived maxObjectSize instead.
  */
case class RocksDBConfig(maxSliceSize: Option[Int] = None) extends BackendConfig:
  val backendName: String = "rocksdb"