package org.aspen_ddp.aspen.demo

import org.apache.logging.log4j.scala.Logging
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.util.printStack
import org.aspen_ddp.aspen.server.store.BackendStoreLoader
import org.aspen_ddp.aspen.server.store.backend.{Backend, RocksDBBackend}

import java.nio.file.{Files, Path}
import scala.concurrent.ExecutionContext

class BackendStoreLoaderImpl extends BackendStoreLoader with Logging:

  override def loadStoreFromPath(storePath: Path)(using ec: ExecutionContext): Option[Backend] =

    val cfgFile = storePath.resolve("store_config.yaml")

    if Files.exists(cfgFile) then

      try
        val cfg = StoreConfig.loadStore(cfgFile.toFile)

        val storeId = StoreId(PoolId(cfg.poolUuid), cfg.index.asInstanceOf[Byte])

        val backend = cfg.backend match
          case b: StoreConfig.RocksDB => new RocksDBBackend(storePath, storeId, ec)

        Some(backend)
      catch
        case t: Throwable =>
          logger.info(s"Failed to load store ${storePath}. Error: $t")
          printStack()
          None
    else
      None
