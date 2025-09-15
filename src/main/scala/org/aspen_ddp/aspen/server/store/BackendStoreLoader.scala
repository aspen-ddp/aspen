package org.aspen_ddp.aspen.server.store

import org.aspen_ddp.aspen.server.store.backend.Backend

import java.nio.file.Path
import scala.concurrent.ExecutionContext

trait BackendStoreLoader:
  def loadStoreFromPath(storePath: Path)(using ExecutionContext): Option[Backend]
