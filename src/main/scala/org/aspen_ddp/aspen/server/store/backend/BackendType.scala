package org.aspen_ddp.aspen.server.store.backend

sealed abstract class BackendType()

case class RocksDBType() extends BackendType
