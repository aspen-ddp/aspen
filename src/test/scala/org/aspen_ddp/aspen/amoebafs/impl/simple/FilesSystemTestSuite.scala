package org.aspen_ddp.aspen.amoebafs.impl.simple

import org.aspen_ddp.aspen.IntegrationTestSuite
import org.aspen_ddp.aspen.client.internal.allocation.PoolObjectAllocator
import org.aspen_ddp.aspen.amoebafs.FileSystem
import org.aspen_ddp.aspen.amoebafs.impl.simple.SimpleFileSystem

import scala.concurrent.Future
import scala.language.implicitConversions

class FilesSystemTestSuite  extends IntegrationTestSuite {
  var fs: FileSystem = scala.compiletime.uninitialized

  // Each test gets a fresh TestNetwork from IntegrationTestSuite.withFixture, so a fixed name
  // cannot collide across tests. The parameter is here for tests that want more than one.
  def bootstrap(fsName: String = "test-fs"): Future[FileSystem] =
    for
      rootPool <- client.getStoragePool(radicle.poolId)
      allocator = new PoolObjectAllocator(client, rootPool)
      fs <- SimpleFileSystem.bootstrap(client, allocator, fsName)
    yield
      fs

  override def subFixtureTeardown(): Unit = {
    if (fs != null)
      fs.shutdown()
    fs = null
  }
}
