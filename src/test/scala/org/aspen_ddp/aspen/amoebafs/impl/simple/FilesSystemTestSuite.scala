package org.aspen_ddp.aspen.amoebafs.impl.simple

import org.aspen_ddp.aspen.IntegrationTestSuite
import org.aspen_ddp.aspen.client.internal.allocation.SinglePoolObjectAllocator
import org.aspen_ddp.aspen.common.objects.{Key, ObjectRevisionGuard}
import org.aspen_ddp.aspen.amoebafs.FileSystem
import org.aspen_ddp.aspen.amoebafs.impl.simple.SimpleFileSystem

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration.*
import scala.language.implicitConversions

class FilesSystemTestSuite  extends IntegrationTestSuite {
  var fs: FileSystem = scala.compiletime.uninitialized

  //override def subFixtureSetup(): Unit = {
  def bootstrap(): Future[FileSystem] =
    for
      kvos <- client.read(radicle)
      rootPool <- client.getStoragePool(kvos.pointer.poolId)
      allocator = new SinglePoolObjectAllocator(client, rootPool, radicle.ida, None )
      fs <- SimpleFileSystem.bootstrap(client,
        ObjectRevisionGuard(radicle, kvos.revision),
        allocator,
        kvos.pointer,
        Key(100))
    yield
      fs

  override def subFixtureTeardown(): Unit = {
    if (fs != null)
      fs.shutdown()
    fs = null
  }
}
