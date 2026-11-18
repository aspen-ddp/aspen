package org.aspen_ddp.aspen.client.internal.allocation

import org.aspen_ddp.aspen.IntegrationTestSuite
import org.aspen_ddp.aspen.client.Transaction
import org.aspen_ddp.aspen.common.metadata.StorageDeviceSetId
import org.aspen_ddp.aspen.common.objects.{Key, ObjectId}
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.server.store.backend.{FsifConfig, MapBackend, RocksDBConfig}

import scala.concurrent.{ExecutionContext, Future}

/** Every live object a store holds is listed in its pool's AllocationTree (rebuild-process 7). The tree is the only way
  * a rebuild finds the objects to restore, and repair takes absence from it as a sign of deletion, so an allocation
  * path that recorded no entry would lose objects on the next rebuild.
  */
class AllocationTreeCoverageSuite extends IntegrationTestSuite:

  /** The live objects of the test network's MapBackends with no entry in their pool's AllocationTree */
  private def unlisted(): Future[List[(PoolId, ObjectId)]] =
    given ExecutionContext = executionContext
    val backends: List[MapBackend] = net.stores ++ net.metadataStores
    val live = backends.flatMap: b =>
      b.m.valuesIterator.filter(_.metadata.refcount.count > 0).map(os => b.storeId.poolId -> os.objectId)
    Future.sequence(live.distinct.map: (poolId, objectId) =>
      client.getStoragePool(poolId)
        .flatMap(_.allocationTree.get(Key(objectId.toBytes)))
        .map(entry => if entry.isEmpty then Some(poolId -> objectId) else None))
    .map(_.flatten)

  atest("bootstrap lists every object it creates"):
    given ExecutionContext = executionContext
    for
      _ <- waitForTransactionsToComplete()
      missing <- unlisted()
    yield
      net.stores.head.m.size should be > 20
      missing should be(Nil)

  atest("pool creation, FSIF bitmaps and allocation-tree node splits are all listed"):
    given ExecutionContext = executionContext
    def allocate(i: Int): Future[Unit] =
      client.transactUntilSuccessful: tx =>
        given Transaction = tx
        tx.replaySafe("test")
        client.getStoragePool(PoolId.BootstrapPoolId).flatMap: pool =>
          pool.allocator.allocateDataObject(Array[Byte](i.toByte)).map(_ => ())
    for
      // Tree roots and the pool object; the FSIF pool adds its index bitmap in the metadata group
      _ <- client.createNewStoragePool("coverage-rocks", testIda, None, RocksDBConfig(),
                                       StorageDeviceSetId.BootstrapStorageDeviceSetId, 0L)
      _ <- client.createNewStoragePool("coverage-fsif", testIda, None, FsifConfig.forObjectSize(testIda, 4096, 3000),
                                       StorageDeviceSetId.BootstrapStorageDeviceSetId, 0L)
      // Enough allocations to split the bootstrap pool's AllocationTree, whose new nodes are allocations too
      _ <- Future.sequence((0 until 200).map(allocate))
      _ <- waitForTransactionsToComplete()
      missing <- unlisted()
    yield
      net.metadataStores.head.m.size should be > 0
      missing should be(Nil)
