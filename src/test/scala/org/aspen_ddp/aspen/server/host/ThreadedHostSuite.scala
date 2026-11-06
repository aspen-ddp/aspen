package org.aspen_ddp.aspen.server.host

import org.aspen_ddp.aspen.IntegrationTestSuite
import org.aspen_ddp.aspen.client.Transaction
import org.aspen_ddp.aspen.common.{DataBuffer, Radicle}
import org.aspen_ddp.aspen.common.objects.{DataObjectPointer, Insert, Key, ObjectRevision}
import org.aspen_ddp.aspen.common.transaction.KeyValueUpdate

import scala.concurrent.{ExecutionContext, Future}

/** The bootstrap host of the TestNetwork runs on a StoreThreadPool: its stores run on their own store threads, and
 *  its control work on its control thread, rather than in lockstep with the test. Assertions are on outcomes only. */
class ThreadedHostSuite extends IntegrationTestSuite:

  override def testThreading: ExecutionContext => HostThreading = _ => new StoreThreadPool(maxThreads = 3)

  override def subFixtureTeardown(): Unit = net.smgr.threading.shutdown()

  private val Objects = 20
  private val WritesPerObject = 10

  private def content(obj: Int, write: Int): DataBuffer = DataBuffer(Array[Byte](obj.toByte, write.toByte))

  atest("concurrent transactions commit on store threads"):
    given ExecutionContext = executionContext

    def allocate(): Future[List[DataObjectPointer]] =
      given tx: Transaction = client.newTransaction()
      val key = Key(Array[Byte](15))
      tx.update(radicle, None, None, List(KeyValueUpdate.DoesNotExist(key)), List(Insert(key, Array[Byte](1))))
      for
        pool <- client.getStoragePool(Radicle.poolId)
        ptrs <- Future.sequence((0 until Objects).toList.map(n => pool.allocator.allocateDataObject(content(n, 0))))
        _ <- tx.commit()
      yield ptrs

    // Each object's writes follow one another, each requiring the revision the one before wrote, so they never
    // collide; the 20 objects' chains run concurrently, so up to 20 of the 200 transactions are in flight at once.
    def overwrites(obj: Int, ptr: DataObjectPointer): Future[Unit] =
      (1 to WritesPerObject).foldLeft(client.read(ptr).map(_.revision)): (frev, write) =>
        frev.flatMap: rev =>
          val tx = client.newTransaction()
          tx.overwrite(ptr, rev, content(obj, write))
          tx.commit().map(_ => ObjectRevision(tx.id))
      .map(_ => ())

    for
      ptrs <- allocate()
      _ <- Future.traverse(ptrs.zipWithIndex)((ptr, obj) => overwrites(obj, ptr))
      states <- Future.traverse(ptrs)(ptr => client.read(ptr))
    yield
      // Every store was assigned a store thread: six stores spread over the pool's three
      net.smgr.threading.asInstanceOf[StoreThreadPool].threadCount should be(3)
      states.map(_.data.getByteArray.toList) should be(
        (0 until Objects).toList.map(obj => content(obj, WritesPerObject).getByteArray.toList))
