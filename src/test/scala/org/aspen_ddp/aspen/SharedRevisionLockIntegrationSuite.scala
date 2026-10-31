package org.aspen_ddp.aspen

import org.aspen_ddp.aspen.client.{Transaction, TransactionAborted}
import org.aspen_ddp.aspen.common.{DataBuffer, Radicle}
import org.aspen_ddp.aspen.common.objects.{DataObjectPointer, Insert, Key}
import org.aspen_ddp.aspen.common.transaction.KeyValueUpdate

import scala.concurrent.Future
import scala.language.implicitConversions

class SharedRevisionLockIntegrationSuite extends IntegrationTestSuite:

  /** Allocates a DataObject holding `content`, linked from the radicle under a key derived from `index` */
  private def allocate(index: Int, content: Array[Byte]): Future[DataObjectPointer] =
    given tx: Transaction = client.newTransaction()
    val radicleKey = Key(Array[Byte](0x53, index.toByte))
    tx.update(radicle, None, None, List(KeyValueUpdate.DoesNotExist(radicleKey)),
      List(Insert(radicleKey, Array[Byte](1))))
    for
      pool <- client.getStoragePool(Radicle.poolId)
      dp <- pool.allocator.allocateDataObject(DataBuffer(content))
      _ <- tx.commit()
    yield dp

  atest("concurrent transactions sharing a lock all commit"):
    for
      fence <- allocate(0, Array[Byte](0))
      objects <- Future.sequence((1 to 8).map(i => allocate(i, Array[Byte](0))))
      fenceState <- client.read(fence)
      states <- Future.sequence(objects.map(client.read(_)))
      // Every transaction is started before any completes, so the shared locks are held concurrently
      _ <- Future.sequence(states.zipWithIndex.map { (state, i) =>
        val tx = client.newTransaction()
        tx.sharedLockRevision(fence, fenceState.revision)
        tx.overwrite(state.pointer, state.revision, DataBuffer(Array[Byte](i.toByte)))
        tx.commit()
      })
      after <- Future.sequence(objects.map(client.read(_)))
      fenceAfter <- client.read(fence)
    yield
      after.map(_.data.getByteArray.toList) should be ((0 until 8).map(i => List(i.toByte)))
      fenceAfter.revision should be (fenceState.revision)

  atest("an overwrite of the locked object makes later shared lockers fail"):
    for
      fence <- allocate(0, Array[Byte](0))
      obj <- allocate(1, Array[Byte](0))
      oldFence <- client.read(fence)
      objState <- client.read(obj)
      _ <-
        val tx = client.newTransaction()
        tx.overwrite(fence, oldFence.revision, DataBuffer(Array[Byte](1)))
        tx.commit()
      result <-
        val tx = client.newTransaction()
        tx.sharedLockRevision(fence, oldFence.revision)
        tx.overwrite(obj, objState.revision, DataBuffer(Array[Byte](2)))
        tx.commit().map(_ => None).recover { case e: TransactionAborted => Some(e) }
      objAfter <- client.read(obj)
    yield
      result should not be (None)
      objAfter.revision should be (objState.revision)
