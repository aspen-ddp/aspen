package org.aspen_ddp.aspen.server.repair

import org.aspen_ddp.aspen.IntegrationTestSuite
import org.aspen_ddp.aspen.client.Transaction
import org.aspen_ddp.aspen.client.internal.allocation.PoolObjectAllocator
import org.aspen_ddp.aspen.common.DataBuffer
import org.aspen_ddp.aspen.common.objects.{Insert, Key, KeyValueObjectPointer, ObjectRefcount, Value}
import org.aspen_ddp.aspen.common.pool.PoolId

import scala.concurrent.Future

class RepairProofsSuite extends IntegrationTestSuite:

  private val k0 = Key(Array[Byte](0))
  private val k1 = Key(Array[Byte](1))

  private def kvObject(): Future[KeyValueObjectPointer] =
    for
      ptr <- client.transactUntilSuccessful: tx =>
               given Transaction = tx
               for
                 pool <- client.getStoragePool(PoolId.BootstrapPoolId)
                 p <- new PoolObjectAllocator(client, pool).allocateKeyValueObject(Map(k0 -> Value(Array[Byte](0))))
               yield p
      _ <- net.waitForTransactionsToComplete()
    yield ptr

  private def insert(ptr: KeyValueObjectPointer, key: Key): Future[Unit] =
    for
      _ <- client.transactUntilSuccessful: tx =>
             tx.update(ptr, None, None, Nil, List(Insert(key, Array[Byte](9))))
             Future.unit
      _ <- net.waitForTransactionsToComplete()
    yield ()

  test("a key proof commits when the key is still at the revision the read saw"):
    for
      ptr <- kvObject()
      read <- client.read(ptr)
      proven <- RepairProofs.proveKey(client, ptr, k0, Some(read.contents(k0).revision))
      _ <- net.waitForTransactionsToComplete()
      after <- client.read(ptr)
    yield
      proven shouldBe true
      after.contents.keySet shouldBe Set(k0)
      after.contents(k0).revision shouldBe read.contents(k0).revision

  test("a key proof aborts when the key changed since the read"):
    for
      ptr <- kvObject()
      read <- client.read(ptr)
      _ <- insert(ptr, k0)
      proven <- RepairProofs.proveKey(client, ptr, k0, Some(read.contents(k0).revision))
    yield
      proven shouldBe false

  test("a key proof of an absent key commits while it is absent and aborts once it exists"):
    for
      ptr <- kvObject()
      absent <- RepairProofs.proveKey(client, ptr, k1, None)
      _ <- net.waitForTransactionsToComplete()
      _ <- insert(ptr, k1)
      present <- RepairProofs.proveKey(client, ptr, k1, None)
    yield
      absent shouldBe true
      present shouldBe false

  test("an object-level proof commits on an unchanged object and aborts after a change"):
    for
      ptr <- kvObject()
      read <- client.read(ptr)
      unchanged <- RepairProofs.proveObjectLevel(client, read)
      _ <- net.waitForTransactionsToComplete()
      fresh <- client.read(ptr)
      _ <- insert(ptr, k1)
      changed <- RepairProofs.proveObjectLevel(client, fresh)
    yield
      unchanged shouldBe true
      changed shouldBe false

  test("a refcount proof commits without changing the refcount and aborts on a stale refcount"):
    for
      ptr <- client.transactUntilSuccessful: tx =>
               given Transaction = tx
               for
                 pool <- client.getStoragePool(PoolId.BootstrapPoolId)
                 p <- new PoolObjectAllocator(client, pool).allocateDataObject(DataBuffer(Array[Byte](1)))
               yield p
      _ <- net.waitForTransactionsToComplete()
      read <- client.read(ptr)
      proven <- RepairProofs.proveRefcount(client, ptr, read.refcount)
      _ <- net.waitForTransactionsToComplete()
      after <- client.read(ptr)
      stale <- RepairProofs.proveRefcount(client, ptr, ObjectRefcount(read.refcount.updateSerial - 1,
                                                                       read.refcount.count))
    yield
      proven shouldBe true
      after.refcount shouldBe read.refcount
      after.revision shouldBe read.revision
      stale shouldBe false

  private def errorKey(i: Int, objectId: org.aspen_ddp.aspen.common.objects.ObjectId): Key =
    val bb = java.nio.ByteBuffer.allocate(17)
    bb.put(i.toByte).putLong(objectId.uuid.getMostSignificantBits).putLong(objectId.uuid.getLeastSignificantBits)
    Key(bb.array())

  /** Whether the pool's errorTree marks store `i` for `objectId` */
  private def marked(i: Int, objectId: org.aspen_ddp.aspen.common.objects.ObjectId): Future[Boolean] =
    client.getStoragePool(PoolId.BootstrapPoolId).flatMap(_.errorTree.get(errorKey(i, objectId))).map(_.isDefined)

  /** Deletes store `i`'s errorTree entry for `objectId`, if any: the test network's stores can miss the allocation */
  private def unmark(i: Int, objectId: org.aspen_ddp.aspen.common.objects.ObjectId): Future[Unit] =
    for
      pool <- client.getStoragePool(PoolId.BootstrapPoolId)
      present <- pool.errorTree.get(errorKey(i, objectId))
      _ <- if present.isEmpty then Future.unit
           else
             val tx = client.newRepairTransaction()
             pool.errorTree.delete(errorKey(i, objectId))(using tx).flatMap(_ => tx.commit()).map(_ => ())
      _ <- net.waitForTransactionsToComplete()
    yield ()

  test("an object-level proof missed by a store marks it: the proof moves the object's revision"):
    val s2 = net.storeId2
    for
      ptr <- kvObject()
      read <- client.read(ptr)
      _ <- unmark(2, ptr.id)
      markedBefore <- marked(2, ptr.id)
      _ = markedBefore shouldBe false
      _ = net.offlineStores = Set(s2)
      _ = net.dropTxMessage = m => m.to == s2
      proven <- RepairProofs.proveObjectLevel(client, read)
      _ <- net.waitForTransactionsToComplete()
      _ = net.dropTxMessage = _ => false
      _ = net.offlineStores = Set()
      after <- client.read(ptr)
      isMarked <- marked(2, ptr.id)
    yield
      proven shouldBe true
      after.revision should not be read.revision
      isMarked shouldBe true

  test("proof transactions that change nothing disable missed-update tracking"):
    Future.successful:
      RepairProofs.newProofTransaction(client).missedUpdateTrackingEnabled shouldBe false
