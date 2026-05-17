package org.aspen_ddp.aspen.client.registries

import org.aspen_ddp.aspen.IntegrationTestSuite
import org.aspen_ddp.aspen.client.Transaction
import org.aspen_ddp.aspen.common.Radicle
import org.aspen_ddp.aspen.common.ida.Replication
import org.aspen_ddp.aspen.common.objects.{ByteArrayKeyOrdering, Insert, Key, KeyValueObjectPointer, ObjectRevisionGuard, Value}
import org.aspen_ddp.aspen.common.transaction.KeyValueUpdate
import org.aspen_ddp.aspen.client.tkvl.{KVObjectRootManager, SinglePoolNodeAllocator}

import java.util.UUID
import scala.concurrent.Future

class UUIDObjectRegistrySuite extends IntegrationTestSuite:

  val registryTreeKey = Key(Array[Byte](100))

  def createRegistry(): Future[UUIDObjectRegistry] =
    given tx: Transaction = client.newTransaction()
    for
      ikvos <- client.read(radicle)
      pool <- client.getStoragePool(Radicle.poolId)
      alloc = pool.createAllocator(Replication(3, 2))
      ptr <- alloc.allocateKeyValueObject(ObjectRevisionGuard(radicle, ikvos.revision), Map(), None, None, None)
      nodeAllocator = SinglePoolNodeAllocator(client, Radicle.poolId)
      _ <- KVObjectRootManager.createNewTree(client, ptr, registryTreeKey, ByteArrayKeyOrdering, nodeAllocator, Map())
      _ <- tx.commit()
      _ <- waitForTransactionsToComplete()
    yield
      UUIDObjectRegistry(client, ptr, registryTreeKey)

  atest("Register and retrieve by UUID"):
    for
      registry <- createRegistry()
      objectId = UUID.randomUUID()
      ptr = radicle

      _ <- registry.registerObject(objectId, ptr)
      retrieved <- registry.getRegisteredObject(objectId)
    yield
      retrieved should be (ptr)

  atest("Duplicate UUID with different pointer fails"):
    for
      registry <- createRegistry()
      objectId = UUID.randomUUID()

      ikvos <- client.read(radicle)
      pool <- client.getStoragePool(Radicle.poolId)
      alloc = pool.createAllocator(Replication(3, 2))

      tx1 = client.newTransaction()
      ptr1 <- alloc.allocateKeyValueObject(ObjectRevisionGuard(radicle, ikvos.revision), Map())(using tx1)
      _ = tx1.update(radicle, Some(ikvos.revision), None, List(KeyValueUpdate.DoesNotExist(Key(Array[Byte](90)))), List(Insert(Key(Array[Byte](90)), Array[Byte](1))))
      _ <- tx1.commit()
      _ <- waitForTransactionsToComplete()

      _ <- registry.registerObject(objectId, ptr1)

      ikvos2 <- client.read(radicle)
      tx2 = client.newTransaction()
      ptr2 <- alloc.allocateKeyValueObject(ObjectRevisionGuard(radicle, ikvos2.revision), Map())(using tx2)
      _ = tx2.update(radicle, Some(ikvos2.revision), None, List(KeyValueUpdate.DoesNotExist(Key(Array[Byte](89)))), List(Insert(Key(Array[Byte](89)), Array[Byte](1))))
      _ <- tx2.commit()
      _ <- waitForTransactionsToComplete()

      result <- registry.registerObject(objectId, ptr2).failed
    yield
      result shouldBe a [ObjectRegistry.DuplicateRegistration]
