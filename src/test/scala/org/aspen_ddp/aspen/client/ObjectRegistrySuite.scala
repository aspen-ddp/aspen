package org.aspen_ddp.aspen.client

import org.aspen_ddp.aspen.IntegrationTestSuite
import org.aspen_ddp.aspen.common.{DataBuffer, Radicle}
import org.aspen_ddp.aspen.common.ida.Replication
import org.aspen_ddp.aspen.common.objects.{ByteArrayKeyOrdering, Key, KeyAlreadyExists, KeyValueObjectPointer, ObjectPointer, ObjectRevisionGuard, Value}
import org.aspen_ddp.aspen.client.tkvl.{KVObjectRootManager, SinglePoolNodeAllocator}

import java.util.UUID
import scala.concurrent.Future

class ObjectRegistrySuite extends IntegrationTestSuite:

  val registryTreeKey = Key(Array[Byte](100))

  def createRegistry(): Future[ObjectRegistry] =
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
      ObjectRegistry(client, ptr, registryTreeKey)

  atest("Get non-existent object fails with NoSuchElementException"):
    for
      registry <- createRegistry()
      result <- registry.getRegisteredObject(UUID.randomUUID()).failed
    yield
      result shouldBe a [NoSuchElementException]

  atest("prepareRegisterObject and retrieve"):
    for
      registry <- createRegistry()
      objectId = UUID.randomUUID()

      ikvos <- client.read(radicle)
      pool <- client.getStoragePool(Radicle.poolId)
      alloc = pool.createAllocator(Replication(3, 2))

      tx = client.newTransaction()
      ptr <- alloc.allocateKeyValueObject(ObjectRevisionGuard(radicle, ikvos.revision), Map())(using tx)
      _ <- registry.prepareRegisterObject(objectId, ptr)(using tx)
      _ <- tx.commit()
      _ <- waitForTransactionsToComplete()

      retrieved <- registry.getRegisteredObject(objectId)
    yield
      retrieved should be (ptr)

  atest("prepareRegisterObject detects duplicate key"):
    for
      registry <- createRegistry()
      objectId = UUID.randomUUID()

      ikvos <- client.read(radicle)
      pool <- client.getStoragePool(Radicle.poolId)
      alloc = pool.createAllocator(Replication(3, 2))

      tx1 = client.newTransaction()
      ptr1 <- alloc.allocateKeyValueObject(ObjectRevisionGuard(radicle, ikvos.revision), Map())(using tx1)
      _ <- registry.prepareRegisterObject(objectId, ptr1)(using tx1)
      _ <- tx1.commit()
      _ <- waitForTransactionsToComplete()

      ikvos2 <- client.read(radicle)
      tx2 = client.newTransaction()
      ptr2 <- alloc.allocateKeyValueObject(ObjectRevisionGuard(radicle, ikvos2.revision), Map())(using tx2)
      result <- registry.prepareRegisterObject(objectId, ptr2)(using tx2).failed
    yield
      result shouldBe a [KeyAlreadyExists]

  atest("registerObject and retrieve"):
    val objectId = UUID.randomUUID()
    for
      registry <- createRegistry()

      // Use the radicle object (which already exists) for testing
      ptr = radicle

      _ <- registry.registerObject(objectId, ptr)

      retrieved <- registry.getRegisteredObject(objectId)
    yield
      retrieved should be (ptr)
