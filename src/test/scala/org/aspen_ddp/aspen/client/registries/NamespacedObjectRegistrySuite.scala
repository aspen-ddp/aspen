package org.aspen_ddp.aspen.client.registries

import org.aspen_ddp.aspen.IntegrationTestSuite
import org.aspen_ddp.aspen.client.Transaction
import org.aspen_ddp.aspen.common.Radicle
import org.aspen_ddp.aspen.common.ida.Replication
import org.aspen_ddp.aspen.common.objects.{ByteArrayKeyOrdering, Insert, Key, KeyValueObjectPointer, ObjectRevisionGuard, Value}
import org.aspen_ddp.aspen.common.transaction.KeyValueUpdate
import org.aspen_ddp.aspen.client.tkvl.{KVObjectRootManager, SinglePoolNodeAllocator}

import scala.concurrent.Future

class NamespacedObjectRegistrySuite extends IntegrationTestSuite:

  val registryTreeKey = Key(Array[Byte](100))

  def createRegistry(): Future[NamespacedObjectRegistry] =
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
      NamespacedObjectRegistry(client, ptr, registryTreeKey)

  test("makeKey produces correct format"):
    NamespacedObjectRegistry.makeKey("myns", "myobj") should be (Key("myns.myobj"))

  atest("Register and retrieve by namespace and name"):
    for
      registry <- createRegistry()
      ptr = radicle

      _ <- registry.registerObject("storage", "pool-config", ptr)
      retrieved <- registry.getRegisteredObject("storage", "pool-config")
    yield
      retrieved should be (ptr)

  atest("Duplicate namespace+name with different pointer fails"):
    for
      registry <- createRegistry()

      ikvos <- client.read(radicle)
      pool <- client.getStoragePool(Radicle.poolId)
      alloc = pool.createAllocator(Replication(3, 2))

      tx1 = client.newTransaction()
      ptr1 <- alloc.allocateKeyValueObject(ObjectRevisionGuard(radicle, ikvos.revision), Map())(using tx1)
      _ = tx1.update(radicle, Some(ikvos.revision), None, List(KeyValueUpdate.DoesNotExist(Key(Array[Byte](80)))), List(Insert(Key(Array[Byte](80)), Array[Byte](1))))
      _ <- tx1.commit()
      _ <- waitForTransactionsToComplete()

      _ <- registry.registerObject("storage", "pool-config", ptr1)

      ikvos2 <- client.read(radicle)
      tx2 = client.newTransaction()
      ptr2 <- alloc.allocateKeyValueObject(ObjectRevisionGuard(radicle, ikvos2.revision), Map())(using tx2)
      _ = tx2.update(radicle, Some(ikvos2.revision), None, List(KeyValueUpdate.DoesNotExist(Key(Array[Byte](79)))), List(Insert(Key(Array[Byte](79)), Array[Byte](1))))
      _ <- tx2.commit()
      _ <- waitForTransactionsToComplete()

      result <- registry.registerObject("storage", "pool-config", ptr2).failed
    yield
      result shouldBe a [ObjectRegistry.DuplicateRegistration]
