package org.aspen_ddp.aspen.client.registries

import org.aspen_ddp.aspen.IntegrationTestSuite
import org.aspen_ddp.aspen.client.Transaction
import org.aspen_ddp.aspen.common.{DataBuffer, Radicle}
import org.aspen_ddp.aspen.common.ida.Replication
import org.aspen_ddp.aspen.common.objects.{ByteArrayKeyOrdering, Insert, Key, KeyAlreadyExists, KeyValueObjectPointer, ObjectRevisionGuard, Value}
import org.aspen_ddp.aspen.common.transaction.KeyValueUpdate
import org.aspen_ddp.aspen.client.tkvl.{KVObjectRootManager, SinglePoolNodeAllocator}

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
      result <- registry.getRegisteredObject(Key("nonexistent")).failed
    yield
      result shouldBe a [NoSuchElementException]

  atest("prepareRegisterObject and retrieve"):
    for
      registry <- createRegistry()
      key = Key("test-object-1")

      ikvos <- client.read(radicle)
      pool <- client.getStoragePool(Radicle.poolId)
      alloc = pool.createAllocator(Replication(3, 2))

      tx = client.newTransaction()
      ptr <- alloc.allocateKeyValueObject(ObjectRevisionGuard(radicle, ikvos.revision), Map())(using tx)
      _ <- registry.prepareRegisterObject(key, ptr)(using tx)
      _ <- tx.commit()
      _ <- waitForTransactionsToComplete()

      retrieved <- registry.getRegisteredObject(key)
    yield
      retrieved should be (ptr)

  atest("prepareRegisterObject detects duplicate key"):
    for
      registry <- createRegistry()
      key = Key("test-object-2")

      ikvos <- client.read(radicle)
      pool <- client.getStoragePool(Radicle.poolId)
      alloc = pool.createAllocator(Replication(3, 2))

      tx1 = client.newTransaction()
      ptr1 <- alloc.allocateKeyValueObject(ObjectRevisionGuard(radicle, ikvos.revision), Map())(using tx1)
      _ <- registry.prepareRegisterObject(key, ptr1)(using tx1)
      _ <- tx1.commit()
      _ <- waitForTransactionsToComplete()

      ikvos2 <- client.read(radicle)
      tx2 = client.newTransaction()
      ptr2 <- alloc.allocateKeyValueObject(ObjectRevisionGuard(radicle, ikvos2.revision), Map())(using tx2)
      result <- registry.prepareRegisterObject(key, ptr2)(using tx2).failed
    yield
      result shouldBe a [KeyAlreadyExists]

  atest("registerObject and retrieve"):
    for
      registry <- createRegistry()
      key = Key("test-object-3")
      ptr = radicle

      _ <- registry.registerObject(key, ptr)
      retrieved <- registry.getRegisteredObject(key)
    yield
      retrieved should be (ptr)

  atest("registerObject is idempotent for same pointer"):
    for
      registry <- createRegistry()
      key = Key("test-object-4")

      ikvos <- client.read(radicle)
      pool <- client.getStoragePool(Radicle.poolId)
      alloc = pool.createAllocator(Replication(3, 2))

      tx = client.newTransaction()
      ptr <- alloc.allocateKeyValueObject(ObjectRevisionGuard(radicle, ikvos.revision), Map())(using tx)
      _ = tx.update(radicle, Some(ikvos.revision), None, List(KeyValueUpdate.DoesNotExist(Key(Array[Byte](99)))), List(Insert(Key(Array[Byte](99)), Array[Byte](1))))
      _ <- tx.commit()
      _ <- waitForTransactionsToComplete()

      _ <- registry.registerObject(key, ptr)
      _ <- registry.registerObject(key, ptr)

      retrieved <- registry.getRegisteredObject(key)
    yield
      retrieved should be (ptr)

  atest("registerObject fails with DuplicateRegistration for different pointer"):
    for
      registry <- createRegistry()
      key = Key("test-object-5")

      ikvos <- client.read(radicle)
      pool <- client.getStoragePool(Radicle.poolId)
      alloc = pool.createAllocator(Replication(3, 2))

      tx1 = client.newTransaction()
      ptr1 <- alloc.allocateKeyValueObject(ObjectRevisionGuard(radicle, ikvos.revision), Map())(using tx1)
      _ = tx1.update(radicle, Some(ikvos.revision), None, List(KeyValueUpdate.DoesNotExist(Key(Array[Byte](98)))), List(Insert(Key(Array[Byte](98)), Array[Byte](1))))
      _ <- tx1.commit()
      _ <- waitForTransactionsToComplete()

      _ <- registry.registerObject(key, ptr1)

      ikvos2 <- client.read(radicle)
      tx2 = client.newTransaction()
      ptr2 <- alloc.allocateKeyValueObject(ObjectRevisionGuard(radicle, ikvos2.revision), Map())(using tx2)
      _ = tx2.update(radicle, Some(ikvos2.revision), None, List(KeyValueUpdate.DoesNotExist(Key(Array[Byte](97)))), List(Insert(Key(Array[Byte](97)), Array[Byte](1))))
      _ <- tx2.commit()
      _ <- waitForTransactionsToComplete()

      result <- registry.registerObject(key, ptr2).failed
    yield
      result shouldBe a [ObjectRegistry.DuplicateRegistration]

  atest("getRegisteredKeyValueObject returns typed pointer"):
    for
      registry <- createRegistry()
      key = Key("test-object-6")

      ikvos <- client.read(radicle)
      pool <- client.getStoragePool(Radicle.poolId)
      alloc = pool.createAllocator(Replication(3, 2))

      tx = client.newTransaction()
      kvPtr <- alloc.allocateKeyValueObject(ObjectRevisionGuard(radicle, ikvos.revision), Map())(using tx)
      _ = tx.update(radicle, Some(ikvos.revision), None, List(KeyValueUpdate.DoesNotExist(Key(Array[Byte](96)))), List(Insert(Key(Array[Byte](96)), Array[Byte](1))))
      _ <- tx.commit()
      _ <- waitForTransactionsToComplete()

      _ <- registry.registerObject(key, kvPtr)
      retrieved <- registry.getRegisteredKeyValueObject(key)
    yield
      retrieved should be (kvPtr)

  atest("getRegisteredDataObject returns typed pointer"):
    for
      registry <- createRegistry()
      key = Key("test-object-7")

      ikvos <- client.read(radicle)
      pool <- client.getStoragePool(Radicle.poolId)
      alloc = pool.createAllocator(Replication(3, 2))

      tx = client.newTransaction()
      dataPtr <- alloc.allocateDataObject(ObjectRevisionGuard(radicle, ikvos.revision), DataBuffer(Array[Byte](1, 2, 3)))(using tx)
      _ = tx.update(radicle, Some(ikvos.revision), None, List(KeyValueUpdate.DoesNotExist(Key(Array[Byte](95)))), List(Insert(Key(Array[Byte](95)), Array[Byte](1))))
      _ <- tx.commit()
      _ <- waitForTransactionsToComplete()

      _ <- registry.registerObject(key, dataPtr)
      retrieved <- registry.getRegisteredDataObject(key)
    yield
      retrieved should be (dataPtr)
