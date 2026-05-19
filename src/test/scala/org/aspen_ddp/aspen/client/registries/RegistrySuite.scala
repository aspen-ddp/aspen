package org.aspen_ddp.aspen.client.registries

import org.aspen_ddp.aspen.IntegrationTestSuite
import org.aspen_ddp.aspen.common.Radicle
import org.aspen_ddp.aspen.common.objects.{ByteArrayKeyOrdering, Insert, Key, KeyAlreadyExists, Value}
import org.aspen_ddp.aspen.common.transaction.KeyValueUpdate
import org.aspen_ddp.aspen.client.tkvl.{KVObjectRootManager, SinglePoolNodeAllocator}

import scala.concurrent.Future

class RegistrySuite extends IntegrationTestSuite:

  val registryTreeKey = Key(Array[Byte](100))

  def createRegistry(): Future[Registry] =
    for
      ikvos <- client.read(radicle)
      pool <- client.getStoragePool(Radicle.poolId)
      alloc = pool.createAllocator
      tx0 = client.newTransaction()
      ptr <- alloc.allocateKeyValueObject()(using tx0)
      _ = tx0.lockRevision(radicle, ikvos.revision)
      _ <- tx0.commit()
      _ <- waitForTransactionsToComplete()
      nodeAllocator = SinglePoolNodeAllocator(client, Radicle.poolId)
      tx1 = client.newTransaction()
      _ <- KVObjectRootManager.createNewTree(client, ptr, registryTreeKey, ByteArrayKeyOrdering, nodeAllocator, Map())(using tx1)
      _ <- tx1.commit()
      _ <- waitForTransactionsToComplete()
    yield
      Registry(client, ptr, registryTreeKey)

  atest("Get non-existent value fails with NoSuchElementException"):
    for
      registry <- createRegistry()
      result <- registry.get(Key("nonexistent")).failed
    yield
      result shouldBe a [NoSuchElementException]

  atest("prepareRegister and retrieve"):
    for
      registry <- createRegistry()
      key = Key("test-value-1")

      pool <- client.getStoragePool(Radicle.poolId)
      alloc = pool.createAllocator

      tx = client.newTransaction()
      ptr <- alloc.allocateKeyValueObject()(using tx)
      value = Value(ptr.toArray)
      _ <- registry.prepareRegister(key, value)(using tx)
      _ <- tx.commit()
      _ <- waitForTransactionsToComplete()

      retrieved <- registry.get(key)
    yield
      java.util.Arrays.equals(retrieved.bytes, value.bytes) should be (true)

  atest("prepareRegister detects duplicate key"):
    for
      registry <- createRegistry()
      key = Key("test-value-2")

      pool <- client.getStoragePool(Radicle.poolId)
      alloc = pool.createAllocator

      tx1 = client.newTransaction()
      ptr1 <- alloc.allocateKeyValueObject()(using tx1)
      value1 = Value(ptr1.toArray)
      _ <- registry.prepareRegister(key, value1)(using tx1)
      _ <- tx1.commit()
      _ <- waitForTransactionsToComplete()

      tx2 = client.newTransaction()
      ptr2 <- alloc.allocateKeyValueObject()(using tx2)
      value2 = Value(ptr2.toArray)
      result <- registry.prepareRegister(key, value2)(using tx2).failed
    yield
      result shouldBe a [KeyAlreadyExists]

  atest("register and retrieve"):
    for
      registry <- createRegistry()
      key = Key("test-value-3")
      value = Value(radicle.toArray)

      _ <- registry.register(key, value)
      retrieved <- registry.get(key)
    yield
      java.util.Arrays.equals(retrieved.bytes, value.bytes) should be (true)

  atest("register is idempotent for same value"):
    for
      registry <- createRegistry()
      key = Key("test-value-4")

      ikvos <- client.read(radicle)
      pool <- client.getStoragePool(Radicle.poolId)
      alloc = pool.createAllocator

      tx = client.newTransaction()
      ptr <- alloc.allocateKeyValueObject()(using tx)
      _ = tx.update(radicle, Some(ikvos.revision), None, List(KeyValueUpdate.DoesNotExist(Key(Array[Byte](99)))), List(Insert(Key(Array[Byte](99)), Array[Byte](1))))
      _ <- tx.commit()
      _ <- waitForTransactionsToComplete()

      value = Value(ptr.toArray)
      _ <- registry.register(key, value)
      _ <- registry.register(key, value)

      retrieved <- registry.get(key)
    yield
      java.util.Arrays.equals(retrieved.bytes, value.bytes) should be (true)

  atest("register fails with DuplicateRegistration for different value"):
    for
      registry <- createRegistry()
      key = Key("test-value-5")

      ikvos <- client.read(radicle)
      pool <- client.getStoragePool(Radicle.poolId)
      alloc = pool.createAllocator

      tx1 = client.newTransaction()
      ptr1 <- alloc.allocateKeyValueObject()(using tx1)
      _ = tx1.update(radicle, Some(ikvos.revision), None, List(KeyValueUpdate.DoesNotExist(Key(Array[Byte](98)))), List(Insert(Key(Array[Byte](98)), Array[Byte](1))))
      _ <- tx1.commit()
      _ <- waitForTransactionsToComplete()

      value1 = Value(ptr1.toArray)
      _ <- registry.register(key, value1)

      ikvos2 <- client.read(radicle)
      tx2 = client.newTransaction()
      ptr2 <- alloc.allocateKeyValueObject()(using tx2)
      _ = tx2.update(radicle, Some(ikvos2.revision), None, List(KeyValueUpdate.DoesNotExist(Key(Array[Byte](97)))), List(Insert(Key(Array[Byte](97)), Array[Byte](1))))
      _ <- tx2.commit()
      _ <- waitForTransactionsToComplete()

      value2 = Value(ptr2.toArray)
      result <- registry.register(key, value2).failed
    yield
      result shouldBe a [Registry.DuplicateRegistration]
