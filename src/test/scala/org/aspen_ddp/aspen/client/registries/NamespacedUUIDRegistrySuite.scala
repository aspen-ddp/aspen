package org.aspen_ddp.aspen.client.registries

import org.aspen_ddp.aspen.IntegrationTestSuite
import org.aspen_ddp.aspen.common.Radicle
import org.aspen_ddp.aspen.common.objects.{ByteArrayKeyOrdering, Key}
import org.aspen_ddp.aspen.client.tkvl.{KVObjectRootManager, SinglePoolNodeAllocator}

import java.util.UUID
import scala.concurrent.Future

class NamespacedUUIDRegistrySuite extends IntegrationTestSuite:

  val registryTreeKey = Key(Array[Byte](100))

  def createRegistry(): Future[NamespacedUUIDRegistry] =
    for
      ikvos <- client.read(radicle)
      pool <- client.getStoragePool(Radicle.poolId)
      alloc = pool.allocator
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
      NamespacedUUIDRegistry(client, ptr, registryTreeKey)

  test("makeKey produces correct format"):
    NamespacedUUIDRegistry.makeKey("myns", "myobj") should be (Key("myns.myobj"))

  atest("Register and retrieve by namespace and name"):
    for
      registry <- createRegistry()
      objectId = UUID.randomUUID()

      _ <- registry.registerObject("storage", "pool-config", objectId)
      retrieved <- registry.getRegisteredObject("storage", "pool-config")
    yield
      retrieved should be (objectId)

  atest("Duplicate namespace+name with different UUID fails"):
    for
      registry <- createRegistry()
      objectId1 = UUID.randomUUID()
      objectId2 = UUID.randomUUID()

      _ <- registry.registerObject("storage", "pool-config", objectId1)
      result <- registry.registerObject("storage", "pool-config", objectId2).failed
    yield
      result shouldBe a [Registry.DuplicateRegistration]
