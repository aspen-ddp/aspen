package org.aspen_ddp.aspen.client.registries

import org.aspen_ddp.aspen.IntegrationTestSuite
import org.aspen_ddp.aspen.client.Transaction
import org.aspen_ddp.aspen.common.Radicle
import org.aspen_ddp.aspen.common.ida.Replication
import org.aspen_ddp.aspen.common.objects.{ByteArrayKeyOrdering, Key, KeyValueObjectPointer, ObjectRevisionGuard, Value}
import org.aspen_ddp.aspen.client.tkvl.{KVObjectRootManager, SinglePoolNodeAllocator}

import java.util.UUID
import scala.concurrent.Future

class NamespacedUUIDRegistrySuite extends IntegrationTestSuite:

  val registryTreeKey = Key(Array[Byte](100))

  def createRegistry(): Future[NamespacedUUIDRegistry] =
    given tx: Transaction = client.newTransaction()
    for
      ikvos <- client.read(radicle)
      pool <- client.getStoragePool(Radicle.poolId)
      alloc = pool.createAllocator
      ptr <- alloc.allocateKeyValueObject(ObjectRevisionGuard(radicle, ikvos.revision), Map(), None, None, None)
      nodeAllocator = SinglePoolNodeAllocator(client, Radicle.poolId)
      _ <- KVObjectRootManager.createNewTree(client, ptr, registryTreeKey, ByteArrayKeyOrdering, nodeAllocator, Map())
      _ <- tx.commit()
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
