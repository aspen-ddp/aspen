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
