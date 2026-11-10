package org.aspen_ddp.aspen.client.registries

import org.aspen_ddp.aspen.client.TestTransactions

import org.aspen_ddp.aspen.{IntegrationTestSuite, PinAudit}
import org.aspen_ddp.aspen.common.Radicle
import org.aspen_ddp.aspen.common.objects.{ByteArrayKeyOrdering, Insert, Key, Value}
import org.aspen_ddp.aspen.common.transaction.KeyValueUpdate
import org.aspen_ddp.aspen.client.tkvl.{KVObjectRootManager, SinglePoolNodeAllocator}

import java.util.UUID
import scala.concurrent.Future

class UUIDObjectRegistrySuite extends IntegrationTestSuite:

  val registryTreeKey = Key(Array[Byte](100))

  def createRegistry(): Future[UUIDObjectRegistry] =
    for
      ikvos <- client.read(radicle)
      pool <- client.getStoragePool(Radicle.poolId)
      alloc = pool.allocator
      tx0 = TestTransactions.replaySafe(client.newTransaction())
      ptr <- alloc.allocateKeyValueObject()(using tx0)
      _ = tx0.lockRevision(radicle, ikvos.revision)
      _ <- tx0.commit()
      _ <- waitForTransactionsToComplete()
      nodeAllocator = SinglePoolNodeAllocator(client, Radicle.poolId)
      tx1 = TestTransactions.replaySafe(client.newTransaction())
      _ <- KVObjectRootManager.createNewTree(client, ptr, registryTreeKey, ByteArrayKeyOrdering, nodeAllocator, Map())(using tx1)
      _ <- tx1.commit()
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
      alloc = pool.allocator

      tx1 = client.newTransaction()
      ptr1 <- alloc.allocateKeyValueObject()(using tx1)
      _ = tx1.update(radicle, Some(ikvos.revision), None, List(KeyValueUpdate.DoesNotExist(Key(Array[Byte](90)))), List(Insert(Key(Array[Byte](90)), Array[Byte](1))))
      _ <- tx1.commit()
      _ <- waitForTransactionsToComplete()

      _ <- registry.registerObject(objectId, ptr1)

      ikvos2 <- client.read(radicle)
      tx2 = client.newTransaction()
      ptr2 <- alloc.allocateKeyValueObject()(using tx2)
      _ = tx2.update(radicle, Some(ikvos2.revision), None, List(KeyValueUpdate.DoesNotExist(Key(Array[Byte](89)))), List(Insert(Key(Array[Byte](89)), Array[Byte](1))))
      _ <- tx2.commit()
      _ <- waitForTransactionsToComplete()

      result <- registry.registerObject(objectId, ptr2).failed
    yield
      result shouldBe a [Registry.DuplicateRegistration]

  atest("registerObject pins the node it inserts into"):
    val objectId = UUID.randomUUID()
    for
      registry <- createRegistry()
      _ <- registry.registerObject(UUID.randomUUID(), radicle)
      // register retries every failure, so an unpinned transaction is caught by the assertion, not enforced
      audit = new PinAudit(client)
      _ <- registry.registerObject(objectId, radicle)
      _ = audit.stop()
      retrieved <- registry.getRegisteredObject(objectId)
    yield
      retrieved should be (radicle)
      audit.unpinned shouldBe empty
      audit.built.flatMap(_.description.requirements).exists:
        case kvu: org.aspen_ddp.aspen.common.transaction.KeyValueUpdate =>
          kvu.requiredRevision.isDefined && kvu.requirements.exists:
            case org.aspen_ddp.aspen.common.transaction.KeyValueUpdate.DoesNotExist(k) => k == Key(objectId)
            case _ => false
        case _ => false
      .shouldBe(true)
