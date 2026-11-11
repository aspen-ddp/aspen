package org.aspen_ddp.aspen.client.registries

import org.aspen_ddp.aspen.client.TestTransactions

import org.aspen_ddp.aspen.{IntegrationTestSuite, PinAudit}
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
      tx0 = TestTransactions.replaySafe(client.newTransaction())
      ptr <- alloc.allocateKeyValueObject()(using tx0)
      _ = tx0.exclusiveLockRevision(radicle, ikvos.revision)
      _ <- tx0.commit()
      _ <- waitForTransactionsToComplete()
      nodeAllocator = SinglePoolNodeAllocator(client, Radicle.poolId)
      tx1 = TestTransactions.replaySafe(client.newTransaction())
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

  atest("getAllEntries returns only matching namespace, names stripped, sorted"):
    for
      registry <- createRegistry()
      poolA = UUID.randomUUID()
      poolB = UUID.randomUUID()
      host1 = UUID.randomUUID()

      // Register out of alphabetical order to prove sorting is by name.
      _ <- registry.registerObject("pool", "zebra", poolB)
      _ <- registry.registerObject("pool", "alpha", poolA)
      _ <- registry.registerObject("host", "node-a", host1)

      pools <- registry.getAllEntries("pool")
      hosts <- registry.getAllEntries("host")
    yield
      pools should be (List("alpha" -> poolA, "zebra" -> poolB))
      hosts should be (List("node-a" -> host1))

  atest("getAllEntries returns empty list for unused namespace"):
    for
      registry <- createRegistry()
      _ <- registry.registerObject("pool", "alpha", UUID.randomUUID())
      groups <- registry.getAllEntries("group")
    yield
      groups should be (Nil)

  atest("getAllEntries does not match a namespace that is a name substring"):
    for
      registry <- createRegistry()
      poolId = UUID.randomUUID()
      // A name in a different namespace whose text contains "pool" must not leak in.
      _ <- registry.registerObject("host", "pool-host", UUID.randomUUID())
      _ <- registry.registerObject("pool", "real", poolId)
      pools <- registry.getAllEntries("pool")
    yield
      pools should be (List("real" -> poolId))

  atest("registerObject pins the node it inserts into"):
    val objectId = UUID.randomUUID()
    for
      registry <- createRegistry()
      _ <- registry.registerObject("storage", "first", UUID.randomUUID())
      // register retries every failure, so an unpinned transaction is caught by the assertion, not enforced
      audit = new PinAudit(client)
      _ <- registry.registerObject("storage", "second", objectId)
      _ = audit.stop()
      retrieved <- registry.getRegisteredObject("storage", "second")
    yield
      retrieved should be (objectId)
      audit.unpinned shouldBe empty
      audit.built.flatMap(_.description.requirements).exists:
        case kvu: org.aspen_ddp.aspen.common.transaction.KeyValueUpdate =>
          kvu.requiredRevision.isDefined && kvu.requirements.exists:
            case org.aspen_ddp.aspen.common.transaction.KeyValueUpdate.DoesNotExist(k) => k == NamespacedUUIDRegistry.makeKey("storage", "second")
            case _ => false
        case _ => false
      .shouldBe(true)
