# Generalized ObjectRegistry Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Generalize ObjectRegistry to use Key instead of UUID, move to `client.registries` package, and add UUIDObjectRegistry and NamespacedObjectRegistry wrappers.

**Architecture:** Move ObjectRegistry into `client.registries` and change its API from UUID to Key. Add two thin wrapper classes that convert their key types (UUID, namespace+name) to Key before forwarding. Delete the old UUID-specific ObjectRegistry.

**Tech Stack:** Scala 3, ScalaTest (AsyncFunSuite via IntegrationTestSuite), sbt

**Spec:** `docs/superpowers/specs/2026-05-17-object-registry-design.md`

---

## File Structure

| Action | File | Responsibility |
|--------|------|---------------|
| Create | `src/main/scala/org/aspen_ddp/aspen/client/registries/ObjectRegistry.scala` | Generic Key-based registry |
| Create | `src/main/scala/org/aspen_ddp/aspen/client/registries/UUIDObjectRegistry.scala` | UUID → Key forwarding wrapper |
| Create | `src/main/scala/org/aspen_ddp/aspen/client/registries/NamespacedObjectRegistry.scala` | namespace+name → Key forwarding wrapper |
| Create | `src/test/scala/org/aspen_ddp/aspen/client/registries/ObjectRegistrySuite.scala` | Comprehensive tests for generic registry |
| Create | `src/test/scala/org/aspen_ddp/aspen/client/registries/UUIDObjectRegistrySuite.scala` | Basic wrapper tests |
| Create | `src/test/scala/org/aspen_ddp/aspen/client/registries/NamespacedObjectRegistrySuite.scala` | Basic wrapper tests |
| Delete | `src/main/scala/org/aspen_ddp/aspen/client/ObjectRegistry.scala` | Replaced by registries version |
| Delete | `src/test/scala/org/aspen_ddp/aspen/client/ObjectRegistrySuite.scala` | Replaced by registries version |

---

### Task 1: Create generic Key-based ObjectRegistry and tests

**Files:**
- Create: `src/main/scala/org/aspen_ddp/aspen/client/registries/ObjectRegistry.scala`
- Create: `src/test/scala/org/aspen_ddp/aspen/client/registries/ObjectRegistrySuite.scala`

- [ ] **Step 1: Write the test file**

Create `src/test/scala/org/aspen_ddp/aspen/client/registries/ObjectRegistrySuite.scala`:

```scala
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
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `sbt 'testOnly *registries.ObjectRegistrySuite'`
Expected: Compilation failure — `ObjectRegistry` class does not exist in `client.registries` package yet.

- [ ] **Step 3: Create the ObjectRegistry implementation**

Create `src/main/scala/org/aspen_ddp/aspen/client/registries/ObjectRegistry.scala`:

```scala
package org.aspen_ddp.aspen.client.registries

import org.aspen_ddp.aspen.client.{AspenClient, StopRetrying, Transaction}
import org.aspen_ddp.aspen.client.tkvl.{KVObjectRootManager, TieredKeyValueList}
import org.aspen_ddp.aspen.common.objects.{DataObjectPointer, Key, KeyAlreadyExists, KeyValueObjectPointer, ObjectPointer, Value}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Failure

object ObjectRegistry:
  class DuplicateRegistration(val key: Key, val existing: ObjectPointer)
    extends Exception(s"Duplicate registration for key $key")

class ObjectRegistry(val client: AspenClient,
                     containingObjectPtr: KeyValueObjectPointer,
                     treeRootKey: Key):

  given ExecutionContext = client.clientContext

  private val tkvl = TieredKeyValueList(client, KVObjectRootManager(client, treeRootKey, containingObjectPtr))

  def getRegisteredObject(key: Key): Future[ObjectPointer] =
    tkvl.get(key).map:
      case None => throw new NoSuchElementException(key.toString)
      case Some(vs) => ObjectPointer(vs.value.bytes)

  def getRegisteredKeyValueObject(key: Key): Future[KeyValueObjectPointer] =
    getRegisteredObject(key).map(_.asInstanceOf[KeyValueObjectPointer])

  def getRegisteredDataObject(key: Key): Future[DataObjectPointer] =
    getRegisteredObject(key).map(_.asInstanceOf[DataObjectPointer])

  def prepareRegisterObject(key: Key, pointer: ObjectPointer)(using tx: Transaction): Future[Unit] =
    tkvl.set(key, Value(pointer.toArray), requirement = Some(Left(true))).map: _ =>
      tx.result.value match
        case Some(Failure(_: KeyAlreadyExists)) => throw KeyAlreadyExists(key)
        case _ => ()

  def registerObject(key: Key, pointer: ObjectPointer): Future[Unit] =
    client.retryStrategy.retryUntilSuccessful:
      tkvl.get(key).flatMap:
        case Some(vs) =>
          if ObjectPointer(vs.value.bytes) == pointer then
            Future.unit
          else
            throw StopRetrying(ObjectRegistry.DuplicateRegistration(key, ObjectPointer(vs.value.bytes)))
        case None =>
          client.transact: tx =>
            given Transaction = tx
            prepareRegisterObject(key, pointer)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `sbt 'testOnly *registries.ObjectRegistrySuite'`
Expected: All 7 tests pass.

- [ ] **Step 5: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/client/registries/ObjectRegistry.scala src/test/scala/org/aspen_ddp/aspen/client/registries/ObjectRegistrySuite.scala
git commit -m "Add generic Key-based ObjectRegistry in client.registries"
```

---

### Task 2: Add UUIDObjectRegistry wrapper

**Files:**
- Create: `src/main/scala/org/aspen_ddp/aspen/client/registries/UUIDObjectRegistry.scala`
- Create: `src/test/scala/org/aspen_ddp/aspen/client/registries/UUIDObjectRegistrySuite.scala`

- [ ] **Step 1: Write the test file**

Create `src/test/scala/org/aspen_ddp/aspen/client/registries/UUIDObjectRegistrySuite.scala`:

```scala
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
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `sbt 'testOnly *UUIDObjectRegistrySuite'`
Expected: Compilation failure — `UUIDObjectRegistry` class does not exist yet.

- [ ] **Step 3: Create the UUIDObjectRegistry implementation**

Create `src/main/scala/org/aspen_ddp/aspen/client/registries/UUIDObjectRegistry.scala`:

```scala
package org.aspen_ddp.aspen.client.registries

import org.aspen_ddp.aspen.client.{AspenClient, Transaction}
import org.aspen_ddp.aspen.common.objects.{DataObjectPointer, Key, KeyValueObjectPointer, ObjectPointer}

import java.util.UUID
import scala.concurrent.Future

class UUIDObjectRegistry(val client: AspenClient,
                         containingObjectPtr: KeyValueObjectPointer,
                         treeRootKey: Key):

  private val registry = ObjectRegistry(client, containingObjectPtr, treeRootKey)

  def getRegisteredObject(objectId: UUID): Future[ObjectPointer] =
    registry.getRegisteredObject(Key(objectId))

  def getRegisteredKeyValueObject(objectId: UUID): Future[KeyValueObjectPointer] =
    registry.getRegisteredKeyValueObject(Key(objectId))

  def getRegisteredDataObject(objectId: UUID): Future[DataObjectPointer] =
    registry.getRegisteredDataObject(Key(objectId))

  def prepareRegisterObject(objectId: UUID, pointer: ObjectPointer)(using tx: Transaction): Future[Unit] =
    registry.prepareRegisterObject(Key(objectId), pointer)

  def registerObject(objectId: UUID, pointer: ObjectPointer): Future[Unit] =
    registry.registerObject(Key(objectId), pointer)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `sbt 'testOnly *UUIDObjectRegistrySuite'`
Expected: All 2 tests pass.

- [ ] **Step 5: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/client/registries/UUIDObjectRegistry.scala src/test/scala/org/aspen_ddp/aspen/client/registries/UUIDObjectRegistrySuite.scala
git commit -m "Add UUIDObjectRegistry wrapper"
```

---

### Task 3: Add NamespacedObjectRegistry wrapper

**Files:**
- Create: `src/main/scala/org/aspen_ddp/aspen/client/registries/NamespacedObjectRegistry.scala`
- Create: `src/test/scala/org/aspen_ddp/aspen/client/registries/NamespacedObjectRegistrySuite.scala`

- [ ] **Step 1: Write the test file**

Create `src/test/scala/org/aspen_ddp/aspen/client/registries/NamespacedObjectRegistrySuite.scala`:

```scala
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
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `sbt 'testOnly *NamespacedObjectRegistrySuite'`
Expected: Compilation failure — `NamespacedObjectRegistry` class does not exist yet.

- [ ] **Step 3: Create the NamespacedObjectRegistry implementation**

Create `src/main/scala/org/aspen_ddp/aspen/client/registries/NamespacedObjectRegistry.scala`:

```scala
package org.aspen_ddp.aspen.client.registries

import org.aspen_ddp.aspen.client.{AspenClient, Transaction}
import org.aspen_ddp.aspen.common.objects.{DataObjectPointer, Key, KeyValueObjectPointer, ObjectPointer}

import scala.concurrent.Future

object NamespacedObjectRegistry:
  def makeKey(namespace: String, name: String): Key = Key(s"$namespace.$name")

class NamespacedObjectRegistry(val client: AspenClient,
                               containingObjectPtr: KeyValueObjectPointer,
                               treeRootKey: Key):

  import NamespacedObjectRegistry.makeKey

  private val registry = ObjectRegistry(client, containingObjectPtr, treeRootKey)

  def getRegisteredObject(namespace: String, name: String): Future[ObjectPointer] =
    registry.getRegisteredObject(makeKey(namespace, name))

  def getRegisteredKeyValueObject(namespace: String, name: String): Future[KeyValueObjectPointer] =
    registry.getRegisteredKeyValueObject(makeKey(namespace, name))

  def getRegisteredDataObject(namespace: String, name: String): Future[DataObjectPointer] =
    registry.getRegisteredDataObject(makeKey(namespace, name))

  def prepareRegisterObject(namespace: String, name: String, pointer: ObjectPointer)(using tx: Transaction): Future[Unit] =
    registry.prepareRegisterObject(makeKey(namespace, name), pointer)

  def registerObject(namespace: String, name: String, pointer: ObjectPointer): Future[Unit] =
    registry.registerObject(makeKey(namespace, name), pointer)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `sbt 'testOnly *NamespacedObjectRegistrySuite'`
Expected: All 3 tests pass.

- [ ] **Step 5: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/client/registries/NamespacedObjectRegistry.scala src/test/scala/org/aspen_ddp/aspen/client/registries/NamespacedObjectRegistrySuite.scala
git commit -m "Add NamespacedObjectRegistry wrapper"
```

---

### Task 4: Remove old ObjectRegistry and run full test suite

**Files:**
- Delete: `src/main/scala/org/aspen_ddp/aspen/client/ObjectRegistry.scala`
- Delete: `src/test/scala/org/aspen_ddp/aspen/client/ObjectRegistrySuite.scala`

- [ ] **Step 1: Delete the old files**

```bash
rm src/main/scala/org/aspen_ddp/aspen/client/ObjectRegistry.scala
rm src/test/scala/org/aspen_ddp/aspen/client/ObjectRegistrySuite.scala
```

- [ ] **Step 2: Compile to verify no remaining references to the old package**

Run: `sbt compile`
Expected: Clean compilation. The old `client.ObjectRegistry` has no other consumers in the codebase.

- [ ] **Step 3: Run the full registry test suite**

Run: `sbt 'testOnly *registries.*Suite'`
Expected: All 12 tests pass (7 ObjectRegistrySuite + 2 UUIDObjectRegistrySuite + 3 NamespacedObjectRegistrySuite).

- [ ] **Step 4: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/client/ObjectRegistry.scala src/test/scala/org/aspen_ddp/aspen/client/ObjectRegistrySuite.scala
git commit -m "Remove old UUID-specific ObjectRegistry"
```
