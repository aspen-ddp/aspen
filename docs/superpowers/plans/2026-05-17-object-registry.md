# ObjectRegistry Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add an `ObjectRegistry` class that wraps a TKVL to map UUID keys to ObjectPointers, with idempotent registration and duplicate detection.

**Architecture:** A single class in the client package wrapping `TieredKeyValueList` with `KVObjectRootManager`. Registration uses the retry strategy directly (not `transactUntilSuccessful`) to support idempotent pre-check reads before transacting. A `DuplicateRegistration` exception in the companion object signals conflicting registrations.

**Tech Stack:** Scala 3, ScalaTest (AsyncFunSuite via IntegrationTestSuite), existing TKVL/KVObjectRootManager infrastructure.

**Spec:** `docs/superpowers/specs/2026-05-17-object-registry-design.md`

---

### Task 1: Create ObjectRegistry with `getRegisteredObject`

Start with the read path — it has no side effects and establishes the class structure.

**Files:**
- Create: `src/main/scala/org/aspen_ddp/aspen/client/ObjectRegistry.scala`
- Create: `src/test/scala/org/aspen_ddp/aspen/client/ObjectRegistrySuite.scala`

- [ ] **Step 1: Write the test file with a test for get-non-existent**

```scala
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
```

- [ ] **Step 2: Write the ObjectRegistry class with `getRegisteredObject`**

```scala
package org.aspen_ddp.aspen.client

import org.aspen_ddp.aspen.client.tkvl.{KVObjectRootManager, TieredKeyValueList}
import org.aspen_ddp.aspen.common.objects.{DataObjectPointer, Key, KeyAlreadyExists, KeyValueObjectPointer, ObjectPointer, Value}

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Failure

object ObjectRegistry:
  class DuplicateRegistration(val objectId: UUID, val existing: ObjectPointer)
    extends Exception(s"Duplicate registration for object $objectId")

class ObjectRegistry(val client: AspenClient,
                     containingObjectPtr: KeyValueObjectPointer,
                     treeRootKey: Key):

  given ExecutionContext = client.clientContext

  private val tkvl = TieredKeyValueList(client, KVObjectRootManager(client, treeRootKey, containingObjectPtr))

  def getRegisteredObject(objectId: UUID): Future[ObjectPointer] =
    tkvl.get(Key(objectId)).map:
      case None => throw NoSuchElementException(objectId.toString)
      case Some(vs) => ObjectPointer(vs.value.bytes)

  def getRegisteredKeyValueObject(objectId: UUID): Future[KeyValueObjectPointer] =
    getRegisteredObject(objectId).map(_.asInstanceOf[KeyValueObjectPointer])

  def getRegisteredDataObject(objectId: UUID): Future[DataObjectPointer] =
    getRegisteredObject(objectId).map(_.asInstanceOf[DataObjectPointer])
```

- [ ] **Step 3: Run the test**

Run: `sbt 'testOnly *ObjectRegistrySuite -- -z "non-existent"'`
Expected: PASS

- [ ] **Step 4: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/client/ObjectRegistry.scala src/test/scala/org/aspen_ddp/aspen/client/ObjectRegistrySuite.scala
git commit -m "Add ObjectRegistry with getRegisteredObject and typed accessors"
```

---

### Task 2: Add `prepareRegisterObject`

The transactional registration method. Used within an externally-managed transaction.

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/client/ObjectRegistry.scala`
- Modify: `src/test/scala/org/aspen_ddp/aspen/client/ObjectRegistrySuite.scala`

- [ ] **Step 1: Write the test for prepareRegisterObject within a transaction**

Add to `ObjectRegistrySuite`. Note: the object allocation and `prepareRegisterObject` share the same transaction — this is the intended usage pattern (allocate + register atomically).

```scala
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
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `sbt 'testOnly *ObjectRegistrySuite -- -z "prepareRegisterObject and retrieve"'`
Expected: FAIL — `prepareRegisterObject` does not exist yet.

- [ ] **Step 3: Implement `prepareRegisterObject`**

Add to `ObjectRegistry` class:

```scala
  def prepareRegisterObject(objectId: UUID, pointer: ObjectPointer)(using tx: Transaction): Future[Unit] =
    tkvl.set(Key(objectId), Value(pointer.toArray), requirement = Some(Left(true))).map: _ =>
      tx.result.value match
        case Some(Failure(_: KeyAlreadyExists)) => throw KeyAlreadyExists(Key(objectId))
        case _ => ()
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `sbt 'testOnly *ObjectRegistrySuite -- -z "prepareRegisterObject and retrieve"'`
Expected: PASS

- [ ] **Step 5: Write the test for prepareRegisterObject duplicate detection**

Add to `ObjectRegistrySuite`. Uses explicit `(using txN)` throughout to avoid `given` scoping issues across multiple transactions in a single for-comprehension.

```scala
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
```

- [ ] **Step 6: Run the test**

Run: `sbt 'testOnly *ObjectRegistrySuite -- -z "detects duplicate"'`
Expected: PASS — the existing implementation should handle this via the `tx.result.value` check.

- [ ] **Step 7: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/client/ObjectRegistry.scala src/test/scala/org/aspen_ddp/aspen/client/ObjectRegistrySuite.scala
git commit -m "Add prepareRegisterObject with eager KeyAlreadyExists detection"
```

---

### Task 3: Add `registerObject`

The standalone registration with retry and idempotency handling.

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/client/ObjectRegistry.scala`
- Modify: `src/test/scala/org/aspen_ddp/aspen/client/ObjectRegistrySuite.scala`

- [ ] **Step 1: Write the test for register and retrieve**

Add to `ObjectRegistrySuite`. The object must be allocated and committed in a separate transaction before calling `registerObject`, which manages its own transactions internally.

```scala
  atest("registerObject and retrieve"):
    for
      registry <- createRegistry()
      objectId = UUID.randomUUID()

      ikvos <- client.read(radicle)
      pool <- client.getStoragePool(Radicle.poolId)
      alloc = pool.createAllocator(Replication(3, 2))

      tx = client.newTransaction()
      ptr <- alloc.allocateKeyValueObject(ObjectRevisionGuard(radicle, ikvos.revision), Map())(using tx)
      _ <- tx.commit()
      _ <- waitForTransactionsToComplete()

      _ <- registry.registerObject(objectId, ptr)

      retrieved <- registry.getRegisteredObject(objectId)
    yield
      retrieved should be (ptr)
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `sbt 'testOnly *ObjectRegistrySuite -- -z "registerObject and retrieve"'`
Expected: FAIL — `registerObject` does not exist yet.

- [ ] **Step 3: Implement `registerObject`**

Add to `ObjectRegistry` class:

```scala
  def registerObject(objectId: UUID, pointer: ObjectPointer): Future[Unit] =
    val key = Key(objectId)
    client.retryStrategy.retryUntilSuccessful:
      tkvl.get(key).flatMap:
        case Some(vs) =>
          if ObjectPointer(vs.value.bytes) == pointer then
            Future.unit
          else
            throw StopRetrying(ObjectRegistry.DuplicateRegistration(objectId, ObjectPointer(vs.value.bytes)))
        case None =>
          client.transact: tx =>
            given Transaction = tx
            prepareRegisterObject(objectId, pointer)
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `sbt 'testOnly *ObjectRegistrySuite -- -z "registerObject and retrieve"'`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/client/ObjectRegistry.scala src/test/scala/org/aspen_ddp/aspen/client/ObjectRegistrySuite.scala
git commit -m "Add registerObject with retry and idempotency pre-check"
```

---

### Task 4: Add idempotency and duplicate detection tests for `registerObject`

**Files:**
- Modify: `src/test/scala/org/aspen_ddp/aspen/client/ObjectRegistrySuite.scala`

- [ ] **Step 1: Write the idempotent re-registration test**

Add to `ObjectRegistrySuite`:

```scala
  atest("registerObject is idempotent for same pointer"):
    for
      registry <- createRegistry()
      objectId = UUID.randomUUID()

      ikvos <- client.read(radicle)
      pool <- client.getStoragePool(Radicle.poolId)
      alloc = pool.createAllocator(Replication(3, 2))

      tx = client.newTransaction()
      ptr <- alloc.allocateKeyValueObject(ObjectRevisionGuard(radicle, ikvos.revision), Map())(using tx)
      _ <- tx.commit()
      _ <- waitForTransactionsToComplete()

      _ <- registry.registerObject(objectId, ptr)
      _ <- registry.registerObject(objectId, ptr)

      retrieved <- registry.getRegisteredObject(objectId)
    yield
      retrieved should be (ptr)
```

- [ ] **Step 2: Run the idempotency test**

Run: `sbt 'testOnly *ObjectRegistrySuite -- -z "idempotent"'`
Expected: PASS

- [ ] **Step 3: Write the duplicate-with-different-pointer test**

Add to `ObjectRegistrySuite`:

```scala
  atest("registerObject fails with DuplicateRegistration for different pointer"):
    for
      registry <- createRegistry()
      objectId = UUID.randomUUID()

      ikvos <- client.read(radicle)
      pool <- client.getStoragePool(Radicle.poolId)
      alloc = pool.createAllocator(Replication(3, 2))

      tx1 = client.newTransaction()
      ptr1 <- alloc.allocateKeyValueObject(ObjectRevisionGuard(radicle, ikvos.revision), Map())(using tx1)
      _ <- tx1.commit()
      _ <- waitForTransactionsToComplete()

      _ <- registry.registerObject(objectId, ptr1)

      ikvos2 <- client.read(radicle)
      tx2 = client.newTransaction()
      ptr2 <- alloc.allocateKeyValueObject(ObjectRevisionGuard(radicle, ikvos2.revision), Map())(using tx2)
      _ <- tx2.commit()
      _ <- waitForTransactionsToComplete()

      result <- registry.registerObject(objectId, ptr2).failed
    yield
      result shouldBe a [ObjectRegistry.DuplicateRegistration]
```

- [ ] **Step 4: Run the duplicate detection test**

Run: `sbt 'testOnly *ObjectRegistrySuite -- -z "DuplicateRegistration"'`
Expected: PASS

- [ ] **Step 5: Write the typed accessor tests**

Add to `ObjectRegistrySuite`:

```scala
  atest("getRegisteredKeyValueObject returns typed pointer"):
    for
      registry <- createRegistry()
      objectId = UUID.randomUUID()

      ikvos <- client.read(radicle)
      pool <- client.getStoragePool(Radicle.poolId)
      alloc = pool.createAllocator(Replication(3, 2))

      tx = client.newTransaction()
      kvPtr <- alloc.allocateKeyValueObject(ObjectRevisionGuard(radicle, ikvos.revision), Map())(using tx)
      _ <- tx.commit()
      _ <- waitForTransactionsToComplete()

      _ <- registry.registerObject(objectId, kvPtr)

      retrieved <- registry.getRegisteredKeyValueObject(objectId)
    yield
      retrieved should be (kvPtr)

  atest("getRegisteredDataObject returns typed pointer"):
    for
      registry <- createRegistry()
      objectId = UUID.randomUUID()

      ikvos <- client.read(radicle)
      pool <- client.getStoragePool(Radicle.poolId)
      alloc = pool.createAllocator(Replication(3, 2))

      tx = client.newTransaction()
      dataPtr <- alloc.allocateDataObject(ObjectRevisionGuard(radicle, ikvos.revision), DataBuffer(Array[Byte](1, 2, 3)))(using tx)
      _ <- tx.commit()
      _ <- waitForTransactionsToComplete()

      _ <- registry.registerObject(objectId, dataPtr)

      retrieved <- registry.getRegisteredDataObject(objectId)
    yield
      retrieved should be (dataPtr)
```

- [ ] **Step 6: Run all ObjectRegistry tests**

Run: `sbt 'testOnly *ObjectRegistrySuite'`
Expected: All tests PASS.

- [ ] **Step 7: Commit**

```bash
git add src/test/scala/org/aspen_ddp/aspen/client/ObjectRegistrySuite.scala
git commit -m "Add idempotency, duplicate detection, and typed accessor tests for ObjectRegistry"
```

---

### Task 5: Final verification

**Files:** None (read-only verification)

- [ ] **Step 1: Run full test suite**

Run: `sbt test`
Expected: All tests pass. No regressions.

- [ ] **Step 2: Verify the ObjectRegistry file is complete and clean**

Read `src/main/scala/org/aspen_ddp/aspen/client/ObjectRegistry.scala` and verify:
- All five public methods are present: `prepareRegisterObject`, `registerObject`, `getRegisteredObject`, `getRegisteredKeyValueObject`, `getRegisteredDataObject`
- `DuplicateRegistration` exception is in companion object
- No unused imports
