# Locater Removal Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Delete the `Locater` type alias and replace all uses with `ObjectPointer`, so backends and frontends have access to the full pointer including `storePointer`.

**Architecture:** Bottom-up: delete `Locater.scala` and `hostedObjectLocaters`, then fix the `Backend.read()` signature and implementations, then update `Frontend` and `Tx`, and finally the `StoreManager` call site. Each task fixes one layer.

**Tech Stack:** Scala 3, ScalaTest

**Spec:** `docs/superpowers/specs/2026-05-17-locater-removal-design.md`

---

### Task 1: Delete Locater.scala and hostedObjectLocaters

**Files:**
- Delete: `src/main/scala/org/aspen_ddp/aspen/server/store/Locater.scala`
- Modify: `src/main/scala/org/aspen_ddp/aspen/common/transaction/TransactionDescription.scala:10,94-99`

- [ ] **Step 1: Delete Locater.scala**

Delete the file `src/main/scala/org/aspen_ddp/aspen/server/store/Locater.scala`.

- [ ] **Step 2: Remove hostedObjectLocaters and Locater import from TransactionDescription**

In `src/main/scala/org/aspen_ddp/aspen/common/transaction/TransactionDescription.scala`:

Remove the import on line 10:
```scala
import org.aspen_ddp.aspen.server.store.Locater
```

Delete the `hostedObjectLocaters` method (lines 94-99):
```scala
  def hostedObjectLocaters(storeId: StoreId): List[Locater] =
    allReferencedObjectsSet.foldLeft(List[Locater]()): (l, op) =>
      if op.poolId == storeId.poolId && storeId.poolIndex < poolIDAMap(storeId.poolId).width then
        op.id :: l
      else
        l
```

All callers will use the existing `allHostedObjects` method instead, which already returns `List[ObjectPointer]`.

- [ ] **Step 3: Commit**

```bash
git add -A
git commit -m "Delete Locater type alias and hostedObjectLocaters method"
```

---

### Task 2: Update Backend trait and implementations

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/server/store/backend/Backend.scala:7,48`
- Modify: `src/main/scala/org/aspen_ddp/aspen/server/store/backend/MapBackend.scala:6,64-73`
- Modify: `src/main/scala/org/aspen_ddp/aspen/server/store/backend/RocksDBBackend.scala:8,117-144`

- [ ] **Step 1: Update Backend.scala**

In `src/main/scala/org/aspen_ddp/aspen/server/store/backend/Backend.scala`:

Replace the import on line 7:
```scala
import org.aspen_ddp.aspen.server.store.Locater
```
with:
```scala
import org.aspen_ddp.aspen.common.objects.ObjectPointer
```

Change the `read` signature on line 48 from:
```scala
  def read(locater: Locater): Unit
```
to:
```scala
  def read(pointer: ObjectPointer): Unit
```

- [ ] **Step 2: Update MapBackend.scala**

In `src/main/scala/org/aspen_ddp/aspen/server/store/backend/MapBackend.scala`:

Replace the import on line 6:
```scala
import org.aspen_ddp.aspen.server.store.{Locater, ObjectState}
```
with:
```scala
import org.aspen_ddp.aspen.common.objects.ObjectPointer
import org.aspen_ddp.aspen.server.store.ObjectState
```

Replace the `read` method (lines 64-74) with:
```scala
  override def read(pointer: ObjectPointer): Unit = {
    chandler.foreach { handler =>
      val objectId = pointer.id
      m.get(objectId) match {
        case None =>
          handler.complete(Read(storeId, objectId, Right(ReadError.ObjectNotFound)))
        case Some(os) =>
          val rs = ReadState(os.objectId, os.metadata, os.objectType, os.data, Set())
          handler.complete(Read(storeId, objectId, Left(rs)))
      }
    }
  }
```

- [ ] **Step 3: Update RocksDBBackend.scala**

In `src/main/scala/org/aspen_ddp/aspen/server/store/backend/RocksDBBackend.scala`:

Replace the import on line 8:
```scala
import org.aspen_ddp.aspen.server.store.Locater
```
with:
```scala
import org.aspen_ddp.aspen.common.objects.ObjectPointer
```

Replace the `read` method (lines 117-144) with:
```scala
  override def read(pointer: ObjectPointer): Unit = {
    val objectId = pointer.id
    logger.debug(s"RocksDBBackend beginning load of object: $objectId")

    synchronized { allocating.get(objectId) } match {
      case Some((objectType, metadata, data)) =>
        chandler.foreach { handler =>
          val rs = ReadState(objectId, metadata, objectType, data, Set())
          handler.complete(Read(storeId, objectId, Left(rs)))
        }
      case None =>
        val key = tokey(objectId)
        db.get(key).onComplete {
          case Failure(err) => logger.error(s"RocksDBBackend failed to load object: $objectId. Error: $err")
          case Success(oresult) =>
            chandler.foreach { handler =>
              oresult match {
                case None =>
                  logger.info(s"RocksDBBackend ObjectNotFound: $objectId")
                  handler.complete(Read(storeId, objectId, Right(ReadError.ObjectNotFound)))
                case Some(value) =>
                  logger.debug(s"RocksDBBackend loaded object: $objectId")
                  val (objectType, metadata, data) = decodeDBValue(value)
                  val rs = ReadState(objectId, metadata, objectType, data, Set())
                  handler.complete(Read(storeId, objectId, Left(rs)))
              }
            }
        }
    }
  }
```

- [ ] **Step 4: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/server/store/backend/Backend.scala src/main/scala/org/aspen_ddp/aspen/server/store/backend/MapBackend.scala src/main/scala/org/aspen_ddp/aspen/server/store/backend/RocksDBBackend.scala
git commit -m "Update Backend.read() to take ObjectPointer instead of Locater"
```

---

### Task 3: Update Tx.scala

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/server/transaction/Tx.scala:4,11,24,37`

- [ ] **Step 1: Update imports**

In `src/main/scala/org/aspen_ddp/aspen/server/transaction/Tx.scala`:

On line 4, add:
```scala
import org.aspen_ddp.aspen.common.objects.{ObjectId, ObjectPointer, ReadError}
```
replacing the existing:
```scala
import org.aspen_ddp.aspen.common.objects.{ObjectId, ReadError}
```

On line 11, remove `Locater` from the import:
```scala
import org.aspen_ddp.aspen.server.store.{Frontend, ObjectState, RequirementsApplyer, RequirementsChecker, RequirementsLocker}
```
replacing:
```scala
import org.aspen_ddp.aspen.server.store.{Frontend, Locater, ObjectState, RequirementsApplyer, RequirementsChecker, RequirementsLocker}
```

- [ ] **Step 2: Update constructor parameter and field**

Change the constructor parameter on line 24 from:
```scala
          private val objectLocaters: List[Locater]) extends Logging {
```
to:
```scala
          private val objectPointers: List[ObjectPointer]) extends Logging {
```

Change line 37 from:
```scala
  private var pendingObjectLoads: Int = objectLocaters.size
```
to:
```scala
  private var pendingObjectLoads: Int = objectPointers.size
```

- [ ] **Step 3: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/server/transaction/Tx.scala
git commit -m "Update Tx constructor to take List[ObjectPointer] instead of List[Locater]"
```

---

### Task 4: Update Frontend.scala

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/server/store/Frontend.scala:6,54-57,107-111,152-175,178-192,195-209,212-229`

- [ ] **Step 1: Update imports**

In `src/main/scala/org/aspen_ddp/aspen/server/store/Frontend.scala`:

On line 6, add `ObjectPointer` to the existing objects import:
```scala
import org.aspen_ddp.aspen.common.objects.{Metadata, ObjectId, ObjectPointer, ObjectRevision, ObjectType, ReadError}
```
replacing:
```scala
import org.aspen_ddp.aspen.common.objects.{Metadata, ObjectId, ObjectRevision, ObjectType, ReadError}
```

- [ ] **Step 2: Update recovery initialization block (lines 52-58)**

Replace:
```scala
    ltrs.foreach { trs =>
      val txd = TransactionDescription.deserialize(trs.serializedTxd)
      val locaters = txd.hostedObjectLocaters(storeId)
      val tx = new Tx(trs, txd, this, net, crl, statusCache, Nil, locaters)
      transactions += (txd.transactionId -> tx)
      locaters.foreach(locater => readObjectForTransaction(tx, locater))
    }
```
with:
```scala
    ltrs.foreach { trs =>
      val txd = TransactionDescription.deserialize(trs.serializedTxd)
      val pointers = txd.allHostedObjects(storeId)
      val tx = new Tx(trs, txd, this, net, crl, statusCache, Nil, pointers)
      transactions += (txd.transactionId -> tx)
      pointers.foreach(ptr => readObjectForTransaction(tx, ptr))
    }
```

- [ ] **Step 3: Update receivePrepare (lines 106-111)**

Replace:
```scala
      val trs = TransactionRecoveryState.initial(m.to, m.txd, m.objectUpdates)
      val locaters = m.txd.hostedObjectLocaters(m.to)
      val tx = new Tx(trs, m.txd, this, net, crl, statusCache, m.preTxRebuilds, locaters)
      transactions += (m.txd.transactionId -> tx)
      tx.receivePrepare(m)
      locaters.foreach(locater => readObjectForTransaction(tx, locater))
```
with:
```scala
      val trs = TransactionRecoveryState.initial(m.to, m.txd, m.objectUpdates)
      val pointers = m.txd.allHostedObjects(m.to)
      val tx = new Tx(trs, m.txd, this, net, crl, statusCache, m.preTxRebuilds, pointers)
      transactions += (m.txd.transactionId -> tx)
      tx.receivePrepare(m)
      pointers.foreach(ptr => readObjectForTransaction(tx, ptr))
```

- [ ] **Step 4: Update readObjectForNetwork (lines 152-176)**

Replace:
```scala
  def readObjectForNetwork(clientId: ClientId, readUUID: UUID, locater: Locater): Unit = {
    objectCache.get(locater) match {
      case Some(os) =>
        logger.trace(s"Reading Cached object for read $readUUID. Revision ${os.metadata.revision}")
        val cs = ReadResponse.CurrentState(os.metadata.revision, os.metadata.refcount, os.metadata.timestamp,
          os.data.size, Some(os.data), os.lockedWriteTransactions )

        val rr = ReadResponse(clientId, storeId, readUUID, HLCTimestamp.now, Right(cs) )
        net.sendClientResponse(rr)


      case None =>
        logger.trace(s"Reading uncached object from backing store for read $readUUID")
        val nr = NetworkRead(clientId, readUUID)

        pendingReads.get(locater) match {
          case Some(lst) =>
            pendingReads += (locater -> (nr :: lst))

          case None =>
            pendingReads += (locater -> (nr :: Nil))
            backend.read(locater)
        }
    }
  }
```
with:
```scala
  def readObjectForNetwork(clientId: ClientId, readUUID: UUID, pointer: ObjectPointer): Unit = {
    val objectId = pointer.id
    objectCache.get(objectId) match {
      case Some(os) =>
        logger.trace(s"Reading Cached object for read $readUUID. Revision ${os.metadata.revision}")
        val cs = ReadResponse.CurrentState(os.metadata.revision, os.metadata.refcount, os.metadata.timestamp,
          os.data.size, Some(os.data), os.lockedWriteTransactions )

        val rr = ReadResponse(clientId, storeId, readUUID, HLCTimestamp.now, Right(cs) )
        net.sendClientResponse(rr)


      case None =>
        logger.trace(s"Reading uncached object from backing store for read $readUUID")
        val nr = NetworkRead(clientId, readUUID)

        pendingReads.get(objectId) match {
          case Some(lst) =>
            pendingReads += (objectId -> (nr :: lst))

          case None =>
            pendingReads += (objectId -> (nr :: Nil))
            backend.read(pointer)
        }
    }
  }
```

- [ ] **Step 5: Update readObjectForOpportunisticRebuild (lines 178-193)**

Replace:
```scala
  def readObjectForOpportunisticRebuild(op: OpportunisticRebuild): Unit = {
    objectCache.get(op.pointer.id) match {
      case Some(os) => opportunisticRebuild(op, os)
      case None =>
        if op.pointer.poolId == storeId.poolId then
          val locater: Locater = op.pointer.id
          pendingReads.get(locater) match {
            case Some(lst) =>
              pendingReads += (locater -> (OpportuneRebuild(op) :: lst))

            case None =>
              pendingReads += (locater -> (OpportuneRebuild(op) :: Nil))
              backend.read(locater)
          }
    }
  }
```
with:
```scala
  def readObjectForOpportunisticRebuild(op: OpportunisticRebuild): Unit = {
    val objectId = op.pointer.id
    objectCache.get(objectId) match {
      case Some(os) => opportunisticRebuild(op, os)
      case None =>
        if op.pointer.poolId == storeId.poolId then
          pendingReads.get(objectId) match {
            case Some(lst) =>
              pendingReads += (objectId -> (OpportuneRebuild(op) :: lst))

            case None =>
              pendingReads += (objectId -> (OpportuneRebuild(op) :: Nil))
              backend.read(op.pointer)
          }
    }
  }
```

- [ ] **Step 6: Update readObjectForRepair (lines 195-210)**

Replace:
```scala
  def readObjectForRepair(current: ClientObjectState, completion: Promise[Unit]): Unit = {
    objectCache.get(current.pointer.id) match {
      case Some(os) => repair(current, completion, os)
      case None =>
        if current.pointer.poolId == storeId.poolId then
          val locater: Locater = current.pointer.id
          pendingReads.get(locater) match {
            case Some(lst) =>
              pendingReads += (locater -> (RepairRead(current, completion) :: lst))

            case None =>
              pendingReads += (locater -> (RepairRead(current, completion) :: Nil))
              backend.read(locater)
          }
    }
  }
```
with:
```scala
  def readObjectForRepair(current: ClientObjectState, completion: Promise[Unit]): Unit = {
    val objectId = current.pointer.id
    objectCache.get(objectId) match {
      case Some(os) => repair(current, completion, os)
      case None =>
        if current.pointer.poolId == storeId.poolId then
          pendingReads.get(objectId) match {
            case Some(lst) =>
              pendingReads += (objectId -> (RepairRead(current, completion) :: lst))

            case None =>
              pendingReads += (objectId -> (RepairRead(current, completion) :: Nil))
              backend.read(current.pointer)
          }
    }
  }
```

- [ ] **Step 7: Update readObjectForTransaction (lines 212-231)**

Replace:
```scala
  private def readObjectForTransaction(transaction: Tx, locater: Locater): Unit = {
    logger.trace(s"Loading object for Tx: ${transaction.transactionId}. Object: $locater")
    objectCache.get(locater) match {
      case Some(os) =>
        logger.trace(s"Loading object from CACHE for Tx: ${transaction.transactionId}. Object: $locater")
        transaction.objectLoaded(os)
      case None =>
        val tr = TransactionRead(transaction.transactionId)

        pendingReads.get(locater) match {
          case Some(lst) =>
            pendingReads += (locater -> (tr :: lst))

          case None =>
            pendingReads += (locater -> (tr :: Nil))
            logger.trace(s"Loading object from Backend for Tx: ${transaction.transactionId}. Object: $locater")
            backend.read(locater)
        }
    }
  }
```
with:
```scala
  private def readObjectForTransaction(transaction: Tx, pointer: ObjectPointer): Unit = {
    val objectId = pointer.id
    logger.trace(s"Loading object for Tx: ${transaction.transactionId}. Object: $objectId")
    objectCache.get(objectId) match {
      case Some(os) =>
        logger.trace(s"Loading object from CACHE for Tx: ${transaction.transactionId}. Object: $objectId")
        transaction.objectLoaded(os)
      case None =>
        val tr = TransactionRead(transaction.transactionId)

        pendingReads.get(objectId) match {
          case Some(lst) =>
            pendingReads += (objectId -> (tr :: lst))

          case None =>
            pendingReads += (objectId -> (tr :: Nil))
            logger.trace(s"Loading object from Backend for Tx: ${transaction.transactionId}. Object: $objectId")
            backend.read(pointer)
        }
    }
  }
```

- [ ] **Step 8: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/server/store/Frontend.scala
git commit -m "Update Frontend to use ObjectPointer instead of Locater"
```

---

### Task 5: Update StoreManager.scala call site

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/server/StoreManager.scala:661`

- [ ] **Step 1: Update the readObjectForNetwork call**

In `src/main/scala/org/aspen_ddp/aspen/server/StoreManager.scala`, change line 661 from:
```scala
              store.frontend.readObjectForNetwork(r.fromClient, r.readUUID, r.objectPointer.id)
```
to:
```scala
              store.frontend.readObjectForNetwork(r.fromClient, r.readUUID, r.objectPointer)
```

- [ ] **Step 2: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/server/StoreManager.scala
git commit -m "Pass full ObjectPointer to readObjectForNetwork instead of extracting .id"
```

---

### Task 6: Compile and test

- [ ] **Step 1: Compile the project**

Run: `sbt compile`
Expected: Clean compilation with no errors.

If there are compilation errors referencing `Locater`, they indicate a missed usage — fix and re-compile.

- [ ] **Step 2: Run the full test suite**

Run: `sbt test`
Expected: All tests pass. No test files reference `Locater` directly, so no test changes should be needed.

- [ ] **Step 3: Verify no remaining Locater references**

Run: `grep -rn "Locater" --include="*.scala" src/`
Expected: No output. If any references remain, fix them.

- [ ] **Step 4: Commit any fixes if needed**

If Steps 1-3 required additional fixes:
```bash
git add -A
git commit -m "Fix remaining Locater references found during verification"
```
