# ObjectPointer Refactor Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Remove redundant IDA, size, and per-store StorePointer data from ObjectPointer; obtain these from StoragePool instead.

**Architecture:** Bottom-up refactor starting from ObjectPointer and its encoding, then fixing all compilation errors outward. IDA is plumbed through constructor parameters and a new `poolIDAMap` on TransactionDescription. StorePointer type and Locater type are deleted.

**Tech Stack:** Scala 3, Protocol Buffers, ScalaTest

**Spec:** `docs/superpowers/specs/2026-05-17-object-pointer-refactor-design.md`

---

### Task 1: Remove StorePointer type and simplify Locater

**Files:**
- Delete: `src/main/scala/org/aspen_ddp/aspen/common/store/StorePointer.scala`
- Modify: `src/main/scala/org/aspen_ddp/aspen/server/store/Locater.scala`

- [ ] **Step 1: Delete StorePointer.scala**

Delete the file `src/main/scala/org/aspen_ddp/aspen/common/store/StorePointer.scala` entirely.

- [ ] **Step 2: Simplify Locater to just ObjectId**

Replace the contents of `src/main/scala/org/aspen_ddp/aspen/server/store/Locater.scala` with:

```scala
package org.aspen_ddp.aspen.server.store

import org.aspen_ddp.aspen.common.objects.ObjectId

type Locater = ObjectId
```

This makes `Locater` a type alias for `ObjectId`, minimizing downstream changes in files that reference the `Locater` type.

- [ ] **Step 3: Commit**

```bash
git add -A && git commit -m "Remove StorePointer type and simplify Locater to ObjectId alias"
```

---

### Task 2: Refactor ObjectPointer core class

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/common/objects/ObjectPointer.scala`

- [ ] **Step 1: Update ObjectPointer class hierarchy**

Replace the entire file with:

```scala
package org.aspen_ddp.aspen.common.objects

import java.nio.{ByteBuffer, ByteOrder}
import java.util.UUID

import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.util.Varint

sealed abstract class ObjectPointer(
                                     val id: ObjectId,
                                     val poolId: PoolId,
                                     val storePointer: Array[Byte]) {

  import ObjectPointer._

  def toArray: Array[Byte] = encodeToByteArray(this)

  def encodedSize: Int = numBytesNeededToEncode(this)

  def encodeInto(bb: ByteBuffer): Unit = ObjectPointer.encodeInto(bb, this)

  final override def equals(other: Any): Boolean = other match {
    case rhs: ObjectPointer => id == rhs.id && poolId == rhs.poolId &&
      java.util.Arrays.equals(storePointer, rhs.storePointer)
    case _ => false
  }

  final override def hashCode: Int = id.hashCode()

  def objectType: ObjectType.Value

  protected def addExtraToStringContent(sb: StringBuilder): Unit = {}

  def shortString: String = s"$objectType($id)"

  override def toString: String = {
    val sb = new StringBuilder
    sb.append(objectType.toString)
    sb.append("ObjectPointer(")
    sb.append(id.toString)
    sb.append(',')
    sb.append(poolId.toString)
    if (storePointer.nonEmpty) {
      sb.append(",sp=[")
      storePointer.foreach(b => sb.append(f"$b%02x"))
      sb.append(']')
    }
    sb.append(')')
    sb.toString()
  }
}

object ObjectPointer {
  protected val DataObjectPointerCode: Byte = 0
  protected val KeyValueObjectPointerCode: Byte = 1

  val EmptyArray = new Array[Byte](0)

  def apply(arr: Array[Byte]): ObjectPointer = fromArray(arr)

  def fromArray(arr: Array[Byte]): ObjectPointer = fromByteBuffer(ByteBuffer.wrap(arr))

  def fromByteBuffer(bb: ByteBuffer): ObjectPointer = {
    val origOrder = bb.order()
    bb.order(ByteOrder.BIG_ENDIAN)

    val baseSize = Varint.getUnsignedInt(bb)
    val endPos = bb.position() + baseSize
    val typeCode = bb.get()

    def getUUID: UUID = {
      val msb = bb.getLong()
      val lsb = bb.getLong()
      new UUID(msb, lsb)
    }

    val objectUUID = getUUID
    val poolUUID = getUUID

    val spLen = endPos - bb.position()
    val sp = if (spLen > 0) {
      val arr = new Array[Byte](spLen)
      bb.get(arr)
      arr
    } else EmptyArray

    bb.order(origOrder)

    typeCode match {
      case DataObjectPointerCode => new DataObjectPointer(ObjectId(objectUUID), PoolId(poolUUID), sp)
      case KeyValueObjectPointerCode => new KeyValueObjectPointer(ObjectId(objectUUID), PoolId(poolUUID), sp)
    }
  }

  def numBytesNeededToEncode(o: ObjectPointer): Int = {
    val baseLen = 1 + 32 + o.storePointer.length // typeCode + 2 UUIDs + storePointer
    Varint.getUnsignedIntEncodingLength(baseLen) + baseLen
  }

  def encodeToByteArray(o: ObjectPointer, numPaddingBytes: Option[Int] = None): Array[Byte] = {
    val totalSize = numBytesNeededToEncode(o) + numPaddingBytes.getOrElse(0)
    val arr = new Array[Byte](totalSize)
    val bb = ByteBuffer.wrap(arr)
    bb.order(ByteOrder.BIG_ENDIAN)
    encodeInto(bb, o)
    arr
  }

  def encodeInto(bb: ByteBuffer, o: ObjectPointer): Unit = {
    val baseSize = 1 + 32 + o.storePointer.length
    val typeCode = o match {
      case _: DataObjectPointer => DataObjectPointerCode
      case _: KeyValueObjectPointer => KeyValueObjectPointerCode
    }

    Varint.putUnsignedInt(bb, baseSize)
    bb.put(typeCode)
    bb.putLong(o.id.uuid.getMostSignificantBits)
    bb.putLong(o.id.uuid.getLeastSignificantBits)
    bb.putLong(o.poolId.uuid.getMostSignificantBits)
    bb.putLong(o.poolId.uuid.getLeastSignificantBits)
    if (o.storePointer.nonEmpty) {
      bb.put(o.storePointer)
    }
  }
}

class DataObjectPointer(id: ObjectId,
                        poolId: PoolId,
                        storePointer: Array[Byte]) extends ObjectPointer(id, poolId, storePointer) {

  override def objectType: ObjectType.Value = ObjectType.Data
}

object DataObjectPointer {

  def apply(arr: Array[Byte]): DataObjectPointer = ObjectPointer.fromArray(arr).asInstanceOf[DataObjectPointer]

  def apply(bb: ByteBuffer): DataObjectPointer = ObjectPointer.fromByteBuffer(bb).asInstanceOf[DataObjectPointer]

  def apply(id: ObjectId,
            poolId: PoolId,
            storePointer: Array[Byte] = ObjectPointer.EmptyArray): DataObjectPointer =
    new DataObjectPointer(id, poolId, storePointer)
}

class KeyValueObjectPointer(
                             id: ObjectId,
                             poolId: PoolId,
                             storePointer: Array[Byte]) extends ObjectPointer(id, poolId, storePointer) {

  override def objectType: ObjectType.Value = ObjectType.KeyValue
}

object KeyValueObjectPointer {

  def apply(arr: Array[Byte]): KeyValueObjectPointer = ObjectPointer.fromArray(arr).asInstanceOf[KeyValueObjectPointer]

  def apply(bb: ByteBuffer): KeyValueObjectPointer = ObjectPointer.fromByteBuffer(bb).asInstanceOf[KeyValueObjectPointer]

  def apply(id: ObjectId,
            poolId: PoolId,
            storePointer: Array[Byte] = ObjectPointer.EmptyArray): KeyValueObjectPointer =
    new KeyValueObjectPointer(id, poolId, storePointer)
}
```

- [ ] **Step 2: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/common/objects/ObjectPointer.scala
git commit -m "Refactor ObjectPointer: remove ida, size, storePointers fields"
```

---

### Task 3: Update protobuf definitions

**Files:**
- Modify: `src/main/protobuf/codec.proto`

- [ ] **Step 1: Update ObjectPointer message**

Replace the `StorePointer` and `ObjectPointer` messages (lines 48-65) with:

```protobuf
message ObjectPointer {
  UUID uuid = 1;
  UUID pool_uuid = 2;
  ObjectType object_type = 3;
  bytes store_pointer = 4;
}
```

Remove the `StorePointer` message entirely.

- [ ] **Step 2: Add PoolIDA message and update TransactionDescription**

Add a new message near the transaction section:

```protobuf
message PoolIDA {
  UUID pool_id = 1;
  IDA ida = 2;
}
```

Add a new field to the `TransactionDescription` message:

```protobuf
repeated PoolIDA pool_idas = <next available field number>;
```

Check the existing TransactionDescription message to determine the correct next field number.

- [ ] **Step 3: Remove StorePointer references from AllocateResponse and AllocationRecoveryState**

Find all remaining references to `StorePointer` in `codec.proto` (lines referencing `StorePointer` in `AllocateResponse`, `AllocationRecoveryState`, etc.) and remove or replace them. `AllocateResponse` should become a success/failure signal without a `StorePointer` field. The `AllocationRecoveryState` message should drop its `storePointer` field.

- [ ] **Step 4: Compile protobuf**

Run: `sbt compile`

This will regenerate the protobuf Java classes. Expect Scala compilation failures at this stage — that's intentional.

- [ ] **Step 5: Commit**

```bash
git add src/main/protobuf/codec.proto
git commit -m "Update protobuf: simplify ObjectPointer, add PoolIDA, remove StorePointer"
```

---

### Task 4: Update Codec encode/decode for ObjectPointer

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/common/network/Codec.scala`

- [ ] **Step 1: Update ObjectPointer encode/decode**

Replace the `encode(o: ObjectPointer)` method (around line 128) with:

```scala
def encode(o: ObjectPointer): codec.ObjectPointer =
  val builder = codec.ObjectPointer.newBuilder()
    .setUuid(encodeUUID(o.id.uuid))
    .setPoolUuid(encodeUUID(o.poolId.uuid))
    .setObjectType(encodeObjectType(o.objectType))

  if (o.storePointer.nonEmpty)
    builder.setStorePointer(com.google.protobuf.ByteString.copyFrom(o.storePointer))

  builder.build
```

Replace the `decode(m: codec.ObjectPointer)` method (around line 143) with:

```scala
def decode(m: codec.ObjectPointer): ObjectPointer =
  val uuid = decodeUUID(m.getUuid)
  val poolUuid = decodeUUID(m.getPoolUuid)
  val objectType = decodeObjectType(m.getObjectType)
  val sp = if m.getStorePointer.isEmpty then ObjectPointer.EmptyArray
           else m.getStorePointer.toByteArray

  objectType match
    case ObjectType.Data => new DataObjectPointer(ObjectId(uuid), PoolId(poolUuid), sp)
    case ObjectType.KeyValue => new KeyValueObjectPointer(ObjectId(uuid), PoolId(poolUuid), sp)
```

- [ ] **Step 2: Remove StorePointer encode/decode methods**

Find and remove the `encode(o: StorePointer)` and `decode(m: codec.StorePointer)` methods from Codec. Remove the import of `StorePointer`.

- [ ] **Step 3: Update TransactionDescription encode/decode**

Find the `encode(o: TransactionDescription)` method and add encoding for the new `poolIDAMap` field:

```scala
o.poolIDAMap.foreach { (poolId, ida) =>
  builder.addPoolIdas(codec.PoolIDA.newBuilder()
    .setPoolId(encodeUUID(poolId.uuid))
    .setIda(encode(ida))
    .build)
}
```

Find the `decode(m: codec.TransactionDescription)` method and add decoding:

```scala
val poolIDAMap = m.getPoolIdasList.asScala.map { pi =>
  PoolId(decodeUUID(pi.getPoolId)) -> decode(pi.getIda)
}.toMap
val primaryObjectIDA = poolIDAMap(PoolId(decodeUUID(m.getPrimaryObject.getPoolUuid)))
```

Pass `primaryObjectIDA` and `poolIDAMap` to the `TransactionDescription` constructor.

- [ ] **Step 4: Update AllocateResponse and AllocationRecoveryState encode/decode**

Find the `AllocateResponse` encode/decode in Codec. Remove `StorePointer` handling — the response becomes success (`true`) or failure (`false`) without a specific store pointer. Replace `result: Option[StorePointer]` references with `success: Boolean`.

Find `AllocationRecoveryState` encode/decode and remove the `storePointer` field.

- [ ] **Step 5: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/common/network/Codec.scala
git commit -m "Update Codec for simplified ObjectPointer and TransactionDescription with poolIDAMap"
```

---

### Task 5: Update TransactionDescription

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/common/transaction/TransactionDescription.scala`

- [ ] **Step 1: Add new fields and rewrite store-derivation methods**

Update the `TransactionDescription` case class to add the new fields and rewrite the methods that derive store sets:

```scala
final case class TransactionDescription(
  transactionId: TransactionId,
  startTimestamp: HLCTimestamp,
  primaryObject: ObjectPointer,
  designatedLeaderUID: Byte,
  requirements: List[TransactionRequirement],
  finalizationActions: List[SerializedFinalizationAction],
  originatingClient: Option[ClientId] = None,
  notifyOnResolution: List[StoreId] = Nil,
  notes: List[String] = Nil,
  primaryObjectIDA: IDA,
  poolIDAMap: Map[PoolId, IDA] = Map()) {

  def objectRequirements: List[TransactionObjectRequirement] = requirements.flatMap {
    case tor: TransactionObjectRequirement => Some(tor)
    case _ => None
  }

  def allReferencedObjectsSet: Set[ObjectPointer] = objectRequirements.map(_.objectPointer).toSet

  def primaryObjectDataStores: Set[StoreId] =
    (0 until primaryObjectIDA.width).map(i => StoreId(primaryObject.poolId, i.toByte)).toSet

  def allDataStores: Set[StoreId] = allReferencedObjectsSet.flatMap { ptr =>
    val ida = poolIDAMap(ptr.poolId)
    (0 until ida.width).map(i => StoreId(ptr.poolId, i.toByte))
  }

  def allHostedObjects(storeId: StoreId): List[ObjectPointer] =
    allReferencedObjectsSet.filter { op =>
      op.poolId == storeId.poolId && poolIDAMap.get(op.poolId).exists(ida => storeId.poolIndex < ida.width)
    }.toList

  def hostedObjectLocaters(storeId: StoreId): List[ObjectId] =
    allHostedObjects(storeId).map(_.id)

  def shortString: String = {
    val sb = new StringBuilder
    val ol = allReferencedObjectsSet.map(_.shortString).toList.sorted
    sb.append(s"Tx $transactionId: Objects: $ol")
    if (notes.nonEmpty) {
      sb.append("\n")
      notes.reverse.foreach { note =>
        sb.append(s"    $note")
        sb.append("\n")
      }
    }
    sb.toString
  }

  def serialize(): DataBuffer = Codec.encode(this).toByteArray
}
```

Add the IDA import at the top of the file:

```scala
import org.aspen_ddp.aspen.common.ida.IDA
import org.aspen_ddp.aspen.common.pool.PoolId
```

Remove the `Locater` import. Remove the `StoreId` import only if no longer used (it is still used, so keep it).

- [ ] **Step 2: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/common/transaction/TransactionDescription.scala
git commit -m "Update TransactionDescription with primaryObjectIDA and poolIDAMap"
```

---

### Task 6: Update TransactionDriver (server side)

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/server/transaction/TransactionDriver.scala`

- [ ] **Step 1: Update IDA and store derivation**

Change line 86 from:
```scala
def ida: IDA = txd.primaryObject.ida
```
to:
```scala
def ida: IDA = txd.primaryObjectIDA
```

Change line 90 from:
```scala
protected val validAcceptorSet: Set[Byte] = txd.primaryObject.storePointers.iterator.map(sp => sp.poolIndex).toSet
```
to:
```scala
protected val validAcceptorSet: Set[Byte] = (0 until ida.width).map(_.toByte).toSet
```

- [ ] **Step 2: Update receiveTxPrepareResponse store iteration**

In `receiveTxPrepareResponse` (around line 240-270), replace the block that iterates `ptr.storePointers` with:

```scala
for (ptr <- allObjects) {
  var nReplies = 0
  var nAbortVotes = 0
  var nCommitVotes = 0
  var canCommitObject = true

  val objIDA = txd.poolIDAMap(ptr.poolId)

  (0 until objIDA.width).foreach { idx =>
    peerDispositions.get(StoreId(ptr.poolId, idx.toByte)).foreach { disposition =>
      nReplies += 1
      disposition match {
        case TransactionDisposition.VoteCommit => nCommitVotes += 1
        case _ => nAbortVotes += 1
      }
    }
  }

  if (nReplies < objIDA.writeThreshold) {
    cantAttempt = true
    break()
  }

  if (nReplies != objIDA.width && nCommitVotes < objIDA.writeThreshold && objIDA.width - nAbortVotes >= objIDA.writeThreshold) {
    cantAttempt = true
    break()
  }

  if (objIDA.width - nAbortVotes < objIDA.writeThreshold)
    canCommitObject = false

  canCommitTransaction = canCommitTransaction && canCommitObject
}
```

- [ ] **Step 3: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/server/transaction/TransactionDriver.scala
git commit -m "Update TransactionDriver to use txd.primaryObjectIDA and poolIDAMap"
```

---

### Task 7: Update TransactionBuilder

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/client/internal/transaction/TransactionBuilder.scala`

- [ ] **Step 1: Add poolIDAMap parameter to buildTranaction**

Change the `buildTranaction` method signature (line 48) to accept a `poolIDAMap`:

```scala
def buildTranaction(opportunisticRebuildManager: OpportunisticRebuildManager,
                    poolIDAMap: Map[PoolId, IDA]): (TransactionDescription,
  Map[StoreId, TransactionData], HLCTimestamp) = synchronized {
```

Add import for `IDA` and `PoolId`:

```scala
import org.aspen_ddp.aspen.common.ida.IDA
import org.aspen_ddp.aspen.common.pool.PoolId
```

- [ ] **Step 2: Update primary object selection**

Replace line 65:
```scala
}.map(_.objectPointer).maxBy(ptr => ptr.ida)
```
with:
```scala
}.map(_.objectPointer).maxBy(ptr => poolIDAMap(ptr.poolId))
```

- [ ] **Step 3: Update addUpdate to use IDA width instead of storePointers**

Replace the `addUpdate` method (lines 79-106) with:

```scala
def addUpdate(pointer: ObjectPointer, encoded: Array[DataBuffer]): Unit = {
  val mpr = opportunisticRebuildManager.getPreTransactionOpportunisticRebuild(pointer)
  val ida = poolIDAMap(pointer.poolId)

  (0 until ida.width).zip(encoded).foreach { (idx, bb) =>
    val storeId = StoreId(pointer.poolId, idx.toByte)
    val lu = ObjectUpdate(pointer.id, bb)
    val opr = mpr.get(idx.toByte)

    updates.get(storeId) match {
      case None =>
        val lpr = opr match {
          case None => List()
          case Some(pr) => pr :: Nil
        }
        updates += (storeId -> TransactionData(List(lu), lpr))

      case Some(td) =>
        val newLu = lu :: td.localUpdates
        val newLp = opr match {
          case None => td.preTransactionRebuilds
          case Some(pr) => pr :: td.preTransactionRebuilds
        }
        updates += (storeId -> TransactionData(newLu, newLp))
    }
  }
}
```

- [ ] **Step 4: Update data encoding to use poolIDAMap**

Replace line 111:
```scala
addUpdate(objectPointer, objectPointer.ida.encode(buf))
```
with:
```scala
addUpdate(objectPointer, poolIDAMap(objectPointer.poolId).encode(buf))
```

Replace line 115:
```scala
addUpdate(kvu.pointer, KeyValueOperation.encode(kvu.operations, kvu.pointer.ida))
```
with:
```scala
addUpdate(kvu.pointer, KeyValueOperation.encode(kvu.operations, poolIDAMap(kvu.pointer.poolId)))
```

- [ ] **Step 5: Update TransactionDescription construction**

Update the `TransactionDescription` constructor call (around line 73) to include the new fields:

```scala
val primaryObjectIDA = poolIDAMap(primaryObject.poolId)

val txd = TransactionDescription(transactionId, startTimestamp, primaryObject, designatedLeaderUID,
  requirements, finalizationActions, originatingClient, notifyOnResolution.toList,
  notes, primaryObjectIDA, poolIDAMap)
```

- [ ] **Step 6: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/client/internal/transaction/TransactionBuilder.scala
git commit -m "Update TransactionBuilder to use poolIDAMap for IDA lookups"
```

---

### Task 8: Update TransactionImpl commit path to build poolIDAMap

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/client/internal/transaction/TransactionImpl.scala`

- [ ] **Step 1: Make commit() async-aware for pool lookups**

The `commit()` method (line 129) currently calls `bldr.buildTranaction()` synchronously. It needs to first resolve the `poolIDAMap` by looking up all referenced pools. Update the commit method:

```scala
def commit(): Future[HLCTimestamp] = synchronized {

  given ExecutionContext = client.clientContext

  if (!promise.isCompleted) {

    state.foreach { bldr =>
      // Collect all pool IDs from the builder's requirements
      val allPoolIds = bldr.allReferencedPoolIds

      // Resolve pool IDAs
      val fPoolIDAMap = Future.sequence(
        allPoolIds.map(poolId => client.getStoragePool(poolId).map(pool => poolId -> pool.defaultIDA))
      ).map(_.toMap)

      fPoolIDAMap.foreach { poolIDAMap =>
        val (txd, transactionData, timestamp) = bldr.buildTranaction(client.opportunisticRebuildManager, poolIDAMap)

        // ... rest of existing commit logic unchanged ...
      }

      fPoolIDAMap.failed.foreach { err =>
        promise.failure(err)
      }
    }
  }
  result
}
```

This requires adding an `allReferencedPoolIds` method to `TransactionBuilder`:

```scala
def allReferencedPoolIds: Set[PoolId] = synchronized {
  val fromRequirements = requirements.flatMap {
    case tor: TransactionObjectRequirement => Some(tor.objectPointer.poolId)
    case _ => None
  }.toSet

  val fromKV = keyValueUpdates.values.map(_.pointer.poolId).toSet
  val fromData = dataObjectUpdates.keys.map(_.poolId).toSet

  fromRequirements ++ fromKV ++ fromData
}
```

- [ ] **Step 2: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/client/internal/transaction/TransactionImpl.scala
git add src/main/scala/org/aspen_ddp/aspen/client/internal/transaction/TransactionBuilder.scala
git commit -m "Wire poolIDAMap resolution into transaction commit path"
```

---

### Task 9: Update ObjectState classes to accept IDA parameter

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/client/ObjectState.scala`

- [ ] **Step 1: Add IDA to ObjectState base class**

Add `val ida: IDA` to the base class and both concrete subclasses that use it. Update all `pointer.ida` references to `ida`:

Add import:
```scala
import org.aspen_ddp.aspen.common.ida.IDA
```

Update `ObjectState` base:
```scala
sealed abstract class ObjectState(
                                   val pointer: ObjectPointer,
                                   val ida: IDA,
                                   val revision: ObjectRevision,
                                   val refcount: ObjectRefcount,
                                   val timestamp: HLCTimestamp,
                                   val readTimestamp: HLCTimestamp) {
```

Update `MetadataObjectState`:
```scala
class MetadataObjectState(
                           pointer: ObjectPointer,
                           ida: IDA,
                           revision: ObjectRevision,
                           refcount: ObjectRefcount,
                           timestamp: HLCTimestamp,
                           readTimestamp: HLCTimestamp) extends ObjectState(pointer, ida, revision, refcount, timestamp, readTimestamp) {
```

Update the `MetadataObjectState.apply` factory to accept `ida: IDA`.

Update `DataObjectState`:
```scala
class DataObjectState(
                       override val pointer: DataObjectPointer,
                       ida: IDA,
                       revision: ObjectRevision,
                       refcount: ObjectRefcount,
                       timestamp: HLCTimestamp,
                       readTimestamp: HLCTimestamp,
                       val sizeOnStore: Int,
                       val data: DataBuffer) extends ObjectState(pointer, ida, revision, refcount, timestamp, readTimestamp) {
```

Change `def size` from `pointer.ida.calculateRestoredObjectSize(sizeOnStore)` to `ida.calculateRestoredObjectSize(sizeOnStore)`.

Change `getRebuildDataForStore` — this method currently uses `pointer.getEncodedDataIndexForStore(storeId)` which is removed. Replace with:

```scala
def getRebuildDataForStore(storeId: StoreId): Option[DataBuffer] =
  if storeId.poolId == pointer.poolId && storeId.poolIndex < ida.width then
    Some(ida.encode(data)(storeId.poolIndex))
  else
    None
```

Update the `DataObjectState.apply` factory to accept `ida: IDA`.

Update `KeyValueObjectState`:
```scala
class KeyValueObjectState(
                           override val pointer: KeyValueObjectPointer,
                           ida: IDA,
                           revision: ObjectRevision,
                           refcount: ObjectRefcount,
                           timestamp: HLCTimestamp,
                           readTimestamp: HLCTimestamp,
                           val minimum: Option[Key],
                           val maximum: Option[Key],
                           val left: Option[Value],
                           val right: Option[Value],
                           val contents: Map[Key, KeyValueObjectState.ValueState]
                         ) extends ObjectState(pointer, ida, revision, refcount, timestamp, readTimestamp) {
```

Replace all `pointer.ida` references in `sizeOnStore`, `guessSizeOnStoreAfterUpdate`, and `getRebuildDataForStore` with `ida`.

Update `getRebuildDataForStore`:
```scala
def getRebuildDataForStore(storeId: StoreId): Option[DataBuffer] =
  if storeId.poolId != pointer.poolId || storeId.poolIndex >= ida.width then
    None
  else
    val idaIndex = storeId.poolIndex

    import org.aspen_ddp.aspen.server.store.KVObjectState
    import org.aspen_ddp.aspen.server.store.ValueState

    var storeContent: Map[Key, ValueState] = Map()

    contents.foreach { t =>
      val (key, vs) = t
      val idaEncodedValue = Value(ida.encode(vs.value.bytes)(idaIndex))
      storeContent += (key -> new ValueState(idaEncodedValue, vs.revision, vs.timestamp, None))
    }

    val kvos = new KVObjectState(minimum, maximum, left, right, storeContent, Map())
    Some(kvos.encode())
```

Update `KeyValueObjectState.compare` factory to accept `ida: IDA`.

- [ ] **Step 2: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/client/ObjectState.scala
git commit -m "Add IDA parameter to ObjectState classes, replace pointer.ida references"
```

---

### Task 10: Update read path — ObjectReader, BaseObjectReader, readers

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/client/internal/read/ObjectReader.scala`
- Modify: `src/main/scala/org/aspen_ddp/aspen/client/internal/read/BaseObjectReader.scala`
- Modify: `src/main/scala/org/aspen_ddp/aspen/client/internal/read/DataObjectReader.scala`
- Modify: `src/main/scala/org/aspen_ddp/aspen/client/internal/read/KeyValueObjectReader.scala`

- [ ] **Step 1: Update ObjectReader trait**

Add `ida: IDA` and compute `allStores` and `receivedResponsesFromAllStores` from IDA width:

```scala
trait ObjectReader {
  val pointer: ObjectPointer
  val ida: IDA

  val allStores: Set[StoreId] = (0 until ida.width).map(i => StoreId(pointer.poolId, i.toByte)).toSet

  // ... other methods unchanged ...

  def receivedResponsesFromAllStores: Boolean = numResponses == ida.width

  // ... rest unchanged ...
}
```

Add IDA import.

- [ ] **Step 2: Update BaseObjectReader**

Add `val ida: IDA` parameter:

```scala
abstract class BaseObjectReader[PointerType <: ObjectPointer, StoreStateType <: StoreState](
                                 val metadataOnly: Boolean,
                                 val pointer: PointerType,
                                 val ida: IDA,
                                 val readUUID: UUID) extends ObjectReader with Logging {
```

Change `def width` and `def threshold`:
```scala
def width: Int = ida.width
def threshold: Int = ida.consistentRestoreThreshold
```

Update `MetadataObjectState` construction in `attemptRestore` to pass `ida`.

- [ ] **Step 3: Update DataObjectReader**

Add `ida: IDA` parameter and pass to `BaseObjectReader`:

```scala
class DataObjectReader(metadataOnly: Boolean, pointer: DataObjectPointer, ida: IDA, readUUID: UUID)
  extends BaseObjectReader[DataObjectPointer, DataObjectStoreState](metadataOnly, pointer, ida, readUUID) {
```

In `restoreObject`, change `pointer.ida.restore(segments)` to `ida.restore(segments)`.

Pass `ida` to `DataObjectState` constructor.

- [ ] **Step 4: Update KeyValueObjectReader**

Add `ida: IDA` parameter and pass to `BaseObjectReader`:

```scala
class KeyValueObjectReader(metadataOnly: Boolean, pointer: KeyValueObjectPointer, ida: IDA, readUUID: UUID)
  extends BaseObjectReader[KeyValueObjectPointer, KeyValueObjectStoreState](metadataOnly, pointer, ida, readUUID) {
```

Replace all `pointer.ida.restoreArray(...)` calls with `ida.restoreArray(...)`.

Pass `ida` to `KeyValueObjectState` constructor.

- [ ] **Step 5: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/client/internal/read/ObjectReader.scala
git add src/main/scala/org/aspen_ddp/aspen/client/internal/read/BaseObjectReader.scala
git add src/main/scala/org/aspen_ddp/aspen/client/internal/read/DataObjectReader.scala
git add src/main/scala/org/aspen_ddp/aspen/client/internal/read/KeyValueObjectReader.scala
git commit -m "Thread IDA through read path: ObjectReader, BaseObjectReader, readers"
```

---

### Task 11: Update BaseReadDriver and ReadManager

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/client/internal/read/BaseReadDriver.scala`
- Modify: `src/main/scala/org/aspen_ddp/aspen/client/internal/read/SimpleReadDriver.scala`
- Modify: `src/main/scala/org/aspen_ddp/aspen/client/internal/read/ReadDriver.scala`
- Modify: `src/main/scala/org/aspen_ddp/aspen/client/internal/read/ReadManager.scala`

- [ ] **Step 1: Add IDA to ReadDriver.Factory and BaseReadDriver**

Update `ReadDriver.Factory` type alias to include IDA:

```scala
type Factory = (AspenClient, ObjectPointer, IDA, UUID, String, Boolean) => ReadDriver
```

Add `ida: IDA` parameter to `BaseReadDriver`:

```scala
abstract class BaseReadDriver(
                               val client: AspenClient,
                               val objectPointer: ObjectPointer,
                               val ida: IDA,
                               val readUUID: UUID,
                               val comment: String,
                               val disableOpportunisticRebuild: Boolean = false
                    ) extends ReadDriver with Logging {
```

Update the `objectReader` construction (line 27-34) to pass `ida`:

```scala
val objectReader: ObjectReader = (objectPointer, readType) match {
  case (p: KeyValueObjectPointer, _: MetadataOnly) => new KeyValueObjectReader(true, p, ida, readUUID)
  case (p: KeyValueObjectPointer, _) => new KeyValueObjectReader(false, p, ida, readUUID)
  case (p: DataObjectPointer, _: MetadataOnly) => new DataObjectReader(true, p, ida, readUUID)
  case (p: DataObjectPointer, _: FullObject) => new DataObjectReader(false, p, ida, readUUID)
  case _ => throw new AssertionError("Invalid read combination")
}
```

Update `sendReadRequests` (line 63-74) — replace `objectPointer.storePointers.foreach(...)` with:

```scala
(0 until ida.width).foreach { idx =>
  sendReadRequest(StoreId(objectPointer.poolId, idx.toByte))
}
```

Also update the log message on line 64 similarly.

Update `noErrorRecoveryReadDriver` factory method to accept IDA.

- [ ] **Step 2: Update SimpleReadDriver**

Add `ida: IDA` parameter and pass through:

```scala
class SimpleReadDriver(
                        val initialDelay: Duration,
                        val maxDelay: Duration,
                        client: AspenClient,
                        objectPointer: ObjectPointer,
                        ida: IDA,
                        readUUID: UUID,
                        comment: String,
                        disableOpportunisticRebuild: Boolean) extends BaseReadDriver(client,
  objectPointer, ida, readUUID, comment, disableOpportunisticRebuild) {
```

Update `SimpleReadDriver.Factory` to accept and pass `ida`:

```scala
class Factory(val initialDelay: Duration, val maxDelay: Duration) {
  def apply(client: AspenClient, objectPointer: ObjectPointer, ida: IDA,
            readUUID: UUID, comment: String,
            disableOpportunisticRebuild: Boolean): ReadDriver = {
    new SimpleReadDriver(initialDelay, maxDelay, client,
      objectPointer, ida, readUUID, comment, disableOpportunisticRebuild)
  }
}
```

- [ ] **Step 3: Update ReadManager.read and CompletionQuery**

Update `ReadManager.read()` to accept `ida: IDA`:

```scala
def read(objectPointer: ObjectPointer, ida: IDA, comment: String,
         disableOpportunisticRebuild: Boolean = false): Future[ObjectState] = {
  val readUUID = UUID.randomUUID()
  val driver = driverFactory(client, objectPointer, ida, readUUID, comment, disableOpportunisticRebuild)
  // ... rest unchanged ...
}
```

Update the `CompletionQuery` class — replace `pointer.hostingStores` with store list derived from IDA, and `pointer.ida.consistentRestoreThreshold` with IDA parameter. Add `ida: IDA` to `CompletionQuery` constructor and `getTransactionFinalized`.

- [ ] **Step 4: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/client/internal/read/BaseReadDriver.scala
git add src/main/scala/org/aspen_ddp/aspen/client/internal/read/SimpleReadDriver.scala
git add src/main/scala/org/aspen_ddp/aspen/client/internal/read/ReadDriver.scala
git add src/main/scala/org/aspen_ddp/aspen/client/internal/read/ReadManager.scala
git commit -m "Thread IDA through ReadDriver, ReadManager, and SimpleReadDriver"
```

---

### Task 12: Update SimpleAspenClient read methods

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/client/internal/SimpleAspenClient.scala`
- Modify: `src/main/scala/org/aspen_ddp/aspen/client/AspenClient.scala`

- [ ] **Step 1: Update AspenClient.read signatures**

The `read` methods on `AspenClient` need to resolve the pool IDA before reading. Update the trait methods or add the pool resolution in the implementation. The simplest approach: the `SimpleAspenClient.read` methods call `getStoragePool` first:

```scala
override def read(pointer: DataObjectPointer, comment: String): Future[DataObjectState] =
  getStoragePool(pointer.poolId).flatMap { pool =>
    rmgr.read(pointer, pool.defaultIDA, comment).map(_.asInstanceOf[DataObjectState])
  }

override def read(pointer: KeyValueObjectPointer, comment: String): Future[KeyValueObjectState] =
  getStoragePool(pointer.poolId).flatMap { pool =>
    rmgr.read(pointer, pool.defaultIDA, comment).map(_.asInstanceOf[KeyValueObjectState])
  }
```

Update the `SimpleReadDriver.Factory` instantiation to match the new signature:

```scala
private val rmgr = new ReadManager(this,
  new SimpleReadDriver.Factory(initialReadDelay, maxReadDelay).apply)
```

- [ ] **Step 2: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/client/internal/SimpleAspenClient.scala
git commit -m "Wire pool IDA resolution into SimpleAspenClient read path"
```

---

### Task 13: Update allocation path

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/client/internal/allocation/AllocationDriver.scala`
- Modify: `src/main/scala/org/aspen_ddp/aspen/client/internal/allocation/BaseAllocationDriver.scala`
- Modify: `src/main/scala/org/aspen_ddp/aspen/client/internal/allocation/SuperSimpleAllocationDriver.scala`
- Modify: `src/main/scala/org/aspen_ddp/aspen/client/internal/allocation/AllocationManager.scala`
- Modify: `src/main/scala/org/aspen_ddp/aspen/client/internal/allocation/SinglePoolObjectAllocator.scala`

- [ ] **Step 1: Update AllocationDriver.receiveAllocationResult**

Remove `result: Option[StorePointer]` parameter. Change to `success: Boolean`:

```scala
def receiveAllocationResult(fromStoreId: StoreId,
                            success: Boolean,
                            storeNotFound: Boolean): Unit
```

- [ ] **Step 2: Update BaseAllocationDriver**

Remove `objectSize` and update constructor. Remove `StorePointer` import. Change `responses` from `Map[Byte, Option[StorePointer]]` to `Map[Byte, Boolean]`.

Update `receiveAllocationResult`:

```scala
def receiveAllocationResult(fromStoreId: StoreId,
                            success: Boolean,
                            storeNotFound: Boolean): Unit = synchronized {
  if (promise.isCompleted) return

  if storeNotFound then
    client.messenger.dropCacheForStore(fromStoreId)
    val storeData = synchronized { objectData(fromStoreId.poolIndex) }
    val msg = Allocate(fromStoreId, client.clientId, newObjectId, objectType, initialRefcount, storeData, timestamp,
      allocationTransactionId, revisionGuard)
    client.messenger.sendClientRequest(msg)
  else
    if (!responses.contains(fromStoreId.poolIndex))
      responses += (fromStoreId.poolIndex -> success)

    if (responses.size == objectData.size) {
      val errors = responses.filter(!_._2).keySet
      if (errors.isEmpty) {
        val op = objectType match {
          case ObjectType.Data => DataObjectPointer(newObjectId, poolId)
          case ObjectType.KeyValue => KeyValueObjectPointer(newObjectId, poolId)
        }
        promise.success(op)
      } else
        promise.failure(AllocationError(poolId))
    }
}
```

Remove `objectSize` from the constructor and from `Allocate` messages.

- [ ] **Step 3: Update AllocationManager**

Remove `objectSize` parameter from `allocate`, `allocateDataObject`, and `allocateKeyValueObject`. Remove the `objectSize.exists(...)` check.

Update `AllocationDriver.Factory.create` call and `Allocate` message construction to remove `objectSize`.

- [ ] **Step 4: Update SinglePoolObjectAllocator**

Remove `maxObjectSize` field. Update calls to `allocationManager`:

```scala
class SinglePoolObjectAllocator(val client: AspenClient,
                                val pool: StoragePool,
                                val objectIDA: IDA) extends ObjectAllocator {

  override def allocateDataObject(...): Future[DataObjectPointer] = {
    client.allocationManager.allocateDataObject(client, t, pool, objectIDA, initialRefcount,
      revisionGuard, initialContent)
  }

  override def allocateKeyValueObject(...): Future[KeyValueObjectPointer] = {
    client.allocationManager.allocateKeyValueObject(client, t, pool, objectIDA, initialRefcount,
      revisionGuard, initialContent, minimum, maximum, left, right)
  }
}
```

- [ ] **Step 5: Update SuperSimpleAllocationDriver similarly**

Match the constructor and factory changes from BaseAllocationDriver.

- [ ] **Step 6: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/client/internal/allocation/
git commit -m "Simplify allocation path: remove StorePointer and objectSize"
```

---

### Task 14: Update AllocationRecoveryState and AllocateResponse message

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/server/crl/AllocationRecoveryState.scala`
- Modify: `src/main/scala/org/aspen_ddp/aspen/common/network/Message.scala`

- [ ] **Step 1: Update AllocationRecoveryState**

Remove `storePointer` field. Check the file and remove any `StorePointer` references.

- [ ] **Step 2: Update AllocateResponse in Message.scala**

Change `result: Option[StorePointer]` to `success: Boolean`. Update all call sites.

- [ ] **Step 3: Update Allocate message**

Remove `objectSize` field from the `Allocate` message if present.

- [ ] **Step 4: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/server/crl/AllocationRecoveryState.scala
git add src/main/scala/org/aspen_ddp/aspen/common/network/Message.scala
git commit -m "Remove StorePointer from AllocationRecoveryState and AllocateResponse"
```

---

### Task 15: Update server-side store and CRL code

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/server/store/Frontend.scala`
- Modify: `src/main/scala/org/aspen_ddp/aspen/server/store/Bootstrap.scala`
- Modify: `src/main/scala/org/aspen_ddp/aspen/server/store/backend/RocksDBBackend.scala`
- Modify: `src/main/scala/org/aspen_ddp/aspen/server/store/backend/MapBackend.scala`
- Modify: `src/main/scala/org/aspen_ddp/aspen/server/store/backend/Backend.scala`
- Modify: `src/main/scala/org/aspen_ddp/aspen/server/crl/simple/LogContent.scala`

- [ ] **Step 1: Update Backend allocation methods**

Remove `StorePointer` return types from allocation methods in `Backend`, `RocksDBBackend`, and `MapBackend`. They should return `Unit` or a simple success indicator instead.

- [ ] **Step 2: Update Frontend**

Replace `Locater` usage with `ObjectId`. Replace `getStoreLocater` calls with direct ObjectId usage. Update `hostedObjectLocaters` call sites since it now returns `List[ObjectId]`.

- [ ] **Step 3: Update Bootstrap**

Remove `getStorePointer` calls. Update pointer construction to use new simplified constructors.

- [ ] **Step 4: Update LogContent**

Remove `StorePointer` construction in CRL recovery. Update `AllocationRecoveryState` construction to match its new signature (without `storePointer`).

- [ ] **Step 5: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/server/
git commit -m "Update server store, backend, and CRL code for simplified ObjectPointer"
```

---

### Task 16: Update TKVL and AmoebaFS IDA references

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/client/tkvl/KeyValueListNode.scala`
- Modify: `src/main/scala/org/aspen_ddp/aspen/amoebafs/impl/simple/SimpleFileContent.scala`
- Modify: `src/main/scala/org/aspen_ddp/aspen/amoebafs/impl/simple/SimpleFileSystem.scala`

- [ ] **Step 1: Update KeyValueListNode**

All references to `node.pointer.ida` (lines 264, 267, 303, 359, 365, 366, 393) need to use an IDA obtained from the `KeyValueObjectState.ida` field (already set in Task 9) or passed through from context. Check how `node` is used — it's a `KeyValueObjectState`, so `node.ida` is available.

Replace all `node.pointer.ida` with `node.ida`.

- [ ] **Step 2: Update SimpleFileContent**

Replace `dos.pointer.ida.calculateEncodedSegmentLength(db.size)` (line 200) and `ptr.ida.calculateEncodedSegmentLength(db.size)` (line 230) with IDA obtained from the pool. Check how the pool/IDA is available in `SimpleFileContent` context — it likely has access to an `AspenClient` or `StoragePool`. Thread the IDA through from the file system's pool.

- [ ] **Step 3: Update SimpleFileSystem**

Replace `fsRoot.ida` (line 69) with pool's `defaultIDA`:

```scala
defaultAllocator = new SinglePoolObjectAllocator(client, rootPool, rootPool.defaultIDA)
```

- [ ] **Step 4: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/client/tkvl/KeyValueListNode.scala
git add src/main/scala/org/aspen_ddp/aspen/amoebafs/impl/simple/SimpleFileContent.scala
git add src/main/scala/org/aspen_ddp/aspen/amoebafs/impl/simple/SimpleFileSystem.scala
git commit -m "Update TKVL and AmoebaFS to use pool IDA instead of pointer.ida"
```

---

### Task 17: Update remaining references and demo code

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/demo/Main.scala`
- Modify: `src/main/scala/org/aspen_ddp/aspen/client/internal/allocation/AllocationFinalizationAction.scala`
- Modify: `src/main/scala/org/aspen_ddp/aspen/client/internal/allocation/DeletionFinalizationAction.scala`
- Modify: `src/main/scala/org/aspen_ddp/aspen/client/internal/transaction/MissedUpdateFinalizationAction.scala`
- Modify: `src/main/scala/org/aspen_ddp/aspen/server/StoreManager.scala`
- Modify: `src/main/scala/org/aspen_ddp/aspen/client/internal/OpportunisticRebuildManager.scala` (if exists)

- [ ] **Step 1: Update Demo Main.scala**

Fix all ObjectPointer/StorePointer constructor calls. The bootstrap radicle pointer (line 346-347) changes from:

```scala
val radicle = KeyValueObjectPointer(Radicle.objectId, Radicle.poolId, None,
  cfg.bootstrapIDA, (0 until cfg.bootstrapIDA.width).map(idx => StorePointer(idx.toByte, Array())).toArray)
```

to:

```scala
val radicle = KeyValueObjectPointer(Radicle.objectId, Radicle.poolId)
```

Fix `radicle.ida` references (line 807) and `radicle.storePointers` (line 816) — obtain IDA from pool.

Fix `SinglePoolObjectAllocator` construction (line 377) to use pool's `defaultIDA` and remove `maxObjectSize`.

- [ ] **Step 2: Update finalization actions**

Review `AllocationFinalizationAction`, `DeletionFinalizationAction`, and `MissedUpdateFinalizationAction` for any `StorePointer`, `pointer.ida`, or `pointer.storePointers` references. Update them to use pool lookups where needed.

- [ ] **Step 3: Update StoreManager**

Fix `getStoreLocater` calls — replace with `ObjectId` usage.

- [ ] **Step 4: Compile check**

Run: `sbt compile`

Fix any remaining compilation errors. This is the critical step that catches every remaining reference.

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "Fix remaining references: demo, finalization actions, StoreManager"
```

---

### Task 18: Update test suites

**Files:**
- Modify: `src/test/scala/org/aspen_ddp/aspen/server/transaction/TransactionDriverSuite.scala`
- Modify: `src/test/scala/org/aspen_ddp/aspen/server/store/FrontendSuite.scala`
- Modify: `src/test/scala/org/aspen_ddp/aspen/server/store/RequirementsCheckerSuite.scala`
- Modify: `src/test/scala/org/aspen_ddp/aspen/server/crl/simple/SimpleCRLSuite.scala`
- Modify: `src/test/scala/org/aspen_ddp/aspen/client/internal/read/BaseReadDriverSuite.scala`
- Modify: `src/test/scala/org/aspen_ddp/aspen/IntegrationTestSuite.scala`
- Modify: `src/test/scala/org/aspen_ddp/aspen/TestNetwork.scala`
- Modify: remaining test files as needed

- [ ] **Step 1: Update TransactionDriverSuite**

Fix ObjectPointer construction (lines 32, 190, 254) — remove `ida` and `StorePointer` array arguments. Add `primaryObjectIDA` and `poolIDAMap` to `TransactionDescription` construction.

- [ ] **Step 2: Update store test suites**

Fix `FrontendSuite`, `RequirementsCheckerSuite`, `SimpleCRLSuite` — remove `StorePointer` references, update pointer construction, update `AllocationRecoveryState` construction.

- [ ] **Step 3: Update read driver test suite**

Fix `BaseReadDriverSuite` — update pointer construction and add IDA parameters to reader/driver construction.

- [ ] **Step 4: Update integration tests**

Fix `IntegrationTestSuite`, `TestNetwork`, and any other test files. Update `SinglePoolObjectAllocator` construction calls to remove `maxObjectSize`. Update pointer construction in all test helpers.

- [ ] **Step 5: Compile and run tests**

```bash
sbt compile
sbt test
```

Fix any remaining issues until all tests pass.

- [ ] **Step 6: Commit**

```bash
git add -A
git commit -m "Update all test suites for ObjectPointer refactor"
```

---

### Task 19: Clean up unused imports and dead code

- [ ] **Step 1: Compile clean**

Run `sbt compile` and verify zero warnings related to unused imports of `StorePointer`, `IDA` (in ObjectPointer), or `Locater`.

- [ ] **Step 2: Search for dead references**

```bash
grep -rn 'StorePointer\|getStorePointer\|getStoreLocater\|hostingStores\|getEncodedDataIndex' src/ --include="*.scala"
```

Any remaining hits are dead code that should be removed.

- [ ] **Step 3: Run full test suite**

```bash
sbt test
```

All tests must pass.

- [ ] **Step 4: Final commit**

```bash
git add -A
git commit -m "Clean up unused imports and dead code from ObjectPointer refactor"
```
