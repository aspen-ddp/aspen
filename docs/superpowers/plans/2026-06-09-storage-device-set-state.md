# StorageDeviceSetState Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `StorageDeviceSetState` as a new top-level metadata object (parallel to `AllocationGroupState`) that hierarchically groups physical storage, with a dedicated Radicle tree, a bootstrap set, required back-references on pool/device state, codec support, and a minimal client API.

**Architecture:** `StorageDeviceSetState` is stored as a DataObject (like `AllocationGroupState`). It is identified by a `StorageDeviceSetId` (UUID), has a `level` (0 = set of devices tracking `StorageDeviceId`s, 1+ = set of sets tracking `StorageDeviceSetId`s), an optional parent set, and a list of assigned `PoolId`s. `StoragePoolState` and `StorageDeviceState` each gain a required `storageDeviceSet: StorageDeviceSetId`. A new Radicle key (`Array[Byte](8)`) roots a `MetadataTree` (UUID → `DataObjectPointer`). Bootstrap creates a level-0 `"bootstrap"` set containing the bootstrap device and assigned to the bootstrap pool.

**Tech Stack:** Scala 3, Google Protocol Buffers (ScalaPB-generated `codec.*`), ScalaTest (`AnyFunSuite` for pure unit tests, `IntegrationTestSuite` for end-to-end), sbt.

**Source spec:** `docs/superpowers/specs/2026-06-09-storage-device-set-state-design.md`

---

## File Structure

**Created:**
- `src/main/scala/org/aspen_ddp/aspen/common/metadata/StorageDeviceSetState.scala` — the `StorageDeviceSetId` value class (+ companion) and `StorageDeviceSetState` case class (+ companion).
- `src/test/scala/org/aspen_ddp/aspen/common/metadata/StorageDeviceSetStateSuite.scala` — pure codec round-trip tests.
- `src/test/scala/org/aspen_ddp/aspen/common/metadata/StorageDeviceSetIntegrationSuite.scala` — bootstrap + client round-trip tests.

**Modified:**
- `src/main/protobuf/codec.proto` — new `StorageDeviceSetState` message; new field on `StoragePoolState` and `StorageDeviceState`.
- `src/main/scala/org/aspen_ddp/aspen/common/metadata/fixed_ids.scala` — `BootstrapStorageDeviceSetId`.
- `src/main/scala/org/aspen_ddp/aspen/common/network/Codec.scala` — encode/decode for the new type and the two augmented types.
- `src/main/scala/org/aspen_ddp/aspen/common/metadata/StoragePoolState.scala` — new field.
- `src/main/scala/org/aspen_ddp/aspen/common/metadata/StorageDeviceState.scala` — new field.
- `src/main/scala/org/aspen_ddp/aspen/common/Radicle.scala` — `StorageDeviceSetsTreeKey`.
- `src/main/scala/org/aspen_ddp/aspen/server/store/Bootstrap.scala` — `allocateData` helper, bootstrap set, tree, radicle content, bootstrap pool field.
- `src/main/scala/org/aspen_ddp/aspen/cmdline/Main.scala` — bootstrap device field.
- `src/main/scala/org/aspen_ddp/aspen/client/AspenClient.scala` — abstract decls + concrete `getStorageDeviceSetState` + `createNewStoragePool` param.
- `src/main/scala/org/aspen_ddp/aspen/client/internal/SimpleAspenClient.scala` — tree field + method impls.
- `src/test/scala/org/aspen_ddp/aspen/TestNetwork.scala` — `TClient` tree field + method impls.
- `src/test/scala/org/aspen_ddp/aspen/client/internal/read/BaseReadDriverSuite.scala` — `TClient` stubs.

---

## Task 1: Define `StorageDeviceSetState`, the ID type, and codec support

**Files:**
- Create: `src/main/scala/org/aspen_ddp/aspen/common/metadata/StorageDeviceSetState.scala`
- Modify: `src/main/protobuf/codec.proto` (after the `AllocationGroupState` message, around line 500)
- Modify: `src/main/scala/org/aspen_ddp/aspen/common/metadata/fixed_ids.scala`
- Modify: `src/main/scala/org/aspen_ddp/aspen/common/network/Codec.scala`
- Create (test): `src/test/scala/org/aspen_ddp/aspen/common/metadata/StorageDeviceSetStateSuite.scala`

- [ ] **Step 1: Add the protobuf message**

In `src/main/protobuf/codec.proto`, immediately after the `AllocationGroupState` message (which ends at line 500), add:

```protobuf
message StorageDeviceSetState {
  UUID setId = 1;
  int32 level = 2;
  string name = 3;
  UUID parent = 4;                   // absent = no parent
  repeated UUID memberDevices = 5;
  repeated UUID memberSets = 6;
  repeated UUID assignedPools = 7;
}
```

- [ ] **Step 2: Add the bootstrap fixed UUID**

In `src/main/scala/org/aspen_ddp/aspen/common/metadata/fixed_ids.scala`, add a line after `BootstrapHostId`:

```scala
object fixed_ids:

  val RadicleObjectId: ObjectId = new ObjectId(new UUID(0, 0))
  val BootstrapPoolId: PoolId = new PoolId(new UUID(0, 1))
  val BootstrapStorageDeviceId: StorageDeviceId = new StorageDeviceId(new UUID(0, 2))
  val BootstrapHostId: HostId = new HostId(new UUID(0, 3))
  val BootstrapStorageDeviceSetId: StorageDeviceSetId = new StorageDeviceSetId(new UUID(0, 4))
```

- [ ] **Step 3: Create the `StorageDeviceSetState.scala` source file**

Create `src/main/scala/org/aspen_ddp/aspen/common/metadata/StorageDeviceSetState.scala`:

```scala
package org.aspen_ddp.aspen.common.metadata

import org.aspen_ddp.aspen.client.DataObjectState
import org.aspen_ddp.aspen.codec
import org.aspen_ddp.aspen.common.network.Codec
import org.aspen_ddp.aspen.common.pool.PoolId

import java.util.UUID

final case class StorageDeviceSetId(uuid: UUID) extends AnyVal

object StorageDeviceSetId:
  val BootstrapStorageDeviceSetId: StorageDeviceSetId = fixed_ids.BootstrapStorageDeviceSetId

object StorageDeviceSetState:
  def apply(cfg: Array[Byte]): StorageDeviceSetState =
    Codec.decode(codec.StorageDeviceSetState.parseFrom(cfg))

  def apply(dos: DataObjectState): StorageDeviceSetState = apply(dos.data.getByteArray)

/** Hierarchical grouping of physical storage, parallel to AllocationGroupState.
 *
 *  The `level` strictly determines the member type:
 *    - level == 0 : a set of devices. `memberDevices` may be non-empty; `memberSets` must be empty.
 *    - level >= 1 : a set of sets.   `memberSets` may be non-empty; `memberDevices` must be empty.
 *
 *  `parent`, if present, must reference a set of a higher level. These invariants are
 *  documented here but enforcement (the mutation/management API) is out of scope for this pass.
 */
final case class StorageDeviceSetState(
    setId: StorageDeviceSetId,
    name: String,
    level: Int,
    parent: Option[StorageDeviceSetId],
    memberDevices: List[StorageDeviceId],
    memberSets: List[StorageDeviceSetId],
    assignedPools: List[PoolId]
):
  def toBytes: Array[Byte] = Codec.encode(this).toByteArray
```

- [ ] **Step 4: Add the codec encode/decode**

In `src/main/scala/org/aspen_ddp/aspen/common/network/Codec.scala`:

First, extend the metadata import on line 10. Change:

```scala
import org.aspen_ddp.aspen.common.metadata.{AllocationGroupState, HostId, HostState, StorageDeviceId, StorageDeviceState, StoragePoolState}
```

to:

```scala
import org.aspen_ddp.aspen.common.metadata.{AllocationGroupState, HostId, HostState, StorageDeviceId, StorageDeviceSetId, StorageDeviceSetState, StorageDeviceState, StoragePoolState}
```

Then, immediately after the `decode(m: codec.AllocationGroupState)` method (which ends at line 1086 with `new AllocationGroupState(...)`), add:

```scala
  def encode(o: StorageDeviceSetState): codec.StorageDeviceSetState =
    codec.StorageDeviceSetState(
      setId = Some(encodeUUID(o.setId.uuid)),
      level = o.level,
      name = o.name,
      parent = o.parent.map(p => encodeUUID(p.uuid)),
      memberDevices = o.memberDevices.map(d => encodeUUID(d.uuid)),
      memberSets = o.memberSets.map(s => encodeUUID(s.uuid)),
      assignedPools = o.assignedPools.map(p => encodeUUID(p.uuid))
    )

  def decode(m: codec.StorageDeviceSetState): StorageDeviceSetState =
    val setId = StorageDeviceSetId(decodeUUID(m.setId.get))
    val parent = m.parent.map(u => StorageDeviceSetId(decodeUUID(u)))
    val memberDevices = m.memberDevices.map(u => StorageDeviceId(decodeUUID(u))).toList
    val memberSets = m.memberSets.map(u => StorageDeviceSetId(decodeUUID(u))).toList
    val assignedPools = m.assignedPools.map(u => PoolId(decodeUUID(u))).toList
    StorageDeviceSetState(setId, m.name, m.level, parent, memberDevices, memberSets, assignedPools)
```

(`PoolId` is already imported in `Codec.scala`.)

- [ ] **Step 5: Write the failing codec round-trip test**

Create `src/test/scala/org/aspen_ddp/aspen/common/metadata/StorageDeviceSetStateSuite.scala`:

```scala
package org.aspen_ddp.aspen.common.metadata

import org.aspen_ddp.aspen.common.pool.PoolId
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.util.UUID

class StorageDeviceSetStateSuite extends AnyFunSuite with Matchers:

  test("StorageDeviceSetState round-trips through codec (level 0, no parent)"):
    val original = StorageDeviceSetState(
      setId = StorageDeviceSetId(UUID.randomUUID()),
      name = "leaf-set",
      level = 0,
      parent = None,
      memberDevices = List(StorageDeviceId(UUID.randomUUID()), StorageDeviceId(UUID.randomUUID())),
      memberSets = Nil,
      assignedPools = List(PoolId(UUID.randomUUID()))
    )

    val decoded = StorageDeviceSetState(original.toBytes)

    decoded should be(original)

  test("StorageDeviceSetState round-trips through codec (level 1, with parent)"):
    val original = StorageDeviceSetState(
      setId = StorageDeviceSetId(UUID.randomUUID()),
      name = "upper-set",
      level = 1,
      parent = Some(StorageDeviceSetId(UUID.randomUUID())),
      memberDevices = Nil,
      memberSets = List(StorageDeviceSetId(UUID.randomUUID())),
      assignedPools = Nil
    )

    val decoded = StorageDeviceSetState(original.toBytes)

    decoded should be(original)
```

- [ ] **Step 6: Run the test to verify it fails**

Run: `sbt 'testOnly *StorageDeviceSetStateSuite'`
Expected: FAIL — until Steps 1–4 are compiled it will fail to compile (e.g. unknown `codec.StorageDeviceSetState`); if Steps 1–4 are already in place it should compile and PASS. The point of running here is to confirm the suite builds and exercises the new code.

- [ ] **Step 7: Run the test to verify it passes**

Run: `sbt 'testOnly *StorageDeviceSetStateSuite'`
Expected: PASS (2 tests)

- [ ] **Step 8: Commit**

```bash
git add src/main/protobuf/codec.proto \
        src/main/scala/org/aspen_ddp/aspen/common/metadata/fixed_ids.scala \
        src/main/scala/org/aspen_ddp/aspen/common/metadata/StorageDeviceSetState.scala \
        src/main/scala/org/aspen_ddp/aspen/common/network/Codec.scala \
        src/test/scala/org/aspen_ddp/aspen/common/metadata/StorageDeviceSetStateSuite.scala
git commit -m "Add StorageDeviceSetState metadata type with codec support"
```

---

## Task 2: Add `storageDeviceSet` to `StoragePoolState`

**Files:**
- Modify: `src/main/protobuf/codec.proto:469-479` (`StoragePoolState` message)
- Modify: `src/main/scala/org/aspen_ddp/aspen/common/metadata/StoragePoolState.scala:31-41`
- Modify: `src/main/scala/org/aspen_ddp/aspen/common/network/Codec.scala:1019-1044`
- Modify: `src/main/scala/org/aspen_ddp/aspen/server/store/Bootstrap.scala:90-96`
- Modify: `src/main/scala/org/aspen_ddp/aspen/client/AspenClient.scala` (import line 9; `createNewStoragePool` at 126-152)
- Modify (test): `src/test/scala/org/aspen_ddp/aspen/common/metadata/StorageDeviceSetStateSuite.scala`

- [ ] **Step 1: Add the protobuf field**

In `src/main/protobuf/codec.proto`, add field 10 to `StoragePoolState`:

```protobuf
message StoragePoolState {
  UUID poolId = 1;
  string name = 2;
  IDA ida = 3;
  uint32 maxObjectSize = 4;
  repeated PoolStoreEntry stores = 5;
  BackendConfig backendConfig = 6;
  int64 currentUsage = 7;
  int64 maximumStoreSize = 8;
  repeated UUID allocationGroups = 9;
  UUID storageDeviceSet = 10;
}
```

- [ ] **Step 2: Add the field to the case class**

In `src/main/scala/org/aspen_ddp/aspen/common/metadata/StoragePoolState.scala`, insert `storageDeviceSet` after `backendConfig` and before the defaulted fields:

```scala
final case class StoragePoolState(
                                   poolId: PoolId,
                                   name: String,
                                   ida: IDA,
                                   maxObjectSize: Option[Int],
                                   stores: Array[StoragePoolState.StoreEntry],
                                   backendConfig: BackendConfig,
                                   storageDeviceSet: StorageDeviceSetId,
                                   currentUsage: Long = 0,
                                   maximumStoreSize: Long = 0,
                                   allocationGroups: List[UUID] = Nil
                                 ):

  def encode(): Array[Byte] = Codec.encode(this).toByteArray
```

(`StorageDeviceSetId` is in the same `common.metadata` package — no import needed.)

- [ ] **Step 3: Update the codec encode/decode for `StoragePoolState`**

In `src/main/scala/org/aspen_ddp/aspen/common/network/Codec.scala`, replace the `encode(o: StoragePoolState)` and `decode(m: codec.StoragePoolState)` methods (lines 1019-1044) with:

```scala
  def encode(o: StoragePoolState): codec.StoragePoolState =
    codec.StoragePoolState(
      poolId = Some(encodeUUID(o.poolId.uuid)),
      name = o.name,
      ida = Some(encode(o.ida)),
      maxObjectSize = o.maxObjectSize.getOrElse(0),
      stores = o.stores.map(encode).toSeq,
      backendConfig = encodeBackendConfig(o.backendConfig),
      currentUsage = o.currentUsage,
      maximumStoreSize = o.maximumStoreSize,
      allocationGroups = o.allocationGroups.map(encodeUUID),
      storageDeviceSet = Some(encodeUUID(o.storageDeviceSet.uuid))
    )

  def decode(m: codec.StoragePoolState): StoragePoolState =
    val poolId = PoolId(decodeUUID(m.poolId.get))
    val name = m.name
    val ida = decode(m.ida.get)
    val maxObjectSize = if m.maxObjectSize == 0 then None else Some(m.maxObjectSize)
    val stores = m.stores.map(decode).toArray
    val backendConfig = decodeBackendConfig(m.backendConfig)
    val currentUsage = m.currentUsage
    val maximumStoreSize = m.maximumStoreSize
    val allocationGroups = m.allocationGroups.map(decodeUUID).toList
    val storageDeviceSet = StorageDeviceSetId(decodeUUID(m.storageDeviceSet.get))

    StoragePoolState(poolId, name, ida, maxObjectSize, stores, backendConfig,
      storageDeviceSet, currentUsage, maximumStoreSize, allocationGroups)
```

- [ ] **Step 4: Update the bootstrap pool construction**

In `src/main/scala/org/aspen_ddp/aspen/server/store/Bootstrap.scala`, add `StorageDeviceSetId` to the metadata import on line 11:

```scala
import org.aspen_ddp.aspen.common.metadata.{BootstrapConfig, HostId, HostState, StorageDeviceSetId, StorageDeviceState, StoragePoolState}
```

Then update the `poolState` construction (lines 90-96) to pass the bootstrap set id:

```scala
    val poolState = StoragePoolState(
      PoolId.BootstrapPoolId,
      PoolId.BootstrapPoolName,
      ida,
      None,
      storeEntrys,
      RocksDBConfig(),
      StorageDeviceSetId.BootstrapStorageDeviceSetId).encode()
```

- [ ] **Step 5: Update `createNewStoragePool` to take a set id**

In `src/main/scala/org/aspen_ddp/aspen/client/AspenClient.scala`, add `StorageDeviceSetId` to the metadata import on line 9:

```scala
import org.aspen_ddp.aspen.common.metadata.{AllocationGroupState, HostId, HostState, StorageDeviceId, StorageDeviceSetId, StorageDeviceState, StoragePoolState}
```

Then change `createNewStoragePool` (lines 126-152) to add the `storageDeviceSet` parameter and thread it into the constructed state:

```scala
  def createNewStoragePool(name: String,
                           ida: IDA,
                           maxObjectSize: Option[Int],
                           storageDeviceIds: List[StorageDeviceId],
                           backendConfig: BackendConfig,
                           storageDeviceSet: StorageDeviceSetId,
                           maximumStoreSize: Long): Future[PoolId] =
    if storageDeviceIds.size < ida.width then
      Future.failed(new IllegalArgumentException("storageDeviceIds list must be at least as long as ida.width"))
    else
      given ExecutionContext = this.clientContext
      val poolId = PoolId(UUID.randomUUID())
      for
        devices <- Future.sequence(storageDeviceIds.map(sid => getStorageDeviceState(sid)))
        stores = devices.map(dev => StoragePoolState.StoreEntry(dev.hostId, dev.storageDeviceId)).toArray
        config = StoragePoolState(
          poolId,
          name,
          ida,
          maxObjectSize,
          stores,
          backendConfig,
          storageDeviceSet,
          0L,
          maximumStoreSize
        )
        _ <- createStoragePool(config)
      yield
        poolId
```

- [ ] **Step 6: Add the failing codec round-trip test for `StoragePoolState`**

Append to `src/test/scala/org/aspen_ddp/aspen/common/metadata/StorageDeviceSetStateSuite.scala` (and add the needed imports at the top of the file):

Add these imports below the existing imports:

```scala
import org.aspen_ddp.aspen.common.ida.Replication
import org.aspen_ddp.aspen.server.store.backend.RocksDBConfig
```

Add this test inside the suite body:

```scala
  test("StoragePoolState round-trips the storageDeviceSet field"):
    val setId = StorageDeviceSetId(UUID.randomUUID())
    val original = StoragePoolState(
      poolId = PoolId(UUID.randomUUID()),
      name = "pool",
      ida = Replication(3, 2),
      maxObjectSize = None,
      stores = Array(StoragePoolState.StoreEntry(HostId(UUID.randomUUID()), StorageDeviceId(UUID.randomUUID()))),
      backendConfig = RocksDBConfig(),
      storageDeviceSet = setId
    )

    val decoded = StoragePoolState(original.encode())

    decoded.storageDeviceSet should be(setId)
```

- [ ] **Step 7: Run the test to verify it fails, then compile-fix**

Run: `sbt 'testOnly *StorageDeviceSetStateSuite'`
Expected: FAIL to compile until Steps 1-5 are in place (the new positional argument changes ripple through `Codec`, `Bootstrap`, and `createNewStoragePool`). Resolve any remaining compile errors by following Steps 2-5 exactly.

- [ ] **Step 8: Run the test to verify it passes**

Run: `sbt 'testOnly *StorageDeviceSetStateSuite'`
Expected: PASS (3 tests)

- [ ] **Step 9: Run the broader build to confirm nothing else broke**

Run: `sbt Test/compile`
Expected: SUCCESS (compiles `Bootstrap`, `AspenClient`, and all test stubs).

- [ ] **Step 10: Commit**

```bash
git add src/main/protobuf/codec.proto \
        src/main/scala/org/aspen_ddp/aspen/common/metadata/StoragePoolState.scala \
        src/main/scala/org/aspen_ddp/aspen/common/network/Codec.scala \
        src/main/scala/org/aspen_ddp/aspen/server/store/Bootstrap.scala \
        src/main/scala/org/aspen_ddp/aspen/client/AspenClient.scala \
        src/test/scala/org/aspen_ddp/aspen/common/metadata/StorageDeviceSetStateSuite.scala
git commit -m "Add required storageDeviceSet field to StoragePoolState"
```

---

## Task 3: Add `storageDeviceSet` to `StorageDeviceState`

**Files:**
- Modify: `src/main/protobuf/codec.proto:561-567` (`StorageDeviceState` message)
- Modify: `src/main/scala/org/aspen_ddp/aspen/common/metadata/StorageDeviceState.scala:44-48`
- Modify: `src/main/scala/org/aspen_ddp/aspen/common/network/Codec.scala:1172-1195`
- Modify: `src/main/scala/org/aspen_ddp/aspen/cmdline/Main.scala:655-663`
- Modify (test): `src/test/scala/org/aspen_ddp/aspen/common/metadata/StorageDeviceSetStateSuite.scala`

- [ ] **Step 1: Add the protobuf field**

In `src/main/protobuf/codec.proto`, add field 6 to `StorageDeviceState`:

```protobuf
message StorageDeviceState {
  StorageDeviceId storage_device_id = 1;
  UUID host_id = 2; // Optional field
  repeated StorageDeviceStoreKeyValue stores = 3;
  int64 current_usage = 4;
  int64 total_size = 5;
  UUID storage_device_set = 6;
}
```

- [ ] **Step 2: Add the field to the case class**

In `src/main/scala/org/aspen_ddp/aspen/common/metadata/StorageDeviceState.scala`, append `storageDeviceSet` after `stores` (lines 44-48):

```scala
case class StorageDeviceState(storageDeviceId: StorageDeviceId,
                              hostId: HostId,
                              currentUsage: Long,
                              totalSize: Long,
                              stores: Map[StoreId, StorageDeviceState.StoreEntry],
                              storageDeviceSet: StorageDeviceSetId):
```

(`StorageDeviceSetId` is in the same package — no import needed. The existing `.copy`-based helpers `setHost`, `setStoreEntry`, `removeStore` are unaffected.)

- [ ] **Step 3: Update the codec encode/decode for `StorageDeviceState`**

In `src/main/scala/org/aspen_ddp/aspen/common/network/Codec.scala`, update `encode(o: StorageDeviceState)` (line 1172) to add the field:

```scala
  def encode(o: StorageDeviceState): codec.StorageDeviceState =
    codec.StorageDeviceState(
      storageDeviceId = Some(encode(o.storageDeviceId)),
      hostId = Some(encodeUUID(o.hostId.uuid)),
      stores = o.stores.map { (storeId, storeEntry) =>
        codec.StorageDeviceStoreKeyValue(
          storeId = ByteString.copyFrom(storeId.toBytes),
          entry = Some(encode(storeEntry))
        )
      }.toSeq,
      currentUsage = o.currentUsage,
      totalSize = o.totalSize,
      storageDeviceSet = Some(encodeUUID(o.storageDeviceSet.uuid))
    )
```

Then update `decode(m: codec.StorageDeviceState)` (lines 1186-1195). The current final lines are:

```scala
    val stores = m.stores.map { keyValue =>
      val storeId = StoreId(keyValue.storeId.toByteArray)
      val entry = decode(keyValue.entry.get)
      storeId -> entry
    }.toMap

    new StorageDeviceState(storageDeviceId, hostId, m.currentUsage, m.totalSize, stores)
```

Change the final construction line to add the decoded set id (insert a `val` for it just before):

```scala
    val stores = m.stores.map { keyValue =>
      val storeId = StoreId(keyValue.storeId.toByteArray)
      val entry = decode(keyValue.entry.get)
      storeId -> entry
    }.toMap

    val storageDeviceSet = StorageDeviceSetId(decodeUUID(m.storageDeviceSet.get))

    new StorageDeviceState(storageDeviceId, hostId, m.currentUsage, m.totalSize, stores, storageDeviceSet)
```

- [ ] **Step 4: Update the bootstrap device construction**

In `src/main/scala/org/aspen_ddp/aspen/cmdline/Main.scala`, update the `bootstrapStorageDevice` construction (lines 655-663) to add the set id as the final argument:

```scala
    val bootstrapStorageDevice = StorageDeviceState(
      storageDevConfig.storageDeviceId,
      hostConfig.hostId,
      0L,
      0L,
      bootstrapStores.map(backend =>
        backend.storeId -> StorageDeviceState.StoreEntry(StorageDeviceState.StoreStatus.Active, None)
      ).toMap,
      StorageDeviceSetId.BootstrapStorageDeviceSetId
    )
```

If `StorageDeviceSetId` is not already imported in `Main.scala`, add it. Find the existing metadata import in `Main.scala` and add `StorageDeviceSetId` to it; if there is none for the relevant symbols, add:

```scala
import org.aspen_ddp.aspen.common.metadata.StorageDeviceSetId
```

(Verify by searching `Main.scala` for `import org.aspen_ddp.aspen.common.metadata` and extending that list rather than adding a duplicate import.)

- [ ] **Step 5: Add the failing codec round-trip test for `StorageDeviceState`**

Append to `src/test/scala/org/aspen_ddp/aspen/common/metadata/StorageDeviceSetStateSuite.scala`:

Add this import below the existing imports:

```scala
import org.aspen_ddp.aspen.common.store.StoreId
```

Add this test inside the suite body:

```scala
  test("StorageDeviceState round-trips the storageDeviceSet field"):
    val setId = StorageDeviceSetId(UUID.randomUUID())
    val original = StorageDeviceState(
      storageDeviceId = StorageDeviceId(UUID.randomUUID()),
      hostId = HostId(UUID.randomUUID()),
      currentUsage = 0L,
      totalSize = 0L,
      stores = Map.empty,
      storageDeviceSet = setId
    )

    val decoded = StorageDeviceState(original.encode())

    decoded.storageDeviceSet should be(setId)
```

- [ ] **Step 6: Run the test to verify it fails, then compile-fix**

Run: `sbt 'testOnly *StorageDeviceSetStateSuite'`
Expected: FAIL to compile until Steps 1-4 are complete (the new constructor arg ripples through `Codec` and `Main`). Resolve compile errors by following Steps 2-4.

- [ ] **Step 7: Run the test to verify it passes**

Run: `sbt 'testOnly *StorageDeviceSetStateSuite'`
Expected: PASS (4 tests)

- [ ] **Step 8: Confirm the full build still compiles**

Run: `sbt Test/compile`
Expected: SUCCESS

- [ ] **Step 9: Commit**

```bash
git add src/main/protobuf/codec.proto \
        src/main/scala/org/aspen_ddp/aspen/common/metadata/StorageDeviceState.scala \
        src/main/scala/org/aspen_ddp/aspen/common/network/Codec.scala \
        src/main/scala/org/aspen_ddp/aspen/cmdline/Main.scala \
        src/test/scala/org/aspen_ddp/aspen/common/metadata/StorageDeviceSetStateSuite.scala
git commit -m "Add required storageDeviceSet field to StorageDeviceState"
```

---

## Task 4: Add the Radicle tree and bootstrap the `"bootstrap"` set

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/common/Radicle.scala:24`
- Modify: `src/main/scala/org/aspen_ddp/aspen/server/store/Bootstrap.scala`
- Create (test): `src/test/scala/org/aspen_ddp/aspen/common/metadata/StorageDeviceSetIntegrationSuite.scala`

- [ ] **Step 1: Add the Radicle tree key**

In `src/main/scala/org/aspen_ddp/aspen/common/Radicle.scala`, add after `StorageDevicesTreeKey` (line 24):

```scala
  private[aspen] val StorageDevicesTreeKey = Key(Array[Byte](7))
  private[aspen] val StorageDeviceSetsTreeKey = Key(Array[Byte](8))
```

- [ ] **Step 2: Add a `DataObject` bootstrap-allocate helper**

In `src/main/scala/org/aspen_ddp/aspen/server/store/Bootstrap.scala`, add the `DataBuffer`, `DataObjectPointer`, and `StorageDeviceSetState` imports. Update the existing imports:

- Line 9 currently: `import org.aspen_ddp.aspen.common.{HLCTimestamp, Radicle}` → change to `import org.aspen_ddp.aspen.common.{DataBuffer, HLCTimestamp, Radicle}`
- Line 11 (from Task 2) add `StorageDeviceSetState`: `import org.aspen_ddp.aspen.common.metadata.{BootstrapConfig, HostId, HostState, StorageDeviceSetId, StorageDeviceSetState, StorageDeviceState, StoragePoolState}`
- Line 12 already imports `DataObjectPointer`? It imports from `common.objects` a list that includes `KeyValueObjectPointer` but not `DataObjectPointer`. Add `DataObjectPointer` to that import list on line 12.

Then add an `allocateData` helper immediately after the `allocateTree` method (which ends at line 84). It mirrors `allocate` but encodes a DataObject:

```scala
    def allocateData(content: Array[Byte],
                     objectId: Option[ObjectId] = None): DataObjectPointer =
      val oid = objectId.getOrElse(ObjectId(UUID.randomUUID()))

      ida.encode(content).zip(stores).foreach { t =>
        val (storeData, store) = t
        store.bootstrapAllocate(oid, ObjectType.Data, bootstrapMetadata, DataBuffer(storeData))
      }

      val p = DataObjectPointer(oid, Radicle.poolId)

      allocTreeContent = (Key(p.id.uuid) -> p.toArray) :: allocTreeContent

      p
```

- [ ] **Step 3: Build the bootstrap set, its tree, and register it in the Radicle**

In `src/main/scala/org/aspen_ddp/aspen/server/store/Bootstrap.scala`, after the `storageDevicesTree` is created (it ends at line 143) and before the `radicleContent` list (line 145), add:

```scala
    val bootstrapDeviceSet = StorageDeviceSetState(
      setId = StorageDeviceSetId.BootstrapStorageDeviceSetId,
      name = "bootstrap",
      level = 0,
      parent = None,
      memberDevices = List(bootstrapStorageDeviceState.storageDeviceId),
      memberSets = Nil,
      assignedPools = List(PoolId.BootstrapPoolId)
    )

    val storageDeviceSetPtr = allocateData(bootstrapDeviceSet.toBytes)

    val storageDeviceSetsTree = allocateTree(
      ByteArrayKeyOrdering,
      Key(StorageDeviceSetId.BootstrapStorageDeviceSetId.uuid) -> storageDeviceSetPtr.toArray
    )
```

Then add the new tree to the `radicleContent` list (currently ends at line 153 with `Radicle.StorageDevicesTreeKey -> storageDevicesTree.encode(),`):

```scala
      Radicle.StorageDevicesTreeKey -> storageDevicesTree.encode(),
      Radicle.StorageDeviceSetsTreeKey -> storageDeviceSetsTree.encode(),
    )
```

- [ ] **Step 4: Write the failing bootstrap integration test**

Create `src/test/scala/org/aspen_ddp/aspen/common/metadata/StorageDeviceSetIntegrationSuite.scala`:

```scala
package org.aspen_ddp.aspen.common.metadata

import org.aspen_ddp.aspen.IntegrationTestSuite
import org.aspen_ddp.aspen.client.internal.MetadataTree
import org.aspen_ddp.aspen.common.Radicle
import org.aspen_ddp.aspen.common.objects.DataObjectPointer
import org.aspen_ddp.aspen.common.pool.PoolId

import scala.concurrent.ExecutionContext

class StorageDeviceSetIntegrationSuite extends IntegrationTestSuite:

  atest("bootstrap creates the 'bootstrap' storage device set"):
    given ExecutionContext = executionContext
    val tree = new MetadataTree(client, radicle, Radicle.StorageDeviceSetsTreeKey)
    for
      ptr <- tree.get(StorageDeviceSetId.BootstrapStorageDeviceSetId.uuid)
      dos <- client.read(ptr.asInstanceOf[DataObjectPointer])
    yield
      val sds = StorageDeviceSetState(dos)
      sds.setId should be(StorageDeviceSetId.BootstrapStorageDeviceSetId)
      sds.name should be("bootstrap")
      sds.level should be(0)
      sds.parent should be(None)
      sds.memberDevices should contain(StorageDeviceId.BootstrapStorageDeviceId)
      sds.memberSets should be(Nil)
      sds.assignedPools should contain(PoolId.BootstrapPoolId)

  atest("bootstrap pool and device reference the bootstrap set"):
    given ExecutionContext = executionContext
    for
      ps <- client.getStoragePoolState(PoolId.BootstrapPoolId)
      ds <- client.getStorageDeviceState(StorageDeviceId.BootstrapStorageDeviceId)
    yield
      ps.storageDeviceSet should be(StorageDeviceSetId.BootstrapStorageDeviceSetId)
      ds.storageDeviceSet should be(StorageDeviceSetId.BootstrapStorageDeviceSetId)
```

`MetadataTree.get` returns `Future[ObjectPointer]`; the `asInstanceOf[DataObjectPointer]` cast matches the DataObject we allocated. `radicle`, `client`, `executionContext`, and `atest` are all provided by `IntegrationTestSuite`.

- [ ] **Step 5: Run the test to verify it fails**

Run: `sbt 'testOnly *StorageDeviceSetIntegrationSuite'`
Expected: FAIL before Steps 1-3 are applied (the tree key/bootstrap wiring is missing — `tree.get` would not find the set, or `Radicle.StorageDeviceSetsTreeKey` won't resolve).

- [ ] **Step 6: Run the test to verify it passes**

Run: `sbt 'testOnly *StorageDeviceSetIntegrationSuite'`
Expected: PASS (2 tests)

- [ ] **Step 7: Run the full test suite to confirm nothing regressed in bootstrap**

Run: `sbt test`
Expected: SUCCESS — existing suites that bootstrap a `TestNetwork` (e.g. `AllocationGroupStateSuite`) still pass.

- [ ] **Step 8: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/common/Radicle.scala \
        src/main/scala/org/aspen_ddp/aspen/server/store/Bootstrap.scala \
        src/test/scala/org/aspen_ddp/aspen/common/metadata/StorageDeviceSetIntegrationSuite.scala
git commit -m "Bootstrap the StorageDeviceSets Radicle tree and 'bootstrap' set"
```

---

## Task 5: Add the client API across all `AspenClient` implementations

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/client/AspenClient.scala`
- Modify: `src/main/scala/org/aspen_ddp/aspen/client/internal/SimpleAspenClient.scala`
- Modify: `src/test/scala/org/aspen_ddp/aspen/TestNetwork.scala`
- Modify: `src/test/scala/org/aspen_ddp/aspen/client/internal/read/BaseReadDriverSuite.scala`
- Modify (test): `src/test/scala/org/aspen_ddp/aspen/common/metadata/StorageDeviceSetIntegrationSuite.scala`

- [ ] **Step 1: Add the abstract + concrete methods to `AspenClient`**

In `src/main/scala/org/aspen_ddp/aspen/client/AspenClient.scala`:

Add `StorageDeviceSetState` to the metadata import on line 9 (it should already contain `StorageDeviceSetId` from Task 2):

```scala
import org.aspen_ddp.aspen.common.metadata.{AllocationGroupState, HostId, HostState, StorageDeviceId, StorageDeviceSetId, StorageDeviceSetState, StorageDeviceState, StoragePoolState}
```

After the concrete `getAllocationGroupState` method (which ends at line 88 with `AllocationGroupState(dos)`), add the concrete getter:

```scala
  def getStorageDeviceSetState(storageDeviceSetId: StorageDeviceSetId): Future[StorageDeviceSetState] =
    given ExecutionContext = this.clientContext
    getStorageDeviceSetPointer(storageDeviceSetId).flatMap: pointer =>
      read(pointer).map: dos =>
        StorageDeviceSetState(dos)
```

After the existing `getAllocationGroupId` abstract declaration (line 92), add:

```scala
  def getStorageDeviceSetId(setName: String): Future[StorageDeviceSetId]
```

After the existing `getAllocationGroupPointer` abstract declaration (line 97), add:

```scala
  private[aspen] def getStorageDeviceSetPointer(storageDeviceSetId: StorageDeviceSetId): Future[DataObjectPointer]
```

After the existing `createAllocationGroup` abstract declaration (line 101), add:

```scala
  def createStorageDeviceSet(name: String, level: Int, parent: Option[StorageDeviceSetId]): Future[StorageDeviceSetId]
```

- [ ] **Step 2: Implement the methods in `SimpleAspenClient`**

In `src/main/scala/org/aspen_ddp/aspen/client/internal/SimpleAspenClient.scala` (which already imports `common.metadata.*`):

Add the tree field after `storageDevicesTree` (line 60):

```scala
  private val storageDevicesTree = new MetadataTree(this, radicle, Radicle.StorageDevicesTreeKey)
  private val storageDeviceSetsTree = new MetadataTree(this, radicle, Radicle.StorageDeviceSetsTreeKey)
```

Add `getStorageDeviceSetId` after the existing `getAllocationGroupId` override (line 88-89):

```scala
  override def getStorageDeviceSetId(setName: String): Future[StorageDeviceSetId] =
    namespacedRegistry.getRegisteredObject("device-set", setName).map(StorageDeviceSetId(_))
```

Add `getStorageDeviceSetPointer` after the existing `getAllocationGroupPointer` override (lines 101-102):

```scala
  override def getStorageDeviceSetPointer(storageDeviceSetId: StorageDeviceSetId): Future[DataObjectPointer] =
    storageDeviceSetsTree.get(storageDeviceSetId.uuid).map(_.asInstanceOf[DataObjectPointer])
```

Add `createStorageDeviceSet` after the existing `createAllocationGroup` override (it ends at line 125 with `ags.groupId`):

```scala
  override def createStorageDeviceSet(name: String, level: Int, parent: Option[StorageDeviceSetId]): Future[StorageDeviceSetId] =
    val sds = StorageDeviceSetState(
      StorageDeviceSetId(UUID.randomUUID()),
      name,
      level,
      parent,
      Nil,
      Nil,
      Nil
    )

    def onFail(err: Throwable): Future[Unit] = err match
      case e: DuplicateRegistration => throw StopRetrying(e)

    transactUntilSuccessfulWithRecovery(onFail): tx =>
      given Transaction = tx
      for
        bsPool <- getStoragePool(PoolId.BootstrapPoolId)
        ptr <- bsPool.allocator.allocateDataObject(DataBuffer(sds.toBytes))
        _ <- storageDeviceSetsTree.preparePut(sds.setId.uuid, ptr)
        _ <- namespacedRegistry.prepareRegisterObject("device-set", sds.name, sds.setId.uuid)
      yield
        sds.setId
```

- [ ] **Step 3: Implement the methods in `TestNetwork.TClient`**

In `src/test/scala/org/aspen_ddp/aspen/TestNetwork.scala`:

Add `StorageDeviceSetId` and `StorageDeviceSetState` to the metadata import on line 30:

```scala
import org.aspen_ddp.aspen.common.metadata.{AllocationGroupState, HostId, HostState, StorageDeviceId, StorageDeviceSetId, StorageDeviceSetState, StorageDeviceState, StoragePoolState}
```

Add the tree field after `storageDevicesTree` (line 115):

```scala
    val storageDevicesTree = new MetadataTree(this, radicle, Radicle.StorageDevicesTreeKey)
    val storageDeviceSetsTree = new MetadataTree(this, radicle, Radicle.StorageDeviceSetsTreeKey)
```

Add the `getStorageDeviceSetId` stub next to the other `???` name lookups (after line 107):

```scala
    def getStorageDeviceSetId(setName: String): Future[StorageDeviceSetId] = ???
```

Add `getStorageDeviceSetPointer` after the `getAllocationGroupPointer` override (lines 126-127):

```scala
    private[aspen] def getStorageDeviceSetPointer(storageDeviceSetId: StorageDeviceSetId): Future[DataObjectPointer] =
      storageDeviceSetsTree.get(storageDeviceSetId.uuid).map(_.asInstanceOf[DataObjectPointer])
```

Add `createStorageDeviceSet` after the `createAllocationGroup` override (it ends at line 148 with `ags.groupId`), mirroring that method's explicit-transaction style:

```scala
    override def createStorageDeviceSet(name: String, level: Int, parent: Option[StorageDeviceSetId]): Future[StorageDeviceSetId] =
      val sds = StorageDeviceSetState(
        StorageDeviceSetId(UUID.randomUUID()),
        name,
        level,
        parent,
        Nil,
        Nil,
        Nil
      )

      val tx = newTransaction()
      given Transaction = tx

      for
        bsPool <- getStoragePool(PoolId.BootstrapPoolId)
        ptr <- bsPool.allocator.allocateDataObject(DataBuffer(sds.toBytes))
        _ <- storageDeviceSetsTree.preparePut(sds.setId.uuid, ptr)
        _ <- namespacedRegistry.prepareRegisterObject("device-set", sds.name, sds.setId.uuid)
        _ <- tx.commit()
      yield
        sds.setId
```

- [ ] **Step 4: Add the `???` stubs to `BaseReadDriverSuite.TClient`**

In `src/test/scala/org/aspen_ddp/aspen/client/internal/read/BaseReadDriverSuite.scala`:

Add `StorageDeviceSetId` to the metadata import on line 20:

```scala
import org.aspen_ddp.aspen.common.metadata.{HostId, HostState, StorageDeviceId, StorageDeviceSetId, StoragePoolState}
```

Add three stubs alongside the existing `???` stubs in `TClient`. Place `getStorageDeviceSetId` after `getAllocationGroupId` (line 68), `getStorageDeviceSetPointer` after `getAllocationGroupPointer` (line 73), and `createStorageDeviceSet` after `createAllocationGroup` (line 75):

```scala
    def getStorageDeviceSetId(setName: String): Future[StorageDeviceSetId] = ???
```

```scala
    def getStorageDeviceSetPointer(storageDeviceSetId: StorageDeviceSetId): Future[DataObjectPointer] = ???
```

```scala
    def createStorageDeviceSet(name: String, level: Int, parent: Option[StorageDeviceSetId]): Future[StorageDeviceSetId] = ???
```

(`getStorageDeviceSetState` is concrete on the trait, so no stub is needed for it.)

- [ ] **Step 5: Write the failing client round-trip tests**

Append to `src/test/scala/org/aspen_ddp/aspen/common/metadata/StorageDeviceSetIntegrationSuite.scala`:

```scala
  atest("createStorageDeviceSet round-trips via the client"):
    given ExecutionContext = executionContext
    for
      setId <- client.createStorageDeviceSet("test-set", level = 0, parent = None)
      _ <- waitForTransactionsToComplete()
      sds <- client.getStorageDeviceSetState(setId)
    yield
      sds.setId should be(setId)
      sds.name should be("test-set")
      sds.level should be(0)
      sds.parent should be(None)
      sds.memberDevices should be(Nil)
      sds.memberSets should be(Nil)
      sds.assignedPools should be(Nil)

  atest("getStorageDeviceSetState reads the bootstrap set"):
    given ExecutionContext = executionContext
    client.getStorageDeviceSetState(StorageDeviceSetId.BootstrapStorageDeviceSetId).map: sds =>
      sds.name should be("bootstrap")
```

- [ ] **Step 6: Run the tests to verify they fail**

Run: `sbt 'testOnly *StorageDeviceSetIntegrationSuite'`
Expected: FAIL to compile until Steps 1-4 are complete (the new abstract methods must be implemented in all three `AspenClient` implementations).

- [ ] **Step 7: Run the tests to verify they pass**

Run: `sbt 'testOnly *StorageDeviceSetIntegrationSuite'`
Expected: PASS (4 tests total in this suite)

- [ ] **Step 8: Run the full test suite**

Run: `sbt test`
Expected: SUCCESS across all suites.

- [ ] **Step 9: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/client/AspenClient.scala \
        src/main/scala/org/aspen_ddp/aspen/client/internal/SimpleAspenClient.scala \
        src/test/scala/org/aspen_ddp/aspen/TestNetwork.scala \
        src/test/scala/org/aspen_ddp/aspen/client/internal/read/BaseReadDriverSuite.scala \
        src/test/scala/org/aspen_ddp/aspen/common/metadata/StorageDeviceSetIntegrationSuite.scala
git commit -m "Add StorageDeviceSet client API to all AspenClient implementations"
```

---

## Task 6: Final verification

**Files:** none (verification only)

- [ ] **Step 1: Clean compile**

Run: `sbt clean compile Test/compile`
Expected: SUCCESS — confirms the regenerated protobuf and all source/test changes compile from scratch.

- [ ] **Step 2: Full test run**

Run: `sbt test`
Expected: SUCCESS — all suites pass, including the two new ones (`StorageDeviceSetStateSuite`, `StorageDeviceSetIntegrationSuite`) and the existing bootstrap-dependent suites.

- [ ] **Step 3: Confirm clean working tree**

Run: `git status`
Expected: clean (all changes committed across Tasks 1-5).

---

## Self-Review Notes

**Spec coverage** — every spec section maps to a task:
- `StorageDeviceSetId` + `StorageDeviceSetState` (DataObject, two member fields, optional parent, assigned pools) → Task 1.
- `BootstrapStorageDeviceSetId` fixed id → Task 1.
- `storageDeviceSet` on `StoragePoolState` (required) + `createNewStoragePool` param → Task 2.
- `storageDeviceSet` on `StorageDeviceState` (required) → Task 3.
- `Radicle.StorageDeviceSetsTreeKey` + bootstrap set named `"bootstrap"` + tree + bootstrap pool/device references → Task 4.
- Client API (`getStorageDeviceSetState`, `getStorageDeviceSetPointer`, `getStorageDeviceSetId`, `createStorageDeviceSet`) across `SimpleAspenClient`, `TestNetwork.TClient`, and `BaseReadDriverSuite.TClient` → Task 5.

**Type consistency** — names are stable across tasks: `StorageDeviceSetId`, `StorageDeviceSetState`, `storageDeviceSet` (field), `StorageDeviceSetsTreeKey`, `BootstrapStorageDeviceSetId`, `createStorageDeviceSet(name, level, parent)`, `getStorageDeviceSetState`, `getStorageDeviceSetPointer`, `getStorageDeviceSetId`. The `"device-set"` registry namespace is used consistently in `createStorageDeviceSet` and `getStorageDeviceSetId`.

**Ordering for green builds** — required fields are added with their construction sites fixed in the same task (Tasks 2, 3); the bootstrap set/tree (Task 4) precedes the client API (Task 5), so `createStorageDeviceSet`'s `preparePut` always targets an existing Radicle tree.

**Note on `.get` decoding** — the codec decodes the new required UUID fields with `.get` (matching every other required-UUID decode in `Codec.scala`). Objects created through bootstrap or the client API always populate the field, so this is safe; there is no migration path for pre-existing serialized pool/device objects without the field (consistent with Aspen's crash-only, fresh-bootstrap model).
