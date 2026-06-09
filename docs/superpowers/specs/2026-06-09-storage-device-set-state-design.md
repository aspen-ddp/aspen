# StorageDeviceSetState — Design

**Date:** 2026-06-09
**Status:** Approved (design)
**Scope:** Metadata plumbing only — the data structure, its Radicle tree, the
bootstrap set, the two new required fields on pool/device state, codec support,
and a minimal client API (getters + creator). No allocation, rebalancing, or
durable-task logic (those are tracked separately in `NOTES.txt`).

## Overview

Add `StorageDeviceSetState` as a new top-level metadata object, parallel to
`AllocationGroupState`. A storage device set is a hierarchical grouping of
*physical* storage:

- Identified by a `StorageDeviceSetId` (UUID).
- Has a human-readable name.
- Has a `level`. The level strictly determines the member type:
  - **Level 0** — a *set of devices*: tracks member `StorageDeviceId`s; cannot
    contain member sets.
  - **Level 1+** — a *set of sets*: tracks member `StorageDeviceSetId`s; cannot
    contain member devices.
- Has an optional parent `StorageDeviceSetId`, which must reference a set of a
  *higher* level.
- Tracks the list of `PoolId`s assigned to the set.

Two existing state objects gain a back-reference to the set they belong to:

- `StoragePoolState.storageDeviceSet` — assigned at pool creation.
- `StorageDeviceState.storageDeviceSet` — assigned when a device is registered.

A dedicated Radicle tree tracks all device sets, and a `"bootstrap"` device set
is created during system bootstrap.

## Design Decisions

These were settled during brainstorming:

1. **Scope:** metadata plumbing only.
2. **Storage type:** `StorageDeviceSetState` is stored as a **DataObject**,
   mirroring its closest analog `AllocationGroupState`. (Pointers are
   `DataObjectPointer`; updates are whole-state overwrites.)
3. **Member representation:** two explicit fields —
   `memberDevices: List[StorageDeviceId]` and `memberSets: List[StorageDeviceSetId]`.
   Only one is ever non-empty, determined by `level`.
4. **New fields:** `storageDeviceSet` is a **required** `StorageDeviceSetId` on
   both `StoragePoolState` and `StorageDeviceState` (matches the request text and
   the fact that it is always assigned at creation/registration).
5. **Bootstrap set name:** `"bootstrap"` (the registry namespace makes a
   `-device-set` suffix redundant).

## Components

### 1. `StorageDeviceSetId` and bootstrap UUID

Define in the new `StorageDeviceSetState.scala` (mirroring how `StorageDeviceId`
lives inside `StorageDeviceState.scala`):

```scala
final case class StorageDeviceSetId(uuid: UUID) extends AnyVal
```

In `common/metadata/fixed_ids.scala`, add:

```scala
val BootstrapStorageDeviceSetId: StorageDeviceSetId = new StorageDeviceSetId(new UUID(0, 4))
```

(Existing fixed UUIDs occupy `(0,0)`–`(0,3)`.)

### 2. `StorageDeviceSetState` (DataObject)

New file `src/main/scala/org/aspen_ddp/aspen/common/metadata/StorageDeviceSetState.scala`:

```scala
final case class StorageDeviceSetState(
    setId: StorageDeviceSetId,
    name: String,
    level: Int,                            // 0 = set of devices, 1+ = set of sets
    parent: Option[StorageDeviceSetId],    // must reference a higher-level set
    memberDevices: List[StorageDeviceId],  // non-empty only when level == 0
    memberSets: List[StorageDeviceSetId],  // non-empty only when level >= 1
    assignedPools: List[PoolId]
):
  def toBytes: Array[Byte] = Codec.encode(this).toByteArray
```

Companion object (mirroring `AllocationGroupState`):

```scala
object StorageDeviceSetState:
  def apply(cfg: Array[Byte]): StorageDeviceSetState =
    Codec.decode(codec.StorageDeviceSetState.parseFrom(cfg))
  def apply(dos: DataObjectState): StorageDeviceSetState =
    apply(dos.data.getByteArray)
```

The level/member invariants are documented in comments. Enforcement (a mutation
API that adds/removes members and validates parent level on the way in) is
explicitly **out of scope** for this pass.

### 3. Radicle tree

In `common/Radicle.scala`, add:

```scala
private[aspen] val StorageDeviceSetsTreeKey = Key(Array[Byte](8))
```

The tree is a `MetadataTree` mapping the set UUID → `DataObjectPointer`, using
`ByteArrayKeyOrdering`, exactly like `allocationGroupsTree`.

### 4. New required fields on existing state

**`StoragePoolState`** — add `storageDeviceSet: StorageDeviceSetId`, inserted
immediately after `backendConfig` and before the defaulted fields
(`currentUsage`, `maximumStoreSize`, `allocationGroups`):

```scala
final case class StoragePoolState(
    poolId: PoolId,
    name: String,
    ida: IDA,
    maxObjectSize: Option[Int],
    stores: Array[StoragePoolState.StoreEntry],
    backendConfig: BackendConfig,
    storageDeviceSet: StorageDeviceSetId,   // NEW (required)
    currentUsage: Long = 0,
    maximumStoreSize: Long = 0,
    allocationGroups: List[UUID] = Nil
)
```

**`StorageDeviceState`** — add `storageDeviceSet: StorageDeviceSetId`, appended
after `stores`:

```scala
case class StorageDeviceState(
    storageDeviceId: StorageDeviceId,
    hostId: HostId,
    currentUsage: Long,
    totalSize: Long,
    stores: Map[StoreId, StorageDeviceState.StoreEntry],
    storageDeviceSet: StorageDeviceSetId    // NEW (required)
)
```

Construction sites to update:

- `StoragePoolState`: `Bootstrap.scala` (bootstrap pool), `createNewStoragePool`
  (in `AspenClient.scala`), and the Codec decoder. `AllocationGroupState`'s
  `.copy` usages are unaffected.
- `StorageDeviceState`: `Main.scala` (bootstrap device), the Codec decoder.
  Existing `.copy` call sites (e.g. `setHost`, `setStoreEntry`, `removeStore`,
  and `StoreManager`/usage managers) are unaffected.

### 5. Serialization

**`src/main/protobuf/codec.proto`** — new message:

```protobuf
message StorageDeviceSetState {
  UUID setId = 1;
  string name = 2;
  int32 level = 3;
  UUID parent = 4;                   // absent = no parent
  repeated UUID memberDevices = 5;
  repeated UUID memberSets = 6;
  repeated UUID assignedPools = 7;
}
```

Add a field to each existing message:

- `StoragePoolState`: `UUID storageDeviceSet = 10;`
- `StorageDeviceState`: `UUID storageDeviceSet = 6;`

**`common/network/Codec.scala`**:

- Add `encode(o: StorageDeviceSetState)` / `decode(m: codec.StorageDeviceSetState)`
  following the `AllocationGroupState` pattern. `parent` encodes as an optional
  UUID; `memberDevices`/`memberSets`/`assignedPools` encode as repeated UUIDs.
- Extend the existing `StoragePoolState` and `StorageDeviceState` encode/decode to
  carry `storageDeviceSet`. Creators always populate the field; the decoder reads
  the UUID directly (an absent value would decode to a zero UUID, which never
  occurs for objects created through the normal paths).

### 6. Bootstrap wiring

**`server/store/Bootstrap.scala`**:

- Add an `allocateData` helper alongside `allocate` / `allocateTree`. It encodes
  the payload with `ida.encode(bytes)`, calls
  `store.bootstrapAllocate(oid, ObjectType.Data, bootstrapMetadata, DataBuffer(slice))`
  for each store, returns a `DataObjectPointer(oid, Radicle.poolId)`, and records
  the pointer in `allocTreeContent` (same bookkeeping as `allocate`).
- Build the bootstrap `StorageDeviceSetState`:
  - `setId = fixed_ids.BootstrapStorageDeviceSetId`
  - `name = "bootstrap"`
  - `level = 0`
  - `parent = None`
  - `memberDevices = List(bootstrapStorageDeviceState.storageDeviceId)`
  - `memberSets = Nil`
  - `assignedPools = List(PoolId.BootstrapPoolId)`
- Allocate it via `allocateData`, then create
  `storageDeviceSetsTree = allocateTree(ByteArrayKeyOrdering, Key(setId.uuid) -> setPtr.toArray)`
  and add `Radicle.StorageDeviceSetsTreeKey -> storageDeviceSetsTree.encode()` to
  `radicleContent`.
- Set `storageDeviceSet = fixed_ids.BootstrapStorageDeviceSetId` on the bootstrap
  `StoragePoolState` constructed in `Bootstrap.scala`.

**`cmdline/Main.scala`**: set
`storageDeviceSet = fixed_ids.BootstrapStorageDeviceSetId` on the
`bootstrapStorageDevice` `StorageDeviceState` (it is constructed here and passed
into `Bootstrap.initialize`).

### 7. Minimal client API

**`client/internal/SimpleAspenClient.scala`**:

- `private val storageDeviceSetsTree = new MetadataTree(this, radicle, Radicle.StorageDeviceSetsTreeKey)`
- `override def getStorageDeviceSetPointer(id: StorageDeviceSetId): Future[DataObjectPointer] = storageDeviceSetsTree.get(id.uuid).map(_.asInstanceOf[DataObjectPointer])`
- `override def getStorageDeviceSetId(name: String): Future[StorageDeviceSetId] = namespacedRegistry.getRegisteredObject("device-set", name).map(StorageDeviceSetId(_))`
- `override def createStorageDeviceSet(name: String, level: Int, parent: Option[StorageDeviceSetId]): Future[StorageDeviceSetId]` — mirrors
  `createAllocationGroup`: builds the state (empty members/pools), allocates a
  DataObject in the bootstrap pool, `preparePut`s it into the tree, and registers
  the name under the `"device-set"` namespace. Recovers `DuplicateRegistration`
  via `StopRetrying`.

**`client/AspenClient.scala`**:

- `getStorageDeviceSetState(id): Future[StorageDeviceSetState]` (concrete, mirrors
  `getAllocationGroupState`).
- Abstract declarations: `getStorageDeviceSetPointer`, `getStorageDeviceSetId`,
  `createStorageDeviceSet`.
- `createNewStoragePool` gains a required `storageDeviceSet: StorageDeviceSetId`
  parameter, threaded into the constructed `StoragePoolState`. (This method
  currently has no in-tree callers — the cmdline `new-pool` is a WIP stub with
  creation commented out — so the signature change is safe.)

## Testing

- **Bootstrap round-trip:** after bootstrap, the Radicle resolves
  `StorageDeviceSetsTreeKey`; the `"bootstrap"` set decodes with level 0, the
  bootstrap device in `memberDevices`, and `BootstrapPoolId` in `assignedPools`;
  and both the bootstrap pool and device carry `BootstrapStorageDeviceSetId`.
- **Codec round-trips:** `StorageDeviceSetState` encode→decode equality
  (including `parent = None` and `parent = Some(...)` cases), and the augmented
  `StoragePoolState` / `StorageDeviceState`.
- **Client round-trip:** `createStorageDeviceSet` → `getStorageDeviceSetState` and
  `getStorageDeviceSetId`, following the existing allocation-group test style.

## Out of Scope (future work, see `NOTES.txt`)

- Device-set-aware allocation and device selection.
- Mutation/management API (add/remove members, assign/unassign pools, set parent,
  level validation).
- Pool set-reassignment and rebalancing durable tasks.
- Draining a device to change its set membership.
- Inspection / cmdline commands for listing and creating device sets.
