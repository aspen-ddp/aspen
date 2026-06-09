# StorageDeviceSetState.selectDeviceForRebuild — Design

Date: 2026-06-09

## Goal

Add a method to `StorageDeviceSetState` that selects another storage device in the
set to host a store which must be **rebuilt from scratch** because its data was lost:

```scala
def selectDeviceForRebuild(
    poolId: PoolId,
    failedIndex: Byte,
    client: AspenClient,
    rng: Random = new Random()
): Future[StorageDeviceId]
```

The chosen device must have **sufficient free space** to rebuild the store. If no
device with sufficient free space is found, the result fails with `AllocationError`.

Selection is **optimistic**: rather than reading the `StorageDeviceState` of every
member device up front (sets may be large), candidates are tried one at a time in
random order, reading each device's state lazily and stopping at the first that fits.

This method is for **tier-0 (level-0) sets only**. On a level-1+ set it fails with
`AllocationError`.

## Signature note

The user-stated signature was `selectDeviceForRebuild(poolId, failedIndex): Future[StorageDeviceId]`.
The design adds `client: AspenClient` and `rng: Random = new Random()`, because the
method must read the pool object and device states, and tests require deterministic
randomness. This matches the sibling `selectDevicesForPool(numStores, client, rng)`.

## Relevant existing code

- `src/main/scala/org/aspen_ddp/aspen/common/metadata/StorageDeviceSetState.scala`
  - Case class fields: `setId, name, level, parent, memberDevices, memberSets, assignedPools`.
  - `level == 0` ⇒ set of devices (`memberDevices` populated). `level >= 1` ⇒ set of sets.
  - Existing `selectDevicesForPool` (public, takes `client` + `rng`) delegates to a
    `private[metadata] selectDevices(...)` core that depends only on a narrow `lookup`
    function so it is unit-testable without a real `AspenClient`. This design follows
    the same public-method + testable-core split.
- `src/main/scala/org/aspen_ddp/aspen/common/metadata/StoragePoolState.scala`
  - `stores: Array[StoreEntry]` indexed by `poolIndex` (the `Byte`); `StoreEntry(hostId, storageDeviceId)`.
  - `getStoreUsageKey(poolIndex: Byte): Key = Key(s"sz${poolIndex}")` — per-store size lives
    in the pool's KV object under this key, encoded via `long2byte`.
  - `apply(kvos: KeyValueObjectState)` decodes the config from `ConfigKey`.
- `src/main/scala/org/aspen_ddp/aspen/common/metadata/StorageDeviceState.scala`
  - `currentUsage: Long`, `totalSize: Long`. Free space = `totalSize - currentUsage`.
- `src/main/scala/org/aspen_ddp/aspen/client/AspenClient.scala`
  - `getStoragePoolPointer(poolId): Future[KeyValueObjectPointer]` + `read(ptr)` → raw KVOS.
  - `getStorageDeviceState(id): Future[StorageDeviceState]`.
- `src/main/scala/org/aspen_ddp/aspen/client/AllocationError.scala`
  - `class AllocationError(message: String) extends Throwable(message)`.
- `src/main/scala/org/aspen_ddp/aspen/common/util/package.scala`
  - `byte2long`, `long2byte`.
- Test template: `src/test/scala/org/aspen_ddp/aspen/common/metadata/StorageDeviceSetSelectionSuite.scala`
  (helpers `dev()`, `leaf(...)`, `noLookup`).

## Decisions (resolved during brainstorming)

1. **Failed device — hard-excluded.** The device at `poolState.stores(failedIndex)` is
   never selected, even as a last resort. "Select another device" is taken literally.
2. **Other pool devices — soft-excluded.** Devices already hosting a store in this pool
   are avoided when possible, but eligible as a fallback if no other device fits.
3. **Required size = current per-store usage; missing record ⇒ 0.** Read
   `getStoreUsageKey(failedIndex)` from the pool KVOS; `byte2long` it. If the key is
   absent, the required size is `0L` (any device with free space qualifies).
4. **Free space is a hard requirement** in both the preferred and the fallback phase. If
   no candidate device (preferred or fallback) has `free >= requiredSize`, fail with
   `AllocationError`.
5. **Read the pool pointer directly.** Use `getStoragePoolPointer` + `read` to obtain the
   raw `KeyValueObjectState`, so we get both the decoded config and the per-store size key
   from a single read.
6. **All failures are `Future.failed(AllocationError(...))`** — never thrown synchronously,
   never a different exception type.

## Design

### Section 1 — Public method (I/O layer)

```scala
def selectDeviceForRebuild(
    poolId: PoolId,
    failedIndex: Byte,
    client: AspenClient,
    rng: Random = new Random()
): Future[StorageDeviceId]
```

Steps:
1. `given ExecutionContext = client.clientContext`.
2. **Tier check:** if `level != 0` ⇒ `Future.failed(AllocationError(...))`. No reads occur.
3. **Empty set:** if `memberDevices.isEmpty` ⇒ `Future.failed(AllocationError(...))`.
4. Read the pool directly: `client.getStoragePoolPointer(poolId).flatMap(client.read(_))`
   ⇒ `poolKvos: KeyValueObjectState`.
5. Decode `poolState = StoragePoolState(poolKvos)`.
6. **Index bounds:** if `failedIndex < 0 || failedIndex >= poolState.stores.length`
   ⇒ `Future.failed(AllocationError(...))`.
7. `requiredSize = poolKvos.contents.get(StoragePoolState.getStoreUsageKey(failedIndex))`
   `.map(vs => byte2long(vs.value.bytes)).getOrElse(0L)`.
8. `failedDevice = poolState.stores(failedIndex).storageDeviceId`;
   `poolDevices = poolState.stores.map(_.storageDeviceId).toSet`.
9. Delegate to the core, passing
   `freeSpaceLookup = id => client.getStorageDeviceState(id).map(s => s.totalSize - s.currentUsage)`.

### Section 2 — Testable core

```scala
private[metadata] def selectRebuildDevice(
    requiredSize: Long,
    failedDevice: StorageDeviceId,
    poolDevices: Set[StorageDeviceId],
    freeSpaceLookup: StorageDeviceId => Future[Long],
    rng: Random
)(using ec: ExecutionContext): Future[StorageDeviceId]
```

Logic:
1. Candidate ordering (Approach A):
   - `eligible = memberDevices.filterNot(_ == failedDevice)` — failed device hard-removed.
   - `preferred = rng.shuffle(eligible.filterNot(poolDevices.contains))`.
   - `fallback = rng.shuffle(eligible.filter(poolDevices.contains))`.
   - `candidates = preferred ++ fallback`.
2. If `candidates.isEmpty` ⇒ `Future.failed(AllocationError(...))`.
3. **Lazy sequential scan** via a small recursive helper `scan(remaining: List[StorageDeviceId])`:
   - For the head device, call `freeSpaceLookup(head)`.
   - If `free >= requiredSize` ⇒ succeed with `head` (no further lookups — short-circuit).
   - Otherwise recurse on the tail.
   - If a `freeSpaceLookup` **fails** for a candidate, skip that candidate and continue
     the scan (recover to the tail) rather than aborting the whole operation.
   - When `remaining` is empty ⇒ `Future.failed(AllocationError(...))`.

Reads device states one at a time, stops early. Unit-tested by injecting a
map-backed `freeSpaceLookup` — no `AspenClient` required.

### Section 3 — Error handling

All failures are `Future.failed(AllocationError(msg))` with distinct messages:
- `level != 0` ⇒ `"selectDeviceForRebuild only supports level-0 sets; set <id> is level <n>"`.
- empty `memberDevices` ⇒ existing level-0 phrasing
  (`"StorageDeviceSet <id> (level 0) has no member devices"`).
- `failedIndex` out of range ⇒ `"failedIndex <i> out of range for pool <id> (<n> stores)"`.
- no candidate / none with space ⇒
  `"no device in set <id> has >= <requiredSize> free bytes available for rebuild"`
  (emitted by the core, which does not carry the pool/store context).

## Testing

New cases in `StorageDeviceSetSelectionSuite.scala`, driving the `private[metadata]`
core directly with a map-backed `freeSpaceLookup` (no real client):

1. Picks a non-pool device with sufficient space (preferred over pool devices).
2. Falls back to a soft-excluded pool device when no non-pool device fits.
3. Failed device is never selected, even when it is the only device with space ⇒ `AllocationError`.
4. No device has enough space ⇒ `AllocationError`.
5. `requiredSize == 0` (missing usage record) ⇒ any non-failed device with free space qualifies.
6. Lazy short-circuit: once a fitting device is found, later candidates' `freeSpaceLookup`
   is not invoked (assert via a probe recording queried devices).
7. A candidate whose `freeSpaceLookup` fails is skipped; the scan continues to the next.
8. `level != 0` ⇒ `AllocationError` (tier-1+ rejection).
9. Empty `memberDevices` ⇒ `AllocationError`.
10. Deterministic ordering under a fixed `rng` seed.

No integration test of the real client read path (`getStoragePoolPointer` + raw-KVOS
decode) — parity with how `selectDevices` is tested.

## Out of scope

- Actually performing the rebuild (orchestration, transitioning store status to
  `Rebuilding`, transaction work). This method only *selects* the target device.
- Mutating `StorageDeviceState` / `StoragePoolState` to record the chosen device.
- Any change to the StorageDeviceSet management/mutation API.
