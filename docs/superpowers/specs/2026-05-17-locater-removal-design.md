# Locater Removal Design

## Goal

Delete the `Locater` type alias (currently `type Locater = ObjectId`) and all associated indirection. Replace with `ObjectPointer` so that the full pointer — including the `storePointer` field — is available to backends and frontends that may need it.

## Background

The recent ObjectPointer refactor stripped ObjectPointer down to `(id: ObjectId, poolId: PoolId, storePointer: Array[Byte])`. The old `Locater` type previously carried per-store addressing info extracted from per-store `StorePointer` instances within the ObjectPointer. Now that ObjectPointer is minimal, `Locater` as a separate concept is unnecessary. The full ObjectPointer can be passed directly.

## Design Decisions

- **Delete `hostedObjectLocaters`**: The method on `TransactionDescription` is redundant with `allHostedObjects`, which already returns `List[ObjectPointer]`. All callers switch to `allHostedObjects`.
- **Backend.read() takes ObjectPointer**: Backends receive the full pointer. They extract `.id` internally for storage key lookups but have access to `storePointer` if needed in the future.
- **Tx constructor takes List[ObjectPointer]**: Direct replacement of `List[Locater]`.
- **Cache/map keys stay as ObjectId**: `Frontend.pendingReads` and `objectCache` remain keyed by `ObjectId`. Methods receive `ObjectPointer` and extract `.id` at the lookup site. This avoids issues with `ObjectPointer.equals` including `storePointer` in comparisons.

## Changes

### 1. Delete Locater.scala

Delete `src/main/scala/org/aspen_ddp/aspen/server/store/Locater.scala`.

### 2. TransactionDescription

- Delete `hostedObjectLocaters(storeId: StoreId): List[Locater]`.
- Remove the `Locater` import.
- All callers switch to `allHostedObjects(storeId: StoreId): List[ObjectPointer]`.

### 3. Backend trait and implementations

**Backend.scala:**
- `read(locater: Locater)` becomes `read(pointer: ObjectPointer)`.
- Import changes from `Locater` to `ObjectPointer`.

**MapBackend.scala:**
- `read(pointer: ObjectPointer)`: extract `pointer.id` for map lookup. Pass `pointer.id` to `Read` completion and `ReadState`.
- Remove `Locater` import, add `ObjectPointer` import.

**RocksDBBackend.scala:**
- `read(pointer: ObjectPointer)`: extract `pointer.id` for `tokey()`, `allocating.get()`, log messages, and completion objects.
- Remove `Locater` import, add `ObjectPointer` import.

### 4. Frontend.scala

- Remove `Locater` import.
- Add `ObjectPointer` import (already present via other paths).
- `readObjectForNetwork(clientId, readUUID, locater: Locater)` becomes `readObjectForNetwork(clientId, readUUID, pointer: ObjectPointer)`. Internally extracts `pointer.id` for `objectCache.get()` and `pendingReads` lookups. Passes full pointer to `backend.read()`.
- `readObjectForTransaction(transaction, locater: Locater)` becomes `readObjectForTransaction(transaction, pointer: ObjectPointer)`. Same pattern.
- `readObjectForOpportunisticRebuild`: the local `val locater: Locater = op.pointer.id` is removed. Uses `op.pointer.id` for map lookups and passes `op.pointer` to `backend.read()`.
- `readObjectForRepair`: same pattern — remove local `locater` variable, use `current.pointer.id` for map lookups, pass `current.pointer` to `backend.read()`.

### 5. Tx.scala

- Constructor parameter changes from `objectLocaters: List[Locater]` to `objectPointers: List[ObjectPointer]`.
- `pendingObjectLoads` initialized from `objectPointers.size`.
- Remove `Locater` from import.
- Add `ObjectPointer` to imports if not already present.

### 6. StoreManager.scala

- Line 661: `store.frontend.readObjectForNetwork(r.fromClient, r.readUUID, r.objectPointer.id)` becomes `store.frontend.readObjectForNetwork(r.fromClient, r.readUUID, r.objectPointer)` — stop extracting `.id`.

### 7. Frontend callers (recovery and prepare paths)

- Lines 54-57: `val locaters = txd.hostedObjectLocaters(storeId)` becomes `val pointers = txd.allHostedObjects(storeId)`. Pass `pointers` to `Tx` constructor and iterate with `pointers.foreach(ptr => readObjectForTransaction(tx, ptr))`.
- Lines 107-111: Same pattern for the `receivePrepare` path.

## Files Modified

| File | Action |
|------|--------|
| `server/store/Locater.scala` | Delete |
| `common/transaction/TransactionDescription.scala` | Remove `hostedObjectLocaters`, remove `Locater` import |
| `server/store/backend/Backend.scala` | Change `read()` signature |
| `server/store/backend/MapBackend.scala` | Update `read()` implementation |
| `server/store/backend/RocksDBBackend.scala` | Update `read()` implementation |
| `server/store/Frontend.scala` | Update all `readObjectFor*` methods |
| `server/transaction/Tx.scala` | Update constructor parameter |
| `server/StoreManager.scala` | Stop extracting `.id` at call site |

## Test Impact

No test files reference `Locater` directly. Tests that construct `Tx` or call `Frontend`/`Backend` methods will need updated parameter types, but the test suites were already updated for the ObjectPointer refactor and may not directly instantiate these server-side classes.
