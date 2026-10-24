# Maximum Slice Size Enforcement Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the per-store slice size the configured limit, derive the user-facing maximum object size from it through the IDA, and enforce it on the client, on stores, and in TKVL configuration.

**Architecture:** `maxSliceSize` is stored on pools, allocation groups and (via the pool) in each store's config file. Each `IDA` gains an exact conversion between slice size and object size. The client checks the derived `maxObjectSize` while building a transaction; stores check the real slice length in `RequirementsChecker` and vote abort with a new `SliceSizeExceeded` error. `NodeAllocator` validates every TKVL tier's node size against its allocator.

**Tech Stack:** Scala 3 (quiet syntax, per `CLAUDE.md`), sbt, ScalaTest `AnyFunSuite`, protobuf (`src/main/protobuf/codec.proto`), os-lib.

**Spec:** `docs/superpowers/specs/2026-10-23-max-slice-size-design.md`

## Global Constraints

- The setting is named `maxSliceSize` on pools, allocation groups and backends; protobuf fields are renamed to match. Backwards compatibility is not a concern (no migration, formats may change).
- Pool and group `maxSliceSize` are fixed at creation and never change.
- Users code against `maxObjectSize`. Every public `maxObjectSize` and `maxSliceSize` member needs a doc comment: `maxObjectSize` explains how it is derived from `maxSliceSize` and the IDA; `maxSliceSize` points users to `maxObjectSize` for most uses.
- A store's effective limit is `min(pool maxSliceSize, backend maxSliceSize)` and comes from the store's own config file (`aspen-store-config.yaml`), never from network state or the transaction.
- A transaction that passes the client check must never be aborted by a store for size alone: the client's derived limit is the exact inverse of the store's slice computation (tests are exact to the byte).
- A group's limit is explicit or absent. A pool or group may not join a parent group whose limit is smaller; a member with no limit cannot join a limited group; the rejection happens before any state changes.
- A TKVL tier's `maxNodeSize` may never exceed its allocator's `maxObjectSize`; failure is loud and names the tier, requested size and limit.
- Key-value objects are in scope.
- Prefer indented `if/then/else`; no single-line `if` unless it enhances readability.

## Review Focus

- Limit of exactly the boundary (`maxObjectSize` bytes ok, `+1` rejected) for Replication, ReedSolomon and AppendingReedSolomon (Task 1, 3, 5).
- `maxSliceSize` too small for a pool's IDA to hold any object (e.g. smaller than one AppendingReedSolomon row or its 8-byte trailer): pool creation is rejected, not silently accepted (Task 2).
- Object already at the limit being overwritten with a *smaller* value, and being deleted, must still commit (Task 5).
- A group with a limit and zero members, and a group with no limit holding limited members (Task 2, 6).
- A KV update whose resulting store size exceeds the limit while the client could not know the current size: the store must vote abort (Task 5).

---

### Task 1: IDA slice/object size conversion

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/common/ida/IDA.scala` (trait `IDA` near `calculateEncodedSegmentLength`, plus `Replication`, `ReedSolomon`, `AppendingReedSolomon`)
- Test: `src/test/scala/org/aspen_ddp/aspen/common/ida/IDAMaxObjectSizeSuite.scala`

**Interfaces:**
- Produces on `IDA`: `def maxObjectSize(maxSliceSize: Long): Int` and `def minSliceSize: Int` (smallest `maxSliceSize` that can hold an empty object).
- Contract: `maxObjectSize(s)` is the largest `n` (capped at `Int.MaxValue`; for `AppendingReedSolomon` capped at `Int.MaxValue - rowBytes`, the existing ranged-write bound) with `calculateEncodedSegmentLength(n) <= s`. If `s < minSliceSize`, throws `IllegalArgumentException`.
- Pinned values: Replication `n = s`; ReedSolomon `n = k*s - 1` (since segment = `n/k + 1`); AppendingReedSolomon `n = (s / cellSize) * rowBytes - 8` (trailer is 8 bytes).

- [ ] **Step 1: Write failing tests** in `IDAMaxObjectSizeSuite`: for each of `Replication(3,2)`, `ReedSolomon(5,3,4)`, `AppendingReedSolomon(5,3,4,8)` and a ten-read-threshold `ReedSolomon(14,10,12)`, for slice sizes covering the boundary, assert `ida.calculateEncodedSegmentLength(ida.maxObjectSize(s)) <= s` and `ida.calculateEncodedSegmentLength(ida.maxObjectSize(s) + 1) > s`. Add a test that `ReedSolomon(14,10,12).maxObjectSize(1000) == 9999`, one that `AppendingReedSolomon(5,3,4,8).maxObjectSize(16) == 2*24 - 8`, and one that `maxObjectSize(0)` and (for AppendingReedSolomon) `maxObjectSize(7)` throw `IllegalArgumentException`.
- [ ] **Step 2: Run** `sbt 'testOnly *IDAMaxObjectSizeSuite'`; expect compile failure/FAIL.
- [ ] **Step 3: Implement** the two members on the trait and the three implementations per the contract; doc-comment the derivation formulas.
- [ ] **Step 4: Run** the suite again; expect PASS.
- [ ] **Step 5: Commit** `git commit -m "Add IDA slice size to object size conversion"` (with the attribution trailer).

---

### Task 2: Rename to `maxSliceSize`; expose derived `maxObjectSize`; group limits and membership rule

**Files:**
- Modify: `common/metadata/StoragePoolState.scala`, `common/metadata/AllocationGroupState.scala`, `common/metadata/BootstrapConfig.scala`, `common/network/Codec.scala`, `src/main/protobuf/codec.proto` (fields at ~576, ~594), `client/StoragePool.scala`, `client/ObjectAllocator.scala`, `client/AspenClient.scala` (`createNewStoragePool`, `createAllocationGroup`), `client/internal/BaseAspenClient.scala:292`, `client/internal/pool/SimpleStoragePool.scala`, `client/internal/allocation/PoolObjectAllocator.scala`, `client/internal/allocation/GroupObjectAllocator.scala`, `server/store/Bootstrap.scala`, `cmdline/Main.scala` (~2210 and any pool/group creation commands), plus every `maxObjectSize` call site found by `grep -rn maxObjectSize src`. (All paths under `src/main/scala/org/aspen_ddp/aspen/` unless noted.)
- Test: `CodecRoundTripSuite`, `AllocationGroupStateSuite`, `UpdateAllocationGroupUsageTaskSuite` (update), new cases in `AllocationGroupStateSuite`.

**Interfaces:**
- Consumes: `IDA.maxObjectSize`, `IDA.minSliceSize` (Task 1).
- Produces:
  - `StoragePoolState.maxSliceSize: Option[Int]`; `StoragePool.maxSliceSize: Option[Int]` and `StoragePool.maxObjectSize: Option[Int] = maxSliceSize.map(ida.maxObjectSize(_))`.
  - `AllocationGroupState.maxSliceSize: Option[Int]` (declared at creation); `AllocationGroupState.maximumObjectSize` is kept as the user-facing derived value, defined as the minimum of the members' `Member.maxObjectSize`.
  - `AllocationGroupState.Member(memberType, uuid, maxSliceSize: Option[Int], maxObjectSize: Option[Int], currentUsage, maximumSize)`.
  - `AspenClient.createNewStoragePool(name, ida, maxSliceSize: Option[Int], backendConfig, storageDeviceSet, maximumStoreSize)`; throws `IllegalArgumentException` when `maxSliceSize < ida.minSliceSize`.
  - `AspenClient.createAllocationGroup(groupName: String, level: Int, maxSliceSize: Option[Int] = None)`.
  - `ObjectAllocator.maxObjectSize: Option[Int]` unchanged in type; `PoolObjectAllocator` returns `pool.maxObjectSize`, `GroupObjectAllocator` returns the group's `maximumObjectSize`.
  - New error `MemberLimitExceedsGroup(memberId: UUID, memberLimit: Option[Int], groupLimit: Int)` thrown by `addPool`/`addGroup` before any transaction update.

- [ ] **Step 1: Write failing tests.** In `AllocationGroupStateSuite`: (a) `addPool` of a pool with limit 2000 into a group with limit 1000 fails with `MemberLimitExceedsGroup` and leaves both objects unchanged; (b) pool limit 1000 into group limit 1000 and 2000 succeeds; (c) a pool with no limit into a limited group fails; (d) any pool into an unlimited group succeeds; (e) the same four for `addGroup`; (f) a limited group with no members has `maximumObjectSize == None`; (g) `Member.maxObjectSize` for a `ReedSolomon(14,10,12)` pool with `maxSliceSize = Some(1000)` is `Some(9999)`. In `CodecRoundTripSuite`: round-trip `maxSliceSize` on pool state, group state and members. Add a test that `createNewStoragePool` with `maxSliceSize = Some(1)` on `AppendingReedSolomon` rejects.
- [ ] **Step 2: Run** `sbt 'testOnly *AllocationGroupStateSuite *CodecRoundTripSuite'`; expect FAIL.
- [ ] **Step 3: Rename** the field everywhere via the compiler (start with `StoragePoolState` and `codec.proto`, fix call sites until `sbt compile` and `sbt Test/compile` pass). Add the group's own `maxSliceSize` to `AllocationGroupState` and its proto message; remove the old minimum-over-members slice computation in favor of the member-derived `maximumObjectSize`.
- [ ] **Step 4: Implement the membership rule** in `addPool`/`addGroup`'s `mod` function: compare the candidate's `maxSliceSize` to the parent's before building any transaction update.
- [ ] **Step 5: Write the doc comments** required by Global Constraints on `StoragePool.maxObjectSize`, `StoragePool.maxSliceSize`, `ObjectAllocator.maxObjectSize`, `AllocationGroupState.maxSliceSize` and `StoragePoolState.maxSliceSize`.
- [ ] **Step 6: Run** the full affected suites plus `sbt Test/compile`; expect PASS.
- [ ] **Step 7: Commit** `git commit -m "Rename max object size to max slice size and add group limits"`.

---

### Task 3: Client-side enforcement in TransactionBuilder

**Files:**
- Modify: `client/internal/transaction/TransactionBuilder.scala` (`buildTranaction` signature and the `ObjectSizeLimitExceeded` check at ~172-181), `client/internal/transaction/TransactionImpl.scala:169`, `client/TransactionError.scala:30`, `client/Transaction.scala:35`
- Test: `src/test/scala/org/aspen_ddp/aspen/client/internal/transaction/TransactionRangedWriteSuite.scala` (update) and new `TransactionMaxSizeSuite.scala` in the same directory.

**Interfaces:**
- Consumes: `StoragePool.maxObjectSize` (Task 2).
- Produces: `buildTranaction(opportunisticRebuildManager, poolIDAMap, poolMaxObjectSizeMap: Map[PoolId, Option[Int]])` keeps its shape, but the map now holds the *derived* `maxObjectSize` and is enforced for `overwrite`, `append`, ranged writes and allocation-with-initial-data (anything in `dataObjectUpdates`, `appendUpdates`, `rangedUpdates`). `ObjectSizeLimitExceeded` gains no new fields; its doc says the limit shown is the pool's derived `maxObjectSize`.
- KV note (decided here): the client cannot know a KV object's current stored size for arbitrary `update` calls, so the client checks only KV allocations with initial content (sum of key and value bytes against `maxObjectSize`). Arbitrary KV updates are caught by the store in Task 5.

- [ ] **Step 1: Write failing tests** in `TransactionMaxSizeSuite`: for `Replication`, `ReedSolomon(5,3,4)` and `AppendingReedSolomon(5,3,4,8)` pools with a `maxObjectSize` map entry, assert that an `overwrite` of exactly `maxObjectSize` bytes builds, `maxObjectSize + 1` throws `ObjectSizeLimitExceeded`; likewise `append` growing past the limit; likewise an allocation with initial content. Add a test that a ranged write at the boundary builds and one byte over throws, replacing the old hard-coded `Int.MaxValue - rowBytes` expectation with the IDA-derived value.
- [ ] **Step 2: Run** `sbt 'testOnly *TransactionMaxSizeSuite *TransactionRangedWriteSuite'`; expect FAIL.
- [ ] **Step 3: Implement** a private helper in `TransactionBuilder`, `checkObjectSize(pointer: ObjectPointer, newLength: Long): Unit`, called from the three update loops in `buildTranaction`; remove the separate `idaLimit` block (the IDA cap lives in `IDA.maxObjectSize`). `TransactionImpl` passes `pool.maxObjectSize`.
- [ ] **Step 4: Run** the transaction suites and `TransactionBuilderSuite`; expect PASS.
- [ ] **Step 5: Commit** `git commit -m "Enforce derived max object size for all data updates on the client"`.

---

### Task 4: Store config carries the limit

**Files:**
- Modify: `server/StoreConfig.scala`, `server/Host.scala` (`tryLoadStore` ~396, `createNewStore` ~1144, `LoadStore` handling ~2051), `server/rebuild/RebuildingStore.scala:143`, `cmdline/Main.scala:1388`, `server/store/backend/BackendConfig.scala`, `server/store/Store.scala` (constructor)
- Test: new `src/test/scala/org/aspen_ddp/aspen/server/StoreConfigSuite.scala`; update any `Store`/`Host` test constructors.

**Interfaces:**
- Produces:
  - `StoreConfig(storeId: StoreId, backend: StoreConfig.StorageBackend, poolMaxSliceSize: Option[Int])` with YAML key `pool-max-slice-size`; `StoreConfig.RocksDB(maxSliceSize: Option[Int])` with YAML key `max-slice-size` under `backend`; `StoreConfig.effectiveMaxSliceSize: Option[Int]` = min of the two when both present.
  - `RocksDBConfig(maxSliceSize: Option[Int] = None)` in `BackendConfig.scala`, carried by `StoragePoolState`, so the backend limit is chosen at pool creation.
  - `Store` gains a constructor parameter `maxSliceSize: Option[Long]`, set by `Host` from `storeConfig.effectiveMaxSliceSize` in `tryLoadStore`/`createNewStore` (thread it through the `LoadStore` event).
- Writers of the file (`createNewStore` from pool state, bootstrap in `Main`, `RebuildingStore.runPass` from the pool state it already has) must write the pool limit.

- [ ] **Step 1: Write failing tests** in `StoreConfigSuite`: YAML round trip with both limits, with one, with neither; `effectiveMaxSliceSize` returns the smaller, the only one, or `None`.
- [ ] **Step 2: Run** `sbt 'testOnly *StoreConfigSuite'`; expect FAIL.
- [ ] **Step 3: Implement** the config changes and thread `maxSliceSize` into `Store`; update the three file writers. `Bootstrap`'s pool uses `None`.
- [ ] **Step 4: Run** `sbt Test/compile 'testOnly *StoreConfigSuite *HostObjectCacheSuite *FrontendSuite'`; expect PASS.
- [ ] **Step 5: Commit** `git commit -m "Carry max slice size in the store config file"`.

---

### Task 5: Store-side enforcement in RequirementsChecker

**Files:**
- Modify: `server/store/RequirementsChecker.scala` (`check`, `DataUpdate` and `KeyValueUpdate` cases), `server/store/RequirementsApplyer.scala:33`, `server/transaction/Tx.scala:111,156`, `common/transaction/RequirementError.scala`
- Test: `src/test/scala/org/aspen_ddp/aspen/server/store/RequirementsCheckerSuite.scala` (extend), `RequirementsApplyerSuite.scala` (extend)

**Interfaces:**
- Consumes: `Store.maxSliceSize` (Task 4).
- Produces: `RequirementsChecker.check(transactionId, txTimestamp, requirements, objects, objectUpdates, maxSliceSize: Option[Long] = None)` and the same extra parameter on `RequirementsApplyer.apply`; `case class SliceSizeExceeded(newSize: Long, maxSliceSize: Long) extends RequirementError`.
- Rule: the resulting slice length is the update payload length for `Overwrite`; the slice length after applying the delta for `Append`/`WriteRange` (derive from `AppendDelta` and `state.data`, no backend read); the serialized KV size after applying the operations for `KeyValueUpdate` (use `KVObjectState`'s own size accounting; the builder picks the method). An update whose resulting size is `<= maxSliceSize`, or `<=` the object's *current* size, passes the size check (so shrinking and deleting an oversized object always works). Failure is reported as an object error.

- [ ] **Step 1: Write failing tests** in `RequirementsCheckerSuite`: Overwrite with payload exactly `maxSliceSize` passes, one byte over yields `SliceSizeExceeded` for that object; Append and WriteRange crossing the limit fail; a shrinking overwrite of an object already above the limit passes; a KV insert that pushes serialized size over the limit fails; `maxSliceSize = None` never fails. In `RequirementsApplyerSuite`: a skipped (oversized) object is not applied.
- [ ] **Step 2: Run** `sbt 'testOnly *RequirementsCheckerSuite *RequirementsApplyerSuite'`; expect FAIL.
- [ ] **Step 3: Implement** the size check inside the `DataUpdate` and `KeyValueUpdate` branches; pass `store.maxSliceSize` from `Tx`.
- [ ] **Step 4: Run** the two suites plus `sbt 'testOnly *BasicIntegrationSuite *AppendIntegrationSuite *RangedWriteIntegrationSuite *ReedSolomonIntegrationSuite'`; expect PASS.
- [ ] **Step 5: Add one integration test** to `ReedSolomonIntegrationSuite` (or a new `MaxSliceSizeIntegrationSuite`) that creates a pool with a small `maxSliceSize`, writes `maxObjectSize` bytes successfully, and sees `maxObjectSize + 1` rejected on the client; and a second pool whose backend limit is smaller than the pool limit where a store vote aborts a transaction built with a client that ignores the check (use a raw builder with `poolMaxObjectSizeMap` empty).
- [ ] **Step 6: Commit** `git commit -m "Vote abort in stores when a slice would exceed the max slice size"`.

---

### Task 6: TKVL node size guard

**Files:**
- Modify: `client/tkvl/NodeAllocator.scala` (class and `apply(client, bb)`), `client/tkvl/Root.scala` (`create`, `apply`), `client/tkvl/SinglePoolNodeAllocator.scala`, `client/tkvl/KeyValueListNode.scala:322,423` (extract the per-node overhead into a named constant/function)
- Test: `src/test/scala/org/aspen_ddp/aspen/client/tkvl/NodeAllocatorLimitSuite.scala`

**Interfaces:**
- Consumes: `ObjectAllocator.maxObjectSize` (Task 2).
- Produces: `NodeAllocator.validate(): Future[Unit]` that fails with `NodeSizeExceedsAllocatorLimit(tier: Int, requested: Int, limit: Int)` (new `KeyValueError`) for any tier whose `maxNodeSize` is greater than its allocator's `maxObjectSize` (allocators with `None` always pass). Called by `Root.create`, by `Root.apply` after decoding, and by whoever constructs a `NodeAllocator` from config. A group's limit is fixed and members cannot exceed it, so a passing check stays valid.

- [ ] **Step 1: Write failing tests** in `NodeAllocatorLimitSuite` using a test client with a pool of known `maxObjectSize`: a tier with `maxNodeSize == limit` validates; `limit + 1` fails with `NodeSizeExceedsAllocatorLimit` naming tier 1 when it is the second tier; an unlimited allocator always validates; `Root.create` with a bad node size fails.
- [ ] **Step 2: Run** `sbt 'testOnly *NodeAllocatorLimitSuite'`; expect FAIL.
- [ ] **Step 3: Implement** `validate()` and call it from the listed places. The comparison uses the same overhead notion as `KeyValueListNode`, so a node of `maxNodeSize` bytes really fits.
- [ ] **Step 4: Run** `sbt 'testOnly *tkvl.*'`; expect PASS (fix any existing test whose node size now exceeds its test pool's limit).
- [ ] **Step 5: Commit** `git commit -m "Reject TKVL node sizes above the allocator's max object size"`.

---

### Task 7: Docs and cleanup

**Files:**
- Modify: `UsageGuide.md`, `Architecture.md` (and `CLAUDE.md` only if its text contradicts the new behavior), `Todo.txt`
- Test: none (documentation); run `sbt Test/compile`.

- [ ] **Step 1: Document** in `UsageGuide.md` that users should rely on `maxObjectSize`, how it is derived from `maxSliceSize` for each IDA (the pinned formulas in Task 1), the fixed-at-creation rule, the group membership rule, and the TKVL guard.
- [ ] **Step 2: Delete the first Todo item** (the `maxObjectSize` paragraph) from `Todo.txt`.
- [ ] **Step 3: Run** the full `sbt test`; expect all PASS.
- [ ] **Step 4: Commit** `git commit -m "Document max slice size and remove the completed Todo item"`.
