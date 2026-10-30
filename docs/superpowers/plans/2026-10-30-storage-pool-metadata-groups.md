# Storage Pool Metadata Groups Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Move every ordinary pool's ErrorTree and AllocationTree out of the bootstrap pool into metadata pools reached
through explicit metadata allocation groups, and keep missed-update tracking on for every ErrorTree write that cannot
loop.

**Architecture:** Two fixed-at-creation flags (`StoragePoolState.isMetadataPool`, `AllocationGroupState.isMetadataGroup`)
plus a membership rule make a strict layout: ordinary pools → metadata pools → bootstrap pool. Bootstrap creates the
first metadata pool and a default metadata group and records the group in the Radicle. Ordinary pool creation allocates
its tree roots and configures its tree node allocators from a metadata group. `MissedUpdateFinalizationAction` disables
tracking only when the ErrorTree write touches the pool the entry is about.

**Tech Stack:** Scala 3, ScalaPB protobuf (`src/main/protobuf/codec.proto`), ScalaTest `AsyncFunSuite` via
`IntegrationTestSuite` / `TestNetwork`, sbt.

**Spec:** `docs/superpowers/specs/2026-10-30-storage-pool-metadata-groups-design.md`

## Global Constraints

- Pool name `aspen-initial-storage-pool-metadata`; group name `aspen-default-storage-pool-metadata`; Radicle key
  name `DefaultStoragePoolMetadataAllocator`.
- The initial metadata pool uses the bootstrap pool's host, device, IDA, `maxSliceSize`, backend config and
  `maximumStoreSize`.
- The per-pool state object (what `storagePoolsTree` points at) stays in the bootstrap pool for every pool kind.
- Membership is symmetric: a pool or child group may join a group only when its metadata flag equals the group's.
  (Spec said a metadata group may nest in an ordinary group; that is dropped for the same reason the user gave for
  forbidding metadata pools in ordinary groups. The spec is updated in Task 7.)
- No backward compatibility with persisted data. New proto fields simply default to false.
- Scala 3 quiet syntax; indented if/then/else; os-lib for filesystem work.
- Flaky tests are not permitted. Full-suite runs use a 70 s hang timeout, not longer.
- Commit directly on `main`.

## Review Focus

1. **A metadata group with no members.** The default group's only member is removed, then a pool is created or an
   ordinary pool's ErrorTree splits. Expect a clear allocation failure, not a hang. Test in Task 4.
2. **Bootstrap with a slice limit.** With `poolMaxSliceSize` set, the default group must be `ExactMatch` with the
   bootstrap IDA and limit, and an ordinary pool created under it must get a node size within that limit. Test in Task 3
   (group state) and Task 4 (node size).
3. **Retrying the creation of an ordinary pool.** `createStoragePool` retries on collision. The group check runs before
   the transaction, so a retry cannot slip past it, and allocating from a group inside a retried transaction must not
   leak roots. Covered by the existing "fails with KeyAlreadyExists" test continuing to pass with `after should be(before)`.
4. **The ErrorTree write for a metadata pool's own entry.** It lands in bootstrap and must keep tracking *on*. This is
   the one case where the old blanket disable and the new rule differ in a way the layout depends on. Test in Task 5.
5. **Bootstrap pool asked to join a metadata group.** The bootstrap pool is not a metadata pool and must be refused, or
   ordinary pools' trees could land in bootstrap and defeat the change. Test in Task 2.

---

### Task 1: Metadata flags on pool and group state

**Files:**
- Modify: `src/main/protobuf/codec.proto` (`message StoragePoolState`, `message AllocationGroupState`)
- Modify: `src/main/scala/org/aspen_ddp/aspen/common/metadata/StoragePoolState.scala`
- Modify: `src/main/scala/org/aspen_ddp/aspen/common/metadata/AllocationGroupState.scala`
- Modify: `src/main/scala/org/aspen_ddp/aspen/common/network/Codec.scala` (`encode/decode` for both states)
- Test: `src/test/scala/org/aspen_ddp/aspen/common/network/CodecRoundTripSuite.scala`

**Interfaces:**
- Produces: `StoragePoolState.isMetadataPool: Boolean = false` (new last case-class field, after `migration`);
  `AllocationGroupState.isMetadataGroup: Boolean = false` (new last field, after `ida`).
  Proto: `bool isMetadataPool = 14;` and `bool isMetadataGroup = 9;`.

- [ ] **Step 1: Write the failing tests** in `CodecRoundTripSuite`:
  `"StoragePoolState round-trips isMetadataPool"`: build a state with `isMetadataPool = true`, assert
  `StoragePoolState(s.encode()).isMetadataPool shouldBe true`, and that a default-constructed state decodes to `false`.
  `"AllocationGroupState round-trips isMetadataGroup"`: same for `AllocationGroupState(g.toBytes).isMetadataGroup`.
- [ ] **Step 2: Run** `sbt 'testOnly *CodecRoundTripSuite'`. Expected: compile failure (no such field).
- [ ] **Step 3: Add the fields, proto fields and codec mapping.** Scaladoc each field as "fixed when the pool/group is
  created", matching the neighbouring `idaPolicy` docs.
- [ ] **Step 4: Run** `sbt 'testOnly *CodecRoundTripSuite'`. Expected: PASS.
- [ ] **Step 5: Commit** `git commit -m "Add metadata pool and metadata group flags"`.

---

### Task 2: Metadata membership rule, metadata group creation, metadata pool creation

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/common/metadata/AllocationGroupState.scala` (`addPool`, `addGroup`)
- Modify: `src/main/scala/org/aspen_ddp/aspen/client/AspenClient.scala` (`createAllocationGroup`,
  `createNewStoragePool`, new `createNewMetadataPool`, `createStoragePool` signature)
- Modify: `src/main/scala/org/aspen_ddp/aspen/client/internal/BaseAspenClient.scala` (`createAllocationGroup`,
  `createStoragePool`)
- Test: `src/test/scala/org/aspen_ddp/aspen/common/metadata/AllocationGroupStateSuite.scala`

**Interfaces:**
- Consumes: Task 1 flags.
- Produces:
  - `AllocationGroupState.MetadataMembershipMismatch(memberId: UUID, memberIsMetadata: Boolean,
    groupIsMetadata: Boolean) extends Exception`. Thrown by `addPool` (member flag = `ps.isMetadataPool`) and
    `addGroup` (member flag = `child.isMetadataGroup`) when the member's flag differs from the group's flag. It is
    raised before any state changes, through a private `checkMemberMetadata`, in the same place as `checkMemberLimit`.
  - `createAllocationGroup(..., ida: Option[IDA] = None, isMetadataGroup: Boolean = false)`.
  - `def createNewMetadataPool(name: String, ida: IDA, maxSliceSize: Option[Int], backendConfig: BackendConfig,
    storageDeviceSet: StorageDeviceSetId, maximumStoreSize: Long): Future[PoolId]`. Creates a pool with
    `isMetadataPool = true` whose trees live in the bootstrap pool (today's behavior).
  - `protected def createStoragePool(config: StoragePoolState, metadataGroup: Option[AllocationGroupId]): Future[PoolId]`.
    `None` means the trees go to the bootstrap pool. This task always passes `None`, and Task 4 adds the `Some` path.
  - Device selection and validation in `createNewStoragePool` moves into a private helper shared by both public
    create methods. It takes `isMetadataPool` and the optional group.

- [ ] **Step 1: Write the failing tests** in `AllocationGroupStateSuite`, under a `// ---- metadata membership rule ----`
  section, using a helper `createMetadataGroup(name, level)` = `createAllocationGroup(name, level,
  idaPolicy = IDAPolicy.AllowAll, isMetadataGroup = true)` and a helper `createMetadataPool(name)` that calls
  `createNewMetadataPool(name, limitIda, None, RocksDBConfig(), BootstrapStorageDeviceSetId, 0L)`:
  - `"a metadata pool joins a metadata group"`: assert the group's members contain the pool and the pool's
    `allocationGroups` contain the group.
  - `"a metadata pool cannot join an ordinary group"`: assert the failure is a `MetadataMembershipMismatch` with
    `memberIsMetadata = true, groupIsMetadata = false`, and that the pool and group states are unchanged.
  - `"an ordinary pool cannot join a metadata group"`: the mirror image.
  - `"the bootstrap pool cannot join a metadata group"`: uses `Radicle.poolId` and expects `MetadataMembershipMismatch`.
  - `"a metadata group nests only in a metadata group"`: a level-0 metadata group joins a level-1 metadata group, while
    joining a level-1 ordinary group fails with a mismatch.
  - `"an ordinary group cannot nest in a metadata group"`: expects a mismatch.
  - `"createNewMetadataPool records isMetadataPool and keeps its trees in the bootstrap pool"`: read the pool's state
    object and decode the `ErrorTreeKey` and `AllocationTreeKey` values with `Root`. Assert both root pointers have
    `poolId == Radicle.poolId` and the node allocator's first tier is `PoolObjectAllocatorId(Radicle.poolId)`. Also
    assert `getStoragePoolState(id).isMetadataPool shouldBe true`.
- [ ] **Step 2: Run** `sbt 'testOnly *AllocationGroupStateSuite -- -z "metadata"'`. Expected: compile failure.
- [ ] **Step 3: Implement the rule, the `isMetadataGroup` parameter (stored in the new group state), and
  `createNewMetadataPool`.**
- [ ] **Step 4: Run** `sbt 'testOnly *AllocationGroupStateSuite'`. Expected: all PASS, existing tests included.
- [ ] **Step 5: Commit** `git commit -m "Enforce metadata membership; add createNewMetadataPool"`.

---

### Task 3: Bootstrap creates the initial metadata pool and default metadata group

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/common/metadata/fixed_ids.scala`
- Modify: `src/main/scala/org/aspen_ddp/aspen/common/pool/PoolId.scala`
- Modify: `src/main/scala/org/aspen_ddp/aspen/common/allocation_group/AllocationGroupId.scala`
- Modify: `src/main/scala/org/aspen_ddp/aspen/common/Radicle.scala`
- Modify: `src/main/scala/org/aspen_ddp/aspen/server/store/Bootstrap.scala`
- Modify: `src/main/scala/org/aspen_ddp/aspen/cmdline/Main.scala` (bootstrap command, around lines 1560-1620)
- Modify: `src/test/scala/org/aspen_ddp/aspen/TestNetwork.scala`
- Modify: `src/main/scala/org/aspen_ddp/aspen/client/AspenClient.scala` (new `getDefaultMetadataGroupId`)
- Create: `src/test/scala/org/aspen_ddp/aspen/server/store/BootstrapMetadataSuite.scala`

**Interfaces:**
- Consumes: Task 1 flags.
- Produces:
  - `fixed_ids.InitialMetadataPoolId: PoolId = PoolId(new UUID(0, 5))` and
    `fixed_ids.DefaultMetadataGroupId: AllocationGroupId = AllocationGroupId(new UUID(0, 6))`. They're fixed because
    the CLI must create the pool's store directories before `Bootstrap.initialize` runs.
  - `PoolId.InitialMetadataPoolName = "aspen-initial-storage-pool-metadata"`, `PoolId.InitialMetadataPoolId`.
  - `object AllocationGroupId` with `DefaultMetadataGroupName = "aspen-default-storage-pool-metadata"` and
    `DefaultMetadataGroupId`.
  - `Radicle.DefaultStoragePoolMetadataAllocatorKey = Key(Array[Byte](12))`, with the value `uuid2byte(groupUUID)`.
  - `AspenClient.getDefaultMetadataGroupId(): Future[AllocationGroupId]`. Reads the Radicle key and fails with
    `IllegalStateException` if it's absent.
  - `TestNetwork.metadataStores: List[MapBackend]`: one per IDA element, `StoreId(InitialMetadataPoolId, i)`, loaded
    into `smgr` like `stores`.

Bootstrap behavior:
- `Bootstrap.initialize` signature is unchanged. The metadata pool's stores start empty, so bootstrap writes nothing to
  them. It `require`s that `bootstrapStorageDeviceState.stores` contains every `StoreId(InitialMetadataPoolId, i)` for
  `i < ida.width`, because each caller lists them.
- The metadata pool state object has `isMetadataPool = true`, `allocationGroups = List(DefaultMetadataGroupId.uuid)`,
  the same store entries, IDA, `maxSliceSize`, `RocksDBConfig(backendMaxSliceSize)` and
  `StorageDeviceSetId.BootstrapStorageDeviceSetId` as the bootstrap pool, and `maximumStoreSize` 0 (matches bootstrap).
  Its ErrorTree and AllocationTree roots are `allocate()`d in bootstrap with `bootstrapNodeAllocator`.
- The group state is a DataObject via `allocateData`, with `level = 0`, `isMetadataGroup = true`, and members
  `List(Member(Pool, InitialMetadataPoolId.uuid, 0, 0))`. If the bootstrap pool has `maxSliceSize` set: `ExactMatch`,
  `ida = Some(ida)`, the same `maxSliceSize`. Otherwise `AllowAll`, `ida = None`, `maxSliceSize = None`.
- The pool goes in `storagePoolsTree` and in namespace `"pool"`. The group goes in `allocationGroupsTree` (previously
  empty) and in namespace `"group"`. The bootstrap device set's `assignedPools` includes both pools.
- CLI bootstrap creates `bootstrapDevDir / StoreId(InitialMetadataPoolId, i).toString` with a `StoreConfig` yaml,
  exactly like the bootstrap stores. It adds them to the device state as `Active` but does not open backends for them.

- [ ] **Step 1: Write the failing tests** in `BootstrapMetadataSuite extends IntegrationTestSuite`:
  - `"bootstrap registers the initial metadata pool"`: assert `getStoragePoolId(InitialMetadataPoolName) ==
    InitialMetadataPoolId`, the state has `isMetadataPool`, the IDA equals `net.ida`, every store entry is on
    `BootstrapStorageDeviceId`, and `allocationGroups == List(DefaultMetadataGroupId.uuid)`.
  - `"bootstrap registers the default metadata group in the Radicle"`: assert `getDefaultMetadataGroupId() ==
    DefaultMetadataGroupId`, `getAllocationGroupId(DefaultMetadataGroupName) == DefaultMetadataGroupId`, the group state
    has `isMetadataGroup`, and the members' UUIDs are `List(InitialMetadataPoolId.uuid)`.
  - `"the initial metadata pool's trees live in the bootstrap pool"`: same assertion as Task 2's metadata-pool test.
  - `"the bootstrap device set lists the initial metadata pool"`.
  - A second suite class in the same file overrides `testPoolMaxSliceSize = Some(4096)` with an IDA whose
    `minSliceSize <= 4096`. Test `"a limited bootstrap makes the default group ExactMatch with the same limit"`: assert
    `idaPolicy == ExactMatch`, `ida == Some(net.ida)`, `maxSliceSize == Some(4096)`.
- [ ] **Step 2: Run** `sbt 'testOnly *BootstrapMetadata*'`. Expected: compile failure.
- [ ] **Step 3: Implement the ids, the Radicle key, the Bootstrap changes, `getDefaultMetadataGroupId`, the
  `TestNetwork.metadataStores` (added to `bootstrapSD.stores` and loaded into `smgr`), and the CLI bootstrap store
  dirs.**
- [ ] **Step 4: Run** `sbt 'testOnly *BootstrapMetadata*'`. Expected: PASS.
- [ ] **Step 5: Run the full suite** `timeout 70 sbt test` (repeat with a fresh timeout if the run is cut short only by
  load). Expected: tests fail only where they assert the old bootstrap layout, such as store counts on the bootstrap
  device, `assignedPools` equality, empty group lists, or rebalancing/migration/device-shutdown expectations that count
  bootstrap stores. Fix each by updating the expectation to the new layout, or by excluding the metadata pool where the
  test means "pools I created". Don't weaken an assertion. Record each such change in the commit message body.
- [ ] **Step 6: Commit** `git commit -m "Bootstrap creates initial metadata pool and default metadata group"`.

---

### Task 4: Ordinary pools allocate their trees from a metadata group

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/client/AspenClient.scala` (`createNewStoragePool`, new error)
- Modify: `src/main/scala/org/aspen_ddp/aspen/client/internal/BaseAspenClient.scala` (`createStoragePool`)
- Test: `src/test/scala/org/aspen_ddp/aspen/common/metadata/StorageDeviceSetIntegrationSuite.scala` (pool creation
  tests already live here)

**Interfaces:**
- Consumes: Task 2 `createStoragePool(config, metadataGroup)` and the private helper. Task 3
  `getDefaultMetadataGroupId()` and the fixed ids.
- Produces:
  - `createNewStoragePool(..., maximumStoreSize: Long, metadataGroup: Option[AllocationGroupId] = None)`. `None`
    resolves to `getDefaultMetadataGroupId()`.
  - `AspenClient.NotAMetadataGroup(groupId: AllocationGroupId) extends Exception`. It's thrown before any transaction
    when the resolved group's state has `isMetadataGroup == false`. An unknown group fails with the existing
    `getAllocationGroupState` error.
  - In `createStoragePool` with `Some(g)`: the ErrorTree and AllocationTree roots come from
    `getAllocator(GroupObjectAllocatorId(g))`. Both `Root`s use
    `new NodeAllocator(this, List((GroupObjectAllocatorId(g), NodeAllocator.fittingNodeSize(SinglePoolNodeAllocator.DefaultMaxNodeSize, NodeAllocator.nodeSizeLimit(groupAllocator, TreeKind.KeyValueObjects)))))`.
    The pool state object is still allocated from the bootstrap pool's allocator.

- [ ] **Step 1: Write the failing tests:**
  - `"createNewStoragePool puts the trees in the default metadata group"`: decode the new pool's ErrorTree and
    AllocationTree `Root`s. Assert both root pointers have `poolId == InitialMetadataPoolId`, the node allocator's first
    tier id is `GroupObjectAllocatorId(DefaultMetadataGroupId)`, and the pool state object's pointer
    (`getStoragePoolPointer`) is in `Radicle.poolId`.
  - `"createNewStoragePool uses an explicit metadata group"`: create a metadata pool and a metadata group containing
    only it, then create an ordinary pool naming that group. Assert the roots land in that metadata pool.
  - `"createNewStoragePool refuses a non-metadata group"`: expect `NotAMetadataGroup`, and `listStoragePools()` is
    unchanged.
  - `"an ordinary pool's ErrorTree splits within the metadata group"`: write enough entries through
    `pool.errorTree.set` with a tx that has tracking disabled to force a split. Follow the existing TKVL split tests'
    approach to triggering one. Then assert every node pointer reachable from the ErrorTree root is in
    `InitialMetadataPoolId`.
  - `"createNewStoragePool fails clearly when the metadata group is empty"`: remove the initial metadata pool from the
    default group via `AllocationGroupState.removePool`, then assert `createNewStoragePool` fails (not hangs) and
    `listStoragePools()` is unchanged.
  - In the Task 3 limited-bootstrap suite: `"an ordinary pool's tree node size fits the group limit"`. Assert the
    decoded ErrorTree node allocator's `getMaxNodeSize(0) <= NodeAllocator.nodeSizeLimit(net.ida, Some(4096)).get`.
- [ ] **Step 2: Run** `sbt 'testOnly *StorageDeviceSetIntegrationSuite *BootstrapMetadata*'`. Expected: the new tests
  FAIL.
- [ ] **Step 3: Implement.** If the empty-group test shows a hang, make the group allocator fail with an
  `AllocationError` when it has no member to select, rather than special-casing pool creation.
- [ ] **Step 4: Run** the same command, then the full suite with `timeout 70 sbt test`. Expected: PASS.
- [ ] **Step 5: Commit** `git commit -m "Ordinary pools allocate metadata trees from a metadata group"`.

---

### Task 5: Missed-update tracking disabled only for same-pool ErrorTree writes

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/client/Transaction.scala`
- Modify: `src/main/scala/org/aspen_ddp/aspen/client/internal/transaction/TransactionImpl.scala`
- Modify: `src/main/scala/org/aspen_ddp/aspen/client/internal/transaction/MissedUpdateFinalizationAction.scala`
- Test: `src/test/scala/org/aspen_ddp/aspen/client/internal/transaction/MissedUpdateFinalizationActionSuite.scala`

**Interfaces:**
- Consumes: Task 3 fixed ids, Task 4 ordinary pool creation.
- Produces:
  - `Transaction.referencedPoolIds: Set[PoolId]` (`private[aspen]`). `TransactionImpl` delegates to
    `TransactionBuilder.allReferencedPoolIds` under `modify`.
  - `MissedUpdateFinalizationAction.limitTracking(entryPool: PoolId, tx: Transaction): Unit`. It calls
    `tx.disableMissedUpdateTracking()` iff `tx.referencedPoolIds.contains(entryPool)`. `markMissedUpdate` stops
    disabling tracking up front and calls `limitTracking(storeId.poolId, tx)` after `pool.errorTree.set(...)` and before
    `tx.commit()`.
  - Update the Scaladoc on `Transaction.disableMissedUpdateTracking` to describe the same-pool rule.
  - `AspenClient.newRepairTransaction` is unchanged. The repair service's deletes stay untracked: a missed delete
    leaves only a stale ErrorTree replica, and changing it is outside this spec's goal. `NewRepairTransactionSuite`
    keeps pinning that.

- [ ] **Step 1: Write the failing tests.** Each one stages `pool.errorTree.set(key, value)(using tx)` on a fresh
  `client.newTransaction()`, calls `limitTracking(poolId, tx)`, asserts on `tx.missedUpdateTrackingEnabled`, then aborts
  the tx.
  - `"an ErrorTree write for the bootstrap pool disables tracking"`: `Radicle.poolId` → `false`.
  - `"an ErrorTree write for the initial metadata pool keeps tracking"`: `InitialMetadataPoolId` → `true`. Its tree is
    in bootstrap.
  - `"an ErrorTree write for an ordinary pool keeps tracking"`: a pool from `createNewStoragePool` → `true`.
  - Existing tests in the suite keep passing unchanged.
- [ ] **Step 2: Run** `sbt 'testOnly *MissedUpdateFinalizationActionSuite'`. Expected: compile failure.
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run** the suite, then the full suite with `timeout 70 sbt test`. Expected: PASS with no new hangs. The
  extra tracked writes are bounded by the layout, so a hang here means a loop and is a bug, not a timeout to raise.
- [ ] **Step 5: Commit** `git commit -m "Disable missed-update tracking only for same-pool ErrorTree writes"`.

---

### Task 6: CLI

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/cmdline/Main.scala` (`createPoolArgs`, `createAllocationGroupArgs`,
  `Commands`, dispatch, `create_pool`, `create_allocation_group`, `formatAllocationGroupState`, pool display)
- Test: `src/test/scala/org/aspen_ddp/aspen/cmdline/MainSuite.scala`

**Interfaces:**
- Consumes: Task 2 `createNewMetadataPool` and `isMetadataGroup`. Task 4 `metadataGroup` parameter and
  `NotAMetadataGroup`.
- Produces:
  - `create-allocation-group ... --metadata`: creates a metadata group.
  - `create-pool ... --metadata-group <group-name>`: resolved by `getAllocationGroupId`. When omitted, the default is
    used. `NotAMetadataGroup` prints `Error: allocation group '<name>' is not a metadata group`, and an unknown name
    prints `Error: allocation group '<name>' not found`.
  - `create-metadata-pool`: the same arguments as `create-pool` minus `--metadata-group`. Calls `createNewMetadataPool`.
  - `formatAllocationGroupState` prints a `Metadata group: yes|no` line, and the pool display prints
    `Metadata pool: yes|no`.

- [ ] **Step 1: Write the failing tests** in `MainSuite`, following its existing parse and format tests:
  - The parser accepts `create-metadata-pool` with `create-pool`'s positional args and rejects `--metadata-group` on it.
  - `create-pool --metadata-group gold` stores `gold` in the parsed config.
  - `create-allocation-group --metadata` sets the flag.
  - `formatAllocationGroupState` output contains `Metadata group: yes` for a flagged state and `Metadata group: no`
    otherwise.
  - If `MainSuite` already has end-to-end command tests against a bootstrapped host, add one that runs
    `create-metadata-pool` followed by `create-pool --metadata-group` on a new metadata group and checks the pool's roots
    as in Task 4.
- [ ] **Step 2: Run** `sbt 'testOnly *MainSuite'`. Expected: FAIL.
- [ ] **Step 3: Implement.** `--metadata-group` takes a group name, matching the other group commands.
- [ ] **Step 4: Run** `sbt 'testOnly *MainSuite'`. Expected: PASS.
- [ ] **Step 5: Commit** `git commit -m "CLI: metadata groups and metadata pools"`.

---

### Task 7: Documentation

**Files:**
- Modify: `CLAUDE.md`. Under AllocationGroups or StoragePools, add the three-tier layout, the symmetric membership
  rule, the default group in the Radicle, and the same-pool missed-update rule.
- Modify: `Todo.txt`. Remove the "StoragePool metadata management changes" item.
- Modify: `docs/superpowers/specs/2026-10-30-storage-pool-metadata-groups-design.md`. Replace the "A metadata group may
  be nested inside an ordinary group" call with the symmetric rule, and note that metadata-pool tree locations need no
  recording in pool state, because the encoded `Root` node allocator already names the group.

- [ ] **Step 1: Make the edits.**
- [ ] **Step 2: Run the full suite** with `timeout 70 sbt test`. Expected: PASS.
- [ ] **Step 3: Commit** `git commit -m "Document storage pool metadata groups"`.
