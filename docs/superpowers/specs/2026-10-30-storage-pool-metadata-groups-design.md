# Storage Pool Metadata Groups — Design

## Intent

Today every storage pool's metadata — its ErrorTree, its AllocationTree, and the nodes of both — is allocated
from the bootstrap pool. On a large system that concentrates a lot of write traffic and storage on the one pool
that every client must reach to find anything. This change moves per-pool metadata trees out of the bootstrap
pool and into dedicated *metadata pools*, reached through *metadata allocation groups*, while leaving the small
per-pool state object (the thing `storagePoolsTree` points at) in the bootstrap pool next to the Radicle.

The second motivation is missed-update tracking. `MissedUpdateFinalizationAction.markMissedUpdate` currently always
disables missed-update tracking on the transaction that writes an ErrorTree entry, because an ErrorTree write that
itself misses a store would otherwise record another ErrorTree entry, potentially forever. Once metadata lives in a
strict hierarchy (below), only the bootstrap pool's own ErrorTree can loop back on itself, so tracking can stay on
for every other ErrorTree write.

## The layout invariant

Every pool belongs to exactly one tier, and a pool's ErrorTree and AllocationTree (roots and all nodes, including
nodes created later by splits) live only in the tier below:

| Pool kind      | Its metadata trees live in              |
|----------------|-----------------------------------------|
| Ordinary pool  | Metadata pools, via a metadata group    |
| Metadata pool  | The bootstrap pool                      |
| Bootstrap pool | The bootstrap pool (itself)             |

This makes a cross-pool missed-update loop structurally impossible: a chain of missed ErrorTree writes walks strictly
downward (ordinary → metadata → bootstrap) and can only revisit a pool at the bootstrap pool, where the same-pool
rule turns tracking off.

The invariant must hold through every path that can change it, including group membership changes made long after a
pool is created (tree splits allocate from whatever the group contains *at split time*).

## Goals

- Bootstrap creates an initial metadata pool and a default metadata group, and records the group in the Radicle.
- Ordinary pool creation allocates the new pool's ErrorTree and AllocationTree roots, and configures both trees' node
  allocators, from a metadata group: an explicitly supplied one, or the Radicle's default.
- Metadata allocation groups are a first-class, explicit concept, enforced so the layout invariant cannot be broken.
- Metadata pools are created through their own API method and CLI command.
- Missed-update tracking is disabled on an ErrorTree write only when the ErrorTree being written lives in the same
  pool the entry is about (in practice, only the bootstrap pool).

## Non-goals

- Migrating existing systems. Backward compatibility with persisted data is not required.
- Moving the per-pool state object (`StoragePoolState` KV object) out of the bootstrap pool.
- Implementing any allocation strategy that allocates objects (e.g. the FixedSizedIndexFile bitmap in `Todo.txt`).
  This change only establishes that such objects, when they exist, are allocated from the pool's metadata group.
- Relocating an existing pool's metadata trees to a different metadata group.

## Anti-goals

- Any configuration, reachable through the API or CLI, in which a pool's metadata trees can be allocated in that same
  pool (other than the bootstrap pool) or in a cycle of pools. If that becomes possible, the missed-update change is
  unsafe and the work has failed even if every test passes.
- Special-casing the default group in enforcement. The default group is just a metadata group whose UUID the Radicle
  happens to hold; all enforcement is via the metadata-group flag.

## Decided design

### Bootstrap (`server.store.Bootstrap.initialize` and its callers)

- Creates a second pool named `aspen-initial-storage-pool-metadata` alongside the bootstrap pool:
  - Same host, same storage device, same IDA, same backend config (and `maxSliceSize`) as the bootstrap pool.
  - It is a metadata pool: its ErrorTree and AllocationTree roots are allocated in the bootstrap pool and its trees'
    node allocator is the bootstrap pool.
  - Its pool state object lives in the bootstrap pool and is registered in `storagePoolsTree` and in the `pool`
    namespace of the namespaced registry, exactly like the bootstrap pool.
  - It is recorded in the bootstrap device set's `assignedPools` and the bootstrap device's store entries.
- Creates an allocation group named `aspen-default-storage-pool-metadata`, flagged as a metadata group, containing the
  new pool as its only member. It is registered in `allocationGroupsTree` and the group namespace.
- Adds a new Radicle key, `DefaultStoragePoolMetadataAllocator`, whose value is the group's UUID.
- The callers that physically create bootstrap stores (`cmdline/Main.scala` bootstrap, `TestNetwork`) must also create
  the metadata pool's stores on the bootstrap device, so `Bootstrap.initialize` will need backends for both pools.

### Metadata groups

- `AllocationGroupState` gains a metadata-group flag, set at creation and fixed for the group's lifetime (like
  `maxSliceSize` and `idaPolicy`).
- A metadata group accepts as members only:
  - metadata pools, and
  - child groups that are themselves metadata groups.
- A metadata pool may join only metadata groups (decided by the user: no valid use case for it joining an ordinary
  group, several potential problems; the restriction can be lifted later if a need appears).
- `AllocationGroupState.addPool` / `addGroup` refuse anything else with a new, specific error, raised before any state
  changes (same style as `MemberLimitIncompatible` / `MemberIDAMismatch`).
- `createAllocationGroup` (API) and `create-allocation-group` (CLI) gain a way to request a metadata group.

### Metadata pools

- `StoragePoolState` gains a flag marking the pool as a metadata pool, fixed at creation.
- A new API method (alongside `createNewStoragePool`) and a new CLI command create a metadata pool. They take the same
  pool parameters as ordinary pool creation, but no metadata group: a metadata pool's trees always go to the bootstrap
  pool (this is today's `createStoragePool` behavior).

### Ordinary pools

- `createNewStoragePool` and `create-pool` take an optional metadata group UUID (CLI: probably by group name, matching
  the other group commands). When absent, the Radicle's `DefaultStoragePoolMetadataAllocator` is used.
- Naming a group that is not a metadata group is refused before any state changes.
- The pool's ErrorTree root and AllocationTree root are allocated from that group, and both trees' `Root` node
  allocators point at that group (a `GroupObjectAllocatorId`) rather than the bootstrap pool.
- The pool state object stays in the bootstrap pool, as today.
- Any objects a future allocation strategy creates for the pool are allocated from the same group. Nothing creates
  such objects yet; the builder should make the chosen group available where that future code will need it (e.g.
  recorded in the pool state), or note why it isn't needed.

### Missed-update tracking (`MissedUpdateFinalizationAction.markMissedUpdate`)

- Today it calls `tx.disableMissedUpdateTracking()` unconditionally before `pool.errorTree.set(...)`.
- New rule: disable tracking only if the ErrorTree node being modified is in the same pool as the store the entry is
  for (`storeId.poolId`). Under the layout invariant this is true only for the bootstrap pool.
- Note the existing ordering: tracking is disabled before `set` locates the node. The check has to happen after the
  target node is known (or be derived from the pool kind; see "Left to the builder").
- `AspenClient.newRepairTransaction` (repair service's ErrorTree deletes) also disables tracking unconditionally.
  The same reasoning applies to it; the builder should decide whether it adopts the same rule or stays as is, and say
  why.

## Calls I made (not discussed explicitly)

- The default metadata group copies the bootstrap pool's limits: if the bootstrap pool has a `maxSliceSize`, the
  group is `ExactMatch` with the bootstrap IDA and that same `maxSliceSize` (group rules require `ExactMatch` for a
  limit, and an unlimited group would reject a limited member). Otherwise it is unlimited; whether it is then
  `ExactMatch` on the bootstrap IDA or `AllowAll` is the builder's choice.
- Membership is symmetric: a pool or child group may join a group only when its metadata flag equals the group's
  (`MetadataMembershipMismatch`). A metadata group therefore cannot be nested inside an ordinary group, nor the
  reverse.
- The default group's limit follows the bootstrap pool's `effectiveMaxSliceSize` (not the bare `maxSliceSize`):
  `ExactMatch` plus that limit when limited, `AllowAll` otherwise.
- The metadata group's location needs no recording in pool state: the encoded `Root` node allocator of each tree
  already names the group.
- The initial metadata pool's `maximumStoreSize` matches the bootstrap pool's.

## Left to the builder

- How `markMissedUpdate` learns which pool the target ErrorTree node is in: exposing the node from the TKVL `set`
  path, or deriving it from the pool kind (bootstrap → itself; anything else → elsewhere, which the invariant
  guarantees). Prefer whichever is simpler and still obviously correct.
- Field names, codec/protobuf changes for the two new flags and the Radicle key, and error class names.
- Exact CLI command and option names.
- Test coverage. At minimum: bootstrap produces the expected pool/group/Radicle entries; ordinary pools' tree roots
  and split nodes land in the metadata group; every refusal path (non-metadata pool or group into a metadata group,
  ordinary create naming a non-metadata group); missed-update tracking stays on for an ordinary pool's ErrorTree write
  and off for the bootstrap pool's.
- Updating `CLAUDE.md` and `Todo.txt` to reflect the new structure.
