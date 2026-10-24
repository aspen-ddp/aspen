# Maximum Slice Size Enforcement Design

## Intent

Aspen currently has a pool-level `maxObjectSize` that is almost entirely unenforced. The real constraint is the
size of the *slice* a single store holds, which depends on the pool's IDA: a Replication slice equals the object,
but a ReedSolomon slice is about `size / k`, so a pool with a read threshold of 10 can hold objects roughly ten
times larger than any one store's limit. This work makes the slice size the configured quantity, derives the
maximum object size from it through the IDA, and enforces it so that no transaction is ever started or committed
that would make a store hold an oversized slice.

## Decisions made with the owner

- The configured limit is a **maximum slice size** (bytes per store), not a logical object size. The maximum
  object size is derived from it per IDA (for example `k * maxSliceSize`, minus format overhead, for
  ReedSolomon).

- The pool's slice limit is **fixed at pool creation and can never change.** *(Owner's decision.)* Because it is
  immutable there are no questions about objects already over a lowered limit, no refresh path for cached limits,
  and every store of a pool always votes on the same pool limit.

## Goals

1. A pool carries an optional `maxSliceSize`. The maximum object size of a pool is derived from it and the pool's
   IDA.
2. The client enforces the derived limit for every operation that sets an object's data, before any message is
   sent: `overwrite`, `append`, `rangedWrites`, allocation with initial data, and key-value object updates. Failure is a
   `TransactionCreationError` (the existing `ObjectSizeLimitExceeded`, extended to say which limit was hit).
3. A backend may be configured with its own optional maximum slice size. Stores enforce the effective limit
   `min(pool limit, backend limit)` when checking requirements and vote to abort a transaction whose result would
   exceed it, using a new `RequirementError`.
4. The IDA-specific length bound that already exists for ranged writes (`Int.MaxValue - rowBytes`) is folded into
   the same derivation rather than kept as a separate special case.
5. **Allocation group limits are explicit and fixed at creation** *(Owner's decision.)* A group no longer derives its
   limit as the smallest of its members' limits. If a group has a limit, it is given when the group is created and
   cannot change. A group may also have no limit, in which case it imposes none. No pool or group may be added to
   a parent group whose limit is smaller than the limit of the thing being added (a member with no limit counts as
   unbounded and so cannot join a limited group). Adding a member is rejected with a clear error before any state
   changes. The existing `AllocationGroupState.maximumObjectSize` (minimum over members) goes away; what
   allocators and the TKVL guard see for a group is its own declared limit.
6. **Key-value objects are in scope** *(Owner's decision.)*: their stored slice size is limited like data
   objects, on both the client check and the store check.
7. **TKVL guard** *(Owner's decision.)*: a TKVL (`NodeAllocator` tier: allocator id + `maxNodeSize`) can never be
   configured with a node size that exceeds what its allocator's pool or allocation group allows. Because the pool
   limit is immutable and a group reports the smallest limit of its members, this can be validated once, when the
   `NodeAllocator` / `Root` is created or decoded, for every tier. The check compares `maxNodeSize` against the
   allocator's derived maximum object size (`ObjectAllocator.maxObjectSize`). It must fail loudly, with an
   error naming the tier, the requested node size and the limit, rather than clamp silently. Since a group's limit is fixed at
   creation and members with larger limits cannot be added, a TKVL that passes the check at creation can never
   become invalid later.

## API note for the builder *(Owner's decision)*

`maxSliceSize` is the internal, storage-facing quantity. End users write their code against `maxObjectSize`, so
the public API must make that easy: pools, allocators and allocation groups expose a clear `maxObjectSize`
(derived from `maxSliceSize` and the IDA), and the doc comments must explain exactly how it is derived. The
comments on `maxSliceSize` must be equally clear and point users to `maxObjectSize` for use in most situations.

## Non-goals and anti-goals

- No change to the on-disk format of objects or slices.
- **Backwards compatibility is not a concern** *(Owner's decision.)*: no migration, no support for stores or pools
  created before this change, and no need to keep the old protobuf field or `StoreConfig` format readable.
  Fields may be renamed and formats changed freely.
- Not a failure if enforcement is client-side only for old clients: the store check is the backstop, the client
  check exists so a doomed transaction is never started.
- Anti-goal: a TKVL that can only fail at insert time because its configured node size was never checked.
- Anti-goal: a Reed-Solomon pool silently being limited to one slice's worth of logical bytes, or conversely a
  store silently accepting a slice larger than it can hold.

## Constraints

- Scala 3 quiet syntax and the other rules in `CLAUDE.md`.
- `RequirementsChecker` is where stores decide commit or abort; the oversize vote belongs there. It must work
  from the transaction's update payloads without extra reads where possible. For `Append` and `WriteRange` the
  resulting slice length comes from the delta applied to the current stored slice.
- `StoragePoolState` and the protobuf message carry the field today as `maxObjectSize` (`uint32`, 0 meaning
  none); the same applies to `AllocationGroupState.Member`.
- `BackendConfig` is a sealed class whose only member is `RocksDBConfig()`.
- TKVL node size currently leaves ~16 bytes of per-node overhead (`KeyValueListNode` subtracts it from
  `maxNodeSize`); the guard must compare against the same notion of size the node code uses for a full node, so
  a node of the configured size really fits in one slice for the allocator's IDA.

## Parts left to the builder

- Naming: the setting is `maxSliceSize` everywhere, on pools, allocation groups and backends *(Owner's decision)*. The protobuf field is renamed to match (no compatibility concern).
- The shape of the `StoreConfig` change (and how the backend limit sits beside the pool limit there).
- The exact per-IDA derivation formulas and how padding/trailer bytes are accounted for.
- How a backend's limit is configured and persisted (the shape of the `RocksDBConfig` change, and whether
  `MapBackend` takes one for tests).
- The exact new `RequirementError` case and the error text surfaced to callers.
- How the client learns the effective limit when the pool and backend limits differ.
- Test plan, including Replication, ReedSolomon, and AppendingReedSolomon at, just below, and just above the
  limit, and a store-side abort when the backend limit is smaller than the pool limit.
