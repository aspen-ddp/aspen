# Design: FSIF allocation strategy (allocated-index bitmap)

**Date:** 2026-10-30
**Status:** Draft

## Intent

FSIF pools (`server.store.backend.fsif`, see `2026-10-29-fsif-backend-design.md`) locate each slice by a 4-byte
external index embedded in the ObjectPointer's `storePointer`. Nothing in production hands out those indices: the
only strategy that does is the test-only `FsifTestIndexStrategy`, which counts upward forever and never reuses an
index. Because of that, `create-pool --backend fsif` is refused (`Main.FsifPoolsEnabled = false`).

This design adds the real strategy. It tracks allocated indices in a per-pool bitmap held in a KeyValue object. It
sets bits in the allocating transaction, and by default clears them in the deleting transaction. Once it exists,
FSIF pools can be created from the CLI.

## Goals

- A production `StoragePoolAllocationStrategy` for FSIF pools whose storePointers satisfy the FSIF contract:
  exactly 4 bytes, a big-endian `Int` `X` with `0 <= X < segmentCount`.
- Indices are reused after deletion, so a pool holds up to `segmentCount` live objects indefinitely.
- An allocation sets its bit atomically with the allocation. If the transaction aborts, nothing leaks.
- By default, a deletion clears its bit atomically with the deletion.
- A transaction can opt out of in-transaction clearing, so a bulk delete (for example removing all of a file's
  blocks in amoebafs) can't be aborted by contention on the bitmap.
- New FSIF pools get the strategy and a zeroed bitmap automatically, and the CLI accepts `--backend fsif`.

## Non-goals

- Caching the bitmap across transactions. Each transaction reads it fresh.
- Changing how any other pool allocates. Ordinary pools keep `StoragePoolAllocationStrategy.Default`.
- Resizing a pool's index space after creation.
- Backward compatibility with persisted data.

## Anti-goals (failure even if it "works")

- Handing out an index that is currently allocated, or one outside `[0, segmentCount)`.
- Leaking an index permanently because an allocation or deletion transaction aborted, or a client crashed
  mid-commit.
- Freeing an index twice, or freeing it while the object still exists.
- Any flaky test.

## Why clearing inside the deleting transaction is safe

A DurableTask-based release was mentioned in earlier notes (the FSIF design's Non-goals, `FsifTestIndexStrategy`'s
comment). Its original rationale was not recalled. Immediate reuse is safe because every FSIF read and `repairDelete`
checks the slice header's ObjectId:

- A store that missed O's delete still holds O at X when X is reallocated to P. P's allocating commit overwrites it
  (FSIF: "allocating at an X that already maps to a different ObjectId overwrites"). `repairDelete(O, X)` is then a
  no-op, and a stale pointer to O reads `ObjectNotFound`.
- Server-side repair deletes (`Frontend.deleteObjectForRepair`) bypass the strategy, so they never touch the bitmap.

The only cost found is contention. A delete's bit lives in a fixed segment, so unlike an allocation it can't dodge
a collision by choosing another segment. A concurrent update to that segment's key aborts the whole deleting
transaction, and the caller retries. That is acceptable for ordinary deletes, and the deferral method below covers
deletes with a high collision risk.

**Builder: keep looking for conflicts.** If design or implementation turns up any reason clearing the bit inside the
deleting transaction is unsafe, beyond contention, stop and raise it rather than working around it.

## Decided design

### Bitmap object

- One KeyValue object per FSIF pool, allocated once at pool creation from the pool's **metadata group**
  (`GroupObjectAllocatorId(metadataGroup)`), the same allocator `BaseAspenClient.createStoragePool` uses for the
  ErrorTree and AllocationTree roots. The group picks whichever member metadata pool it allocates from. There is no
  fixed "metadata pool" per pool; afterward the bitmap is found only through its pointer in the pool state. It is
  never in the FSIF pool itself.
- Keys are segment numbers `0 .. ceil(segmentCount / 1024) - 1`. Each value is a 1024-bit (128-byte) slice of the
  bitmap. Bit `b` of segment `s` is external index `s * 1024 + b`. The last segment's bits at or past `segmentCount`
  are never allocated. Example: 102,400 indices give 100 keys.
- The object is created zeroed (every segment present, all bits clear) as part of creating the pool.
- `segmentCount` is pool-wide: it comes from the pool's `FsifConfig`, and the FSIF design already fixes segment
  geometry per pool, since every store shares one external index space.
- *(Claude's call.)* If the zeroed bitmap would exceed what the metadata group's allocator can hold (its object size limit, as
  `NodeAllocator.nodeSizeLimit` computes for the trees), pool creation fails up
  front with a clear error naming both sizes. The bitmap is never split across objects. At 128 bytes per 1024
  indices that limit is far beyond any realistic `segmentCount`.
- `StoragePoolState` records the strategy's type UUID under `AllocationStrategyKey`. Its config under
  `AllocationStrategyConfigKey` is the bitmap object's encoded pointer, which names the metadata pool it landed in.

### Allocation

- The first allocation from a given FSIF pool in a transaction reads the bitmap object. That read is remembered in
  the transaction (per pool), and later allocations and deletions for that pool in the same transaction reuse it.
  The bitmap is read at most once per pool per transaction.
- The strategy picks a random segment with a free bit and remembers it for the transaction. Later allocations take
  further free bits from that segment, and move to another random segment only once it is full.
- If no segment has a free bit, the allocation fails with a clear error (a new exception naming the pool). *(Claude's
  call.)* The bitmap is not re-read: the transaction's read is fresh, so a "full" result is accurate as of it.
- *(Claude's call.)* An index cleared by a deletion in this transaction is not reallocated in the same transaction.
  The stores would otherwise see a delete and an allocation at the same X in one transaction.
- `createDataObjectPointer` and `createKeyValueObjectPointer` already return `Future`s, so this needs no interface
  change.

### Deletion (default: in the transaction)

- `StoragePoolAllocationStrategy.deleteObject` becomes asynchronous and returns `Future[Unit]`. `TransactionImpl`'s
  commit-hook loop (`runCommitHooks`) waits on it. Its existing contract still holds: it runs after the caller's
  `commit()` and before the build, it may add to the transaction, it must not commit or abort, and a failure aborts
  the transaction with that failure. The loop's guarantee that the caller can't modify the transaction after
  `commit()` must survive the change from synchronous to asynchronous hooks.
- The FSIF strategy reads the bitmap (or reuses this transaction's read) and clears the deleted object's bit, taking
  the index from its storePointer.

### Writing the bitmap back

- Allocations need a pre-build hook too, which doesn't exist today: `runCommitHooks` only notifies deletions. Extend
  it so each strategy with pending work gets one call before the build, after every deletion has been reported.
- In that call the strategy adds exactly one `update` to the bitmap object: an `Insert` per touched segment key with
  the new 128-byte value, guarded by a `KeyValueUpdate.KeyRevision` requirement on that key at the revision read.
  One update per object per transaction is a hard Aspen rule, which is why everything is merged here.
- The hook may add an object in a pool not yet resolved (the metadata pool holding the bitmap).
  `runCommitHooks` already loops on that.
- A collision on any guarded key aborts the transaction like any other requirement failure.

### Deferred release: `Transaction.deferAllocationRelease()`

- A new public method on `Transaction`, applying to every deletion in the transaction. It may be called any time
  before `commit()`.
- The deletion hook receives whether deferral was requested. Ordinary pools ignore it.
- The FSIF strategy, when deferred, doesn't touch the bitmap for deletions. In the deleting transaction it instead
  creates a system DurableTask with `client.prepareSystemDurableTask` (as `UpdateAllocationGroupUsageTask` does), so
  the hosts' `SystemTaskExecutorService` runs it and resumes it after a crash, no matter whether a CLI, amoebafs or a
  host did the delete. *(Claude's call.)* The task records the pool and the indices to free, and clears them in its
  own transaction(s). Its factory is registered with the built-in type registry. Creating the task in the deleting transaction means it exists if and only if the delete committed,
  so no index leaks and none is freed twice. The index is unavailable until the task runs.
- Allocations in a deferring transaction still set their bits in the transaction.

### Pool creation and CLI

- *(Claude's call.)* FSIF is refused for metadata pools (`createNewMetadataPool`, CLI `create-metadata-pool
  --backend fsif`) with a clear error. Metadata pools hold only KeyValue trees, which the FSIF design calls usually a
  mistake. Refusing it also removes the circular case of a bitmap in the bootstrap pool or in its own pool. The
  bootstrap pool already stays RocksDB.

- Creating a pool whose `backendConfig` is `FsifConfig` allocates the zeroed bitmap (sized from
  `FsifConfig.segmentCount`) from the metadata group and records the strategy and config in its `StoragePoolState`,
  in the same flow that creates its ErrorTree and AllocationTree.
- Flip `Main.FsifPoolsEnabled` to `true` and remove the refusal message and the Todo reference in `Main.scala`.
- Remove the item from `Todo.txt`, and update CLAUDE.md's FSIF line (which says `create-pool --backend fsif` is
  refused) and the FSIF design's Non-goal wording about DurableTask-based clearing.

## Left to the builder

- The strategy's names, its type UUID and its registration with the type registry, the free-bit search, and the
  per-transaction state's shape. Strategies are created per `StoragePool` instance, so per-transaction state has to
  be keyed by transaction, or attached to it through a small hook on `Transaction`.
- The exact signature change to the strategy trait: how deferral is passed, and what the pre-build hook is called.
  That includes how `runCommitHooks` finds strategies with pending allocation work. It must also catch a pool whose
  only activity in the transaction is a deletion.
- The DurableTask's state encoding and step split. Keep it simple: one step that clears all its indices is fine if
  it fits. If a collision makes it fail, it retries the step like any `SteppedDurableTask`.
- Whether `FsifTestIndexStrategy` is kept for tests or replaced by the real strategy. Prefer replacing it wherever
  that doesn't slow tests down.
- Test design, following `CLAUDE.local.md`: correctness, then no flakiness, then speed.
