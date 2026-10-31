# Virtual Disk Engine Implementation Plan (iSCSI part 1 of 4)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** An Aspen-backed virtual block device engine (`org.aspen_ddp.aspen.vdisk`): create, list, grow and delete named
disks, take single-writer ownership of one, and serve read/write/flush/FUA/unmap against it with write-back cache
semantics. It also adds the CLI commands that manage disks.

**Architecture:** Each disk has a KeyValue *config object* holding its geometry and the root of a `DataObjectTKVL`
(block index -> block `DataObjectPointer`), and a DataObject *fence* holding the owner epoch. The owner keeps a
per-block in-memory state: committed metadata, dirty segments, and an in-flight commit. Writes complete from memory.
Background writeback commits each block's dirty segments as ranged writes, with blocks in parallel. New blocks and
deletions are batched into serialized *map transactions*. Every commit takes a new **shared** revision lock on the
fence, so a takeover's overwrite of the fence aborts every later commit by the old owner.

**Tech Stack:** Scala 3, Aspen client API, Scaffeine (clean cache), ScalaTest (`AnyFunSuite` for pure code,
`IntegrationTestSuite` for engine tests).

**Spec:** `docs/superpowers/specs/2026-10-30-iscsi-tcmu-target-design.md`

**Later plans:** 2 covers the tcmu-runner bridge (socket protocol, C plugin, daemon command). 3 covers ALUA failover.
4 covers cell-size measurements and the recommendation.

## Deviations from the spec (found while planning)

1. **`RevisionLock` is exclusive in the store.** `RequirementsLocker` sets a single `lockedToTransaction`, so
   concurrent data commits that each lock the descriptor would collide with one another. Task 1 adds
   `SharedRevisionLock`. Any number of transactions can hold it together. Any requirement that would change the
   object (or take an exclusive lock on it) collides with it. The store keeps a best-effort, in-memory
   writer-preference hint so that a takeover is not starved by a steady stream of commits. The fence is a
   DataObject, so a takeover is a plain `overwrite`.
2. **Block object size is rounded down.** `minSliceSizeFor(5,120,000)` gives a `maxObjectSize` slightly above
   5,120,000 because of row rounding (for example 5,120,152 with `cellSize` 16), and that isn't a multiple of
   4096. `blockObjectSize` is therefore the largest multiple of the logical block size that is at most the
   allocator's `maxObjectSize`. That's exactly 5,120,000 for the target pool. Creation fails only when this comes
   out below one logical block.
3. **Map transactions retry.** TKVL finalization actions (`TailRemovalFinalizationAction`, root updates) rewrite
   map nodes in their own transactions, so a map transaction can legitimately abort. A failed map transaction that
   wasn't fenced is retried. Its inserts use `requireDoesNotExist`, so `KeyAlreadyExists` on a retry means an
   earlier attempt committed: the engine re-reads the entry, adopts the pointer, and sends the data through a normal
   data transaction. The strict "revision mismatch means stop serving" rule applies to block objects, which only the
   owner ever writes.

## Global Constraints

- IDA for the target deployment: `RandomAccessReedSolomon` 16/10/12. The engine must work with **any** `cellSize`
  and with any allocator whose `maxObjectSize` is defined.
- Write-back semantics: WRITE completes from memory. FLUSH and FUA complete only after the covered data is
  committed. Read-your-writes always holds.
- Single writer: every transaction that changes block objects or the map carries
  `sharedLockRevision(fencePointer, fenceRevision)`.
- No artificial delays: no global lock is held across Aspen calls, there is never only one transaction in flight
  per disk, and no whole-object rewrite happens for a small write.
- Logical block size is 4096 by default, and 512 is allowed. Disk size and every I/O offset and length must be
  multiples of it.
- Registry namespace for disk names: `"vdisk"`.
- Scala 3 quiet syntax with indented if/then/else. Use `os-lib` for any filesystem work. Scaffeine for caches.
- Tests: no flakiness, as fast as possible. Engine tests use `testIda = RandomAccessReedSolomon(5, 3, 4, 64)` and
  `testPoolMaxSliceSize = Some(ida.minSliceSizeFor(16384).toInt)`, which gives 16 KiB block objects (four 4 KiB
  sectors) on the bootstrap pool.
- Backward compatibility with persisted data is not required. Commit directly on `main`.

## Review Focus

1. Unaligned or zero-length I/O: unaligned is rejected with `InvalidRequest` and touches no state, while
   zero-length completes immediately. Test in Task 5.
2. I/O spanning a block-object boundary: it's split exactly, with nothing lost or duplicated at the seam. Test in
   Task 2 (geometry) and Task 5 (end to end).
3. Out-of-range I/O (offset + length > size): `OutOfRange`, and nothing is staged. Test in Task 5.
4. A read of a block while its first-write map transaction is in flight returns the written bytes, not zeros.
   Test in Task 5.
5. Write, then unmap, then write the same block before any flush: the content afterwards is just the second
   write, with no `KeyAlreadyExists` and no stuck flush. Test in Task 8.

---

### Task 1: `SharedRevisionLock` requirement

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/common/transaction/TransactionRequirement.scala`
- Modify: `src/main/protobuf/codec.proto` (add a `SharedRevisionLock` message plus a new oneof field in
  `TransactionRequirement`)
- Modify: `src/main/scala/org/aspen_ddp/aspen/common/network/Codec.scala`
- Modify: `src/main/scala/org/aspen_ddp/aspen/server/store/ObjectState.scala`
- Modify: `src/main/scala/org/aspen_ddp/aspen/server/store/RequirementsChecker.scala`,
  `RequirementsLocker.scala`, `RequirementsApplyer.scala`
- Modify: `src/main/scala/org/aspen_ddp/aspen/client/Transaction.scala`,
  `client/internal/transaction/TransactionBuilder.scala`, and any other `Transaction` implementations (the compiler
  lists them)
- Test: `src/test/scala/org/aspen_ddp/aspen/server/store/RequirementsCheckerSuite.scala`,
  `src/test/scala/org/aspen_ddp/aspen/SharedRevisionLockIntegrationSuite.scala`

**Interfaces:**
- Produces: `case class SharedRevisionLock(objectPointer: ObjectPointer, requiredRevision: ObjectRevision) extends
  TransactionObjectRequirement`
- Produces: `Transaction.sharedLockRevision(objectPointer: ObjectPointer, requiredRevision: ObjectRevision): Unit`.
  In the builder it throws `ConflictingRequirements` if the same transaction also updates or locks that object, as
  `lockRevision` does.
- Produces (store): `ObjectState.sharedLocks: Set[TransactionId]` and `ObjectState.exclusiveWaitUntil: Long`
  (`System.nanoTime` deadline, 0 = none). `SharedRevisionLock.ExclusivePreferenceWindow = 250.millis`.

Semantics:
- A `SharedRevisionLock` check passes when the revision matches, `lockedToTransaction` is empty or is this
  transaction, and `exclusiveWaitUntil` has passed. Otherwise it throws `TransactionCollision` (on a lock or the
  preference window) or `RevisionMismatch`.
- The exclusive path (`getState` in the checker, which serves every other object requirement) also throws
  `TransactionCollision` when `sharedLocks` holds any other transaction, and in that case sets `exclusiveWaitUntil`
  to now plus the window.
- Locking adds the transaction to `sharedLocks`, and unlocking removes it. The applyer does nothing.
  `lockedWriteTransactions` includes `sharedLocks`.
- Every `match` on `RevisionLock` gets a `SharedRevisionLock` case. The sealed hierarchy makes the compiler find
  them all.

- [ ] **Step 1: Write failing checker tests** in `RequirementsCheckerSuite`, mirroring the existing
  `"RevisionLock okay"` setup:
  - `"SharedRevisionLock okay"`: no errors.
  - `"two SharedRevisionLocks on one object both pass"`: lock tx1, then check tx2 and expect no errors.
  - `"DataUpdate collides with a held SharedRevisionLock"`: lock tx1 shared, check a tx2 `DataUpdate` on the same
    object, and expect `TransactionCollision(tx1)`. Also expect `exclusiveWaitUntil > System.nanoTime()`.
  - `"SharedRevisionLock refused during the exclusive preference window"`: after the previous case, a tx3 shared
    check gets a `TransactionCollision`.
  - `"SharedRevisionLock fails on revision mismatch"`: `RevisionMismatch`.
- [ ] **Step 2:** Run `sbt 'testOnly *RequirementsCheckerSuite -- -z "SharedRevisionLock"'`. Expected: compile
  failure (the type doesn't exist yet).
- [ ] **Step 3: Implement** the requirement, codec and proto fields, store cases, and the client method, following
  the semantics above.
- [ ] **Step 4:** Run the same command. Expected: PASS.
- [ ] **Step 5: Write the integration test** `SharedRevisionLockIntegrationSuite` (an `IntegrationTestSuite`).
  `"concurrent transactions sharing a lock all commit"`: allocate a fence DataObject and 8 data objects, then start
  8 transactions at once, each doing `sharedLockRevision(fence, rev)` plus an `overwrite` of its own object.
  `Future.sequence` must succeed. `"an overwrite of the locked object makes later shared lockers fail"`: overwrite
  the fence, then a transaction that shared-locks the old revision fails its commit.
- [ ] **Step 6:** Run `sbt 'testOnly *SharedRevisionLockIntegrationSuite'`, then the full suite with
  `sbt test`, under a 70 s hang budget. Expected: PASS.
- [ ] **Step 7: Commit:** `git commit -m "Add SharedRevisionLock transaction requirement"`.

### Task 2: Disk geometry

**Files:**
- Create: `src/main/scala/org/aspen_ddp/aspen/vdisk/DiskGeometry.scala`
- Create: `src/main/scala/org/aspen_ddp/aspen/vdisk/VDiskError.scala`
- Test: `src/test/scala/org/aspen_ddp/aspen/vdisk/DiskGeometrySuite.scala`

**Interfaces:**
- Produces: `final case class DiskGeometry(size: Long, logicalBlockSize: Int, blockObjectSize: Int)`, with:
  - `def blockCount: Long`, the number of block objects needed to cover `size`
  - `def segments(offset: Long, length: Int): List[BlockSegment]`
  - `def validate(offset: Long, length: Int): Option[VDiskError]`, which gives `InvalidRequest` when unaligned and
    `OutOfRange` past `size`
  - `def blocksFullyCovered(offset: Long, length: Long): List[Long]`
- Produces: `final case class BlockSegment(blockIndex: Long, offsetInBlock: Int, length: Int, offsetInRequest: Int)`
- Produces: `object DiskGeometry { def blockObjectSizeFor(maxObjectSize: Int, logicalBlockSize: Int): Option[Int] }`.
  It returns the largest multiple of `logicalBlockSize` that is at most `maxObjectSize`, or `None` if that is below
  `logicalBlockSize`.
- Produces: `sealed abstract class VDiskError(msg: String) extends Exception(msg)`, with these cases:
  `InvalidRequest`, `OutOfRange`, `Fenced`, `DiskFailed(reason: String)`, `DiskNotFound(name)`,
  `DiskAlreadyExists(name)`, `InvalidGeometry(reason)`. They go in `vdisk/VDiskError.scala`.

- [ ] **Step 1: Write failing tests:**
  - `blockObjectSizeFor(5120152, 4096) == Some(5120000)`
  - `blockObjectSizeFor(5120000, 4096) == Some(5120000)`
  - `blockObjectSizeFor(4095, 4096) == None`
  - `blockObjectSizeFor(16384 + 50, 512) == Some(16384)`
  - With `g = DiskGeometry(65536, 4096, 16384)`:
    - `g.segments(12288, 8192) == List(BlockSegment(0, 12288, 4096, 0), BlockSegment(1, 0, 4096, 4096))`
    - `g.validate(100, 4096)` is `InvalidRequest`
    - `g.validate(61440, 8192)` is `OutOfRange`
    - `g.validate(0, 0) == None`
    - `g.blocksFullyCovered(8192, 32768) == List(1L)`
  - A randomized test: for 1,000 random aligned in-range requests, the segment lengths sum to the request length,
    the `offsetInRequest` values are contiguous, and every segment stays inside its block.
- [ ] **Step 2:** Run `sbt 'testOnly *DiskGeometrySuite'`. Expected: FAIL (does not compile).
- [ ] **Step 3: Implement** `DiskGeometry`, `BlockSegment`, `VDiskError`.
- [ ] **Step 4:** Run it again. Expected: PASS.
- [ ] **Step 5: Commit:** `git commit -m "vdisk: disk geometry and errors"`.

### Task 3: Segment map (dirty and in-flight data)

**Files:**
- Create: `src/main/scala/org/aspen_ddp/aspen/vdisk/SegmentMap.scala`
- Test: `src/test/scala/org/aspen_ddp/aspen/vdisk/SegmentMapSuite.scala`

**Interfaces:**
- Produces: `final class SegmentMap` (mutable, not thread-safe; the owner serializes access). It holds
  non-overlapping `(offset, bytes, seq)` pieces within one block.
  - `def put(offset: Int, data: Array[Byte], seq: Long): Unit`. Newer data replaces overlapped parts of older
    pieces, which are split as needed.
  - `def snapshot: SegmentSnapshot`. This is an immutable copy, used both as a read overlay and as a commit's
    payload.
  - `def removeUpTo(seq: Long): Unit`. It drops every piece whose `seq <= seq`. Pieces written later survive.
  - `def bytes: Long`, `def isEmpty: Boolean`, `def maxSeq: Long`.
- Produces: `final class SegmentSnapshot(val pieces: Vector[(Int, Array[Byte])], val maxSeq: Long)`, with
  `def overlay(target: Array[Byte], targetOffsetInBlock: Int): Unit`, which copies the covered bytes into `target`.
  Also `def writes: List[RangedWrite]` and `def end: Int` (the highest byte covered plus one).

- [ ] **Step 1: Write failing tests:**
  - Put `[0,8)` at seq 1, then `[4,12)` at seq 2. The snapshot pieces are `[0,4)` from 1 and `[4,12)` from 2.
  - After `removeUpTo(1)`, only `[4,12)` remains.
  - Put `[0,100)` at seq 1, then `[10,20)` at seq 2, then `removeUpTo(1)`. Only `[10,20)` remains.
  - `overlay` writes only covered bytes. Untouched target bytes keep their value.
  - A randomized test checks 500 random put/removeUpTo sequences against a naive byte-array model with a per-byte
    seq.
- [ ] **Step 2:** Run `sbt 'testOnly *SegmentMapSuite'`. Expected: FAIL.
- [ ] **Step 3: Implement** using a `java.util.TreeMap[Int, Piece]` keyed by offset.
- [ ] **Step 4:** Run it again. Expected: PASS.
- [ ] **Step 5: Commit:** `git commit -m "vdisk: segment map for dirty block data"`.

### Task 4: Disk catalog: create, load, list, grow, delete, takeover

**Files:**
- Create: `src/main/scala/org/aspen_ddp/aspen/vdisk/DiskConfig.scala` (KV keys and encoding)
- Create: `src/main/scala/org/aspen_ddp/aspen/vdisk/Fence.scala`
- Create: `src/main/scala/org/aspen_ddp/aspen/vdisk/DiskCatalog.scala`
- Test: `src/test/scala/org/aspen_ddp/aspen/vdisk/DiskCatalogSuite.scala` (an `IntegrationTestSuite` with the
  engine test IDA and slice size from Global Constraints)

**Interfaces:**
- Produces: `final case class FenceState(epoch: Long, owner: UUID)` with `toArray` and `fromArray`
  (8 + 16 bytes, big-endian).
- Produces: `final case class DiskInfo(name: String, diskId: UUID, configPointer: KeyValueObjectPointer,
  fencePointer: DataObjectPointer, geometry: DiskGeometry, allocatorId: ObjectAllocatorId)`.
- Produces: the config object keys `DiskConfig.SizeKey`, `LogicalBlockSizeKey`, `BlockObjectSizeKey`,
  `AllocatorKey`, `FenceKey` and `MapRootKey`, with `Key("size")` and so on. The map root is hosted under
  `MapRootKey` by `KVObjectRootManager` with a `DataObjects` `Root` that uses `IntegerKeyOrdering` and
  `SinglePoolNodeAllocator.forPool` on the block pool.
- Produces: `object DiskCatalog`, with each operation's `given ExecutionContext` taken from
  `client.clientContext`:
  - `create(client: AspenClient, name: String, allocator: ObjectAllocator, size: Long, logicalBlockSize: Int =
    4096): Future[DiskInfo]`. It runs as one transaction: `prepareRegisterId("vdisk", name, diskId)`, allocate the
    fence (`FenceState(0, zero UUID)`) and the config object, create the empty tree, and `prepareRegisterObject`.
    It fails with `DiskAlreadyExists` or `InvalidGeometry` (size not aligned, size <= 0, `logicalBlockSize` not
    512 or 4096, `maxObjectSize` undefined, or `blockObjectSizeFor` gives `None`).
  - `load(client: AspenClient, name: String): Future[DiskInfo]`, which fails with `DiskNotFound`.
  - `list(client: AspenClient): Future[List[DiskInfo]]`, sorted by name.
  - `grow(client: AspenClient, name: String, newSize: Long): Future[DiskInfo]`. It's a KV update of `SizeKey` only,
    under a `KeyRevision` requirement on that key, and retries collisions. It fails with `InvalidGeometry` unless
    `newSize > size` and `newSize` is aligned.
  - `takeOwnership(client: AspenClient, info: DiskInfo, owner: UUID): Future[(FenceState, ObjectRevision)]`. It
    reads the fence, overwrites it with `epoch + 1`, and retries collisions with `transactUntilSuccessful`. It
    returns the new state and the revision it committed.
  - `delete(client: AspenClient, name: String): Future[Unit]`. It takes ownership with a random owner (which fences
    any live owner), then runs `deleteTree` with an entry callback that sets each block object's refcount to 0 in
    its own transaction, using `deferAllocationRelease()`. Then it deletes the config and fence objects and removes
    both registrations. Each step can be re-run, so a crash midway leaves a disk that `delete` can finish.

- [ ] **Step 1: Write failing tests:**
  - `"create then load round-trips geometry"`: size 65536 gives `blockObjectSize == 16384` and
    `logicalBlockSize == 4096`.
  - `"create rejects a duplicate name"`: `DiskAlreadyExists`.
  - `"create rejects unaligned size"`: `InvalidGeometry`.
  - `"list returns disks by name"`.
  - `"grow raises size and refuses shrink"`.
  - `"takeOwnership increments the epoch"`: two calls give epochs 1 and 2.
  - `"delete removes the disk and its registrations"`: `load` then fails with `DiskNotFound`.
- [ ] **Step 2:** Run `sbt 'testOnly *DiskCatalogSuite'`. Expected: FAIL.
- [ ] **Step 3: Implement** `FenceState`, `DiskConfig`, `DiskInfo`, `DiskCatalog`, following
  `SimpleFileSystem.create` for the single create transaction.
- [ ] **Step 4:** Run it again. Expected: PASS.
- [ ] **Step 5: Commit:** `git commit -m "vdisk: disk catalog and ownership fence"`.

### Task 5: VirtualDisk core write, flush, FUA and read path

**Files:**
- Create: `src/main/scala/org/aspen_ddp/aspen/vdisk/BlockDevice.scala` (the frontend seam)
- Create: `src/main/scala/org/aspen_ddp/aspen/vdisk/BlockState.scala`
- Create: `src/main/scala/org/aspen_ddp/aspen/vdisk/BlockMap.scala`
- Create: `src/main/scala/org/aspen_ddp/aspen/vdisk/VirtualDisk.scala`
- Create: `src/main/scala/org/aspen_ddp/aspen/vdisk/VDiskConfig.scala`
- Test: `src/test/scala/org/aspen_ddp/aspen/vdisk/VirtualDiskSuite.scala`

**Interfaces:**
- Consumes: Tasks 1 to 4.
- Produces: `trait BlockDevice` with:
  - `def geometry: DiskGeometry`
  - `def read(offset: Long, length: Int): Future[Array[Byte]]`
  - `def write(offset: Long, data: Array[Byte], fua: Boolean): Future[Unit]`
  - `def flush(): Future[Unit]`
  - `def unmap(ranges: List[(Long, Long)]): Future[Unit]`
  - `def grow(newSize: Long): Future[Unit]`
  - `def close(): Unit`, which is crash-only: it discards dirty data, never flushes, and fails pending requests
    with `Fenced`

  Tasks 5 to 8 fill in every member. Until Task 8, `unmap` and `grow` throw `NotImplementedError`.
- Produces: `final case class VDiskConfig(dirtyHardLimit: Long = 256 MiB, dirtySoftLimit: Long = 64 MiB,
  coalesceWindow: FiniteDuration = 20.millis, maxInFlightDataTx: Int = 64, maxMapTxBytes: Long = 32 MiB,
  cleanCacheBytes: Long = 512 MiB, maxReadahead: Int = 1 MiB)`.
- Produces: `object VirtualDisk { def acquire(client: AspenClient, name: String, config: VDiskConfig =
  VDiskConfig()): Future[VirtualDisk] }`. It loads the disk, takes ownership, and returns an owned device.
  `class VirtualDisk extends BlockDevice`.
- Produces: `final class BlockMap(tree: DataObjectTKVL)` with `def lookup(index: Long): Future[Option[DataObjectPointer]]`.
  The result is cached in memory, including absence, since the owner is the only writer. It also has
  `def prepare(inserts: List[(Long, DataObjectPointer)], deletes: List[Long])(using Transaction): Future[Unit]`,
  which uses `TKVLInsert(Key(index), Value(ptr.toArray), requireDoesNotExist = true)`, and
  `def recordCommitted(...)` and `def refresh(index: Long)`, which bypasses the cache.
- `BlockState` holds, per block: `pointer: Option[DataObjectPointer]` (unknown or absent until looked up),
  `revision: Option[ObjectRevision]`, `length: Option[Int]`, `dirty: SegmentMap`,
  `inFlight: Option[SegmentSnapshot]`, `committedSeq: Long`, `generation: Long`, plus the waiters for flush and FUA
  keyed by seq. All mutation happens under the `VirtualDisk` monitor, and Aspen calls are made outside it.

Behavior (the spec's performance principles, made concrete):
- **write:** validate, then split by `geometry.segments`. For each segment, `dirty.put(offsetInBlock, bytes,
  nextSeq)` and schedule writeback after `coalesceWindow`, or immediately when the dirty total exceeds
  `dirtySoftLimit`, the write is FUA, or a flush is pending. Complete the write immediately unless it's FUA, in
  which case complete when each segment's block has `committedSeq >= thatSeq`.
- **flush:** record, for every block with dirty or in-flight data, the target `maxSeq`, trigger writeback now, and
  complete when every target is reached. Map transactions pending at flush time are included.
- **Data transaction** for a mapped block with no commit in flight: move `dirty.snapshot` to `inFlight`, then build
  the `RangedWriteState`. Take it from clean-cache pieces through `RangedWriteState.fromRanges(pointer, revision,
  length, pieces)` (which is `private[aspen]`, so it's accessible from `vdisk`) when they cover every write clipped
  to `length`. Otherwise use `client.readRangedWriteState(pointer, writes)`. Then
  `tx.sharedLockRevision(fence, fenceRev)` and `tx.rangedWrites(state, snapshot.writes, zeroFill = true)`, and
  commit. When the dirty data covers all of `[0, blockObjectSize)`, use `overwrite` instead. On success: revision
  becomes `tx.revision`, length becomes `max(length, snapshot.end)`, the written bytes go into the clean cache,
  `dirty.removeUpTo(snapshot.maxSeq)`, `committedSeq = snapshot.maxSeq`, `generation += 1`, resolve waiters, and
  reschedule if more data is dirty.
- **Map transaction:** at most one per disk in flight. It batches unmapped blocks that have dirty data, in index
  order, up to `maxMapTxBytes` of initial content. Each one gets `allocateDataObject(content)`, where `content` runs
  from 0 to `snapshot.end` with zeros in the gaps, so untouched tails cost nothing, then `blockMap.prepare(inserts,
  Nil)` and `sharedLockRevision`. On success: pointer, revision `tx.revision`, length `snapshot.end`, and the same
  completion steps as a data transaction.
- **read:** validate and split. For each segment, take an overlay snapshot (dirty plus in-flight) before any I/O.
  If the block is absent, return zeros under the overlay. Otherwise fetch the uncovered parts from the clean cache
  or with `client.readRanges` (clipped to `length`, zeros beyond it). Assemble the fetched bytes, then apply the
  overlay. Insert the fetched bytes into the clean cache only if the block's `generation` didn't change during the
  fetch. In this task the clean cache is a plain per-block piece list with no eviction. Task 6 bounds it and adds
  readahead.

- [ ] **Step 1: Write failing tests** in `VirtualDiskSuite` (a disk of 65536 bytes, block objects of 16384):
  - `"write then read returns the data before any flush"`
  - `"flush makes data visible to a new owner"`: write and flush, `acquire` a second instance, and it reads the
    same bytes.
  - `"fua write is durable without flush"`: a FUA write, then a second instance reads it.
  - `"unwritten ranges read as zeros"`
  - `"write spanning two block objects round-trips"`: offset 12288, length 8192, covering Review Focus 2.
  - `"overlapping writes during an in-flight commit keep the newest bytes"`: issue 20 overlapping writes without
    awaiting them, then flush, then read on a second owner. The result matches a byte-array model.
  - `"writes to many blocks commit concurrently"`: 4 blocks written, flush succeeds.
  - `"read during first-write allocation returns written bytes"`: write, then read immediately with
    `coalesceWindow = 0.millis`, before the flush completes (Review Focus 4).
  - `"unaligned and zero-length requests"`: `write(100, ...)` fails `InvalidRequest`, and `read(0, 0)` returns an
    empty array (Review Focus 1).
  - `"out-of-range requests fail without staging"`: `write(61440, 8192 bytes)` fails `OutOfRange`, and a following
    flush completes immediately (Review Focus 3).
  - `"second write to a mapped block uses ranged writes"`: after the first flush, a 4 KiB write commits, and the
    test asserts the data is correct through a second owner.
- [ ] **Step 2:** Run `sbt 'testOnly *VirtualDiskSuite'`. Expected: FAIL.
- [ ] **Step 3: Implement** `BlockDevice`, `VDiskConfig`, `BlockState`, `BlockMap` and `VirtualDisk`, following the
  behavior above. Leave failure handling minimal for now: any commit failure fails the waiting futures with
  `DiskFailed`. Task 7 replaces this.
- [ ] **Step 4:** Run it again. Expected: PASS. Then run it 20 times in a loop
  (`for i in $(seq 20); do sbt -batch 'testOnly *VirtualDiskSuite' || break; done`) to show it isn't flaky.
- [ ] **Step 5: Commit:** `git commit -m "vdisk: write-back block device core"`.

### Task 6: Bounded clean cache and readahead

**Files:**
- Create: `src/main/scala/org/aspen_ddp/aspen/vdisk/CleanCache.scala`
- Create: `src/main/scala/org/aspen_ddp/aspen/vdisk/SequentialDetector.scala`
- Modify: `src/main/scala/org/aspen_ddp/aspen/vdisk/VirtualDisk.scala`
- Test: `src/test/scala/org/aspen_ddp/aspen/vdisk/SequentialDetectorSuite.scala`, `VirtualDiskSuite.scala`

**Interfaces:**
- Produces: `final class CleanCache(maxBytes: Long)`, a Scaffeine `Cache[Long, CleanBlock]` with a byte weigher.
  `CleanBlock` is an immutable sorted piece list. Methods: `get(index): Option[CleanBlock]`,
  `merge(index, offset, bytes)`, `invalidate(index)`.
- Produces: `final class SequentialDetector(maxStreams: Int = 8, initialWindow: Int, maxWindow: Int)` with
  `def onRead(offset: Long, length: Int): Int`. It returns the readahead length past the request: 0 for a random
  read, and doubling from `initialWindow` (= 4 × `logicalBlockSize`) up to `maxWindow` while one stream keeps
  reading where it left off.
- `VirtualDisk.read` asks the detector, fetches `[request end, request end + readahead)` clipped to the block and
  the object length in the same `readRanges` call as the request, and merges everything fetched into the cache. A
  readahead never crosses into the next block object. The next sequential read starts a new fetch there.

- [ ] **Step 1: Write failing tests:**
  - A random read returns 0.
  - Sequential 4 KiB reads give 16 KiB, 32 KiB, 64 KiB and so on, capped at `maxWindow`.
  - Two interleaved sequential streams are both detected.
  - A gap resets the stream.
  - In `VirtualDiskSuite`, `"sequential reads hit the clean cache"`: after a sequential read of block 0, a re-read
    is served without a `readRanges` call. Count calls through a small `VirtualDisk` test hook,
    `private[vdisk] def fetchCount: Long`.
  - `"clean cache evicts beyond its byte bound"`: `cleanCacheBytes = 16384`, read two blocks, and the first block's
    entry is gone.
- [ ] **Step 2:** Run `sbt 'testOnly *SequentialDetectorSuite *VirtualDiskSuite'`. Expected: FAIL.
- [ ] **Step 3: Implement** both classes and wire them into `VirtualDisk`.
- [ ] **Step 4:** Run it again. Expected: PASS.
- [ ] **Step 5: Commit:** `git commit -m "vdisk: bounded clean cache and readahead"`.

### Task 7: Fencing and commit-failure resolution

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/vdisk/VirtualDisk.scala`
- Test: `src/test/scala/org/aspen_ddp/aspen/vdisk/VirtualDiskFailureSuite.scala`

**Interfaces:**
- Produces: `VirtualDisk.state: DiskState`, where `enum DiskState { case Serving, Fenced, Failed(reason: String) }`.
  Once it's no longer `Serving`, every pending and new request fails with `Fenced` or `DiskFailed`, the dirty and
  clean state is dropped, and no new transactions are started.
- Resolution when any commit fails:
  1. Read the fence. If its revision differs from `fenceRev`, the disk is fenced.
  2. For a data transaction, read the block's revision and length with `client.readRange(pointer, 0, Some(0))`,
     which gives `revision` and `objectLength`. If it equals the failed transaction's revision, treat the commit as
     successful. If it equals the prior revision, put `inFlight` back ahead of the dirty data and retry. Anything
     else is `Failed`.
  3. For a map transaction, retry. On `KeyAlreadyExists` for an index, call `blockMap.refresh(index)`, adopt the
     pointer, read its revision and length as in step 2, and send that block's snapshot through a data transaction.
  Retries use the client's `retryStrategy` backoff and never time out.

- [ ] **Step 1: Write failing tests:**
  - `"a takeover fences the old owner"`: owner A writes and flushes, B acquires, then A's `write` plus `flush`
    fails `Fenced`, `A.state == Fenced`, and B reads A's flushed data.
  - `"a fenced owner's unflushed writes never appear after takeover"`: A writes block 2 with `coalesceWindow`
    = 10.s, B acquires, then A flushes (which fails), and B reads zeros for block 2.
  - `"crash-only close loses only unflushed data"`: A writes X and flushes, writes Y, then `close()`s. B reads X,
    and Y's range is either Y or the old data, never a mix within one 4 KiB sector.
  - `"a data commit that actually committed is recognized"`: inject a failure through a `VirtualDisk` test hook
    `private[vdisk] var failNextCommitAfterSuccess: Boolean`, which turns a successful commit result into a
    failure. The disk stays `Serving`, and the data reads back.
  - `"map transaction KeyAlreadyExists adopts the existing pointer"`: the same hook applied to a map transaction,
    then another write to that block and a flush succeed.
  - `"a foreign write to a block object fails the disk"`: overwrite the block object directly through the client,
    then a write plus flush gives `DiskFailed`.
- [ ] **Step 2:** Run `sbt 'testOnly *VirtualDiskFailureSuite'`. Expected: FAIL.
- [ ] **Step 3: Implement** the resolution above and the `DiskState` transitions.
- [ ] **Step 4:** Run it again. Expected: PASS. Then run it 20 times in a loop (as in Task 5).
- [ ] **Step 5: Commit:** `git commit -m "vdisk: fencing and commit-failure resolution"`.

### Task 8: UNMAP, grow, and dirty-limit backpressure

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/vdisk/VirtualDisk.scala`, `BlockMap.scala`
- Test: `src/test/scala/org/aspen_ddp/aspen/vdisk/VirtualDiskSuite.scala`

Behavior:
- **unmap(ranges):** validate each range. For every block in `geometry.blocksFullyCovered`: drop dirty data and
  resolve its waiters as satisfied (superseded), invalidate the clean cache, and mark it absent. If it was mapped,
  queue `(index, pointer)` for deletion in the next map transaction. Ignore partial blocks. Complete immediately,
  and let flush wait for the queued deletions.
- **Deletions in a map transaction:** `setRefcount(ptr, current, current.update(0))`, where `current` comes from
  `client.readMetadata(ptr)` in the same transaction build, plus a `blockMap.prepare` delete and
  `tx.deferAllocationRelease()` (the disk uses one FSIF pool, so it stays within the one-pool limit). A block that
  is queued for deletion and written again in the same batch becomes a *replace*: the old object is deleted and the
  key is inserted without `requireDoesNotExist`. `BlockMap.prepare` gains `replaces: List[(Long, DataObjectPointer)]`
  for this.
- **grow(newSize):** `DiskCatalog.grow`, then update `geometry`. In-flight I/O is unaffected.
- **Backpressure:** while the dirty total is at or above `dirtyHardLimit`, new `write` futures complete only once
  enough writeback has finished to bring it below the limit. Between the soft and hard limits, writeback skips the
  coalescing window.

- [ ] **Step 1: Write failing tests:**
  - `"unmap of a whole block reads back zeros and frees the object"`: write and flush block 1, unmap
    `(16384, 16384)`, flush, and then `blockMap.lookup(1)` on a fresh owner is `None` and the block reads zeros.
  - `"partial unmap is ignored"`.
  - `"write, unmap, write before flush keeps the second write"` (Review Focus 5): flush succeeds, and a fresh owner
    reads only the second write.
  - `"grow extends the device"`: after `grow(131072)`, a write at 98304 succeeds.
  - `"writers wait at the hard dirty limit"`: `dirtyHardLimit = 32768` and `coalesceWindow = 10.s`. Writing 64 KiB
    in 4 KiB pieces doesn't complete every write until writeback runs, and everything reads back correctly after a
    flush.
- [ ] **Step 2:** Run `sbt 'testOnly *VirtualDiskSuite'`. Expected: FAIL.
- [ ] **Step 3: Implement** the behavior above.
- [ ] **Step 4:** Run it again. Expected: PASS. Then run it 20 times in a loop.
- [ ] **Step 5: Commit:** `git commit -m "vdisk: unmap, grow and dirty-limit backpressure"`.

### Task 9: CLI commands and documentation

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/cmdline/Main.scala` (add `create-disk`, `list-disks`, `grow-disk`
  and `delete-disk`, following `create-filesystem` and `list-filesystems`)
- Test: `src/test/scala/org/aspen_ddp/aspen/cmdline/MainSuite.scala`
- Modify: `CLAUDE.md` (one Key Architectural Components bullet for vdisk), `UsageGuide.md` (the disk commands)

**Interfaces:**
- `create-disk <disk-name> <pool-name> <size> [--block-size 512|4096]`
- `list-disks`
- `grow-disk <disk-name> <new-size>`
- `delete-disk <disk-name>`
- Sizes accept a byte count with an optional `K`, `M`, `G` or `T` binary suffix:
  `private[cmdline] def parseByteSize(s: String): Either[String, Long]`.
- Produces: `private[cmdline] def formatDiskInfo(d: DiskInfo): String`, giving one line with the name, size
  (through the existing `formatBytes`), block size, block object size and disk id.

- [ ] **Step 1: Write failing tests:**
  - `parseByteSize("100G") == Right(107374182400L)`
  - `parseByteSize("4096") == Right(4096L)`
  - `parseByteSize("1.5G")` is a `Left`
  - `parseByteSize("-1")` is a `Left`
  - `formatDiskInfo` renders the expected line for a fixed `DiskInfo`
- [ ] **Step 2:** Run `sbt 'testOnly *MainSuite -- -z "Disk"'` and `-z "parseByteSize"`. Expected: FAIL.
- [ ] **Step 3: Implement** the commands. Error messages: `a disk named '<name>' already exists`,
  `disk '<name>' not found`, `storage pool '<pool>' not found`, and `InvalidGeometry`'s reason. Then add the docs
  lines.
- [ ] **Step 4:** Run the MainSuite tests, then the full suite (`sbt test`) under a 70 s hang budget. Expected:
  PASS.
- [ ] **Step 5: Commit:** `git commit -m "vdisk: CLI commands and docs"`.
