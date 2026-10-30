# FSIF Allocation Strategy Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Hand out and reclaim FSIF external indices through a per-pool bitmap held in a KeyValue object, so FSIF
pools can be created from the CLI.

**Architecture:** A new `FsifIndexAllocationStrategy` reads the pool's bitmap once per transaction, reserves free
bits for allocations, and in a single pre-build hook writes every touched segment back under key-revision guards.
Deletions clear their bits in the same hook, or, if `Transaction.deferAllocationRelease()` was called, they enroll a
system DurableTask that clears them later. `TransactionImpl`'s commit hooks become asynchronous and run against a
restricted view of the transaction, so the caller still can't modify it after `commit()`.

**Tech Stack:** Scala 3, ScalaTest (`AnyFunSuite`, `IntegrationTestSuite`/`atest`), sbt.

**Spec:** `docs/superpowers/specs/2026-10-30-fsif-allocation-strategy-design.md`

## Global Constraints

- storePointer is exactly 4 bytes, a big-endian `Int` `X` with `0 <= X < segmentCount`.
- One bitmap KeyValue object per FSIF pool, allocated at pool creation from the pool's metadata group allocator
  (`GroupObjectAllocatorId(metadataGroup)`), never in the FSIF pool itself.
- Bitmap segments: 1024 bits (128 bytes) per key; `ceil(segmentCount / 1024)` keys, all present and zeroed at
  creation. Bits at or past `segmentCount` are never allocated.
- At most one bitmap read per pool per transaction, and exactly one `update` to the bitmap object per transaction.
- FSIF is refused for metadata pools (client API and CLI).
- Prefer Scala 3 quiet syntax and indented if/then/else (CLAUDE.md).
- Tests: correctness, then no flakiness, then speed (`CLAUDE.local.md`). 70 seconds is enough to detect a hung
  full test run.
- Commit directly on `main`. Every commit message ends with `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.
- If anything makes clearing a bit inside the deleting transaction unsafe for a reason other than contention, stop
  and raise it (spec, "Builder: keep looking for conflicts").

## Decisions this plan makes (within the spec's "Left to the builder")

- `StoragePoolAllocationStrategy.deleteObject` is **replaced** by a single per-pool, per-transaction
  `prepareCommit(poolId, allocated, deleted, deferRelease)` hook that returns `Future[Unit]`. The transaction calls it
  once for each pool in which it allocates or deletes. That answers "which strategies have pending work" (any
  pool with an allocated or deleted object, including a pool with only deletions) without a registration API.
- The bits to set come from the transaction's **staged** allocated objects, not from the pointers the strategy
  handed out. A pointer that was created but never staged (`createDataObjectPointer` without
  `stageDataObjectAllocation`) therefore sets no bit.
- Per-transaction strategy state lives on the transaction: `Transaction.attachment(key)(init)`.
- Strategy config bytes are `<4-byte segmentCount><bitmap KeyValueObjectPointer bytes>`.
- Segment key `s` is `Key(int2byte(s))`.
- Free-bit search: start at a random segment, take the first one (wrapping) that has a free bit not already reserved
  in this transaction, and take its lowest such bit. Keep using that segment until it has none left.
- Clearing a bit that is already clear is a no-op with a warning log. It never aborts the delete.
- The bitmap size limit is enforced by the existing `ObjectSizeLimitExceeded` check. Pool creation is
  `runCreate(StopRetrying.unlessCollision)`, so that check fails creation permanently, and its message names both
  sizes.
- The deferred-release task is a one-step `SteppedDurableTask` named `FsifReleaseIndicesTask`.

## Review Focus

1. **Collision between two clients on one segment.** The second commit must abort with `TransactionAborted` and
   leave the bitmap as the first left it. Test in Task 3 (`concurrent allocations in one segment...`).
2. **A transaction that deletes an object and allocates in the same pool.** The freed index must not be handed out
   again in that transaction. Test in Task 3 (`an index freed in a transaction is not reallocated in it`).
3. **A caller touching the transaction while async hooks run.** It must throw `PostCommitTransactionModification`,
   even though hooks now run asynchronously. Test in Task 2.
4. **A pointer whose storePointer isn't a valid index** (wrong length, or out of range) being deleted from an FSIF
   pool. The hook must fail the transaction with `IllegalArgumentException`, never clear some other bit. Test in
   Task 3.
5. **A full pool.** It must fail with `IndicesExhausted(poolId)`, not loop or hand out an index twice. Test in
   Task 3.

---

### Task 1: Bitmap encoding helpers

**Files:**
- Create: `src/main/scala/org/aspen_ddp/aspen/client/internal/allocation/FsifIndexBitmap.scala`
- Test: `src/test/scala/org/aspen_ddp/aspen/client/internal/allocation/FsifIndexBitmapSuite.scala`

**Interfaces:**
- Produces, in `object FsifIndexBitmap`:
  - `val SegmentBits: Int = 1024`, `val SegmentBytes: Int = 128`
  - `def segmentsFor(indexCount: Int): Int`, which is `ceil(indexCount / 1024)` and requires `indexCount > 0`
  - `def segmentKey(segment: Int): Key`, which is `Key(int2byte(segment))`
  - `def zeroedContent(indexCount: Int): Map[Key, Value]`
  - `def isSet(seg: Array[Byte], bit: Int): Boolean`
  - `def withBit(seg: Array[Byte], bit: Int, value: Boolean): Array[Byte]`, which returns a copy
  - `def freeBit(seg: Array[Byte], usableBits: Int, reserved: Int => Boolean): Option[Int]`, which returns the
    lowest clear, unreserved bit below `usableBits`
  - `def usableBits(segment: Int, indexCount: Int): Int`, which is 1024 except for the last segment
  - `def index(segment: Int, bit: Int): Int` and `def locate(index: Int): (Int, Int)`
  - `def storePointer(index: Int): Array[Byte]`, the 4-byte big-endian value
  - `def indexOf(storePointer: Array[Byte], indexCount: Int): Int`, which throws `IllegalArgumentException` unless the
    length is 4 and `0 <= X < indexCount`
  - `def encodeIndices(indices: Seq[Int]): Array[Byte]` / `def decodeIndices(bytes: Array[Byte]): List[Int]`, packed
    4-byte ints (used by Task 4)

- [ ] **Step 1: Write the failing tests** (`class FsifIndexBitmapSuite extends AnyFunSuite with Matchers`)
  - `"segmentsFor rounds up to whole 1024-bit segments"`: `segmentsFor(1) == 1`, `segmentsFor(1024) == 1`,
    `segmentsFor(1025) == 2`, `segmentsFor(102400) == 100`.
  - `"zeroedContent has every segment key with 128 zero bytes"`: for `3000`, the keys are
    `segmentKey(0..2)` and every value is `Array.fill(128)(0)`.
  - `"usableBits limits the last segment to the index count"`: `usableBits(2, 3000) == 952`,
    `usableBits(0, 3000) == 1024`.
  - `"freeBit skips set and reserved bits and never returns a bit at or past usableBits"`: with bits 0 and 1 set and
    bit 2 reserved, it returns `Some(3)`. A segment with all of its first 5 bits set and `usableBits = 5` gives
    `None`.
  - `"withBit copies, and index/locate round-trip"`: the original array is unchanged, and
    `locate(index(2, 17)) == (2, 17)` with `index(2, 17) == 2065`.
  - `"storePointer and indexOf round-trip and indexOf rejects bad pointers"`: `indexOf(storePointer(77), 100) == 77`.
    `indexOf(Array(0,0,0), 100)`, `indexOf(storePointer(100), 100)` and `indexOf(storePointer(-1), 100)` all throw
    `IllegalArgumentException`.
  - `"encodeIndices round-trips"`: `decodeIndices(encodeIndices(List(0, 5, 70000))) == List(0, 5, 70000)`.

- [ ] **Step 2: Run them to verify they fail**
  Run: `sbt 'testOnly *FsifIndexBitmapSuite'`. Expected: compilation failure, `FsifIndexBitmap` not found.

- [ ] **Step 3: Implement `object FsifIndexBitmap`** with the signatures above. Bit `b` is bit `b % 8` (LSB first) of
  byte `b / 8`.

- [ ] **Step 4: Run them to verify they pass**
  Run: `sbt 'testOnly *FsifIndexBitmapSuite'`. Expected: all pass.

- [ ] **Step 5: Commit** with the message `FSIF index bitmap encoding helpers`.

---

### Task 2: Async per-pool commit hooks and `deferAllocationRelease`

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/client/StoragePoolAllocationStrategy.scala`, to replace
  `deleteObject` with `prepareCommit`
- Modify: `src/main/scala/org/aspen_ddp/aspen/client/Transaction.scala`, to add three members
- Modify: `src/main/scala/org/aspen_ddp/aspen/client/internal/transaction/TransactionBuilder.scala`, to add an
  accessor for `allocatingObjects`
- Modify: `src/main/scala/org/aspen_ddp/aspen/client/internal/transaction/TransactionImpl.scala:31-171`, covering
  the hook loop, the hook view and the attachments
- Modify: `src/test/scala/org/aspen_ddp/aspen/FsifTestIndexStrategy.scala`, to switch it to `prepareCommit`
- Test: `src/test/scala/org/aspen_ddp/aspen/client/internal/allocation/PoolObjectAllocatorSuite.scala`

**Interfaces:**
- Produces, in `trait StoragePoolAllocationStrategy`:
  ```scala
  /** Called while `t` is being committed, once for each pool in which `t` allocates or deletes objects, after every
   *  object the caller added. `allocated` holds only staged allocations; `deferRelease` is
   *  `t.allocationReleaseDeferred`. May add to `t` (including in other pools); must not commit or abort it; a
   *  failure aborts it with that failure. Adding an allocation or deletion in a pool whose hook already ran fails
   *  the transaction with IllegalStateException.
   */
  def prepareCommit(poolId: PoolId, allocated: List[ObjectPointer], deleted: List[ObjectPointer],
                    deferRelease: Boolean)(using t: Transaction): Future[Unit]
  ```
  `Default.prepareCommit` returns `Future.unit`.
- Produces, in `trait Transaction`:
  - `def deferAllocationRelease(): Unit`. It's public, and like any modification it throws
    `PostCommitTransactionModification` after `commit()`. Its scaladoc says what it's for: bulk deletes whose
    in-transaction index release would risk collisions. It's a no-op for ordinary pools.
  - `private[aspen] def allocationReleaseDeferred: Boolean`
  - `private[aspen] def attachment[T <: AnyRef](key: AnyRef)(init: => T): T`, a per-transaction map that is
    synchronized and usable both before and during commit hooks. `init` runs at most once per key.
- Produces, in `TransactionBuilder`: `def allocatedObjects: Set[ObjectPointer]` (synchronized).

`TransactionImpl` changes:
- Remove `runningCommitHooks`. Once `commitStarted`, `modify` always throws for the caller.
- Add `private object hookView extends Transaction`. Its members forward to `TransactionImpl` internals through a
  `hookModify` that is permitted only while `hooksActive` is true. Its `commit()` throws `IllegalStateException`, and
  `abort`/`abortAndThrow` forward (and so throw `AbortAfterCommit`). Hooks are always called `using hookView`.
- `runCommitHooks(pools, given: Map[PoolId, (Set[ObjectPointer], Set[ObjectPointer])])` loops until a pass finds no
  pool with work it hasn't been given yet:
  1. Resolve pools.
  2. Group `bldr.allocatedObjects` and `bldr.deletedObjects` by pool.
  3. Fail with `IllegalStateException` if a pool already in `given` has gained objects.
  4. Call `prepareCommit` sequentially for each new pool.

  Set `hooksActive = false` before the build.

- [ ] **Step 1: Rewrite `TestStrategy` and the existing deletion tests for the new hook.** `TestStrategy` records
  each `prepareCommit` call as `(allocated, deleted, deferRelease)` in `@volatile var calls`, and still adds the
  `DeletionRecorder` finalization action for each deleted pointer. Add an optional `gate: Option[Promise[Unit]]`
  that `prepareCommit` waits on before returning. Rename and adjust these tests:
  - `"custom allocation strategy is created once, on first use, and told of allocations and deletions"`. The
    allocating transaction records `calls == List((List(ptr), Nil, false))`. The deleting transaction records
    `(Nil, List(ptr), false)`. The `DeletionRecorder` count is `>= 1`, and the object is gone.
  - `"refcount changes that do not delete are not reported to the strategy"`. The only call is the allocation's;
    the increment transaction adds no call.
- [ ] **Step 2: Add the new tests**
  - `"deferAllocationRelease reaches prepareCommit"`: delete with `tx.deferAllocationRelease()`. The recorded
    call's `deferRelease` is `true`.
  - `"modifying a transaction while its commit hooks run throws PostCommitTransactionModification"`: set the gate.
    Allocate, call `commit()`, and then (while the hook is held) `tx.note("x")` throws
    `PostCommitTransactionModification`. Release the gate, and the commit succeeds.
  - `"a failing prepareCommit aborts the transaction with its exception"`: the strategy returns
    `Future.failed(new RuntimeException("boom"))`, configured with a flag on `TestStrategy`. `tx.commit()` fails
    with that exception, and the allocated object is never readable.
  - `"a hook may add an update to the transaction"`: `prepareCommit` adds an `Insert` of a fresh key to the radicle
    `using t`. After commit, the radicle holds that key.
  - `"a hook that allocates in a pool whose hook already ran fails the transaction"`: `prepareCommit` allocates
    through its own pool's allocator `using t`. Here that's the bootstrap pool, where the existing suite installs
    `TestStrategy`. `commit()` fails with `IllegalStateException`. Allocating in a pool not yet prepared is
    exercised end to end by Task 4, whose task object lands in the bootstrap pool while the strategy's pool is
    another one.
- [ ] **Step 3: Run them to verify they fail**
  Run: `sbt 'testOnly *PoolObjectAllocatorSuite'`. Expected: compilation failure (`prepareCommit` isn't a member).
- [ ] **Step 4: Implement the trait, `Transaction`, builder and `TransactionImpl` changes above.** Update
  `FsifTestIndexStrategy` to `prepareCommit` returning `Future.unit`.
- [ ] **Step 5: Run the affected suites**
  Run: `sbt 'testOnly *PoolObjectAllocatorSuite *FsifIntegrationSuite *TransactionImpl* *TransactionBuilder*'`.
  Expected: all pass.
- [ ] **Step 6: Commit** with the message `Async per-pool allocation strategy commit hooks; Transaction.deferAllocationRelease`.

---

### Task 3: `FsifIndexAllocationStrategy` (in-transaction set and clear)

**Files:**
- Create: `src/main/scala/org/aspen_ddp/aspen/client/internal/allocation/FsifIndexAllocationStrategy.scala`
- Modify: `src/main/scala/org/aspen_ddp/aspen/common/TypeFactories.scala`, to register the factory
- Test: `src/test/scala/org/aspen_ddp/aspen/client/internal/allocation/FsifIndexAllocationStrategySuite.scala`

**Interfaces:**
- Consumes: `FsifIndexBitmap` (Task 1), `prepareCommit` / `attachment` / `allocationReleaseDeferred` (Task 2).
- Produces, in `object FsifIndexAllocationStrategy extends StoragePoolAllocationStrategy.Factory with RegisteredTypeFactory`:
  - `val typeUUID: UUID`, a new random UUID
  - `final case class IndicesExhausted(poolId: PoolId) extends Exception(s"FSIF pool $poolId has no free index")`
  - `def encodeConfig(indexCount: Int, bitmap: KeyValueObjectPointer): Array[Byte]` and
    `def decodeConfig(bytes: Array[Byte]): (Int, KeyValueObjectPointer)`
  - `def prepareBitmap(indexCount: Int, allocator: ObjectAllocator)(using Transaction): Future[Map[Key, Value]]`,
    which allocates the zeroed bitmap and returns the pool-state entries
    `AllocationStrategyKey -> uuid2byte(typeUUID)` and `AllocationStrategyConfigKey -> encodeConfig(...)`
  - `class Strategy(client, val indexCount: Int, val bitmap: KeyValueObjectPointer) extends StoragePoolAllocationStrategy`
    (Task 4 reads `bitmap` and `indexCount`)
- Per-transaction state: `t.attachment((typeUUID, poolId))(new TxState)`. It holds a `Future[KeyValueObjectState]`
  for the bitmap read (created once), the current segment, and the reserved indices, guarded by `synchronized`.

Behavior:
- `createDataObjectPointer` / `createKeyValueObjectPointer`: read the bitmap (or reuse the transaction's read),
  reserve a bit, and return a pointer with `storePointer(index)` and a random ObjectId. If no free bit remains, fail
  with `IndicesExhausted(poolId)`.
- `prepareCommit`: with no allocations, and either no deletions or `deferRelease = true`, add nothing to the bitmap.
  Otherwise, start from the transaction's bitmap read (reading it now if none was taken). Set the bit of each
  allocated pointer's `indexOf`. Unless `deferRelease` is set, clear the bit of each deleted pointer's `indexOf`.
  Then add one `t.update(bitmap, None, None, keyRevisions, inserts)` for the touched segments only.
  `deferRelease` with deletions is handled in Task 4; in this task it leaves deletions untouched.

Test setup (`class FsifIndexAllocationStrategySuite extends IntegrationTestSuite`): register
`FsifIndexAllocationStrategy` through `userTypeFactories`. **Never install the strategy on the bootstrap pool.** The
bitmap itself and system task objects (Task 4) are allocated there, and a second allocation in an already-prepared
pool fails by design. A helper `install(indexCount)` does the following:
- Create an ordinary pool with
  `client.createNewStoragePool(name, Replication(3, 2), None, RocksDBConfig(), StorageDeviceSetId.BootstrapStorageDeviceSetId, 0L)`,
  as `MissedUpdateFinalizationActionSuite` does.
- Wait until its stores are served. Mirror `FsifIntegrationSuite.awaitStoresServed`, triggering the TestNetwork
  host's device check. Find out how the TestNetwork's `smgr` host picks up a new pool's stores, and say in the task
  report what was needed.
- In one transaction, call `prepareBitmap(indexCount, radiclePool.allocator)`.
- Write the returned entries onto the new pool's state object, the same way
  `PoolObjectAllocatorSuite.customStrategyPool` does.
- `dropCachedStoragePool`, and return the pool id, a fresh `PoolObjectAllocator` and the bitmap pointer.

A helper `setBits(bitmapPtr): Future[Set[Int]]` reads every segment and returns the indices that are set.

- [ ] **Step 1: Write the failing tests**
  - `"allocations get distinct 4-byte indices and set exactly their bits"`: with `install(3000)`, allocate 5
    DataObjects and 2 KeyValue objects in one transaction. All storePointers are 4 bytes, distinct, and `< 3000`.
    `setBits` equals their indices.
  - `"allocations in one transaction stay in one segment until it is full"`: with `install(2048)`, allocate 1030 in
    one transaction. Exactly 1024 come from one segment and 6 from the other.
  - `"an aborted allocation leaves the bitmap unchanged"`: allocate with a failing `KeyValueUpdate.DoesNotExist`
    requirement on an existing radicle key. The commit fails, and `setBits` is empty.
  - `"a pointer created but never staged sets no bit"`: call `createDataObjectPointer()` (not staged), then
    allocate one object normally. `setBits` holds only the staged object's index.
  - `"deleting an object clears its bit in the same transaction and the index can be reused"`: with `install(1)`,
    allocate, delete, then allocate again. The second allocation succeeds with index 0, and `setBits == Set(0)`.
  - `"an index freed in a transaction is not reallocated in it"`: with `install(1)`, allocate O. In one transaction,
    delete O and allocate. The allocation fails with `IndicesExhausted`. A following transaction allocates index 0.
  - `"a full pool fails allocation with IndicesExhausted"`: with `install(3)`, 3 allocations succeed, and the 4th
    fails with `IndicesExhausted(poolId)`.
  - `"concurrent allocations in one segment: the second to commit aborts"`: with `install(64)`, create both
    transactions' allocations before either commits. The first commits. The second fails with
    `TransactionAborted`, and `setBits` equals the first one's index.
  - `"deleting an object whose storePointer is not a valid index fails the transaction"`: split `install` so that
    one object is allocated in the new pool before the strategy is written, which gives it an empty storePointer.
    After the strategy is installed, deleting that object fails with `IllegalArgumentException`, and the object
    still exists.
- [ ] **Step 2: Run them to verify they fail**
  Run: `sbt 'testOnly *FsifIndexAllocationStrategySuite'`. Expected: compilation failure.
- [ ] **Step 3: Implement `FsifIndexAllocationStrategy`** and add it to `TypeFactories.factories`.
- [ ] **Step 4: Run them to verify they pass**
  Run: `sbt 'testOnly *FsifIndexAllocationStrategySuite *FsifIndexBitmapSuite *PoolObjectAllocatorSuite'`.
  Expected: all pass.
- [ ] **Step 5: Commit** with the message `FsifIndexAllocationStrategy: allocated-index bitmap set and cleared in the transaction`.

---

### Task 4: Deferred release through a system DurableTask

**Files:**
- Create: `src/main/scala/org/aspen_ddp/aspen/client/internal/allocation/FsifReleaseIndicesTask.scala`
- Modify: `FsifIndexAllocationStrategy.scala`, the `deferRelease` branch of `prepareCommit`
- Modify: `src/main/scala/org/aspen_ddp/aspen/common/TypeFactories.scala`
- Test: `FsifIndexAllocationStrategySuite.scala`

**Interfaces:**
- Consumes: `FsifIndexAllocationStrategy.Strategy.bitmap` / `indexCount`, and `FsifIndexBitmap.encodeIndices` /
  `decodeIndices`.
- Produces: `object FsifReleaseIndicesTask extends DurableTaskFactory` with a new `typeUUID` and
  `def prepare(client: AspenClient, poolId: PoolId, indices: Seq[Int])(using Transaction): Future[Unit]`. It calls
  `client.prepareSystemDurableTask(typeUUID, Map(SteppedDurableTask.StepStateKey -> initial))`, where `initial` is
  `SteppedDurableTask.getInitialContent(Map("pool" -> uuid2byte(poolId.uuid), "indices" -> encodeIndices(indices)))._2.bytes`.
- `class FsifReleaseIndicesTask(taskPointer, client) extends SteppedDurableTask` has one step:
  1. `client.getStoragePool(poolId)`, then take its `allocationStrategy` as `FsifIndexAllocationStrategy.Strategy`
     (anything else is `StopRetrying(IllegalStateException)`).
  2. Read the bitmap and clear the indices' bits (an already-clear bit gets a warning, as in Task 3).
  3. Add one guarded `update`.

  The task's own step commit retries on collision, as every `SteppedDurableTask` does.
- `prepareCommit` with `deferRelease` and deletions calls `FsifReleaseIndicesTask.prepare(client, poolId, indices)`.
  It validates each index with `indexOf` first, so a bad pointer still fails the deleting transaction.

- [ ] **Step 1: Write the failing tests**
  - `"a deferred release keeps the bit set and enrolls one task that clears it"`: with `install(16)`, allocate O.
    Delete O with `deferAllocationRelease()`. Afterward, `setBits` still holds O's index, and
    `SystemTaskServiceState.scan` lists exactly one task whose `TaskTypeKey` is `FsifReleaseIndicesTask.typeUUID`.
    Build that task with `FsifReleaseIndicesTask.createTask(client, DurableTaskPointer(ptr), ...)` and await
    `completed`. `setBits` is now empty.
  - `"an aborted deferred delete enrolls no task"`: the delete transaction also carries a failing requirement.
    `scan` shows no new task, and the bit is still set.
  - `"deferring in a transaction that also allocates still sets the new bit in the transaction"`: in one deferred
    transaction, delete O and allocate P. P's bit is set at commit, and O's is cleared only after the task runs.
- [ ] **Step 2: Run them to verify they fail**
  Run: `sbt 'testOnly *FsifIndexAllocationStrategySuite'`. Expected: compilation failure.
- [ ] **Step 3: Implement the task and the deferred branch.** Register the factory in `TypeFactories.factories`.
- [ ] **Step 4: Run them to verify they pass**
  Run: `sbt 'testOnly *FsifIndexAllocationStrategySuite'`. Expected: all pass.
- [ ] **Step 5: Commit** with the message `Deferred FSIF index release via a system DurableTask`.

---

### Task 5: FSIF pools get the strategy at creation; metadata pools refuse FSIF

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/client/internal/BaseAspenClient.scala:474-512` (`createStoragePool` / `createPoolObj`)
- Modify: `src/main/scala/org/aspen_ddp/aspen/client/AspenClient.scala:385-392` (`createNewMetadataPool`)
- Test: `src/test/scala/org/aspen_ddp/aspen/client/internal/allocation/FsifIndexAllocationStrategySuite.scala`,
  plus a new small suite in the same file for the size limit

**Interfaces:**
- Consumes: `FsifIndexAllocationStrategy.prepareBitmap` (Task 3).
- In `createPoolObj`, when `config.backendConfig` is `FsifConfig(_, segmentCount)`, call
  `prepareBitmap(segmentCount, treeAlloc)` (the metadata group allocator) and merge its two entries into the
  pool-object map.
- `createNewMetadataPool` with an `FsifConfig` returns
  `Future.failed(IllegalArgumentException("FSIF is not supported for metadata pools"))` before any I/O.

- [ ] **Step 1: Write the failing tests**
  - `"a new FSIF pool records the strategy and a zeroed bitmap in its metadata group"`: call
    `createNewStoragePool(..., FsifConfig.forObjectSize(testIda, 4096, 3000), ...)`, on
    `StorageDeviceSetId.BootstrapStorageDeviceSetId`. Then read the pool state object:
    - `AllocationStrategyKey` holds `typeUUID`.
    - `decodeConfig` gives `3000`, and a bitmap pointer whose pool is a member of the default metadata group.
    - The bitmap has 3 zeroed segments.
    - `getStoragePool(...).allocationStrategy` is a `FsifIndexAllocationStrategy.Strategy`.
  - `"a new RocksDB pool has no allocation strategy"`: the `AllocationStrategyKey` is absent.
  - `"createNewMetadataPool refuses FSIF"`: it fails with `IllegalArgumentException`.
  - In `class FsifBitmapSizeLimitSuite extends IntegrationTestSuite`, overriding `testPoolMaxSliceSize` with a small
    limit such as `Some(1024)`: `"a bitmap larger than its allocator's object limit fails with ObjectSizeLimitExceeded"`.
    `prepareBitmap(64 * 1024, radiclePool.allocator)` in a transaction makes `commit()` fail with
    `ObjectSizeLimitExceeded`.
- [ ] **Step 2: Run them to verify they fail**
  Run: `sbt 'testOnly *FsifIndexAllocationStrategySuite *FsifBitmapSizeLimitSuite'`. Expected: the new tests fail.
- [ ] **Step 3: Implement both changes.**
- [ ] **Step 4: Run them to verify they pass**
  Run: `sbt 'testOnly *FsifIndexAllocationStrategySuite *FsifBitmapSizeLimitSuite'`. Expected: all pass.
- [ ] **Step 5: Commit** with the message `New FSIF pools get FsifIndexAllocationStrategy and a zeroed bitmap; refuse FSIF metadata pools`.

---

### Task 6: Enable the CLI, switch the FSIF integration suite, and clean up

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/cmdline/Main.scala:1002-1023`
- Modify: `src/test/scala/org/aspen_ddp/aspen/cmdline/MainSuite.scala:770-800`
- Modify: `src/test/scala/org/aspen_ddp/aspen/FsifIntegrationSuite.scala` (lines 83, 93, 181, 186-200, plus a new test)
- Delete: `src/test/scala/org/aspen_ddp/aspen/FsifTestIndexStrategy.scala`
- Modify: `CLAUDE.md:146-149`, `Todo.txt:4-21`, `docs/superpowers/specs/2026-10-29-fsif-backend-design.md:34-36`,
  `src/main/scala/org/aspen_ddp/aspen/server/store/backend/fsif/DESIGN.md:38` and `:807`

**Interfaces:**
- Remove `FsifPoolsEnabled`, `FsifPoolsRefused` and the `fsifEnabled` parameter.
- `validatePoolBackend(c: Args): Either[String, Unit]` additionally returns
  `Left("--backend fsif is not supported for metadata pools")` when `c.mode == "create-metadata-pool"`.

- [ ] **Step 1: Update the tests**
  - In `MainSuite`, drop `fsifEnabled = true` from the sizing test. Replace the "refuses until an allocation
    strategy" test with `"create-pool accepts --backend fsif"`, which parses successfully, and
    `"create-metadata-pool refuses --backend fsif"`, which fails to parse with the message on stderr.
  - In `FsifIntegrationSuite`:
    - Remove `userTypeFactories`, `setAllocationStrategy` and the class comment's mention of the test strategy.
    - Add `"FSIF pool allocations come from its bitmap and a deleted object's index is reused"`, using
      `segmentCount = 256`. Allocate two objects, which get distinct indices. Delete the first. A new allocation
      gets a bit that was free, and the data written there reads back correctly on every store subset
      (`readsWithStoresDown`).
- [ ] **Step 2: Run them to verify they fail**
  Run: `sbt 'testOnly *MainSuite *FsifIntegrationSuite'`. Expected: the CLI tests fail. The FSIF suite fails to
  compile only once the test strategy is deleted, so delete it as part of Step 3.
- [ ] **Step 3: Implement the CLI change, delete `FsifTestIndexStrategy.scala`, and update the docs:**
  - **CLAUDE.md:** replace the sentence about `create-pool --backend fsif` being refused with one line:
    "FSIF pools use `FsifIndexAllocationStrategy` (a per-pool bitmap KeyValue object in the metadata group; bits
    set/cleared in the transaction, or by a system DurableTask after `Transaction.deferAllocationRelease()`).
    Metadata pools can't use FSIF."
  - **FSIF docs:** remove the "DurableTask-based clearing" / "gated" wording, and point to the new design.
  - **Todo.txt:** remove the item.
- [ ] **Step 4: Run the full test suite**
  Run: `sbt test`. Expected: every test passes, and no run exceeds the 70-second hang threshold.
- [ ] **Step 5: Commit** with the message `Enable create-pool --backend fsif; FSIF suite uses the real allocation strategy`.
