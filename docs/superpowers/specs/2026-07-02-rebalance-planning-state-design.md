# Rebalance Planning State Gathering — Design

Date: 2026-07-02

## Goal

Implement `org.aspen_ddp.aspen.common.rebalancing.State.getStateForRebalancePlanning`, which
gathers a point-in-time snapshot (`PlanState`) of all the device and pool state needed by the
rebalancing planner. The function reads the state of every storage device in a given
`StorageDeviceSetState`, determines the set of storage pools those devices host, reads each pool's
state, and assembles a `PlanState`.

Because a large system may contain many devices and pools, the reads must not all be issued at
once. This design also introduces a generic, reusable concurrency-limited parallel execution
helper and implements the device and pool reads in terms of it.

## Scope

- **In scope:** the state-gathering function and the generic concurrency limiter.
- **Out of scope:** the plan-calculation (the actual rebalancing decision logic) and the execution
  phase. Those are separate future specs.

## Part 1 — Generic Concurrency Limiter

### Location

`src/main/scala/org/aspen_ddp/aspen/common/util/package.scala`, as a sibling to the existing
`runSequentially`.

### Signature

```scala
def runBoundedParallel[A, B](items: Seq[A], maxConcurrent: Int)
                            (f: A => Future[B])
                            (implicit ec: ExecutionContext): Future[Seq[B]]
```

### Behavior

- Invokes `f` over `items` with at most `maxConcurrent` invocations in flight at any instant.
- Results are returned in the same order as `items` (matching `Future.sequence` semantics),
  regardless of completion order.
- Fails fast: if any invocation fails, the returned `Future` fails with that error. Invocations
  already started are allowed to run to completion; no guarantee is made about which not-yet-started
  items are attempted after a failure.
- `require(maxConcurrent > 0)`.
- An empty `items` yields `Future.successful(Vector.empty)`.

### Implementation

A fixed pool of `min(maxConcurrent, items.length)` worker chains pull from a shared atomic index.
Each worker runs `f` on the item at its claimed index, stores the result at that index, then claims
the next index, until the index is exhausted. This produces a true sliding window — a new
invocation begins the instant one completes — rather than a chunked-barrier approach where the
slowest item in a batch stalls the following batch.

```scala
def runBoundedParallel[A, B](items: Seq[A], maxConcurrent: Int)
                            (f: A => Future[B])
                            (implicit ec: ExecutionContext): Future[Seq[B]] =
  require(maxConcurrent > 0, s"maxConcurrent must be > 0, got $maxConcurrent")
  val arr = items.toIndexedSeq
  if arr.isEmpty then
    Future.successful(Vector.empty)
  else
    val results   = new AtomicReferenceArray[B](arr.length)
    val nextIndex = new AtomicInteger(0)

    def worker(): Future[Unit] =
      val i = nextIndex.getAndIncrement()
      if i >= arr.length then
        Future.unit
      else
        f(arr(i)).flatMap: b =>
          results.set(i, b)
          worker()

    val workers = (0 until math.min(maxConcurrent, arr.length)).map(_ => worker())
    Future.sequence(workers).map(_ => Vector.tabulate(arr.length)(results.get))
```

`AtomicReferenceArray` plus the `Future.sequence` join provides both correct result ordering and
safe cross-thread memory visibility (each index is written by exactly one worker; the final read
happens-after all workers complete).

New imports required in `package.scala`: `java.util.concurrent.atomic.{AtomicInteger,
AtomicReferenceArray}` (and `ExecutionContext`/`Future` if not already imported).

## Part 2 — `getStateForRebalancePlanning`

### Location

`src/main/scala/org/aspen_ddp/aspen/common/rebalancing/State.scala` — fill in the existing `= ???`
stub. The `Store`/`Pool`/`Device`/`PlanState` case classes already present in the `State` object are
kept unchanged.

### Signature

```scala
def getStateForRebalancePlanning(client: AspenClient,
                                 storageDeviceSet: StorageDeviceSetState,
                                 maxConcurrentReads: Int = DefaultMaxConcurrentReads): Future[PlanState]
```

`DefaultMaxConcurrentReads` is a constant in the `State` object, value **20**. The single
`maxConcurrentReads` limit covers both read phases; because the phases run one after the other, at
most `maxConcurrentReads` reads are ever in flight at one time.

### Flow

All async work runs on `client.clientContext` (`given ExecutionContext = client.clientContext`).

1. **Guard the set level.** If `storageDeviceSet.level != 0`, return
   `Future.failed(new IllegalArgumentException(...))`. Rebalancing operates on level-0 leaf sets
   (consistent with the level-0 restriction enforced by `StorageDeviceSetState.selectDeviceForRebuild`).
   An *empty* level-0 set is not an error — it yields an empty `PlanState`.

2. **Read all device states** with the concurrency limiter:
   ```scala
   runBoundedParallel(storageDeviceSet.memberDevices, maxConcurrentReads)(client.getStorageDeviceState)
   ```

3. **Collect unique pool ids** from every store on every device:
   ```scala
   deviceStates.flatMap(_.stores.keys).map(_.poolId).toSet
   ```

4. **Read each pool's KV object** with the concurrency limiter. A single read per pool yields both
   the pool config (for the IDA) and the per-store usage keys:
   ```scala
   runBoundedParallel(poolIds.toSeq, maxConcurrentReads): poolId =>
     client.getStoragePoolPointer(poolId).flatMap(client.read).map(kvos => poolId -> kvos)
   ```
   Collected into a `Map[PoolId, KeyValueObjectState]`.

5. **Per-store size** is read from the owning pool's KV object (identical to `selectDeviceForRebuild`):
   ```scala
   poolKvos.contents
     .get(StoragePoolState.getStoreUsageKey(storeId.poolIndex))
     .map(vs => byte2long(vs.value.bytes))
     .getOrElse(0L)
   ```

6. **Assemble `PlanState`:**
   - Build a `Store(storeId, size, entry.status)` from each device's `StoreEntry`.
   - `devices`: each `StorageDeviceState` → `Device(deviceId, itsStores)`, where `itsStores` is built
     directly from that device's own `stores` map (so a store mid-transfer is attributed to the
     correct device on each side).
   - `pools`: for each collected pool id → `Pool(poolId, StoragePoolState(kvos).ida, storesForThatPool)`,
     where `storesForThatPool` is every discovered store whose `poolId` matches.

### Edge cases and notes

- **Mid-transfer stores.** A store transferring between two devices can appear as `TransferringOut`
  on the source and `TransferringIn` on the destination. In a pool's store map these collapse to a
  single entry (last-wins). This is acceptable for a first pass because transferring stores are not
  valid rebalancing move candidates; a code comment will note it.
- **Guard exception type.** `IllegalArgumentException` is used rather than the client package's
  `AllocationError`, since this operation is not an allocation.
- **No unit test.** The function is straightforward I/O orchestration and testing it would require
  maintaining a mock `AspenClient`, which is not worth the cost. Correctness is covered by
  compilation and by the reuse of the already-exercised read patterns.

## Verified APIs (as of this design)

- `AspenClient.getStorageDeviceState(id): Future[StorageDeviceState]`
- `AspenClient.getStoragePoolPointer(poolId): Future[KeyValueObjectPointer]` (`private[aspen]`)
- `AspenClient.read(pointer: KeyValueObjectPointer): Future[KeyValueObjectState]` (single-arg form
  from `ObjectReader`, delegates to `read(pointer, "")`)
- `AspenClient.clientContext: ExecutionContext`
- `StorageDeviceState.stores: Map[StoreId, StorageDeviceState.StoreEntry]`;
  `StoreEntry(status: StoreStatus, transferDevice: Option[StorageDeviceId])`;
  `StoreStatus` enum: `Initializing, Active, TransferringIn, TransferringOut, Rebuilding`.
- `StorageDeviceSetState.level: Int`, `.memberDevices: List[StorageDeviceId]`.
- `StoreId(poolId: PoolId, poolIndex: Byte)`.
- `StoragePoolState(kvos: KeyValueObjectState): StoragePoolState`; `.ida: IDA`;
  `StoragePoolState.getStoreUsageKey(poolIndex: Byte): Key`.
- `org.aspen_ddp.aspen.common.util.byte2long`.

## Files changed

- `src/main/scala/org/aspen_ddp/aspen/common/util/package.scala` — add `runBoundedParallel`.
- `src/main/scala/org/aspen_ddp/aspen/common/rebalancing/State.scala` — implement
  `getStateForRebalancePlanning` and add the `DefaultMaxConcurrentReads` constant.
