# Network Snapshot Slow Path Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A singleton `NetworkCachingService` writes daily, zstd-compressed snapshots of every host endpoint and
every store→host mapping; every node applies them on startup (empty cache) and once a day thereafter.

**Architecture:** A new package `org.aspen_ddp.aspen.common.network.snapshot` holds the snapshot format, the
`DurableService` writer (modelled on `RebalancingDurableService`), and a reader that fetches the snapshots and
hands them to a new `MetadataManager.applySnapshot`. The network plugins start the reader next to
`metadataManager.setAspenClient` and close connections for host entries the snapshot retires.

Two spec choices are settled here: `applySnapshot` populates `MetadataManager`'s in-memory maps directly, so
`NoCache` processes benefit without a `NonPersistentCache`; and the service enumerates via the existing
`client.listHosts()` / `listStoragePools()` (namespaced registry), so no metadata-tree iterator is needed.

**Tech Stack:** Scala 3, ScalaPB (`codec.proto` / `Codec`), zstd-jni, RocksDB, ScalaTest.

**Spec:** `docs/superpowers/specs/2026-10-22-network-snapshot-design.md`

## Global Constraints

- The Radicle is not modified; snapshot pointers live in the service's KV state object.
- The two snapshot DataObjects are allocated once (by Bootstrap) and overwritten in place on every run.
- Snapshot entries never replace a held entry that is at least as new (newer-wins on HLC timestamps).
- Drop rule: a local entry absent from a snapshot is dropped only if `snapshot.scanStart - entryTimestamp > dropMargin`;
  `dropMargin` defaults to 10 minutes.
- Bootstrap stores and hosts, and the node's own HostId, are never added, replaced, or dropped by a snapshot.
- On startup, read immediately (no jitter) if the NetworkCache loaded nothing — this includes `NoCache`, i.e. the CLI.
  Otherwise the first read is at a random point within 24h, then every 24h.
- Same endpoint (address, dataPort, httpPort, storeTransferPort) with a newer timestamp only advances the timestamp;
  the live entry is kept.
- Service run period 24h; readers' period 24h. Test seams, no CLI.
- Systems with no `NetworkCachingService` entry: readers log at debug and do nothing.
- Follow CLAUDE.md: quiet syntax, os-lib, all encode/decode in `Codec` + `codec.proto`.

## Review Focus

1. **A service run where any host or pool read fails** — the run must abort without overwriting; a partial snapshot
   would make every reader drop the missing entries. (Task 2 test.)
2. **A snapshot containing a host/store a node has a pending lookup for** — the lookup must still resolve, with the
   newer of the two winning. (Task 3 test.)
3. **The Bootstrap-time empty snapshot (`scanStart` = 0)** — applying it must drop nothing and must not count as
   "applied" in a way that blocks the first real one. (Task 3 test.)
4. **`scheduleRandomlyWithinWindow` truncates to the window's unit** — a `Duration(24, HOURS)` window gives
   hour-granular delays; pass the window in milliseconds. (Task 4 test asserts the delay is drawn below the window.)
5. **Retired QUIC entries** — a host whose endpoint the snapshot changed must have its outbound connection closed
   and its peer-query clocks reset, exactly as for gossip. (Task 4 test.)

---

### Task 1: Snapshot format, zstd, and NetworkCache removals

**Files:**
- Modify: `build.sbt` — add `"com.github.luben" % "zstd-jni" % "1.5.6-4"`
- Modify: `src/main/protobuf/codec.proto`
- Modify: `src/main/scala/org/aspen_ddp/aspen/common/network/Codec.scala`
- Create: `src/main/scala/org/aspen_ddp/aspen/common/network/snapshot/NetworkSnapshot.scala`
- Modify: `src/main/scala/org/aspen_ddp/aspen/common/network/NetworkCache.scala`, `RocksDBNetworkCache.scala`
- Modify: `src/test/scala/org/aspen_ddp/aspen/common/network/MetadataManagerFixture.scala` (`RecordingNetworkCache`)
- Test: `src/test/scala/org/aspen_ddp/aspen/common/network/snapshot/NetworkSnapshotSuite.scala`,
  `src/test/scala/org/aspen_ddp/aspen/common/network/NetworkCacheSuite.scala`

**Interfaces:**
- Produces:
  - proto: `NetworkSnapshotStore { UUID pool_id = 1; uint32 pool_index = 2; UUID host_id = 3; uint64 timestamp = 4; }`,
    `NetworkHostSnapshot { uint64 scan_start = 1; repeated NetworkCacheHost hosts = 2; }`,
    `NetworkStoreSnapshot { uint64 scan_start = 1; repeated NetworkSnapshotStore stores = 2; }`
  - `final case class HostSnapshot(scanStart: HLCTimestamp, hosts: List[CachedHost])`
  - `final case class StoreSnapshot(scanStart: HLCTimestamp, stores: List[CachedStore])`
  - `object NetworkSnapshot`:
    `encodeHosts(s: HostSnapshot): Array[Byte]`, `decodeHosts(b: Array[Byte]): HostSnapshot`,
    `encodeStores(s: StoreSnapshot): Array[Byte]`, `decodeStores(b: Array[Byte]): StoreSnapshot` —
    protobuf via `Codec`, then zstd (level 3). Decoding fails (throws) on corrupt input.
  - `NetworkCache.removeHost(hostId: HostId): Unit`
  - `NetworkCache.bulkUpdate(content: NetworkCacheContent, removedStores: Iterable[StoreId] = Nil, removedHosts: Iterable[HostId] = Nil): Unit`
    — newer-only puts as today, removals unconditional, one RocksDB `WriteBatch`; when `content` repeats a key the
    newest entry wins. `NoCache` ignores all of it.

- [ ] **Step 1: Write the failing tests**

`NetworkSnapshotSuite`:
- `"hosts round-trip through encode/decode"` — two `CachedHost`s, scanStart `HLCTimestamp(42)`; decoded equals input.
- `"stores round-trip through encode/decode"` — `CachedStore(StoreId(PoolId(uuid), 3.toByte), hostId, HLCTimestamp(7))`.
- `"empty snapshots round-trip"` — `HostSnapshot(HLCTimestamp.Zero, Nil)` and `StoreSnapshot(HLCTimestamp.Zero, Nil)`.
- `"encoded output is compressed"` — 1000 hosts with identical name/address: `encodeHosts(s).length` is less than
  `Codec.encode(...)`'s uncompressed length / 5.
- `"corrupt bytes fail to decode"` — `an[Exception] should be thrownBy NetworkSnapshot.decodeHosts(Array[Byte](1,2,3))`.

`NetworkCacheSuite` (RocksDB):
- `"removeHost deletes the host"` — put, remove, reopen: `load().hosts` is empty.
- `"bulkUpdate applies removals"` — seed store s1 and host h1; `bulkUpdate(NetworkCacheContent(Nil, Nil), List(s1), List(h1))`;
  reopen: both gone.
- `"bulkUpdate keeps the newest of duplicate keys"` — content lists h1 at ts 9 then h1 at ts 5; loaded timestamp is 9.
  Same for a store.

- [ ] **Step 2: Run to verify they fail**

Run: `sbt 'testOnly *NetworkSnapshotSuite *NetworkCacheSuite'`
Expected: compile failure (`NetworkSnapshot`, `removeHost` undefined).

- [ ] **Step 3: Implement** the proto messages, `Codec` encode/decode functions (existing naming pattern),
`NetworkSnapshot`, `removeHost`, and the new `bulkUpdate` signature on the trait, `NoCache`, `RocksDBNetworkCache`
and `RecordingNetworkCache` (record removals so later tasks can assert on them: `removedHosts`, `removedStores`,
`bulkUpdates` lists).

- [ ] **Step 4: Run to verify they pass**

Run: `sbt 'testOnly *NetworkSnapshotSuite *NetworkCacheSuite *MetadataManagerNetworkCacheSuite'`
Expected: all pass.

- [ ] **Step 5: Commit** — `NetworkSnapshot format; NetworkCache removals and bulk dedupe`

---

### Task 2: NetworkCachingService (writer) and Bootstrap registration

**Files:**
- Create: `src/main/scala/org/aspen_ddp/aspen/common/network/snapshot/NetworkCachingService.scala`
- Modify: `src/main/scala/org/aspen_ddp/aspen/server/store/Bootstrap.scala` (allocate + register, next to the rebalancer)
- Modify: `src/main/scala/org/aspen_ddp/aspen/server/TypeFactories.scala`
- Test: `src/test/scala/org/aspen_ddp/aspen/common/network/snapshot/NetworkCachingServiceSuite.scala`
  (an `IntegrationTestSuite`, structured like `RebalancingServiceSuite`: `userTypeFactories`, `makeExecutor()`)

**Interfaces:**
- Consumes: Task 1's `NetworkSnapshot`, `HostSnapshot`, `StoreSnapshot`.
- Produces:
  - `object NetworkCachingService extends DurableServiceFactory`:
    - fresh fixed `ServiceTypeUUID`, `ServiceUUID` (generate once, hard-code)
    - state keys `HostSnapshotKey = Key(Array[Byte](0))`, `StoreSnapshotKey = Key(Array[Byte](1))` (values:
      `DataObjectPointer` bytes), `LastRunKey = Key(Array[Byte](2))` (value: `HLCTimestamp.asLong`, 8 bytes BE)
    - `DefaultRunPeriod = 24 hours`, `DefaultPollPeriod = 10 minutes`
    - `private[aspen] def setTestPeriods(client: AspenClient, poll: Duration, run: Duration): Unit` — weak-keyed by
      client, like `RebalancingDurableService.setTestPollPeriod`
    - `def initialServiceState(hostSnapshot: DataObjectPointer, storeSnapshot: DataObjectPointer): Map[Key, Array[Byte]]`
      (LastRun = `HLCTimestamp.Zero`)
    - `def emptyHostSnapshot: Array[Byte]`, `def emptyStoreSnapshot: Array[Byte]` (encoded, scanStart Zero) —
      what Bootstrap writes into the two DataObjects
    - `def readSnapshots(client: AspenClient): Future[Option[(HostSnapshot, StoreSnapshot)]]` — `None` when the
      services tree has no entry for `ServiceUUID`; otherwise reads the state object, then both DataObjects.
  - `class NetworkCachingService(client, statePointer, pollPeriod, runPeriod) extends DurableService`

**Run algorithm** (on each poll, when `HLCTimestamp.now - lastRun >= runPeriod`; non-concurrent, guarded by a
`stopped` flag exactly like the rebalancer's `reconcile`):
1. `scanStart = HLCTimestamp.now`.
2. `client.listHosts()`, then `getHostStateAndTimestamp` for each **sequentially** → `CachedHost(..., timestamp)`.
3. `client.listStoragePools()`, then `getStoragePoolStateAndTimestamp` for each sequentially → one `CachedStore`
   per `stores` index (`StoreId(poolId, index.toByte)`, `entry.hostId`, the ConfigKey timestamp).
4. If any read in 2–3 fails, log a warning and abort the run: write nothing, leave LastRun unchanged.
5. One transaction: read the state object and both DataObjects, `overwrite` both (required revisions from the
   reads), and update `LastRunKey` to `scanStart`.

- [ ] **Step 1: Write the failing tests** (test periods: poll 100 ms, run 1 hour unless stated)
- `"bootstrap registers the service with empty snapshots"` — `readSnapshots(client)` yields
  `Some((HostSnapshot(Zero, Nil), StoreSnapshot(Zero, Nil)))` before any executor runs.
- `"a due run writes every host and every store"` — start executor; await (poll) until hosts snapshot's scanStart
  `> Zero`; assert hosts contain `HostId.BootstrapHostId` with its HostState's address/ports, and stores contain
  `StoreId(bootstrapPoolId, i)` for every bootstrap store, each mapped to the host in the pool state.
- `"the snapshot objects are overwritten, not reallocated"` — record the two pointers from the state object before
  and after a run; they are equal.
- `"a recent run defers the next one"` — set LastRun to now; after several polls, scanStart is still Zero.
- `"a failed host read aborts the run"` — register a host name in the namespaced registry whose HostState pointer
  cannot be read (follow `RebalancingServiceSuite.addBogusMemberDevice`'s approach); after several polls, both
  snapshots are still empty and LastRun is Zero.
- `"readSnapshots is None without a service entry"` — delete the entry from the services tree in a transaction;
  result is `None`.

- [ ] **Step 2: Run to verify they fail**

Run: `sbt 'testOnly *NetworkCachingServiceSuite'`
Expected: compile failure.

- [ ] **Step 3: Implement** the factory, service, Bootstrap allocation (`allocateData(emptyHostSnapshot)`,
`allocateData(emptyStoreSnapshot)`, `allocate(initialServiceState(...).toList)`, `ServiceEntry` with
`UnclaimedHostId`, added to `servicesTree`), and add the factory to `TypeFactories.factories`.

- [ ] **Step 4: Run to verify they pass**

Run: `sbt 'testOnly *NetworkCachingServiceSuite *RebalancingServiceSuite *DurableServiceSuite'`
Expected: all pass (the latter two guard against Bootstrap/services-tree regressions).

- [ ] **Step 5: Commit** — `NetworkCachingService: daily host and store snapshots`

---

### Task 3: MetadataManager.applySnapshot

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/common/network/MetadataManager.scala`
- Test: `src/test/scala/org/aspen_ddp/aspen/common/network/MetadataManagerSnapshotSuite.scala`
  (`AnyFunSuite with Matchers with MetadataManagerFixture`, like `MetadataManagerNetworkCacheSuite`)

**Interfaces:**
- Consumes: Task 1's `HostSnapshot`, `StoreSnapshot`, `NetworkCache.bulkUpdate(content, removedStores, removedHosts)`.
- Produces:
  - `def cacheWasEmpty: Boolean` — true when the startup `networkCache.load()` returned no stores and no hosts
    (always true for `NoCache`, and when load threw).
  - `def applySnapshot(hosts: HostSnapshot, stores: StoreSnapshot, dropMargin: Duration): List[T]` — returns the
    host entries it replaced or dropped, for the caller to close. Never throws (same containment rules as
    `endpointChanged`: a failing `createHostEntry` puts that host back to never-looked-up and continues).

**Apply rules** (whole method under the monitor; one `networkCache.bulkUpdate` call at the end, guarded like the
other cache helpers):
- Skip bootstrap stores/hosts and `selfHostId` throughout.
- Hosts, per snapshot entry with timestamp newer than `hostTimestamps` (or none held):
  - resolved entry, same endpoint → advance `hostTimestamps` only; cache entry gets the new timestamp
  - resolved entry, different endpoint → new entry via `createHostEntry` (fresh `EvictingQueue[Message](1)`), old one
    returned in the result
  - pending lookup (`Left(phl)`) → record as `phl.gossiped` if newer (build a `HostEndpointChanged`)
  - unknown → create the entry (load everything)
- Stores, per snapshot entry newer than `storeTimestamps` (or none held): set mapping and timestamp.
- Drops: a resolved non-bootstrap, non-self host, or a non-bootstrap store mapping, absent from the snapshot whose
  held timestamp `t` satisfies `snapshot.scanStart - t > dropMargin` → removed from `hosts`/`stores` (host entry
  returned in the result); its timestamp is **kept** so late gossip cannot resurrect it. Pending lookups are
  never dropped. Each snapshot uses its own `scanStart` (hosts vs stores).

- [ ] **Step 1: Write the failing tests** (`dropMargin = 10.minutes`; timestamps built with
  `HLCTimestamp(ms << 16)` so margins are real milliseconds)
- `"unknown hosts and stores are loaded with no lookup"` — empty manager; apply; `getHostEntryOrQueueMessage(store0, …)`
  returns the snapshot's address; `client.lookups` and `client.poolLookups` empty.
- `"older snapshot entries do not replace newer held ones"` — seed via cache at T; snapshot entry at T-1 with another
  address; address unchanged, result empty.
- `"a newer entry with a new endpoint replaces and returns the old one"`.
- `"a newer entry with the same endpoint only advances the timestamp"` — same entry instance (`eq`) still held;
  result empty; `mgr.hostTimestamp(remoteHostId)` is the new value; the cache's bulk update carries the new timestamp.
- `"entries absent from the snapshot and older than the margin are dropped"` — held at scanStart − 11 min: host
  dropped (returned), store mapping gone, cache `removedHosts`/`removedStores` contain them.
- `"entries within the margin are kept"` — held at scanStart − 9 min: kept.
- `"a dropped host is not resurrected by older gossip"` — after the drop, `endpointChanged` with the dropped entry's
  timestamp returns None and `peekHostEntry` is None.
- `"bootstrap and self entries are never touched"` — snapshot names the bootstrap host with another address and
  omits bootstrap stores; manager built with `selfHostId`; all unchanged, nothing returned.
- `"a pending host lookup resolves with the newer of snapshot and read"` — start a lookup (send to an unknown host),
  apply a snapshot entry at T+5, complete the lookup at T: entry has the snapshot's address.
- `"the empty bootstrap snapshot drops nothing"` — `HostSnapshot(Zero, Nil)`/`StoreSnapshot(Zero, Nil)` against a
  seeded manager: nothing dropped, result empty.
- `"cacheWasEmpty"` — true for `NoCache` and an empty `RecordingNetworkCache`, false for a seeded one.

- [ ] **Step 2: Run to verify they fail**

Run: `sbt 'testOnly *MetadataManagerSnapshotSuite'`
Expected: compile failure.

- [ ] **Step 3: Implement** `cacheWasEmpty` and `applySnapshot` per the rules above.

- [ ] **Step 4: Run to verify they pass**

Run: `sbt 'testOnly *MetadataManager*'`
Expected: all pass.

- [ ] **Step 5: Commit** — `MetadataManager: apply network snapshots`

---

### Task 4: NetworkSnapshotReader, plugin wiring, docs

**Files:**
- Create: `src/main/scala/org/aspen_ddp/aspen/common/network/snapshot/NetworkSnapshotReader.scala`
- Modify: `src/main/scala/org/aspen_ddp/aspen/common/network/implementations/quic/QuicNet.scala` (`startIoThread`, shutdown)
- Modify: `src/main/scala/org/aspen_ddp/aspen/common/network/implementations/zmqnet/ZMQNet.scala` (`startIoThread`, shutdown)
- Modify: `CLAUDE.md` (network bullets), `Todo.txt` (remove the slow-path item)
- Test: `src/test/scala/org/aspen_ddp/aspen/common/network/snapshot/NetworkSnapshotReaderSuite.scala`,
  `src/test/scala/org/aspen_ddp/aspen/common/network/implementations/quic/QuicNetSnapshotSuite.scala`

**Interfaces:**
- Consumes: Task 2's `NetworkCachingService.readSnapshots`; Task 3's `applySnapshot`, `cacheWasEmpty`.
- Produces:
  - `class NetworkSnapshotReader[T <: MetadataManager.HostEntry](mm: MetadataManager[T], fetch: () => Future[Option[(HostSnapshot, StoreSnapshot)]], tasks: BackgroundTaskManager, onRetired: T => Unit, period: Duration = 24.hours, dropMargin: Duration = 10.minutes)(using ExecutionContext)`
    - `def start(): Unit` — if `mm.cacheWasEmpty`, read now; then schedule the next read at a random point within
      `period` (window passed in **milliseconds**), and after each read schedule the next one `period` later.
    - `def stop(): Unit` — cancels the scheduled task; a read in flight applies nothing after `stop`.
    - A read: `fetch()`; `None` → `logger.debug`; `Some` → skip each snapshot whose `scanStart` is not newer than the
      last one this reader applied, apply via `mm.applySnapshot`, call `onRetired` on each returned entry
      (outside the manager's monitor, each guarded with `NonFatal`). A failed fetch or decode logs a warning;
      the schedule continues.
  - `object NetworkSnapshotReader { def start[T <: MetadataManager.HostEntry](client: AspenClient, mm: MetadataManager[T], onRetired: T => Unit): NetworkSnapshotReader[T] }`
    — wires `fetch = () => NetworkCachingService.readSnapshots(client)` and `client.backgroundTaskManager`.
- QuicNet: `startIoThread` starts the reader once (alongside the recheck task); `onRetired` does what
  `endpointChanged` does for the old entry — `failingSince.remove`, `lastQueried.remove`, `closeOutbound(old)`.
  The reader is stopped on shutdown.
- ZMQNet: same start/stop; `onRetired` is a no-op (ZMQ does not act on `HostEndpointChanged` today either).

- [ ] **Step 1: Write the failing tests**

`NetworkSnapshotReaderSuite` (fake `fetch` recording calls; a real `MetadataManager` from the fixture; a
`BackgroundTaskManager` test double that records requested delays and lets the test fire the task):
- `"an empty cache reads immediately"` — `start()` calls fetch once before any task fires.
- `"a seeded cache waits for its random slot"` — no fetch on `start()`; one scheduled task whose delay is
  `< period` and was requested in milliseconds.
- `"each read schedules the next one period later"`.
- `"a snapshot no newer than the last applied is skipped"` — fetch returns the same snapshot twice; `applySnapshot`'s
  effect (a recorded `onRetired` or cache bulk update) happens once.
- `"no service entry is a quiet no-op"` — fetch returns `None`; manager unchanged; next read still scheduled.
- `"a failed fetch keeps the schedule"`.
- `"stop cancels the schedule"`.

`QuicNetSnapshotSuite` (use `QuicNetFixture` hosts):
- `"a snapshot with a new endpoint closes the old connection"` — node A sends to B (connection up); A's manager
  applies a snapshot moving B to C's address with a newer timestamp via a reader with a fake fetch; A's next send
  reaches C's listener / A has no outbound connection to B's old address. Use whatever the fixture already exposes
  for asserting connection closes (see `QuicNetWrongHostSuite`).

- [ ] **Step 2: Run to verify they fail**

Run: `sbt 'testOnly *NetworkSnapshotReaderSuite *QuicNetSnapshotSuite'`
Expected: compile failure.

- [ ] **Step 3: Implement** the reader and the plugin wiring. Update CLAUDE.md's network bullets with one bullet:
the daily `NetworkCachingService` snapshot, applied immediately when the cache is empty and at a random daily slot
otherwise, with the 10-minute drop margin. Remove the slow-path item from `Todo.txt`.

- [ ] **Step 4: Run the full suite**

Run: `sbt test`
Expected: all pass (baseline 1276 plus this plan's new tests).

- [ ] **Step 5: Commit** — `NetworkSnapshotReader: apply snapshots on startup and daily`
