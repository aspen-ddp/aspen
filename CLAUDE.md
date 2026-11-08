## Project Overview

Aspen is a distributed data platform for building higher-level distributed applications, written in Scala 3. 
It implements a distributed object storage system that uses transactions to allocate, update, and delete
one or more objects at a time with Atomic, Consistent, and Durable transaction guarantees.

## Key Architectural Components
- **Objects**:
  - All data within Aspen is stored within objects
  - Identified by UUID
  - Contain a Revision & Refcount
    - The revision is the UUID of the last transaction to update the object
    - Objects are deleted when their refcount is set to 0
  - There are two types DataObjects and KeyValue objects
    - DataObjects contain binary data
    - KeyValue objects contain Key/Value pairs. Both Keys and Values are binary data.
- **DataStores**:
  - Stores replicas/slices of object data
  - Logical entities with no fixed physical location or on-disk format
  - Identified by numerical index within its enclosing StoragePool
- **StoragePools**:
  - Identified by UUID
  - Contains a fixed IDA and the number of DataStores exactly matches the width of the IDA
  - Objects allocated within the pool have their data dispersed amongst the enclosing stores
- **AllocationGroups**:
  - Allocate from member pools and child groups. An optional `maxSliceSize`, fixed at creation and allowed only
    with `IDAPolicy.ExactMatch`, is a floor: members must have no limit or one at least as large, and an unlimited
    group accepts only unlimited members
  - `maxSliceSize` is the source of truth for pools and groups; every `maxObjectSize` is derived from it and the IDA
  - Metadata layout: an ordinary pool's ErrorTree/AllocationTree live in a metadata pool reached through a metadata
    group; metadata pools' trees live in the bootstrap pool; the bootstrap pool's live in itself. All pool state
    objects stay in the bootstrap pool
  - `StoragePoolState.isMetadataPool` / `AllocationGroupState.isMetadataGroup` are fixed at creation. A pool or child
    group may join a group only when its flag equals the group's (`MetadataMembershipMismatch`)
  - Bootstrap creates `aspen-initial-storage-pool-metadata` and `aspen-default-storage-pool-metadata`; the group id is
    in the Radicle (`DefaultStoragePoolMetadataAllocator`, `AspenClient.getDefaultMetadataGroupId`).
    `createNewStoragePool(..., metadataGroup)` defaults to it; `createNewMetadataPool` and CLI `create-metadata-pool`,
    `create-pool --metadata-group`, `create-allocation-group --metadata` build on it
  - `MissedUpdateFinalizationAction` disables missed-update tracking on an ErrorTree write only when the transaction
    references the pool the entry is about (in practice the bootstrap pool)
- **VDisks** (`vdisk` package):
  - A virtual block device on Aspen. `DiskCatalog` names disks in registry namespace `vdisk`; CLI `create-disk`,
    `list-disks`, `grow-disk`, `delete-disk`
  - Per disk: a KV config object (geometry plus a `DataObjectTKVL` block map) and a DataObject fence. The owner takes
    ownership by overwriting the fence and every commit carries a `SharedRevisionLock` on it, so a newer owner fences
    out older ones
  - Write-back: WRITE completes from memory; durable at FLUSH/FUA. Block objects are the largest multiple of the
    logical block size <= the allocator's `maxObjectSize`
  - `vdisk-server` drives the kernel's TCMU interface directly through FFM (`vdisk.tcmu`: netlink, configfs, the
    `/dev/uioN` ring, SCSI emulation in `vdisk.tcmu.scsi`) and owns `org.kernel.TCMUService1` on the system D-Bus so
    targetcli sees `user:aspen`. It runs as root and refuses to start while tcmu-runner holds that name.
    `LockableBlockDevice` is the frontend seam
- **Information Dispersal Algorithm (IDA)**:
  - Defines the replication/erasure-coding strategy used to provide robustness against data loss
  - Currently Replication, ReedSolomon and RandomAccessReedSolomon are supported
  - RandomAccessReedSolomon lays data out in rows of `k * cellSize` bytes with an 8-byte length trailer, so
    DataObjects in such pools support efficient appends (`Transaction.append` with an `AppendPosition`) and
    byte-range/tail reads (`readRange`, `readAppendPosition`, `readRanges`), all served by a single `ByteRanges`
    read request (ranges plus an optional tail). `readRanges` and `readRangedWriteState` therefore read one
    consistent revision. They also support ranged writes
    (`Transaction.rangedWrites`): the caller supplies a `RangedWriteState` (from `RangedWriteState.fromObject` or
    `ObjectReader.readRangedWriteState`) and stores receive only sparse XOR deltas of the changed bytes.
    Stores read deltas through `EncodedAppendDelta`, which parses only the header and region table and XORs
    straight from the encoded buffer, so checking and applying never copy the delta.
    Replication supports all of them too. Classic ReedSolomon does not: those operations fail with
    `RangeReadNotSupported` / `AppendNotSupported` / `RangeWriteNotSupported`
- **ObjectPointer**:
  - Allocations result in a binary-encoded ObjectPointer.
  - Intended to be embedded within other objects to form distributed data structures
  - This pointer must be supplied in order to retrieve object data
  - Contains the Object UUID, StoragePool, and optional additional content needed by stores to locate object data
- **Transactions**:
  - Provides the only means to modify object content
  - Provides Atomic, Consistent, and Durable guarantees
  - May update multiple objects at once
    - Any individual object can only be modified once. Multiple updates to the same object is not permitted.
  - At least one TransactionRequirement must be supplied
    - All supplied requirements must be satisfied for the transaction to commit
- **Finalization Actions**:
  - Specify required actions that must be completed before transaction state may be forgotten by the servers
  - Executed after a transaction successfully commits.
  - All FinalizationActions must complete before transaction state may be deleted
  - Can be used for arbitrary "fast" operations such as logging which DataStores failed to properly process a
    transaction or insering a newly allocated node into the next tier up in a distributed B-tree
  - Not suitable for long-lived operations as they "clog up" resources on the Servers
  - Should be idempotent operations as they may be executed multiple times
  - Provides At-Least-Once guarantees
- **Durable Tasks**:
  - Provides support for long-lived and/or "crash-proof", multi-step tasks.
    - The key is that each step in the task uses a multi-object transaction to both preform the task's logical
      step and to update the state of the durable task to note that the step has been successfully completed.
    - If a crash occurs, the durable task state is used to simply pickup where it left off. The 
      next as-yet-to-be-completed step in the task is simply restarted.
    - Example use: allocating an inode and inserting it into a directory in a distributed file system. Both 
      operations must complete successfully to avoid a corrupted file system but cannot be done in a single
      transaction. Using a DurableTask ensures that the operations proceed successfully and in the correct order.
  - Provides Exactly-Once guarantees
- **Hosts** (`server.Host`, components in `server.host`): a thin shell over `DeviceControl` (device management on
  one control thread) and `StoreRouter` (stores on their own executors); see "Host threading model" below
  
## Key Design Aspects
- Building and using Distributed Data Structures is the primary use case for Aspen-based systems
  - Linked lists, Tiered Key Value Lists (similar to B-Trees), and other data structures can be used to
    efficiently store and retrieve data in Aspen
  - Every Transaction that allocates an object should also store the resulting ObjectPointer in another object
    to prevent data loss in the event of a crash.
  - A TKVL tree is KeyValue-object or DataObject based (`TreeKind` in its `Root`); shared logic is `TierEngine` over a
    pluggable `NodeEncoding`. `DataObjectTKVL` is more compact, stores values only (no per-key revisions/requirements)
    and uses whole-node revision checks: intended for single-writer / low-contention use. A second update to one of its
    nodes in a transaction fails with `NodeAlreadyModified`; `getContainingNode`/`TieredKeyValueListNode` are KV-only.
  - `prepareUpdates(inserts: List[TKVLInsert], deletes)` changes many keys in one transaction on either kind, updating
    each node once. `TKVLInsert.requireDoesNotExist` makes allocate-and-insert retry loops safe.
  - Each `RootManager` owns a `TierCache` (default `LruTierCache`: 5 nodes per tier >= 1, plus the root when on
    tier >= 1; `NoCache` for tests and finalization actions). Stale entries cost only reads, except that `splitTree`
    through another instance can make warm instances return moved entries. `LruTierCache(anyRoot = true)` also
    keeps tier-0 and empty roots and shares one read among concurrent root misses; writes then read the root fresh.
    Only for a single writer that remembers its own changes (a vdisk's `BlockMap`, whose `refresh` clears it)
- Aspen is designed and implemented largely as a logical system
  - Messages are addressed to logical entities like DataStores rather than physical hosts
  - The last step prior to sending a message is to look up the address of the network endpoint currently
    hosting the logical entity. The message is then sent to that physical host which will in turn deliver
    the message to the logical entity
- Unlike most scalable distributed storage systems, Aspen achieves scale not through consistent hashing
  or sharding, but rather through binary ObjectPointers that encode the location of the object within the
  logical storage system.
- Because DataStores do not have a fixed location, they may be freely migrated between physical machines and 
  storage devices.
  - This allows for optimization of distributed data structures in various ways. For example, placing the upper
    tiers of a distributed B-Tree on low-latency NVMe storage and the bottom tier on HDD. Allows fast lookups
    of cheaply stored bulk data
- Aspen is a self-hosting system.
  - Transactions, object storage and retrieval, and DataStore implementation are foundational
  - Most other aspects of Aspen are built on top of distributed objects stored in the Aspen system. Such as:
    - Logging of object update errors
    - System Metadata such as the available StoragePools, DataStores, network endpoints, physical media, etc
  - Clients start from a local `aspen-client-config.yaml` (system id, credentials, and a cached list of bootstrap hosts)
    and fetch the current bootstrap yaml (bootstrap StoragePool and network endpoints) from any host via `GET /bootstrap`
    - From this, the "Radicle Object", with a zeroed UUID, is loaded
      - This object serves as the ultimate root for locating all data stored within an Aspen system
      - It contains embedded ObjectPointers to the roots of all critical distributed data structures used
        to store system metadata such as physical storage hosts, netowrk configurations, storage pool
        configurations, etc
- Aspen uses a Crash-Only architecture. By intentional design, there is no clean shutdown process.
- The Network Layer is designed to be pluggable via `common.network.NetworkPlugin`. QUIC is the only plugin:
  - QUIC (`implementations/quic`, Netty 4.2): Mutual TLS on every connection. Every message goes on a stream, which
    QUIC retransmits: a lost datagram waited for Aspen's own retries, and bursts overflow receive buffers even on
    loopback. Only 1-byte keepalives are datagrams. Messages of 1024 encoded bytes or less share one small-message
    stream (the pool when it is full); larger ones use a round-robin pool of `streamsPerConnection` (4). Messages are
    dropped (never queued) when over budget. UDP sockets request 16 MiB buffers; a smaller OS grant is logged. With
    epoll, packets go out in UDP GSO batches and come in through GRO and `recvmmsg` (`udpOffload`), and path MTU
    discovery, with don't-fragment set, raises them from 1200 to `maxUdpPayloadBytes` (1452).
    Payloads are not copied on send and are copied exactly once on receive.
  - Send buffering (`QuicConfig`): one host-wide `SendBudget` (a quarter of the heap, at least 256 MiB) shared by all
    connections; a connection may queue below its floor (256 KiB) always, or below its ceiling (a quarter of that
    budget, at least 64 MiB) while the shared budget has room. A stream whose writes wait over 500 ms is logged. The
    pre-handshake queue has its own bound (16 MiB) outside the shared budget
- A pluggable `NetworkCache` persists store->host and host->endpoint mappings, scoped to the system id.
  - `NoCache` for the CLI; RocksDB for hosts (`<host-root>/cache/network`) and `amoebafs --network-cache-dir`
  - MetadataManager seeds from it at startup, trusts entries immediately, and writes every change back
- `NetworkCachingService` writes a daily snapshot of every host endpoint and store->host mapping. `NetworkSnapshotReader`
  applies it at startup when the cache was empty (always for `NoCache`) and otherwise at a random slot each day.
  Newer entries replace held ones; entries absent from it and older than its scan start by more than a 10 minute
  drop margin are dropped.
- QUIC repairs stale host endpoints: after `peerQueryDelay` (2 min) of failed dials to a host reported online,
  it asks `peerQueryFanout` (3) peers (clients ask their relays). Newer answers replace the entry.
- QUIC envelopes carry the intended HostId. A host receiving one meant for another HostId drops it and replies
  `WrongHost` (rate-limited per connection). A handshake certificate UUID mismatch is treated the same way.
  Either marks the entry known-wrong, drops sends to it, and starts the peer query immediately.
- `aspen host --load-jars=<version>` loads user RegisteredTypeFactory lists from every jar in `<host-dir>/jars/<version>/`
  (may be a symlink, e.g. `latest`) via `UserJarLoader`: one URLClassLoader, parent-first, `AspenTypeProvider` classes
  found with ServiceLoader. Duplicate UUIDs (including against built-ins) fail startup naming both classes and jars
- `AbortVoteReport` logs a host's abort votes by failed requirement once a second while they happen
- `BackendTiming` logs each host's write stages about once a second while writes happen: the store's view of a commit,
  RocksDB's pool wait and write, and FSIF's io wait, journal write and force, data force, copy-on-write and backpressure
- DataStores are pluggable: RocksDB, in-memory Map, and FSIF (`server.store.backend.fsif`, a flat file of fixed-size
  segments located by a 4-byte storePointer, with a journal and checkpointed index). FSIF freezes writes on ENOSPC
  and keeps serving reads. FSIF pools use `FsifIndexAllocationStrategy` (a per-pool bitmap KeyValue object in
  the metadata group; bits set/cleared in the transaction, or by a system DurableTask after
  `Transaction.deferAllocationRelease()`: one task per transaction, covering all its FSIF pools). Metadata pools
  can't use FSIF.

## Subsystem Details

### Transaction Design
- TransactionRequirements are varied. They can specify simple things like required revision, refcount, key-value revision,
  key doesn't exist, etc. It is intended to be expanded over time and is implemented as a sealed abstract class to ensure
  that new additions will be properly handled in the codebase. 
- Paxos is used to ensure transaction consensus
  - Transactions are initiated by the client but the Paxos participants are the DataStores hosting only one
    of the targeted objects. This is done to reduce the number of participants and does not affect correctness.
  - The Paxos algorithm itself is unmodified but some clever message handling is done to reduce the number of
    round-trips and optimize data flow. The details are only important when working on transaction message
    handling.
  - The CRL is used to store the persistent state required by the Paxos protocol. Each storage device has its own
    (`<device>/crl`, opened when the device loads, sized by `crl-num-streams`/`crl-max-file-size-mb` in its device
    config) shared by its stores; a dead CRL thread stalls only that device. An unloaded device is not loaded again
    until its CRL has shut down (`LocalDevices.closingCrls`). The sizing is fixed when the device is created: stream
    files on disk that differ from it in count or size make `openCRL` throw, so the CRL is treated as lost
  - Stores hold requirement locks from a commit vote until the transaction resolves. `SharedRevisionLock` is a
    non-exclusive requirement on an object's revision: many transactions may hold it, but it collides with any write
    to that object
  - A KeyValue update's operations lock what they write, as requirements do (`server.store.KVWrites`): each key an
    Insert or Delete names (an absent key through `absentKeyLocks`, shared with `DoesNotExist`/`MayExist`), and the
    whole object for a min/max/left/right change. Blind writes of one key collide, so stores applying committed
    transactions in different orders cannot diverge
  - A prepare that reaches a store after its transaction was finalized is not applied there: the store relies on
    missed-update repair. A collision is an ordinary abort vote, even with an already-completed transaction, except
    one with a lock holder the transaction is built on (it requires the revision the holder writes): the store then
    withholds its vote until the holder releases its locks, or for `Tx.CollisionWait` (500 ms, checked at heartbeat),
    so a single writer's next transaction need not abort while a store is behind on the previous one's outcome
  - A prepare for a transaction a store does not know, whose update an object there already shows (its revision, or
    a key's for a KeyValueUpdate without a required revision: `RequirementsApplyer.appliedBy`), proves it committed.
    The store answers `TxResolved(committed)` instead of a vote (`Tx.proveCommitted`) and applies nothing again, so a
    recovery driver for a recovered commit vote, after the other stores finalized and forgot the transaction, cannot
    decide abort on votes against the commit's own result and drop the update the recovering store holds
  - A driver's data-less prepare can overtake the client's prepare carrying a store's data. The store withholds its
    vote until the data, a client's prepare, or the outcome arrives, or until `Frontend.MissingDataWait` (5 s) passes:
    missing data then gets an abort vote. A vote given stands, but later data is still used to apply a commit
  - A commit decision can overtake the data too. The store holds the commit until the data, a client's prepare, or
    TxFinalized arrives, then applies it and sends TxCommitted, so the driver marks no missed update for that store
    if the data beat the finalizer's `errorTimeout` (2 s)
  - A store that misses `errorTimeout` is marked in its pool's ErrorTree for every object it hosts that the transaction
    changes (a lock changes nothing, so there is nothing to repair and the object's timestamp would not catch up). A client's
    `MissedUpdateMarker` writes one batch per pool at a time and merges waiting entries by key, so a slow spell makes
    batches larger rather than adding colliding writes
  - A lost TxFinalized is recovered by the store itself: a Tx idle for `Host.txHeartbeatTimeout` gets a driver, which
    learns the outcome from the stores' status caches, so `Host.txStatusCacheDuration` must outlast that timeout. At high volume the cache is costly (about 175 bytes
    per transaction, capped at `DefaultMaxEntries`), so these stay short: 5 s and 7 s with the host's 1 s heartbeat
  - Every referenced pool's stores receive the prepare and check only the requirements of the objects they host (plus
    non-object requirements). The driver tallies each object's votes from its own pool's stores
  - A store keeps a committed transaction's CRL state until its backend commits succeed; a failed commit keeps it (and
    the pinned object state, still served) while the store is loaded
  - One in-flight transaction commit per object (`Frontend.commitInFlight`): a transaction touching an object whose
    commit is in flight, whether or not it builds on it, withholds its vote with no locks and no timer until the commit
    persists, then checks its requirements against the state as it is then. A follower resolved committed meanwhile
    holds its commit (queued per object, oldest first) until earlier commits of its objects persist. Locks still
    release when a commit is applied in memory: the in-flight marker orders followers. After a failed backend commit
    every transaction on that object votes abort (`BackendCommitFailed`) until the store reloads. Objects a
    transaction only checks or locks get no backend commit, so concurrent `SharedRevisionLock` holders never wait
  - Backpressure (`CrlAdmission`, one per device, shared by its stores): a store admits a prepare for a transaction it
    has not seen (not admitted or recovered by its Frontend, nor finalized in the host-wide status cache) for its
    description, object data and CRL record, and drops it silently while the device's admitted bytes are at or past
    half a CRL stream (`SimpleCRL.admissionLimitBytes`). Object data arriving later is charged once per object without
    a check. Everything else (known transactions' prepares, accepts, resolutions, finalizations, heartbeats, status
    requests) is handled. Nothing is exempt. Bytes are released when the entry is deleted, a Tx is forgotten without
    one, or the store closes; a Tx waiting on an in-flight commit keeps its admission. Recovered transactions are
    counted too, without a check. Accepted gap: a retransmit past the mark while a store awaits its crl_save.log is
    dropped
-  Lifecycle:
  - The client builds a TransactionDescription and sends it plus object data to the stores
  - The stores check all local state against the TransactionRequirements and determine their Commit/Abort decision
  - The CrashRecoveryLog is used to store this decision and Paxos is then used to arrive at an overall commit/abort decision
  - The client and all stores are informed of the final decision
  - If committed, FinalizationActions are then run. Once complete, the CRL content is dropped and the transaction is forgotten.
  - If the refcount of the object is set to zero, it is deleted

### Host threading model
- The model deliberately departs from ordinary Scala Future usage: callbacks never run on a shared pool, and each
  piece of state has a single owner thread (the control thread or one store's executor), actor-style. See the
  scaladoc of `HostThreading.scala`
- `HostThreading` (`server.host`) decides where a host's work runs. It is a `Host` constructor parameter, chosen in
  code. The default strategy, which `Main` passes, is `StoreThreadPool`: each store's executor is its own
  `StoreMailbox` (a FIFO with at most one drain at a time, so the store's tasks never overlap and keep their order),
  scheduled onto one of the `aspen-store-N` daemon threads. By default the pool is fixed: up to
  `aspen host --store-threads` threads (default a quarter of the CPUs, at most 4), created lazily, each new store on
  the thread with the fewest stores; stores never move. Fewer busy threads beat many idle ones: waking a parked thread
  for each task multiplied CPU per operation, and fixed pools of 2 to 4 threads beat one per CPU and both adaptive
  policies tried (docs/host-threading-measurement.md). `StoreThreadPool.Adaptive` (code only, parked until revisited)
  runs `StoreBalancer` instead: it packs stores by load, grows on drain-wait congestion and moves stores between threads;
  a move only changes where the mailbox's next drain is queued. Its known problems are in `StoreThreadPool`'s scaladoc
  and `Todo.txt`. Everything touching a
  `Store`, `Frontend` or `Tx` runs there, so they take no locks. The exception is `Store.transactionDrivers`, a
  `@volatile` immutable Map so `hasTransactions`/`logTransactionStatus` may read it from any thread;
  `TransactionDriver`s keep their own locks for their timer threads
- Each store's object cache is its own LRU, owned by its executor, under one host byte budget (`HostObjectCache`): an
  over-budget store evicts its own oldest entries down to the budget, or posts a shed to the store holding the host's
  oldest; a shed frees to `maxBytes / 32` below the budget. `Frontend` pins states held by transactions and in-flight commits; a failed
  backend commit stays pinned and served
- The hot path (network messages, I/O completions, heartbeats, repairs) goes through `StoreRouter` straight to the
  store's executor: a concurrent map lookup on the calling thread, then the task, which runs only if that store is
  not closed when the task runs (tasks queued ahead of a close still run). Unknown stores are answered from the
  calling thread; offline ones are silent, as are all unknown ones until the startup scan's LoadStores are handled.
  A store whose load is under way is silent too: `loading` from `StoreLoader.startOpen` until install, and
  `awaitingDevice` while its device's shutdown-marker lookup (or adoption) decides. Neither mark is `offline`
- Backend opens are backend-neutral: `BackendOpener.open` (`server.store.backend`) returns a Future and decides where
  the open runs (RocksDB inline, FSIF on `FsifOpenEc`); `StoreLoader.startOpen` handles an already-completed one
  inline and otherwise posts `BackendOpened`. A rebuild's staging store opens through the same opener (passed by
  `StoreRebuildFactory.createRebuild`). The host and the rebuild see only `Backend.hasFailed`/`isWriteFrozen`,
  `BackendStateLost` and `Backend.isOutOfSpace`; only `DefaultBackendOpener` names concrete backends
- Store lifecycle: the control thread builds the `Store` and calls `router.install`, where ownership passes to the
  executor; a close removes it from the router (marking it offline) and then runs the close on its executor. Before
  install and after close the backend and directory belong to the control side, which otherwise reads only
  `storeId`, `path`, `crlSaveFile` and the thread-safe `hasFailed`/`isWriteFrozen`. `release` frees the slot
- Device management (the `server.host` classes `DeviceControl` wires) runs on one control thread, `aspen-host-control`,
  with no locks. Rules (scaladoc of `HostThreading.scala`): the only `given ExecutionContext` is the control context;
  never block the control thread on a Future; other threads read control state only through the `@volatile`
  `HostStoreView` (an immutable snapshot the control thread republishes on every change: `containsStore`,
  `getStoreIds`, `getDevicePath`, `isRepairable`) or the router's concurrent map
  (`hasTransactions`, `logTransactionStatus`); store and control state stay apart: control posts store work to the
  executor (`StoreRouter.onStore`) and gets results back through a Future whose callbacks name the control context
- Callbacks name the control context. Work run elsewhere (backend opens, client transactions on the host's `ec`) names
  its context; timers and other non-Future callbacks post to `controlEvents` before touching control state
- Tests use `LockstepThreading`: everything posted waits on one FIFO drained by `testingOnlyHandleEvents`, stores run
  inline, and the control context is the suite's serial `ec`, so integration suites keep their exact ordering.
  `ThreadedHostSuite` runs the TestNetwork's host on a fixed `StoreThreadPool` (`IntegrationTestSuite.testThreading`),
  and `AdaptiveThreadedHostSuite` on an adaptive one that moves stores between threads mid-test

### Host and StorageDevice Management
- Most administration is done by reading and writing host and device objects containing the desired state
- Hosts will poll these instances and act on changes
  - This approach is preferred over sending messages directly to the host, since those messages could be lost
  - An example is seeing that a DataStore is marked for transfer in or out. When seen, the hoste will start the process
- Planned physical moves: `shutdownStorageDevice` makes Active stores Offline and the device ShuttingDown; its host
  closes the stores, leaving their CRL state live in the device CRL (no per-store marker or save file), writes
  `shutdown.state` (its HostId) in the device dir, and commits Shutdown. Each device dir also has `owning-host`
  (`server.DeviceMarkers`), so the 10 s scan decides without the network; only a dir with `shutdown.state` needs a
  device-state lookup. On the same host a Shutdown device waits for `restartStorageDevice`; a restart that cancels a
  shutdown in flight has the reconcile load the closed stores again (`DeviceShutdown.reloadClosedStores`). Another host
  adopts it, in one transaction rewriting the device, its pool entries and both HostStates, then rewrites
  `owning-host` and loads it: it opens the device CRL and each store recovers its transactions from it
- Adoption needs no shutdown: a drive pulled from a running or crashed host is in the state that host's crash leaves
  it in. A device whose `shutdown.state` (or, absent that, `owning-host`) names another host is looked up and adopted
  when the tree records it as owned by that host and not Failed, `<device>/crl` exists (if it holds stores), and
  every Offline or Active entry has a directory. Physical presence is trusted: there is no fencing of the old host.
  A running old host whose reconcile finds the tree naming another owner relinquishes the device
  (`DeviceShutdown.relinquishDevice`): it closes the stores, unloads the device, writes nothing, and keeps the
  directory disowned in memory. The adopter nudges it with `CheckStorageDevice`
- `crl_save.log`, `transferring-out` and `transferred-in` are for network transfers only: a transfer out closes the
  store with `extractCrl`, copying its state from the device CRL into `crl_save.log` to travel with it. Such a file is
  then the only copy of that state: if it fails to load (missing, truncated, or naming another store) the store is
  never served. It gets a `rebuild-required` marker, its entry is flipped to Rebuilding by the reconcile, and the
  directory is discarded before the rebuild. Adoption of a device does the same for a `transferring-out` store
  without a valid save file instead of refusing the device, and adopts any `rebuild-required` directory as Rebuilding
  whatever its other markers. A network transfer validates the staged copy's save file before promotion and retries on
  failure, since the source still holds the state
- A lost device CRL does the same to every store on the device: if `<device>/crl` is absent from a device holding store
  dirs, or `openCRL` throws, each store dir gets `rebuild-required`, the old directory is kept as `crl.lost-<uuid>`, and
  a fresh CRL is opened (`DeviceDiscovery.openDeviceCrl`)
- A backend fail-stop during a store's close marks it `rebuild-required` instead of closing it cleanly (no
  `crl_save.log`). Such a store is never transferred: its transfer is aborted so it is rebuilt
- A store's device-CRL entries are dropped for good (`CrashRecoveryLog.dropStore`, durable) only once it cannot be
  served: when a rebuild discards a `rebuild-required` directory (`StoreRebuilds.discardLostStore`: marker durable,
  then drop durable, then the directory removed and the rebuild started), or when an unloaded store's save file still
  holds them, or when the reconcile deletes the directory of a store transferred away (`DeviceReconciler`, retried
  on failure). Without the marker on disk, entries stay in the device CRL. A transfer out reads them
  (`readStoreState`, leaving them live) into `crl_save.log` (loaded or not, `StoreTransfers.closeForTransferOut`)
  and deletes them from the device CRL (`deleteTransactions`) only once that file is durable: a crash or failed save
  before that leaves the device CRL holding them
- `server.DurableFiles` forces files and their directories so markers and moves survive a pulled disk. That durability
  holds on Linux only, the supported host platform: directory fsync is a no-op on Windows and a plain fsync on macOS,
  each logging one warning

### Hybrid Logical Clock (HLC) Timestamps
- Provides timestamps that are guaranteed to be later than some previously observed event(s)
- Similar to vector clocks but much simpler and in an NTP-compatible timestamp
- Used extensively in Aspen to facilitate reliable timestamp comparisons that are robust against many traditional
  pitfalls associated with comparing pure NTP-managed timestamps.

## System Usage
- **aspen.client.AspenClient**: 
  - Serves as the primary interface for applications using Aspen object storage
  - Supports object allocation, transaction building, and retrieving object data
- Transaction collisions are to be avoided by system designers
  - There is a built-in collision handling mechanism but it is inefficient for heavy collisions
  - Application design and usage patterns should take this into account and reduce the potential
    for collisions whenever possible

## Building and Testing
- `sbt 'testOnly *TestName -- -z "test substring"'` - Run a specific test by substring
- Tests run in parallel inside sbt's JVM; `.jvmopts` sets a 4 GiB heap for the large-message QUIC tests

## Key Dependencies
- **Scaffeine**: Used for all caching operations
- **os-lib**: Should be used for all filesystem operations. java.nio is used only where os-lib has no equivalent
  (e.g. DSYNC channels, free-space queries); convert at the boundary with `os.Path.toNIO`
  (or `toIO` for `java.io.File`)
- **Netty 4.2 QUIC** (`netty-codec-native-quic`, BoringSSL): Used for the QUIC network plugin

## Development Rules
- Prefer Scala 3 "quiet mode" syntax. Braces should still be used when they clearly enhance readability
- Prefer indented if/then/else syntax. Avoid single lines e.g. "if a == 1 then value" unless it enhances readability
- `cmdline` is a work-in-progress; treat its interfaces as unstable
