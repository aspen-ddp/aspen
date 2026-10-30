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
    through another instance can make warm instances return moved entries.
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
  - QUIC (`implementations/quic`, Netty 4.2): Mutual TLS on every connection, datagrams for
    encoded messages of 1024 bytes or less, a small round-robin pool of streams for larger ones, and messages
    dropped (never queued) when a connection's buffers are full. Payloads are not copied on send and are
    copied exactly once on receive.
- A pluggable `NetworkCache` persists store->host and host->endpoint mappings, scoped to the system id.
  - `NoCache` for the CLI; RocksDB for hosts (`<host-root>/cache/network`) and `amoebafs --network-cache-dir`
  - MetadataManager seeds from it at startup, trusts entries immediately, and writes every change back
- `NetworkCachingService` writes a daily snapshot of every host endpoint and store->host mapping. `NetworkSnapshotReader`
  applies it at startup when the cache was empty (always for `NoCache`) and otherwise at a random slot each day.
  Newer entries replace held ones; entries absent from it and older than its scan start by more than a 10 minute
  drop margin are dropped.
- QUIC repairs stale host endpoints: after `peerQueryDelay` (2 min) of failed dials to a host reported online,
  it asks `peerQueryFanout` (3) peers (clients ask their relays) over a datagram. Newer answers replace the entry.
- QUIC envelopes carry the intended HostId. A host receiving one meant for another HostId drops it and replies
  `WrongHost` (rate-limited per connection). A handshake certificate UUID mismatch is treated the same way.
  Either marks the entry known-wrong, drops sends to it, and starts the peer query immediately.
- `aspen host --load-jars=<version>` loads user RegisteredTypeFactory lists from every jar in `<host-dir>/jars/<version>/`
  (may be a symlink, e.g. `latest`) via `UserJarLoader`: one URLClassLoader, parent-first, `AspenTypeProvider` classes
  found with ServiceLoader. Duplicate UUIDs (including against built-ins) fail startup naming both classes and jars
- DataStores are pluggable. Currently only a RocksDB and in-memory Map are supported.

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
  - The CRL is used to store the persistent state required by the Paxos protocol
-  Lifecycle:
  - The client builds a TransactionDescription and sends it plus object data to the stores
  - The stores check all local state against the TransactionRequirements and determine their Commit/Abort decision
  - The CrashRecoveryLog is used to store this decision and Paxos is then used to arrive at an overall commit/abort decision
  - The client and all stores are informed of the final decision
  - If committed, FinalizationActions are then run. Once complete, the CRL content is dropped and the transaction is forgotten.
  - If the refcount of the object is set to zero, it is deleted

### Host and StorageDevice Management
- Most administration is done by reading and writing host and device objects containing the desired state
- Hosts will poll these instances and act on changes
  - This approach is preferred over sending messages directly to the host, since those messages could be lost
  - An example is seeing that a DataStore is marked for transfer in or out. When seen, the hoste will start the process
- Planned physical moves: `shutdownStorageDevice` makes Active stores Offline and the device ShuttingDown; its host
  closes the stores (each gets an empty `transferring-out` marker and its `crl_save.log`), writes `shutdown.state`
  (its HostId) in the device dir, and commits Shutdown. Each device dir also has `owning-host` (`server.DeviceMarkers`),
  so the 10 s scan decides without the network; only a dir with `shutdown.state` needs a device-state lookup. On the
  same host a Shutdown device waits for `restartStorageDevice`. Another host adopts it only when Shutdown and every
  Offline store dir has `transferring-out` + `crl_save.log` (or `transferred-in`), in one transaction rewriting the
  device, its pool entries and both HostStates, then rewrites `owning-host`, removes the markers and loads it
- A `crl_save.log` in a store's final directory is the only copy of its CRL state: if it fails to load (missing,
  truncated, or naming another store) the store is never served. It gets a `rebuild-required` marker, its entry is
  flipped to Rebuilding by the reconcile, and the directory is discarded before the rebuild. Adoption of a Shutdown
  device does the same for such stores instead of refusing the device, and adopts any `rebuild-required` directory as
  Rebuilding whatever its other markers. A network transfer validates the staged copy's save file before promotion and
  retries on failure, since the source still holds the state
- A backend fail-stop during a store's close marks it `rebuild-required` instead of closing it cleanly (no
  `transferring-out`, no `crl_save.log`). Such a store is never transferred: its transfer is aborted so it is rebuilt
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
