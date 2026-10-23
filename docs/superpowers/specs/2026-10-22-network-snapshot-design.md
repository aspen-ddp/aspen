# Network Snapshot Slow Path — Design

## Intent

Store→host and host→endpoint changes reach nodes through gossip, and gossip can be missed. Today nothing
corrects a node that missed one except the peer endpoint query (hosts only, and only when dials are failing)
and the `TxUnknownStore` / `WrongHost` reactions. This adds a slow, gossip-independent safety net: once a day
every node pulls a complete, system-wide picture of where every store lives and how to reach every host.

It also addresses the read-explosion problem that motivated the NetworkCache: a fresh node in a
10,000-host system would otherwise read every host and pool object individually. With the snapshot it does
two object reads.

## Goals

- A singleton `NetworkCachingService` (a `DurableService`, built like `RebalancingDurableService`) that every
  24 hours reads every host and every storage pool and overwrites two zstd-compressed DataObjects:
  - **Hosts:** HostId, name, address, dataPort, httpPort, storeTransferPort, and the timestamp of the
    HostState object it was read from.
  - **Stores:** StoreId, HostId, and the timestamp of the StoragePoolState's ConfigKey key-value entry.
- Every node — hosts and all clients (AmoebaFS, anything holding an `AspenClient` with a `MetadataManager`) —
  reads the snapshot once every 24 hours at a randomized time, and applies it.
- Applying a snapshot loads **everything** in it into the node's in-memory state and its NetworkCache, not just
  entries the node already knows about.
- The snapshot is authoritative as a complete listing: local entries absent from it are dropped if they are
  older than the snapshot.

## Non-goals

- Replacing gossip, the peer endpoint query, or `WrongHost`. Those remain the fast paths; this is the net
  beneath them.
- Real-time accuracy. A snapshot may be up to ~24h old; newer-wins timestamps make that harmless.
- Migrating already-bootstrapped systems (see "Existing systems" below).

## Anti-goals

- The Radicle must not grow. It is read frequently; the snapshot pointers live in the service's own state
  object.
- No allocation churn: the two snapshot objects are allocated once and **overwritten** in place on every run.
- 10,000 nodes must not hit the snapshot at the same moment, nor the service hit the cluster all at once.
- A snapshot must never regress a node: an entry it holds that is newer than the snapshot's is kept.

## Decided

### Writer: `NetworkCachingService`

- A `DurableServiceFactory`/`DurableService` pair with fixed `ServiceTypeUUID` and `ServiceUUID`, registered in
  the services tree by `Bootstrap` alongside the rebalancer and `SystemTaskExecutorService`.
- Bootstrap allocates the two snapshot DataObjects (empty snapshots) and the service's KV state object, which
  holds the pointers to both, so the pointers always exist and readers never handle an absent key.
- Each run:
  1. Records `scanStart`, an HLC timestamp taken before reading anything.
  2. Reads every host (hosts tree) and every storage pool (storage pools tree).
  3. Encodes each list (protobuf, in `codec.proto` via `Codec`, per project convention), zstd-compresses it, and
     overwrites the corresponding DataObject. `scanStart` is carried in each snapshot's payload.
     The two objects need not be overwritten in one transaction: readers apply each snapshot independently
     using its own `scanStart`.
- The state object also records when the last run completed, so a service that moves to another host (lease
  loss, crash) runs when the next run is due rather than restarting the 24h clock or running immediately.

### Readers: every `MetadataManager`

- Find the service state object through the services tree (as
  `RebalancingDurableService.readServiceStatePointer` does), read the two pointers from it, then read and
  decompress the two snapshots.
- **When:**
  - On startup, if the NetworkCache is empty, read immediately (as soon as an `AspenClient` is available), with
    no jitter: a fleet-wide cold start is not a practical concern. This includes the CLI and every other
    process without a persistent cache — short-lived processes benefit from the snapshot too.
  - Otherwise, at a random point within the next 24 hours, then every 24 hours after that.
- Processes without a persistent cache must actually benefit from the immediate read: the snapshot's host
  endpoints and store mappings must be usable without per-host or per-pool reads. Either applying the snapshot
  populates `MetadataManager`'s in-memory state directly, or `NoCache` becomes `NonPersistentCache` (holds
  entries in memory, never persists them). The builder picks whichever fits the implementation; if the
  current code can't use the snapshot this way, that is an issue to fix as part of this work.
- **Apply** (under the `MetadataManager` monitor, matching how gossip is applied):
  - Every snapshot entry newer than what the node holds (or not held at all) replaces / is added to the
    in-memory maps and timestamps, using the same newer-wins rule as `storeMoved` / `endpointChanged`.
  - Every local store mapping or host entry **not** in the snapshot whose timestamp is older than
    `scanStart - dropMargin` (default 10 minutes) is dropped from memory and from the cache. Others are kept.
    The margin exists because a transaction's timestamp is taken when it is built, not when it commits
    (`TransactionBuilder`), so a change committing during the scan can be absent from the snapshot while
    carrying a timestamp older than `scanStart`.
  - A dropped host goes through the existing `dropHostEntry` path so the plugin closes its connection; its
    timestamp is kept so a late gossip retransmit cannot resurrect it.
  - A snapshot host entry with a newer timestamp but the same endpoint (address and ports) only advances the
    held timestamp; the live entry and its connection are kept. Any HostState write advances its timestamp, so
    replacing would churn every connection daily.
  - Every snapshot host becomes a live host entry. This is cheap for QUIC; ZMQ opens a socket per entry, which
    is accepted since ZMQ is slated for removal.
  - Bootstrap stores and hosts, and the node's own HostId, are never added, replaced, or dropped by a snapshot
    (same exemption as cache seeding).
  - The NetworkCache is updated in one bulk call rather than per entry.
- `NetworkCache.bulkUpdate` must keep the newest entry when its input contains a key more than once (deferred
  fix from the NetworkCache work), and the cache needs a way to remove dropped host entries and remove stores
  in bulk.

## Left to the builder

- The zstd library (e.g. `zstd-jni`) and compression level.
- Protobuf message shapes for the snapshots, and whether the hosts/stores lists are one protobuf each or share
  a wrapper carrying `scanStart`.
- An iterator over the metadata trees (`MetadataTree` has only `get`/`preparePut` today) for the service's scan.
- Which pool the snapshot objects are allocated in (the bootstrap pool is the natural default, matching the
  other service state objects), and handling for a pool `maxObjectSize` smaller than the snapshot.
- How the service paces its reads of the host and pool trees so a run doesn't burst load on the cluster.
- The service's run period and the reader period: both default to 24 hours; a test seam like the
  rebalancer's `setTestPollPeriod` is expected. No CLI.
- Failure handling: a failed run or read logs and retries at the next scheduled time (or sooner, the builder's
  call); a reader that fails never clears local state.
- Whether a snapshot whose `scanStart` is no newer than the last one applied is skipped.

## Existing systems

Already-bootstrapped systems have no `NetworkCachingService` entry. Readers that find no service entry log at
debug level and do nothing; the system keeps working on gossip and the existing fallbacks.
