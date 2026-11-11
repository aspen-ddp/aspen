## Project Overview

Aspen is a distributed data platform for building higher-level distributed applications, written in Scala 3.
It implements a distributed object storage system that uses transactions to allocate, update, and delete
one or more objects at a time with Atomic, Consistent, and Durable transaction guarantees.

## Module guides (read before working in a subsystem)

Package root: `src/main/scala/org/aspen_ddp/aspen/` (tests mirror it under `src/test/scala/...`). A module's
CLAUDE.md loads automatically only when a file under that main-tree directory is read. **When the work is test-only,
or spans modules, Read the relevant guides below explicitly.**

| Guide (relative to package root) | Covers |
|---|---|
| `client/CLAUDE.md` | AspenClient API, bootstrap/config, IDA data ops (append, ranged reads/writes) |
| `client/internal/CLAUDE.md` | Read/Transaction Managers and Drivers, opportunistic rebuild |
| `client/internal/transaction/CLAUDE.md` | TransactionBuilder pin rule, missed-update marking, UnackedCommits writing |
| `client/internal/allocation/CLAUDE.md` | Allocation strategies, FSIF index bitmap |
| `client/tkvl/CLAUDE.md` | TKVL trees: TierEngine, encodings, batches, requirements, caches, split/join |
| `common/metadata/CLAUDE.md` | Pool, group, host, device and device-set state objects, metadata layout, management tasks |
| `common/network/CLAUDE.md` | Codec/protobuf, messages, NetworkPlugin, NetworkCache, snapshots |
| `common/network/implementations/quic/CLAUDE.md` | QUIC plugin: streams, send budget, MTU, endpoint repair, WrongHost |
| `server/CLAUDE.md` | Server-wide: threading rules, DurableFiles, user jars, diagnostic logs |
| `server/host/CLAUDE.md` | Host threading model, StoreRouter/loader, device lifecycle, adoption, transfers, lost CRLs |
| `server/store/CLAUDE.md` | Frontend: locks, collisions, in-flight commits, admission, object cache, backends, reconcile |
| `server/transaction/CLAUDE.md` | Paxos/Tx protocol: votes, proofs, late resolution, UnackedCommits, stubs, status cache |
| `server/crl/CLAUDE.md` | Crash Recovery Log format, sizing, admission limit, applied stubs |
| `server/repair/CLAUDE.md` | ErrorTree/UnackedCommits repair scans, RepairProofs |
| `server/rebuild/CLAUDE.md` | Store rebuilds, acceptor-since |
| `vdisk/CLAUDE.md` | Virtual block devices and the TCMU frontend |

Further design docs: `docs/superpowers/specs/` (dated designs), `docs/safety`, `Architecture.md`,
`vdisk/DESIGN.md`, `server/store/backend/fsif/DESIGN.md`, `docs/host-threading-measurement.md`.

## Key Architectural Components
- **Objects**: all data lives in objects, identified by UUID, carrying a Revision (UUID of the last transaction to
  update it) and a Refcount (deleted when set to 0). DataObjects hold binary data; KeyValue objects hold binary
  Key/Value pairs
- **DataStores**: store replicas/slices of object data. Logical entities with no fixed physical location or on-disk
  format, identified by index within their StoragePool. Backends are pluggable: RocksDB, in-memory Map, FSIF
- **StoragePools**: identified by UUID, a fixed IDA, and exactly IDA-width DataStores. Objects allocated in the pool
  are dispersed across its stores
- **AllocationGroups**: allocate from member pools and child groups. `maxSliceSize` is the source of truth for
  object-size limits. Ordinary pools keep their ErrorTree/AllocationTree/UnackedCommits trees in metadata pools;
  metadata pools in the bootstrap pool; pool state objects always in the bootstrap pool (details:
  `common/metadata/CLAUDE.md`)
- **IDA**: Replication, ReedSolomon, RandomAccessReedSolomon. Appends, byte-range reads and ranged writes are
  supported by Replication and RandomAccessReedSolomon only; classic ReedSolomon fails them with
  `RangeReadNotSupported` / `AppendNotSupported` / `RangeWriteNotSupported` (details: `client/CLAUDE.md`)
- **ObjectPointer**: binary-encoded result of an allocation (UUID, pool, optional store-locating content). Must be
  supplied to read the object; intended to be embedded in other objects to form distributed data structures
- **Transactions**: the only way to modify objects. ACD guarantees, may update many objects, but **each object at
  most once per transaction**. At least one TransactionRequirement is required and all must hold to commit.
  Requirements are a sealed hierarchy so new kinds are handled everywhere
- **Finalization Actions**: run after commit; all must complete before servers may forget the transaction. Fast
  only (they hold server resources), idempotent, at-least-once
- **Durable Tasks** (`compute`): long-lived, crash-proof multi-step tasks. Each step is one multi-object transaction
  that does the work and records the step done; after a crash the next unfinished step restarts. Exactly-once
- **Hosts** (`server.Host`): a thin shell over `DeviceControl` (device management on one control thread) and
  `StoreRouter` (stores on their own executors)
- **VDisks** (`vdisk`): virtual block devices on Aspen, served to the kernel via TCMU

## Cross-cutting invariants
- **Pin rule**: every client transaction must contain a pin, a write that invalidates its own requirement once
  applied (a `DataUpdate`/`VersionBump` at a required revision, a `KeyValueUpdate` with a required revision, a
  `KeyRevision` on a key the update writes, a `RefcountUpdate` that changes the count). Allocations, locks and
  `DoesNotExist` are not pins. `TransactionBuilder` fails an unpinned one with `UnpinnedTransaction` unless
  `Transaction.replaySafe(reason)` marks it (replay must be provably harmless)
- Every transaction that allocates an object must also store the new ObjectPointer in another object
- Crash-only architecture: there is no clean shutdown process
- Logical addressing: messages go to logical entities (DataStores); the endpoint of the host currently holding one
  is looked up as the last step before sending
- Scale comes from ObjectPointers encoding location, not consistent hashing or sharding; stores migrate freely
  between machines and media
- Self-hosting: error logs, missed-update tracking, and system metadata (pools, stores, hosts, endpoints, devices)
  are Aspen objects. Clients start from `aspen-client-config.yaml`, fetch the bootstrap yaml via `GET /bootstrap`,
  then load the Radicle object (zeroed UUID), the root of all system metadata
- Server threading: callbacks never run on a shared pool; every piece of host state has one owner thread (the
  control thread or one store's executor), actor-style, and nothing blocks the control thread. Read
  `server/CLAUDE.md` before touching server code
- Administration is done by writing desired state into host/device objects that hosts poll, not by messages
- Collisions are expensive: design data structures and usage patterns to avoid them
- HLC timestamps (`common.HLCTimestamp`): NTP-compatible, guaranteed later than previously observed events; use
  them for cross-host time comparisons

## Building and Testing
- `sbt 'testOnly *TestName -- -z "test substring"'` - Run a specific test by substring
- Tests run in parallel inside sbt's JVM; `.jvmopts` sets a 4 GiB heap for the large-message QUIC tests
- Tests use `LockstepThreading` (deterministic); `ThreadedHostSuite`/`AdaptiveThreadedHostSuite` exercise real
  threads (see `server/host/CLAUDE.md`)

## Key Dependencies
- **Scaffeine**: Used for all caching operations
- **os-lib**: Should be used for all filesystem operations. java.nio is used only where os-lib has no equivalent
  (e.g. DSYNC channels, free-space queries); convert at the boundary with `os.Path.toNIO`
  (or `toIO` for `java.io.File`)
- **Netty 4.2 QUIC** (`netty-codec-native-quic`, BoringSSL): the QUIC network plugin
- **Protocol Buffers**: all encoding in `common.network.Codec` and one `codec.proto`

## Development Rules
- Prefer Scala 3 "quiet mode" syntax. Braces should still be used when they clearly enhance readability
- Prefer indented if/then/else syntax. Avoid single lines e.g. "if a == 1 then value" unless it enhances readability
- `cmdline` is a work-in-progress; treat its interfaces as unstable
- When changing a subsystem's behavior, update its module CLAUDE.md, not this file, unless an invariant above changes
