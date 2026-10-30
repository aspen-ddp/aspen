# Design: iSCSI target via TCMU (Aspen virtual disks)

**Date:** 2026-10-30
**Status:** Draft

## Intent

Add an application, alongside AmoebaFS, that serves Aspen-backed virtual block devices to Linux initiators over
iSCSI. The kernel's LIO target and `tcmu-runner` handle iSCSI. A small C `tcmu-runner` handler plugin forwards SCSI
block commands over a Unix domain socket to a JVM daemon, which stores the disk's contents in Aspen.

The first workload is an ordinary Linux filesystem (ext4 or xfs) on the disk. Success means it behaves like any
other Linux disk that is somewhat slower because of the network, erasure coding and transaction overhead. Failure,
even if it technically works, means unusually long pauses or other artifacts that reveal it is something other
than a disk.

`features/iscsi-target.txt` is the starting sketch. This design keeps its tcmu-runner + C shim + Unix socket bridge
but departs from it where noted, most importantly by not processing requests one transaction at a time.

## Goals

- Serve multiple named virtual disks, each created by a CLI command and served by a long-running daemon, following
  the AmoebaFS pattern (`create-filesystem` / `amoebafs <name>`).
- Disk-cache write semantics: WRITE completes once the data is in the daemon's memory. FLUSH (SYNCHRONIZE CACHE)
  and FUA writes complete only once the covered data is committed to Aspen. After a crash, flushed data is intact
  and unflushed data may be lost, which is the standard contract filesystems are written against.
- Strictly single-writer: at any moment at most one daemon can commit changes to a disk's objects. This is
  enforced by Aspen transactions (epoch fencing), not by convention.
- Failover to a second daemon through standard iSCSI multipath with ALUA active/passive, where the protocol allows
  it. The fencing guarantees safety even when the old owner is still alive.
- Thin provisioning: block objects are allocated on first write. UNMAP releases whole block objects. Online grow.
- No artificial delays under normal operation: no I/O waits on anything it does not depend on (see Performance
  principles).
- The JVM side sits behind a narrow block-device interface, so the tcmu-runner/socket bridge can later be replaced
  by a JVM that drives the TCMU ring directly through the Java foreign-memory API without touching the Aspen side.

## Non-goals

- Snapshots and clones. They are expected later, so the layout should not preclude them, but nothing is built now.
- Shrinking a disk.
- Multiple concurrent writers, or active/active serving of one disk.
- Our own iSCSI protocol implementation. LIO does iSCSI.
- The foreign-memory TCMU frontend itself. Only the seam for it is in scope.
- Backward compatibility with persisted data.

## Anti-goals (failure even if it "works")

- Two daemons both committing to the same disk, even briefly, including a partitioned old owner after a takeover.
- Acknowledging a FLUSH or FUA write whose data is not committed.
- A read returning data older than an already-acknowledged write to the same range (read-your-writes within the
  daemon).
- Stalls caused by the design rather than by the storage: a global lock or single in-flight transaction, a FLUSH
  that rewrites every dirty object from scratch, whole-object rewrites for 4 KiB writes, a hard dirty-cache cliff,
  or synchronous metadata work on the write acknowledgment path.

## Constraints and fixed decisions

### Storage configuration

- IDA: `RandomAccessReedSolomon`, width 16, read threshold 10, write threshold 12. This is fixed.
- Pool backend: FSIF. Pools are created with `create-pool --backend fsif --max-object-size 5120000` (5,000 KiB).
  `FsifConfig.forObjectSize` already derives the segment payload size from `ida.minSliceSizeFor`, which for
  RandomAccessReedSolomon includes the 8-byte trailer row padding (tested in `IDAMaxObjectSizeSuite`). The pool
  therefore accepts objects of exactly 5,120,000 bytes, and no FSIF change is needed. The 500 KiB per-slice data
  size can be changed later. Smaller segments enlarge the stores' in-memory index, and 500 KiB is the chosen
  balance.
- Block objects are DataObjects of exactly `blockObjectSize` = the allocator's `maxObjectSize` (5,120,000 bytes
  = 1250 x 4 KiB here). Disk byte offset `o` lives in block object `o / blockObjectSize` at offset
  `o % blockObjectSize`. `blockObjectSize` is recorded per disk at creation and never changes. Any allocator is
  accepted (5,120,000 is just this setup's value), but creation fails unless its `maxObjectSize` is a multiple of
  the disk's logical block size (designer's call).
- The block-index -> `DataObjectPointer` map is a `DataObjectTKVL` (integer keys). This fits its single-writer
  intent. Block objects are allocated from an `ObjectAllocator` over the FSIF pool or group.
- Small reads and writes use the RandomAccessReedSolomon byte-range operations (`readRange`/`readRanges`,
  `Transaction.rangedWrites` with a `RangedWriteState`) instead of whole-object I/O.

### Write semantics

- Write-back cache semantics, as above. The daemon reports a volatile write cache (WCE=1) so initiators send
  FLUSH, and honors FUA.
- Ordering: SCSI allows commands to complete in any order, and Linux relies only on FLUSH/FUA for ordering. The
  daemon must still guarantee read-your-writes: once a WRITE has completed, a later READ of that range returns it.

### Single-writer fencing (agreed)

- Each disk has a descriptor (a KeyValue object) that holds an owner epoch and the owner's identity.
- Every transaction that modifies the disk's data or map carries a `RevisionLock`
  (`Transaction.lockRevision`) on the descriptor at the revision the owner observed when it took ownership. A lock
  is a requirement, not a write, so concurrent data transactions don't modify the descriptor.
- Taking ownership is one transaction that rewrites the descriptor with a new epoch and owner. From then on, every
  in-flight or later commit by the previous owner fails its requirement and aborts.
- A daemon serves I/O for a disk only after its takeover transaction has committed. When one of its commits fails
  the epoch requirement, it has been fenced. It must fail every outstanding and later I/O for that disk, drop its
  caches, and report the ALUA state as standby/unavailable.
- **Builder must verify:** that many concurrent transactions each holding a `RevisionLock` on the same descriptor
  revision do not collide with one another in the store's locking. If they do, add a non-exclusive
  revision-check requirement (the requirement hierarchy is designed to be extended) rather than serializing
  commits.

### Failover

- Model the HA behavior on `tcmu-runner`'s existing exclusive-lock support for ALUA implicit failover (as used by
  its rbd handler: the handler `lock` / `unlock` / `get_lock_state` callbacks). Acquiring the tcmu lock maps to the
  takeover transaction, and lock state maps to "my epoch is current".
- Standard multipath behavior applies: after a takeover, unflushed writes held by the old owner are lost and
  flushed data is preserved.

### Frontend interface (seam for a future foreign-memory frontend)

- The Aspen side implements a block-device interface with async completion: open(disk), read(offset, length),
  write(offset, data, fua), flush, unmap(ranges), resize(newSize), close, plus lock acquire/release/state for ALUA.
  Requests carry an id, multiple requests can be outstanding at once, and completions may be returned out of
  order.
- Errors map to SCSI status/sense in the frontend, not in the Aspen side. The interface reports semantic failures
  (out of range, fenced, medium error) and the frontend translates them.
- The tcmu-runner bridge is one frontend: the C plugin plus a Netty Unix-domain-socket server in the JVM.
- The C plugin stays thin. It parses the cfgstring (socket path and disk name), maintains the socket connection,
  frames commands and completions, and copies data between the TCMU iovecs and the socket. It does no caching and
  no SCSI logic beyond what tcmu-runner requires of a handler.
- The binary framing in `features/iscsi-target.txt` is a starting point. It needs, at least, a length on READ
  requests, a FUA flag, a status in replies, and lock operations. Payloads must not pass through an intermediate
  serialization library.

### Operations (mirror AmoebaFS)

- Disk names are registered in the namespaced registry (a new namespace such as `"vdisk"`), and the id maps
  through the object registry to the disk's descriptor. This is the same two-hop pattern as
  `FileSystem.RegistryNamespace` / `SimpleFileSystem.create`. Creation is one transaction that claims the name,
  allocates the descriptor and empty map, and registers both.
- CLI commands (`cmdline` is unstable, so names are the builder's choice within this shape): create a disk (name,
  size, allocator/pool, logical block size), list disks, grow a disk, delete a disk (releases all block objects),
  and launch the daemon (Unix socket path, network cache dir like `amoebafs --network-cache-dir`). A daemon can
  serve every disk tcmu-runner opens on its socket.
- Aspen holds the authoritative disk size. OPEN fails if the size tcmu-runner reports disagrees. A tcmu RESIZE
  (from targetcli) commits the grow to Aspen before it is acknowledged.
- **Descriptor layout (designer's call):** the fencing object (epoch and owner) holds nothing else, so writing it
  only ever means a takeover. Size, logical block size, `blockObjectSize`, the allocator id and the map root live in
  a separate disk-config object. A grow, from the owner's RESIZE or from the CLI, rewrites only the config object
  (with a grow-only requirement), so it never fences the owner. The owner re-reads the config on RESIZE. Delete is
  meant for a disk nobody is serving: it first takes ownership (which fences any live owner, and that's
  acceptable), then releases every block object and the registrations.

## Performance principles

These are the core of the design. The rule is that no request waits on something it doesn't depend on.

- **Write acknowledgment path:** copy into the dirty cache, then complete. No Aspen round-trip, no allocation and
  no map update on this path.
- **Background writeback:** dirty data is committed continuously, not only at FLUSH, so a FLUSH waits only for
  what is still dirty or in flight. A short coalescing window before writing an object back is fine, since it
  delays durability, not acknowledgments, but FLUSH and FUA bypass it.
- **Concurrent commits:** transactions on different block objects run in parallel. Each block object has at most
  one transaction in flight. Writes arriving meanwhile accumulate and are merged into that object's next
  transaction, chained on the revision the previous one produced.
- **Choosing ranged vs whole-object writes:** ranged writes need the pre-image of the changed bytes (from the
  cache, or by `readRangedWriteState`). When the dirty bytes cover most of the object, or the object is new, write
  the whole object instead. Where the threshold sits is the builder's call.
- **First write to a block (designer's call):** allocating the block object, inserting it into the
  `DataObjectTKVL`, and writing its first data commit in one transaction, so a crash or fencing never leaks a
  block object (CLAUDE.md: every allocation stores its pointer in the same transaction). Allocate with the dirty
  content, or check whether `rangedWrites(..., zeroFill = true)` on a small or empty object avoids writing 5 MiB of
  zeros for untouched regions.
- **Map-mutating transactions are serialized (designer's call):** the `DataObjectTKVL` uses whole-node revision
  checks, so transactions that change the map (first writes, whole-object UNMAP deletes, grow if it touches the map)
  run one at a time per disk, chained on the map revisions the previous one produced. Each one batches every
  pending new block and removal (`prepareUpdates`, one node update per transaction). Data-only transactions on
  already-mapped blocks don't touch or depend on map nodes, so they stay fully parallel. Because map transactions
  are serialized, a revision mismatch on a map node really is an invariant violation.
- **FUA:** commits only that write's range (plus anything already queued for the same object) before
  acknowledging. It doesn't drain the whole cache.
- **Dirty-cache limit:** a configurable bound, with writeback pressure that ramps up as it fills. Writers are
  throttled only at the hard limit, and that should be rare.
- **Reads:** serve from the dirty and clean caches first. Unallocated ranges return zeros with no I/O. Small
  random reads use byte-range reads. A detected sequential pattern triggers readahead with larger range reads, so
  whole-object reads happen only when the access pattern justifies them. The heuristic is the builder's, but it
  must keep small random reads (such as inode reads) small.
- **Map lookups:** the block-index -> pointer map is authoritative in memory for the owner (single writer), filled
  lazily from the TKVL. No lookup should hit the network twice.
- **UNMAP:** ranges covering whole block objects delete them (refcount 0) and remove their map entries, batched
  (`deferAllocationRelease` where useful). Partial-object UNMAP is ignored. The device does not advertise that
  unmapped blocks read back as zero (LBPRZ = 0).

Sanity bounds from the user, for ordinary conditions on the test setup: individual writes complete well under
100 ms, and a FLUSH with a full dirty cache behind it completes within a couple of seconds. These aren't hard
targets, because testing on one machine with local NVMe will be much faster than networked HDD stores. The real
test is structural: the design must not add delay.

## Failure behavior

- **Daemon crash:** crash-only. On restart (or on another daemon), ownership is retaken by the takeover
  transaction and the disk is served from Aspen state. Nothing local needs recovering.
- **Fenced:** fail all I/O for that disk and report the ALUA state as non-active (see Fencing).
- **Unexpected revision mismatch on a block object or map node:** under single-writer this means a bug or a
  fencing failure. Stop serving the disk with a hard error. Don't re-read and retry.
- **Aspen unavailable or slow:** Aspen's own retry handles it. The daemon doesn't time out requests itself, so
  stalls are bounded by the storage, not by the design.
- **Commit failed for some other reason, outcome unknown (designer's call):** don't guess. Before the object's next
  transaction is built, read the object's current revision. If it equals the failed transaction's id, the commit
  happened: advance the cached state. If it equals the previous revision, the commit didn't happen: retry with the
  same writes. Anything else is an invariant violation (see above). A requirement failure on the fencing object
  always means fenced.

### Failover lifecycle (designer's call, following tcmu-runner's rbd handler)

- Each daemon can OPEN a disk without owning it. A non-owner reports ALUA standby and rejects I/O as tcmu-runner
  expects.
- tcmu's `lock` call takes ownership with the takeover transaction even if another daemon is alive and serving.
  That's how implicit ALUA failover works when the initiator switches paths, and the fencing keeps it safe.
- A fenced daemon doesn't try to take ownership back by itself. It waits for the next tcmu `lock` call.
- The phase-1 block-device interface includes acquire, release and state from the start, so the bridge doesn't need
  reshaping when failover is added.

## Testing

- Unit and integration tests drive the Aspen side through the block-device interface against the existing
  in-process test cluster. Cover: read-your-writes, FLUSH/FUA durability (drop the daemon instance without
  flushing and reload it: flushed data is present and unflushed data may or may not be), concurrent writes to
  many objects, overlapping writes during an in-flight commit, sparse reads, UNMAP, grow, and fencing (a second
  owner takes over and the first owner's commits fail and its I/O errors). Tests follow the project's
  correctness > reliability > speed priorities: no flakiness, and small objects and disks where possible.
- A frame-protocol test for the JVM socket server that uses a test client in place of the C plugin.
- The C plugin and the end-to-end path (targetcli, iSCSI login, mkfs.ext4, fio, kernel build, multipath failover)
  are validated manually on a Linux host with LIO and tcmu-runner. Document the setup steps.

## Left to the builder

- Package and module names, CLI command names and flags, and the build arrangement for the C plugin (it builds
  against tcmu-runner's headers, outside sbt).
- The exact frame format, cache sizes and defaults, the coalescing window, the readahead heuristic, and the
  ranged-vs-whole-object threshold.
- The RandomAccessReedSolomon `cellSize` for the pool. Measure, and lean toward a value that keeps an aligned
  4 KiB access on few stores, such as 4096.
- Any small client API additions needed, for example building the next `RangedWriteState` from known bytes after
  a commit without re-reading.
- Phasing. A sensible order is: Aspen-side block device and tests, then the socket bridge and C plugin, then CLI,
  then ALUA lock/failover.
