# vdisk: block devices on Aspen, exported over iSCSI

This package implements virtual block devices ("vdisks") stored in Aspen objects, and the daemon that serves them
to the Linux iSCSI target. This document covers how the pieces fit together, where each behavior is implemented,
and where to look when adding a feature or chasing a bug.

Related documents:

- `docs/superpowers/specs/2026-10-30-iscsi-tcmu-target-design.md`: the design spec (requirements and decisions).
- `docs/superpowers/plans/2026-10-30-vdisk-engine.md` and `2026-10-31-tcmu-bridge.md`: the implementation plans
  for part 1 (the engine) and part 2 (the bridge).
- `docs/vdisk-tcmu-setup.md`: the operator guide (targetcli, timeouts, block size, resize).
- `native/tcmu-aspen/README.md`: building, testing and configuring the C plugin.

## 1. The stack

```
 iSCSI initiator (open-iscsi, sd, ext4/xfs)
        │  SCSI over TCP
 LIO target (kernel) ── target_core_user (TCMU) ── shared-memory command ring
        │
 tcmu-runner 1.5.4 (root daemon) ── loads handler_aspen.so    native/tcmu-aspen/handler_aspen.c
        │  Unix domain socket, private binary protocol         vdisk/bridge/Protocol.scala, aspen_proto.h
 aspen vdisk-server (JVM) ── BridgeServer / BridgeSession     vdisk/bridge/
        │  LockableBlockDevice                                 vdisk/BlockDevice.scala
 DiskHandle ── VirtualDisk ── BlockCommitter, BlockMap        vdisk/
        │  AspenClient transactions and ranged reads/writes
 Aspen objects: config KV object, fence DataObject, DataObjectTKVL block map, block DataObjects
```

Responsibilities are split so that SCSI knowledge stays in tcmu-runner. The C plugin only translates callouts into
protocol requests; it reads only one SCSI field itself, the FUA bit. The JVM daemon holds no SCSI logic either: it
serves the `LockableBlockDevice` interface. That interface is the seam for a future Java frontend that drives the
TCMU ring directly through foreign memory and skips tcmu-runner.

The main thing to understand is that **the disk is a write-back cache with a single writer.**
- **Write-back:** a WRITE completes once its data is in the owner's memory. Data is durable only after a FLUSH
  (SYNCHRONIZE CACHE) or a FUA write. This matches the semantics of a disk with a volatile write cache, so
  filesystems already issue the flushes they need.
- **Single writer:** exactly one owner may write a disk. Ownership is taken by bumping an epoch in a fence object.
  Every commit is conditional on the fence revision the owner saw, so a new owner fences out the old one without
  needing to talk to it.

## 2. File map

### Engine (`vdisk/`)

| File | What it holds |
|------|---------------|
| `BlockDevice.scala` | `BlockDevice` (read/write/flush/unmap/grow/close), `LockState`, `LockableBlockDevice` (acquire/release/lockState). The frontend contract. |
| `VDiskError.scala` | Every error the engine reports. The bridge maps each one to a wire status. |
| `VDiskConfig.scala` | Tuning: dirty limits, coalescing window, in-flight transaction cap, map transaction size, clean cache size, readahead. |
| `DiskGeometry.scala` | Size, logical block size and block object size. Request validation, and splitting a request into per-block `BlockSegment`s. |
| `DiskConfig.scala` | `DiskInfo`, plus the keys and encodings of the disk's config object. |
| `Fence.scala` | `FenceState(epoch, owner)`, 24 bytes. |
| `DiskCatalog.scala` | Create, load, list, grow and delete disks. `takeOwnership`. Name registry. |
| `DiskHandle.scala` | `LockableBlockDevice` over a named disk: open without owning, acquire/release ownership, and replace a fenced or failed `VirtualDisk`. |
| `VirtualDisk.scala` | The engine itself: per-block state, the write-back scheduler, the read path, UNMAP, grow and stop. |
| `BlockCommitter.scala` | The data and map transaction loops, retries, and resolving transactions whose outcome is unknown. Holds no state of its own; works through `VirtualDisk`'s `Host`. |
| `BlockState.scala` | In-memory state of one block object: pointer, revision, dirty data, in-flight snapshot, waiters, timers. |
| `SegmentMap.scala` | A block's dirty data as non-overlapping `(offset, bytes, seq)` pieces. `SegmentSnapshot` is an immutable view of it. |
| `BlockMap.scala` | The block index → object pointer map (a `DataObjectTKVL`), with an in-memory memo of every lookup. |
| `CleanCache.scala` | Bounded Scaffeine cache of committed block bytes, held as pieces. |
| `SequentialDetector.scala` | Detects sequential streams and sizes their readahead. |

### Bridge (`vdisk/bridge/`)

| File | What it holds |
|------|---------------|
| `Protocol.scala` | Wire constants (magic, version, ops, statuses, flags) and response encoding. Must match `native/tcmu-aspen/aspen_proto.h`. |
| `FrameDecoder.scala` | Netty decoder from bytes to `Request`. Framing violations close the connection. |
| `BridgeSession.scala` | One connection = one disk. The OPEN state machine, op dispatch to the device, and error → status mapping. |
| `BridgeServer.scala` | Epoll domain-socket server, and the secure socket publish/unlink. `prepareSocketDir`/`checkSocketDir`. |

### Elsewhere

| Path | What it holds |
|------|---------------|
| `cmdline/Main.scala` | `create-disk`, `list-disks`, `grow-disk`, `delete-disk`, and `vdisk-server [--socket] [--network-cache-dir]` (`run_vdisk_server`). |
| `native/tcmu-aspen/handler_aspen.c` | The tcmu-runner plugin. |
| `native/tcmu-aspen/aspen_proto.{h,c}` | C framing and cfgstring parsing; no tcmu dependency. |
| `native/tcmu-aspen/test_*.c`, `tcmu_stub.{c,h}` | C tests. `test_handler` builds the plugin against stubbed tcmu functions and plays the daemon. |
| `native/tcmu-aspen/e2e/` | Root-only end-to-end scripts (targetcli setup and teardown, fio smoke job). |
| `submodules/tcmu-runner` | tcmu-runner v1.5.4 headers the plugin compiles against. |

## 3. How a disk is stored in Aspen

`DiskCatalog.create` commits all of the following in **one** transaction, so a name never resolves to a half-built
disk:

- **Name registration:** the name is registered in the namespaced registry under namespace `vdisk`, mapping to the
  disk id (a UUID). Names must be non-empty and may not contain `@`, because `@` separates the disk name from the
  socket path in the tcmu cfgstring.
- **Object registration:** the disk id is registered in the object registry, mapping to the config object.
- **Config object** (KeyValue; keys in `DiskConfig`):
  - `size`
  - `logical-block-size`: 512 or 4096, default 4096
  - `block-object-size`
  - `allocator`
  - `fence`: pointer to the fence object
  - `map`: the `Root` of the block map

  Only `size` ever changes after creation.
- **Fence object** (DataObject): a `FenceState(epoch, owner UUID)`.
- **Block map:** a `DataObjectTKVL`, created empty, keyed by block index with `IntegerKeyOrdering`. Its values are
  encoded `DataObjectPointer`s.

Block objects are allocated lazily: a block with no map entry reads as zeros (thin provisioning). The block object
size is the largest multiple of the logical block size that is no larger than the allocator's `maxObjectSize`
(`DiskGeometry.blockObjectSizeFor`). For example, RandomAccessReedSolomon with 16/10/12 and a 5,120,000-byte
maximum gives 5,120,000-byte blocks with a 4096-byte logical block size. An object holds only the bytes up to the
end of its last write; bytes past the object's length read as zero.

Deleting a disk (`DiskCatalog.delete`) takes ownership first, which fences any live server. It then:
1. deletes every block, each under its own fence revision;
2. deletes the config and fence objects;
3. removes the object registration;
4. removes the name.

Each step can be re-run, so a crash midway leaves a disk that `delete-disk` can finish.

## 4. Ownership and fencing

- **Taking ownership:** `DiskCatalog.takeOwnership` overwrites the fence with `epoch + 1` and a fresh owner UUID,
  and returns the revision it committed at (`fenceRevision`).
- **Guarding commits:** every data and map transaction adds `tx.sharedLockRevision(fencePointer, fenceRevision)`.
  A `SharedRevisionLock` lets any number of the owner's transactions hold it at once, but it collides with a write
  to the fence. So once a new owner overwrites the fence, every later commit by the old owner aborts.
- **Detecting fencing:** a failed commit first checks the fence (`VirtualDisk.fenceHeld`). If the fence revision
  moved, the disk stops with `Fenced`, and every pending and future request fails with that error.

A stale owner can therefore never overwrite a newer owner's data. It learns it was fenced only when a commit or a
stale read fails, though. An idle fenced owner still reports `Locked`; see the limitations section.

## 5. VirtualDisk internals

### Concurrency model

All state lives under the `VirtualDisk` monitor, which is **never held across a Future or an Aspen call**. The
`locked { ... }` helper runs a block under the monitor with a `Deferred` collector:
- Promise completions, `launch`ed Aspen work and map-transaction kicks are queued with `later`/`launch`.
- They run after the monitor is released.
- `launch` runs work on the client's `ExecutionContext`, so a write's acknowledgment never waits on building a
  transaction.

`mapKickRequested` collects requests to start a map transaction during a monitor section, so all blocks made
ready in one section join a single batch.

`BlockCommitter` runs the transaction loops. Everything it needs from the disk goes through the `Host` trait, and
each Host method takes the monitor itself. If you add a code path that needs disk state from inside a commit loop,
add a Host method; don't reach into `VirtualDisk`.

### Per-block state (`BlockState`)

| Field | Meaning |
|-------|---------|
| `lookedUp`, `pointer` | Whether the map entry is known, and the object pointer (`None` = unallocated, reads as zeros). |
| `revision`, `length`, `revisionLearned` | The object's current revision and length. "Learned" means it came from a read rather than from one of this owner's commits, so it may be stale. |
| `dirty: SegmentMap` | **Every** uncommitted write, including the pieces an in-flight commit is writing. A read's overlay is therefore just `dirty.snapshot`. |
| `inFlight` | Snapshot being committed. At most one commit is in flight per block. |
| `committedSeq`, waiters | Every write has a disk-wide `seq`. FUA and FLUSH wait for `committedSeq >= seq`. |
| `generation` | Bumped by each commit or unmap. A read caches what it fetched only if the generation hasn't changed. |
| `unmaps` | Bumped by each whole-block UNMAP. A commit or map entry carries the count it started at (its *epoch*); a mismatch means it is superseded. |
| `timer`, `timerSeq` | The coalescing-window timer. |

### Write path

`write` validates the request, assigns `seq` and splits the request into block segments. For each segment,
`putDirty` adds the piece to `dirty`, updates `dirtyTotal` and calls `dirtied`:

- **Map entry unknown:** start a lookup (`startLookup`). The lookup overlaps the coalescing window.
- **Urgent:** call `kick`, which starts a commit now. A block is urgent when FUA is set, a FLUSH is pending, the
  block has waiters, the dirty total is above `dirtySoftLimit` or at `dirtyHardLimit`, or the coalescing window is
  0.
- **Otherwise:** `armTimer` waits `coalesceWindow` (20 ms by default) so that neighboring writes merge into one
  commit.

`kick` routes the block to the right commit:
- **Has an object:** a data transaction (`startDataTx`), limited to `maxInFlightDataTx` at once; extra blocks wait
  in `awaitingData`.
- **No object yet:** the block goes to `awaitingMap`, to be created by the next map transaction.

The write's Future completes immediately unless FUA is set, or the dirty total is at `dirtyHardLimit` (in which
case it waits in `throttled`). This throttle is the only back-pressure, and it is natural rather than an artificial
delay.

### Data transactions (`BlockCommitter.commitBlock` / `runDataTx`)

- **Dirty data covers the whole block:** `tx.overwrite` at the known revision (or the revision read from metadata).
- **Otherwise:** `tx.rangedWrites` with `zeroFill = true`. The pre-image `RangedWriteState` comes from the clean
  cache when it holds every byte being written (no I/O). Otherwise it comes from `client.readRangedWriteState`
  (counted as a `preImageRead`). Stores receive only XOR deltas.

On success, `dataCommitted`:
1. records the new revision and length;
2. merges the committed pieces into the clean cache;
3. drops dirty pieces up to the snapshot's max seq (newer writes to the same bytes stay);
4. completes the satisfied waiters;
5. re-kicks the block if more dirty data remains.

**Unknown outcome.** A sent transaction that reported failure is resolved before the next attempt (`resolveData`):
- Check the fence. If it moved, the disk is `Fenced`.
- Read the block's revision:
  - It equals the new revision: the transaction committed (counted as a `recoveredCommit`).
  - It equals the base revision: the transaction didn't commit, so retry with the client's backoff.
  - Anything else, with a learned revision: forget the revision and retry.
  - Anything else otherwise: the disk is `DiskFailed`. The owner is the only writer, so this is an invariant break.

### Map transactions (`BlockCommitter.commitMap` / `runMapTx`)

Map transactions run one at a time per disk. `kickMap` batches:
- every `awaitingMap` block, in index order, up to `maxMapTxBytes` of initial content;
- up to 256 pending deletions (`MaxMapTxDeletes`) of unmapped blocks with no commit in flight.

One transaction allocates the new block objects with their initial content (dirty data up to its end, gaps
zero-filled), inserts or replaces their map entries, and sets the refcount of deleted objects to 0. Inserts use
`requireDoesNotExist`. When anything is deleted the transaction calls `deferAllocationRelease()`; a disk's blocks
all come from one allocator, so this stays within FSIF's limit of one pool per transaction.

**Retries after an unknown outcome** discover what a failed attempt actually did:
- **`KeyAlreadyExists` on an insert:** the attempt did create the block. The block *adopts* the mapped object
  (`adopted`), and its dirty data then goes through an ordinary data transaction.
- **The object to delete is gone:** the deletion is done (`gone` / `mapEntryDone`).
- **Either one with no attempt sent:** another writer touched the map, so the fence is checked first.

### Read path (`read` → `readSegment` → `fetchSegment` / `fetchOnce`)

1. Take the dirty overlay **before** any I/O, so every write that completed before the read is reflected. If the
   overlay alone covers the segment, return at once.
2. Make sure the map entry is known (`BlockMap.lookup`, memoized and deduplicated). If the block has no object,
   return zeros plus the overlay.
3. Work out the gaps that neither the clean cache nor the overlay covers, clipped to the object's length. If the
   `SequentialDetector` says this read continues a stream, extend the fetch with readahead within the same block
   object; the window starts at 4 logical blocks and doubles up to `maxReadahead`. Fetch everything in **one**
   `client.readRanges` call.
4. Cache the result only if the fetched revision matches the known one (or no revision is known and no commit is
   in flight), and the block's generation hasn't changed. Bytes from another revision are never returned: the read
   retries with backoff (`StaleFetch`), after checking the fence. A mismatch against a *learned* revision makes the
   block forget it, since a lagging store may have supplied that revision.

`rangeReader` is the single point through which every fetch passes. Tests replace it to simulate stale stores.

### UNMAP (`unmap` → `applyUnmap`)

- **Blocks fully covered** (`unmapBlock`):
  - Drop their dirty data, bump `unmaps`, and mark them unallocated so they read as zeros at once.
  - Queue a deletion of the old object (`pendingDeletes`) for the next map transaction. If a commit is in flight
    on the block, the deletion waits for it to end.
  - Writes that were superseded and had FUA or FLUSH waiters complete only once the deletion commits. Until then a
    crash would bring back the old contents.
- **Partially covered pieces** are zeroed with an ordinary dirty write. The exception is a block already known to
  read as zeros (looked up, no pointer, no dirty data, no in-flight commit). Zeroing is required because
  tcmu-runner always reports LBPRZ=1: unmapped blocks must read back as zero. If such a block's map entry isn't yet
  known, it is looked up before the unmap applies.
- **FLUSH** also waits for every outstanding deletion (`awaitDeletes`).

The epoch (`unmaps`) check is threaded through every commit loop. A data or map commit that an unmap superseded is
dropped or requeued, never retried. This is the subtlest part of the engine; most of `VirtualDiskSuite`'s UNMAP
cases cover these races.

### Grow

`VirtualDisk.grow` calls `DiskCatalog.grow`, which re-reads the stored size on every attempt:
- equal to the target: done, without a commit;
- larger than the target: `InvalidGeometry` (disks never shrink);
- otherwise: commits the new size under a key-revision requirement.

The disk then switches to the new geometry. Because grow is idempotent, a RESIZE whose reply was lost, or one that
follows a CLI `grow-disk`, succeeds. `DiskHandle.grow` short-circuits a RESIZE to the size already being served.

### Stop and close

`stop(err)` is final:
- cancels timers;
- fails every waiter, pending read, throttled write and delete waiter;
- clears all caches and queues.

No new transaction starts afterwards (`Host.checkServing`). `close()` is crash-only: it calls `stop(Fenced)`
without flushing, so unflushed writes are lost, exactly as they would be when power is pulled from a disk with a
write cache.

## 6. DiskHandle: the ownership lifecycle

`DiskHandle.open` loads the catalog entry but does **not** take ownership, so OPEN can verify geometry before
fencing anybody. The operations:

- **`acquire`:** closes any old disk that is fenced or failed, then calls `VirtualDisk.acquire` (load, take
  ownership, open the map tree, get the allocator). Concurrent calls share one takeover Future. An `acquire` while
  `Serving` is a no-op.
- **`release`:** if the disk is `Serving`, flush, then close; otherwise just close. An `acquire` that arrives during
  a release waits for it.
- **`close`:** crash-only. It bumps `generation`, so a takeover still in flight discards its disk and fails with
  `Fenced`.
- **`lockState`:**

  | Disk state | `lockState` |
  |------------|-------------|
  | No disk | `Unlocked` |
  | `Serving` | `Locked` |
  | `Fenced` | `Fenced` |
  | `Failed(reason)` | `Failed(reason)` |

- **I/O without ownership:** fails with `NotOwner`.

Every decision is made inside one `synchronized` block; Futures run outside it.

## 7. The bridge protocol

All integers are big-endian. The golden byte vectors in `ProtocolSuite` and `test_proto.c` pin the format on both
sides. **Change both, and both tests, together.**

Request header (32 bytes):

| Offset | Size | Field |
|--------|------|-------|
| 0 | 2 | magic `0xA5D1` |
| 2 | 1 | version (1) |
| 3 | 1 | op |
| 4 | 4 | flags (`1` = FUA) |
| 8 | 8 | request id |
| 16 | 8 | offset |
| 24 | 4 | length |
| 28 | 4 | payload length (≤ 64 MiB) |

Response header (24 bytes):

| Offset | Size | Field |
|--------|------|-------|
| 0 | 2 | magic `0xA5D2` |
| 2 | 1 | version |
| 3 | 1 | op |
| 4 | 4 | status |
| 8 | 8 | request id |
| 16 | 4 | payload length |
| 20 | 4 | reserved |

Ops:

| Op | Request | OK reply payload |
|----|---------|------------------|
| OPEN = 1 | payload = disk name (UTF-8); offset = size tcmu expects; length = logical block size tcmu uses | u32 block object size, u32 logical block size |
| READ = 2 | offset, length | `length` bytes |
| WRITE = 3 | offset, length = payload length, flags may set FUA | none |
| FLUSH = 4 | none | none |
| UNMAP = 5 | payload = n × (u64 offset, u64 length) | none |
| RESIZE = 6 | offset = new size | none |
| LOCK = 7 / UNLOCK = 8 | none | none |
| LOCK_STATE = 9 | none | 1 byte: 0 unlocked, 1 locked, 2 fenced, 3 failed |

Statuses, and how the plugin turns each one into a SCSI status:

| Wire status | Comes from | tcmu status |
|-------------|-----------|-------------|
| Ok = 0 | | `TCMU_STS_OK` |
| InvalidRequest = 1 | `InvalidRequest`, `InvalidGeometry` | `TCMU_STS_INVALID_CDB` |
| OutOfRange = 2 | `OutOfRange` | `TCMU_STS_RANGE` |
| Fenced = 3, NotOwner = 4 | `Fenced`, `NotOwner` | `TCMU_STS_FENCED` |
| DiskFailed = 5 | `DiskFailed` | `TCMU_STS_RD_ERR` / `TCMU_STS_WR_ERR` |
| NotFound = 6 | `DiskNotFound` | OPEN → `-ENOENT` |
| SizeMismatch = 7 | OPEN geometry check | OPEN → `-EINVAL` |
| ProtocolError = 8, Internal = 9 | session state or op errors, unexpected exceptions | `TCMU_STS_HW_ERR` |

Every non-OK reply carries a UTF-8 error message of at most 1024 bytes as its payload. The plugin logs it for a
failed OPEN or LOCK.

**Session rules** (`BridgeSession`):
- One connection serves one disk.
- Before a successful OPEN, every other op gets `ProtocolError`. A failed OPEN returns the session to
  `NotOpened`, so the client may retry.
- Requests are dispatched as they arrive. Replies are written as each Future completes, so **replies can be out of
  order**; the client matches them by request id.
- All session state is touched only on the connection's event loop.
- A dropped connection **closes** the device, crash-only, without a flush. UNLOCK is the clean path: it flushes.

**Socket security** (`BridgeServer`):
- The socket is bound inside a fresh 0700 directory, set to mode 0600, then hard-linked into place. Another user
  can never connect to it, and a live socket is never clobbered.
- An existing path is replaced only if it is a socket and connecting to it is refused.
- `stop()` unlinks the path only if it is still the server's own inode (checked by its file key).
- The parent directory must be owned by the daemon's user and must not be writable by group or others.
- The kernel limits socket paths to 107 bytes.

## 8. The tcmu-runner plugin (`handler_aspen.c`)

- **Threading:** each device has one socket and one reader thread (`reader_main`).
  - A table of `struct pending` holds outstanding requests, keyed by request id.
  - `send_lock` serializes whole requests on the socket; `lock` guards the table and flags.
- **I/O callouts:** with `nr_threads = 0` the callouts are asynchronous. Each one queues a request and returns
  `TCMU_STS_OK`. The reader completes the command with `tcmur_cmd_complete` when the reply arrives. Write data is
  sent straight from tcmu's iovecs, and READ replies are read straight into them.
- **FUA:** tcmu-runner's generic write path drops FUA, so `aspen_write` reads it from the CDB itself: opcodes
  0x2A, 0xAA and 0x8A, bit `cdb[1] & 0x08`.
- **open:**
  1. Parse the cfgstring `aspen/<disk>[@<socket>]`. Anything from the first `;` is tcmu-runner's own options and
     is ignored. The default socket is `/run/aspen/vdisk.sock`.
  2. Connect, then send OPEN with tcmu's size and block size.
  3. Check the returned block size and block object size.
  4. Enable the write cache and UNMAP, and set the optimal unmap granularity and the unmap granularity alignment
     to one block object. The alignment must be nonzero, because tcmu-runner divides by it in
     `handle_unmap_in_writesame`.
  5. Send LOCK.
  6. Start the reader thread.
- **close:** sends UNLOCK, waits for the reply, shuts the socket down and joins the reader.
- **reconfig:** for `TCMULIB_CFG_DEV_SIZE` only, sends RESIZE. It returns `-EBUSY` while the device is closed.
- **Connection loss** (`connection_lost`):
  - Every pending command completes with `TCMU_STS_BUSY`, so the initiator retries it.
  - `tcmu_notify_conn_lost` is called **before** the completions, and tcmu-runner then closes and reopens the
    device.
  - Until the reopen, new commands get BUSY and notify again. tcmu-runner silently drops a notification while the
    device is still in recovery, so the plugin repeats it, and tcmu-runner discards the duplicates.
  - A daemon restart therefore looks like a brief stall to the initiator, as long as it finishes within open-iscsi's
    `replacement_timeout`.
- **Private pointer:** it is `NULL` between close and reopen. Every callout checks for that and returns BUSY or
  `-EBUSY`.

## 9. Failure behavior at a glance

| Event | What happens |
|-------|--------------|
| `vdisk-server` crashes or restarts | Unflushed writes are lost, as on power loss; flushed data is safe. The plugin returns BUSY and tcmu-runner reopens; OPEN and LOCK take ownership again. |
| A second server takes the disk | The new owner bumps the fence. The old owner's next commit aborts, its disk stops with `Fenced`, and its I/O returns `TCMU_STS_FENCED`. |
| A commit reports failure but did commit | Resolved by revision (data) or by finding the map change (map); counted in `Stats`. |
| A store serves an old revision | Bytes from the wrong revision are never returned or cached; the read retries. |
| An invariant breaks (a block at an unexpected revision, or its object gone) | The disk stops with `DiskFailed`, and I/O returns medium errors. A new `acquire` (reopen) starts over. |
| The connection carries garbage | `FrameViolation`; the connection closes, and the device closes crash-only. |
| A requested size or block size doesn't match the disk | OPEN replies `SizeMismatch`; tcmu open fails with `-EINVAL`. |

## 10. Testing

| Suite | Covers |
|-------|--------|
| `DiskGeometrySuite`, `SegmentMapSuite`, `CleanCacheSuite`, `SequentialDetectorSuite` | Pure data structures. |
| `DiskCatalogSuite` | Create/load/list/grow/delete, name validation, idempotent grow and delete. |
| `VirtualDiskSuite` | Read-your-writes, coalescing, FLUSH/FUA durability, map batching, ranged vs. overwrite commits, readahead, UNMAP races, grow. |
| `VirtualDiskFailureSuite` | Takeover and fencing, crash-only close, unknown-outcome commits, adoption, stale stores. |
| `DiskHandleSuite` | acquire/release/close interleavings and lock states. |
| `bridge/ProtocolSuite` | Golden vectors and decoder violations. |
| `bridge/BridgeServerSuite` | Session state machine, out-of-order replies, error mapping, and socket security, against a `FakeDevice`. |
| `bridge/BridgeIntegrationSuite` | `TestBridgeClient` → real `BridgeServer` → real `DiskHandle` on an in-process Aspen system. |
| `make -C native/tcmu-aspen check` | `test_proto` (framing, cfgstring) and `test_handler` (the plugin against stubbed tcmu plus a fake daemon). |
| `native/tcmu-aspen/e2e/` | Root-only: LIO + iSCSI + ext4 + fio. Not run automatically. |

`VirtualDisk` exposes `private[vdisk]` test hooks:
- `commitGate`, `attemptGate`, `deleteLookupGate`, `readGate`: hold an operation at a precise point.
- `failNextCommitAfterSuccess`: simulate a lost commit reply.
- `rangeReader`: inject stale reads.
- `stats`, `dirtyBytes`, `fetchCount`, `cleanBytes`, `blockPointer`: introspection.

Use these to make a race deterministic rather than relying on timing. Flaky tests are not permitted.

## 11. Where to look

| Task | Start in |
|------|----------|
| Add a wire op | `Protocol.scala` + `aspen_proto.h`, both golden tests, `BridgeSession.handleIo`, a callout in `handler_aspen.c`, `tcmu_status`. |
| Add a device capability (e.g. a new SCSI-visible setting) | `aspen_open` in `handler_aspen.c` (the `tcmu_dev_set_*` calls). |
| Change write-back timing or limits | `VDiskConfig`; `urgent`, `dirtied`, `kick` and `throttleWrite` in `VirtualDisk`. |
| Change how commits are built | `BlockCommitter.runDataTx` / `runMapTx`. |
| A commit hangs | `busy()` / `kick()` gating, `awaitingMap`/`awaitingData`, `mapTxRunning`; use `commitGate` to reproduce. |
| A read returns stale or wrong data | The overlay snapshot in `readSegment`; the revision and generation checks in `fetchOnce`; `CleanCache.merge`. |
| Data reappears after UNMAP | `unmapBlock`, `pendingDeletes`/`requeueDelete`, the epoch checks in `dataInput` and `mapInputs`. |
| Unexpected `DiskFailed` | The message names the cause: `resolveData`, `adopt`/`gone` in `commitMap`, or a failed map lookup. |
| Takeover or lock issues | `DiskHandle.acquire`/`release`, `DiskCatalog.takeOwnership`, `VirtualDisk.fenceHeld`. |
| tcmu-runner stuck in recovery or I/O stuck at BUSY | `connection_lost`, `queue_io`, `aspen_open`; tcmu-runner's log. |
| Resize | `aspen_reconfig` → RESIZE → `DiskHandle.grow` → `DiskCatalog.grow`. |
| Server startup or socket errors | `BridgeServer.start`/`checkExisting`/`checkSocketDir`, `Main.run_vdisk_server`. |

## 12. Known limitations and planned work

The authoritative list is the "vdisk tcmu bridge (part 2) follow-ups" entry in `Todo.txt`. The main items:

- **The end-to-end run hasn't happened yet.** It needs root; see `docs/vdisk-tcmu-setup.md`.
- **No FUA from Linux.** tcmu-runner 1.5.4 reports DPOFUA=0, so Linux sends WRITE + SYNCHRONIZE CACHE instead of
  FUA, and the FUA path is unused with Linux initiators.
- **Fencing is detected lazily.** An idle fenced owner reports `Locked`. Part 3 (ALUA failover) needs an active
  fence check for `get_lock_state`.
- **Grow-then-reopen dead end.** After `aspen grow-disk`, a tcmu reopen before the targetcli resize fails: OPEN
  replies `SizeMismatch`, and reconfig returns `-EBUSY` while the device is closed.
- **Resize/reopen race.** `aspen_reconfig` (netlink thread) reads the private pointer without synchronizing with
  `aspen_close` (recovery thread).
- **Readahead stays within one block object.** It never crosses a block boundary.
- **Part 3:** ALUA multi-path failover, built on `LOCK`/`UNLOCK`/`LOCK_STATE`.
- **Part 4:** measurements across cell sizes.
