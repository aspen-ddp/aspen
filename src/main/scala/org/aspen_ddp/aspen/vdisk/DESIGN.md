# vdisk: block devices on Aspen, exported over iSCSI

This package implements virtual block devices ("vdisks") stored in Aspen objects, and the daemon that serves them
to the Linux iSCSI target. This document covers how the pieces fit together, where each behavior is implemented,
and where to look when adding a feature or chasing a bug.

Related documents:

- `docs/superpowers/specs/2026-10-30-iscsi-tcmu-target-design.md`: the original design spec (the engine and its
  requirements).
- `docs/superpowers/specs/2026-10-31-native-tcmu-design.md`: the design of the native TCMU frontend that replaced
  tcmu-runner (requirements and decisions).
- `docs/superpowers/plans/2026-10-30-vdisk-engine.md` and `2026-10-31-native-tcmu.md`: the implementation plans for
  the engine and the native frontend. (`2026-10-31-tcmu-bridge.md` is the plan of the removed tcmu-runner bridge.)
- `docs/vdisk-tcmu-setup.md`: the operator guide (D-Bus policy, targetcli, timeouts, block size, grow, restart).
- `scripts/vdisk-e2e/README.md`: the root-only end-to-end scripts.

## 1. The stack

```
 iSCSI initiator (open-iscsi, sd, ext4/xfs)
        │  SCSI over TCP
 LIO target (kernel) ── target_core_user (TCMU)
        │  shared-memory command ring, mmap of /dev/uioN    vdisk/tcmu/ring/
        │  generic netlink TCM-USER (device add/remove/reconfig)  vdisk/tcmu/Netlink.scala
 aspen vdisk-server (JVM, root, FFM downcalls)             vdisk/tcmu/sys/Libc.scala
        │  TcmuService (lifecycle) ── TcmuDevice (one per device)   vdisk/tcmu/
        │  ScsiCommands + Emulation (SCSI emulation)       vdisk/tcmu/scsi/
        │  LockableBlockDevice                             vdisk/BlockDevice.scala
 DiskHandle ── VirtualDisk ── BlockCommitter, BlockMap     vdisk/
        │  AspenClient transactions and ranged reads/writes
 Aspen objects: config KV object, fence DataObject, DataObjectTKVL block map, block DataObjects

 targetcli ──(system D-Bus: org.kernel.TCMUService1)──> vdisk-server   vdisk/tcmu/dbus/
```

There is no tcmu-runner and no plugin. `vdisk-server` talks to the kernel's TCMU interface directly, through
Java's Foreign Function & Memory API (`vdisk.tcmu.sys.Libc`), and does the SCSI emulation tcmu-runner used to do
in Scala (`vdisk.tcmu.scsi`). It also owns tcmu-runner's D-Bus name, `org.kernel.TCMUService1`, so targetcli still
sees the `user:aspen` backstore type. The two cannot run on one host: `vdisk-server` refuses to start when the name
is already owned. It also refuses to start unless it runs as root.

The engine below `LockableBlockDevice` knows nothing about SCSI or TCMU. `ScsiCommands` turns each CDB into
`read`/`write`/`flush`/`unmap` calls on that interface, and `TcmuService` drives `open`/`acquire`/`grow`/`release`.

The main thing to understand is that **the disk is a write-back cache with a single writer.**
- **Write-back:** a WRITE completes once its data is in the owner's memory. Data is durable only after a FLUSH
  (SYNCHRONIZE CACHE) or a FUA write. This matches the semantics of a disk with a volatile write cache, so
  filesystems already issue the flushes they need. The device reports WCE=1 and advertises FUA (DPOFUA), so Linux
  initiators send real FUA writes.
- **Single writer:** exactly one owner may write a disk. Ownership is taken by bumping an epoch in a fence object.
  Every commit is conditional on the fence revision the owner saw, so a new owner fences out the old one without
  needing to talk to it.

### Threads

| Thread | What runs on it |
|--------|-----------------|
| main | `Main.run_vdisk_server`: startup, then `Thread.currentThread.join()` for the life of the process. |
| `tcmu-netlink` (platform, daemon) | `Main.tcmuEventLoop`: blocks in `TcmuNetlink.receive`, hands each event to `TcmuService.onEvent`, and replies from the Future's completion, so a slow add does not hold up other events. |
| `tcmu-<device>` (platform, daemon, one per served device) | `TcmuDevice.loop`: blocks in `UioChannel.await` (poll of the UIO fd and a wake-up pipe), takes every new entry off the ring and dispatches it. FFM downcalls pin virtual threads, so these are platform threads. |
| the Aspen client's `ExecutionContext` | The engine's Futures. A command's completion runs on whichever thread completes its Future (`completionContext` is `parasitic`), under the device's ring read lock. |
| `tcmu-timeout` | The timer for `TcmuServiceConfig.acquireTimeout`. |
| dbus-java dispatch threads | `CheckConfig`, which blocks for up to 10 s while it asks Aspen whether the disk exists. |

## 2. File map

### Engine (`vdisk/`)

| File | What it holds |
|------|---------------|
| `BlockDevice.scala` | `BlockDevice` (read/write/flush/unmap/grow/close), `LockState`, `LockableBlockDevice` (acquire/release/lockState). The frontend contract. |
| `VDiskError.scala` | Every error the engine reports. `scsi.ErrorMap` maps each one to sense data. |
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

### TCMU frontend (`vdisk/tcmu/`)

| File | What it holds |
|------|---------------|
| `sys/Libc.scala` | `Linux` (constants: open flags, netlink, errnos) and `Libc`, the FFM downcalls (`open`, `close`, `read`, `write`, `mmap` (`mmapShared`), `socket`, `bind`, `setsockopt`, `send`, `recv`, `poll`, `pipe`, `geteuid`). `errno` is captured on every call (`captureCallState`); a failure throws `SysError(call, errno, message)`. EINTR is retried. |
| `ring/RingLayout.scala` | Mailbox and entry offsets, entry ops, mailbox flags, from `/usr/include/linux/target_core_user.h`. |
| `ring/RingItem.scala` | What the ring yields: `Command`, `Tmr`, `Malformed`; `Iovec`; the `RingCorrupt` exception. |
| `ring/Ring.scala` | Decoder/encoder for one mapped ring: `open` (validate the mailbox), `nextItems`, `complete`, `resync`, `gather`/`scatter` of the data area, `mailboxDump`. |
| `KernelChannel.scala` | `KernelChannel` (await/kick/close), `RingControl` (resetRing), and `UioChannel`, the UIO fd plus a wake-up pipe. |
| `Netlink.scala` | `TcmuGenl` (the TCM-USER family's commands and attributes), `TcmuEvent` and `ReconfigChange`, `GenlCodec` (pure encode/decode), and `TcmuNetlink` (the socket: open, receive, reply). |
| `Configfs.scala` | `UioName` (parses `tcm-user/<hba>/<device>/<cfgstring>`) and `Configfs`: configfs `attrib/hw_block_size`, `attrib/hw_max_sectors`, `info` (`Size:`), `wwn/vpd_unit_serial`, `action/block_dev` and `action/reset_ring`; sysfs `/sys/class/uio` (name, map size, scan). |
| `Cfgstring.scala` | Parses `aspen/<disk>[;...]` and holds `ConfigDesc`, the help targetcli prints. |
| `DeviceSettings.scala` | What SCSI emulation needs about one device: kernel size, block size, `hw_max_sectors`, block object size, device name, unit serial. |
| `TcmuDevice.scala` | Serves one device: the `tcmu-<device>` loop, dispatch, completion, ring reset/resync, per-command trace. |
| `TcmuService.scala` | `TcmuKernel` / `LinuxTcmuKernel` (map a ring), `TcmuServiceConfig`, and `TcmuService`: device add, remove and reconfig, and the startup scan. |
| `FailureLog.scala` | Rate-limited failure reporting for one device. |
| `scsi/Cdb.scala` | `Op` (opcodes) and `Cdb` (CDB length, LBA, transfer length, FUA). |
| `scsi/ScsiResult.scala` | `ScsiResult`, `ScsiStatus`, `Sense` (fixed-format sense builders) and `ErrorMap` (`VDiskError` → sense). |
| `scsi/Emulation.scala` | Commands that do not touch disk data: INQUIRY and VPD pages, READ CAPACITY (10), MODE SENSE/SELECT, TEST UNIT READY, REQUEST SENSE, START STOP UNIT. |
| `scsi/ScsiCommands.scala` | `ScsiHandler` and `ScsiCommands`: dispatch, unit attention, the data commands, the range lock. |
| `dbus/TcmuDbusService.scala` | `HandlerLogic` (CheckConfig/ChangeMedium/ConfigDesc logic) and `TcmuDbusService` (the system-bus objects and name). |

`src/main/java/org/aspen_ddp/aspen/vdisk/tcmu/dbus/TcmuService1.java` declares the `org.kernel.TCMUService1` D-Bus
interface. It is in Java because dbus-java derives D-Bus signatures from Java generic signatures, which Scala does
not emit for primitive type arguments.

### Elsewhere

| Path | What it holds |
|------|---------------|
| `cmdline/Main.scala` | `create-disk`, `list-disks`, `grow-disk`, `delete-disk`, and `vdisk-server [--network-cache-dir]` (`run_vdisk_server`, `tcmuEventLoop`). |
| `deploy/dbus/org.kernel.TCMUService1.conf` | The D-Bus policy that lets root own `org.kernel.TCMUService1`. Installed in `/etc/dbus-1/system.d/`. |
| `scripts/vdisk-e2e/` | Root-only end-to-end scripts: setup, restart under I/O, grow, teardown, and fio jobs. |
| `aspen`, `.jvmopts` | Both pass `--enable-native-access=ALL-UNNAMED` (the launcher's java command and the sbt test JVM). |
| `submodules/tcmu-runner` | tcmu-runner v1.5.4 sources: the reference for kernel interaction and SCSI behavior. Not built. |

## 3. How a disk is stored in Aspen

`DiskCatalog.create` commits all of the following in **one** transaction, so a name never resolves to a half-built
disk:

- **Name registration:** the name is registered in the namespaced registry under namespace `vdisk`, mapping to the
  disk id (a UUID). Names must be non-empty and may not contain `@`. (`@` once separated the disk name from a
  socket path in the cfgstring; `Cfgstring.parse` now rejects that form with a message saying it was removed.)
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
  encoded `DataObjectPointer`s. Its nodes, like the blocks, come from the disk's allocator (normally an allocation
  group), at up to 1 MiB each or the allocator's node size limit when that is smaller.

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
`requireDoesNotExist`. When anything is deleted the transaction calls `deferAllocationRelease()`. The deletions may
span several FSIF pools of the disk's allocation group; FSIF enrolls one release task covering all of them.

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
  read as zeros (looked up, no pointer, no dirty data, no in-flight commit). Zeroing is required because the
  frontend reports LBPRZ=1 (VPD 0xB2 and READ CAPACITY 16): unmapped blocks must read back as zero. If such a
  block's map entry isn't yet known, it is looked up before the unmap applies.
- **FLUSH** also waits for every outstanding deletion (`awaitDeletes`).

The epoch (`unmaps`) check is threaded through every commit loop. A data or map commit that an unmap superseded is
dropped or requeued, never retried. This is the subtlest part of the engine; most of `VirtualDiskSuite`'s UNMAP
cases cover these races.

### Grow

`VirtualDisk.grow` calls `DiskCatalog.grow`, which re-reads the stored size on every attempt:
- equal to the target: done, without a commit;
- larger than the target: `InvalidGeometry` (disks never shrink);
- otherwise: commits the new size under a key-revision requirement.

The disk then switches to the new geometry. Because grow is idempotent, a size reconfig whose reply was lost, or
one that follows a CLI `grow-disk`, succeeds. `DiskHandle.grow` short-circuits a grow to the size already being
served.

### Stop and close

`stop(err)` is final:
- cancels timers;
- fails every waiter, pending read, throttled write and delete waiter;
- clears all caches and queues.

No new transaction starts afterwards (`Host.checkServing`). `close()` is crash-only: it calls `stop(Fenced)`
without flushing, so unflushed writes are lost, exactly as they would be when power is pulled from a disk with a
write cache.

## 6. DiskHandle: the ownership lifecycle

`DiskHandle.open` loads the catalog entry but does **not** take ownership, so a TCMU device add can verify
geometry before fencing anybody. The operations:

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

## 7. The command ring (`ring/`, `TcmuDevice`)

Layouts come from `/usr/include/linux/target_core_user.h` (newer than the submodule's
`target_core_user_local.h`). Every multi-byte field is host byte order. The ring is `/dev/uioN`, opened
`O_RDWR|O_CLOEXEC` and mapped shared (`LinuxTcmuKernel.openRing`) for the size in
`/sys/class/uio/uioN/maps/map0/size`, into a `MemorySegment` of exactly that size owned by a shared `Arena`. All
access goes through that segment, so a bad offset throws instead of faulting, and once the arena is closed every
access throws.

**Layout.** The `tcmu_mailbox` header (`version`, `flags`, `cmdr_off`, `cmdr_size`, `cmd_head`, and `cmd_tail` on
its own cache line at offset 64), then the command ring at `cmdr_off` for `cmdr_size` bytes, then the data area up
to the end of the map. Entries are `PAD`, `CMD` or `TMR` (the low 3 bits of `len_op`), 8-aligned.

**Open** (`Ring.open`) validates the mailbox: version 2, the OOOC (out-of-order completion) flag, `cmdr_off` and
`cmdr_size` 8-aligned and inside the map, `cmd_tail` inside the ring. Consumption starts at `cmd_tail`, as
tcmu-runner does.

**Consuming** (`Ring.nextItems`, device thread only). It walks a private consume tail up to the kernel's
`cmd_head` (read with acquire semantics). For every entry:
- The length must be at least 8 and must stay inside the ring and before `cmd_head`; the op must be known. A
  violation means no later entry can be trusted: it throws `RingCorrupt` (see section 11).
- A `CMD` entry is decoded: no BIDI or DIF iovecs, the iovec array inside the entry, the CDB inside the map with a
  length its opcode supports, and every iovec inside the data area. A failure yields `RingItem.Malformed` with the
  entry dump, and the command is still completed (with HARDWARE ERROR).
- A `TMR` entry yields `RingItem.Tmr`. `TcmuDevice` logs it at info and takes no action.

**Completing** (`Ring.complete`, any thread, under the ring's own lock). Completions may be out of order. As
`tcmulib_command_complete` does, the response goes into the next `CMD` entry at the mailbox `cmd_tail` (skipping
`PAD` and `TMR`), carrying the command's `cmd_id`; then `cmd_tail` advances with release semantics. The whole
96-byte sense area is written, padded with zeros, since the slot may hold another command's request bytes. With
the kernel's READ_LEN flag, a read's `read_len` is filled in. `complete` writes nothing and returns false when the
`cmd_id` is not outstanding (a double completion) or when the mailbox `cmd_tail` is not the one this ring last
wrote (the kernel reset the ring). After a completion, `UioChannel.kick` writes 4 bytes to the UIO fd.

**Data.** `gather` copies data-out from the iovecs into a byte array; `scatter` copies data-in back. Data is copied
once each way. `ScsiHandler` requires `dataOut` to be called synchronously within `handle`, because the data area
belongs to the command only until it completes.

**Concurrency in `TcmuDevice`.** A read-write lock guards ring access from completions. Completions and gathers
take the read lock, so they run concurrently. A ring reset and `stop` take the write lock. Every command records the
ring `generation` it was consumed in; a completion for a command consumed before a reset is dropped and logged at
warn, because its data area may already belong to a retried command. After `stop` returns, nothing touches the
segment, so the caller may unmap it.

**Reset and resync** (`TcmuDevice.resync`, `Ring.resync`, `Configfs.control(...).resetRing`). As libtcmu does:
write `1` to `action/block_dev`, `1` to `action/reset_ring`, then `0` to `action/block_dev` (always unblocked,
even when the reset fails). This runs under the device's write lock and the ring's lock, then revalidates the
mailbox, restarts consumption at the new `cmd_tail` and forgets every outstanding command: the kernel retries
them. A failed reset is retried on the next notification. A startup-scan add (a device
left by a previous process) resets the ring before mapping it.

**Every command is completed exactly once.** A `Command` is handed to its Future before anything that can throw;
the Future never fails (`ScsiCommands.handle` turns every error into sense data); a `Malformed` completable entry
is completed directly; and `Ring.complete`'s outstanding-count check refuses a second completion.

**Per-command trace.** At debug, `TcmuDevice` logs each command: opcode, LBA, length, FUA, `cmd_id`, latency, status
and sense key.

## 8. Device lifecycle: netlink, configfs and UIO (`Netlink.scala`, `Configfs.scala`, `TcmuService`)

**Netlink** (`TcmuNetlink.open`). A generic netlink socket resolves family `TCM-USER` and its multicast group
`config`, joins the group, and sends `TCMU_CMD_SET_FEATURES` with `TCMU_ATTR_SUPP_KERN_CMD_REPLY`, so the kernel
waits for a reply to each event. (If the kernel answers EOPNOTSUPP it logs a warning and runs without replies.)
Events that arrive before the SET_FEATURES ack are kept for the first `receive`. Each event (`TcmuEvent.Added`,
`Removed`, `Reconfig`) is answered with the matching `*_DONE` command, `TCMU_ATTR_CMD_STATUS` (`0` or `-errno`) and
the device id. The kernel blocks the targetcli operation until the reply, so a failed add is a failed targetcli
command, and the reason is in vdisk-server's log.

**`Main.tcmuEventLoop`** never returns. A failed or throwing `onEvent` replies `-EIO`, because the kernel waits for
every reply. An unparseable message is logged at warn and dropped. A `receive` failure (for example ENOBUFS after
a multicast overflow) is retried after 1 ms, doubling to 1 s, and reset on the next success; the first failure of
a run, every 100th, and the recovery are logged.

**The UIO name** (`/sys/class/uio/uioN/name`, `UioName.parse`) is `tcm-user/<hba>/<device>/<cfgstring>`, where the
cfgstring is the device's `dev_config`, `<subtype>/<config>` (`aspen/<disk>`): the same string `CheckConfig` gets.
Only subtype `aspen` (`TcmuService.Subtype`) is served. The configfs directory is
`/sys/kernel/config/target/core/user_<hba>/<device>/`.

**cfgstring** (`Cfgstring.parse`). The kernel's cfgstring is `aspen/<disk>`; targetcli adds the `aspen/` prefix,
so the operator types `cfgstring=<disk>`. Anything from the first `;` is ignored. The old `<disk>@<socket>` form is
rejected with a message saying it was removed.

**Add** (`TcmuService.add`). All operations on one minor run one at a time, in arrival order (`serialized`).
1. Read the UIO name, check the subtype, parse the cfgstring.
2. Read configfs: size (`info`'s `Size:`), `attrib/hw_block_size`, `attrib/hw_max_sectors`, the map size, and
   `wwn/vpd_unit_serial` (the unit serial; the disk name if absent).
3. `DiskHandle.open` (an unknown disk is `-ENOENT`), bounded by `TcmuServiceConfig.acquireTimeout` (60 s;
   `-ETIMEDOUT`). A disk that opens after the timeout is closed when it does.
4. Check geometry: the kernel block size must equal the disk's logical block size (`-EINVAL`), and the kernel size
   must be a multiple of it.
5. `acquire` (takes ownership, fencing any previous owner), with the same timeout.
6. **Size.** A kernel size less than or equal to the Aspen size is served as is: READ CAPACITY and range checks use
   the kernel's size, so a disk grown with `aspen grow-disk` but not yet resized in targetcli still opens. A kernel
   size larger than the Aspen size is a grow: the add logs a warning with both sizes and calls `grow` to the kernel
   size, and replies only once the grow has committed. Grow cannot be undone, so a targetcli size typo grows the
   disk for good; the warning makes it easy to spot.
7. For a startup-scan add, reset the ring. Map the ring, validate it, build `ScsiCommands` and `TcmuDevice`, and
   start serving. Only then does the reply report success.

A failed add leaves nothing open: it stops the device, unmaps, and closes the disk crash-only (without
releasing it). An `Added` for a minor already served under the same UIO name (the startup scan found a device whose
event came after it) replies 0 and changes nothing. An `Added` for a served minor under a different UIO name means
the kernel unregistered the old device without a remove (it does that when an add's reply was not 0), so the stale
entry is dropped crash-only first.

**Remove** stops the device thread, `release`s the disk (flush, then close), and unmaps. It always replies 0: the
kernel removes the device regardless.

**Reconfig.** Size (`ReconfigChange.Size`): a smaller size or one not a multiple of the block size is `-EINVAL`; an
equal size is a no-op. A larger size grows the Aspen disk if needed (a size the disk already has, after
`aspen grow-disk`, needs no commit), then `ScsiCommands.resized` serves the new size and arms a CAPACITY DATA HAS
CHANGED unit attention. The reply is sent only after the grow committed. Reconfig of the cfgstring or the write
cache setting, or an unknown attribute, is refused with `-EINVAL`. A reconfig for a minor that is not served is
`-ENODEV`. A size reconfig is expected from targetcli's `set attribute dev_size=<bytes>` (configfs
`attrib/dev_size`; not yet verified on a real kernel); targetcli has no resize command for user backstores.

**Startup** (`Main.run_vdisk_server`), in order: check `geteuid() == 0` ("Error: vdisk-server must run as root"),
start the Aspen client, open netlink, take the D-Bus name, then `TcmuService.startupScan`. The scan adds every
`aspen` UIO device already present, resetting each ring first, so commands left by a previous process are failed
back to the kernel for retry and ownership is taken again through `acquire`. One device's failure does not stop
the others. Then it starts the `tcmu-netlink` thread and prints "Serving virtual disks through TCMU". A failure
before that point exits with status 1; nothing after it exits the process.

**Crash-only.** There is no clean shutdown. On a crash the kernel keeps its devices and their queued commands; the
initiator stalls, and unflushed writes are lost, as on power loss. The restarted server's scan resets the rings and
serves again.

## 9. SCSI emulation (`scsi/`)

`ScsiCommands.handle` never fails: every error, synchronous or not, becomes a CHECK CONDITION. Non-data commands
go to `Emulation`; the rest are data commands on the `LockableBlockDevice`. Ranges are checked against the kernel's
size (`DeviceSettings`), which may be smaller than the Aspen disk. It follows tcmu-runner's `tcmur_cmd_handler.c`
and `scsi.c`, with the differences listed in the two files' scaladoc.

| Command | Behavior |
|---------|----------|
| READ 6/10/12/16 | `read`; the data-in length must match the transfer length. |
| WRITE 6/10/12/16 | `write`, with FUA from `cdb[1] & 0x08` (`Cdb.fua`). |
| WRITE AND VERIFY 10/16 | Write, then flush. |
| SYNCHRONIZE CACHE 10/16 | `flush`. |
| UNMAP | `unmap` of up to 4 descriptors (no ANCHOR). A descriptor data length past the parameter list is PARAMETER LIST LENGTH ERROR and nothing is unmapped. |
| WRITE SAME 10/16 | With the UNMAP bit, an all-zero block and a range aligned to one block object: `unmap`. Otherwise chunked writes of the repeated block (at most 1 MiB and `hw_max_sectors` per write). |
| COMPARE AND WRITE | One block: read, compare, write (honoring FUA). A mismatch is MISCOMPARE with the byte offset in the INFORMATION field. |
| FORMAT UNIT | Unmaps the whole device, which then reads back as zeros. |
| INQUIRY | Standard (SPC-4, CmdQue) and VPD 0x00, 0x80 (unit serial), 0x83 (device identification), 0xB0 (block limits: max transfer from `hw_max_sectors`, unmap granularity and alignment of one block object), 0xB2 (logical block provisioning: LBPU, LBPWS, LBPWS10, LBPRZ). |
| READ CAPACITY 10, SERVICE ACTION IN 16 / READ CAPACITY 16 | The kernel's size; READ CAPACITY 16 sets LBPME and LBPRZ. |
| MODE SENSE / MODE SELECT 6/10 | Read-write error recovery, caching (WCE=1) and control pages. The mode parameter header sets DPOFUA, so Linux sends real FUA writes. MODE SELECT accepts only the current values. |
| TEST UNIT READY, REQUEST SENSE, START STOP UNIT | Emulated. |
| anything else | ILLEGAL REQUEST, INVALID COMMAND OPERATION CODE. |

**Unit attention.** After a size reconfig, the next command other than INQUIRY, REQUEST SENSE or REPORT LUNS gets
UNIT ATTENTION, CAPACITY DATA HAS CHANGED (`Sense.CapacityChanged`), once.

**Range lock** (`ScsiCommands.locked`). A COMPARE AND WRITE waits for every overlapping modification registered
before it, and a modification waits for every overlapping COMPARE AND WRITE registered before it. Ordinary writes
do not wait for each other.

**Errors → sense** (`ErrorMap`, values from tcmu-runner's `tcmu_sts_to_scsi`, fixed-format sense):

| Error | SCSI result |
|-------|-------------|
| `InvalidRequest`, `InvalidGeometry` | ILLEGAL REQUEST, INVALID FIELD IN CDB (5/0x2400) |
| `OutOfRange` | ILLEGAL REQUEST, LBA OUT OF RANGE (5/0x2100) |
| `Fenced`, `NotOwner` | NOT READY, 0x040B (tcmu-runner's `TCMU_STS_FENCED`) |
| `DiskFailed` | MEDIUM ERROR, WRITE ERROR (3/0x0C00) on a write, UNRECOVERED READ ERROR (3/0x1100) otherwise |
| anything else | HARDWARE ERROR (4/0x4400) |

Expected errors (fenced, not owner, out of range, invalid request) are logged at debug; others at warn.

## 10. D-Bus service (`dbus/`)

targetcli finds user backstore types through tcmu-runner's D-Bus service. `TcmuDbusService.start` connects to the
system bus (dbus-java, native Unix-socket transport) and exports:
- `/org/kernel/TCMUService1`: `org.freedesktop.DBus.ObjectManager`, whose `GetManagedObjects` lists the handler and
  its `ConfigDesc`;
- `/org/kernel/TCMUService1/aspen`: interface `org.kernel.TCMUService1` (`TcmuService1.java`), with the `ConfigDesc`
  property (`Cfgstring.ConfigDesc`), `CheckConfig(s) → (b, s)` and `ChangeMedium(s, t, s) → (i, s)`.

It then requests `org.kernel.TCMUService1` with `DO_NOT_QUEUE`. If another process owns the name (tcmu-runner),
startup fails with "org.kernel.TCMUService1 is already owned on the system bus (is tcmu-runner running?)".

`HandlerLogic.checkConfig` parses the full cfgstring (`aspen/<disk>`) and asks the `DiskCatalog` whether the disk
exists, waiting up to 10 s. It returns `(true, "success")`, or false with the reason (bad cfgstring, the removed
`@` form, disk not found, or Aspen unreachable). `ChangeMedium` returns `-EOPNOTSUPP`.

The policy file `deploy/dbus/org.kernel.TCMUService1.conf` lets root own the name and anyone send to it. It goes in
`/etc/dbus-1/system.d/`. A distribution tcmu-runner package's `tcmu-runner.conf` may already allow the same.

## 11. Failure handling and logging

The rule: **an unexpected condition in one command fails that command with sense data, logs the ring state, and
the device keeps serving.** Nothing after startup exits the process.

**`FailureLog`** (one per device) rate-limits reports by kind. The first report of a kind is logged at error with
its dump. Later reports within the window (60 s) are only counted. The next report after the window logs a warn
line "N more <kind> since <first>" and starts a new window. The kinds `TcmuDevice` reports:

| Kind | Meaning | What happens |
|------|---------|--------------|
| `malformed-entry` | A CMD or TMR entry failed validation (bad iovec, CDB, counts). | A CMD is completed with HARDWARE ERROR. |
| `ring-corrupt` | The ring itself is inconsistent (`cmd_head`/`cmd_tail` out of range, a bad entry length, an unknown op). | Ring reset and resync; the kernel retries outstanding commands. |
| `resync-failed` | The configfs reset or the mailbox revalidation failed. | Retried on the next notification. |
| `command-error` | A command's Future failed (should not happen: `ScsiCommands` never fails). | HARDWARE ERROR. |
| `data-overflow` | A response is larger than the command's iovecs. | HARDWARE ERROR. |
| `dispatch-error`, `completion-error`, `kick-error`, `loop-error` | An exception in that step. | The command is completed with HARDWARE ERROR where it can be. |
| `stale-completion` | `Ring.complete` refused a completion (not outstanding, or the ring was reset). | Nothing is written. |
| `channel-error` | `UioChannel.await` failed. | Retried after 1 ms, doubling to 1 s. |

**The dump** is the mailbox (`Ring.mailboxDump`: version, flags, `cmdr_off`, `cmdr_size`, `cmd_head`, `cmd_tail`,
commands in flight), plus, for an entry, its header (`len_op`, `cmd_id`, `kflags`, `uflags`), CDB in hex and iovec
list. The log line starts with the device name; `TcmuService` log lines name the device, the UIO minor and the
disk.

**Lifecycle failures** (netlink, configfs, open, acquire, grow) are logged with the step and errno (`tcmu: adding
device <name> (uio<N>, disk <disk>) failed at <step>: errno <n>: <message>`) and returned to the kernel as the
reply status. A D-Bus `CheckConfig` failure is logged at info.

### Failure behavior at a glance

| Event | What happens |
|-------|--------------|
| `vdisk-server` crashes or restarts | Unflushed writes are lost, as on power loss; flushed data is safe. The kernel keeps the devices; the initiator stalls. The restarted server's startup scan resets each ring, takes ownership again and serves. Commands not completed within the kernel's `cmd_time_out` (30 s default) fail. |
| A second server takes the disk | The new owner bumps the fence. The old owner's next commit aborts, its disk stops with `Fenced`, and its I/O returns NOT READY 0x040B. |
| A commit reports failure but did commit | Resolved by revision (data) or by finding the map change (map); counted in `Stats`. |
| A store serves an old revision | Bytes from the wrong revision are never returned or cached; the read retries. |
| An invariant breaks (a block at an unexpected revision, or its object gone) | The disk stops with `DiskFailed`, and I/O returns medium errors. Removing and re-adding the backstore (a new `acquire`) starts over. |
| One bad ring entry | That command fails with sense; the ring state is logged; the device keeps serving. |
| The ring is inconsistent | Logged, ring reset through configfs, resync; the kernel retries. |
| The targetcli block size doesn't match the disk | The add replies `-EINVAL`; targetcli's create fails. |
| The targetcli size is larger than the disk | The add grows the Aspen disk (warn logged). |
| The cfgstring names an unknown disk | `CheckConfig` refuses it; a device added anyway replies `-ENOENT`. |

## 12. Testing

| Suite | Covers |
|-------|--------|
| `DiskGeometrySuite`, `SegmentMapSuite`, `CleanCacheSuite`, `SequentialDetectorSuite` | Pure data structures. |
| `DiskCatalogSuite` | Create/load/list/grow/delete, name validation, idempotent grow and delete. |
| `VirtualDiskSuite` | Read-your-writes, coalescing, FLUSH/FUA durability, map batching, ranged vs. overwrite commits, readahead, UNMAP races, grow. |
| `VirtualDiskFailureSuite` | Takeover and fencing, crash-only close, unknown-outcome commits, adoption, stale stores. |
| `DiskHandleSuite` | acquire/release/close interleavings and lock states. |
| `tcmu/LibcSuite` | Real FFM downcalls: open (errno capture), pipe, poll, mmapShared and the closed arena, geteuid. |
| `tcmu/RingSuite` | The ring codec against `FakeKernel`, an `Arena`-allocated segment laid out like the real ring: PAD wraparound, out-of-order and concurrent completion, double completion, every malformed-entry and corrupt-ring case, resync. |
| `tcmu/TcmuDeviceSuite` | The serving loop: each command completed exactly once, failures logged, the device still serving, reset and stop races. |
| `tcmu/CdbSuite`, `SenseSuite`, `EmulationSuite`, `ScsiCommandsSuite` | CDB parsing, sense, golden INQUIRY/VPD/MODE SENSE/READ CAPACITY bytes, and each data command against `FakeBlockDevice`. |
| `tcmu/NetlinkSuite`, `ConfigfsSuite`, `CfgstringSuite` | Golden netlink messages; configfs/sysfs parsing and writes on a temporary tree; cfgstring rules. |
| `tcmu/TcmuServiceSuite` | Add/remove/reconfig against `FakeTcmuKernel`: geometry, grow on add, timeouts, serialization per minor, duplicate and stale adds, startup scan. |
| `tcmu/TcmuDbusServiceSuite` | `HandlerLogic` (CheckConfig, ConfigDesc, ChangeMedium) without a bus. |
| `tcmu/TcmuIntegrationSuite` | `FakeKernel` ring → real `TcmuDevice`/`ScsiCommands` → real `DiskHandle` on the in-process Aspen system: read-your-writes, FUA and FLUSH durability, UNMAP, grow before the targetcli resize, grow on add, fencing. |
| `cmdline/MainSuite` | `vdisk-server` argument parsing and `tcmuEventLoop` (replies, `-EIO`, backoff, logging). |
| `scripts/vdisk-e2e/` | Root-only: targetcli through our D-Bus service, iSCSI login, ext4, fio, restart under I/O, grow. Not run automatically. |

`VirtualDisk` exposes `private[vdisk]` test hooks:
- `commitGate`, `attemptGate`, `deleteLookupGate`, `readGate`: hold an operation at a precise point.
- `failNextCommitAfterSuccess`: simulate a lost commit reply.
- `rangeReader`: inject stale reads.
- `stats`, `dirtyBytes`, `fetchCount`, `cleanBytes`, `blockPointer`: introspection.

Use these to make a race deterministic rather than relying on timing. Flaky tests are not permitted.

## 13. Where to look

| Task | Start in |
|------|----------|
| Add or change a SCSI command | `ScsiCommands.dispatch`/`dataCommand` (data) or `Emulation.handle` (non-data); `Op` in `Cdb.scala`; `ScsiCommandsSuite`/`EmulationSuite`. Check tcmu-runner's `tcmur_cmd_handler.c` and `scsi.c`. |
| Change a VPD page, mode page or capacity response | `Emulation` (`vpdPage`, `blockLimits`, `modePages`, `readCapacity16`) and its golden vectors. |
| A command returns the wrong sense | `ErrorMap.result`, `Sense`; the debug trace line in `TcmuDevice.trace`. |
| A ring entry is rejected or the ring resets | The `malformed-entry`/`ring-corrupt` log line and its dump; `Ring.nextItems`/`decodeCmd`; `RingLayout`. |
| I/O hangs at the initiator | `TcmuDevice.completed`/`finish` (dropped completions are logged), `Ring.complete`, the `tcmu-<device>` thread's state; a `stale-completion` or `channel-error` line. |
| A targetcli create fails | The `tcmu: adding device ... failed at <step>` line; `TcmuService.add`/`readKernel`/`checkGeometry`; `CheckConfig` in `HandlerLogic`. |
| targetcli does not show `user:aspen` | The D-Bus policy file, the "Serving org.kernel.TCMUService1" log line, `TcmuDbusService.start`; `busctl introspect org.kernel.TCMUService1 /org/kernel/TCMUService1/aspen`. |
| Grow / resize | `TcmuService.resize` and `add` step 6 → `DiskHandle.grow` → `DiskCatalog.grow`; `ScsiCommands.resized`. |
| Startup or restart problems | `Main.run_vdisk_server`, `TcmuService.startupScan`, `TcmuNetlink.open`, `Configfs.control(...).resetRing`. |
| A new libc call | `sys/Libc.scala` (`fn`, `captureOpt`, `retrying`); `LibcSuite`. |
| Change write-back timing or limits | `VDiskConfig`; `urgent`, `dirtied`, `kick` and `throttleWrite` in `VirtualDisk`. |
| Change how commits are built | `BlockCommitter.runDataTx` / `runMapTx`. |
| A commit hangs | `busy()` / `kick()` gating, `awaitingMap`/`awaitingData`, `mapTxRunning`; use `commitGate` to reproduce. |
| A read returns stale or wrong data | The overlay snapshot in `readSegment`; the revision and generation checks in `fetchOnce`; `CleanCache.merge`. |
| Data reappears after UNMAP | `unmapBlock`, `pendingDeletes`/`requeueDelete`, the epoch checks in `dataInput` and `mapInputs`. |
| Unexpected `DiskFailed` | The message names the cause: `resolveData`, `adopt`/`gone` in `commitMap`, or a failed map lookup. |
| Takeover or lock issues | `DiskHandle.acquire`/`release`, `DiskCatalog.takeOwnership`, `VirtualDisk.fenceHeld`. |

## 14. Known limitations and planned work

The authoritative list is the vdisk entry in `Todo.txt`. The main items:

- **The root end-to-end run hasn't happened yet.** The scripts in `scripts/vdisk-e2e/` need root and were checked
  only with `bash -n`.
- **TMR entries are acknowledged by consumption only.** `TcmuDevice` logs them and takes no action (no abort of
  in-flight commands).
- **Fencing is detected lazily.** An idle fenced owner reports `Locked`. Part 3 (ALUA failover) needs an active
  fence check.
- **Readahead stays within one block object.** It never crosses a block boundary.
- **Part 3:** ALUA multi-path failover, built on `acquire`/`release`/`lockState`.
- **Part 4:** measurements across cell sizes.
