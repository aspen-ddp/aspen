# Crash Recovery Log (CRL): interface contract, SimpleCRL, and failure analysis

Status: derived from the code on `main` (commit c3ad5493), not from CLAUDE.md. Where CLAUDE.md or a design doc
disagrees with the code, the code is reported and the disagreement is listed in section 11. Three of the flaws in
section 11 were reproduced by running the real `SimpleCRL` from a scratch program outside the repository; those are
marked "reproduced". No source file was modified.

Path shorthand used for citations: `S` = `src/main/scala/org/aspen_ddp/aspen/server`. `S/crl` is the CRL package.
`file:N` is a line number in that file.

Sibling documents (not duplicated here): [paxos-transactions.md](paxos-transactions.md),
[transaction-requirements.md](transaction-requirements.md), [transaction-recovery.md](transaction-recovery.md),
[repair-process.md](repair-process.md), [rebuild-process.md](rebuild-process.md),
[systemic-loss-protection.md](systemic-loss-protection.md).

---

## 1. Purpose and fault model

A store is a Paxos acceptor for every transaction that touches one of its objects, and a participant that must be able
to apply a committed update it has voted for. Both facts live in memory. The CRL is the only place they are made
durable before the store speaks. If a store forgets a promise or an accepted value it can later promise or accept
something that contradicts what the other acceptors believe (a split decision). If it forgets the object data of a
commit vote, a decided commit can no longer be applied by that store.

The CRL therefore has one job: **make a store's per-transaction Paxos state, vote, transaction description and object
data durable before any message that depends on them is sent, and give them back after a crash.**

### 1.1 Assumptions the guarantees rest on

These are not enforced by code. Violating one voids the guarantees.

| Id | Assumption | Where it matters |
|----|------------|------------------|
| A1 | A write to a file opened with `DSYNC` (`O_DSYNC`) that returns normally is on stable storage, including after power loss. Disk write caches either honour flush/FUA or are disabled. | All of section 5.5 |
| A2 | A failed or never-returning `write` means the data is not relied on. Writes are never retried. | `S/crl/simple/StreamWriter.scala:56-62` |
| A3 | Exactly one `CrashRecoveryLog` instance has the device's `crl/` files open at a time, in exactly one process. Nothing enforces this with a file lock. | adoption, duplicate mounts |
| A4 | Backend commit completion means the object update is durable in the backend (RocksDB uses `setSync(true)`, `S/store/backend/rocksdb/BufferedConsistentRocksDB.scala:111`; FSIF journal force). The CRL entry of a committed transaction is deleted only after that. | section 3.4 |
| A5 | The CRL and the stores it serves are on the same device and move together (today). | sections 7-9 |
| A6 | One thread (the store's executor) calls `save`/`delete` for a given store; the CRL thread serialises everything else. | section 5.3 |

---

## 2. The CRL interface and its contract

Interface: `S/crl/CrashRecoveryLog.scala:117-171` (trait), factory `S/crl/CrashRecoveryLogFactory.scala:8-11`,
persisted unit `S/crl/TransactionRecoveryState.scala:8-14`.

`TransactionRecoveryState(storeId, serializedTxd, objectUpdates, disposition, status, paxosAcceptorState)` is the record
for one (store, transaction). `paxosAcceptorState` is `PersistentState(promised: Option[ProposalId],
accepted: Option[(ProposalId, Boolean)])`.

| Method | Meaning |
|--------|---------|
| `save(txid, state, onDurable)` | Make `state` durable. `onDurable` runs once it is. Saves for one (store, txid) replace one another; the newest wins. |
| `deleteTransaction(store, txid)` | Best-effort, lazy, **no acknowledgement**. A crash before the deletion is written leaves the transaction recoverable. |
| `deleteTransactions(store, txids): Future` | As above, but the Future completes only when the deletion is durable. |
| `dropTransactionObjectData(store, txid)` | Stop carrying the object data of this transaction in future entries. Memory-only; not durable by itself. |
| `readStoreState(store): Future[List[TRS]]` | The store's live transactions. Leaves them live. Ordered with saves (FIFO on the CRL thread). |
| `getFullRecoveryState(store)` | Blocking variant, used only by tests (`S/store/Frontend.scala:271`). |
| `loadStore(store, trs)` | Default: `save` each, complete when all are durable. Used to load a `crl_save.log`. |
| `dropStore(store): Future` | Delete every entry the store holds *at the time of the call*, durably. |
| `storesWithEntries(): Future[Set[StoreId]]` | Which stores have live entries. |
| `shutdown()` | Blocks until the CRL threads stop. Idempotent in SimpleCRL. |
| `admissionLimitBytes` | How many live bytes the CRL admits (section 6). |
| `openCRL(devicePath, config)` (factory) | Recover and open. **Throws** if the files cannot be recovered. |

### 2.1 Numbered contract

Items marked **(intended)** are what the design requires; section 11 lists where SimpleCRL does not meet them.

**Durability and ordering**

- **C1 (save-before-send).** `onDurable` of `save(t, s, h)` runs only after a log entry containing a record of `t` at
  least as new as `s` has been written with `DSYNC` and the write returned. A caller must send nothing that depends on
  `s` before `onDurable`.
- **C2 (entry order).** Entries are written one at a time, in serial-number order. Entry N+1 is not issued until entry
  N's write returned and its handlers ran (`writeInProgress`, `S/crl/simple/SimpleCRL.scala:166-168, 216-220`). So when a
  handler of entry N runs, every earlier entry is durable. Handlers of one entry run in unspecified order, on the
  writer thread.
- **C3 (single run).** Each handler runs at most once, and only if the entry's write returned normally. If the CRL
  thread dies, handlers never run (A2).
- **C4 (newest wins).** After recovery, each live transaction has the state of its newest durable record. Records in
  older entries never override newer ones (`LogEntry.loadStaticEntryContent`, `S/crl/simple/LogEntry.scala:169-185`).
- **C5 (what "live" means).** A record is live from its first durable save until a durable deletion of the same
  (store, txid) newer than it, or a `dropStore` covering it.

**Atomicity and corruption**

- **C6 (entry atomicity, intended).** A log entry is recovered whole or not at all. SimpleCRL approximates this with a
  stream UUID in the header and again as the last 16 bytes of the static region (`LogEntry.scala:21-31,109`,
  `StreamReader.scala:47-74`). It detects a write that stopped early at an aligned boundary; it does **not** detect
  reordered sectors or a silently truncated gather write (F1, F5).
- **C7 (prefix, intended).** Recovery returns the state as of a prefix of the entry sequence: the first unreadable
  entry ends the log (`StreamReader.findHighestEntry`, `StreamReader.scala:40-44`). Anything lost this way must never
  have been acknowledged. F1 and F2 are the cases where acknowledged entries fall outside the recovered prefix.
- **C8 (broken chain is fatal).** If an entry needed for recovery is unreadable, `openCRL` throws
  (`Recovery.BrokenEntryChain`, `Recovery.scala:16, 65-83`). It never returns a partial state silently.

**Deletion**

- **C9 (deletion finality in an entry).** A delete of (store, txid) applies to that txid's saves in the same entry and in
  all older entries (`LogEntry.scala:40-58, 169-185`). A save of the same txid queued after the delete into the same
  unwritten entry is dropped but its handler still fires (F4).
- **C10 (delete durability).** `deleteTransactions`/`dropStore` complete only after the entry carrying the deletion is
  durable. A delete with no durable record may be undone by a crash.

**Capacity**

- **C11 (live bound, intended).** The live data of a CRL never exceeds half a stream, so copying the live content of a
  recycled stream forward always fits (`SimpleCRL.scala:104-107`). Enforced only by prepare admission, which has
  gaps (F8). `Stream.writeEntry`'s `require` is the fail-stop backstop (`Stream.scala:28`).
- **C12 (sizing is fixed).** The count and file size of the streams are fixed at creation. Opening with another sizing
  throws `SizingMismatch` (`SimpleCRL.scala:84-93`).

**Ownership**

- **C13 (one CRL per device).** Every store on a device saves to, recovers from and deletes from the one CRL in
  `<device>/crl` (`CrashRecoveryLog.deviceCrlDir`, `CrashRecoveryLog.scala:15`).
- **C14 (readStoreState is ordered).** `readStoreState` is answered in order with the saves and deletes issued before
  it, so it sees every save whose call preceded it. `dropStore` relies on this to delete only what existed when called
  (`CrashRecoveryLog.scala:155-162`).
- **C15 (recovered status).** Disk recovery always returns `TransactionStatus.Unresolved`
  (`Recovery.scala:100`). `readStoreState` returns the in-memory record, whose status is whatever was last saved (F14).

---

## 3. How the CRL fits into the transaction workflow

### 3.1 What is saved, and what each completion gates

All `crl.save` call sites are in `S/transaction/Tx.scala`. The state saved is always
`TransactionRecoveryState(storeId, serializedTxd, trsObjectUpdates, disposition, txStatus, acceptor.persistentState)`.

| Event at the store | Save site | What `onDurable` sends | What it protects |
|--------------------|-----------|------------------------|------------------|
| Prepare handled, all objects loaded | `Tx.scala:742` (`respondToPrepare`) | `TxPrepareResponse` (Promise or Nack, vote, collisions) | Promise (Paxos), vote disposition, txd, object data |
| Prepare answered before objects loaded (response delayed) | `Tx.scala:427` (`sendDelayedPrepare`) | the delayed `TxPrepareResponse` | same; the disposition is decided at this moment |
| Accept handled | `Tx.scala:762` (`receiveAccept`) | `TxAcceptResponse` (Accepted or Nack) | Accepted proposal and value |
| Backend commits of this transaction complete | `Tx.scala:610` | nothing; `dropTransactionObjectData` | stop carrying object data |
| TxFinalized (all commits succeeded) | `Frontend.scala:481` via `Frontend.scala:537` | nothing; `deleteTransaction` | end of the record |

Consequences:

- **The vote and the promise travel together.** The first prepare response carries both the promise and the commit/abort
  vote, and both are in the state saved at `Tx.scala:742` before it is sent. This is what makes the 1-RTT commit safe
  across a crash. The Paxos side is described in [paxos-transactions.md](paxos-transactions.md).
- **Nacks are saved too.** The state is saved before a Nack is sent (no state change, but the same path).
- **No decision is saved.** The commit/abort resolution is not a CRL record. After a crash a transaction is `Unresolved`
  and is re-resolved by Paxos/status caches ([transaction-recovery.md](transaction-recovery.md)).
- **No vote is sent for a transaction the CRL has not accepted.** A dead CRL thread therefore stops every vote of every
  store on the device while the stores otherwise keep running (F9).

### 3.2 What data the record must contain

A commit vote requires the store to hold the object data it will apply (`Tx.scala` `missingObjectData`/`awaitingData`:
the vote is withheld until the data, a client prepare, the outcome, or `Frontend.MissingDataWait` arrives). So every
commit vote that reaches `onDurable` has its object data in the same record. Data that arrives *after* a vote
(`Tx.adoptObjectUpdates`) is persisted only by the next save of that transaction; a store that voted abort for lack of
data and later applies a commit with late data has that data only in memory. That store did not count towards the
commit's write threshold; if it crashes before its backend commit, the miss is handled by repair
([repair-process.md](repair-process.md)).

### 3.3 Loading and recovery of a store

1. The device's CRL is opened before any of its stores (`S/host/DeviceDiscovery.scala:498-520, 563-567`).
2. `StoreLoader.handleLoad` (`S/host/StoreLoader.scala:269-`) builds the `Store` with `deferRecovery = true`. If the
   store directory has a `crl_save.log` it is read first (`StoreLoader.scala:282`); an unusable one makes the store
   permanently unservable (`crlSaveFileLost`, `S/host/StoreRebuilds.scala:344`). Otherwise the store recovers from the
   device CRL.
3. `StoreRouter.readAndRecover` (`S/host/StoreRouter.scala:222-250`) runs on the store's executor and calls
   `readStoreState`. It never blocks: the Future completes on the CRL thread and a task is posted back to the executor.
   The store holds all transaction messages until `Frontend.recover` has recreated its transactions
   (`Frontend.scala:271-340`), so it saves nothing in between. A throw while reading or recovering fail-stops the store
   (`Host.StoreRecoveryFailed`).
4. For a `crl_save.log`: `store.crl.loadStore` re-saves every record into the device CRL, then `crlLoaded` removes the
   file durably (`DurableFiles.remove`) and only then recovers (`StoreRouter.scala:257-263`). A crash between "records
   durable in the device CRL" and "file removed" loads the same records again under the same ids, which is harmless
   because the store served nothing in between.
5. Each recovered transaction is counted into the device's `CrlAdmission` without a limit check
   (`Frontend.admitRecovered`, `Frontend.scala:458`).

### 3.4 Deletion is gated on the backend

`Frontend.transactionFinalized` (`Frontend.scala:537`) deletes the CRL record at TxFinalized, except that a committed
transaction with outstanding or failed backend commits keeps it: `TxCommits` (`Frontend.scala:209-210, 527`). A failed
backend commit keeps the record, with its object data, for the life of the loaded store. A reload recovers the
transaction from it. This is the rule behind A4.

### 3.5 Transfer out, drop, rebuild

| Operation | CRL calls | Source |
|-----------|-----------|--------|
| Planned device shutdown | stores close with `extractCrl = false`: records stay live in the device CRL; then `crl.shutdown()` | `DeviceShutdown.scala:215-232, 333-348` |
| Network transfer out, store open | `readStoreState` on the executor, `saveStoreState` to `crl_save.log` (tmp, fsync, rename, directory fsync), then `deleteTransactions` | `StoreLoader.scala:477-535` |
| Network transfer out, store closed | same sequence from `extractUnloaded`; fails if the store was opened meanwhile | `S/host/StoreTransfers.scala:91-113` |
| Rebuild of a store with lost state | marker `rebuild-required` durable, then `dropStore`, then the directory renamed and removed | `StoreRebuilds.scala:499-540` |
| Store removed from the device by other means | `dropLeftoverCrlState` finds entries of stores with no tree entry, directory, or load, and drops them | `S/host/DeviceReconciler.scala:76-110` |
| Store whose `crl_save.log` load failed | `closeUnloaded(dropDurably = true)` | `StoreLoader.scala:405-414` |

The rule that makes this safe: **an entry leaves the device CRL only after another durable copy exists, or after the
store has been made unservable on disk.** The transfer deletes after the save file is durable; the rebuild drops after
the marker is durable.

---

## 4. Per-device CRL: lifecycle

- Created: `StorageDeviceManager` writes the config, `owning-host`, and `createDeviceCrlDir` (which forces the device
  directory) (`S/StorageDeviceManager.scala:112`, `CrashRecoveryLog.scala:19-22`). A device that holds stores but has no
  `crl/` is considered to have lost it.
- Opened: `openDeviceCrl` (`DeviceDiscovery.scala:498-520`). On the control thread, blocking (known; Backlog.txt).
- Lost: if the directory is absent from a device that holds stores, **or `openCRL` throws anything** (`catch case t:
  Throwable`, `DeviceDiscovery.scala:512-515`), every store directory with a `config` file gets a durable
  `rebuild-required` marker, `crl/` is moved to `crl.lost-<uuid>` (kept deliberately, Backlog.txt) and a fresh CRL is
  opened. The reconcile pass later flips each store to Rebuilding. Nothing distinguishes "the files are damaged" from
  "the process hit an out-of-memory error while reading them" (F6).
- Unloaded: `shutDownCrl` runs `crl.shutdown()` on a cached pool and records the path in `closingCrls`, so the device is
  not loaded again over open files (`DeviceShutdown.scala:342-348`, `LocalDevices.scala:37`). That protects only against
  a second open in the same process (A3).
- Sizing: `crl-num-streams` (default 3) and `crl-max-file-size-mb` (default 300) come from the device's yaml
  (`S/StorageDeviceConfig.scala:30-31`). The config travels with the device. Editing it on an existing device makes
  `SizingMismatch` and therefore a lost CRL (F6).

---

## 5. SimpleCRL, the reference implementation

Code: `S/crl/simple/`. 1 417 lines in total.

### 5.1 Files and streams

A CRL is `numStreams` files `0.crl ... N-1.crl` in `<device>/crl`, each exactly `maxSizeInBytes` long. They are opened
with `CREATE | DSYNC | WRITE` (`StreamWriter.scala:38-41`). A file shorter than `maxSizeInBytes` is extended by seeking
to the last byte and writing one byte (`StreamWriter.scala:43-47`). This makes **sparse** files: no block is reserved
(F7). `SimpleCRL.checkSizing` rejects any directory that has some `*.crl` files but not exactly the expected set at the
expected size (`SimpleCRL.scala:84-93`).

The streams form a ring. Exactly one is **active** and written append-only. When an entry does not fit, the writer
advances to the next stream, **recycles** it (new random stream UUID, write offset 0), and continues there
(`SimpleCRL.scala:170-183`, `Stream.scala:14-21`). Recycling is in memory only: the old bytes stay on disk until the
new entry is written over them.

### 5.2 Entry format

Header (`LogEntry.scala:124-141`), 70 bytes, big-endian:

| Field | Bytes |
|-------|-------|
| stream UUID | 16 |
| entry serial number | 8 |
| oldestEntryNeeded (serial) | 8 |
| previous entry location (stream 2, offset 8, length 4) | 14 |
| dynamic data size | 8 |
| static data size | 8 |
| number of transactions | 4 |
| number of deletions | 4 |

Then, in order: **dynamic data** (serialized transaction descriptions and object-update payloads, written only the
first time a transaction appears in a stream, `LogEntry.scala:90-100`), **static data** (one fixed record per
transaction in the entry, then 33-byte TxIds of deletions, then the 16-byte stream UUID again), then zero padding to a
4 KiB multiple (`padTo4k`, `LogEntry.scala:187-195`).

The static record of a transaction (`LogContent.scala:116-132`): TxId 33 (pool UUID 16, store index 1, tx UUID 16),
txd location 14, disposition 1 (0/1/2), Paxos mask byte 1 + promised (int + byte) 5 + accepted (int + byte) 5, object
update count 4, then `count x (object UUID 16 + data location 14)`. That is 63 bytes plus 30 per object update. The
Paxos mask has bit 0 = promise present, bit 1 = accepted present, bit 2 = accepted value (`LogContent.scala:149-173`).

So a save of an existing transaction costs only its static record (no data is rewritten), but every entry is at least
4 KiB and every write is a `DSYNC` (section 5.5).

### 5.3 In-memory structures and threads

Two threads per CRL:

- the **CRL thread** (`ioThread`) takes `ActionRequest`s from `ioQueue` and owns all mutable state: the `transactions`
  map, the age-ordered queue of `Tx` (`queueHead`/`queueTail`), `currentLogEntry`, the stream write offsets;
- the **writer thread** (`StreamWriter`) takes `Write` commands, does `channel.position(offset); channel.write(buffers)`,
  and then runs `onWriteComplete`, which runs the entry's handlers and queues `WriteComplete` back to the CRL thread
  (`StreamWriter.scala:67-80`, `Stream.scala:35-38`).

Handlers therefore run on the writer thread. An exception in a handler propagates out of `writeThreadLoop` and kills
the writer (F9).

`transactions: Map[TxId, Tx]` holds the full `TransactionRecoveryState` of every live record, **including object data**,
in memory. `Tx` (`LogContent.scala:93-`) also records where its txd and object data currently are on disk
(`txdLocation`, `objectUpdateLocations`), a `keepObjectUpdates` flag, and its place in the queue
(`entrySerialNumber` = the serial of the entry where it entered the queue).

### 5.4 Write path

`save` is a message to the CRL thread. `onSaveTransaction` (`SimpleCRL.scala:267-289`):

1. Look up the `Tx`. If absent, create it, put it at the queue head with the *current entry's* serial, and register it.
2. If the number of object updates changed, forget the old data locations so the new data is written
   (`SimpleCRL.scala:281-283`).
3. Set `tx.state = state`, add the tx and the handler to `currentLogEntry`, call `startWrite`.

`startWrite` writes `currentLogEntry` only if no write is in progress (`SimpleCRL.scala:166-168`). While a write is in
flight, further saves accumulate in the next entry: this is group commit, and the only batching mechanism. After a
write, `currentLogEntry = new LogEntry(location, serial + 1, oldestEntryNeeded)` (`SimpleCRL.scala:190-191`).

`writeCurrentLogEntry` (`SimpleCRL.scala:170-191`):

1. If the entry does not fit the current stream, advance `currentStream`, `recycleStream()` it, and for every live `Tx`
   whose txd or object data lies in the recycled stream (`Tx.closeStream`, `LogContent.scala:185-204`) clear its
   locations, move it to the queue head and add it to the entry being written. Then set the entry's
   `oldestEntryNeeded` to the queue tail's serial.
2. `Stream.writeEntry` builds the buffers (`LogEntry.createEntryBuffers`, `LogEntry.scala:59-121`): header, then the
   txd and update buffers of transactions that have no location, then the static region, then padding. It issues one
   gathering write.

The completion callback is attached to the write; all handlers of the entry fire after it returns.

`oldestEntryNeeded` is the serial of the oldest entry any live transaction's *first* appearance (or last move) is in.
Because a transaction's txd and data are written once and referenced by location from every later record, the oldest
entry needed is the one holding the oldest live transaction's data (`SimpleCRL.scala:162-164`).

### 5.5 fsync behaviour

- Every stream file is opened `DSYNC`: each `write` returns only after data and the metadata needed to read it are on
  stable storage (A1). There is no separate `force`, no `fdatasync` call, no `O_DIRECT`.
- Each entry is one `write(ByteBuffer[])`, padded to a 4 KiB multiple. Exactly one write is in flight per CRL.
- Throughput is therefore `entries/s = 1 / write latency`, and batching comes only from saves arriving while a write
  is in flight. A slow `DSYNC` delays every vote of every store on the device.
- The first pass over a freshly created sparse file additionally forces block allocation and journal commits, so the
  first cycle (300 MB by default) is slower than steady state.
- `DurableFiles` (`S/DurableFiles.scala`) is *not* used by SimpleCRL's log. It is used for `crl_save.log`, the
  `rebuild-required`, `transferring-out` and `owning-host` markers: write a temp file, `force(true)`, atomic rename,
  fsync the directory (`DurableFiles.scala:35-56`). Directory fsync is a no-op on Windows and not `F_FULLFSYNC` on
  macOS (`DurableFiles.scala:69-77`); Linux is the only supported platform.
- Neither the creation of the stream files nor the `crl/` directory entry is followed by a directory fsync
  (F11).

### 5.6 Recovery

`SimpleCRL.apply` (`SimpleCRL.scala:62-80`) checks the sizing, runs `Recovery.recover`, and constructs the CRL.

`Recovery.recover` (`Recovery.scala:30-111`):

1. For each stream, read the **head entry** (offset 0) and keep the stream whose head has the highest serial. That is
   the active stream.
2. Walk the active stream from offset 0 with `findHighestEntry`, stopping at the first entry that fails
   `StreamReader.readEntry`. The last good entry gives the next write offset, next serial and the
   `oldestEntryNeeded`.
3. From the last entry, follow the `previous entry` links backwards, loading each entry's static region into a
   `RecoveringState`, **until reaching the entry whose serial equals `oldestEntryNeeded`**. Each step must reach a
   strictly older serial and a readable entry, or `BrokenEntryChain` is thrown (`Recovery.scala:65-83`).
4. Entries are applied newest to oldest. A transaction already present is not replaced; one named in a deletion of
   the same or a newer entry is skipped (`LogEntry.scala:169-185`). That implements C4 and C9.
5. Each surviving transaction's txd and object data are read from their recorded locations (`readStreamLocation`,
   `Recovery.scala:87-89`). **There is no checksum on any of it.**

`StreamReader.readEntry` accepts an entry iff the header UUID and the trailing UUID both equal the file's current UUID,
where the "current UUID" of a file is read from its first 16 bytes (`StreamReader.scala:15-19, 47-74`). A recycled
stream gets a new UUID with its first new entry, which is what makes all older entries in that file unreadable.

After recovery the CRL constructor **re-saves every recovered transaction** into the active stream
(`SimpleCRL.scala:132-137`), waiting for the write. Every open therefore appends a full copy of the live set (up to
half a stream), and may itself rotate and recycle. It does not wait for store recovery.

### 5.7 Recycling (compaction) strategy, restated as an argument

The design invariant is: **before a stream is overwritten, everything live in it has been re-emitted**. Live data is
bounded by half a stream, so the re-emission always fits. The mechanics are in 5.4(1). Why it is sufficient for the
chain walk: after the moves, every live transaction's queue position is in the new entry, so the new entry's
`oldestEntryNeeded` is at least the new entry's own serial or the serial of the oldest *unmoved* transaction, which by
the ring order is in a stream younger than the recycled one. So no entry in the recycled stream is needed afterwards.

What the argument does **not** cover is the instant of the overwrite itself: the re-emission is written *into the same
stream whose old contents it replaces*, starting at offset 0 (F3).

### 5.8 Deletion

`deleteTransaction` queues `DeleteTransaction`; the CRL thread records the TxId in the current entry's deletions, removes
the `Tx` from the map and the queue, and calls `startWrite` (`SimpleCRL.scala:291-296`). Deleting therefore causes an
entry write (not coalesced with anything unless a write is already in flight). `deleteTransactions` does the same for a
list, atomically in one entry, and attaches its handler to that entry (`SimpleCRL.scala:202-214`); an empty list
completes at once, with no write. A deleted `Tx` that is already in the *current* entry's `txs` stays there and is
filtered at recovery by the deletion list.

### 5.9 Shutdown and thread death

`shutdown()` queues `Shutdown` behind earlier requests; the writer drains earlier writes, closes the channels and
signals (`SimpleCRL.scala:343-346`, `StreamWriter.scala:76-80`). A pending, unwritten `currentLogEntry` is discarded.
That is safe: none of its handlers has run (C3).

If the CRL thread or the writer thread dies, the `Shutdown` is completed by the dying thread, channels are closed, and
`onThreadDeath(t)` is called (`SimpleCRL.scala:145-155`, `StreamWriter.scala:55-65`). The host's `onThreadDeath` only
logs (`S/../cmdline/Main.scala:1700-1707`).

---

## 6. Admission and backpressure

`CrlAdmission` (`S/crl/CrlAdmission.scala:27-66`) is one atomic byte counter per device, created at device load with
limit `crl.admissionLimitBytes` = half a stream (`DeviceDiscovery.scala:567`, `SimpleCRL.scala:107`).

- A store admits a prepare for a transaction it has not seen only while `total < limit` and adds the transaction's
  description, object data and static record (`63 + 30 per update`) (`Frontend.admitPrepare`, `Frontend.scala:426-448`).
  Otherwise it drops the prepare silently (`Store.scala:120-125`).
- Everything else is handled: known transactions, their later prepares, accepts, resolutions, heartbeats, finalization.
  Late object data is charged without a check; recovered transactions are charged without a check
  (`Frontend.scala:458-469`). One admission may cross the limit.
- Bytes are released when the record is deleted, or when the Tx is forgotten without ever reaching the CRL
  (`Frontend.deleteTransactionState`, `releaseAdmission`). A closing store keeps its bytes **retained** until its
  entries leave the CRL or the store reloads (`CrlAdmission.scala:50-64`, `Frontend.close`, `Frontend.scala:493`).
- Admission does **not** cover: records of stores that are not loaded since process start (F8), the duplication caused
  by rewriting object data when the update count changes, or deletions queued in an entry.

Admission is a liveness tool and the only thing that maintains C11. A failure of C11 is not a data-loss event by
itself: it kills the CRL thread (`Stream.scala:28`), which stops the device (F9) and leaves the CRL recoverable on
restart.

---

## 7. Sharing one CRL among a device's stores

All stores on a device share one ring, one writer thread and one admission counter. Consequences, all verified in code:

- **Ordering across stores.** Entries interleave records of different stores. Recovery returns each store's records by
  filtering on `StoreId` (`SimpleCRL.scala:261-265`).
- **Deleting one store does not touch another.** `dropStore` lists the store's TxIds with `readStoreState` and then
  `deleteTransactions` them; other stores' records stay live and keep being copied forward.
- **A store being rebuilt.** Its records stay live in the shared ring until `discardLostStore` has made the
  `rebuild-required` marker durable and then `dropStore` completes (`StoreRebuilds.scala:499-522`). Until then they are
  copied forward, uncounted by admission after a restart (F8). After the drop, the directory is renamed and removed. The
  rebuild starts only when the path is clear.
- **A store that fails.** A backend fail-stop at close marks the store `rebuild-required`; its entries are not dropped
  until the rebuild discards it. A store whose `crl_save.log` is unusable is marked the same way.
- **A shared stall.** A slow `DSYNC` or a full ring affects every store on the device, including metadata or bootstrap
  stores if they are on it. There is no per-store allocation by design (Todo/anti-goals of the 2026-11-07 spec).
- **Fate sharing.** A lost or unrecoverable CRL marks **all** stores on the device `rebuild-required`.

---

## 8. Co-locating the CRL with the stores it serves

This is the only layout implemented (`CrashRecoveryLog.deviceCrlDir(devicePath) = devicePath / "crl"`).

### 8.1 Benefits

- **One failure domain and one unit of movement.** The device carries every store's data *and* its transaction state.
  A drive pulled from a crashed or running host is in exactly the state a crash would leave it in, so another host can
  adopt it without any cooperation from the old host (`DeviceDiscovery.tryAdoptDevice`, `DeviceDiscovery.scala:314-`;
  design 2026-11-08). No state travels by network or by a shutdown step.
- **No cross-device ordering problem.** "Backend commit durable, then delete CRL record" (A4) is within one device. The
  CRL and the data cannot get out of step by one surviving without the other.
- **Simple recovery.** A restart finds one self-describing directory. A missing or unreadable `crl/` on a device with
  stores has a single, uniform treatment (section 4).
- **Sequential, append-only access.** SimpleCRL writes ascending offsets within one file at a time.

### 8.2 Drawbacks

- **The IO is doubled, and the bandwidth halved.** Every byte of transaction object data is written once to the CRL
  and again to the backend (RocksDB WAL and SST, or the FSIF journal and segment). Copy-forward writes live data a
  third time, and every open writes the whole live set again (section 5.6). Each entry is padded to 4 KiB and written
  with `DSYNC`. On a device that is also serving the stores, CRL traffic competes with store traffic for the same
  bandwidth and the same flush queue.
- **Flushes couple the workloads.** On a journaling file system an `O_DSYNC` write forces a journal commit and a
  device cache flush. A flush issued for CRL durability also drains the store's own pending writes, and the other
  way round. A busy backend therefore raises CRL latency, and CRL latency is on the critical path of every vote.
- **Wear.** On flash the CRL ring is rewritten continuously (up to `numStreams x size`, 900 MB default, plus
  padding), adding write amplification to the device that holds the data.
- **Rotating disks are hit hardest.** Each CRL commit is a synchronous write with a rotational delay (about 4 ms
  average on 7200 rpm) and, because the CRL files are far from the backend's files, a seek. Interleaved with the
  backend's own writes (RocksDB WAL appends and compactions, FSIF journal and segment writes) the head moves between
  regions several times per transaction, and the single-outstanding-write rule means the seek is on the vote's
  critical path. Group commit hides some of this, but the first-cycle sparse allocation, the fsync coupling above and
  the 4 KiB padding all work against a spinning disk. The result is that an HDD-backed store pays most of its
  latency in the CRL while also halving the bandwidth left for data.
- **Capacity is taken from the data.** 900 MB by default per device, not reclaimable by the store; `crl.lost-*`
  directories add more (kept deliberately).
- **Single-device failure is total.** If the device dies, state and data die together. This is by design (the stores
  are rebuilt from peers), but it is also why a damaged CRL turns into a rebuild of every store on the device.

---

## 9. Planned (not implemented) off-device CRLs

Backlog.txt describes placing the CRL of an HDD on an NVMe device. Nothing in the code reads a per-device CRL
location today; `deviceCrlDir` is the only definition and the factory takes only `devicePath` and the config. The
analysis below is therefore about the proposal, and about what the code already assumes.

### 9.1 What it buys

- **Faster votes on slow media.** CRL appends go to flash: no seek, a cheap `DSYNC`, group commit that works. The HDD
  then sees one write of the data, not two, and its heads stay with the data.
- **Less interference.** The flush coupling of section 8.2 disappears for the store's device; admission can use a larger
  ring without taking HDD capacity.
- **Isolation of a slow disk's CRL stalls** (partly: a shared NVMe CRL device becomes a new shared resource).

### 9.2 What it costs

- **Failure modes span two devices.** The records of an HDD's stores now live on a different physical device with a
  different lifetime.
  - Loss of the CRL device loses the Paxos and vote state of **every** device that used it. Under the current rule
    (lost CRL means every store on the device is `rebuild-required`) this is a simultaneous rebuild of many drives,
    and the stores lost together are likely to belong to the same pools. That is the correlated-loss pattern in
    [systemic-loss-protection.md](systemic-loss-protection.md), now triggered by one SSD.
  - Loss of the data device leaves orphaned records on the CRL device; `dropLeftoverCrlState` handles stores that are
    gone from the tree.
  - The two devices can disagree after a partial failure (data device restored, or rolled back by its own write
    cache, while the CRL says the transaction was finalized and deleted). Nothing in the design cross-checks them.
- **A device cannot be moved by simply pulling it.** Its transaction state is not on it. The proposal is a clean
  shutdown that copies the CRL state onto the device as the last step, before `shutdown.state`
  (Backlog.txt, "Off-device CRLs"). Consequences:
  - A crash, a power loss, or a drive pulled early cannot be adopted elsewhere; the adopter refuses a device with
    stores and no `<device>/crl` (`DeviceDiscovery.scala:324-328`) until it returns to the host that holds the CRL.
    This removes the benefit that the unclean-adoption design was written for.
  - The copy step becomes part of the durability story: temporary directory, `force`, directory fsync, atomic rename,
    then the owner marker. A pulled drive in the middle of it has neither a valid old nor a valid new copy.
  - The lost-CRL rule must consult the configured location; two live copies must never exist; a stale `crl/` on the
    device must be removed durably once the CRL lives elsewhere.
- **A cross-device barrier is needed for A4.** The delete of a record on the CRL device must follow backend durability
  on the data device. That already holds (fsync per device); it becomes a property to test rather than free.
- **Sizing and recovery location.** Sizing still travels with the device config; recovery time and memory are those of
  a host-level CRL again (up to the sum over devices).
- **Operational blast radius.** More failure surface: the CRL device's own space, health and mount, and a host that
  must also back up or restore it consistently.

### 9.3 Practical reading

Off-device CRLs improve latency and throughput on slow media and give up two things the current design has:
adoption without cooperation, and independent failure domains. They are reasonable for a host-bound deployment of
HDD pools with a mirrored, monitored CRL device, and a poor fit where drives move or hosts are replaced by pulling
disks. Whatever is built must keep "CRL lost means rebuild" from firing on a bulk event.

---

## 10. Failure scenarios

Each scenario states what the code does, whether it is safe, and why. "Safe" means no acknowledged state is lost and
no wrong state is recovered, assuming A1-A6.

| # | Scenario | Outcome | Safe? |
|---|----------|---------|-------|
| 10.1 | Crash mid-write of an appended entry | See below | Yes for appends; no for a rotation write (F3) |
| 10.2 | Torn or corrupted entry | See below | No, partially (F1, F5) |
| 10.3 | Power loss with write caching | See below | Yes if A1; no if the drive lies |
| 10.4 | A full stream | See below | Yes by design; F3 at the moment of the overwrite |
| 10.5 | A slow fsync | See below | Safe; liveness only |
| 10.6 | A dead CRL thread | See below | Safe for state; F9 for operations |
| 10.7 | The device is pulled | See below | Safe |
| 10.8 | Adoption by another host | See below | Safe under A3 |
| 10.9 | Crash during transfer-out, before `crl_save.log` durable | See below | Safe |
| 10.10 | Crash during transfer-out, after `crl_save.log` durable | See below | Safe |
| 10.11 | Crash between `dropStore` and directory removal | See below | Safe |
| 10.12 | Stores sharing a CRL, one rebuilt | See below | Safe except F8 and F12 |
| 10.13 | Crash during recovery's re-save | See below | Safe unless it rotates (F3) |
| 10.14 | CRL files unrecoverable at open | See below | Safe for the CRL; F6 for the system |
| 10.15 | Sizing mismatch | See below | Safe for state, destructive (F6) |
| 10.16 | A completion handler throws | See below | State safe; device stops |
| 10.17 | Device full | See below | Not safe for liveness (F7) |

**10.1 Crash mid-write.** An appended entry is written after the last valid one in the active stream. A crash leaves
either nothing, a prefix of the new bytes, or (without ordering guarantees from the disk) an arbitrary subset of its
sectors. None of its handlers has run, so no message depended on it. Recovery stops at the previous entry
(`findHighestEntry`): the tail UUID is the last thing written, and sectors beyond the previous entry carry the stream's
older cycle (different UUID) or zeros, so the partial entry is rejected. The next entry is written over it. Safe for
appended entries *provided every sector of the new entry either fully persisted or the header/tail check catches it*
(see 10.2). A rotation write is different: it overwrites live data, and its failure can destroy state that was
acknowledged (F3).

**10.2 Torn or corrupted entry.** Detection is two 16-byte UUIDs. Cases:

- Prefix persisted, tail not: detected (tail UUID mismatch). Entry ignored, its successors invisible.
- Header persisted, tail persisted, a middle sector not (sector reordering): **not detected**. The decoder reads old
  bytes of an earlier cycle where the dynamic or static data should be. `Tx.loadTx` validates only the disposition
  byte (`LogContent.scala:60-64`); promise and accept numbers, locations and counts are unvalidated. Since the entry is
  the last one, nothing was acknowledged from it, but its garbage becomes a recovered transaction state or throws
  `CorruptedEntry`/`BufferUnderflow` (then the CRL is declared lost, F6). If it is *not* the last entry, the entries
  after it are acknowledged and now reachable only through it (F5).
- A write that silently stops short: **not detected** and **acknowledged**. The gathering write is limited to 1 024
  buffers per call and the return value is ignored, so a large entry completes its handlers while missing its
  tail. Reproduced (F1).
- Corruption of an already-durable entry later (bit rot): a bad UUID ends the log at that entry (C7), or, if a needed
  older entry is hit, `BrokenEntryChain` makes the CRL unrecoverable (C8). Bad data bytes are not detected at all (no
  checksum).

**10.3 Power loss with write caching.** `DSYNC` on Linux sends flush/FUA when the device reports a volatile cache.
With a drive that acknowledges flushes it did not perform, an acknowledged entry can be lost together with later ones
(recovery then returns a shorter prefix). That is Paxos amnesia: a promise or accept may be forgotten after being
sent. The CRL cannot detect it. The same drive failure would break the backends too; it is an operational
requirement (A1), not a software property.

**10.4 A full stream.** `canWriteEntry` fails, the next stream is recycled, live content with data in it is re-emitted
into the same file from offset 0 (5.4, 5.7). If the entry still does not fit (live set larger than a stream because of
F8 or a large pending entry), `Stream.writeEntry`'s `require` throws on the CRL thread: the CRL thread dies, the device
stops (10.6). The files are unchanged by the failed attempt (the recycle is in memory), so a restart recovers. The
overwrite itself is not crash-safe (F3).

**10.5 A slow fsync.** Entries are strictly serial, so every vote on the device waits. The pending entry grows with the
saves that arrive; it is bounded only by admission (for new transactions) and the live set. Admission drops new prepares
at half a stream, so clients retransmit and the cluster slows. Long waits push transactions past
`Tx.CollisionWait`/heartbeat timeouts, so other stores drive them or abort. No state is lost. A `DSYNC` that never
returns blocks the writer thread and the device indefinitely; there is no timeout or detection (Backlog.txt).

**10.6 A dead CRL thread.** `onThreadDeath` logs. `save` calls are queued and never answered, so no vote, accept or
promise is sent: **no safety loss**. But also `readStoreState`, `deleteTransactions`, `dropStore` and
`storesWithEntries` are served by the same thread and never complete: store loads, transfers, rebuilds and reconciles
that wait on them hang. The stores stay installed and keep serving reads while they cannot vote (F9).

**10.7 The device is pulled.** Writes fail with an I/O error: the writer thread dies (10.6). Whatever was acknowledged
before is on the device. The host's device handling then takes over (not CRL logic). The pulled drive contains a
consistent post-crash CRL, which is what makes adoption possible.

**10.8 Adoption by another host.** The adopter does not open the CRL on its own initiative: `tryAdoptDevice` requires
`<device>/crl` and a directory for every Offline/Active store, commits an ownership transaction, then loads the device
(`DeviceDiscovery.scala:314-` and `openDeviceCrl`), which runs the recovery of 5.6 and re-saves the live set. Safe
under A3: there is no fencing and no file lock. A running old host relinquishes the device when it sees the tree name
another owner (`DeviceShutdown.relinquishDevice`, `DeviceShutdown.scala:242`) but until it does it may still write if
the drive is reachable by both (not defended, by decision). A device with `transferring-out` and no valid
`crl_save.log` is adopted as lost (`DeviceDiscovery.adoptionClass`, `DeviceDiscovery.scala:286-`). A device with a
`rebuild-required` store is adopted with that store Rebuilding.

**10.9 Transfer-out, crash before `crl_save.log` is durable.** The state is still in the device CRL: `extractCrl` reads
it with `readStoreState` (leaves it live) and deletes only after `saveStoreState` returned (tmp, fsync, atomic rename,
directory fsync) (`StoreLoader.scala:497-523`). The `transferring-out` marker is written only after the close completes.
A restart finds no marker, loads the store normally and recovers from the device CRL (a stray temp file is ignored).
Safe.

**10.10 Transfer-out, crash after `crl_save.log` durable, before the CRL delete is durable.** The records are in both
places. The restart finds a valid `crl_save.log` and loads from it: `loadStore` re-saves under the same ids, the file
is removed durably, then the store recovers (`StoreRouter.crlLoaded`). The device CRL record may be older or equal;
the re-save makes the file's state the newest. Safe because the store served nothing between the extraction and the
reload, so the two copies are identical. If the delete became durable first and the file did too, the save file is the
only copy and is what the destination validates before promotion (`StoreTransfers.scala:480`). If the file is later
lost or truncated, `loadValidStoreState` rejects it and the store is `rebuild-required`; the source retries until the
staged copy validates, since the source still holds the state.

**10.11 Crash between `dropStore` and the directory removal.** The `rebuild-required` marker is durable before the
drop, so the store can never be served; the drop completes only when its entry is durable
(`StoreRebuilds.scala:499-522`). On restart the marker is still there, `discardLostStore` runs again, `dropStore` finds
no entries (`DeleteTransactions` with an empty list completes without a write) and the directory is removed. A crash
before the drop is durable leaves the entries live and the marker present, so the next attempt drops them. Safe.

**10.12 Several stores share a CRL; one is rebuilt.** The rebuilt store's records are deleted through the same ring in
one entry; the others' records are untouched. A late save for the same StoreId after the drop (store loaded again) is
kept, by design (C14). The one hazard is a same-txid re-save racing the drop's delete, which removes it (F12). The
rebuilt store comes back as a fresh StoreId-identical store with no Paxos memory; whether that is safe is a property of
the rebuild process (F18).

**10.13 Crash during recovery's re-save.** The constructor appends a full copy of the live set before the CRL accepts
requests. A torn append is ignored like any torn append (10.1); the old entries are still the recovered prefix. If that
append does not fit and rotates, the F3 hazard applies to the open itself, and repeated crashes (a crash loop) rotate
the ring at the rate of the live set size.

**10.14 CRL files unrecoverable.** `openCRL` throws (`BrokenEntryChain`, `CorruptedEntry`, I/O error, anything).
`openDeviceCrl` marks every store with a config file `rebuild-required` (durably, one by one), moves `crl/` aside, opens
a fresh one (`DeviceDiscovery.scala:512-520`). A crash between the markers and the move leaves markers and the old
`crl/`: the next open may succeed (e.g. transient error) but the stores stay unservable behind their markers. The
decision is irreversible and made from local evidence only (F6).

**10.15 Sizing mismatch.** `SizingMismatch` is thrown before any file is read; the handling in 10.14 follows. State
is not harmed (the files are kept as `crl.lost-*`), but all stores are rebuilt. A crash during the creation of the
stream files of a new CRL produces the same mismatch; harmless only if the device holds no stores yet.

**10.16 A completion handler throws.** `runCompletionHandlers` is called inside the writer loop
(`StreamWriter.scala:74-75`, `Stream.scala:35-38`): the exception escapes the loop, the writer thread dies, remaining
handlers of that entry never run, and the device stops (10.6). No state is lost.

**10.17 Device full.** The stream files are sparse: block allocation happens on first write to each region. On a full
device (or a copy-on-write file system, where an overwrite also allocates) a CRL write can fail with `ENOSPC`; the
writer dies. Since freeing space requires transactions (deletes, refcount changes), the device can be unable to
recover without operator action (F7). FSIF freezes its own writes and keeps serving; the CRL has no equivalent.

---

## 11. Suspected flaws and open questions

Each item carries a **Verification** paragraph from an independent adversarial pass (re-derived from the code; scratch
programs in `/tmp/claude-1000/-home-rakis-devel-aspen/ae495a5c-bf93-422a-9982-f278ed6d3ddf/scratchpad/{exp,iov,verify-crl}`).
Verdicts: CONFIRMED, PLAUSIBLE, REFUTED.

Confidence is my confidence that the described behaviour exists in the code as written. "Reproduced" means I ran
the real `SimpleCRL` from a scratch program (outside the repository) and observed the failure. Severity is stated
separately where it matters.

### F1. Large entries are silently truncated; acknowledged saves are lost (reproduced, high confidence, severe)

> **FIXED (2026-11-09).** `StreamWriter.writeFully` loops the gathering write until every buffer is written. Covered by
> the SimpleCRLSuite tests "StreamWriter writes every buffer of a write with more buffers than IOV_MAX" and "An entry
> holding more transactions than IOV_MAX buffers recovers, including the re-save at open".

**Verification (adversarial pass): CONFIRMED, severity data-loss.** Re-ran `iov/T.java` (1025 buffers wrote 10240 of 10250; 3000 wrote 10240 of 30000) and `Exp` E1 (1 of 501 recovered). `StreamWriter.scala:74` indeed discards the return value and `Stream.writeEntry` always hands over one array. Buffer count per entry is 1 header + (1 txd + 1 per update with data) per transaction + 1 static + at most 1 pad (`LogEntry.scala:84-121`), so about 340 two-update or about 510 one-update transactions overflow IOV_MAX. Triggers that need no slow disk: (a) the open-time re-save `SimpleCRL.scala:132-136` puts every recovered transaction into one entry (`initializing` suppresses `startWrite`), so a device recovering more than about 340-510 live transactions writes an unreadable entry on every start, and every entry written after it is lost with it (`findHighestEntry` stops at it); (b) a rotation that copy-forwards that many (`SimpleCRL.scala:178-182`); (c) batching behind a slow DSYNC (E1 forces this by blocking the writer in a completion handler, so (c) is the least likely). The F1 text above is otherwise accurate. Fix sketch: loop `while buffers.exists(_.hasRemaining)` (or coalesce into chunks of at most 1024 buffers and check the return value), and assert the byte count equals the entry size.

`Stream.writeEntry` builds one buffer per transaction description, one per object update, a header, a static region and
padding, and `StreamWriter` calls `channel.write(w.buffers)` with that array and **ignores the return value**
(`S/crl/simple/StreamWriter.scala:74`, `LogEntry.scala:59-121`). `FileChannel.write(ByteBuffer[])` writes at most
`IOV_MAX` = 1 024 buffers per call and returns the bytes written; it does not loop. Confirmed with a standalone Java
program on this host (JDK 25): 1 025 buffers wrote 10 240 of 10 250 bytes, 3 000 buffers wrote 10 240 of 30 000.

An entry with more than about 1 020 buffers (for example 340 new transactions with a description and two updates each
batched during a slow `DSYNC`, or a rotation that re-emits more than ~500 live transactions, or a recovery re-save of
the same) is written without its static region and trailing UUID. The write "completes", handlers run, votes and
promises are sent, and the entry is unreadable on recovery. Because `findHighestEntry` stops at the first unreadable
entry, **every later entry is lost too**.

Reproduction: `SimpleCRL` over a temp directory, first save gated, 500 further saves with two updates each queued
behind it, gate released, all 500 completions fired, CRL shut down and reopened: **1 transaction recovered of 501
acknowledged**. Scratch program: `/tmp/claude-1000/-home-rakis-devel-aspen/ae495a5c-bf93-422a-9982-f278ed6d3ddf/scratchpad/exp/Exp.scala` (E1).

Effect: forgotten promises and accepts (split-decision risk) and lost object data of commit votes, silently, exactly
under load. The tests use few transactions per entry and did not hit it.

### F2. After recovery spanning two streams, the active stream gets the wrong UUID; later acknowledged entries are lost (reproduced, high confidence, severe)

> **FIXED (2026-11-09).** `Recovery.recover` takes the active stream's UUID from the newest entry and the walk over
> older entries no longer changes it. This also fixes F20. Covered by the SimpleCRLSuite test "Saves acknowledged after
> a recovery that spanned two streams survive the next recoveries".

**Verification: CONFIRMED, severity data-loss, and it escalates (new experiment `verify-crl/Exp3.scala`).** Re-ran E2 (Z lost). `Recovery.scala:81` assigns the UUID for each visited entry and the last assignment is the oldest visited entry's, returned at `:106` and given to the active stream at `SimpleCRL.scala:126-128`. The open-time re-save (`:132-136`) is itself written with the wrong UUID when the walk crossed a stream, so even with no new traffic the first entry of every such run is unreadable. Escalation: after the restart, keep saving until the ring rotates into the next stream (fresh random UUID, readable), shut down and reopen. The walk from the newest entry follows its previous-entry link into the wrong-UUID entries, `readEntry` returns None, and recovery throws `BrokenEntryChain` (Exp3 output: `no entry can be read at StreamLocation(StreamId(1),8192,4096)`). `openDeviceCrl` then marks every store on the device rebuild-required (F6). So F2 is not only lost acknowledged saves: one ordinary restart in the state "oldest needed entry in an earlier stream" followed by a rotation and a second restart destroys the device. Fix sketch: return `readers(activeStreamId.number).currentUUID` as the active stream's UUID; add a regression test that restarts twice across a stream boundary.

`Recovery.recover` sets `currentStreamUUID = h.streamUUID` for every entry it visits while walking back
(`Recovery.scala:81`) and returns the **last** value (`Recovery.scala:106`), which is the UUID of the *oldest* visited
entry. `SimpleCRL` hands that value to the *active* stream as its UUID (`SimpleCRL.scala:126-128`). When the walk crosses
a stream boundary (the usual case once a transaction lives longer than one stream), the active stream writes new entries
with another stream's UUID. `StreamReader.readEntry` compares an entry's UUID to the UUID at the start of its own file
(`StreamReader.scala:15-19, 69`), so the next recovery stops at the first entry written after the previous recovery.

Reproduction: 3 streams of 12 KiB; transaction X saved, then Y saved three times so the log crosses into stream 1; shut
down and reopen (2 transactions recovered); save Z and wait for its completion; shut down and reopen: **2 transactions
recovered, Z (acknowledged) lost**. Scratch program `Exp.scala` (E2).

Effect: every restart that begins with a long-lived transaction spread over two streams loses every record
acknowledged after that restart until the next rotation. Forgotten Paxos promises/accepts and vote records.

### F3. Rotation overwrites, in place, the stream that holds the only copy of live data (high confidence in the mechanism; not executed)

**Verification: PLAUSIBLE (mechanism confirmed by reading, not executed), severity data-loss.** `Stream.recycleStream` (`Stream.scala:14-17`) resets the offset to 0 and picks a new UUID; `writeCurrentLogEntry` (`SimpleCRL.scala:170-191`) then writes the copy-forward entry over the same file in one `write`. The last durable entry's `oldestEntryNeeded` is the only thing that tells recovery which older entries matter, and it still points into the stream being overwritten until the new entry is durable. A crash that tears this write (power loss or kernel crash; a plain process kill does not tear a `write` syscall) can leave old sector 0 replaced (new file UUID, so every old entry in the file is unreadable: `BrokenEntryChain`) or old header with foreign tails. No test covers a torn rotation. Trigger needs power loss inside a single-entry write window, repeated on every rotation, so rare but not negligible on a fleet. Fix sketch: write copy-forward into a stream that holds no needed data (reserve one spare stream, or require `oldestEntryNeeded` to have advanced past the stream first), and only then recycle.

On a rotation the live transactions with data in stream *k+1* are re-emitted into the entry that is then written at
offset 0 of the same file (`SimpleCRL.scala:171-189`, `Stream.scala:14-21,24-40`). The old copies and the static
records in that stream are still needed by the chain until the new entry is durable (the last durable entry's
`oldestEntryNeeded` points into that stream). A crash or I/O error during this write, or a partial persistence of its
sectors, can:

- change the file's first-16-byte UUID (header sector persisted) and so make every old entry in the file unreadable:
  the walk throws `BrokenEntryChain` and the whole device's stores become `rebuild-required` (F6); or
- leave the old entry 0 header intact while overwriting later sectors: old record locations then point at foreign bytes
  and `readStreamLocation` returns them without validation: wrong transaction descriptions and object data presented
  as recovered state; or
- (with F1) leave an acknowledged but truncated copy and no old copy.

The window is one multi-megabyte write per rotation, repeated on every restart that rotates (5.6) and in crash loops.
A safe design needs the copy to be durable before the old stream is touched, e.g. a spare stream in the ring. Note
that sibling rotation tests (`SimpleCRLSuite` "Recycle Streams", "Recovery after stream recycling...") only exercise a
clean shutdown.

### F4. A save after a delete in the same unwritten entry is acknowledged but never written, and leaves a phantom live transaction (reproduced, high confidence for the phantom; medium for real-world reachability)

**Verification: mechanism CONFIRMED (E3, E4 re-run), real-world trigger PLAUSIBLE-low, severity liveness/data-loss when it fires.** In the host's normal flow the phantom is healed: an aborted transaction deletes its CRL record on resolution (`Tx.scala:627`), a late accept/prepare can re-save it (`Tx.scala:751-762`), but `TxFinalized` deletes again (`Tx.scala:790-799` comment: "a prepare after an abort ... may have saved this Tx again"; `Frontend.transactionFinalized` `:537-540`), and `SimpleCRL.onDeleteTransaction` (`:291-296`) removes whatever is in `transactions`, phantom included. After the final delete the Tx is removed from the Frontend (`Frontend.scala:619-623`) and prepares for it are ignored via the status cache (`:639`), so no permanent phantom path was found in the Frontend. What remains: the phantom lives until the finalization delete (briefly holds `oldestEntryNeeded`, shows up in `readStoreState`), and the load/transfer paths (`loadStore`, `crl_save.log` re-save) where no later delete is guaranteed. The "ack without durability" aspect is harmless on the abort path. Fix sketch: in `onSaveTransaction`, if `currentLogEntry` already deletes the id, do not create or queue a `Tx` (just add the completion handler).

`LogEntry.addTx` skips a transaction whose id is in `txDelSet` but still records its handler (`LogEntry.scala:40-53`).
`onSaveTransaction` has already created a `Tx`, put it at the queue head and registered it in `transactions`
(`SimpleCRL.scala:270-279`). Consequences:

1. The save reported success without being durable. In the transfer-out/load paths the caller may delete its only other
   copy next (`crlLoaded` removes `crl_save.log` once `loadStore`'s saves report success, `StoreRouter.scala:257-263`;
   `loadStore` is `CrashRecoveryLog.scala:127-139`). A delete issued for the same ids just before the load, landing in
   the same unwritten entry, loses the state permanently. Needs an unlikely interleaving (delete then reload within
   one entry), so low likelihood.
2. The phantom `Tx` has no data locations, so `closeStream` never moves it, but it sits in the queue with the old
   serial and holds `oldestEntryNeeded` back forever. After the ring wraps, the newest entries claim an oldest needed
   entry that has been overwritten, and recovery throws `BrokenEntryChain`: the CRL is declared lost and every store on
   the device is rebuilt. Reproduced: save, delete, late save in one entry, then 14 further saves through all three
   streams; reopen: `BrokenEntryChain: no entry can be read at StreamLocation(StreamId(2),8192,4096)`. Scratch program
   `Exp2.scala` (E3, E4).
3. `getFullRecoveryState`/`readStoreState` return the phantom: a transfer-out would write a long-finalized
   transaction's state into `crl_save.log`, and the destination would resurrect it (E3 shows the CRL reporting the
   phantom).

The existing test "Save after a delete in the same unwritten entry is skipped but reports success"
(`SimpleCRLSuite.scala:1039`) asserts only the recovered list, not the live in-memory state. The comment in the code
calls this "a timing artifact", so it is expected to occur.

### F5. No checksum anywhere; sector reordering is undetected, and garbage becomes state (medium confidence, severe if reachable)

**Verification: CONFIRMED as a design gap (no CRC/checksum anywhere under `server/crl`; grep empty), severity data-loss only if the device reorders or corrupts sectors.** Not triggerable by software alone. Fix sketch: a CRC32C per entry header and per entry body, checked in `readEntry` and `readStreamLocation`.

The only integrity checks are two UUIDs and a 3-valued disposition byte (`StreamReader.scala:47-74`,
`LogContent.scala:60-64`). Promised/accepted proposal numbers, locations, counts and all payload bytes are taken as
read. A torn write with reordered sectors (allowed for a multi-sector `write` on most devices), an old-cycle sector
surviving inside a new entry, or media bit rot returns a transaction with an arbitrary promise or accept, an arbitrary
txd, or object data of another transaction. It can lower a promise (allowing a conflicting accept), or apply foreign
bytes to an object. Whether it requires power loss or a device misbehaving is the same A1 question as 10.3, but a
checksum would convert silent corruption into C8 (a loud failure).

### F6. Any exception from `openCRL` irreversibly turns every store on the device into a rebuild (high confidence the behaviour exists; severity systemic)

**Verification: CONFIRMED, severity systemic (data-loss class).** `DeviceDiscovery.scala:498-520`: the `catch case t: Throwable` at `:510-515` covers `SizingMismatch`, `BrokenEntryChain`, OOM and I/O errors alike, then `markRebuildRequired` on every store dir and the `os.move` to `crl.lost-<uuid>` (nothing reads that directory: grep finds only the move and tests). F2 (verified above) and F4 give in-product ways to reach it. Note also: when the first `openCRL` fails after `StreamWriter` was constructed (for example the `require` in the open-time re-save), the writer thread and file handles are not shut down before the second `openCRL`, so two writers can briefly exist over the same files. Fix sketch: catch only `BrokenEntryChain`/`CorruptedEntry`; rethrow `SizingMismatch`, `Error` and I/O errors so the device stays unloaded and alarms; shut down a half-built CRL.

`openDeviceCrl` catches `Throwable` (including `OutOfMemoryError`, `InterruptedException`), marks every store durable
`rebuild-required`, and moves `crl/` aside (`DeviceDiscovery.scala:507-520`). Causes that are not "the files are
damaged": a hand-edit of `crl-num-streams`/`crl-max-file-size-mb` (`SizingMismatch`), an out-of-memory during
recovery (the recovered set, with object data, is held on the heap), `EMFILE`, a transient I/O error, a read-only
remount, and every software bug in this section that makes recovery throw (F3, F4, F5). One mistaken change of version
or configuration across a fleet rebuilds every device at once. This is the correlated loss that
[systemic-loss-protection.md](systemic-loss-protection.md) says is unsolved. Concrete CRL-side mitigations: distinguish
`SizingMismatch` and I/O errors (do not mark), never mark on `Error`, retry before declaring loss, and do not flip
more than one device at a time.

### F7. Sparse stream files: a full device kills the CRL, and the CRL is needed to free space (medium confidence)

**Verification: PLAUSIBLE, severity liveness.** Confirmed by reading `StreamWriter.scala:40-47`: files are extended by a one-byte write at the end, giving sparse files; ENOSPC on a first write to a hole kills the writer thread (F9 then applies). Not executed. Fix sketch: `fallocate`-style preallocation (write zeros or `FileChannel.truncate` plus a real allocation) at creation.

`StreamWriter` extends the files by writing the last byte (`StreamWriter.scala:43-47`) and never reserves blocks. The
first write to each unallocated region (and every overwrite on a copy-on-write file system) can fail with `ENOSPC`.
The writer thread dies (`StreamWriter.scala:55-65`) and the device cannot process any transaction; deleting data,
the usual remedy, is itself a transaction. No `fallocate`, no free-space check at open.

### F8. Admission undercounts live CRL bytes, so C11 can fail and kill the CRL (medium confidence)

**Verification: PLAUSIBLE, severity liveness.** `new CrlAdmission(...)` starts at zero (`DeviceDiscovery.scala:566-567`); recovered records of stores that do not load are not admitted, yet the open-time re-save and every rotation copy them. If live bytes exceed the stream, `require(canWriteEntry)` (`Stream.scala:28`) throws on the CRL thread and the CRL dies (F9). Trigger needs a large live set held for non-loading stores. Fix sketch: at open, charge admission for every recovered record whatever its store, release on drop/transfer.

`CrlAdmission` starts at zero when a device loads (`DeviceDiscovery.scala:566-567`). The CRL then already holds the
recovered records of every store, but only the records of *stores that load* are counted (`admitRecovered`,
`Frontend.scala:458`). Records of stores that never load on this run (Offline, `rebuild-required`, awaiting drop,
`transferring-out`, a Shutdown-to-restart gap) are live, copied forward at every rotation, and uncounted;
`DeviceReconciler.dropLeftoverCrlState`'s own comment names the same problem for stores without a directory
(`DeviceReconciler.scala:66-72`). Late object data, recovered transactions and the per-update duplication are charged
without a check. If live bytes exceed the stream during a rotation, `require(canWriteEntry)` throws
(`Stream.scala:28`) and the device stops. Recovery on restart can reproduce it (the re-save in 5.6 emits the live set
once more).

### F9. Thread death is silent for the stores, and several callers hang (medium-high confidence)

**Verification: CONFIRMED, severity liveness.** `Main.scala:1702` only logs. After death nothing drains `ioQueue` (`SimpleCRL.scala:145-155`), so `save` completions never fire (stores never vote) and `readStoreState`/`dropStore`/`deleteTransactions`/`storesWithEntries` futures never complete. Nothing in the host detects the dead CRL or takes the device out of service. Fix sketch: on thread death mark the device failed (unload stores, stop answering as acceptors, raise an operator alarm) rather than leaving zombie stores.

`onThreadDeath` only logs (`Main.scala:1700-1707`). After a death the stores stay installed and keep serving reads while
they can no longer vote; peers will mark them as missed updates and repair later. `readStoreState`, `dropStore`,
`deleteTransactions`, `storesWithEntries` and `loadStore` complete through the CRL thread and therefore never
complete: a store load (`awaitCrlLoad`), a transfer extraction and a rebuild drop all wait forever with no timeout.
CLAUDE.md's "reads keep working" is true only for object reads. The writer thread can also be killed by an exception
from any completion handler (10.16). Whether a stale-reading zombie store is acceptable to clients is a question for
the read path, not answered here.

### F10. No exclusion of a second opener of the same CRL files (medium confidence; high severity if it happens)

**Verification: CONFIRMED (no `FileLock`/`tryLock` anywhere in `src/main`), severity data-loss only with a double mount, which the adoption design declines to fence.** Fix sketch: take an exclusive `FileChannel.tryLock` on `crl/lock` while open.

Nothing takes a `FileLock` on the stream files (searched: no `FileLock`/`tryLock` in the tree). `closingCrls` guards
only an in-process reopen. A device mounted twice (a duplicate mount, a mistaken second host process, a SAN device
adopted while the old host is still writing) yields two CRLs writing the same files with independent serial numbers
and offsets: interleaved or overwritten entries, lost acknowledged state. The unclean-adoption design explicitly
declines fencing; this is the record of what that decision depends on.

### F11. Stream file creation and the `crl/` directory entries are not made durable (low confidence of harm)

**Verification: PLAUSIBLE-low, severity liveness (spurious F6).** Not executed; on journaling filesystems the DSYNC write of the size-extension byte normally persists the new directory entry, but POSIX does not require it. Fix sketch: `DurableFiles.syncDirectory(crlDir)` after creating the files.

New stream files are created and extended with a `DSYNC` write, but their directory is never fsynced
(`StreamWriter.scala:38-47`, `SimpleCRL.scala:48-50`). On the common journaling file systems the file's own sync
carries the directory entry, but POSIX does not promise it. After a crash right after the first saves, a stream
file could be missing, and `checkSizing` would throw (`SimpleCRL.scala:88`), producing F6 on a device that now holds
stores.

### F12. `dropStore` can delete a same-txid re-save made after it read the list (low confidence; narrow)

**Verification: PLAUSIBLE-low.** `CrashRecoveryLog.dropStore` (`CrashRecoveryLog.scala:159-162`) reads then queues the delete from a parasitic callback on the CRL thread, so any `SaveTransaction` already queued for the same txid lands between them and is deleted. Needs a store of that id loaded and saving during a drop. Fix sketch: a CRL-side `DropStore` request that deletes everything of the store present when it is processed on the CRL thread (atomic with the read).

The default `dropStore` is `readStoreState` followed by `deleteTransactions(ids)` (`CrashRecoveryLog.scala:159-162`).
Only entries *present at the read* are named, but a transaction re-saved with the same id between the read and the
delete is removed by it. Reachable only if a store of that id is loaded again during a drop (transferred back).
`dropLeftoverCrlState` and `discardLostStore` guard with `crlDropsRunning`/`droppingCrlState` and directory checks;
no equivalent guard in the transfer-in path was verified.

### F13. `dropTransactionObjectData` is memory-only; the durable record can point into recycled space (low confidence)

**Verification: CONFIRMED mechanism; trigger needs a narrow window; severity data-loss (value regression). Detailed answer to the priority question.** `dropTransactionObjectData` (`SimpleCRL.scala:298-299`) is memory-only; the CRL record of a committed transaction is deleted only at finalization (`Frontend.transactionFinalized` `:537-540`, after outstanding commits). A crash between the backend commit persisting and that delete recovers the record with disposition VoteCommit, its object data and Paxos state (`Recovery.scala:90-102`). A recovered commit vote keeps its vote and is not re-checked (`Tx.scala:394-401`); when the outcome arrives as committed (TxResolved/TxFinalized from peers, or `tracelessCommitFound`), `doCommit` calls `RequirementsApplyer.apply`, whose only protection is `RequirementsChecker.check(applying = true)`. That check rejects a replay when the transaction carries a revision requirement (DataUpdate, VersionBump, KV update with `requiredRevision`: object revision has moved on, and the `ObjectTimestampError` check also fires), a `KeyRevision`/`DoesNotExist` key requirement, or a `RefcountUpdate` whose required refcount no longer matches. It does NOT reject: (1) a blind KV Insert (no required revision, no key requirement, or only `MayExist`/`Exists`): `RequirementsApplyer.scala:107-110` overwrites the key with the older value, regressing a newer committed blind write T2 that persisted before the crash; (2) a KV `Delete` guarded only by `Exists`, after the key was re-inserted by T2; (3) a `RefcountUpdate` whose required refcount equals the current one again (ABA: T1 takes 1 to 2, T2 takes 2 back to 1, T1's replay matches and sets 2). Nothing else stops it: the host status cache is in memory and empty after a crash; the horizon (`Tx.tooOld`) applies only to transactions the store does not know, not to recovered ones; `alreadyApplied` (`Tx.scala:526-533`) only affects logging/`TxCommitted` errors, and for a blind Insert it is false anyway once T2 replaced the key's revision. The same replay applies to the traceless-commit lookup path. Window: T1 backend-committed on this store, T2 (blind on the same key/object) committed and persisted, crash before T1's finalization delete (T1's finalization normally lags by at least `errorTimeout`, 2 s). TKVL nodes mostly use revision/key requirements, so exposure is limited to callers using `tx.update(..., requiredRevision = None, ...)` with blind inserts or Exists-guarded deletes. Fix sketch: make `dropTransactionObjectData` durable (write a tombstone record so a recovered commit vote with dropped data is not re-applied), or have `doCommit` skip a recovered transaction whose objects show a revision/timestamp newer than the transaction's start timestamp.

After `dropTransactionObjectData`, the last durable record of the transaction still lists its update locations. If
those lie in a stream that is recycled before the transaction is saved or moved again (possible when the update data
was written in a later entry than the txd, i.e. after a data-less first prepare), a crash recovers the transaction with
foreign bytes as object data, undetected (F5). Such a transaction has already applied its commit (data is dropped only
after the backend commit, `Tx.scala:608-610`), so a re-application is normally excluded by the revision check
(`alreadyApplied`); whether every update form is idempotent in this way is not established here
([transaction-recovery.md](transaction-recovery.md)).

### F14. `readStoreState` and disk recovery return different statuses (low confidence of harm)

**Verification: CONFIRMED (code), severity doc-only.** `Recovery.scala:100` sets Unresolved; `SimpleCRL.scala:265` returns the live state. Fix sketch: pick one and document it.

Disk recovery sets `TransactionStatus.Unresolved` (`Recovery.scala:100`); `readStoreState` returns `tx.state` as last
saved (`SimpleCRL.scala:265`). A record saved after resolution (`Tx.txStatus` is saved as it stands) keeps its
Committed/Aborted status in a transfer's `crl_save.log` but loses it after a crash. Consumers
([transaction-recovery.md](transaction-recovery.md)) must treat both as equivalent or this is an inconsistency.

### F15. Admission has no exemption, so cross-device back-pressure can stall finalization (low-medium confidence; liveness only)

**Verification: PLAUSIBLE (design-level), severity liveness.** Admission rule re-read (`CrlAdmission`, `Frontend.admitPrepare`); no exemption exists. Not demonstrated. Fix sketch: exempt finalization-action transactions or reserve a share of the mark for them.

The record of a committed transaction stays live until its finalization actions (further transactions) complete. Those
transactions are new to every store that must vote on them and are dropped when the store's device CRL is above the
mark. If every device in the relevant pools is above the mark because of records waiting for finalization, nothing
frees space. The design explicitly decided "No transaction is exempt" (2026-11-07 spec, section 4). The liveness
argument is not in the spec; it deserves one.

### F16. Every open rewrites the full live set and every crash loop rotates the ring (medium confidence; amplifies F3 and F8)

**Verification: CONFIRMED (code `SimpleCRL.scala:132-137`), severity amplifier.** It makes F1(a) and F2 apply to every start, not only to rotations.

`SimpleCRL.scala:132-137`. Size is up to half a stream and the open blocks the host control thread (Backlog.txt).
Combined with F3 each restart is a rotation-class risk.

### F17. Durable backend assumption (A4) for backends other than RocksDB/FSIF (not verified)

**Verification: REFUTED for RocksDB and FSIF, doc-only for MapBackend.** RocksDB writes with `setSync(true)` (`BufferedConsistentRocksDB.scala:110-112`); FSIF's commit completes when the journal batch is forced (`FsifBackend.scala` header, durable-before-map). `MapBackend` is memory only, so it loses its data on a crash regardless of the CRL.

The CRL delete of a committed transaction is gated by backend commit completion (`Frontend.scala:209-210`). The
in-memory `MapBackend` and any future backend must make "complete" mean durable, or a power loss destroys both the data
and its CRL record. Verified only for `BufferedConsistentRocksDB.scala:111`; FSIF's journal force is described in
CLAUDE.md and not re-verified here.

### F18. A rebuilt store has no memory of the Paxos state it lost (open question; low confidence)

**Verification: OPEN, not a CRL code defect; PLAUSIBLE only under correlated loss.** Safe in practice by timing: a rebuild takes long enough that every transaction in flight at the loss has been finalized at the other stores, and old prepares vote `TransactionTooOld`. Belongs to the Paxos documents.

After a lost CRL the store's directory is discarded and the store rebuilt under the same `StoreId`. A transaction in
progress at the time had promises and accepts from this acceptor. The rebuild process does not process transactions
while rebuilding (Todo.txt) and the horizon makes old prepares vote abort
(`TransactionTooOld`, [transaction-recovery.md](transaction-recovery.md)), but a transaction decided by an accept
quorum that included the lost store, whose other accepted members then fail, can be decided differently by a later
proposer that reaches the amnesiac store. Whether the rebuilt store is blocked from Paxos participation for any
transaction older than its rebuild, and whether the quorum structure makes the case impossible, is for
[rebuild-process.md](rebuild-process.md) and [paxos-transactions.md](paxos-transactions.md) to settle.

### F19. Zero-filled regions parse as valid empty entries (low confidence; derived by reading, not executed)

**Verification: CONFIRMED by derivation (header size 70; an all-zero file parses as 4096-byte empty entries), severity doc-only.** Harmless today because ties in `highestHead` keep stream 0 and serial numbers start at 0.

A never-written stream file has UUID (0,0) at offset 0 and `readEntry` accepts an all-zero header (header UUID 0 equals
the file's current UUID 0; the tail at byte 50 is also 0), as an empty entry of 4 096 bytes, repeating to the end of
the file. A reopened but never-written CRL therefore picks stream 0 as active with its write offset at the end of the
file and rotates on the first write, and a zero-filled stream looks "valid" to the previous-entry link of a later entry
(`Recovery.scala:38-44, 65-83`). Probably harmless today; it makes the "head entry with the highest serial" rule
depend on serial numbers starting at 0.


### F20 (new). F2 turns a routine restart into a device-wide rebuild (see F2; Exp3)

> **FIXED (2026-11-09)** with F2.

Not a separate bug but a separate consequence worth tracking: with F2 a second restart after a rotation throws
`BrokenEntryChain`, F6 converts that into `rebuild-required` on every store of the device, and the reconcile then
discards them (systemic-loss-protection.md D2/X3). Reproduced with `verify-crl/Exp3.scala`.

### F21 (new). `openDeviceCrl` retry after a failed first open does not stop the first attempt's threads

If `SimpleCRL`'s constructor fails after `StreamWriter` was built (for example `Stream.writeEntry`'s `require` during
the open-time re-save, `SimpleCRL.scala:132-136`), the writer thread and channels are left running while
`openDeviceCrl` moves `crl/` aside and opens a new CRL (`DeviceDiscovery.scala:510-520`). Confidence: medium (read,
not executed). Fix sketch: close the writer in a constructor failure path.

### 11.1 Corrections to CLAUDE.md

| CLAUDE.md claim | Code |
|-----------------|------|
| "a dead CRL thread stalls that device ... reads keep working" (`S/crl/CLAUDE.md`) | `readStoreState`, `dropStore`, `deleteTransactions` and `storesWithEntries` all need the CRL thread; they hang. Object reads of the stores continue. |
| "The sizing is fixed ... the CRL is treated as lost" | True, and it happens for any configuration edit, not only a disk fault (F6). |
| Stream recycling "the oldest live data is copied to the head of the buffer whenever it is about to overwrite that live data" | The copy is written *over* the stream being recycled (F3). |
| "Only read at application startup" | Also read by `readStoreState` for every store load, transfer and drop. |
| Admission "so a recycled stream's live copy-forward always fits" | Only for the bytes admission counts (F8). |

### 11.2 Reproducing the experiments

The scratch programs are outside the repository and were compiled against `target/aspen-classpath` (`dotty.tools.dotc.Main`
from the cached 3.7.3 compiler) and run with `java -cp exp/out:$(cat target/aspen-classpath) Exp` and `Exp2`. The
Java program for the `IOV_MAX` behaviour is `.../scratchpad/iov/T.java`. Outputs observed:

```
E2 after second restart: 2 txs (expected 3 incl. acked Z)
E1 recovered 1 txs of 501 acked
E3 getFullRecoveryState holds 2 txs (U, and deleted P?)
E4 recovery threw: ...BrokenEntryChain: CRL entry chain is broken: no entry can be read at StreamLocation(StreamId(2),8192,4096)
1025 buffers: wrote 10240 of 10250
```
