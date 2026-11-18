# Crash Recovery Log (CRL): interface contract, SimpleCRL, and failure analysis

Derived from the code. Path shorthand: `S` = `src/main/scala/org/aspen_ddp/aspen/server`; `S/crl` is the CRL package.
Code is cited as file plus symbol.

Sibling documents (not duplicated here): [paxos-transactions.md](paxos-transactions.md),
[transaction-requirements.md](transaction-requirements.md), [transaction-recovery.md](transaction-recovery.md),
[repair-process.md](repair-process.md), [rebuild-process.md](rebuild-process.md),
[systemic-loss-protection.md](systemic-loss-protection.md), [cross-document-review.md](cross-document-review.md).

---

## 1. Purpose and fault model

A store is a Paxos acceptor for every transaction that touches one of its objects, and a participant that must be able
to apply a committed update it voted for. Both facts live in memory. The CRL is the only place they are made durable
before the store speaks. If a store forgets a promise or an accepted value it can later promise or accept something
that contradicts what the other acceptors believe (a split decision). If it forgets the object data of a commit vote,
a decided commit can no longer be applied by that store.

The CRL has one job: **make a store's per-transaction Paxos state, vote, transaction description and object data
durable before any message that depends on them is sent, and give them back after a crash.** It also holds applied
stubs (section 3.6), a second, much smaller kind of record.

### 1.1 Assumptions the guarantees rest on

| Id | Assumption | Where it matters |
|----|------------|------------------|
| A1 | A `write` to a file opened with `DSYNC` that returns normally is on stable storage, including after power loss. Disk write caches honour flush/FUA or are disabled. | All of section 5.6 |
| A2 | A failed or never-returning `write` means the data is not relied on. Writes are never retried; the CRL dies instead (5.10). | `S/crl/simple/StreamWriter` |
| A3 | Only one CRL instance writes a device's `crl/` files. Enforced by `CrlLock` for every opener on the same file system (5.1); not enforced for two hosts mounting one shared device. | adoption, double mounts |
| A4 | Backend commit completion means the object update is durable in the backend (RocksDB writes with `setSync(true)`, FSIF completes once its journal batch is forced). The CRL record of a committed transaction is deleted only after that. `MapBackend` gives no durability and is for tests only. | section 3.4 |
| A5 | The CRL and the stores it serves are on the same device and move together. | sections 7-9 |
| A6 | A store's `save`/`delete*` calls come from its own executor; the CRL thread serialises everything. | section 5.3 |
| A7 | The file system overwrites in place. On copy-on-write file systems (btrfs, ZFS) every CRL write allocates and can fail with `ENOSPC`; the open warns once. | 5.1, 10.17 |

---

## 2. The CRL interface and its contract

Interface: the `CrashRecoveryLog` trait (`S/crl/CrashRecoveryLog.scala`), factory `CrashRecoveryLogFactory.openCRL`
(`S/crl/CrashRecoveryLogFactory.scala`), records `TransactionRecoveryState` and `StoreRecoveryState`.

`TransactionRecoveryState(storeId, serializedTxd, objectUpdates, disposition, status, paxosAcceptorState)` is the record
for one (store, transaction). `paxosAcceptorState` is `PersistentState(promised: Option[ProposalId],
accepted: Option[(ProposalId, Boolean)])`. `StoreRecoveryState(transactions, appliedStubs)` is everything held for one
store; an applied stub is a `TransactionId` with its transaction's start `HLCTimestamp`.

| Method | Meaning |
|--------|---------|
| `save(txid, state, onDurable)` | Make `state` durable; `onDurable` runs once it is. Saves for one (store, txid) replace one another; the newest wins. |
| `deleteTransaction(store, txid)` | Lazy, unacknowledged deletion of the transaction state. A crash before it is written leaves the transaction recoverable. An applied stub of the id stays. |
| `deleteTransactions(store, txids): Future` | Deletes transaction state **and** applied stubs of each id, in one entry; completes once that entry is durable. An empty list completes at once. |
| `dropTransactionObjectData(store, txid)` | Memory only: the next rewrite of the record (a save, a copy-forward) leaves the object data out. Nothing is written. |
| `whenDurable(store, handler)` | Runs `handler` once every request queued before it is durable. Orders a write outside the CRL (a backend deletion) after a deletion, which has no completion of its own. |
| `readStoreState(store): Future[StoreRecoveryState]` | The store's live transactions and stubs, answered in queue order with every earlier request. Leaves them live. Returns the in-memory state, so a transaction whose data was dropped is still returned with its data. |
| `getFullRecoveryState` / `getAppliedStubs` | Blocking variants of `readStoreState` (SimpleCRL awaits it). Only the undeferred, test-only Frontend path blocks. |
| `loadStore(store, state): Future` | Saves a `crl_save.log`'s state: every stub first, then (once those are durable) every transaction. SimpleCRL tracks every save so a CRL death fails the Future. |
| `saveAppliedStub(store, txid, start, onDurable)` | Saves a stub, replacing any transaction state of the id in the same entry. Never subject to admission checks. |
| `deleteAppliedStub(store, txid)` | Lazy, unacknowledged deletion of the stub only. Writes nothing if no stub is held. |
| `dropStore(store): Future` | Deletes every transaction and stub of the store held when the request is processed, durably. In SimpleCRL its transaction deletions are cancellable by a later save of the same id in the same entry (5.8). |
| `storesWithEntries(): Future[Set[StoreId]]` | Stores with live entries, stub-only stores included. |
| `recoveredBytesByStore` | What the open recovered for each store, as admission counts it (section 6). |
| `failed: Future[Throwable]` | Completes, with the cause, if the CRL dies (5.10). |
| `shutdown()` | Blocks until the CRL's threads have stopped and its files are closed. Idempotent. |
| `admissionLimitBytes` | The device's admission limit (section 6): half a stream in SimpleCRL. |
| `openCRL(devicePath, config)` | Recover and open. Throws `CrlLocked`, `CrlSizingMismatch`, `CrlCorrupt`, `BrokenEntryChain`, or whatever reading the files threw. |

The trait's defaults (read through `getFullRecoveryState`, `dropStore` as read-then-delete, `whenDurable` at once,
no failure) are for CRLs that keep nothing on disk, such as test CRLs. `loadStore`'s default runs its callbacks on the
global pool; SimpleCRL overrides it.

### 2.1 Numbered contract

**Durability and ordering**

- **C1 (save before send).** `onDurable` of `save(t, s, h)` runs only after the entry holding `h` has been written
  with `DSYNC` and the write returned. A caller sends nothing that depends on `s` before `onDurable`. Exception, by
  design: a save of an id that the same unwritten entry already deletes finally is skipped and still acknowledged
  (C9). A deletion is the store's decision to forget the transaction, which already makes every earlier state of it
  forgettable.
- **C2 (entry order).** Entries are written one at a time, in serial order. Entry N+1 is not issued until entry N's
  write returned and every handler of N ran (`writeInProgress`; the writer queues `WriteComplete` only after the
  handlers). So when a handler of entry N runs, every earlier entry is durable. Handlers run on the **writer thread**,
  newest first within an entry (`LogEntry.completionHandlers` is prepended and run in list order). Callers that touch
  store state post to the store's executor (`Frontend.post`); `Tx`'s prepare and accept handlers only send messages.
- **C3 (single run).** Each handler runs at most once, and only if its entry's write returned normally. A CRL that
  dies runs no handler of an entry not yet written.
- **C4 (newest wins).** After recovery, each live record has the state of its newest durable record
  (`LogEntry.loadStaticEntryContent`, applied newest entry first).
- **C5 (live).** A record is live from its first durable save until a durable deletion of the same (store, txid) in
  the same or a newer entry, or a `dropStore` or `deleteTransactions` covering it.

**Atomicity and corruption**

- **C6 (entry atomicity).** An entry is taken whole or not at all. Its header carries a CRC32C of the header
  (`header_crc`) and one of the body through the trailing UUID (`body_crc`), and the stream UUID appears in the header
  and as the last 16 bytes of the static region (`LogEntry.createEntryBuffers`, `StreamReader.readHeader`,
  `StreamReader.bodyIntact`).
- **C7 (only the in-flight write may be torn).** Recovery drops the newest entry if its body fails its checksum: that
  can only be the write in flight at a crash, which no handler acknowledged. Any other failed checksum, and any
  intact entry newer than the newest one found, throws `CrlCorrupt` (5.7).
- **C8 (broken chain is fatal).** If an entry needed for recovery cannot be read, or data a live record names lies
  outside the dynamic data of every verified entry, `openCRL` throws (`Recovery.BrokenEntryChain`, `CrlCorrupt`). It
  never returns a partial state.

**Deletion**

- **C9 (deletion finality in an entry).** A deletion of (store, txid) applies to that txid's saves in the same entry
  and in all older ones. A save of the same txid queued after a final deletion into the same unwritten entry is
  skipped and acknowledged once the deletion is durable, and creates no in-memory record (`LogEntry.deletesFinally`,
  `SimpleCRL.onSaveTransaction`). Deletions made by a stub's save or by `dropStore` are cancellable: a later save of
  the id in the same entry removes the deletion and is written (`LogEntry.deleteTxUnlessResaved`, `LogEntry.addTx`).
- **C10 (deletion durability).** `deleteTransactions` and `dropStore` complete only after the entry carrying the
  deletion is durable. `deleteTransaction` and `deleteAppliedStub` have no completion; `whenDurable` orders later
  work after them.

**Capacity**

- **C11 (live bound).** The live data of a CRL must stay below one stream minus the entry pending at a rotation, so a
  rotation's copy-forward always fits. Admission keeps live bytes near half a stream (section 6). `Stream.writeEntry`'s
  `require` is the fail-stop backstop: a write that does not fit kills the CRL (5.10). Admission does not bound a
  single transaction (see CRL-W2).
- **C12 (sizing is fixed).** Stream count and size are fixed when a CRL is created and recorded in `crl/sizing`; an
  existing CRL always opens with them, whatever the device config says.

**Ownership**

- **C13 (one CRL per device).** Every store on a device saves to, recovers from and deletes from the CRL in
  `<device>/crl` (`CrashRecoveryLog.deviceCrlDir`).
- **C14 (queue order).** Every request is processed in the order it was queued. `readStoreState` sees every save queued
  before it, and `dropStore` drops exactly what was saved before it.
- **C15 (recovered status).** Disk recovery and `readStoreState` return the same status: the one last saved. It is
  stored in bits 3-4 of the static record's Paxos mask byte (`Tx.writeStaticEntry`, `Tx.loadTx`), and a recovered Tx
  restores a saved resolution from it (`Tx.oresolution`, transaction-recovery.md).

---

## 3. How the CRL fits into the transaction workflow

### 3.1 What is saved, and what each completion gates

All `crl.save` calls go through `Tx.saveState` (`S/transaction/Tx.scala`). The state saved is always
`TransactionRecoveryState(storeId, serializedTxd, trsObjectUpdates, disposition, txStatus, acceptor.persistentState)`,
where `trsObjectUpdates` leaves out the data once the commits have persisted (`saveObjectUpdates`) and any update
removed by `persistWithoutUpdateOf`.

| Event at the store | Save site | What `onDurable` does | What it protects |
|--------------------|-----------|-----------------------|------------------|
| Prepare answered, objects loaded | `Tx.answerPrepareAsAcceptor` | sends `TxPrepareResponse` (Promise or Nack, vote, collisions) and the vote to the client | promise, vote, txd, object data |
| Prepare answered before the objects loaded | `Tx.sendDelayedPrepare` | sends the delayed `TxPrepareResponse` | as above; the disposition is decided now |
| Accept | `Tx.receiveAccept` | sends `TxAcceptResponse` | accepted proposal and value |
| Object data adopted from a later prepare | `Tx.saveAdoptedData` | posts: releases a commit held for the data (`adoptedDataUnsaved`) | the data a commit will apply |
| Object this transaction allocated is being deleted | `Tx.persistWithoutUpdateOf` | posts: lets the backend deletion proceed | a restart must not allocate it again (transaction-recovery.md) |
| Backend commits of the transaction complete | `Tx.allObjectCommitsComplete` | `dropTransactionObjectData`, nothing written | stop carrying object data |
| Finalized (or aborted, expired, forgotten) | `Frontend.concludeTransactionState`, `Frontend.deleteTransactionState` | `deleteTransaction` | end of the record |
| Finalized, and an UnackedCommits entry may name the store | `Frontend.recordAppliedStub` | `saveAppliedStub`; posts: releases the transaction's admission | the record is replaced by an applied stub (3.6) |

Consequences:

- **The vote and the promise travel together.** The first prepare response carries both, and both are in the state
  saved before it is sent. This is what makes the 1-RTT commit safe across a crash ([paxos-transactions.md](paxos-transactions.md)).
- **Nacks are saved too**, through the same path.
- **No decision record.** No save is made because a transaction resolved, but every save writes the Tx's current
  status, so a prepare or accept handled after the resolution persists `Committed` or `Aborted`, and recovery restores
  it: the commit is applied, or the abort's state deleted, once the objects load. Any other recovered transaction is
  `Unresolved` and is resolved again ([transaction-recovery.md](transaction-recovery.md)).
- **No vote is sent for a transaction the CRL has not made durable.** A dead CRL therefore stops every vote of every
  store on the device; the host then unloads the device (5.10).

### 3.2 What data the record must contain

A commit vote requires a client prepare (prefer-abort, `Tx.awaitingClientPrepare`) and the object data it will apply
(`MissingObjectUpdate`), so every commit vote that reaches `onDurable` has its object data in the same record. Data
that arrives after the first save (a driver's data-less prepare, then the client's, `Tx.adoptObjectUpdates`) is saved
again by `saveAdoptedData`, and a commit decided meanwhile is held until that save is durable (`commitAwaitsData`): a
commit applied first and then lost to a crash before its backend commit completed would recover the Tx without the
data. A save that changes the update count makes SimpleCRL write the updates again, keeping the txd's location
(`SimpleCRL.onSaveTransaction`).

### 3.3 Loading and recovery of a store

1. The device's CRL is opened before any of its stores are offered for loading (`DeviceDiscovery.loadOwnedDevice`,
   `openDeviceCrl`), on the control thread.
2. `StoreLoader.handleLoad` reads the store's `crl_save.log`, if any, before the `Store` exists. An unusable
   one (`loadValidStoreState` returns `Left` for anything thrown, including transient errors) makes the store
   permanently unservable (`StoreRebuilds.crlSaveFileLost`).
3. With no save file, `StoreRouter.recoverDeferred` -> `readAndRecover` runs on the store's executor and calls
   `readStoreState`. It does not block: the Future completes on the CRL thread and recovery is routed back to the
   executor. The store holds its transaction messages until `Frontend.recover` has recreated its transactions, so it
   saves nothing in between. A throw while reading or recovering fail-stops the store (`Host.StoreFailStopped`).
4. With a save file: `crl.loadStore` saves every record into the device CRL, then `StoreRouter.crlLoaded` removes the
   file durably (`DurableFiles.remove`) and only then recovers. A crash between "records durable in the device CRL"
   and "file removed" loads the same records again under the same ids, which is harmless: the store served nothing in
   between. A failed `loadStore` unloads the store (`handleUnloadFailed`), which drops whatever reached the CRL
   (`closeUnloaded(dropDurably = true)`): the file is still the whole state.
5. Recovered transactions are counted into the device's admission without a limit check (`Frontend.admitRecovered`),
   as are recovered stubs (`chargeStubs`), replacing the share the device's open retained for the store (section 6).

### 3.4 Deletion is gated on the backend

A committed transaction's record is deleted at finalization only once its backend commits have succeeded:
`Frontend.transactionFinalized` defers to `txCommits` while commits are outstanding or failed, and
`transactionCommitComplete` concludes it when the last one succeeds. A failed backend commit keeps the record, with
its object data, for the life of the loaded store; a reload recovers the transaction from it. A recovered commit vote
that quarantines objects keeps its record, data included, until the quarantine clears (`quarantinedConclusions`).
This is the rule behind A4.

A backend deletion of an object that a transaction allocated recently waits until no CRL state durably holds that
allocation's update (`Frontend.awaitAllocationsDurable`): an unconcluded transaction's state is saved again without
the update (`Tx.persistWithoutUpdateOf`), and a concluded one's deletion or stub only has to be durable
(`whenDurable`).

### 3.5 Transfer out, drop, rebuild

| Operation | CRL calls | Source |
|-----------|-----------|--------|
| Planned device shutdown | stores close with `extractCrl = false`, records stay live; then `crl.shutdown()` | `DeviceShutdown.closeForShutdown`, `shutDownCrl` |
| Transfer out, store open | on the executor after `closing`: `readStoreState`; `saveStoreState` to `crl_save.log` (tmp, fsync, rename, directory fsync); `store.close()`; then `deleteTransactions(state.entryIds)` and the retained admission released | `StoreLoader.handleClose(extractCrl = true)` |
| Transfer out, store closed | `readStoreState`, merged over any earlier save file (`StoreRecoveryState.supersededBy`), saved, then `deleteTransactions`; fails if the store was opened during the read (`opensStarted`) | `StoreTransfers.extractUnloaded` |
| Retire of a copy no longer its store's owner (a lost store before its rebuild, a transfer out's source, a re-placed rebuild's source) | marker `rebuild-required` durable; `lost-crl-state.log` kept if present, else the copy's `crl_save.log` renamed to it, else `readStoreState` saved to it; `dropStore`; directory renamed into pending-delete | `StoreCopies.retire` (from `StoreRebuilds.discardLostStore` and the Deleted Stores pass) |
| Store no longer on the device (safety net: a retire drops first) | `storesWithEntries`; for each store not in the device's tree entry, not loaded, with no directory, no drop and no retire running: `dropStore` | `DeviceReconciler.dropLeftoverCrlState` |
| Store whose `crl_save.log` load failed | `dropStore` | `StoreLoader.closeUnloaded(dropDurably = true)` |

The rule that makes this safe: **entries leave the device CRL only through a written deletion, and only after another
durable copy exists or the store has been made unservable on disk.** The transfer deletes after the save file is
durable; the rebuild drops after the marker (and the `lost-crl-state.log` copy) is durable. Every operation carries
applied stubs with the transactions: the save file holds both kinds of entry (`codec.StoreCRLEntry.applied_stub`,
with the start timestamp), and the deletions name `state.entryIds`.

The leftover drop is safe against a store landing on the device meanwhile: the directory and loaded checks run in the
control task that issues the `DropStore`, a store that lands has its directory before anything saves for it, and the
CRL processes requests in queue order, so the drop is processed before any save of the new store, and a save landing
in the drop's own entry cancels the drop's deletion of that id (C9).

### 3.6 Applied stubs

An applied stub (store S, transaction T) means "S already holds T's update, so an UnackedCommits entry naming S for T
is stale" (`S/crl/StoreRecoveryState.scala`). It carries T's start timestamp and nothing else: no description, data,
Paxos state or requirements. It is never recovered as a transaction: the Frontend keeps the stub txids
(`Frontend.recoverState`) and consults them before applying T (transaction-recovery.md).

- **Namespace.** Stubs are separate records under the same TxId. A stub's save replaces the transaction state of the
  id in the same entry (`SimpleCRL.onSaveAppliedStub`, a cancellable deletion), so the deletion and the stub are
  durable together. A later save of the transaction lives beside the stub; in the same entry it cancels the stub-made
  deletion. Only `deleteAppliedStub`, `deleteTransactions` and `dropStore` remove a stub; `deleteTransaction` never
  does.
- **Write point.** Only where the transaction's record would be deleted (after its backend commits succeeded), never
  while they are outstanding or failed: `recordAppliedStub` requires it.
- **Crash between "commits durable" and "stub durable".** The record is still live, so the store recovers T as a
  commit vote and resolves it as any recovered transaction.
- **Copy-forward.** A stub has a zero-length `anchor` location at the end of the dynamic data of the entry that first
  wrote it, so a rotation copies it forward exactly as it does a transaction's data (`Stub.closeStream`).
- **Recovery.** A stub is live unless a newer or same-entry stub deletion names it; an entry never holds both a stub
  and its deletion (`LogEntry.addStub`/`deleteStub`). The open keeps each stub's anchor in the newest entry holding it.
- **Admission.** Each stub is charged `CrlAdmission.AppliedStubBytes` (41: TxId 33, timestamp 8) without a check,
  and released when dropped; a closing store retains the charge (section 6).
- **Lifetime.** The store drops a stub at heartbeat once T is past the horizon plus the expiry margin and the store
  holds no Tx for T (`Frontend.expireStubs`). Past that point a prepare for T creates no Tx (`Frontend.receivePrepare`
  refuses it below the forget floor), so the stub can suppress nothing. Stubs are therefore bounded by their write
  rate times about 30 s, whatever the repair scans do. A rebuilt store has none (its entries are dropped); it is
  muted for older transactions instead (rebuild-process.md).

---

## 4. Per-device CRL lifecycle

- **Created.** `StorageDeviceManager` writes the device config and `owning-host` and calls
  `CrashRecoveryLog.createDeviceCrlDir` (makes `crl/`, forces the device directory). The stream files are created at
  the first open (5.1).
- **Opened.** `DeviceDiscovery.openDeviceCrl`, on the control thread, blocking (Backlog.txt, "Device CRL recovery
  currently runs on the host control thread"). Recovery reads the walked entries and the CRC of each; it writes
  nothing. Then `loadOwnedDevice` creates the device's `CrlAdmission` with `admissionLimitBytes`, charges
  `recoveredBytesByStore` retained per store, and subscribes `DeviceShutdown.crlFailed` to `crl.failed`.
- **Refused, retried.** `CrlLocked` (another writer holds `crl/lock`) and `CrlSizingMismatch` (stream files without a
  readable `sizing` file, or not of the recorded count and size) are rethrown: the device stays unloaded, nothing is
  marked, every scan retries. A sizing mismatch is logged once; it means damage or a hand edit, never configuration.
  An `IOException`, `UncheckedIOException` or `VirtualMachineError` (`DeviceDiscovery.isTransient`) is rethrown too,
  up to `CrlOpenAttempts` (3) consecutive scans.
- **Lost.** If `crl/` is absent from a device that holds stores (a child directory with a store `config`), or holds no
  stream files there (`openCRL` is passed `holdsStores` and throws `CrlMissing` before writing anything: stores exist
  only on a device whose CRL was opened, so a `crl/` emptied by hand, a file system repair or a partial restore is a
  lost CRL, never a new one), or the open throws anything else, or a transient error a third time in a row: every such store directory gets a durable
  `rebuild-required` marker, one by one; `crl/` is moved to `crl.lost-<uuid>` (kept deliberately, Backlog.txt); the
  device directory is forced and a fresh CRL is opened. `CrlCorrupt` is logged under its own cause. The reconcile then
  flips each store to Rebuilding. A device without stores simply has its CRL created.
- **Failed.** A CRL whose thread dies (5.10) completes `failed`; `DeviceShutdown.crlFailed` disowns the directory,
  closes the stores, gossips them offline and unloads the device until the host restarts. It is neither reloaded
  before that (a reopen loop would reach `CrlOpenAttempts`) nor marked Failed in the tree.
- **Unloaded.** `DeviceShutdown.shutDownCrl` runs `crl.shutdown()` on `CrlShutdownEc` and records the path in
  `LocalDevices.closingCrls` until it returns, so the device is not loaded again over open files.
- **Sizing.** `crl-num-streams` and `crl-max-file-size-mb` are required keys of the device yaml
  (`StorageDeviceConfig`; `aspen` writes 3 and 300 by default). They size only a new CRL; an existing one keeps the
  sizing in `crl/sizing` and logs a warning if the config differs.

---

## 5. SimpleCRL, the reference implementation

Code: `S/crl/simple/`.

### 5.1 Files, creation and the lock

A CRL is `crl/sizing`, `crl/lock`, and `numStreams` files `0.crl ... N-1.crl`, each exactly `maxSizeInBytes` long.

`SimpleCRL.apply`:

1. `CrlLock.acquire` takes an exclusive POSIX lock on `crl/lock` before anything is read. A second opener in another
   process gets `CrlLocked` from `tryLock`. A second opener in this process is refused by a registry keyed by the
   lock file's identity before it opens any descriptor: closing any descriptor of the file would drop the process's
   lock. The lock is released once the writer has closed the stream files (shutdown, a failed open, or death). It does
   not fence two hosts mounting one shared device.
2. `recordedSizing`: an existing `sizing` file gives the sizing (at least 2 streams, else `CrlSizingMismatch`). With
   no `sizing` file, stream files mean `CrlSizingMismatch`; no stream files mean a new CRL, whose requested sizing is
   written durably (`DurableFiles.write`) before any stream file. A `sizing` file with no stream files is a creation
   that crashed before them, and its sizing stands.
3. `createStreamsIfNew`: a leftover `*.crl.tmp` means a creation that never finished: it and any stream files are
   removed and creation starts over. With no stream files, each is preallocated as `<n>.crl.tmp`
   (`FilePreallocation.preallocate`: `fallocate(2)` mode 0 through FFM on Linux, zero-fill elsewhere or on
   `EOPNOTSUPP`; forced), renamed into place, and the directory forced. Only then does the writer start, so a `.tmp`
   at open always means a creation that wrote no entry.
4. `checkSizing`: the stream files must be exactly the expected set, each of the recorded size.
5. `Recovery.recover` (5.7), then the constructor (5.7). A constructor that fails shuts its writer down and waits for
   it before rethrowing, so a retry never runs two writers over the same files.

`StreamWriter` opens existing files only, with `DSYNC | WRITE`, and refuses one of the wrong size. Because the files
are preallocated and the file system overwrites in place (A7), a CRL write never allocates, and a full device cannot
fail it with `ENOSPC`.

The streams form a ring. One is **active** and written append-only. When an entry does not fit, the writer advances
to the next stream, **recycles** it (new random stream UUID, write offset 0; in memory only) and writes there
(`SimpleCRL.writeCurrentLogEntry`, `Stream.recycleStream`).

### 5.2 Entry format

Header (`LogEntry`, `StaticEntryHeaderSize` = 86 bytes, big-endian):

| Field | Bytes |
|-------|-------|
| stream UUID | 16 |
| entry serial number | 8 |
| oldestEntryNeeded (serial) | 8 |
| previous entry location (stream 2, offset 8, length 4) | 14 |
| dynamic data size | 8 |
| static data size | 8 |
| number of transactions, transaction deletions, applied stubs, stub deletions | 4 x 4 |
| body_crc: CRC32C of dynamic and static data through the trailing UUID | 4 |
| header_crc: CRC32C of the header bytes before it | 4 |

Then **dynamic data** (serialized txds and object-update payloads of records with no current location, raw), then
**static data**: one record per transaction in the entry, one 41-byte record per stub (TxId, start timestamp), 33-byte
TxIds of transaction deletions, then of stub deletions, then the stream UUID again. Then zero padding to a 4 KiB
multiple (`LogEntry.padTo4k`), so entries start on 4 KiB boundaries.

A transaction's static record (`Tx.writeStaticEntry`): TxId 33 (pool UUID 16, store index 1, tx UUID 16), txd
location 14, disposition 1, Paxos mask 1 + promised 5 + accepted 5, update count 4, then per update object UUID 16 +
location 14: 63 bytes plus 30 per update (`CrlAdmission.TxOverheadBytes`, `ObjectUpdateOverheadBytes`). The mask has
bit 0 promise present, bit 1 accepted present, bit 2 accepted value, bits 3-4 status (0 Unresolved, 1 Committed,
2 Aborted). After `dropTransactionObjectData` the record is written with count 0.

A save of an existing transaction costs only its static record; its txd and data are written once and referenced by
location from every later record. Every entry is at least 4 KiB and every write is a `DSYNC`.

### 5.3 In-memory structures and threads

Two threads per CRL:

- the **CRL thread** takes `ActionRequest`s from `ioQueue` and owns all mutable state: `transactions` and `stubs`
  (maps of `Record`s), the age-ordered queue (`queueHead`/`queueTail`), `currentLogEntry`, the streams' offsets, and
  `afterInFlight` barriers;
- the **writer thread** (`StreamWriter`) takes `Write` commands, positions the channel and writes every buffer
  (`writeFully` loops, since a gathering write takes at most `IOV_MAX` buffers per call), then runs the entry's
  completion handlers and queues `WriteComplete` to the CRL thread (`Stream.writeEntry`).

`transactions` holds the full `TransactionRecoveryState` of every live record, object data included, even after
`dropTransactionObjectData` (which only clears the record's flag and locations). A `Tx` record also knows where its
txd and data currently are (`txdLocation`, `objectUpdateLocations`) and its queue position (`entrySerialNumber`, the
entry holding its oldest data). A `Stub` record has its `anchor`.

### 5.4 Write path

`save` queues `SaveTransaction`. `onSaveTransaction`:

1. If the current entry deletes the id finally (C9), queue only the handler and return.
2. Look up the record; if absent, create it with no locations, put it at the queue head with the current entry's
   serial, and register it.
3. If the update count changed, forget the update locations so the data is written again.
4. Set the state, add the record and the handler to `currentLogEntry`, `startWrite`.

`startWrite` writes `currentLogEntry` only if it is not empty and no write is in progress. While a write is in
flight, further requests accumulate in the next entry: group commit, the only batching. `LogEntry.isEmpty` counts
records and deletions, not handlers. After a write is issued, the next entry is created with the location of the one
just issued as its previous entry and `oldestEntryNeeded` as of that moment (a stale value is only older, which is
safe: recovery walks further back over entries the ring has not recycled).

`Stream.writeEntry` builds the buffers (`LogEntry.createEntryBuffers`): header, then the txd and update buffers of
records with no location (which are assigned their new locations), stub anchors, the static region, the checksums,
padding. The buffers are built on the CRL thread when the write is issued; the writer only writes them.

### 5.5 Rotation (compaction)

When the current entry does not fit the active stream, `writeCurrentLogEntry` advances to stream *k*, recycles it, and
for every live record with data in *k* or in *k+1* (`Record.closeStream`) clears its locations, moves it to the queue
head and adds it to the entry. It then sets the entry's `oldestEntryNeeded` to the queue tail's serial.

**Invariant: the stream after the active one holds no needed entry.** The rotation into *k* copies forward the live
data of *k+1*, the next stream to be recycled, so *k* itself was cleared by the previous rotation. Whatever part of a
torn rotation write persists, it overwrites only entries no recovery needs. After the moves every record whose data
was in *k+1* is in the new entry, so the new entry's `oldestEntryNeeded` lies in *k-1* or *k*, never in *k+1*. With
two streams the copy is the whole live set. A record found with data still in the recycled stream itself would break
the invariant; it is logged as an error and copied too. No path produces one: a record's txd is never newer than its
data, and data is written only into the active stream, so a record's oldest data is always reached as "next" before it
is recycled.

The rotation entry holds the pending entry plus the copy-forward. Admission keeps that within one stream for ordinary
loads (section 6).

### 5.6 Durability

- Every stream file is opened `DSYNC`: each `write` returns only after its data and the metadata needed to read it are
  on stable storage (A1). No separate `force`, no `O_DIRECT`.
- One write in flight per CRL; throughput is `entries/s = 1 / (write latency + handler time)`, since the next entry is
  issued only after the previous one's handlers ran (C2).
- Preallocation with `fallocate` leaves unwritten extents; the first write to each converts it, a metadata change
  `DSYNC` commits with the write.
- `DurableFiles` (tmp, `force`, atomic rename, directory fsync) is used for `sizing`, the stream files' creation, and
  outside the log for `crl_save.log`, `lost-crl-state.log` and the store markers. Directory fsync is a no-op on Windows
  and not `F_FULLFSYNC` on macOS; Linux is the supported platform.

### 5.7 Recovery

`Recovery.recover`:

1. **Newest entry.** Streams are ordered by the serial of their head entry (offset 0) and the first with a wholly
   intact entry wins (`StreamReader.lastIntactEntry`). Within a stream, entries are followed from the head while their
   headers are intact, carry the stream's current UUID (read from offset 0) and have rising serials; only the last
   one's body is checked. A torn last entry is dropped with a warning and the entry before it is the last. A stream
   whose only entry is a torn head (a rotation write in flight at a crash) yields to the stream before it.
2. **Nothing newer.** `newerIntactEntry` scans the active stream past the last entry, and all of the next stream, for
   an intact entry of any cycle with a higher serial, hopping intact headers by their size and anything else by
   4 KiB. A hit throws `CrlCorrupt`: a corrupt entry hides a newer one (mid-stream rot, or a rotted head hiding the
   real active stream).
3. **Walk.** From the last entry, follow `previous entry` links, loading each entry's static region into a
   `RecoveringState`, until reaching the last entry's `oldestEntryNeeded`. Each step must reach an entry of its
   stream's current cycle with a lower serial and an intact header and body, or `BrokenEntryChain`/`CrlCorrupt` is
   thrown. The extents of each verified entry's dynamic data are recorded.
4. **Apply.** Entries are applied newest first: a record already present is not replaced; one named by a deletion of
   the same or a newer entry is skipped (C4, C9). Stubs follow the same rule against stub deletions.
5. **Data.** Each surviving transaction's txd and data are read from their locations, which must lie within the
   dynamic data of a verified entry (`Recovery.entryHolding`), so they are covered by its checksum. Its queue serial
   is the oldest such entry.

The result also sets the active stream (with the **last entry's** UUID, so new entries remain readable from that
stream), the next offset and serial, and the previous-entry link (to the last entry itself).

**The open writes nothing.** The constructor queues each recovered record by its serial with the locations its
newest static record names (a stub by its anchor in the newest entry holding it), as if this instance had written
them, and starts the first entry with `oldestEntryNeeded` = max(queue tail, the last entry's value). The last durable
entry kept the ring invariant (a torn rotation leaves the stream before it active, where it still holds), so the next
rotation's copy-forward finds and moves them as the instance before the crash would have. A live set larger than a
stream therefore still opens; its next rotation hits the `Stream.writeEntry` backstop.

After a torn newest entry is dropped, the next entry reuses its serial and offset and overwrites it. If the torn entry
was a rotation head in the next stream, both that head and the new entry carry the same serial until the next
rotation overwrites the head; recovery ignores the head because its body fails.

Zero-filled regions are not entries: a zero header fails `header_crc`, and a zero stream UUID is rejected
(`StreamReader.readHeader`), so a never-written CRL opens empty at stream 0, offset 0.

### 5.8 Deletion

`deleteTransaction` records the TxId in the current entry's deletions, removes the record from the map and the queue,
and starts a write. `deleteTransactions` and `dropStore` (`onDeleteTransactions`) put all their deletions in one entry
with the handler: each id gets a transaction deletion and a stub deletion (66 bytes per id), and the record and stub
are removed from memory. `dropStore`'s ids are those of the store held when the request is processed, and its
transaction deletions are cancellable. `deleteAppliedStub` writes a stub deletion only for a stub held in memory. A
deleted record already in the current entry stays there and is filtered at recovery by the deletion in the same entry.

### 5.9 Barriers

`whenDurable` queues a `Barrier`: if the current entry is non-empty its handler is added to it (run on the writer
thread); else if a write is in flight it waits in `afterInFlight` and runs on the CRL thread at `WriteComplete`; else
it runs at once. Every request queued before the barrier is in the current entry, in the entry being written, or
already durable, so this is exact.

### 5.10 Shutdown and death

`shutdown()` queues `Shutdown` behind earlier requests; the CRL thread passes it to the writer, which drains the
write in flight, closes the channels, releases the lock and signals. The pending, unwritten `currentLogEntry` is
discarded; none of its handlers has run (C3). Futures of requests not yet written are never completed (CRL-W5).

The CRL has one death path. A writer that throws (an I/O error, a completion handler that throws) closes its channels
and queues `WriterDied`; the CRL thread throws it. Any throw on the CRL thread (including `Stream.writeEntry`'s
`require`) marks the CRL dead, fails every pending tracked Future (`readStoreState`, `deleteTransactions`, `dropStore`,
`storesWithEntries`, `loadStore`'s saves) with `CrlFailed`, shuts the writer down (releasing the lock), completes
`failed` and calls `onThreadDeath` once. Every later tracked call fails with `CrlFailed`. Handler-style completions
(`save`, `saveAppliedStub`, `whenDurable`) never run for an unwritten entry, so no vote follows. An entry whose write
was already in flight when the CRL thread died is still written and its handlers run. The files hold what was
written, so a restart recovers them as after a crash.

---

## 6. Admission and backpressure

`CrlAdmission` (`S/crl/CrlAdmission.scala`) is one atomic byte counter per device, with limit
`crl.admissionLimitBytes` = half a stream (150 MB by default).

- **New transactions.** `Frontend.admitPrepare` admits a prepare for a transaction it has not seen only while the
  total is below its store's tier ceiling (`AdmissionTier`, the store's pool's, persisted in its StoreConfig): 3/4 of
  the limit for data pools, 7/8 for metadata pools, the whole limit for the bootstrap pool. A data transaction's
  finalization writes trees in a metadata pool, and a metadata transaction's in the bootstrap pool, so lower-tier load
  never takes the headroom a higher tier's finalization needs (cross-document-review.md). It charges the description,
  object data and static record (`63 + 30 per update` + data). Otherwise the prepare is dropped silently
  (`Store.receiveTransactionMessage`) and the client retransmits.
- **Charged without a check:** known and admitted transactions' later data (`chargeData`), recovered transactions
  (`admitRecovered`), stubs (`chargeStubs`), and at device open everything the CRL holds, per store
  (`recoveredBytesByStore`, `chargeRetained`). The check is against the total before the admission, so one admission
  may cross the ceiling by its own size, which nothing bounds (CRL-W2).
- **Released** when the record is deleted (`deleteTransactionState`, `concludeTransactionState`), when a Tx is
  forgotten without reaching the CRL, or, for a transaction replaced by a stub, once the stub is durable. A stub's
  charge is released when it expires (`dropAppliedStub`).
- **Retained.** A closing store keeps its bytes counted for it (`CrlAdmission.retain`) until its entries leave the CRL
  (`releaseRetained` after an extraction's deletion, a `dropStore`, a leftover drop) or it loads again and counts its
  own. Records of stores that never load on this run are therefore counted from the open while they are copied forward.
- **Not counted:** static records rewritten by later saves (not live), deletions queued in an entry, and the second
  copy of data rewritten when the update count changes.

Admission is a liveness tool and the only thing that maintains C11. A failure of C11 kills the CRL (5.10), which
unloads the device until restart and leaves the files recoverable.

---

## 7. Sharing one CRL among a device's stores

All stores on a device share one ring, one writer thread and one admission counter.

- **Ordering across stores.** Entries interleave records of different stores; `readStoreState` filters by `StoreId`.
- **Deleting one store does not touch another.** `dropStore` names only that store's ids; others' records stay live
  and keep being copied forward.
- **A store being rebuilt.** Its records stay live until its retire (`StoreCopies.retire`) has saved them to
  `lost-crl-state.log` and `dropStore` completes; they are counted by admission from the open. Then the directory
  moves to pending-delete and the rebuild starts.
- **A store that fails.** A backend fail-stop at close marks it `rebuild-required`; its entries are dropped when the
  rebuild retires it. A transfer-out close had already moved them into `crl_save.log`, which the retire renames to
  `lost-crl-state.log`. A store whose `crl_save.log` is unusable is marked the same way.
- **A shared stall.** A slow `DSYNC`, a full admission lane or a dead CRL affects every store on the device. There is
  no per-store allocation by design.
- **Fate sharing.** A lost CRL marks every store on the device `rebuild-required`; a failed CRL unloads every store.

---

## 8. Co-locating the CRL with the stores it serves

This is the only layout implemented (`CrashRecoveryLog.deviceCrlDir(devicePath) = devicePath / "crl"`).

### 8.1 Benefits

- **One failure domain and one unit of movement.** The device carries every store's data and its transaction state. A
  drive pulled from a crashed or running host is in exactly the state a crash would leave, so another host can adopt
  it without cooperation from the old host (`DeviceDiscovery.tryAdoptDevice`).
- **No cross-device ordering problem.** "Backend commit durable, then delete CRL record" (A4) is within one device.
- **Simple recovery.** A restart finds one self-describing directory.
- **Sequential, append-only access.** SimpleCRL writes ascending offsets within one file at a time.

### 8.2 Drawbacks

- **IO doubled.** Every byte of object data is written to the CRL and again to the backend (RocksDB WAL and SST, or
  the FSIF journal and segment). Copy-forward writes live data a third time. Each entry is padded to 4 KiB and written
  with `DSYNC`. CRL traffic competes with store traffic for the same bandwidth and flush queue.
- **Flushes couple the workloads.** On a journaling file system an `O_DSYNC` write forces a journal commit and a
  device cache flush, which also drains the store's own pending writes, and the other way round. A busy backend
  raises CRL latency, which is on the critical path of every vote.
- **Wear.** On flash the ring (900 MB by default) is rewritten continuously.
- **Rotating disks are hit hardest.** Each CRL commit is a synchronous write with a rotational delay and, because the
  CRL files are far from the backend's, a seek on the vote's critical path. Group commit hides some of it.
- **Capacity.** 900 MB per device by default, not reclaimable by the stores; `crl.lost-*` directories add more.
- **Single-device failure is total.** State and data die together (stores are rebuilt from peers), and a damaged CRL
  turns into a rebuild of every store on the device.

---

## 9. Planned (not implemented) off-device CRLs

Backlog.txt ("Off-device CRLs") describes placing the CRL of an HDD on an NVMe device. Nothing in the code reads a
per-device CRL location; `deviceCrlDir` is the only definition.

- **Buys:** faster votes on slow media, one write of the data on the HDD, no flush coupling.
- **Costs:** failure modes span two devices. Losing the CRL device loses the Paxos state of every device that used it:
  under the current rule a simultaneous rebuild of many drives, likely of the same pools, the correlated-loss pattern
  of [systemic-loss-protection.md](systemic-loss-protection.md). The two devices can disagree after a partial failure,
  and nothing cross-checks them. A device can no longer be moved by pulling it: the proposal copies the CRL onto the
  device at shutdown, so a crash or early pull leaves a device the adopter refuses (`tryAdoptDevice` requires
  `<device>/crl` when it holds stores). The lost-CRL rule must consult the configured location, and "CRL lost means
  rebuild" must not fire on a bulk event.

---

## 10. Failure scenarios

"Safe" means no acknowledged state is lost and no wrong state is recovered, assuming A1-A7.

| # | Scenario | Safe? |
|---|----------|-------|
| 10.1 | Crash mid-write of an appended entry | Yes |
| 10.2 | Crash mid-write of a rotation entry | Yes |
| 10.3 | Corruption of a durable entry | Detected; the CRL is lost if a needed or hiding entry is hit |
| 10.4 | Power loss with a lying write cache | No (A1) |
| 10.5 | Live set or entry larger than a stream | Safe; the device is unloaded until restart |
| 10.6 | A slow or hung `DSYNC` | Safe; liveness only |
| 10.7 | A dead CRL thread or writer | Safe; the device is unloaded until restart |
| 10.8 | The device is pulled | Safe |
| 10.9 | Adoption by another host | Safe under A3 |
| 10.10 | Crash during transfer-out, before `crl_save.log` is durable | Safe |
| 10.11 | Crash during transfer-out, after `crl_save.log` is durable | Safe |
| 10.12 | Crash during a retire | Safe |
| 10.13 | Stores sharing a CRL, one dropped while it is loaded again | Safe |
| 10.14 | CRL files unrecoverable at open | Safe for the CRL; every store rebuilt |
| 10.15 | Sizing mismatch, or another opener holds the lock | Safe; the device is refused |
| 10.16 | Device full | Safe except on copy-on-write file systems |
| 10.17 | Crash during CRL creation | Safe |

**10.1 Crash mid-write of an appended entry.** None of the entry's handlers has run, so nothing depended on it. If
its header did not persist, the walk from the head stops at the previous entry. If the header persisted but not the
whole body, `body_crc` fails and the entry is dropped (C7). The next entry is written over it.

**10.2 Crash mid-write of a rotation entry.** The rotation into *k* overwrites only entries no recovery needs (5.5).
If the head's header did not persist, *k* keeps its old UUID and is not the newest stream; if it persisted with a
torn body, the head is dropped and *k* yields to *k-1*. Either way *k-1* is active and the records still in *k+1* are
recovered from there. Tested by tearing every rotation of a three-lap run at each sector prefix (SimpleCRLSuite).

**10.3 Corruption of a durable entry.** A bad checksum in any entry the walk visits, or in the newest entry's header
when a newer entry follows, throws `CrlCorrupt`; data is read only from verified entries. Rot exactly in the newest
acknowledged entry is indistinguishable from a torn write: that entry is dropped and its promises, votes or data are
lost, as with a lying cache. Rot in an entry recovery no longer needs still makes the CRL lost when it hides a newer
entry or lies in the walk. Both are accepted: the outcome is a rebuild, simple rather than optimized.

**10.4 Power loss with write caching.** `DSYNC` sends flush/FUA when the device reports a volatile cache. A drive that
acknowledges flushes it did not perform can lose acknowledged entries (Paxos amnesia); if it loses an entry that a
newer surviving one follows, recovery throws `CrlCorrupt`, otherwise it returns a shorter prefix silently. The same
drive breaks the backends too; it is an operational requirement (A1).

**10.5 An entry that does not fit.** `Stream.writeEntry`'s `require` throws on the CRL thread, which dies (10.7). The
recycle was in memory only, so the files are unchanged and a restart recovers. Admission keeps ordinary loads below
this; a single transaction larger than about a stream minus the live set reaches it (CRL-W2).

**10.6 A slow or hung `DSYNC`.** Entries are strictly serial, so every vote on the device waits. The pending entry
grows with arriving saves, bounded by admission for new transactions. Clients retransmit; transactions past their
timeouts are driven or aborted by others. No state is lost. A `DSYNC` that never returns blocks the device
indefinitely and `crl.shutdown()` with it (Backlog.txt, "Isolate every device's I/O").

**10.7 A dead CRL thread or writer.** Every pending and later Future fails with `CrlFailed`, no save completion runs,
and `DeviceShutdown.crlFailed` unloads the device until restart (5.10, section 4). Rebuilds and transfers on the device
fail their CRL calls and are resumed by the restart. No safety loss.

**10.8 The device is pulled.** Writes fail with an I/O error; the writer dies (10.7). Whatever was acknowledged is on
the device, which holds a consistent post-crash CRL: that is what makes adoption possible.

**10.9 Adoption by another host.** `tryAdoptDevice` requires `<device>/crl` and a directory for every store the tree
lists, commits an ownership transaction, then loads the device, which recovers the CRL (it writes nothing). The lock
keeps a second process on the same host or file system out. A running old host relinquishes the device when it sees
the tree name another owner (`DeviceShutdown.relinquishDevice`); on shared storage it may write until then (not
defended, by decision). A store with `transferring-out` and no valid `crl_save.log` is adopted as lost
(`DeviceDiscovery.adoptionClass`); one with `rebuild-required` is adopted Rebuilding.

**10.10 Transfer-out, crash before `crl_save.log` is durable.** The state is still in the device CRL: `readStoreState`
left it live, and the deletion follows only the durable save. The `transferring-out` marker is written only after the
close completes. A restart loads the store normally and recovers from the device CRL. Safe.

**10.11 Transfer-out, crash after `crl_save.log` is durable, before the deletion is durable.** The records are in both
places, identical, because the store served nothing in between. The restart finds the save file and loads from it:
`loadStore` re-saves under the same ids, the file is removed durably, then the store recovers. If the deletion became
durable first, the save file is the only copy; the destination validates the staged copy before promotion
(`loadValidStoreState`, while the source still holds the state).

**10.12 Crash during a retire (`StoreCopies.retire`).** The `rebuild-required` marker is durable before anything else, so the
store is never served. A crash before `lost-crl-state.log` or before the drop is durable leaves the entries live; the
next attempt saves (unless the file exists) and drops again. A crash after the drop and before the move finds no
entries (an empty deletion completes without a write) and moves the directory.

**10.13 One store dropped while it is loaded again.** `dropStore` drops exactly what was held when processed (C14); a
same-id save in the drop's own entry cancels that deletion (C9). Other stores' records are untouched. The rebuilt
store comes back with no Paxos memory; it is muted for every transaction started before its `acceptor-since`
(rebuild-process.md).

**10.14 CRL files unrecoverable.** `openCRL` throws `BrokenEntryChain`, `CrlCorrupt`, a decode error, or an I/O or VM
error three scans in a row: `openDeviceCrl` marks every store `rebuild-required` (durably, one by one), moves `crl/`
aside, and opens a fresh one. A crash between the markers and the move leaves markers and the old `crl/`: the next
open may succeed, but the marked stores stay unservable. The decision is irreversible and made from local evidence
only (Backlog.txt, "System-level protection against correlated lost store declarations": S13 covers transient,
configuration and version conditions).

**10.15 Sizing mismatch or lock held.** Thrown before any stream file is read; the device stays unloaded with its
files and stores untouched and each scan retries. Not a lost CRL.

**10.16 Device full.** The stream files are preallocated, so a CRL write never allocates, except on copy-on-write file
systems (A7), where an `ENOSPC` kills the CRL (10.7). Freezing the device's writes instead is tracked in Backlog.txt
("Full storage devices").

**10.17 Crash during CRL creation.** `sizing` is written durably first; stream files go through `.tmp` and rename, and
the writer starts only after the directory is forced. A `.tmp` at open restarts the creation; a `sizing` with no
stream files creates them. No entry was written in either case, and no store can exist on the device yet: on a device
holding stores either state is a lost CRL (`CrlMissing`, 4).

---

## 11. Known limitations (accepted)

- Recovery runs on the host control thread and reads up to the whole ring, checksumming every walked entry (Backlog.txt,
  "Device CRL recovery currently runs on the host control thread").
- A hung device hangs its CRL and the host's shutdown of it (Backlog.txt, "Isolate every device's I/O").
- A lost CRL rebuilds every store on the device from local evidence, including after three transient open failures;
  `crl.lost-*` is kept but never used (Backlog.txt, "System-level protection against correlated lost store
  declarations", S13, S7/Q1). `loadValidStoreState` likewise treats any throw, transient or not, as a lost
  `crl_save.log` (`crlSaveFileLost`), under the same Backlog item.
- An `ENOSPC` on a copy-on-write file system kills the CRL rather than freezing writes (Backlog.txt, "Full storage
  devices").
- `CrlLock` does not fence two hosts mounting shared storage (systemic-loss-protection.md).
- Rot in the newest acknowledged entry is indistinguishable from a torn write (10.3).
- Admission lanes reserve a quarter of the limit on devices with only data stores (Backlog.txt, "CRL admission lanes
  waste headroom on data-only devices"); same-tier finalizers (TKVL split, join, tail removal) still compete with
  their tier's traffic.

---

## Open weaknesses

### CRL-W1. Recovery takes object data for evidence of corruption (PLAUSIBLE; systemic loss)
Mechanism: `Recovery.recover` calls `StreamReader.newerIntactEntry` on the active stream from the next write offset
and on the whole next stream. In regions that are not intact headers it probes every 4 KiB boundary with
`readHeader`, which accepts any bytes whose `header_crc` matches, whose stream UUID is non-zero and equals the UUID
16 bytes before the claimed body end, and whose sizes fit; `bodyIntact` then checks a CRC over bytes the header
itself describes. None of this is secret or tied to this CRL: `newerIntactEntry` deliberately ignores the stream's
cycle UUID. Those probed regions are the previous cycle's (or the torn in-flight entry's) dynamic data, which holds
raw object payloads written by `LogEntry.createEntryBuffers` (`allocate(ou.data.asReadOnlyBuffer())`). A payload
containing a well-formed entry with a high serial at a 4 KiB file boundary makes the scan throw `CrlCorrupt`.
Trigger / scenario: (a) a client writes objects whose content is a crafted CRL entry (header, matching trailing UUID,
both CRCs, serial 2^62), repeated at many phases or over many writes so one lands 4 KiB-aligned in a CRL file;
(b) innocently, a VDisk or object holding an image of another Aspen device's CRL (a nested Aspen, a backup of
`crl.lost-*`), whose inner entries are 4 KiB aligned. On the next restart, if the aligned copy lies past the last
entry of the active stream (inside the old cycle's partial entry that the scan steps through) or past the end of the
next stream's last cycle, recovery throws.
Consequence: the device's CRL is declared lost (`openDeviceCrl`): every store on it is marked `rebuild-required` and
rebuilt. Repeatable by a tenant against every device holding its replicas, so it can push pools toward the
correlated-loss pattern.
Suggested fix: make entries unforgeable by content: record a random CRL id in `crl/sizing` at creation and include
it in every header (or seed both CRCs with it), and have `readHeader` reject headers without it. Add a test that
writes object data containing a valid foreign entry and reopens.
Effort: S   Priority: 3

### CRL-W2. One large transaction kills the CRL of every device it touches (CONFIRMED; liveness)
Mechanism: `Frontend.admitPrepare` checks only that the device total is below the tier ceiling before admitting, and
`chargeData` adds later data without a check; nothing compares a transaction's own CRL bytes with the stream size
(no check in `DescriptionValidator`, `Store`, or the client's `TransactionBuilder` beyond per-object
`maxSliceSize`, which may be unlimited and does not bound the number of objects). `SimpleCRL.writeCurrentLogEntry`
rotates and `Stream.writeEntry`'s `require(canWriteEntry)` throws on the CRL thread when the entry (pending saves plus
copy-forward) exceeds a stream.
Trigger / scenario: a client prepares a transaction whose updates to one store total more than about
`crl-max-file-size-mb` minus the live data of the next stream (with defaults, roughly 150-300 MB; much less on
devices configured with small CRLs), e.g. five 64 MB objects. Each replica store's save makes an entry that cannot
fit; the CRL dies; `crlFailed` unloads the device. After the host restarts (the open recovers fine: nothing was
written), the client's driver retransmits, the prepare is admitted again and the device dies again.
Consequence: every device holding a store of the transaction is unloaded, repeatedly, by one client request; all
other stores on those devices go offline with it.
Suggested fix: bound a transaction's CRL bytes per store (`CrlAdmission.recoveredBytes` of its prepare, plus late
data) by a fixed fraction of the stream that admission's headroom accounts for, e.g. a quarter of the limit, and
vote abort with a named `RequirementError` instead of admitting; expose the bound to clients (pool or system
metadata) so `TransactionBuilder` fails early. Add a SimpleCRL test that an oversized save is refused, not fatal.
Effort: M   Priority: 4

### CRL-W4. Completion handlers run on the writer thread before the next write is issued (PLAUSIBLE; performance)
Mechanism: `Stream.writeEntry`'s callback runs `entry.runCompletionHandlers()` and only then queues `WriteComplete`;
the writer thread runs the callback before taking its next command, and the CRL thread issues the next entry only on
`WriteComplete`. The handlers of `Tx.answerPrepareAsAcceptor`, `sendDelayedPrepare` and `receiveAccept` send network
messages inline (`net.sendTransactionMessage`, `sendClientResponse`: QUIC lookup, encode and submit per message).
Trigger / scenario: group commit under load: an entry carrying thousands of saves runs thousands of sends serially on
the one writer thread of the device, adding their time to every entry's cycle. On NVMe, where a `DSYNC` takes tens
of microseconds, the handler time can exceed the write time.
Consequence: device CRL throughput, and so every vote on the device, is bounded by single-threaded message encoding
as well as by the write.
Suggested fix: hand an entry's handlers to a dedicated, ordered completion executor (or post each to its store's
executor) and queue `WriteComplete` at once; C2's guarantee (every earlier entry durable when a handler runs) is
unaffected. Measure with `BackendTiming`-style timing first.
Effort: S   Priority: 5

### CRL-W5. `shutdown()` leaves pending Futures uncompleted forever (CONFIRMED mechanism; PLAUSIBLE trigger; liveness)
Mechanism: `SimpleCRL`'s `Shutdown` case shuts the writer down and returns; only the death path calls `failPending`,
and `dead` stays false, so every tracked Future not yet answered (`readStoreState`, `deleteTransactions`,
`dropStore`, `storesWithEntries`, `loadStore`) and every later call never completes; `whenDurable` handlers in the
discarded entry never run.
Trigger / scenario: a device is unloaded (`DeviceShutdown.unloadDevice`) while a `discardLostStore` drop, a leftover
drop or an extraction is pending on its CRL. Their `onComplete` never runs, so `StoreRebuilds.droppingCrlState` and
`DeviceReconciler.crlDropsRunning` keep the store until the process restarts.
Consequence: callbacks and promises leak; if the same device is loaded again in the process, that store's discard
(and so its rebuild) never starts until a restart. Today a device unloaded in-process is normally reloaded only after
a restart, which limits the effect.
Suggested fix: on `Shutdown`, fail every pending tracked promise (and every later call) with a `CrlClosed` error,
as death does with `CrlFailed`.
Effort: S   Priority: 5
