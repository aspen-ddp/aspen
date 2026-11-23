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
| AS1 | A `write` to a file opened with `DSYNC` that returns normally is on stable storage, including after power loss. Disk write caches honour flush/FUA or are disabled. | All of section 5.6 |
| AS2 | A failed or never-returning `write` means the data is not relied on. Writes are never retried; the CRL dies instead (5.10). | `S/crl/simple/StreamWriter` |
| AS3 | Only one CRL instance writes a device's `crl/` files. Enforced by `CrlLock` for every opener on the same file system (5.1); not enforced for two hosts mounting one shared device. | adoption, double mounts |
| AS4 | Backend commit completion means the object update is durable in the backend (RocksDB writes with `setSync(true)`, FSIF completes once its journal batch is forced). The CRL record of a committed transaction is deleted only after that. `MapBackend` gives no durability and is for tests only. | section 3.4 |
| AS5 | The CRL and the stores it serves are on the same device and move together. | sections 7-9 |
| AS6 | A store's `save`/`delete*` calls come from its own executor; the CRL thread serialises everything. | section 5.3 |
| AS7 | The file system overwrites in place. On copy-on-write file systems (btrfs, ZFS) every CRL write allocates and can fail with `ENOSPC`; a CRL's creation warns of it once. | 5.1, 10.17 |

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
| `save(txid, state, onDurable)` | Make `state` durable; `onDurable` runs once it is, on the CRL's completion thread (C2). Saves for one (store, txid) replace one another; the newest wins. |
| `deleteTransaction(store, txid)` | Lazy, unacknowledged deletion of the transaction state. A crash before it is written leaves the transaction recoverable. An applied stub of the id stays. |
| `deleteTransactions(store, txids): Future` | Deletes transaction state **and** applied stubs of each id, in the newest unwritten entry, spanning entries when they would overflow it (W1); completes once the last of them is durable. An empty list writes nothing and completes once every earlier request is durable (a barrier, 5.8). |
| `dropTransactionObjectData(store, txid)` | Memory only: saves made after it leave the object data out. Nothing is written. Saves already queued, and the last-built form an evacuation carries, keep the data until a later save is built (5.3). |
| `whenDurable(store, handler)` | Runs `handler` once every request queued before it is durable. Orders a write outside the CRL (a backend deletion) after a deletion, which has no completion of its own. |
| `readStoreState(store): Future[StoreRecoveryState]` | The store's live transactions and stubs, answered in queue order with every earlier request. Leaves them live. Returns the in-memory state, so a transaction whose data was dropped is still returned with its data. |
| `getFullRecoveryState` / `getAppliedStubs` | Blocking variants of `readStoreState` (SimpleCRL awaits it). Only the undeferred, test-only Frontend path blocks. |
| `loadStore(store, state): Future` | Saves a `crl_save.log`'s state: every stub first, then (once those are durable) every transaction. SimpleCRL tracks every save so a CRL death fails the Future. |
| `saveAppliedStub(store, txid, start, onDurable)` | Saves a stub, replacing any transaction state of the id in the same entry. Never subject to admission checks. |
| `deleteAppliedStub(store, txid)` | Lazy, unacknowledged deletion of the stub only. Writes nothing if no stub is held. |
| `dropStore(store): Future` | Deletes every transaction and stub of the store held when the request is processed, durably. In SimpleCRL its transaction deletions are cancellable by a later save of the same id in the same entry (5.8). A store holding nothing writes nothing and completes once every earlier request is durable (a barrier, 5.8). |
| `storesWithEntries(): Future[Set[StoreId]]` | Stores with live entries, stub-only stores included. |
| `recoveredBytesByStore` | What the open recovered for each store, as admission counts it (section 6). |
| `failed: Future[Throwable]` | Completes, with the cause, if the CRL dies (5.10). |
| `shutdown()` | Blocks until the CRL's thread and writer have stopped and its files are closed. Handlers already handed to the completion thread run first; every Future not answered by then, and every later one, fails with `CrlClosed` (5.10). Idempotent. |
| `admissionLimitBytes` | The device's admission limit (section 6): `(n − 2) · S/2 − S/16 − H · (n − 1)` in SimpleCRL, for `n` streams of `S` bytes (`H = 118`, `SimpleCRL.admissionLimitFor`). |
| `maxAdmissionLimitBytes` | The most admission limit the CRL can reach under any device config: `(m − 2) · S/2 − S/16 − H · (m − 1)` in SimpleCRL for `m` the larger of the ring size and `CrlSizingDefaults.MaxMaxStreams` (1024), whatever `crl-max-streams` this open allows. A loaded state is judged never to fit against it (A3), so no config edit can declare a state lost. |
| `maxTransactionBytes` | The most bytes, as admission counts them, one store's transaction may hold: `S/2 − 8192` in SimpleCRL. A prepare past it is dropped unsaved (section 6, Oversized). |
| `requestGrowth()` | Asks for more room; cheap, idempotent, from any thread. SimpleCRL adds one stream if its growth policy allows (5.11). |
| `onAdmissionLimitRaised(f)` | Registers `f`, called on the CRL thread with the new `admissionLimitBytes` after each committed growth. |
| `openCRL(devicePath, config, holdsStores)` | Recover and open. Throws `CrlLocked`, `CrlSizingMismatch`, `CrlConfigInvalid` (no CRL yet, and the config cannot size one), `CrlMissing` (`holdsStores`, and no stream files), `CrlCorrupt`, `BrokenEntryChain`, or whatever reading the files threw. |

The trait's defaults (read through `getFullRecoveryState`, `dropStore` as read-then-delete, `whenDurable` at once,
no failure, no per-transaction bound, no growth) are for CRLs that keep nothing on disk, such as test CRLs.
`loadStore`'s default runs its callbacks on the global pool; SimpleCRL overrides it.

### 2.1 Numbered contract

**Durability and ordering**

- **C1 (save before send).** `onDurable` of `save(t, s, h)` runs only after the entry holding `h` has been written with
  `DSYNC` and the write returned. A caller sends nothing that depends on `s` before `onDurable`. Exception, by design: a
  save of an id that an unwritten entry, the one in flight included, already deletes finally is skipped and still
  acknowledged (C9, W1b), and so is a save that a later deletion of the id in the same unwritten entry takes out of it
  (`LogEntry.deleteTx`, 5.8). A deletion is the store's decision to forget the transaction: a save queued beside or
  behind it would make durable a state the deletion is about to remove anyway. That this is safe for the save's sender
  is the transaction protocol's argument, not the CRL's: a store queues a final deletion only for a transaction it has
  concluded ([cross-document-review.md](cross-document-review.md) 2.1).
- **C2 (entry order).** Entries are written one at a time, in serial order. Entry N+1 is not issued until entry N's
  write returned and the CRL thread processed its `WriteComplete` (`writeInProgress`). At that point the CRL thread
  issues the next entry and hands N's handlers, and then the barriers waiting on N (`afterInFlight`), to the
  **completion thread**, which runs them while N+1 is written. So when a handler of entry N runs, every entry up to N is
  durable. The completion thread runs every handler (save, stub, deletion, barrier) in the order the CRL thread hands
  them over: entry by entry in serial order, then the barriers and skipped saves waiting on that entry's write
  (`afterInFlight`) in request order, but **newest first within an entry** (`LogEntry.completionHandlers` is prepended
  and run in list order). So two handlers of one entry run in the reverse of their requests' order, and a caller may
  rely only on every entry up to its own being durable, not on the order within an entry. A handler never blocks it.
  Callers that touch store state post to the store's executor (`Frontend.post`); `Tx`'s prepare and accept handlers only
  send messages and post (`saveState` posts `voteDurable`).
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
  `StreamReader.bodyIntact`). The header also bears the CRL's id, so only this CRL's writes are entries (5.2).
- **C7 (only the in-flight write may be torn).** Recovery drops the newest entry if its body fails its checksum: that
  can only be the write in flight at a crash, which no handler acknowledged. Any other failed checksum (the entry
  before a dropped one included), and any intact entry newer than the newest one found, throws `CrlCorrupt` (5.7).
- **C8 (broken chain is fatal).** If an entry needed for recovery cannot be read, or data a live record names lies
  outside the dynamic data of every verified entry, `openCRL` throws (`Recovery.BrokenEntryChain`, `CrlCorrupt`). It
  never returns a partial state.

**Deletion**

- **C9 (deletion finality in an entry).** A deletion of (store, txid) applies to that txid's saves in the same entry
  and in all older ones. A save of the same txid queued while a final deletion of it lies in any unwritten entry,
  the one in flight included (W1b), is skipped and acknowledged once the deletion is durable, and creates no
  in-memory record (`LogEntry.deletesFinally`, `SimpleCRL.skippedAfterFinalDeletion`). Deletions made by a stub's
  save or by `dropStore` are cancellable: a later save of the id in the same entry removes the deletion and is written
  (`LogEntry.deleteTxUnlessResaved`, `LogEntry.addTx`).
- **C10 (deletion durability).** `deleteTransactions` and `dropStore` complete only after the entry carrying the
  deletion is durable. `deleteTransaction` and `deleteAppliedStub` have no completion; `whenDurable` orders later
  work after them.

**Capacity**

- **C11 (live bound).** An evacuation entry always fits an empty stream, and every other entry is at most half a
  stream (W1). With the live data within 6.1's bound, which admission keeps (section 6), every entry is written within
  `n − 1` rotations. A head not written after `n − 1` rotations in a row fails the CRL (W6, 5.10);
  `Stream.writeEntry`'s `require` stays as the fail-stop backstop. A single transaction is bounded too: a prepare that
  would make its counted bytes on a store exceed `maxTransactionBytes` is dropped unsaved (section 6, Oversized), so
  a lone record always fits an entry.
- **C12 (sizing is recorded).** The stream size is fixed when a CRL is created; the stream count only grows, one
  stream at a time (5.11), and never shrinks. Both, with the ring order, are recorded in `crl/sizing`; an existing
  CRL always opens with them, whatever the device config says.

**Ownership**

- **C13 (one CRL per device).** Every store on a device saves to, recovers from and deletes from the CRL in
  `<device>/crl` (`CrashRecoveryLog.deviceCrlDir`).
- **C14 (queue order).** Every request is processed in the order it was queued. `readStoreState` sees every save queued
  before it, and `dropStore` drops exactly what was saved before it.
- **C15 (recovered status).** Disk recovery and `readStoreState` return the same status: the one last saved. It is
  stored in bits 3-4 of the static record's Paxos mask byte (`Tx.writeStatic`, `Tx.loadTx`, in
  `S/crl/simple/LogContent.scala`), and a recovered Tx restores a saved resolution from it (`Tx.oresolution`,
  transaction-recovery.md).

---

## 3. How the CRL fits into the transaction workflow

### 3.1 What is saved, and what each completion gates

All `crl.save` calls go through `Tx.saveState` (`S/transaction/Tx.scala`). The state saved is always
`TransactionRecoveryState(storeId, serializedTxd, trsObjectUpdates, disposition, savedStatus,
acceptor.persistentState)`, where `trsObjectUpdates` leaves out the data once the commits have persisted
(`saveObjectUpdates`) and any update removed by `persistWithoutUpdateOf`, and `savedStatus` is the Tx's status, except
that a resolution learned from the UnackedCommits tree (`resolvedFromTree`) saves `Unresolved`, so a restart looks the
entry up again ([transaction-recovery.md](transaction-recovery.md)).

| Event at the store | Save site | What `onDurable` does | What it protects |
|--------------------|-----------|-----------------------|------------------|
| Prepare answered, objects loaded | `Tx.answerPrepareAsAcceptor` | sends `TxPrepareResponse` (Promise or Nack, vote, collisions) and the vote to the client | promise, vote, txd, object data |
| Prepare answered before the objects loaded | `Tx.sendDelayedPrepare` | sends the delayed `TxPrepareResponse` | as above; the disposition is decided now |
| Accept | `Tx.receiveAccept` | sends `TxAcceptResponse` | accepted proposal and value |
| A frozen Tx (past expiry) holds a decided vote no save holds yet | `Tx.saveUndurableVote` | nothing sent; the save's completion sets `voteDurable` | the vote a later status reply reports ([paxos-transactions.md](paxos-transactions.md), expiry) |
| Object data adopted from a later prepare | `Tx.saveAdoptedData` | posts: releases a commit held for the data (`adoptedDataUnsaved`) | the data a commit will apply |
| An object this transaction allocated is being deleted, or a key its applied blind Insert wrote | `Tx.persistWithoutUpdateOf` | posts: lets the backend deletion proceed | a restart must not allocate the object, or Insert the key, again over the deletion (transaction-recovery.md, transaction-requirements.md) |
| Backend commits of the transaction complete | `Tx.allObjectCommitsComplete` | `dropTransactionObjectData`, nothing written, unless a backend commit failed (`keptForReplay`) | stop carrying object data |
| Finalized (or aborted, expired, forgotten) | `Frontend.concludeTransactionState`, `Frontend.deleteTransactionState` | `deleteTransaction` | end of the record |
| Finalized, and an UnackedCommits entry may name the store | `Frontend.recordAppliedStub` | `saveAppliedStub`; posts: releases the transaction's admission | the record is replaced by an applied stub (3.6) |

Consequences:

- **The vote and the promise travel together.** The first prepare response carries both, and both are in the state saved
  before it is sent. This is what makes the 1-RTT commit safe across a crash
  ([paxos-transactions.md](paxos-transactions.md)).
- **Nacks are saved too**, through the same path.
- **No decision record.** No save is made because a transaction resolved, but every save writes `savedStatus`, so a
  prepare or accept handled after a resolution learned through Paxos or a `TxResolved` persists `Committed` or
  `Aborted`, and recovery restores it: the commit is applied, or the abort's state deleted, once the objects load. Any
  other recovered transaction is `Unresolved` and is resolved again
  ([transaction-recovery.md](transaction-recovery.md)).
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
   one (`loadValidStoreState` returns `Left` for a file missing, cut short, unparsable or another store's) makes the
   store permanently unservable (`StoreRebuilds.crlSaveFileLost`). A read that fails without saying anything of the
   content (`CrashRecoveryLog.isUnreadable`: an I/O or VM error other than a missing file or bytes that do not parse)
   is thrown instead, and the load held back and retried as below. Whether there is one is asked of `server.FileStat`: a
   stat that fails is no absence, and the load is held back and retried with backoff (`awaitUnreadRetry`), as it is
   for an incarnation file that cannot be stat'd or read. Taken as absent, the store would recover from a device CRL
   holding none of its state, and the file, still there, would have the next load drop everything saved since.
3. With no save file, `StoreRouter.recoverDeferred` -> `readAndRecover` runs on the store's executor and calls
   `readStoreState`. It does not block: the Future completes on the CRL thread and recovery is routed back to the
   executor. The store holds its transaction messages until `Frontend.recover` has recreated its transactions, so it
   saves nothing in between. A throw while reading or recovering fail-stops the store (`Host.StoreFailStopped`),
   which moves its copy to `lost/` once its close ends (`StoreRebuilds.failStopClosed`), its entries kept for the rebuild's retire, except a
   read failed because the CRL itself died (`CrlFailed`) or was shut down (`CrlClosed`). That is the device's failure, not the store's: the CRL files still hold the store's state for
   a restart (5.10), so the store is left holding its messages, its copy where it is, for `DeviceShutdown.crlFailed` or the
   unload that shut the CRL down to close.
4. With a save file: its state is first charged to the device's admission, checked (`CrlAdmission.tryChargeLoaded`,
   section 6.1 A3); a state that does not fit now leaves the store offline with its file kept, retried at the device
   scans, and one that never can, even at the most streams any config allows (`StoreLoader.neverFits`,
   `maxAdmissionLimitBytes`), is treated as an unusable file (step 2). Then `crl.dropStore` drops whatever the device
   CRL holds for the store, durably, and `crl.loadStore` saves every record of the file into it
   (`StoreLoader.awaitCrlLoad`), then `StoreRouter.crlLoaded` removes the file durably on the store's executor (the
   unlink, then a force of the store directory) and only then recovers. A failed removal is a load that failed with its
   data intact: the store has recovered nothing, so it served nothing, and the device CRL holds exactly the file's
   records. `crlLoaded` fails the load and posts `SaveFileRemovalFailed`; `StoreLoader.handleSaveFileRemovalFailed`
   unloads the store keeping those entries (`closeUnloaded(dropDurably = false)`: if the unlink succeeded and only the
   force failed, they are its whole state) and retries its load at every device scan (its copy AwaitingRetry). The router
   records the file (`saveFileRemovalPending`), so the retry's load removes it again, forcing the directory even when
   the file is already absent, before the store recovers: a file a crash brought back after the store had served would
   be loaded over newer device-CRL state. A crash before the retry succeeds brings back at most a file whose records the
   device CRL already holds. A crash between "records durable in the device CRL" and "file removed" loads the same
   records again under the same ids, which is harmless: the store served nothing in between. A failed `loadStore`
   unloads the store (`handleUnloadFailed`), which drops whatever reached the CRL (`closeUnloaded(dropDurably = true)`):
   the file is still the whole state. The file is always the store's whole state (a load removes it, so the store has
   served nowhere since it was written), and the device CRL can hold only a copy of part of it (10.11, or a crashed
   earlier load) or entries an earlier tenancy of the store left on this device, whose copy left its path without a
   retire (removed by hand) before the safety net (`dropLeftoverCrlState`) could drop them. A rebuilt copy carries such
   a file too, empty, written by `RebuildingStore.writeEmptyCrlState`, from `finish` (rebuild-process.md 5.6), so its first load drops what an
   earlier copy left. Recovered as the store's own, such entries would rejoin transactions it has since forgotten, and a
   stale commit vote would find its own UnackedCommits entry (a transfer keeps the incarnation) and apply again. The
   drop first is why neither can happen.
5. Recovered transactions are counted into the device's admission without a limit check
   (`StoreAdmission.admitRecovered`), as are recovered stubs (`StoreAdmission.chargeStubs`). Only then is what was
   retained for the store released: the share the device's open, its last close, step 4's charge or a replaced store
   retained (`Frontend.recoverState`, section 6.1 A2).

### 3.4 Deletion is gated on the backend

A committed transaction's record is deleted at finalization only once its backend commits have succeeded:
`Frontend.transactionFinalized` defers to `txCommits` while commits are outstanding or failed, and
`transactionCommitComplete` concludes it when the last one succeeds. A failed backend commit keeps the record, with
its object data, for the life of the loaded store; a reload recovers the transaction from it and re-applies it
through the UnackedCommits entry its `TxCommitted` earned (`keptForReplay`).
This is the rule behind AS4.

A backend deletion of an object that a transaction allocated recently waits until no CRL state durably holds that
allocation's update (`Frontend.awaitAllocationsDurable`): an unconcluded transaction's state is saved again without
the update (`Tx.persistWithoutUpdateOf`), and a concluded one's deletion or stub only has to be durable
(`whenDurable`). A deletion of KeyValue keys (by a commit or a repair write) waits the same way for every recent
transaction that may have Inserted one of them blindly, with no required revision, and holds that update here as
applied (it issued its commit of the object, or voted commit: `Tx.holdsUpdateOf`) or has concluded
(`Frontend.awaitBlindInsertsDurable`, `blindInserts`; both indexes are `CrlStateRecords`): a recovered commit vote
re-applies an Insert of an absent key. Both saves drop the update from every later save of the transaction too
(`Tx.droppedUpdates`), and the update's piece leaves the image when that save's entry is built (5.3: a form's build
unpins the pieces it does not name).

### 3.5 Transfer out, drop, rebuild

| Operation | CRL calls | Source |
|-----------|-----------|--------|
| Planned device shutdown | stores close with `extractCrl = false`, records stay live; then `crl.shutdown()` | `DeviceShutdown.closeForShutdown`, `shutDownCrl` |
| Transfer out, store open | on the executor after `closing`: `readStoreState`; `saveStoreState` to `crl_save.log` (tmp, fsync, rename, directory fsync); `store.close()`; then `deleteTransactions(state.entryIds)` and the retained admission released | `StoreLoader.handleClose(extractCrl = true)` |
| Transfer out, store closed | on the transfer's source device only (the device the pool entry names, whose directory ships; another device of the host may hold an earlier copy of the store, whose entries are its retire's); with a valid `crl_save.log`: the file ships unchanged and `dropStore` removes the device CRL's entries (a copy of part of the file or an earlier tenancy's leftovers, 3.3 step 4), releasing the charge retained when the drop was issued; without one: `readStoreState`, saved, then `deleteTransactions`. Either way the release is of the charge retained when the extraction began (`releaseRetained(storeId, charge)`), never a later one. An unusable save file, or a stat of the directory or the file that fails (`FileStat`), fails it before anything is read; no open of the copy can start during the read, the deletion or the drop: the store's TransferOut work on the source device, from `startOut`'s close until the control task that moves the copy to `outbound/`, refuses every open of it (`StoreLoader.startOpen`, a load retry included), and the copy's phase keeps every later open off the store (only `live/` copies are loaded) | `StoreTransfersOut.extractUnloaded`, `closeForTransferOut` |
| Retire of a copy no longer its store's owner (a lost store before its rebuild, a transfer out's source, a re-placed rebuild's source, an earlier tenancy's copy at the path of a store transferring in) | refused (answers not clear, retried at a later check) while a retire of the store runs on the host, the copy is unsynced or its move to `lost/` awaits its retry, and for a path whose stat fails (`server.FileStat`); copy moved to `lost/` durably (unless it is there); `lost-crl-state.log` kept if present, else the copy's `crl_save.log` renamed to it (any entries `readStoreState` still returns saved first as `lost-crl-entries.log`), else `readStoreState` saved to it; `dropStore`; everything retained for the store released (`releaseRetained(storeId)`: a device holds at most one copy of a store, P2, and the retired one is in `lost/`, never loaded); copy moved into `pending-delete/` | `StoreCopies.retire` (from `StoreRebuilds.discardLostStore`, `StoreTransfers.retireEarlierTenancy` and the reconcile's `Retire` action, the Deleted Stores case) |
| Store no longer on the device (safety net: a retire drops first) | `storesWithEntries`; for each store not in the device's tree entry, with no served or opening copy and no copy (no copy record, or one whose path a stat answers absent, `HostCopies.existence`, `server.FileStat`: a stat that fails keeps them), no drop and no retire running: `dropStore` | `DeviceReconciler.dropLeftoverCrlState` |
| Store whose `loadStore` of its `crl_save.log` failed (the file stays) | `dropStore` | `StoreLoader.handleUnloadFailed`, `closeUnloaded(dropDurably = true)` |

The rule that makes this safe: **entries leave the device CRL only through a written deletion, and only after another
durable copy exists or the store has been made unservable on disk.** The transfer deletes after the save file is
durable (or, finding a valid save file already there, drops entries the file supersedes as a load would); the rebuild
drops after the copy is in `lost/` (and the `lost-crl-state.log` copy is durable). A store the Missing Stores case finds Active
with no directory or config is not dropped: nothing durable records that declaration, and a directory that comes back
before the flip commits loads with its state. It needs no drop of its own: a rebuild retires a copy that
reappears (`StoreRebuilds.retireForeignCopy`, which re-reads the store's copies from the disk first,
`HostCopies.reread`, and `StoreCopies.retire`; one back while the rebuild runs has the rebuilt copy discarded before
its flip, and the next pass retires it), and the rebuilt copy's empty
`crl_save.log` drops what is left at its first load (3.3, step 4). Every operation carries applied stubs with the
transactions: the save file holds both kinds of entry (`codec.StoreCRLEntry.applied_stub`, with the start timestamp),
and the deletions name `state.entryIds`.

The leftover drop is safe against a store landing on the device meanwhile: the directory and loaded checks run in the
control task that issues the `DropStore`, a store that lands has its directory before anything saves for it, and the
CRL processes requests in queue order, so the drop is processed before any save of the new store, and a save landing
in the drop's own entry cancels the drop's deletion of that id (C9). Its admission release covers only the records it
removes: it reads the store's retained charge in that same control task and releases it on completion only if it is
still that charge (`CrlAdmission.releaseRetained(storeId, charge)`, compared by identity: every `retain` and release
replaces the store's `CrlAdmission.Retained` value). A landing store's `crl_save.log` charge (A3) is merged into a new
value, so the drop releases nothing, and that store's recovery releases the whole value once it has counted its records
(A2): the landed records are never uncounted, and the leftover records' charge is released with them. A landing store
with no save file saves nothing before its recovery's `readStoreState`, which queues behind the drop; that recovery
releases the leftover charge, and the drop's later identity check then finds nothing to release twice.

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
- **Evacuation.** A stub has a zero-length `anchor` location at the end of the dynamic data of the newest entry that
  wrote it, so a rotation's evacuation carries it exactly as it does a transaction's data (`BuiltStub`, 5.5).
- **Recovery.** A stub is live unless a newer or same-entry stub deletion names it; an entry never holds both a stub
  and its deletion (`LogEntry.addStub`/`deleteStub`). The open keeps each stub's anchor in the newest entry holding it.
- **Admission.** Each stub is charged `CrlAdmission.AppliedStubBytes` (41: TxId 33, timestamp 8) without a check,
  and released when dropped; a closing store retains the charge (section 6).
- **Lifetime.** The store drops a stub at heartbeat once T is past the horizon plus the expiry margin and the store
  holds no Tx for T, waiting on a held commit or on its backend commits (`Frontend.expireStubs`,
  `AppliedStubs.dueForDrop`). Past that point a prepare for T creates no Tx (`Frontend.receivePrepare`
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
  nothing. Then `loadOwnedDevice` creates the device's `CrlAdmission` with `admissionLimitBytes`, registers its
  `raiseLimit` with `onAdmissionLimitRaised`, charges `recoveredBytesByStore` retained per store
  (`chargeRetained`), and subscribes `DeviceShutdown.crlFailed` to `crl.failed`, all before any store is offered
  for loading.
- **Duplicate copies.** After the open, a store with more than one copy outside `pending-delete/`
  (`DeviceDiscovery.retireDuplicates`, P2) has every copy moved to `lost/`; the one with the least copy id stays there
  and the others move on into `pending-delete/` with their files, unretired. The store's device-CRL entries stay live,
  counted as retained, until the remaining Lost copy's retire saves and drops them (3.5); the leftover drop skips them
  meanwhile, as that copy is recorded.
- **Refused, retried.** `CrlLocked` (another writer holds `crl/lock`) and `CrlSizingMismatch` (stream files without a
  readable `sizing` file, or not of the recorded count and size, or a `.crl.tmp` that is no unfinished creation, 5.1)
  are rethrown: the device stays unloaded, nothing is
  moved to `lost/`, every scan retries. A sizing mismatch is logged once; it means damage or a hand edit, never configuration.
  So is `CrlConfigInvalid`: `crl/` holds neither a `sizing` file nor stream files, and the device config cannot size
  a new CRL (`CrlSizingDefaults.resolve` refuses it); nothing is created. The config is held to `resolve` only then,
  so it can never turn the refusal of a damaged CRL into a lost one. An emptied `crl/` under stores is refused this
  way too while the config is invalid; once it is fixed, the open throws `CrlMissing` and the CRL is lost (below).
  An `IOException`, `UncheckedIOException` or `VirtualMachineError` (`DeviceDiscovery.isTransient`) is rethrown too,
  up to `CrlOpenAttempts` (3) consecutive device scans; an open outside a scan (host start, `CheckAllDevices`, a
  nudge) is retried uncounted.
- **Lost.** If `crl/` is absent from a device that holds stores (a copy in any phase directory but `pending-delete/`
  whose `config` exists, `CopyLayout.list`; a retired copy keeps its CRL state in its own files), or holds no
  stream files there (`openCRL` is passed `holdsStores` and throws `CrlMissing` before writing anything: stores exist
  only on a device whose CRL was opened, so a `crl/` emptied by hand, a file system repair or a partial restore is a
  lost CRL, never a new one), or the open throws anything else, or a transient error a third time in a row: every
  such store copy is moved to `lost/` durably (`CopyMoves.moveUnattached`), one by one, except one that held nothing in the lost
  CRL (`DeviceDiscovery.holdsOwnState`): its `crl_save.log` validates as its own, so its whole state is that file (3.3,
  step 4: a store with a save file has served nowhere since it was written, and its load drops what the device CRL
  holds for it), or it is in `rebuilt/`, a finished rebuild awaiting its flip that has never served
  (only `live/` copies are loaded), so its flip and load proceed as over the old CRL. Every existence
  test on this path is `server.FileStat`'s: a copy whose `config` cannot be stat'd counts as a store (missing it
  could take an emptied `crl/` for a new CRL), and a failed stat of a copy, of `crl/` or of
  what `holdsOwnState` tests, or a read of a store's config or save file that fails without saying anything of its
  content (`CrashRecoveryLog.isUnreadable`), throws before anything is moved (every store is decided before any is
  moved), leaving the device unloaded for the next scan,
  uncounted by `CrlOpenAttempts`. A move that fails or lands unforced throws the same way, and every copy in `lost/`,
  moved now or by an earlier scan, is forced (`CopyMoves.forceUnattached`) before `crl/` is touched, so a crash cannot undo a
  move to `lost/` over the empty replacement CRL. `crl/` is moved to
  `crl.lost-<uuid>` (kept deliberately, Backlog.txt); the device directory is forced; a fresh CRL is created in
  `crl.creating/` (`CrashRecoveryLog.replacementCrlDir`, `CrashRecoveryLogFactory.createCRL`, `SimpleCRL.create`: an
  empty directory, every file forced, nothing opened), renamed to `crl/` durably once finished (`DurableFiles.move`),
  and opened with `holdsStores`. A crash or a failed creation (ENOSPC while preallocating) leaves no `crl/`: device
  load removes `crl.creating/`, and the next load declares the CRL lost again, moving nothing aside. `CrlCorrupt` is
  logged under its own cause. The reconcile then flips each store whose copy is in `lost/` to Rebuilding.
- **Storeless devices.** A device with no store directory and no `crl/` has its CRL created in place. One with no
  store directory whose open throws is never declared lost: the error is rethrown and the device stays unloaded,
  retried at each scan, so no CRL is ever moved aside and replaced unless the stores it served were moved to `lost/` first.
- **Failed.** A CRL whose thread dies (5.10) completes `failed`; `DeviceShutdown.crlFailed` disowns the directory,
  closes the stores, gossips them offline and unloads the device until the host restarts. It is neither reloaded
  before that (a reopen loop would reach `CrlOpenAttempts`) nor marked Failed in the tree.
- **Unloaded.** `DeviceShutdown.shutDownCrl` runs `crl.shutdown()` on `CrlShutdownEc` and records the path in
  `LocalDevices.closingCrls` until it returns, so the device is not loaded again over open files.
- **Sizing.** The device yaml's CRL keys are optional (`StorageDeviceConfig`, `CrlSizingDefaults.resolve`, from the
  capacity of the file system holding the device): `crl-num-streams` 4 (also the minimum), `crl-max-file-size-mb`
  capacity / 2048 rounded down to 4 KiB, at least 64 MiB, `crl-max-streams` 20 (at most 1024) and
  `crl-min-free-percent` 5. The first two size only a new CRL; an existing one keeps the sizing in `crl/sizing` and
  logs a warning if the config asks another stream size or more streams than the ring has. The last two bound
  growth (5.11) and are read at each open (`CrlSizingDefaults.resolveExisting`): a `crl-max-streams` outside 4 to
  1024 or a `crl-min-free-percent` outside 0 to 100 disables growth for that open (`crl-max-streams` taken as 0,
  `crl-min-free-percent` as 100), with a warning.
- **Grown.** When admission is full the Frontend asks the CRL to grow (`requestGrowth`); a committed growth raises
  the device's admission limit through `onAdmissionLimitRaised` (`CrlAdmission.raiseLimit`, registered by
  `DeviceDiscovery`).

---

## 5. SimpleCRL, the reference implementation

Code: `S/crl/simple/`.

### 5.1 Files, creation and the lock

A CRL is `crl/sizing`, `crl/lock`, and one file `<k>.crl` for each `k` in `0 until n`, each exactly `maxSizeInBytes`
long. `crl/sizing` records the stream size, the CRL id and the **ring**: the stream numbers in the order they are
written (`Sizing.ring`, e.g. `ring: 0,4,1,2,3`). A growth inserts the next number after the active stream (5.11), so
the ring need not be in numeric order. "Next", "successor" and "after" below always mean ring order
(`Sizing.successor`), in rotation and in recovery alike. A sizing file whose ring is missing or not a permutation of
`0 until num-streams` is unreadable.

`SimpleCRL.apply` (steps 2 to 5 are `CrlDirectory`'s, `S/crl/simple/CrlDirectory.scala`, which also holds `Sizing`
and `SizingFile`, both re-exported by `SimpleCRL`):

1. `CrlLock.acquire` takes an exclusive POSIX lock on `crl/lock` before anything is read. Then, with `mustExist`
   (`holdsStores`) and no stream files, the open throws `CrlMissing` before anything is written. A second opener in
   another process gets `CrlLocked` from `tryLock`. A second opener in this process is refused by a registry keyed by
   the lock file's identity before it opens any descriptor: closing any descriptor of the file would drop the process's
   lock. The lock is released once the writer has closed the stream files (shutdown, a failed open, or death). It does
   not fence two hosts mounting one shared device.
2. `recordedSizing`: an existing `sizing` file gives the sizing and the CRL id (at least 3 streams of a multiple of
   4096 bytes, S1, and a `crl-id`, else `CrlSizingMismatch`; a new CRL asked for fewer or another size is refused).
   With no `sizing` file, stream files mean `CrlSizingMismatch`; no stream files mean a new CRL, whose requested
   sizing (its ring in numeric order) and a new random CRL id are written durably (`DurableFiles.write`) before any
   stream file. A `sizing` file with no stream files is a creation that crashed before them, and its sizing and id
   stand.
3. `removeUncommittedGrowth`: a `<n>.crl` or `<n>.crl.tmp` (`n` the ring size) that the ring does not list is a
   growth that never committed (5.11). It is removed, with a warning, but only when every listed stream exists and
   no other stream file is unlisted; otherwise the open is `CrlSizingMismatch` and removes nothing, since removing
   it beside missing streams could leave none and step 4 would then create an empty CRL under live stores.
4. `createStreamsIfNew`: with no stream files, each is preallocated as `<n>.crl.tmp` (`FilePreallocation.preallocate`:
   `fallocate(2)` mode 0 through FFM on Linux, zero-fill elsewhere or on `EOPNOTSUPP`; forced), renamed into place, and
   the directory forced. Only then does the writer start, so a creation that crashed wrote no entry and leaves each
   listed stream at most once, as `<n>.crl` or as `<n>.crl.tmp`, and no other `.crl.tmp`. It never leaves one under
   `mustExist`: a CRL is created in place only on a device holding no stores, and the replacement of a lost CRL is
   created apart (`SimpleCRL.create`, in `crl.creating/`) and renamed to `crl/` only once finished. Any `*.crl.tmp` left
   after step 3 is therefore taken for an unfinished creation only when `mustExist` is unset and every one is a listed
   stream's `<n>.crl.tmp` with no `<n>.crl` beside it: every `*.crl.tmp` and every stream file is removed, and creation
   starts over. Any other `.crl.tmp` (a stray beside its stream, an unlisted name, or any under `mustExist`) comes from
   damage, a copy or restore tool or a hand edit, beside a CRL that may hold entries: the open is `CrlSizingMismatch`
   and removes nothing, so the device is refused, not emptied.
5. `checkSizing`: the stream files must be exactly the set the ring lists, each of the recorded size.
6. `Recovery.recover` (5.7), then the constructor (5.7). A constructor that fails shuts its writer down and waits for
   it before rethrowing, so a retry never runs two writers over the same files.

The open's own file tests (`DeviceFactory.openCRL`, `CrlDirectory.streamFiles`, `recordedSizing`,
`removeUncommittedGrowth`, `CrlLock`, `StreamReader`) are `server.FileStat`'s: a stat that fails throws its
`IOException`, which `DeviceDiscovery.openDeviceCrl` retries as transient, and is never read as a missing file. Every
absence the open acts on (`CrlMissing`, a new CRL's creation, a growth's removal, an empty stream) is a known one. And
`createStreamsIfNew` itself throws `CrlMissing` under `mustExist` when it finds no stream files, so no stream file is
created, or renamed over a live one, on a device that holds stores, even if its check and step 1's disagree.

`StreamWriter` opens existing files only, with `DSYNC | WRITE`, and refuses one of the wrong size. Because the files
are preallocated and the file system overwrites in place (AS7), a CRL write never allocates, and a full device cannot
fail it with `ENOSPC`.

The streams form a ring. One is **active** and written append-only. When an entry does not fit, the writer advances
to the next stream, **recycles** it (new random stream UUID, write offset 0; in memory only), writes an evacuation
entry there first, and then the entry (`SimpleCRL.writeHead`, `Stream.recycleStream`, 5.5).

### 5.2 Entry format

Header (`LogEntry`, `StaticEntryHeaderSize` = 102 bytes, big-endian):

| Field | Bytes |
|-------|-------|
| stream UUID | 16 |
| CRL id (`SimpleCRL.Sizing.crlId`) | 16 |
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

A transaction's static record (`Tx.writeStatic`, `S/crl/simple/LogContent.scala`): TxId 33 (pool UUID 16, store
index 1, tx UUID 16), txd location 14, disposition 1, Paxos mask 1 + promised 5 + accepted 5, update count 4, then per
update object UUID 16 + location 14: 63 bytes plus 30 per update (`CrlAdmission.TxOverheadBytes`,
`ObjectUpdateOverheadBytes`). The mask has
bit 0 promise present, bit 1 accepted present, bit 2 accepted value, bits 3-4 status (0 Unresolved, 1 Committed,
2 Aborted). After `dropTransactionObjectData` the record is written with count 0.

A save of an existing transaction costs only its static record; its txd and data are written once and referenced by
location from every later record. Every entry is at least 4 KiB and every write is a `DSYNC`.

**Object data never passes for an entry.** Old cycles leave raw object payloads in the streams, and recovery probes
them at 4 KiB boundaries (5.7). `StreamReader.readHeader` takes a header for an entry only if it bears the CRL id
recorded in `sizing`, a random UUID chosen at creation that never leaves the device. A payload shaped like an entry,
whether crafted by a client or an innocent image of another Aspen CRL (a nested Aspen, a backup of `crl.lost-*`),
bears another id and is skipped like any other non-entry bytes. An image of this same CRL stored on its own device
does bear the id, but every entry in it is older than the CRL's newest: serials only rise, across opens too (the next
entry takes the last one's serial plus one). So it can never be mistaken for a newer entry. What remains is a forger
who has learned the id, which means read access to the device itself.

### 5.3 In-memory structures and threads

Three threads per CRL, and a fourth for growth when the CRL may grow:

- the **CRL thread** takes `ActionRequest`s from `ioQueue` and owns all mutable state: the live records
  (`transactions`, `stubs`), the durable image (`Image`) with its live-piece index (`LivePieces`), the unwritten
  entries (`sealedEntries`, `currentLogEntry`), the streams' offsets, and `afterInFlight` barriers;
- the **writer thread** (`StreamWriter`) takes `Write` commands, positions the channel and writes every buffer
  (`writeFully` loops, since a gathering write takes at most `IOV_MAX` buffers per call), then queues `WriteComplete`
  to the CRL thread (`Stream.writeEntry`). It runs no handler;
- the **completion thread** (`SimpleCRL.completions`) runs the handlers the CRL thread hands it at each
  `WriteComplete` (C2), so neither the next write nor the next entry's build waits for their network sends;
- the **growth thread** (none when `crl-max-streams` is at or below the ring size at open) runs a growth's slow
  steps off the CRL thread, so a zero-filling preallocation never delays a write (5.11).

The CRL keeps two views of each record (6.1, W1a and W3):

- **Live records** (`transactions` of `LiveTx`, `stubs`): the newest saved state of each, object data included even
  after `dropTransactionObjectData` (which only clears `keepObjectUpdates`). `readStoreState`, `dropStore` and
  `storesWithEntries` read them. A deletion removes a record from them at once.
- **The durable image** (`Image`): each record no built entry has deleted, in its **last-built form**, the form of the
  last built entry holding it (`BuiltTx`: a `TxForm` and its pieces; `BuiltStub`: start and anchor). A form
  (`TxForm`) is a save's immutable `TransactionRecoveryState` and whether its update data is kept. Only an entry's
  build changes the image (`LogEntry.createEntryBuffers`). A deleted record stays in it, pinned and carried by
  evacuations, until the entry holding its deletion is built.

A record's dynamic data is in pieces, each a `Piece`: a location and the serial of the entry holding it. A
transaction's pieces are its txd and each kept update, by index (`TxForm.pieceData`); a stub's is its zero-length
anchor at the end of the dynamic data of the newest entry that wrote it. A save's form either writes its pieces or
names the ones of the record's last-built form where they lie at its build (`TxSave.writesPieces`, 5.4). An update
count change writes the txd again too (W4), so all of a record's pieces lie in one entry, its home.

**`oldestEntryNeeded` and `LivePieces`.** `LivePieces` counts, per entry serial, the pieces of the image: exactly the
pieces of last-built forms are pinned. Its oldest serial is `oldestEntryNeeded` (the next entry's serial if it is
empty). At an entry's build, in order:

- the pieces it writes are located and their serials added;
- each form it holds becomes its record's last-built form, and the pieces of the form it supersedes that it does not
  name are removed (relocated by an evacuation, the old pieces of a count change, dropped updates);
- each deletion it holds takes its record out of the image and removes all its pieces;
- it stamps `oldestEntryNeeded`.

A removal from a serial no piece holds is an accounting fault that could unpin another record's piece:
`LivePieces.remove` throws, on the CRL thread, and the CRL fails (5.10). An evacuation's build also requires that it
carries the record's current last-built form, and a save naming pieces that its update count matches the last-built
form's unless its data is dropped (`createEntryBuffers`'s `require`s); either failing fails the CRL the same way.

Why it is safe: recovery walks from the last entry back to that entry's `oldestEntryNeeded` and reads every live
record's pieces only from the entries it walked (`Recovery.entryHolding`, else `BrokenEntryChain`). Every built record
has a piece, and a record's static record is in no older entry than any of its pieces, so the walk also reaches every
live static record. An entry is built only once every entry built before it is durable (one write in flight, built
in queue order), so the newest durable static record of every record a recovery from it can find live is the record's
last-built form, or the one this entry gives it, whose pieces are all pinned: the stamp covers them. A superseded
piece is unpinned only by the build of the entry that supersedes it; if that entry is torn at a crash,
recovery starts from the entry before it, stamped while the piece was still pinned, and finds it where the record's
newest durable static record names it. A deleted record is unpinned only by its deletion's build for the same reason:
with entries queued (W1), an entry built between the deletion request and its entry would otherwise stamp past the
record while the last durable entry still names it live (6.1, "Why W1a and W3 are needed").

### 5.4 Write path

`save` queues `SaveTransaction`. `onSaveTransaction`:

1. If any unwritten entry, the one in flight included, deletes the id finally (C9, W1b), queue only the handler
   (on the newest unwritten entry when a queued one deletes it; in `afterInFlight` when only the entry in flight
   does, run at its `WriteComplete`) and return.
2. Take the form: the state, with the update data kept unless the record's data was dropped.
3. Decide what it writes: its pieces when the record is new, when the update count changes (W4), or when it replaces
   a form in the same entry that was to write them; otherwise it names the pieces an earlier entry wrote or will
   write (FIFO, so that entry is built first). A form taken after `dropTransactionObjectData` writes no updates and
   is not a count change: it names its txd where the last-built form has it (W4's exception).
4. Add the form and the handler to the newest unwritten entry, sealing it first if the form would push it past
   `maxEntryBytes = S/2` (W1); `startWrite`.

Entries are frozen (W1a): an entry writes only the forms its own requests took, so a later save never changes what
an earlier entry writes or its size. Deletions, stubs and barriers go to the newest unwritten entry the same way. A
request's deletions may span entries, sealing as they go, with its handler on the last; a stub and the deletion it
makes stay in one entry. An empty entry takes any one request: a lone transaction is at most `T` (admission's
Oversized), and `T` plus an entry's fixed bytes rounds up to at most `S/2`.

`startWrite` writes the head, the oldest sealed entry, if no write is in progress; with nothing sealed, the newest
entry is sealed first if it holds anything. While a write is in flight, further requests accumulate in unwritten
entries: group commit, the only batching. `LogEntry.isEmpty` counts records and deletions, not handlers. An entry's
serial number, previous-entry link and offset are given when it is built, since evacuations may be written before it.

`Stream.writeEntry` builds the buffers (`LogEntry.createEntryBuffers`): header, then the pieces it writes (which are
located then), the static region (each record naming its pieces), the checksums, padding. The pieces are located, and
the image updated (5.3), before the header, which carries the `oldestEntryNeeded` stamped after them. The buffers are
built on the CRL thread when the write is issued; the writer only writes them.

### 5.5 Rotation (evacuation first)

When the head does not fit the rest of the active stream *X*, `writeHead` enters *Y* = successor(*X*) at offset 0,
recycles it, and first writes an **evacuation entry** carrying every image record with a piece in *Z* = successor(*Y*)
(or, by a bug, in *Y* itself: logged as an error and carried too). Each record is carried in its last-built form
(`TxEvacuation`, W5): its pieces in *Z* are written again, the others named where they lie. An evacuation writes no
pending state, no new piece and no deletion, and it is at most what *Z* held (6.1, Lemma A), so it fits the empty
stream. Once it is durable the head is tried again: in *Y* beside the evacuation if it fits, otherwise after another
rotation. With nothing of the image in *Z*, no evacuation is written and the head goes to *Y*'s offset 0. A head not
written after `n − 1` rotations in a row fails the CRL (W6, 10.5); one larger than a stream fails it at once.

**Invariant: the stream after the active one holds no needed entry.** The rotation into *Y* evacuates *Z*, the next
stream to be entered, so *Y* itself was emptied of the image by the previous rotation. Whatever part of a torn
evacuation write persists, it overwrites only entries no recovery needs: the entry before it, from which recovery then
starts, was stamped while the pieces it moves were still pinned in *Z*, and none in *Y*. After the evacuation's build
no pinned piece is in *Z* (a deleted record waiting for its deletion's build, and a record whose new form is still
unwritten, are in the image and carried too), so its `oldestEntryNeeded` lies in a stream written after *Z*, never in
*Z*. A piece found still in the entered stream itself would break the invariant; no path produces one: pieces are
written only into the active stream, so every piece in a stream is reached as "next" before the stream is entered.

### 5.6 Durability

- Every stream file is opened `DSYNC`: each `write` returns only after its data and the metadata needed to read it are
  on stable storage (AS1). No separate `force`, no `O_DIRECT`.
- One write in flight per CRL; throughput is about `entries/s = 1 / max(write latency, handler time)`: the next entry
  is issued as the previous one's handlers are handed to the completion thread, so the write and the handlers'
  sends overlap (C2). `BackendTiming` logs both, as the `crl write` and `crl handlers` stages.
- Preallocation with `fallocate` leaves unwritten extents; the first write to each converts it, a metadata change
  `DSYNC` commits with the write.
- `DurableFiles` (tmp, `force`, atomic rename, directory fsync) is used for `sizing`, the stream files' creation, and
  outside the log for `crl_save.log`, `lost-crl-state.log` and the `phase-param.*` files. Directory fsync is a no-op on Windows
  and not `F_FULLFSYNC` on macOS; Linux is the supported platform.

### 5.7 Recovery

`Recovery.recover`:

1. **Newest entry.** Streams are ordered by the serial of their head entry (offset 0) and the first with a wholly
   intact entry wins (`StreamReader.lastIntactEntry`). Within a stream, entries are followed from the head while their
   headers are intact, carry the stream's current UUID (read from offset 0) and have rising serials; only the last
   one's body is checked. A torn last entry is dropped with a warning and the entry before it is the last once its
   body is checked too: only one write can be torn, so a failure there is corruption and throws `CrlCorrupt`. The
   entry returned has always passed its body check, so the walk does not check it again. A stream whose only entry is
   a torn head (a rotation write in flight at a crash) yields to the stream before it.
2. **Nothing newer.** `newerIntactEntry` scans the active stream past the last entry, and all of the next stream, for
   an intact entry of any cycle with a higher serial, hopping intact headers by their size and anything else by
   4 KiB. An intact header bears this CRL's id (5.2), so object data in those regions is never a hit. A hit throws
   `CrlCorrupt`: a corrupt entry hides a newer one (mid-stream rot, or a rotted head hiding the
   real active stream).
3. **Walk.** From the last entry, follow `previous entry` links, loading each entry's static region into a
   `RecoveringState`, until reaching the last entry's `oldestEntryNeeded`. Each step must reach an entry of its
   stream's current cycle with a lower serial and an intact header and body, or `BrokenEntryChain`/`CrlCorrupt` is
   thrown; the last entry's body is taken as checked by step 1. The extents of each verified entry's dynamic data are
   recorded.
4. **Apply.** Entries are applied newest first: a record already present is not replaced; one named by a deletion of
   the same or a newer entry is skipped (C4, C9). Stubs follow the same rule against stub deletions.
5. **Data.** Each surviving transaction's txd and data are read from their locations, which must lie within the
   dynamic data of a verified entry (`Recovery.entryHolding`), so they are covered by its checksum. Each piece is
   returned with that entry's serial (`Recovery.RecoveredTx`).

The result also sets the active stream (with the **last entry's** UUID, so new entries remain readable from that
stream), the next offset and serial, and the previous-entry link (to the last entry itself).

**The open writes nothing.** The constructor gives each recovered record the pieces its newest static record names
and counts each in `LivePieces` by the serial of the entry holding it (a stub by its anchor in the newest entry holding
it), as if this instance had written them. The first entry's `oldestEntryNeeded` is stamped from them when it is built,
so it is no newer than any entry the recovery read a piece from. The last durable
entry kept the ring invariant (a torn rotation leaves the stream before it active, where it still holds), so the next
rotation's evacuation finds and moves them as the instance before the crash would have. A live set past 6.1's bound
therefore still opens; its next rotations fail the CRL at W6.

After a torn newest entry is dropped, the next entry reuses its serial and offset and overwrites it. If the torn entry
was a rotation head in the next stream, both that head and the new entry carry the same serial until the next
rotation overwrites the head; recovery ignores the head because its body fails.

Zero-filled regions are not entries: a zero header fails `header_crc`, and a zero stream UUID is rejected
(`StreamReader.readHeader`), so a never-written CRL opens empty at stream 0, offset 0.

### 5.8 Deletion

`deleteTransaction` records the TxId in the newest unwritten entry's deletions, removes the record from the live
records, and starts a write. The record stays in the image, its pieces pinned and evacuations carrying it, until that
entry is built (W3, 5.3). `deleteTransactions` and `dropStore` (`onDeleteTransactions`) give each id a transaction
deletion and a stub deletion (66 bytes per id) in the newest unwritten entry, sealing it when they would overflow it
(W1), with the handler on the last: it runs once every deletion is durable. The records and stubs are removed from the
live records. `dropStore`'s ids are those of the store held when the request is processed, and its transaction
deletions are cancellable by a later save in the same entry. An empty list (for `dropStore`, a store holding
nothing) is a barrier (`whenWritten`): it completes once every earlier request is durable, so every earlier deletion
of the store's ids, a transfer out's final ones included, is written before it completes, and a load's saves queued
behind it (`StoreLoader.awaitCrlLoad`) create records rather than being skipped behind a final deletion (W1b). `deleteAppliedStub` writes a stub deletion only
for a stub held in memory. A deleted transaction's save already in the same entry is taken out of it
(`LogEntry.deleteTx`, as `deleteStub` does for stubs); saves of it in older unwritten entries stay, are built first, and
the deletion
supersedes them. A save after a final deletion in any unwritten entry is skipped (W1b, C9).

### 5.9 Barriers

`whenDurable` queues a `Barrier`: if an unwritten entry holds anything, its handler is added to the newest one; else if
a write is in flight it waits in `afterInFlight` and is handed over at that write's `WriteComplete`, after the entry's
own handlers; else it is handed over at once. It runs on the completion thread in every case (C2). Every request queued
before the barrier is in an unwritten entry no newer than that one, in the entry being written, or already durable, and
entries are written in order, so this is exact.

### 5.10 Shutdown and death

`shutdown()` queues `Shutdown` behind earlier requests. The CRL thread lets the completion thread run every handler
already handed to it (durable entries are answered as such), then marks the CRL closed, fails every pending tracked
Future with `CrlClosed` and passes the shutdown to the writer, which drains the write in flight, closes the channels,
releases the lock and signals. The unwritten entries, sealed and newest, are discarded, as is the in-flight entry's
completion: none of their handlers runs (C3), so no vote follows. Every later tracked call fails with `CrlClosed`
(`closed` is set before the pending Futures are failed, and `tracked` adds its promise before checking it, the same
handshake as `dead`). So a caller waiting on a drop, a deletion or a read (a rebuild's discard, Rebuild work `DiscardingLost`,
`DeviceReconciler.crlDropsRunning`, a transfer's extraction) always hears back and frees what it holds; a drop or
deletion that failed this way is retried by a later check.

The CRL has one death path. A writer that throws (an I/O error) closes its channels and queues `WriterDied`, and a
completion handler that throws queues `HandlerFailed` (the rest of its batch does not run); the CRL thread throws
either. Any throw on the CRL thread (including a head not written after `n − 1` rotations, W6, and `Stream.writeEntry`'s
`require`) marks the CRL dead, fails every pending tracked Future (`readStoreState`, `deleteTransactions`, `dropStore`,
`storesWithEntries`, `loadStore`'s saves) with `CrlFailed`, shuts the writer down (releasing the lock), completes
`failed` and calls `onThreadDeath` once. Every later tracked call fails with `CrlFailed`. Handler-style completions
(`save`, `saveAppliedStub`, `whenDurable`) never run for an unwritten entry, so no vote follows. An entry whose write
was already in flight when the CRL thread died is still written, but its handlers never run, since only the CRL thread
hands them over. Handlers already handed over still run: their entries are durable, and the shutdown of the completion
thread lets them finish. The files hold what was written, so a restart recovers them as after a crash.

Shutdown and death stop growth first (5.11), before the lock is released.

### 5.11 Growth

A SimpleCRL grows by one stream of its recorded size at a time and never shrinks (`requestGrowth`, `GrowthPolicy`).
`StoreAdmission.admitPrepare` (through `Frontend.admitPrepare`) calls `requestGrowth` on every `Full`; the call is
cheap and idempotent, and wakes the growth thread unless an attempt is under way, the ring is at `crl-max-streams`
(refused at once, logged once), or a refused or failed attempt's back-off (`retryAfter`, 10 s) has not passed. With `n`
the ring size, the steps are, each durable
before the next:

1. On the growth thread, the policy is checked: the ring is below `crl-max-streams`, and the file system's free space
   less one stream stays at or above `crl-min-free-percent` of its capacity. Then `<n>.crl.tmp` is preallocated at
   the stream size and forced (`FilePreallocation.preallocate`).
2. On the growth thread, it is renamed to `<n>.crl` and the directory forced. `GrowthReady(n)` is queued to the CRL
   thread.
3. On the CRL thread, at a quiescent point (`commitGrowthWhenQuiescent`: no rotation under way between its
   evacuation entry and its head, `rotationsWithoutHead == 0`), `crl/sizing` is rewritten with `n` inserted right
   after the active stream *A* (`DurableFiles.write`: tmp, force, atomic rename, directory force). This rewrite is
   the **commit point**, and the last step that touches the files.
4. Then the writer opens the stream (`StreamWriter.addStream`), the CRL's ring and stream table take it, and the
   `onAdmissionLimitRaised` listeners get the new `admissionLimitBytes`, `(n − 1) · S/2 − S/16 − H · n`.

**Why the ring invariant holds after the insert.** The stream after *A* is now `n`, which holds no entry at all.
*A*'s old successor, now after `n`, holds nothing of the image: the rotation into *A* evacuated it, that rotation is
complete (step 3 waits for its head), and pieces are written only into the active stream *A*. So the rotation into
`n` evacuates the stream after it, which holds nothing pinned, and every later rotation enters a stream holding
nothing needed, as before. A rotation in progress is never re-aimed: between an evacuation and its head, the stream
after the active one may still hold pieces the evacuation in flight is moving, and W6's count must run over one
ring.

**Why Lemmas A and B hold for `n + 1` streams.** Lemma A is per stream and does not depend on the ring size. Lemma B for
the grown ring needs the live bytes within `n · (S/2 − H)`; they are within the old ring's `(n − 1) · (S/2 − H)`, since
everything was admitted under the old limit, until step 4 raises it, after the commit. From then on Lemma C gives the
bound for the new limit and Lemma B holds for the new ring. W6 counts `n` rotations from the next head on, over the new
ring.

**Open-time rules, which rest on the commit point being last.** A crash before step 3's rename leaves `<n>.crl.tmp`
or an unlisted `<n>.crl` beside a ring of `n` streams: the next open removes it (`removeUncommittedGrowth`, 5.1) and
opens the old ring. A crash after it leaves a listed `<n>.crl` that may hold no entry: it is all zeros, which
recovery reads as no entry, and it lies right after the stream that was active, as the invariant requires. Beside a
growth leftover, any other unlisted stream file or `.crl.tmp`, or a listed stream missing, is `CrlSizingMismatch`,
never removed (`removeUncommittedGrowth`). Without a growth leftover, an unlisted `.crl` or a missing listed stream is
`CrlSizingMismatch` (`checkSizing`), and so is an unlisted `.crl.tmp` (step 4 of 5.1); a growth never leaves
one, so the case arises only from damage. Because nothing touches the files after the sizing rewrite, there is no third
state.

**Failures.** A refused or failed attempt logs, removes what it created, backs off (a refusal for free space or a
failed step; an attempt that finds the ring at the cap ends without a back-off, as `requestGrowth` refuses those
at once), and never fails the CRL. A `GrowthReady` whose number is not the ring size is logged as an error and
ignored. A growth thread that dies of anything else logs it, and the CRL no longer grows until it is reopened. The
failed rename of a step-2 leftover `<n>.crl` from an earlier attempt fails that attempt, whose clean-up
removes it, so the next attempt succeeds. There
are two fail-stop exceptions, both after the new sizing file is renamed into place: its directory force fails and a
second force fails too (whether the new ring is durable is then unknown, so nothing may be written to the stream),
and `addStream` fails to open the committed file. Each throws on the CRL thread (5.10); a restart opens whichever ring
is on disk, both of which are consistent. A sizing file unreadable after a failed rewrite is fatal the same way.

**Shutdown and death.** They set `growthStopped`, interrupt the growth thread and take `growthFileLock`, under which
every growth step that touches the directory runs, before the CRL lock is released. The step in progress aborts (a
zero-fill or force with `ClosedByInterruptException`; a `fallocate` is one call), no later step runs, and the
leftovers are removed at the next open. A `StreamWriter` asked to write to a stream it does not know dies rather than
completing the write unwritten.

---

## 6. Admission and backpressure

`CrlAdmission` (`S/crl/CrlAdmission.scala`) is one atomic byte counter per device, with limit
`crl.admissionLimitBytes`, and `crl.maxTransactionBytes` bounds one store's transaction. For a SimpleCRL ring of `n`
streams of `S` bytes, 6.1 sets the limit SimpleCRL must give at `(n − 2) · S/2 − S/16 − H · (n − 1)`
(`SimpleCRL.admissionLimitFor`, `H = 118`): `15S/16 − 354` for the minimum of 4 streams, `9S − S/16 − 2242` for 20. The
per-transaction bound is `T = S/2 − 8192` (`SimpleCRL.EntryOverheadReserve`). 6.1 proves that, with the rules it lists,
these keep every entry writable, and it states the one assumption the proof
cannot discharge. W1-W6 and S1 are SimpleCRL's, A1-A4 the Frontend's and the store loader's; all are implemented.

- **New transactions.** `StoreAdmission.admitPrepare` (the store's share of the device's admission, owned by its
  Frontend and reached through `Frontend.admitPrepare`) admits a prepare for a transaction it has not seen only
  while the total is below its store's tier ceiling (`AdmissionTier`, the store's pool's, persisted in its StoreConfig):
  3/4 of the limit for data pools, 7/8 for metadata pools, the whole limit for the bootstrap pool. A data transaction's
  finalization writes trees in a metadata pool, and a metadata transaction's in the bootstrap pool, so lower-tier load
  never takes the headroom a higher tier's finalization needs (cross-document-review.md). It charges the description,
  object data and static record (`63 + 30 per update` + data). Otherwise the prepare is dropped silently
  (`Store.receiveTransactionMessage`) and the client retransmits. The check is against the total before the
  admission, so one admission may cross the ceiling by its own size, at most `T`.
- **Oversized.** A prepare that would make its transaction's counted bytes on the store exceed `T`
  (`crl.maxTransactionBytes`), with data already counted for an admitted one, is dropped the same way and charges
  nothing (`PrepareAdmission.Oversized`): no reply, no vote, no CRL write, and a warning, rate-limited
  (`Frontend.OversizedLogPeriod`, passed to `StoreAdmission`), naming the transaction and its bytes. The check is
  one comparison on the prepare path. The bound keeps a lone record's entry within `S/2` (W1), which Lemma B needs, and
  keeps any record from outgrowing a stream, which would fail the CRL at `writeHead`'s `require` on every retransmit. A
  transaction
  oversized on every store can never commit; its client sees `TransactionOutcomeUnknown` once retransmits stop
  (`client/internal/CLAUDE.md`). Data arriving after a data-less prepare that would cross `T` is dropped the same way,
  leaving the store as one that never received it: a commit elsewhere reaches it through missed-update repair, and
  the data-less admission is released by the usual paths. Nothing ties a pool's `maxSliceSize` to `T` yet
  (Backlog.txt).
- **Late data** of an admitted transaction is charged by `admitPrepare` only while the total is below the store's
  tier ceiling (`tryAdmit`), as a new transaction is, and the prepare is dropped as `Full` otherwise (A1). A
  data-less prepare of an admitted transaction charges nothing and is handled.
- **Charged without a check:** stubs (`StoreAdmission.chargeStubs`), recovered transactions (`admitRecovered`, its
  `chargeData` included: that data is already in the CRL), and at device open everything the CRL holds, per store
  (`recoveredBytesByStore`, `chargeRetained`). A loading store's recovery counts its recovered state and only then
  releases what was retained for it (`Frontend.recoverState`, A2).
- **Loaded state** (`loadStore`, from a `crl_save.log`) is charged before it is saved, in one check-and-charge
  (`CrlAdmission.tryChargeLoaded`) that succeeds only while the total plus its bytes, less what is already retained
  for the store, stays at most `limit + T`, and is retained for the store (A3).
- **Released** when the record is deleted (`deleteTransactionState`, `concludeTransactionState`), when a Tx is
  forgotten without reaching the CRL, or, for a transaction replaced by a stub, once the stub is durable. A stub's
  charge is released when it expires (`Frontend.expireStubs`, `releaseDroppedStub`). A store replaced by a reload
  moves all its admissions into what is retained for the store (`StoreRouter.install`, `Frontend.retainAdmissions`,
  `StoreAdmission.retain`), released once the replacing
  store has counted the same records (A2).
- **Retained.** A closing store keeps its bytes counted for it (`CrlAdmission.retain`) until its entries leave the CRL
  (`releaseRetained` after an extraction's deletion, a `dropStore`, a leftover drop of the charge it was issued for,
  3.5) or it loads again and counts its
  own. Records of stores that never load on this run are therefore counted from the open while they are evacuated.
- **Not counted:** entry headers, trailing UUIDs and padding, static records rewritten by later saves, deletions
  queued in an entry, and the second copies written when the update count changes. None of these is ever moved by an
  evacuation, so none of them needs room in the ring (6.1).

**Growth.** On `Full`, the Frontend also calls `crl.requestGrowth()`; a committed growth raises the device's limit
(`CrlAdmission.raiseLimit`, only upward) and with it every lane's ceiling, so the retransmitted prepare meets the
raised limit (5.11).

Admission is a liveness tool and the only thing that maintains C11. A failure of C11 fails the CRL (5.10), which
unloads the device until restart and leaves the files recoverable.

### 6.1 Why the limit keeps every entry writable

**Notation.** `S` is the stream size; `n ≥ 3` the ring size; `T = S/2 − 8192`; `H = 118`, an entry's fixed bytes
(header 102, trailing stream UUID 16). An entry whose records and deletions take `c` bytes is `H + c` rounded up to a
multiple of 4096. A stream's *used* bytes are the sizes of the entries written to it since it was last entered, at
most `S`.

**Rules.** The proof uses each of these. W1 and W2 are the rotation scheme; the others are what makes it provable.

SimpleCRL (implemented: `SimpleCRL.writeHead`, `entryFor`, `onSaveTransaction`, `LogEntry.createEntryBuffers`):

- **S1, aligned streams.** `SimpleCRL.apply` refuses a stream size that is not a multiple of 4096.
- **W1, bounded entries.** No entry other than an evacuation entry exceeds `maxEntryBytes = S/2`. Adding a save, stub
  or deletion that would push the newest unwritten entry past it seals that entry and opens another; a request's
  deletions may span entries, with its handler on the last; a stub and the deletion it makes stay in one entry.
  Sealed entries are written in order, one write in flight. Saves, stubs, deletions, barriers and handlers go to the
  newest unwritten entry. A lone record always fits: its counted bytes are at most `T` (Oversized), and `T + H`
  rounded up to 4 KiB is below `S/2`.
- **W1a, entries are frozen.** An entry writes only the forms its own requests gave. When a save is added, the entry
  takes a snapshot of the record: a reference to the save's immutable `TransactionRecoveryState`, whether its update
  data is kept, and the set of pieces it must write (none when the record's pieces are already written and its update
  count is unchanged). A later save of the same record goes to the newest unwritten entry with its own snapshot, and
  never changes an older entry's. At its build the entry writes its snapshots' pieces and static records, naming each
  already-written piece where it lies then (an evacuation may have moved it; the static record's size does not
  change). So an entry's size is fixed when its requests are added, and only shrinks after (a deletion that removes a
  same-entry save).
- **W1b, a deletion is final across every unwritten entry.** A save of a transaction whose final deletion lies in any
  unwritten entry, not only the newest, is skipped and creates no record (`LogEntry.deletesFinally`, 5.8). Its
  handler runs once that deletion is durable: on the newest unwritten entry, or, when only the entry in flight
  deletes it, through `afterInFlight` at that write's completion.
- **W2, evacuation first.** When the oldest unwritten entry, the *head* `h`, does not fit the rest of the active stream
  `X`, the writer enters `Y = successor(X)` at offset 0 and first writes an evacuation entry `E` carrying every image
  record (below) homed in `Z = successor(Y)`. Once `E` is durable, `h` is written in `Y` if `|E| + |h| ≤ S`;
  otherwise the writer enters `Z` the same way. With nothing homed in `Z`, no `E` is needed and `h` fits.
- **W3, the durable image.** Every record keeps its *last-built form*, the snapshot (W1a) of the last built entry
  holding it, apart from its current in-memory state, until an entry superseding it is built: the entry holding its
  next snapshot, or its deletion. Exactly the pieces of last-built forms are pinned in `LivePieces`. This extends
  5.3's rule for relocated and dropped pieces to deletions and update-count changes: a deleted record's pieces are
  unpinned when the entry holding its deletion is built, not when the deletion is requested, and until then an
  evacuation carries it like any other record.
- **W4, one home per record.** A snapshot whose update count differs from the record's previous snapshot writes the
  txd again too, not only the updates. So every record's image pieces lie in one entry, its *home*, which holds a
  static record of it no smaller than the last-built one. The one exception is a snapshot taken after
  `dropTransactionObjectData`: it keeps no update, so it writes nothing and names its txd piece where it lies. That
  keeps the one home (its only piece is the txd, in the home already), and its static record, `63` bytes, is no
  larger than any earlier one's, so Lemma A's per-record bound still holds.
- **W5, evacuation writes nothing new.** An evacuation entry writes, for each record it carries, its last-built form,
  naming the pieces that were in `Z` at their new locations and the others where they lie. It writes no pending
  state, no new piece and no deletion.
- **W6, fail-stop.** A head not written after `n − 1` rotations in a row means an assumption below was broken: the CRL
  fails (5.10) instead of rotating on. The files are recoverable as after a crash.

Frontend and store loader (implemented: `StoreAdmission.admitPrepare`, `Frontend.recoverState`, `retainAdmissions`,
`StoreRouter.install`, `StoreLoader.handleLoad`, `CrlAdmission.tryChargeLoaded`, `Tx.stateDeleted`):

- **A1, late data is checked.** Data arriving in a prepare for an admitted transaction is charged only while the total
  is below the store's tier ceiling (one `tryAdmit`), and the prepare is dropped (`Full`) otherwise, with no reply: the
  client retransmits. `admitRecovered`'s `chargeData` is exempt: it counts data the CRL already holds, which the
  retained charge counted until then.
- **A2, nothing live is uncounted across a reload.** A loading store's recovered state is charged before what was
  counted for its records is released. Everything counted for them is retained for the store: by its last close, the
  device's open, a loaded state's charge (A3), and, for a store the reload replaces, by `StoreRouter.install`, which
  moves the old store's admissions there on the shared executor (`retainAdmissions`). `Frontend.recoverState`
  releases it after its count. The replacing store's CRL read is issued from a task queued behind the install's, on
  the executor the old store saved from, so it follows every save the old store made; nothing reaches the old store
  after that task (comment at `StoreRouter.install`).
- **A3, loaded state is checked.** `loadStore`'s state is charged before it is saved, in one atomic check-and-charge on
  `CrlAdmission` (a compare-and-set loop like `tryAdmit`) that succeeds only if the total plus its bytes, less what is
  already retained for the store, is at most `limit + T`. The charge is retained for the store, so its recovery's own
  count replaces it under A2: a swap, not an addition. A refused load leaves the store offline with its `crl_save.log`
  kept and is retried later. A state that can never fit (its bytes above `L_max + T`, where `L_max` is the limit at
  `CrlSizingDefaults.MaxMaxStreams` streams, `maxAdmissionLimitBytes`, or one transaction counted above `T`, as a store
  moved from a larger CRL may hold) is not retried: its save file is treated as lost (`crlSaveFileLost`) and the store
  is rebuilt (`StoreLoader.neverFits`). `L_max` is not the limit at this open's `crl-max-streams`: a declaration is
  irreversible and a config edit is often rolled out fleet-wide, so a lowered cap, or an invalid one that disables
  growth (`resolveExisting`), must not make an intact state lost on every device that loads one
  (systemic-loss-protection S13). Both bounds follow only from the stream size the CRL recorded at creation. A state
  above `limit + T` but within `L_max + T` is refused as one that does not fit yet: it asks the CRL to grow and waits,
  warned, until growth, the config or free space allows it. The check leaves out what is already retained for the store
  (`retainedBytes`): what the CRL holds for a store with a save file is a copy of part of that file (an interrupted
  extraction or load), entries an earlier tenancy of the store left (3.3, step 4), or, for a load replacing a loaded
  store, that store's live records. Whichever it is, the load's `dropStore` is durable before any of the file's records
  is saved (`StoreLoader.awaitCrlLoad`), so the image never holds both: until the drop is built it holds the old
  records, covered by the retained charge, and none of the file's; after it, the file's records, covered by the loaded
  charge (Lemma C). A refused load is retried at each device scan (its copy AwaitingRetry, `StoreLoader.retryFailedLoads`) and asks the CRL
  to grow. Not implemented: transfer planning does not yet avoid a destination whose CRL cannot take the state; such a
  transfer ends in the never-fits rebuild at the destination (Backlog.txt, "Transfers do not check the destination's CRL
  room").
- **A4, no save without a charge.** A Tx saves to the CRL only while its transaction holds an admission record
  (`StoreAdmission.admitted`, or a retained or recovered charge). The one Tx in `transactions` without a record is an
  abort whose state `deleteTransactionState` deleted at resolution, kept until `TxFinalized` (`Tx.stateDeleted`). It
  saves nothing more: a prepare (`admitPrepare` admits it as a known transaction, charging nothing), an accept, or a
  prepare response delayed until its objects loaded is answered with a `TxResolved` of the abort to the proposer,
  never a promise or an acceptance; late data is not adopted; `Tx.saveState` requires it. This is sound for Paxos:
  the abort is the value already chosen, and an acceptor that promises and accepts nothing can help choose no other.

**Why W1a and W3 are needed even without rotations.** With entries queued (W1), an entry built after a deletion
request but before the entry holding the deletion would otherwise stamp `oldestEntryNeeded` past the deleted record's
pieces. A crash then recovers from an entry that names the record live (its deletion is not durable) while recovery's
walk stops short of its pieces: `BrokenEntryChain`, or, for a stub replacing a transaction, the transaction's state
lost before the stub is durable. The same holds for an update-count change whose new form waits behind a second
rotation: its old pieces in `Z` are needed until the new form is built. And an entry that built a record's current
state rather than its snapshot would write a form charged after the entry was sealed (late data arriving after a
release), and could grow past `S/2` after it was sealed.

**Definitions.**

- The *image* is the set of records no built entry has deleted, each in its last-built form (W3). Its pieces are the
  pinned pieces.
- A record's *footprint* `fp(r)` is the size of its last-built static record plus its pieces: for a transaction,
  `63 + 30u` (`u` updates, 0 once its data is dropped) plus its txd and kept update data; for a stub, 41. The
  *live bytes* `F` are the sum of `fp` over the image. They leave out headers, padding, stale static records, second
  copies and deletion records: an evacuation moves none of these.
- A record homed in a stream *lies in* it (W4).

**What admission counts.** For a transaction it counts `63` plus its txd plus `30 + data` for every object whose data
the store has seen (each object once, since a transaction updates an object at most once), and keeps that until the
release. Every form of the transaction updates a subset of those objects, so its count is at least the footprint of
any form it has had. A stub counts 41, its footprint. Counted bytes and `F` differ in four ways, each handled below:
`F` holds no header, padding or stale bytes (the ring analysis pays one `H` per evacuation entry; padding is absorbed
by 4 KiB rounding); records still in unwritten entries are counted but not in `F`; a deleted record whose deletion
is unwritten is in `F` after its release (Lemma C picks a moment when it was still counted); and stubs are charged
without a check.

**Lemma A (an evacuation entry fits an empty stream).** `|E| ≤ used(Z) ≤ S`.

*Proof.* Every piece is located by the entry that writes it (`createEntryBuffers`), and that entry also writes the
record's static record. So every record homed in `Z` has its home entry in `Z`, in `Z`'s current cycle (when the
writer last entered `Z`, nothing lay in it), and that entry holds all its pieces and a static record no smaller than
its last-built one (W4). `E` carries for each such record at most that static record and those pieces (W5), and no
deletion. Distinct records' bytes in `Z` are distinct, so `E`'s records take at most the content `Σ c_e` of `Z`'s
entries, and

    |E| = roundup4k(H + content(E)) ≤ roundup4k(Σ_e (H + c_e)) ≤ Σ_e roundup4k(H + c_e) = used(Z) ≤ S.

The bound is per record, so a record whose static record grew since its home was written would break it. W4 rules
that out: the static record of a transaction grows only with its update count (it is `63 + 30u` while data is kept,
`63` after a drop, which never reverts), and a count change rewrites every piece, txd included, into the entry that
builds the new count. Without W4 the lemma is false: fill `Z` with data-less prepares (txd only, static record 63),
then save each with `u` updates written in later streams. `E` must carry `63 + 30u` per record beside txds holding
roughly `50u` bytes of requirements each, about 1.6 times what `Z` held. Stubs (41 bytes, anchored where their
static record is) and dropped records (static record 63) never grow. ∎

**Lemma B (the ring takes the head within `n − 1` rotations).** If, when `h` does not fit the active stream `X`,
`F ≤ (n − 1) · (S/2 − H)`, then `h` is written within `n − 1` rotations.

*Proof.* Name the streams in ring order `X, Y_1, …, Y_{n−1}`. Rotation `j` enters `Y_j` and evacuates
`Z_j = Y_{j+1}` (`Z_{n−1} = X`). `Y_1` lies empty of the image: it was evacuated when `X` was entered, and pieces are
written only into the active stream. Each `Z_j` lies empty of the image once `E_j` is durable, so each `Y_j` is entered
holding nothing pinned (W3: a pending count change or deletion keeps its pieces in the image and they are carried
too). Between rotations nothing is built but evacuation entries, so `F` does not change: an evacuation rewrites the
last-built forms (W5), a deletion or save request changes the image only when its entry is built. Rotations 1 to
`n − 1` evacuate distinct streams, and a record carried into `Y_j` is not carried again before `n − 1` rotations, so
the records carried by `E_1, …, E_{n−1}` are disjoint: their footprints `c_j` sum to at most `F`.

`|h| ≤ S/2` at each placement attempt (W1). That is all the argument needs: it does not rely on `|h|` staying the
same from one attempt to the next. Rotation `j` fails to place `h` only if `|E_j| > S − |h|`. Both sides are
multiples of 4096 (`|h|` is an entry size, `S` a multiple of 4096 by S1), so `|E_j| ≥ S − |h| + 4096`, while
`|E_j| ≤ H + c_j + 4095`. Hence `c_j > S − |h| − H ≥ S/2 − H`. If all `n − 1` rotations fail,
`F ≥ Σ c_j > (n − 1)(S/2 − H)`, a contradiction. ∎

The bound needs only the head; entries behind it are each a new instance of the lemma. Every rotation writes an
evacuation entry that fits (Lemma A), so `Stream.writeEntry`'s `require` is never reached; a broken bound shows as
rotations that never place the head, which W6 turns into a failed CRL.

**Lemma C (what admission bounds).** At every build, `F ≤ limit + T + 41 · N`, where `N` is the largest number of
*unoffset stubs* outstanding at once: applied stubs saved for a transaction that holds no state in this CRL (a
`TxFinalized` naming the store owed for a transaction it never held or already deleted), or saved with
`keepAdmission` (the replaced state's charge stays with a newer Tx).

*Proof.* By A4 every save is preceded by a charge of the data it carries, held while the save is unwritten; a record's
charge is released only after its deletion (or its stub's save) is queued, so a release seen by an admission
precedes, in the CRL's queue, every save that admission allows.

*(1) A charge covers every image record at one moment.* Consider a build, and let `t_0` be when the last request
processed before it was queued. By W1a, an entry builds only snapshots its own requests took, all processed before it
was sealed; an evacuation builds snapshots an earlier entry built. A record built in an entry at or before this one,
with no deletion processed yet, was charged before the request giving its form was queued and is released only after
its deletion is queued, after `t_0`. A record whose deletion is processed but unbuilt (it is in the image by W3): the
deletion went to the newest unwritten entry, later than every entry built so far, so every snapshot built so far was
taken by a request queued before that deletion, so before its release. So at `t_g`, the earliest of `t_0` and the
releases of the records deleted but not built, every image record is charged, each by its own charge, at least its
footprint.

*(2) The covered total stays at most `limit + T + 41 · N`.* Let the covered total be the device total less each
stub charge whose replaced state's charge is still held, and less, while a store holds two charges for one set of
records, the one that covers no image record: under A2, the retained charge once its recovery has counted the same
records; under A3, the loaded charge until the load's `dropStore` is built (the image still holds the CRL's earlier
records of the store, which the retained charge covers, and none of the file's), and the retained charge after it.
Both totals the switch chooses between are kept within the bound from the charge on: the check bounds the total less
the retained charge, and leaving out the loaded charge leaves the total as it was before it. By event: a checked
charge (a new transaction, late data under A1) sees the total, which is at least either total above, below `limit`,
and adds at most `T`; a loaded state's check-and-charge (A3) keeps the total less what is retained for the store at
most `limit + T`; a replacing stub's charge is excluded
until its state's release, which then lowers the covered total by that state's charge (at least 63) less 41; an
unoffset stub adds 41 and its expiry removes it (`Frontend.releaseDroppedStub` queues the deletion, then releases);
a reload's swap changes nothing (A2); every other release lowers it.
So the covered total less `41 ·` (unoffset stubs outstanding) stays at most `limit + T`: each event keeps it, and at
the device's open the covered total is the footprint of the recovered image, the durable image of the previous run.
That run's last durable build bounded it the same way. A stub recovered at open, charged by `chargeStubs` with nothing
offsetting it, counts toward `N` if it was unoffset before the crash; one whose replaced state's release offset it
before the crash already lies within `limit + T` there, since that release lowered the covered total by more than 41.

*(3)* At `t_g` every image record is charged by a distinct charge: a durable replacing stub by its replaced state's
charge (at least 63, the state is out of the image), anything else by its own. So `F` is at most the covered total at
`t_g`. ∎

Without A1, late data is charged after any number of data-less admissions made below the ceiling: `F` reaches
`limit + k · T`. Without A2, other stores admit into the gap between the release of a loading store's charges and its
recovery's count, up to that store's recovered bytes. Without A3, a store moved from a device with a larger CRL can
hold more than this whole CRL. Without A4, a save after a release is uncharged. Without W1a, the counterexample above
puts late data charged after a release into an entry sealed before it.

**The limit.** With `limit = (n − 2) · S/2 − S/16 − H · (n − 1)`, Lemma C gives
`F ≤ (n − 1) · (S/2 − H) − S/16 − 8192 + 41 · N`, and Lemma B needs `F ≤ (n − 1) · (S/2 − H)`. So every head is
written within `n − 1` rotations whenever

    41 · N ≤ S/16 + 8192

that is, at most about `S/656` unoffset stubs outstanding, whatever the ring size: 102500 for 64 MiB streams, 1798
for 1 MiB. The limit charges one `H` for each of the `n − 1` evacuation entries a head may wait behind, so no pairing
of `crl-max-file-size-mb` and `crl-max-streams` (or growth up to `MaxMaxStreams`) can leave admission itself past
Lemma B's bound; the limit stays positive for every `n ≥ 3` and `S ≥ 4096`. In the form "live bytes at most
`limit + T + unchecked charges`", the unchecked charges this leaves are the unoffset stubs, and the room for them is
the `S/16` reserve plus the 8192 bytes `T` keeps back from half a stream (`SimpleCRL.EntryOverheadReserve`). The limit
is `15S/16 − 354` at 4 streams, above the `S/2` floor. A ring of 3 streams (accepted by `SimpleCRL.apply`, refused by
`CrlSizingDefaults.resolve`) is covered by the same proof: limit
`7S/16 − 236`, 2 rotations.

**What is not proven.** `N` has no structural bound: an unoffset stub cannot be refused (the store must remember that
it holds the transaction's update), and a device that drops prepares under load is exactly the one that later receives
owed `TxFinalized`s for the transactions that committed without it. The reserve makes the assumption quantitative, not
unconditional. Past the margin the CRL fails at W6; nothing is lost and a restart recovers it. Liveness after that
restart is an assumption too: the unwritten entries, deletions included, are gone, so the recovered image is the
durable one, and the bound holds again once `N` is back within the margin. Unoffset stubs expire at their
transaction's start plus the horizon and expiry margin (`Frontend.expireStubs`), so it is, unless new ones keep
arriving faster; until then each restart may fail again at W6. An unconditional bound needs a transaction-protocol
change that checks an unoffset stub's charge and leaves its `TxFinalized` unhandled when the CRL is full.

---

## 7. Sharing one CRL among a device's stores

All stores on a device share one ring, one writer thread and one admission counter.

- **Ordering across stores.** Entries interleave records of different stores; `readStoreState` filters by `StoreId`.
- **Deleting one store does not touch another.** `dropStore` names only that store's ids; others' records stay live
  and keep being evacuated.
- **A store being rebuilt.** Its records stay live until its retire (`StoreCopies.retire`) has saved them to
  `lost-crl-state.log` and `dropStore` completes; they are counted by admission from the open. Then the directory
  moves to pending-delete and the rebuild starts. A store the reconcile finds Active with no directory keeps them
  until its rebuilt copy's first load drops them (3.3 step 4), or until a re-placement takes its entry off the device
  and they become leftovers (3.5).
- **A store that fails.** A backend fail-stop at close moves its copy to `lost/`; its entries are dropped when the
  rebuild retires it. A transfer-out close had already moved them into `crl_save.log`, which the retire renames to
  `lost-crl-state.log`. A store whose `crl_save.log` is unusable is moved to `lost/` the same way.
- **A shared stall.** A slow `DSYNC`, a full admission lane or a dead CRL affects every store on the device. There is
  no per-store allocation by design.
- **Fate sharing.** A lost CRL moves every store copy on the device to `lost/` but those that held nothing in it
  (section 4, `holdsOwnState`); a failed CRL unloads every store.

---

## 8. Co-locating the CRL with the stores it serves

This is the only layout implemented (`CrashRecoveryLog.deviceCrlDir(devicePath) = devicePath / "crl"`).

### 8.1 Benefits

- **One failure domain and one unit of movement.** The device carries every store's data and its transaction state. A
  drive pulled from a crashed or running host is in exactly the state a crash would leave, so another host can adopt
  it without cooperation from the old host (`DeviceDiscovery.tryAdoptDevice`).
- **No cross-device ordering problem.** "Backend commit durable, then delete CRL record" (AS4) is within one device.
- **Simple recovery.** A restart finds one self-describing directory.
- **Sequential, append-only access.** SimpleCRL writes ascending offsets within one file at a time.

### 8.2 Drawbacks

- **IO doubled.** Every byte of object data is written to the CRL and again to the backend (RocksDB WAL and SST, or
  the FSIF journal and segment). An evacuation writes data still live a lap later a third time. Each entry is padded
  to 4 KiB and written with `DSYNC`. CRL traffic competes with store traffic for the same bandwidth and flush queue.
- **Flushes couple the workloads.** On a journaling file system an `O_DSYNC` write forces a journal commit and a
  device cache flush, which also drains the store's own pending writes, and the other way round. A busy backend
  raises CRL latency, which is on the critical path of every vote.
- **Wear.** On flash the ring (4 streams of capacity / 2048, at least 64 MiB each, by default) is rewritten
  continuously.
- **Rotating disks are hit hardest.** Each CRL commit is a synchronous write with a rotational delay and, because the
  CRL files are far from the backend's, a seek on the vote's critical path. Group commit hides some of it.
- **Capacity.** About 0.2% of the device by default (at least 256 MiB), growing up to 20 streams (about 1%) under
  load and never shrinking, not reclaimable by the stores; `crl.lost-*` directories add more.
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

"Safe" means no acknowledged state is lost and no wrong state is recovered, assuming AS1-AS7.

| # | Scenario | Safe? |
|---|----------|-------|
| 10.1 | Crash mid-write of an appended entry | Yes |
| 10.2 | Crash mid-write of an evacuation entry | Yes |
| 10.3 | Corruption of a durable entry | Detected; the CRL is lost if a needed or hiding entry is hit |
| 10.4 | Power loss with a lying write cache | No (AS1) |
| 10.5 | A head that does not fit, or a live set past the proven bound | Safe; past the bound the device is unloaded until restart |
| 10.6 | A slow or hung `DSYNC` | Safe; liveness only |
| 10.7 | A dead CRL thread or writer | Safe; the device is unloaded until restart |
| 10.8 | The device is pulled | Safe |
| 10.9 | Adoption by another host | Safe under AS3 |
| 10.10 | Crash during transfer-out, before `crl_save.log` is durable | Safe |
| 10.11 | Crash during transfer-out, after `crl_save.log` is durable | Safe |
| 10.12 | Crash during a retire | Safe |
| 10.13 | Stores sharing a CRL, one dropped while it is loaded again | Safe |
| 10.14 | CRL files unrecoverable at open | Safe for the CRL; every store rebuilt |
| 10.15 | Sizing mismatch, or another opener holds the lock | Safe; the device is refused |
| 10.16 | Device full | Safe except on copy-on-write file systems |
| 10.17 | Crash during CRL creation | Safe |
| 10.18 | Saves queued behind a full ring | Safe; they wait at most `n − 1` rotations |
| 10.19 | Crash during a growth | Safe |
| 10.20 | A transaction too large for one entry | Safe; it is dropped and never commits |

**10.1 Crash mid-write of an appended entry.** None of the entry's handlers has run, so nothing depended on it. If
its header did not persist, the walk from the head stops at the previous entry. If the header persisted but not the
whole body, `body_crc` fails and the entry is dropped (C7). The next entry is written over it.

**10.2 Crash mid-write of an evacuation entry.** The rotation into *Y* writes its evacuation entry first, at *Y*'s
offset 0, and *Y* holds nothing pinned (6.1, Lemma B), so a torn write there overwrites only entries no recovery
needs. If the head's header did not persist, *Y* keeps its old UUID and is not the newest stream; if it persisted with
a torn body, the head is dropped and *Y* yields to *X*. Either way *X* is active, the entry before the evacuation was
stamped while the moved pieces were pinned in *Z*, and the records still in *Z* are recovered from there. A deletion
or update-count change queued behind the evacuation is not durable, and the evacuation carried the record's
last-built form (W3, W5), so recovery returns that form. Tested by tearing each write of every rotation of a
three-lap run, the evacuation and then the head, at each sector prefix (SimpleCRLSuite).

**10.3 Corruption of a durable entry.** A bad checksum in any entry the walk visits, or in the newest entry's header
when a newer entry follows, throws `CrlCorrupt`; data is read only from verified entries. That includes the entry before
a dropped torn one: rot there beside a torn write is two faults, not one torn write (5.7). Rot exactly in the newest
acknowledged entry is indistinguishable from a torn write: that entry is dropped and its promises, votes or data are
lost, as with a lying cache. Rot in an entry recovery no longer needs still makes the CRL lost when it hides a newer
entry or lies in the walk. Both are accepted: the outcome is a rebuild, simple rather than optimized.

**10.4 Power loss with write caching.** `DSYNC` sends flush/FUA when the device reports a volatile cache. A drive that
acknowledges flushes it did not perform can lose acknowledged entries (Paxos amnesia); if it loses an entry that a
newer surviving one follows, recovery throws `CrlCorrupt`, otherwise it returns a shorter prefix silently. The same
drive breaks the backends too; it is an operational requirement (AS1).

**10.5 A head that does not fit, or a live set past the proven bound.** An evacuation entry always fits an empty
stream (6.1, Lemma A) and every other entry is at most half a stream (W1), so `Stream.writeEntry`'s `require` is
reached only by a bug. A lone record larger than a stream, which admission's Oversized keeps out, fails the CRL at
once (`writeHead`). With live bytes within 6.1's bound every head is written within `n − 1` rotations (10.18).
Past the bound, which only unoffset stubs beyond 6.1's margin can reach, the head is still not written after `n − 1`
rotations and the CRL fails (W6, 10.7) rather than rotating on: the unwritten entries were never acknowledged, so the
files hold a consistent CRL and a restart recovers it. Their deletions are lost with them, so the recovered live set
may be larger than before the failure; the reloaded stores delete those records again as their transactions
conclude. That the restarted CRL makes progress is an assumption: it holds once the unoffset stubs, which expire,
are back within the margin, and until then each restart may fail again (6.1, "What is not proven").

**10.6 A slow or hung `DSYNC`.** Entries are strictly serial, so every vote on the device waits. The pending entry
grows with arriving saves, bounded by admission for new transactions. Clients retransmit; transactions past their
timeouts are driven or aborted by others. No state is lost. A `DSYNC` that never returns blocks the device
indefinitely and `crl.shutdown()` with it (Backlog.txt, "Isolate every device's I/O").

**10.7 A dead CRL thread or writer.** Every pending and later Future fails with `CrlFailed`, no save completion runs,
and `DeviceShutdown.crlFailed` unloads the device until restart (5.10, section 4). Rebuilds and transfers on the device
fail their CRL calls and are resumed by the restart. No safety loss. A store whose recovery read the death fails is
not fail-stopped: it stays unrecovered, holding its messages, until `crlFailed` closes it with its copy where it is (3.3 step 3), and
the restart recovers it.

**10.8 The device is pulled.** Writes fail with an I/O error; the writer dies (10.7). Whatever was acknowledged is on
the device, which holds a consistent post-crash CRL: that is what makes adoption possible.

**10.9 Adoption by another host.** `tryAdoptDevice` requires `<device>/crl` and a directory for every store the tree
lists, commits an ownership transaction, then loads the device, which recovers the CRL (it writes nothing). The lock
keeps a second process on the same host or file system out. A running old host relinquishes the device when it sees
the tree name another owner (`DeviceShutdown.relinquishDevice`); on shared storage it may write until then (not
defended, by decision). A store whose copy is in `outbound/` with no valid `crl_save.log` is adopted as lost
(`DeviceDiscovery.adoptionClass`); one whose copy is in `lost/` is adopted Rebuilding. A listing or parameter file (`phase-param.*`) read
that fails, or a save file that cannot be read (`CrashRecoveryLog.isUnreadable`), fails the adoption for a later scan
to retry.

**10.10 Transfer-out, crash before `crl_save.log` is durable.** The state is still in the device CRL: `readStoreState`
left it live, and the deletion follows only the durable save. The copy moves to `outbound/` only after the
close completes. A restart loads the store normally and recovers from the device CRL. Safe.

**10.11 Transfer-out, crash after `crl_save.log` is durable, before the deletion is durable.** The records are in both
places, identical, because the store served nothing in between. The restart finds the save file and loads from it: the
load drops the device CRL's copies, `loadStore` saves the file's records again, the file is removed durably, then the
store recovers. If the deletion became durable first, the save file is the only copy; the destination validates the
staged copy before promotion (`loadValidStoreState`, while the source still holds the state). A resumed pool update of a
copy promoted earlier (`StoreTransfers.startIn`) checks it again: an `arrived/` copy whose `phase-param.arrived` names another transfer,
or whose save file no longer validates, is deleted (rename-first) and the transfer
started over, since the source holds the state until the pool update commits; a copy in any other phase is an earlier
tenancy's and is retired first (`StoreTransfers.retireEarlierTenancy`, 3.5).

**10.12 Crash during a retire (`StoreCopies.retire`).** The copy is moved to `lost/` (durably) before anything else,
so the store is never served. A crash before `lost-crl-state.log` or before the drop is durable leaves the entries live;
the next attempt saves (unless the file exists) and drops again. A crash after the drop and before the move finds no
entries (the empty drop writes nothing and completes once every earlier request is durable) and moves the copy into
`pending-delete/`.

**10.13 One store dropped while it is loaded again.** `dropStore` drops exactly what was held when processed (C14); a
same-id save in the drop's own entry cancels that deletion (C9). Other stores' records are untouched. The rebuilt
store comes back with no Paxos memory; it is muted for every transaction started before its `acceptor-since`
(rebuild-process.md).

**10.14 CRL files unrecoverable.** `openCRL` throws `BrokenEntryChain`, `CrlCorrupt`, a decode error, or an I/O or VM
error three scans in a row: `openDeviceCrl` moves every store copy to `lost/` (durably, one by one) except those
that held nothing in it (section 4), moves `crl/` aside, and creates a fresh one apart, renames it into place and
opens it. A crash between those moves and the CRL's move leaves copies in `lost/` and the old `crl/`: the next
open may succeed, but those stores stay unservable. The decision is irreversible and made from local evidence
only (Backlog.txt, "System-level protection against correlated lost store declarations": S13 covers transient,
configuration and version conditions).

**10.15 Sizing mismatch or lock held.** Thrown before any stream file is read; the device stays unloaded with its
files and stores untouched and each scan retries. Not a lost CRL.

**10.16 Device full.** The stream files are preallocated, so a CRL write never allocates, except on copy-on-write file
systems (AS7), where an `ENOSPC` kills the CRL (10.7). Freezing the device's writes instead is tracked in Backlog.txt
("Full storage devices").

**10.17 Crash during CRL creation.** `sizing` is written durably first; stream files go through `.tmp` and rename, and
the writer starts only after the directory is forced. `.crl.tmp` files at open restart the creation only when every one
is a listed stream's with no `<n>.crl` beside it and the device holds no stores; any other is `CrlSizingMismatch` (5.1,
step 4). A `sizing` with no stream files creates them. No entry was written in either case, and no store can exist on
the device yet: on a device holding stores either state is a lost CRL (`CrlMissing`, 4).

**10.18 Saves queued behind a full ring.** The head waits while evacuations run, unvoted: no handler of an unwritten
entry runs, so nothing depends on it. Each rotation evacuates one stream ahead of the writer, and with live bytes
within 6.1's bound the head fits beside one of the first `n − 1` evacuations (Lemma B). Meanwhile new prepares see
the admission total (section 6); late data is held back the same way (A1). A crash during the wait loses only the
unwritten entries, as in 10.1.

**10.19 Crash during a growth.** Before the sizing rewrite commits it, the new stream is a `.crl.tmp` or an unlisted
`.crl`, removed at the next open, which opens the old ring (5.1 step 3). After it, the new stream is listed, right after
the stream that was active, and holds no entry or only entries written after the commit; a torn first write there is
10.1 or 10.2. Recovery follows the recorded ring. Tested by crashing at each step (SimpleCRLSuite).

**10.20 A transaction too large for one entry.** A prepare whose transaction would hold more than `maxTransactionBytes`
on the store is dropped before anything is charged or saved (section 6, Oversized), so it cannot fail the CRL; a
client retransmit is dropped again. It never commits on that store. An entry larger than a stream is still refused
at `writeHead`'s `require`, which only a bug can reach.

---

## 11. Known limitations (accepted)

- Recovery runs on the host control thread and reads up to the whole ring, checksumming every walked entry (Backlog.txt,
  "Device CRL recovery currently runs on the host control thread").
- A hung device hangs its CRL and the host's shutdown of it (Backlog.txt, "Isolate every device's I/O").
- A lost CRL rebuilds every store on the device from local evidence, including after three transient open failures;
  `crl.lost-*` is kept but never used (Backlog.txt, "System-level protection against correlated lost store
  declarations", S13, S7/Q1).
- An `ENOSPC` on a copy-on-write file system kills the CRL rather than freezing writes (Backlog.txt, "Full storage
  devices").
- `CrlLock` does not fence two hosts mounting shared storage (systemic-loss-protection.md).
- Rot in the newest acknowledged entry is indistinguishable from a torn write (10.3).
- A transaction whose CRL bytes on a store exceed `maxTransactionBytes` (about half a stream) is dropped silently and
  can never commit there; its client sees only an unknown outcome (10.20).
- A loaded state above `limit + T` but within `L_max + T` waits offline for growth (A3), even past this open's
  `crl-max-streams`, until an operator raises that cap. If growth is refused for good (the file system's free-space
  floor, `crl-min-free-percent`), it stays offline, warned once a minute, until space is freed or an operator acts; it
  is not rebuilt. The same holds once the growth thread has died (5.11), until the CRL is reopened.
- Admission lanes reserve a quarter of the limit on devices with only data stores (Backlog.txt, "CRL admission lanes
  waste headroom on data-only devices"); same-tier finalizers (TKVL split, join, tail removal) still compete with
  their tier's traffic.

---

## Open weaknesses

(none)
