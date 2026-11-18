### Client Internals
- Largely a collection of Managers and Drivers used to overcome message loss to and from DataStores.
  - Drivers track response state and re-issue messages to slow/offline stores until the individual operation concludes
  - Managers track the drivers and deliver messages to them as they arrive from the network
- There are two key Managers:
  - ReadManager - Ensures reads complete
  - TransactionManager - Ensures transactions complete
- Subpackages with their own guides: `transaction/` (builder, pin rule, missed-update marking), `allocation/`
- `ReadManager` releases a read the moment its result completes. A response that arrives after that is dropped
  (logged at trace), except that a late `StoreNotFound` still drops the sender's cached endpoint. Stale stores found by
  a read are not repaired by the read: repair is the server's job (`server/repair`). The client keeps no object
  cache, and a `TxPrepare` carries only the transaction's object updates (no pre-transaction rebuilds).
- A stale pointer or cached state of a deleted object can neither read nor revive it, even through a store that
  missed the deletion (a zombie slice): ids are never reused, every requirement on a missing or refcount-0 object
  fails `MissingObject`, and zombies are short of both thresholds (safety cross-document G7)
- A read chooses the revision `consistentRestoreThreshold` (always a majority) replies hold, whatever its timestamp,
  at object level (`BaseObjectReader.getStoreStates`) and per key (`KeyValueObjectReader.resolve`): timestamps are not
  monotone in commit order (a refcount or revision-less KV update raises a stale store's timestamp; blind key writes
  commit out of timestamp order). The restored timestamp is the max over the matching stores. A key is deleted only
  when no revision can still reach the threshold and no store holds the lock of any segment's revision (repair F12)
- `InvalidObject.provesDeletion` counts only `ObjectNotFound`/`ObjectMismatch` replies (or a consistent majority at
  refcount 0). `StoreNotFound` and `AllocationPending` (a commit voter that has not applied the object's allocation,
  `server/store/CLAUDE.md`) prove nothing. `BaseReadDriver` does not even hand either to the object reader: it
  re-reads that store at once (the store parks an `AllocationPending` re-read until the allocation applies or aborts,
  `Frontend.parkAllocationRead`). So a read of an object whose allocation committed waits for it to apply and never
  fails or proves it deleted, which a client that learned the commit from the votes (before any store applied it)
  needs to read its own new object. Callers that drop or skip an object on absence (repair, rebuild, the allocation
  and split finalization actions) act only on the proof
- `ClientTransactionDriver.result` is a `ClientOutcome` (`Committed`, `Aborted`, `Unknown`). `SimpleClientTransactionDriver`
  stops retransmitting and completes `Unknown` once the client's clock is past `startTimestamp + horizon + MaxStartLead`
  (`TransactionHorizon.clientGiveUp`), checked at each retransmit tick. A transaction already that late at `begin()`
  fails fast with `Unknown` without sending. `TransactionImpl` maps `Unknown` to `TransactionOutcomeUnknown`.
  Commit is also learned from the stores' `TransactionVote`s: `Committed` once every referenced object has
  `writeThreshold` `VoteCommit`s from its pool's stores (a store's first vote counts; a differing later one is logged
  and ignored). Abort votes never complete it: `Aborted` comes only from the leader's `TransactionResolved`/
  `TransactionFinalized`. After a vote-completion `SimpleClientTransactionDriver` keeps retransmitting until the
  leader's outcome arrives or the give-up point (`result` stays `Committed`; it logs no "HUNG" line meanwhile, and at
  the give-up point logs at info that the leader's outcome never arrived rather than warning "outcome unknown").
  `TransactionManager` forgets a driver on `ClientTransactionDriver.stopped` (leader outcome heard, or the give-up
  point; with no retransmits it completes with `result`), not on `result`, so a leader abort after a vote-completion
  reaches the driver and is logged as a split decision
- A transaction every store drops (each store's CRL `maxTransactionBytes` is below the transaction's bytes on it,
  `server/store/CLAUDE.md` Oversized; or every device stays full) gets no reply at all: no vote, no
  `TxPrepareResponse`, no leader outcome. `SimpleClientTransactionDriver` retransmits the prepare every
  `txRetransmitDelay`, logging `HUNG Client Transaction` at info every third tick, until the client's clock passes
  `startTimestamp + Horizon + MaxStartLead` (22 s). Then it logs a warning ("past the transaction horizon: outcome
  unknown"), completes `Unknown`, and the commit Future fails with `TransactionOutcomeUnknown`; nothing is recorded in
  the client's status cache. Nothing committed, but the client cannot tell this from an unknown outcome: retry
  helpers treat it as transient (`StopRetrying.unlessCollision`, `CollisionRetry.mayHaveCommitted` turns true), so an
  oversized transaction retried as is fails the same way on every attempt. Stores only warn, rate-limited, naming the
  transaction and its bytes. Keep each object's slice on any one store at or below 10 MiB (Architecture.md)
- `BaseReadDriver` re-reads a store whose reply holds a lock of a known-committed transaction at once; that `Read`
  carries `awaitCommit` (the known-committed txids among the locks) so the store can park the reply until it applies
  them (`Frontend.parkRead`, `server/store/CLAUDE.md`), so the re-read is answered once with the applied commit
  rather than spinning on the lock. A store that answers it anyway (cap, `ParkedReadWait`) is re-read again as before.
  First reads and full resends carry it empty
