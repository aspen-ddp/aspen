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
  `server/store/CLAUDE.md`) are errors that prove nothing, so a read of an object whose allocation just committed
  never proves it deleted. Callers that drop or skip an object on absence (repair, rebuild, the allocation and split
  finalization actions) act only on the proof
- `ClientTransactionDriver.result` is a `ClientOutcome` (`Committed`, `Aborted`, `Unknown`). `SimpleClientTransactionDriver`
  stops retransmitting and completes `Unknown` once the client's clock is past `startTimestamp + horizon + MaxStartLead`
  (`TransactionHorizon.clientGiveUp`), checked at each retransmit tick. A transaction already that late at `begin()`
  fails fast with `Unknown` without sending. `TransactionImpl` maps `Unknown` to `TransactionOutcomeUnknown`.
  `TransactionManager` forgets a driver however it concludes (an outcome it hears, or `Unknown` it reaches itself,
  which no message would remove)
