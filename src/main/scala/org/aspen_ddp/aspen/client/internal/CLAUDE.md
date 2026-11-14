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
  a read are not repaired by the read: repair is the server's job (`server/repair`).
- `ClientTransactionDriver.result` is a `ClientOutcome` (`Committed`, `Aborted`, `Unknown`). `SimpleClientTransactionDriver`
  stops retransmitting and completes `Unknown` once the client's clock is past `startTimestamp + horizon + MaxStartLead`
  (`TransactionHorizon.clientGiveUp`), checked at each retransmit tick. A transaction already that late at `begin()`
  fails fast with `Unknown` without sending. `TransactionImpl` maps `Unknown` to `TransactionOutcomeUnknown`.
  `TransactionManager` forgets a driver however it concludes (an outcome it hears, or `Unknown` it reaches itself,
  which no message would remove)
