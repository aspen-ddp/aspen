### Transaction protocol
Store-local mechanics (locks, collisions, in-flight commits, admission): `server/store/CLAUDE.md`. Client-side
pin rule and finalizer entries: `client/internal/transaction/CLAUDE.md`. Safety arguments: `docs/safety/`.
Designs: `docs/superpowers/specs/2026-11-13-transaction-horizon-design.md` (forget floor, expiry),
`2026-11-09-replay-pins-design.md`, `2026-11-08-traceless-commits-design.md`. Safety argument for the horizon:
`docs/safety/paxos-transactions.md` 10.10

#### Lifecycle
- The client builds a TransactionDescription and sends it plus object data to the stores
- The stores check all local state against the TransactionRequirements and determine their Commit/Abort decision
- The CrashRecoveryLog is used to store this decision and Paxos is then used to arrive at an overall commit/abort decision
- Each store sends its vote to the originating client (`TransactionVote`) from the handler of the CRL save that holds
  it, alongside every prepare response (`Tx.sendPrepareResponse`; never for a muted or frozen Tx, nor an Undetermined vote)
- The client and all stores are informed of the final decision
- If committed, FinalizationActions are then run. Once complete, the CRL content is dropped and the transaction is forgotten.
- A Tx's object data is dropped from its CRL state once its commits persist, in memory only (`crl/CLAUDE.md`). Two
  store deletions first re-save the state without that object's update (`persistWithoutUpdateOf`, `droppedUpdates`;
  later saves leave it out too): a deletion of an object the Tx allocated, so a restart cannot allocate it again over
  the deletion (transaction-requirements 9.4; `Frontend.awaitAllocationsDurable`), and a deletion of keys the Tx's
  applied blind Insert wrote, so its recovered commit vote cannot Insert them again (10.5c;
  `Frontend.awaitBlindInsertsDurable`). See `server/store/CLAUDE.md`, Deletion after allocation and Key deletion after
  a blind insert
- Finalization failure path (`TransactionDriver.startFinalizer`): a finalizer that cannot be created, or whose
  `complete` fails, is logged at error and started again from scratch after a backoff (1 s doubling to 60 s) until
  one completes, the driver finalizes another way, or it shuts down. Never finalize without the actions: the marker's
  entries are what repair and owed stores rely on. TxFinalized's `owed` includes the planned stores of failed
  finalizers (`failedHandedOff`). An action that gives up must fail its `complete`, never leave it pending
  An action whose type cannot be created (user jar missing, version skew) is a failed action: the marker still runs,
  and the transaction stays unfinalized, alarmed, until a driver that has the type finalizes it (transaction-recovery
  10.3)
- Marker order (transaction-recovery 4.4; `RegisteredTransactionFinalizerFactory.Finalizer`): every action,
  the missed-update marker included, executes at once, so an action failing or retrying through an outage never keeps
  the missed stores unmarked. `complete` waits for every action, then fails with the first failure.
  An action whose type cannot be loaded, or whose factory throws, is a failed action (`Finalizer.of`), not a failed
  finalizer creation; so is one whose `execute()` throws (caught in `Finalizer`), so the marker is never orphaned
- Stall alarm (paxos-transactions 7.7): a committed transaction its driver has not finalized `FinalizationStallAfter`
  (60 s) after resolving it is a stall (`TransactionDriver.finalizationStall`; `Store.stalledFinalizations` is the
  metric). Each store logs one `FINALIZATION STALLED` error line a minute while it has any (`FinalizationStallReport`).
  Stalls are by design: an action failing every attempt, a metadata outage blocking the marker (transaction-recovery
  10.3), an entry too large for a tiny node (repair-process 13, large UnackedCommits entries)
- If the refcount of the object is set to zero, it is deleted

#### Paxos
- Transactions are initiated by the client but the Paxos participants are the DataStores hosting only one
  of the targeted objects. This is done to reduce the number of participants and does not affect correctness.
- The Paxos algorithm itself is unmodified but some clever message handling is done to reduce the number of
  round-trips and optimize data flow. The details are only important when working on transaction message
  handling.
- The CRL stores the persistent state Paxos requires (`server/crl/CLAUDE.md`)
- The driver tallies each object's votes from its own pool's stores. `Undetermined` (a Tx resolved before it voted)
  is no vote: it counts neither as a reply nor as an abort. A round whose votes cannot decide proposes an accepted
  value its promise quorum reports (paxos-transactions 7.2)
- Every object is classified (paxos-transactions 7.2): commit-certain (`c >= t`), abort-certain (`w - a < t`) or
  undecided. Any abort-certain object decides abort at once; all commit-certain decides commit. An undecided tally only
  waits (silence may be transient): a driver never proposes abort on it, since a client may learn commit from stores'
  votes. Expiry ends such a Tx (the expiry rules A-G below). A proposed abort (abort-certain tally) is an ordinary Paxos
  proposal (paxos-transactions P7a): an accepted value in the quorum still wins
- Proposer ids are pool indexes, so only the primary pool's stores propose (`TransactionDriver.mayPropose`). A driver on
  any other store is learn-only: it sends prepares and resolves, proves late and finalizes from what it hears, but sets
  no proposal and sends no Accept. A Tx counts prepares and heartbeats as driver activity only from a primary-pool
  sender (`Tx.fromProposer`), so a learn-only driver's retransmits never stop a primary store taking over an idle
  transaction; with no primary store reachable, the Tx ends by expiry (paxos-transactions 7.6)
- Only the designated leader's first driver, the one its client's prepare creating the Tx starts
  (`Store.driveTransaction(initialProposal = true)`), keeps the initial proposal `(1, L)`. Every other driver starts
  at round 2 or higher, above the highest id its own acceptor promised or accepted (`TransactionDriver.Start`,
  `Tx.highestProposal`; paxos-transactions 7.5)
- The local proposal is sticky across rounds (votes are immutable), so a later round skips the tally: its first
  prepare quorum queues the Accept at once (`queueAcceptMessages`, `acceptQueuedThisRound`; paxos-transactions 7.3)
- A conflicting second outcome (split decision) is logged as an error and counted in the host status cache's
  `splitDecisions` (`TransactionStatusCache.recordSplitDecision`) by the cache itself, the Tx (`receiveResolved`,
  `receiveFinalized`) and the driver (`Start.onSplitDecision`); each keeps its first outcome, and a driver that resolved
  commit ignores an abort `TxFinalized` (P19). A store drops a driver on `TxFinalized` only if the driver is
  then finalized (`isFinalized`); one that ignored it stays until its own `TxFinalized`

#### Proofs and late resolution
- A prepare for a transaction a store does not know, whose update an object there already shows (its revision, or
  a key's for a KeyValueUpdate without a required revision: `RequirementsApplyer.appliedBy`), proves it committed.
  The store answers `TxResolved(committed, proof = true)` (`Tx.proveCommitted`) for its driver only and applies
  nothing, records nothing in the status cache, and casts no commit vote. A driver holding a proof never proposes
  abort (an abort Paxos already accepted still resolves). A resolution reached after the transaction was forgotten
  is late (`late = true`; the cached outcome is finalized): the driver runs no finalization actions, and a store
  applies it only when it holds its own commit vote and its UnackedCommits entry names the store's own incarnation
  (UnackedCommits below). Everything else (abort vote, prover, muted Tx) applies nothing and sends no TxCommitted.
  A proof plus a memoryless post-proof quorum resolves late only while T is inside the horizon by the driver's
  clock (`TransactionDriver.Start.pastHorizon`): an expiry commit is accepted by no acceptor, so its unfrozen holders
  answer memoryless promises while its finalizer runs, and a late outcome would cancel that finalizer. It exists only
  past horizon + expiry margin by the decider's clock, and the 8 s margin covers inter-host skew
- Driver prepares are prefer-abort (`fromClient == false`): a Tx with no vote and no client prepare never checks
  requirements on one. It proves at once if it can (`appliedBy`), else withholds its whole response until the data, a
  client's prepare, or the outcome arrives, or until `Frontend.MissingDataWait` (5 s) passes: then it votes abort
  (`NoClientPrepare`). The 5 s run from the Tx's creation, not a client retransmit (paxos-transactions 6.2). End to end:
  ReplayIntegrationSuite's driver-first test, whose leader driver retransmits (`TestNetwork.HostOptions.leaderRetransmits`;
  by default TestNetwork's leader driver never sends prepares). A Tx recovered without a
  vote does not wait for a driver: it votes abort and promises at once (paxos-transactions 6.2), but stays undecided
  after load so a client retransmit that arrives first is still checked. This holds for data-less transactions too, so
  their late resolution may take 5 s longer. A vote
  given stands, but later data is still used to apply a commit. Adopted data is saved to the CRL first: the commit
  waits for that save (`Tx.adoptedDataUnsaved`, `saveAdoptedData`; P13), its objects unsettled for repair meanwhile
  (`Frontend.adoptedDataSaves`), since a `TxFinalized` during the save drops the Tx from `transactions`. A delayed duplicate client prepare of an
  aborted transaction cannot commit it: every store that took part remembers the abort for longer than it would join T,
  and refuses T past its forget floor (below)
- Description validation (transaction-requirements 2.5): a client-prepared Tx checks its description once,
  before its requirements (`DescriptionValidator`, `Tx.invalidDescription`). A refusal votes abort for every object
  (`InvalidDescription`, a non-object error, never waited out) and logs a warning. The check covers its pool's IDA
  (`Frontend.poolIDA`), the primary object and leader, one update per object, and the pin rule. It runs at vote only,
  never at the apply re-check
- Muted prepares at rebuilt stores (`acceptor-since`) and for start timestamps ahead of the host's clock
  (`Frontend.MaxStartLead`): `server/store/CLAUDE.md`
- Transaction horizon (`common.transaction.TransactionHorizon`, 20 s on `startTimestamp` against the host's wall clock):
  a prepare for a transaction a store does not hold whose start is below its forget floor (`Frontend.forgetFloorMillis`:
  now - horizon, its load + `MaxStartLead`, the cache's eviction watermark) is refused outright, no Tx, even when the
  host's status cache holds T resolved but unfinalized (another store's Tx may have written it after this store forgot
  T; a finalized entry has the prepare ignored); details `server/store/CLAUDE.md`. So no driver ever counts the fresh
  state of an acceptor that forgot T, and a commit is never re-decided as abort nor an abort as commit. Proofs and late
  resolution still work inside the window but are no longer load-bearing

#### UnackedCommits
- Written by the finalizer for each commit voter that sent no TxCommitted, or whose TxCommitted set `keptForReplay`
  (`Tx.keptForReplay`: a failed backend commit keeps T's CRL state past TxFinalized, and only the
  entry makes the reloaded store's recovered vote, which learns T late, re-apply it; `client/internal/transaction/CLAUDE.md`);
  an entry durable means T committed, not that the other actions are done (they may be failing and retried): only an
  acceptor having forgotten T (on TxFinalized) implies that
- A recovered unresolved commit vote looks its entry up (`UnackedCommitLookup`, from the
  Host) once loaded and again before accepting any abort. It is driven at once (no driver only while the CRL loads,
  `Frontend.recoveryPending`): under the forget floor no store that forgot T answers a driver, so Paxos cannot choose
  abort over a chosen commit (paxos-transactions 6.4, 10.10). A found entry proves the commit to its own
  driver, at its start or, if one already runs, through `Frontend.onCommitProven` -> `Store.giveProof`; the Tx applies
  its saved update locally, logs
  it at info and stays until TxFinalized, writing no status-cache entry (a Tx holding a late commit that finds its
  entry applies and is forgotten at once). A failed or unanswered lookup holds the
  abort; a failed one is retried at heartbeat, an unanswered one awaited, never re-issued (its untimed read would
  only pile up beside it; logged after `Frontend.UnackedLookupTimeout`, 5 s). Past the horizon its peers refuse its
  driver, so such a Tx ends by expiry (below)
- The apply decision (`Frontend.unackedLookupComplete`; the lookup answers `UnackedLookupResult.Absent` or
  `Found(incarnation)`, `Host.unackedLookup` decoding the entry): `Found(Some(own))`, naming `Frontend.incarnation`,
  and `Found(None)` (a driver that did not count the votes never heard from the store, or heard two incarnations from
  it) apply through the entry (`Tx.unackedCommitFound`; `None` logged at info). Every Tx that looks up holds its own
  commit vote, which may be the counted one the commit relied on: with `t = r` (Replication(3, 2)) skipping it can
  leave T below the read threshold, beyond reconcile. A vote that did not count is a replay's, which passed T's pin,
  so T is what the store lacks. A recovered vote's re-check failure there is an update already applied, and an
  Insert of an absent key is one never applied (`server/store/CLAUDE.md`, Key deletion after a blind insert).
  `Found` of another incarnation (a rebuilt store, Rebuild N3) logs both at info and calls
  `Tx.unackedCommitNotOwed`: T committed, and nothing is applied through the entry, held
  aborts dropped and never accepted. A recovered or expiring commit vote does as for a matching entry, applying
  nothing (`notOwedHere`, as with an applied stub: its objects in `objectCommitErrors`): it resolves committed,
  records nothing in the status cache, proves the commit to its driver and stays until TxFinalized, since T's
  finalization may still be running (the marker writing other stores' entries). A held late commit, or a vote whose
  expiry Discard was decided, concludes at once (`applyNothing`, a real late outcome exists). Neither writes a stub of
  its own (`applyNothing` still writes one already owed: `stubDue`). A forget-lookup (`forgetLookupHeld`, T already
  applied here) writes its stub as for any found entry; one still unanswered once T is past the horizon plus the
  expiry margin is forgotten as finalized without a stub (`Tx.forgetWithoutStubPastExpiry`, from
  `Frontend.expiryHeartbeat`; paxos-transactions 10.10): the floor then refuses every new Tx for T, so the stub could
  suppress nothing. A found entry of any incarnation is never taken as absent. The entry is left for the repair scan,
  whose reconcile installs the committed state
- A recovered Tx whose saved status is resolved (re-saved by a prepare or accept after the resolution) restores it
  into `oresolution` and needs no lookup. A restored commit with a commit vote relocks (`Tx.lock`: locks and slice
  reservation, no check) and counts as relocking (`Frontend.recoveringTransactions`), holding them until `doCommit`
  releases them: its backend commit may not have completed, and without them a restored non-voter could apply first
  and fail its re-check, which `recoveredApplied` would take as applied, and reads and repair would see no lock for an
  update not yet applied (transaction-requirements R13). Any other restored resolution takes no locks: once its
  objects load and every relocking recovered commit vote has relocked (`Frontend.recoveredTransactionLoaded` defers
  its `completeLoad`, so lock order sees every holder) it applies the commit with the recovered re-check rules, or
  deletes the abort's state. Either then waits for TxFinalized as a live resolved Tx does.
  A resolution learned from the tree (`unackedCommitFound`, `unackedCommitNotOwed`) saves `Unresolved`
  (`Tx.savedStatus`): restored as resolved it would seed the host cache with a non-finalized `Committed` for a T its
  peers forgot and lose `notOwedHere`; restored unresolved it looks its entry up again (transaction-recovery 7.2)
  Until finalized, its status replies report the saved status too (`Tx.statusForExpiry`: `Unresolved`, with its
  vote and accept), so no peer's expiry adopts a non-late commit from it; peers decide by the tally
- Coverage: entries exist only for unacknowledged counted voters (every silent store when the voters are unknown).
  These reach the write threshold, so uncounted voters and non-voters are repaired by their ErrorTree marks alone
  (transaction-recovery 5.3). Any narrowing of entries must keep this rule

#### Expiry (transaction horizon design, section 2)
- A Tx still unfinalized past the horizon plus the expiry margin (`Frontend.expiryMargin`: `MaxStartLead` +
  `Host.txHeartbeatTimeout` + one heartbeat; T's start against the host's wall clock) is `expiring` (`Tx.mayExpire`):
  unresolved, or resolved and waiting for a TxFinalized that its host missed for longer than the caches remember T
  (resolved by Paxos, or applied from its UnackedCommits entry: `provesCommitToItsDriver`). At store
  heartbeat (`Frontend.expiryHeartbeat`) it sends `TxStatusRequest` to every other store of T, at once, then after 1,
  2, 4 and every 8 heartbeats. Each round has a fresh `requestUUID` and counts only its own replies, plus the Tx's own
  acceptor state (a muted Tx holds none)
- Freeze (`Tx.frozen`, paxos-transactions 10.10): once T is past expiry by the store's own clock, its Tx promises and
  accepts nothing more, as if offline, which Paxos allows. Derived from the clock, so a reload cannot thaw it. An
  unresolved Tx answers a status request only once frozen with its objects loaded (or muted for good), so every state a
  tally counts is final: a vote never changes and nothing more is accepted. A frozen Tx decides no vote
  (`determineDisposition` returns at once while T's outcome is unknown here and it is not muted) and sends no delayed
  prepare response, so a frozen Tx still undecided (an in-flight commit, a collision, a late client prepare) stays a
  non-voter for good. Before that it is silent
- A reply (`TxStatusResponse.TxStatus`) carries the outcome if known, plus `accepted` (the latest accepted value),
  `votedCommit` (a commit voter holds T's update for its objects; reported, and counted in a Tx's own round, only
  once a CRL save holding the vote is durable, `Tx.voteDurable`: rules A and F rely on the vote's locks surviving a
  crash. An unresolved commit voter whose save is in flight is silent, to its own expiry round too
  (`Tx.ownExpiryStatus`), so that round holds rather than discard a commit by D or E; a vote decided just before the
  freeze whose response is dropped is still saved, `Tx.saveUndurableVote`) and `amnesiac` (a rebuilt store, for T started
  before its `acceptorSince`, `Frontend.mayHaveForgottenUndecided`: no state, but its old incarnation may have
  accepted T). A silent store counts as possibly having accepted commit and possibly holding the update
- The pure `ExpiryTally.decide` rules, in order (client 1-RTT commit design, section 4.3(b); proofs in
  `ExpiryTally.decideUnknown`'s scaladoc, checked exhaustively by `ExpiryTallyConsistencySuite`). Two lemmas carry
  them, both from immutable votes and from drivers proposing abort only on an abort-certain tally: **L1** every
  accepted abort traces back to an abort-certain tally (so some object never had `t` commit votes: no commit was
  chosen or reported), **L2** every accepted commit to a commit-certain tally or a proof (commit is T's only outcome).
  Counts per pool of every referenced object, changed or lock-only, with that pool's write threshold `t`: `vc` replied
  and voted commit, `k` amnesiac, `s` silent; every other replier is a provable non-commit-voter. Primary pool: `aa` /
  `ac` report abort / commit as their latest accept, `k_p` its amnesiac stores. Per changed pool `r` is its read
  threshold, `IDA.consistentRestoreThreshold` (a majority, never `restoreThreshold`).
  (1) **Adopt** any reply with an outcome (a commit over an abort): a committed one through `TxResolved` (late if
  finalized or late, so a commit vote looks up its entry), an aborted one as a `TxFinalized` abort.
  (A) **Commit** when every referenced pool has `vc >= t` (no tally was abort-certain, so by L1 no abort was accepted;
  a client may have learned commit from these votes). (B) **Commit** when `ac >= 1` and every changed pool has
  `vc >= r` (L2). (C) **Discard** when `aa >= 1` (L1). (D) **Discard** when some changed pool has `vc + s < r` (the
  update can never be restored; `dataLost`, logged as an error, when `ac >= 1` or every pool has `vc + k + s >= t`: a
  commit may have been chosen or reported). (E) **Discard** when some pool has `vc + k + s < t` (no tally was
  commit-certain). (F) **Commit** when no store of T is silent, every pool has `vc + k >= t` and `vc > w - t`,
  `k_p + n_p < t` (`n_p`: primary stores replying "unknown") and every changed pool has `vc >= r` (only the amnesiac
  and "unknown" stores, fewer than a quorum, could hide an accepted abort: a store that accepted, was finalized or
  discarded T and let its cached outcome expire answers "unknown" too; a conflicting commit's `t` votes would overlap
  T's live voters since `vc + t > w`. A lock-only pool needs `vc > w - t` stated: its amnesiac stores hold no lock,
  and with only `vc + k >= t` a stale commit vote beside rebuilt abort voters committed T over a failed requirement).
  (G) **Hold** otherwise: some store is silent, or, every store reported, `k_p + n_p >= t` (with a changed primary
  object D fires first, so only an unchanged primary object reaches it) or some lock-only pool has `vc <= w - t`
  (logged `UNDECIDABLE TRANSACTION`, then every `Frontend.UndecidableLogRounds` rounds). Accepted divergence: one store discards by C over an abort accepted
  by fewer than a quorum, the accepter forgets T, and another commits by F: no client heard abort (never chosen),
  F's locked voters exclude a conflicting commit, and the commit's missed-update marks repair the discarder.
  On a commit (A, B, F) the deciding store sends `TxResolved(commit)` to every store of T, applies it (a commit
  voter's locks have kept its locked parts unchanged: repair defers to a store's own locks, and a conflicting commit
  would need a locked voter), and its driver finalizes it (`Frontend.onExpiryCommit` -> `Store.finalizeExpiryCommit`),
  whose missed-update marks repair the stores that never applied it.
  No two observers reach a commit and a discard: reported states are final, so observers differ only in which stores
  are silent to them, and a store silent to one observer only adds to the other's `vc + s` / `vc + k + s`. With every
  store replied and none amnesiac (rebuild-process 6.4) either A or E fires.
  A Tx that knows its outcome passes it (`known`): rule 1 then adopts only a finalized or late commit of a known
  commit, and otherwise it concludes once more than `w - t` primary stores are non-holders (no acceptor state and not
  amnesiac: a commit is forgotten only at TxFinalized), or holds
- `Discard`: an abort voter, a Tx with no vote, a prover or a muted Tx is discarded at once. A commit voter
  (`needsUnackedCheck`, which covers expiring Txs too) first looks up its entry: none found by a lookup started after
  the decision discards it; one found applies T and concludes it as a late commit; a failed lookup keeps it holding and
  is retried at heartbeat. It never discards on a failure. A Tx that knows its outcome concludes instead
  (`Tx.concludeKnownOutcome`): an entry-applied one as finalized with its applied stub; another commit as a late commit
  of a transaction applied here (its own lookup decides the stub); an abort as finalized.
  Discarding releases locks and objects, deletes the CRL state as for a forgotten abort, forgets the Tx, and leaves the
  status cache alone (no outcome is known). An expiring commit vote also holds an abort for a lookup, as a recovered
  one does, and its store drives it meanwhile, as it does a recovered one
- Why a rebuilt store must count as amnesiac, never as having nothing: its acceptor state was lost with T undecided,
  so with one store that never heard of T it once made the last holders of a chosen commit discard it (rebuild-process
  6.4, cross-document-review 2.1; HorizonIntegrationSuite "holders of a chosen commit apply it past expiry though a
  rebuilt store forgot it")
- A driver of a transaction past the horizon plus the margin that has no outcome is stopped and removed once its
  store holds no Tx for it (`Store.removeExpiredDrivers`). A driver with an outcome keeps running its finalization
- Rejoin invariant: once a store discards T, its floor makes it refuse every later prepare for T: a discarded
  acceptor is indistinguishable from an offline one, and Paxos is safe with offline acceptors. **A store never
  rejoins a transaction it forgot.**
- A resolved abort deletes its CRL state, releasing its admission, while the Tx waits for `TxFinalized`
  (`Tx.stateDeleted`). It saves nothing more (crash-recovery-log 6.1, A4): a prepare, an accept or a delayed prepare
  response is answered with `TxResolved(committed = false)` to the proposer, never a promise or acceptance. Sound: the
  abort is the chosen value, and an acceptor that promises and accepts nothing can help choose no other

#### Applied stubs
- A durable (store, txid) record in the device CRL, charged to `CrlAdmission`, meaning the store holds
  that transaction's update. It carries T's start timestamp (from the Tx's txd, or `TxFinalized.startTimestamp` when
  the store holds no Tx). Written where the Tx's CRL state would be deleted, after its commits persist: on an
  owed-listing `TxFinalized` (`owed`), on applying through an UnackedCommits entry, and by a lookup when the Tx is
  forgotten without a listing one. A driver reached by another's non-late commit `TxFinalized` cancels its finalizer
  and finalizes at once, listing the received `owed` plus the stores its mark had handed off (`handedOffStores`), so
  a stub may precede its entry. A commit at a store with a stub applies nothing; a non-late one reports its objects in
  `objectCommitErrors`. The store drops a stub itself (`Frontend.expireStubs`, at heartbeat) once T is past the
  horizon plus the expiry margin and it holds no Tx for T (live, deferred or awaiting backend commits):
  below the floor no prepare creates a Tx for T, so the stub can suppress nothing (crash-recovery-log 3.6). A queue
  ordered by expiry (`stubExpiry`, stale entries skipped) means a heartbeat looks only at the stubs due, and a stub a
  Tx keeps past its expiry waits in `heldStubs` without blocking the rest. Stubs are so
  bounded by their rate times ~30 s whatever the repair scans do; the scan neither reads nor drops them. An entry with
  no stub is harmless at a store that saw T: it never
  rejoins T, so no replay Tx exists there to apply through it (a second driver's lost TxFinalized included). Residual:
  a store that never saw T (below a quorum): its entry can exist only through the unknown-voters case, which names no
  incarnation. A replay Tx there that voted commit applies T through it on a late commit (its vote passed T's pin,
  and it has held its locks since; a blind Insert of a key whose later deletion repair installed there is applied
  and reported, as is a non-voter's blind Insert of a key absent before its apply that a committed Delete started
  since reached, `server/store/CLAUDE.md`);
  otherwise the entry's reconcile installs the committed state, since repair leaves the entry until the store can no
  longer join T
  (`server/repair/CLAUDE.md`; transaction-recovery 5.4)
- A Tx finalized while its commit is held for its turn defers concluding its state (`Frontend.finalizationDeferred`).
  Every deferring Tx of the txid (a replay's too) is listed, and the state is deleted or stubbed only by the call that
  empties the list; a stub owed meanwhile passes to every listed Tx
- CRL encoding of stubs: `server/crl/CLAUDE.md`

#### Commit delivery, lost messages, status cache
- A commit voter unlocks before applying (`Tx.doCommit`). An applied object is still ordered by the in-flight
  commit marker, but one it cannot apply would leave its counted vote standing for neither. An apply that throws
  fail-stops the store (`server/store/CLAUDE.md`, Fail-stop). A live voter whose re-check fails on a changed object
  meets an invariant violation: it logs `INVARIANT VIOLATION` at error, naming the transaction each object now shows
  (where two conflicting commits would surface), and skips and reports the object. A recovered voter (through its
  entry too) takes such an object as already applied (`Tx.issueCommits`, transaction-requirements 8.5); a recovered
  Tx that did not vote commit applies nothing and reports every object it changes (`Tx.doCommit`): it may have applied
  and acknowledged T before its crash, leaving no ErrorTree mark, and held no lock keeping a later deletion of a key T
  Inserts out. A lock-only object that fails the re-check is skipped
  and reported only
- Lock order (transaction-requirements 8.3, R15): a committed Tx holding no locks at a store does not apply while
  another Tx there holds locks its requirements or writes collide with (`RequirementsChecker.lockHolders`) on an
  object both change. It is held (`Tx.awaitLockHolders`, `commitHeld`, no locks, references kept) until each holder
  releases (`Frontend.locksReleased`, after the holder applied or aborted), then takes the usual `holdForTurn` path.
  Otherwise a later commit that does not chain on the vote (a refcount change, a key-level write, a repair refcount
  or key proof) would raise the object timestamp, fail the vote's re-check and leave the counted vote standing for
  neither lock nor update. A lock-only holder is not waited for; a Tx holding its own locks never waits. The wait is
  outside `commitInFlight`, so the holder's own commit never queues behind it. A new vote waits for it only if the
  waiter would wait for it as a holder: the vote changes an object the waiter changes and the waiter collides with
  the locks it would take (`Tx.lockWaitBlocked`: `RequirementsLocker.plannedLocks` through
  `RequirementsChecker.PlannedLocks`, tested by the waiter's `wouldWaitFor` with the `Frontend.changes` filter), so it
  cannot be starved; any other vote, lock-only ones included, decides at once. A colliding vote waits at most `Tx.CollisionWait` (`checkCollisionWait`, `lockWaitBlockStart`),
  then votes abort naming the waiter: a waiter may wait on a holder that needs a vote another store's waiter
  withholds, and the abort ends such a cross-store cycle. A holder that never resolves (undecidable expiry tally)
  holds the waiter for as long as its locks: beyond the write threshold's margin, alarmed, no way out
- A commit decision can overtake the data too. The store holds the commit until the data, a client's prepare, or
  TxFinalized arrives, then applies it and sends TxCommitted, so the driver marks no missed update for that store
  if the data beat the finalizer's `errorTimeout` (2 s). Stores that miss it are marked in the ErrorTree
  (`client/internal/transaction/CLAUDE.md`) and repaired (`server/repair/CLAUDE.md`)
- A lost TxFinalized is recovered by the store itself: a Tx idle for `Host.txHeartbeatTimeout` (5 heartbeats, 5 s)
  gets a driver, which learns the outcome from the stores' status caches. `Host.txStatusCacheDuration` is
  `TransactionHorizon.statusCacheDuration`: horizon + `MaxStartLead` + 8 heartbeats, 30 s with the host's 1 s
  heartbeat, asserted (`Host.checkStatusCacheDuration`) to be at least horizon + `MaxStartLead` + that timeout + one
  heartbeat, so a Tx first created near the horizon still finds its peers' entries when its idle driver starts, and an
  entry expires only once its transaction is below its store's floor. At high volume the cache is costly (about 175
  bytes per transaction, capped at `DefaultMaxEntries`); a size eviction is logged as an error (at most once per
  `EvictionLogPeriod`, 10 s, with the count) and raises the host's
  eviction watermark. A driver that still learns nothing past the horizon meets refusals, and the Tx ends by expiry
- The status cache is safety evidence, not only an optimization (paxos P17): a finalized entry is late-commit evidence
  (the relay reports it late) and finalization evidence (a prepare for a transaction the store does not hold is
  ignored), so an entry must never be wrong or downgraded. Its retention is not relied on: a lost entry's transaction
  is below the forget floor. The relay of a resolved, unfinalized entry (`Store.handleTransactionMessage`) goes
  as a `TxResolved` to the prepare's sender, whose recovery driver learns the outcome, and to the store itself, so the
  `outcomeDecided` Tx that prepare creates and the store's own driver learn it too (paxos-transactions 10.2). Not to
  itself while its own Tx for T is resolved: the entry may be that Tx's (`Tx.resolved`), and after a lost TxFinalized
  its idle driver would take it as a non-late commit and re-run T's finalization actions; it learns from its peers
  (late from their finalized relay, or by Paxos) (transaction-recovery 4.2)
- `TransactionStatusCache.updateStatus` keeps Committed over an opposite outcome (P19). Under the horizon an
  opposite outcome should never arise: one that does is logged as an error ("Split decision") and counted
  (`splitDecisions`)
- `AbortVoteReport` logs a host's abort votes by failed requirement once a second while they happen
