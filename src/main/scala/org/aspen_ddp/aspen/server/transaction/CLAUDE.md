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
- The client and all stores are informed of the final decision
- If committed, FinalizationActions are then run. Once complete, the CRL content is dropped and the transaction is forgotten.
- Finalization failure path (`TransactionDriver.startFinalizer`): a finalizer that cannot be created, or whose
  `complete` fails (it fails as soon as any action does), is logged at error and started again from scratch after a
  backoff (1 s doubling to 60 s) until one completes, the driver finalizes another way, or it shuts down. Never
  finalize without the actions: the marker's entries are what repair and owed stores rely on. TxFinalized's `owed`
  includes the planned stores of failed finalizers (`failedHandedOff`). An action that gives up must fail its
  `complete`, never leave it pending
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
  value its promise quorum reports (paxos F7)
- Every object is classified (paxos F8): commit-certain (`c >= t`), abort-certain (`w - a < t`) or undecided. Any
  abort-certain object decides abort at once; all commit-certain decides commit. An undecided tally waits (silence may
  be transient) until T is past the horizon plus the expiry margin (`TransactionDriver.Start.pastExpiry`, from
  `Frontend.pastExpiry`), then proposes abort (rebuild F6). Expiry alone never ends it: a commit voter and an abort
  voter both hold acceptor state, so `ExpiryTally` holds, and a silent store back past the horizon refuses silently.
  The abort is an ordinary Paxos proposal (paxos-transactions P7a): an accepted value in the quorum still wins
- Proposer ids are pool indexes, so only the primary pool's stores propose (`TransactionDriver.mayPropose`). A driver on
  any other store is learn-only: it sends prepares and resolves, proves late and finalizes from what it hears, but sets
  no proposal and sends no Accept. A Tx counts prepares and heartbeats as driver activity only from a primary-pool
  sender (`Tx.fromProposer`), so a learn-only driver's retransmits never stop a primary store taking over an idle
  transaction; with no primary store reachable, the Tx ends by expiry (paxos F4)
- Only the designated leader's first driver, the one its client's prepare creating the Tx starts
  (`Store.driveTransaction(initialProposal = true)`), keeps the initial proposal `(1, L)`. Every other driver starts
  at round 2 or higher, above the highest id its own acceptor promised or accepted (`TransactionDriver.Start`,
  `Tx.highestProposal`; paxos F5)
- A conflicting second outcome (split decision) is logged as an error and counted in the host status cache's
  `splitDecisions` (`TransactionStatusCache.recordSplitDecision`) by the cache itself, the Tx (`receiveResolved`,
  `receiveFinalized`) and the driver (`Start.onSplitDecision`); each keeps its first outcome, and a driver that resolved
  commit ignores an abort `TxFinalized` (paxos F11)

#### Proofs and late resolution
- A prepare for a transaction a store does not know, whose update an object there already shows (its revision, or
  a key's for a KeyValueUpdate without a required revision: `RequirementsApplyer.appliedBy`), proves it committed.
  The store answers `TxResolved(committed, proof = true)` (`Tx.proveCommitted`) for its driver only and applies
  nothing, records nothing in the status cache, and casts no commit vote. A driver holding a proof never proposes
  abort (an abort Paxos already accepted still resolves). A resolution reached after the transaction was forgotten
  is late (`late = true`; the cached outcome is finalized): the driver runs no finalization actions, and a store
  applies it only when it holds its own commit vote and its UnackedCommits entry names the store's own incarnation
  (UnackedCommits below). Everything else (abort vote, prover, muted Tx) applies nothing and sends no TxCommitted
- Driver prepares are prefer-abort (`fromClient == false`): a Tx with no vote and no client prepare never checks
  requirements on one. It proves at once if it can (`appliedBy`), else withholds its whole response until the data, a
  client's prepare, or the outcome arrives, or until `Frontend.MissingDataWait` (5 s) passes: then it votes abort
  (`NoClientPrepare`). This holds for data-less transactions too, so their late resolution may take 5 s longer. A vote
  given stands, but later data is still used to apply a commit. A delayed duplicate client prepare of an aborted
  transaction cannot commit it: every store that took part remembers the abort for longer than it would join T, and
  refuses T past its forget floor (below)
- Description validation (transaction-requirements F7, Q-N4): a client-prepared Tx checks its description once,
  before its requirements (`DescriptionValidator`, `Tx.invalidDescription`). A refusal votes abort for every object
  (`InvalidDescription`, a non-object error, never waited out) and logs a warning. The check covers its pool's IDA
  (`Frontend.poolIDA`), the primary object and leader, one update per object, and the pin rule. It runs at vote only,
  never at the apply re-check
- Muted prepares at rebuilt stores (`acceptor-since`) and for start timestamps ahead of the host's clock
  (`Frontend.MaxStartLead`): `server/store/CLAUDE.md`
- Transaction horizon (`common.transaction.TransactionHorizon`, 20 s on `startTimestamp` against the host's wall
  clock): a prepare for a transaction a store does not hold whose start is below its forget floor
  (`Frontend.forgetFloorMillis`: now - horizon, its load + `MaxStartLead`, the cache's eviction watermark) is refused
  outright, no Tx; details `server/store/CLAUDE.md`. So no driver ever counts the fresh state of an acceptor that
  forgot T, and a commit is never re-decided as abort nor an abort as commit. Proofs and late resolution still work
  inside the window but are no longer load-bearing

#### UnackedCommits
- Written by the finalizer for each commit voter that sent no TxCommitted (`client/internal/transaction/CLAUDE.md`);
  an entry durable means every other finalization action is done
- A recovered unresolved commit vote looks its entry up (`UnackedCommitLookup`, from the
  Host) once loaded and again before accepting any abort; no driver starts for it before the first lookup answers or
  while the CRL loads. A found entry proves the commit to its own driver; the Tx applies its saved update locally, logs
  it at info and stays until TxFinalized, writing no status-cache entry (a Tx holding a late commit that finds its
  entry applies and is forgotten at once). A failed or unanswered lookup holds the
  abort and retries at heartbeat (`Frontend.UnackedLookupTimeout`, 5 s). Past the horizon its peers refuse its
  driver, so such a Tx ends by expiry (below)
- The apply decision (`Frontend.unackedLookupComplete`; the lookup answers `UnackedLookupResult.Absent` or
  `Found(incarnation)`, `Host.unackedLookup` decoding the entry): only `Found(Some(own))`, naming
  `Frontend.incarnation`, applies through the entry (`Tx.unackedCommitFound`). `Found` of another incarnation (a
  rebuilt store, Rebuild N3) or `None` (the driver never heard from the store, or heard two incarnations from it)
  logs both at info and calls `Tx.unackedCommitNotOwed`: T committed, and nothing is applied through the entry, held
  aborts dropped and never accepted. A recovered or expiring commit vote does as for a matching entry, applying
  nothing (`notOwedHere`, as with an applied stub: its objects in `objectCommitErrors`): it resolves committed,
  records nothing in the status cache, proves the commit to its driver and stays until TxFinalized, since T's
  finalization may still be running (the marker writing other stores' entries). A held late commit, or a vote whose
  expiry Discard was decided, concludes at once (`applyNothing`, a real late outcome exists). Neither writes a stub of
  its own (`applyNothing` still writes one already owed: `stubDue`). A forget-lookup (`forgetLookupHeld`, T already
  applied here) writes its stub as for any found entry. A found entry of any incarnation is never taken as absent.
  The entry is left for the repair scan, whose reconcile installs the committed state
- A recovered Tx whose saved status is resolved (re-saved by a prepare or accept after the resolution) restores it
  into `oresolution`. It takes no locks and needs no lookup: once its objects load it applies the commit with the
  recovered re-check rules, or deletes the abort's state, then waits for TxFinalized as a live resolved Tx does
- Coverage: entries exist only for unacknowledged counted voters (every silent store when the voters are unknown).
  These reach the write threshold, so uncounted voters and non-voters are repaired by their ErrorTree marks alone
  (transaction-recovery F13/F15). Any narrowing of entries must keep this rule

#### Expiry (transaction horizon design, section 2)
- A Tx still unfinalized past the horizon plus the expiry margin (`Frontend.expiryMargin`: `MaxStartLead` +
  `Host.txHeartbeatTimeout` + one heartbeat; T's start against the host's wall clock) is `expiring` (`Tx.mayExpire`):
  unresolved, or resolved and waiting for a TxFinalized that its host missed for longer than the caches remember T
  (resolved by Paxos, or applied from its UnackedCommits entry: `provesCommitToItsDriver`). At store
  heartbeat (`Frontend.expiryHeartbeat`) it sends `TxStatusRequest` to every other store of T, at once, then after 1,
  2, 4 and every 8 heartbeats. Each round has a fresh `requestUUID` and counts only its own replies, plus the Tx's own
  acceptor state (a muted Tx holds none)
- The pure `ExpiryTally.decide` rules, in order: (1) any reply with an outcome is adopted (a commit over an abort): a
  committed one through `TxResolved` (late if finalized or late, so a commit vote looks up its entry), an aborted one
  as a `TxFinalized` abort; (2) otherwise the primary pool's (`txd.primaryObjectIDA`) stores that replied "unknown" or
  without acceptor state are non-holders, and silent ones possible holders; (3) more than `width - writeThreshold`
  non-holders: `Discard`; (4) else `Hold` and ask again. A Tx that knows its outcome passes it (`known`): rule 1 then
  adopts only a finalized or late commit of a known commit, and an outcome reply without acceptor state (a status
  cache answering) is a non-holder, since re-adopting the known outcome would change nothing
- `Discard`: an abort voter, a Tx with no vote, a prover or a muted Tx is discarded at once. A commit voter
  (`needsUnackedCheck`, which covers expiring Txs too) first looks up its entry: none found by a lookup started after
  the decision discards it; one found applies T and concludes it as a late commit; a failed lookup keeps it holding and
  is retried at heartbeat. It never discards on a failure. A Tx that knows its outcome concludes instead
  (`Tx.concludeKnownOutcome`): an entry-applied one as finalized with its applied stub; another commit as a late commit
  of a transaction applied here (its own lookup decides the stub); an abort as finalized.
  Discarding releases locks and objects, deletes the CRL state as for a forgotten abort, forgets the Tx, and leaves the
  status cache alone (no outcome is known). An expiring commit vote also holds an abort for a lookup, as a recovered
  one does, but its store drives it meanwhile (`awaitsFirstUnackedLookup` is for recovered votes only)
- Why discarding is safe: an acceptor forgets T only once T is decided (an abort at resolution, a commit at
  TxFinalized), and a store answers "unknown" only below its forget floor, so a non-holder never becomes a holder.
  More than `w - t` of them rule out a committed, unfinalized T, which a full quorum would still hold. So T aborted,
  or committed and was finalized (its finalizer wrote this store's entry if it was owed), or can never be decided
- A driver of a transaction past the horizon plus the margin that has no outcome is stopped and removed once its
  store holds no Tx for it (`Store.removeExpiredDrivers`). A driver with an outcome keeps running its finalization
- Rejoin invariant: once a store discards T, its floor makes it refuse every later prepare for T: a discarded
  acceptor is indistinguishable from an offline one, and Paxos is safe with offline acceptors. **A store never
  rejoins a transaction it forgot.**

#### Applied stubs
- A durable (store, txid) record in the device CRL, charged to `CrlAdmission`, meaning the store holds
  that transaction's update. Written where the Tx's CRL state would be deleted, after its commits persist: on an
  owed-listing `TxFinalized` (`owed`), on applying through an UnackedCommits entry, and by a lookup when the Tx is
  forgotten without a listing one. A driver reached by another's non-late commit `TxFinalized` cancels its finalizer
  and finalizes at once, listing the received `owed` plus the stores its mark had handed off (`handedOffStores`), so
  a stub may precede its entry. A commit at a store with a stub applies nothing; a non-late one reports its objects in
  `objectCommitErrors`. The repair scan drops a stub with its entry (a chunked entry's head), and an orphan stub (pre-walk snapshot, no entry)
  only once two consecutive scans found it so. An entry with no stub is harmless at a store that saw T: it never
  rejoins T, so no replay Tx exists there to apply through it (a second driver's lost TxFinalized included). Residual:
  a store that never saw T (below a quorum) gets T from reconcile: its entry can exist only through the unknown-voters
  case, which names no incarnation, so a late commit there applies nothing through it, and the entry's reconcile
  installs the committed state, since repair leaves the entry until the store can no longer join T
  (`server/repair/CLAUDE.md`; transaction-recovery Todo 2.9 re-assessment)
- A Tx finalized while its commit is held for its turn defers concluding its state (`Frontend.finalizationDeferred`).
  Every deferring Tx of the txid (a replay's too) is listed, and the state is deleted or stubbed only by the call that
  empties the list; a stub owed meanwhile passes to every listed Tx
- CRL encoding of stubs: `server/crl/CLAUDE.md`

#### Commit delivery, lost messages, status cache
- A commit voter unlocks before applying (`Tx.doCommit`). An applied object is still ordered by the in-flight
  commit marker, but one it cannot apply would leave its counted vote standing for neither, so the store
  fail-stops (`server/store/CLAUDE.md`, Fail-stop). A recovered voter whose re-check fails quarantines the object
  until repair verifies it (`Tx.quarantining`; `server/store/CLAUDE.md`, Quarantine)
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
- `TransactionStatusCache.updateStatus` keeps Committed over an opposite outcome (Ruling 30). Under the horizon an
  opposite outcome should never arise: one that does is logged as an error ("Split decision") and counted
  (`splitDecisions`)
- `AbortVoteReport` logs a host's abort votes by failed requirement once a second while they happen
