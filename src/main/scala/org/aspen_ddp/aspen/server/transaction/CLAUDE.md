### Transaction protocol
Store-local mechanics (locks, collisions, in-flight commits, admission): `server/store/CLAUDE.md`. Client-side
pin rule and finalizer entries: `client/internal/transaction/CLAUDE.md`. Safety arguments: `docs/safety/`.
Designs: `docs/superpowers/specs/2026-11-09-replay-pins-design.md` (current), `2026-11-08-traceless-commits-design.md`

#### Lifecycle
- The client builds a TransactionDescription and sends it plus object data to the stores
- The stores check all local state against the TransactionRequirements and determine their Commit/Abort decision
- The CrashRecoveryLog is used to store this decision and Paxos is then used to arrive at an overall commit/abort decision
- The client and all stores are informed of the final decision
- If committed, FinalizationActions are then run. Once complete, the CRL content is dropped and the transaction is forgotten.
- If the refcount of the object is set to zero, it is deleted

#### Paxos
- Transactions are initiated by the client but the Paxos participants are the DataStores hosting only one
  of the targeted objects. This is done to reduce the number of participants and does not affect correctness.
- The Paxos algorithm itself is unmodified but some clever message handling is done to reduce the number of
  round-trips and optimize data flow. The details are only important when working on transaction message
  handling.
- The CRL stores the persistent state Paxos requires (`server/crl/CLAUDE.md`)
- The driver tallies each object's votes from its own pool's stores

#### Proofs and late resolution
- A prepare for a transaction a store does not know, whose update an object there already shows (its revision, or
  a key's for a KeyValueUpdate without a required revision: `RequirementsApplyer.appliedBy`), proves it committed.
  The store answers `TxResolved(committed, proof = true)` (`Tx.proveCommitted`) for its driver only and applies
  nothing, records nothing in the status cache, and casts no commit vote. A driver holding a proof never proposes
  abort (an abort Paxos already accepted still resolves). A resolution reached after the transaction was forgotten
  is late (`late = true`; the cached outcome is finalized): the driver runs no finalization actions, and a store
  applies it only when it holds its own commit vote and its UnackedCommits entry exists. Everything else (abort
  vote, prover, muted Tx) applies nothing and sends no TxCommitted
- Driver prepares are prefer-abort (`fromClient == false`): a Tx with no vote and no client prepare never checks
  requirements on one. It proves at once if it can (`appliedBy`), else withholds its whole response until the data, a
  client's prepare, or the outcome arrives, or until `Frontend.MissingDataWait` (5 s) passes: then it votes abort
  (`NoClientPrepare`). This holds for data-less transactions too, so their late resolution may take 5 s longer. A vote
  given stands, but later data is still used to apply a commit. Accepted corner case: a delayed duplicate client
  prepare of an aborted transaction (see the `Transaction.commit` scaladoc)
- Muted prepares at rebuilt stores (`acceptor-since`): `server/store/CLAUDE.md`

#### UnackedCommits
- Written by the finalizer for each commit voter that sent no TxCommitted (`client/internal/transaction/CLAUDE.md`);
  an entry durable means every other finalization action is done
- A recovered unresolved commit vote looks its entry up (`UnackedCommitLookup`, from the
  Host) once loaded and again before accepting any abort; no driver starts for it before the first lookup answers or
  while the CRL loads. A found entry proves the commit to its own driver; the Tx applies its saved update locally, logs
  it at info and stays until TxFinalized, writing no status-cache entry (a Tx holding a late commit that finds its
  entry applies and is forgotten at once). A failed or unanswered lookup holds the
  abort and retries at heartbeat (`Frontend.UnackedLookupTimeout`, 5 s)

#### Applied stubs
- A durable (store, txid) record in the device CRL, charged to `CrlAdmission`, meaning the store holds
  that transaction's update. Written where the Tx's CRL state would be deleted, after its commits persist: on an
  owed-listing `TxFinalized` (`owed`), on applying through an UnackedCommits entry, and by a lookup when the Tx is
  forgotten without a listing one. A driver reached by another's non-late commit `TxFinalized` cancels its finalizer
  and finalizes at once, listing the received `owed` plus the stores its mark had handed off (`handedOffStores`), so
  a stub may precede its entry. A commit at a store with a stub applies nothing; a non-late one reports its objects in
  `objectCommitErrors`. The repair scan drops a stub with its entry, and an orphan stub (pre-walk snapshot, no entry)
  only once two consecutive scans found it so. Residuals: a store that never saw T and missed its TxFinalized (below a
  quorum, self-heals), and a second driver's host crashing after its hand-off but before its TxFinalized arrives
- CRL encoding of stubs: `server/crl/CLAUDE.md`

#### Commit delivery, lost messages, status cache
- A commit decision can overtake the data too. The store holds the commit until the data, a client's prepare, or
  TxFinalized arrives, then applies it and sends TxCommitted, so the driver marks no missed update for that store
  if the data beat the finalizer's `errorTimeout` (2 s). Stores that miss it are marked in the ErrorTree
  (`client/internal/transaction/CLAUDE.md`) and repaired (`server/repair/CLAUDE.md`)
- A lost TxFinalized is recovered by the store itself: a Tx idle for `Host.txHeartbeatTimeout` gets a driver, which
  learns the outcome from the stores' status caches, so `Host.txStatusCacheDuration` must outlast that timeout. At
  high volume the cache is costly (about 175 bytes per transaction, capped at `DefaultMaxEntries`), so these stay
  short: 5 s and 7 s with the host's 1 s heartbeat
- `AbortVoteReport` logs a host's abort votes by failed requirement once a second while they happen
