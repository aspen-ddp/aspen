# Safety specifications

These documents state, as checkable specifications, the correctness guarantees of Aspen's transaction, recovery,
repair and rebuild machinery. Each describes the current design and implementation only: its mechanisms, the
invariants they establish, the safety arguments that rest on them, the failure scenarios as they play out today, and
the accepted limitations. Each ends with `## Open weaknesses`, the flaws found in it that are not yet fixed. History
(earlier findings and how they were fixed) lives in git, not here.

Paths in the documents are relative to `src/main/scala/org/aspen_ddp/aspen/` unless they start with `docs/`. Code is
cited as file plus symbol, never by line number. Thresholds follow `CLAUDE.md`: `w` width, `t` write threshold, `r`
read threshold (`IDA.consistentRestoreThreshold`, a majority), with `majority <= r <= t <= w`.

## Verdicts

Every open weakness carries one verdict:

* **CONFIRMED**: the mechanism was traced end to end in the code; the trigger may still be rare.
* **PLAUSIBLE**: the mechanism is real, but whether or how often the trigger occurs is uncertain.

A candidate that could be traced to neither is not recorded. A candidate refuted by a guard is not recorded either;
the guard is stated in the relevant design section instead.

Each weakness also has an effort (S hours, M a day or two, L a design plus several days) and a priority tier: 1 lost
committed data, split decisions, device-wide loss; 2 resurrection, overwritten updates, permanent replica divergence;
3 systemic loss; 4 liveness; 5 performance and hygiene; 6 coverage gaps; 7 documentation corrections outside
docs/safety; 8 unimplemented features.

## Documents

| Document | Covers | Weakness ids |
|---|---|---|
| [paxos-transactions.md](paxos-transactions.md) | Votes, the Paxos instance, the client's commit from votes, proofs and late commits, the status cache, the transaction horizon, forget floor and expiry | P-W* |
| [transaction-requirements.md](transaction-requirements.md) | Requirement checks, lock classes, vote tallying, the commit window, description validation, fail-stop and the apply re-check | Q-W* |
| [transaction-recovery.md](transaction-recovery.md) | Driver takeover, participant recovery, finalizers, the missed-update marker, UnackedCommits entries and applied stubs | R-W* |
| [crash-recovery-log.md](crash-recovery-log.md) | The CRL contract, SimpleCRL's format, rotation and recovery, admission, per-device sharing | CRL-W* |
| [repair-process.md](repair-process.md) | ErrorTree and UnackedCommits repair scans, Reconcile, deferral, parking, proofs, deletes | RP-W* |
| [rebuild-process.md](rebuild-process.md) | Store rebuild triggers, discard, walk, pending allocations, promotion, the rebuilt acceptor | RB-W* |
| [systemic-loss-protection.md](systemic-loss-protection.md) | Problem statement: every path that declares a store lost or destroys retained state | SL-W* |
| [cross-document-review.md](cross-document-review.md) | How the subsystems rely on each other, contradictions between documents, interaction flaws | X-W* |

## Where the work is tracked

* Open weaknesses: the `## Open weaknesses` section at the end of each document above.
* The prioritized work list built from them: `Todo.txt` at the repository root.
* Accepted, deferred or design-track items (correlated lost-store declarations, hung devices, mixed-version upgrades,
  bounded reads, and others): `Backlog.txt`. A weakness already tracked there is not repeated in these documents.
