package org.aspen_ddp.aspen.server.transaction

import org.aspen_ddp.aspen.common.ida.{IDA, ReedSolomon, Replication}
import org.aspen_ddp.aspen.common.network.TxStatusResponse.TxStatus
import org.aspen_ddp.aspen.common.objects.{DataObjectPointer, ObjectId}
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.transaction.{DataUpdate, DataUpdateOperation, ExclusiveRevisionLock,
  TransactionDescription, TransactionObjectRequirement, TransactionStatus}
import org.aspen_ddp.aspen.server.store.FrontendSuite
import org.aspen_ddp.aspen.server.transaction.ExpiryTally.Decision
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.util.UUID

/** The pure decision of an expiring Tx from its peers' status replies (ExpiryTally rule 1 and rules A-G). The primary
  * pool is ReedSolomon(5, 3, 4): width 5, read threshold 3, write threshold 4, so `width - writeThreshold` is 1. T
  * overwrites one object of it
  */
class ExpiryTallySuite extends AnyFunSuite with Matchers:
  import FrontendSuite.{op1, otherPoolId, poolId, rev0}

  private val rs = ReedSolomon(5, 3, 4)

  private def txdWith(primary: IDA, others: Map[org.aspen_ddp.aspen.common.pool.PoolId, IDA] = Map(),
                      extra: List[TransactionObjectRequirement] = Nil,
                      primaryReq: TransactionObjectRequirement = DataUpdate(op1, rev0, DataUpdateOperation.Overwrite))
  : TransactionDescription =
    FrontendSuite.mktxd().copy(primaryObjectIDA = primary, poolIDAMap = Map(poolId -> primary) ++ others,
      requirements = primaryReq :: extra)

  private val otherPtr = DataObjectPointer(ObjectId(new UUID(0, 99)), otherPoolId, Array[Byte]())
  /** T also overwrites an object of a Replication(3, 2) secondary pool (read and write threshold 2) */
  private val withSecondary = txdWith(rs, Map(otherPoolId -> Replication(3, 2)),
    List(DataUpdate(otherPtr, rev0, DataUpdateOperation.Overwrite)))

  private val txd = txdWith(rs)

  private def primary(i: Int): StoreId = StoreId(poolId, i.toByte)
  private def secondary(i: Int): StoreId = StoreId(otherPoolId, i.toByte)

  /** A store below its floor with no Tx and no cached outcome */
  private val unknown: Option[TxStatus] = None
  /** A frozen holder: its vote and latest accept */
  private def holder(voted: Boolean = true, accepted: Option[Boolean] = None): Option[TxStatus] =
    Some(TxStatus(TransactionStatus.Unresolved, finalized = false, late = false, holdsAcceptorState = true,
      accepted = accepted, votedCommit = voted))
  private val acceptedCommit = holder(accepted = Some(true))
  /** A rebuilt store, for T started before its acceptor */
  private val amnesiac: Option[TxStatus] = Some(TxStatus(TransactionStatus.Unresolved, finalized = false,
    late = false, holdsAcceptorState = true, amnesiac = true))
  private def outcome(status: TransactionStatus.Value, finalized: Boolean = false, late: Boolean = false) =
    Some(TxStatus(status, finalized, late, holdsAcceptorState = false))

  test("any outcome-bearing reply is adopted"):
    ExpiryTally.decide(txd, Map(primary(0) -> unknown, primary(1) -> unknown, primary(2) -> unknown,
      secondary(0) -> outcome(TransactionStatus.Committed, late = true))) shouldBe Decision.Adopt(true, true)
    ExpiryTally.decide(txd, Map(primary(0) -> outcome(TransactionStatus.Committed, finalized = true))) shouldBe
      Decision.Adopt(true, true)
    ExpiryTally.decide(txd, Map(primary(0) -> holder(), primary(1) -> outcome(TransactionStatus.Committed))) shouldBe
      Decision.Adopt(true, false)
    ExpiryTally.decide(txd, Map(primary(0) -> holder(), primary(1) -> outcome(TransactionStatus.Aborted))) match
      case Decision.Adopt(committed, _) => committed shouldBe false
      case other => fail(s"Expected an aborted Adopt, got $other")

  // ---- Rules A-G, one by one ----

  test("every pool at its write threshold of commit votes commits though a secondary store is silent and nothing was accepted"):
    // Rule A
    ExpiryTally.decide(withSecondary, Map(primary(0) -> holder(), primary(1) -> holder(), primary(2) -> holder(),
      primary(3) -> holder(), primary(4) -> unknown, secondary(0) -> holder(), secondary(1) -> holder())) shouldBe
      Decision.Commit

  test("one accepted commit with commit voters at the read threshold commits"):
    // Rule B: the accept traces back to a commit-certain tally (L2). The old rule needed more than w - t = 1 accepts
    ExpiryTally.decide(txd, Map(primary(0) -> acceptedCommit, primary(1) -> holder(), primary(2) -> holder(),
      primary(3) -> unknown)) shouldBe Decision.Commit

  test("an accepted abort discards"):
    // Rule C: the accept traces back to an abort-certain tally (L1), though the visible votes are not
    ExpiryTally.decide(txd, Map(primary(0) -> holder(), primary(1) -> holder(), primary(2) -> holder(),
      primary(3) -> holder(voted = false, accepted = Some(false)))) shouldBe Decision.Discard(dataLost = false)

  test("the F6 split, one commit vote, one abort vote and an unknown store, discards"):
    // Every store replied, none amnesiac. Rule E: at most one commit voter, short of the write threshold of two, so no
    // tally was commit-certain. The primary object is changed, so D (one holder of the update, short of the read
    // threshold of two) precedes E here, with the same decision: nothing was lost
    ExpiryTally.decide(txdWith(Replication(3, 2)), Map(primary(0) -> holder(), primary(1) -> holder(voted = false),
      primary(2) -> unknown)) shouldBe Decision.Discard(dataLost = false)

  test("a rebuilt store's lost vote with every other store replied commits"):
    // Rule F: no store is silent, nothing was accepted except perhaps by the one amnesiac store, fewer than a quorum,
    // so no abort was chosen; frozen acceptors accept nothing more; three voters hold the update, its read threshold
    ExpiryTally.decide(txd, Map(primary(0) -> holder(), primary(1) -> holder(), primary(2) -> holder(),
      primary(3) -> holder(voted = false), primary(4) -> amnesiac)) shouldBe Decision.Commit
    // The same with one store silent holds: the silent store may have accepted abort
    ExpiryTally.decide(txd, Map(primary(0) -> holder(), primary(1) -> holder(), primary(2) -> holder(),
      primary(4) -> amnesiac)) shouldBe Decision.Hold(undecidable = false)

  test("a forgotten primary store's unknown reply may hide a chosen abort: rule F holds"):
    // T locks a Replication(3, 2) primary object and overwrites a secondary one. Stores 1 and 2 voted abort and
    // accepted it, so abort was chosen and finalized; store 1 was then rebuilt (amnesiac), and store 2 forgot T and
    // its cached outcome, so it answers "unknown". "Unknown" cannot tell a store that never joined from one that
    // accepted and forgot, so F counts it with the amnesiac stores: two of them, a quorum, may hide a chosen abort.
    // Every store replied, and the amnesiac store may instead have voted commit with store 0, so a client may have
    // been told commit: undecidable
    val lockedPrimary = txdWith(Replication(3, 2), Map(otherPoolId -> Replication(3, 2)),
      List(DataUpdate(otherPtr, rev0, DataUpdateOperation.Overwrite)), primaryReq = ExclusiveRevisionLock(op1, rev0))
    ExpiryTally.decide(lockedPrimary, Map(primary(0) -> holder(), primary(1) -> amnesiac, primary(2) -> unknown,
      secondary(0) -> holder(), secondary(1) -> holder(), secondary(2) -> holder())) shouldBe
      Decision.Hold(undecidable = true)

  test("a lock-only pool's stale commit vote beside a rebuilt abort voter holds: a conflicting commit was possible"):
    // T overwrites a Replication(3, 2) primary object and holds ExclusiveRevisionLock(Y) on a Replication(3, 2)
    // lock-only object. T' updated Y: L1 and L2 voted commit for it, so T' committed, and T got abort votes from L1
    // and L2 (requirement failed) and a stale commit vote from L0. L1 was rebuilt before its abort reached T's driver.
    // L has vc + k = 2 >= t, but only one live commit voter, so the two other stores may hold no lock of T: a
    // conflicting commit needs t = 2 of them. F's conflict argument needs vc > w - t in every referenced pool
    val lockedSecondary = txdWith(Replication(3, 2), Map(otherPoolId -> Replication(3, 2)),
      List(ExclusiveRevisionLock(otherPtr, rev0)))
    ExpiryTally.decide(lockedSecondary, Map(primary(0) -> holder(), primary(1) -> holder(), primary(2) -> holder(),
      secondary(0) -> holder(), secondary(1) -> amnesiac, secondary(2) -> holder(voted = false))) shouldBe
      Decision.Hold(undecidable = true)

  test("a lock-only primary's stale commit vote beside a rebuilt store and an abort voter holds"):
    // The same with T's lock on the primary object: p0 voted commit (stale), p1 was rebuilt, p2 is a frozen abort
    // voter that accepted nothing. k_p + n_p = 1 < t, but a conflicting commit was possible at p1 and p2
    val lockedPrimary = txdWith(Replication(3, 2), Map(otherPoolId -> Replication(3, 2)),
      List(DataUpdate(otherPtr, rev0, DataUpdateOperation.Overwrite)), primaryReq = ExclusiveRevisionLock(op1, rev0))
    ExpiryTally.decide(lockedPrimary, Map(primary(0) -> holder(), primary(1) -> amnesiac,
      primary(2) -> holder(voted = false), secondary(0) -> holder(), secondary(1) -> holder(),
      secondary(2) -> holder())) shouldBe Decision.Hold(undecidable = true)

  test("fewer known commit votes than the write threshold with a store silent holds"):
    // Rule G: the silent stores may be commit voters (A) or abort acceptors (C)
    ExpiryTally.decide(txd, Map(primary(0) -> holder(), primary(1) -> holder(), primary(2) -> holder())) shouldBe
      Decision.Hold(undecidable = false)

  test("t or more rebuilt primary stores with every store replied is undecidable"):
    // Rule G with every store replied. Only an unchanged primary object reaches it: a changed one would be below its
    // read threshold (D), since t + r > w. Two amnesiac stores of Replication(3, 2), a quorum, may have chosen abort;
    // or their votes, with store 2's, may have been commit-certain
    val lockedPrimary = txdWith(Replication(3, 2), Map(otherPoolId -> Replication(3, 2)),
      List(DataUpdate(otherPtr, rev0, DataUpdateOperation.Overwrite)), primaryReq = ExclusiveRevisionLock(op1, rev0))
    ExpiryTally.decide(lockedPrimary, Map(primary(0) -> amnesiac, primary(1) -> amnesiac, primary(2) -> holder(),
      secondary(0) -> holder(), secondary(1) -> holder(), secondary(2) -> holder())) shouldBe
      Decision.Hold(undecidable = true)

  test("a lock-only pool's commit voters count toward rule A"):
    val locked = txdWith(rs, Map(otherPoolId -> Replication(3, 2)), List(ExclusiveRevisionLock(otherPtr, rev0)))
    val primaryVotes = Map(primary(0) -> holder(), primary(1) -> holder(), primary(2) -> holder(),
      primary(3) -> holder(), primary(4) -> unknown)
    ExpiryTally.decide(locked, primaryVotes ++ Map(secondary(0) -> holder(), secondary(1) -> holder())) shouldBe
      Decision.Commit
    // One lock-only commit voter and two non-voters: that pool never had its write threshold of commit votes (E)
    ExpiryTally.decide(locked, primaryVotes ++ Map(secondary(0) -> holder(), secondary(1) -> unknown,
      secondary(2) -> unknown)) shouldBe Decision.Discard(dataLost = false)

  test("dataLost is set when the votes may have decided commit"):
    // Rule D: two holders of the update, short of the read threshold of three. Nothing was accepted, but the two
    // amnesiac stores may have voted commit with them: the client may have been told commit
    ExpiryTally.decide(txd, Map(primary(0) -> holder(), primary(1) -> holder(), primary(2) -> amnesiac,
      primary(3) -> amnesiac, primary(4) -> unknown)) shouldBe Decision.Discard(dataLost = true)

  // ---- Commit (A, B, F) ----

  test("commit accepts beyond width - writeThreshold and commit voters at the read threshold commit"):
    // cross-document-review 2.2: the leader, which also accepted commit, was rebuilt; store 4 never heard of T
    ExpiryTally.decide(txd, Map(primary(0) -> amnesiac, primary(1) -> acceptedCommit, primary(2) -> acceptedCommit,
      primary(3) -> acceptedCommit, primary(4) -> unknown)) shouldBe Decision.Commit
    // Two accepts suffice (any quorum that chose abort would include one of them); a voter need not have accepted
    ExpiryTally.decide(txd, Map(primary(0) -> amnesiac, primary(1) -> acceptedCommit, primary(2) -> acceptedCommit,
      primary(3) -> holder(), primary(4) -> unknown)) shouldBe Decision.Commit
    // An accept counts whatever the acceptor's own vote
    ExpiryTally.decide(txd, Map(primary(0) -> holder(), primary(1) -> holder(), primary(2) -> holder(),
      primary(3) -> holder(voted = false, accepted = Some(true)), primary(4) -> holder(voted = false,
        accepted = Some(true)))) shouldBe Decision.Commit

  test("a silent store is never counted as an accept or a vote toward commit"):
    ExpiryTally.decide(txd, Map(primary(0) -> amnesiac, primary(1) -> acceptedCommit, primary(2) -> acceptedCommit,
      primary(4) -> unknown)) shouldBe Decision.Hold(undecidable = false)

  // ---- Discard (C, D, E) ----

  test("too few commit voters to restore the update discard, as lost if the commit may have been chosen"):
    // Three commit accepts and an amnesiac: a commit may be chosen, but only two stores hold its update
    ExpiryTally.decide(txd, Map(primary(0) -> amnesiac, primary(1) -> acceptedCommit, primary(2) -> acceptedCommit,
      primary(3) -> holder(voted = false, accepted = Some(true)), primary(4) -> unknown)) shouldBe
      Decision.Discard(dataLost = true)
    // No commit can have been chosen: discarded, and nothing was lost
    ExpiryTally.decide(txd, Map(primary(0) -> unknown, primary(1) -> holder(), primary(2) -> holder(),
      primary(3) -> unknown, primary(4) -> unknown)) shouldBe Decision.Discard(dataLost = false)

  test("stuck holders short of the write threshold discard; at it they commit"):
    // HorizonIntegrationSuite "stuck holders short of a quorum all discard": three commit voters and two non-voters,
    // short of the write threshold of four, so no tally was commit-certain (E)
    ExpiryTally.decide(txd, Map(primary(0) -> holder(), primary(1) -> holder(), primary(2) -> holder(),
      primary(3) -> unknown, primary(4) -> unknown)) shouldBe Decision.Discard(dataLost = false)
    // Five commit voters that accepted nothing: under the old rules they discarded (no accept); a client may have
    // learned commit from their votes, so they commit (A)
    ExpiryTally.decide(txd, (0 until 5).map(i => primary(i) -> holder()).toMap) shouldBe Decision.Commit

  test("one commit accept with a rebuilt store commits: the accept shows the tally was commit-certain"):
    // Under the old rules one accept, with the amnesiac store 0, could not make a quorum, so it discarded. By L2 the
    // accept traces back to a commit-certain tally (or a proof), and three voters hold the update, its read threshold
    // (B)
    ExpiryTally.decide(txd, Map(primary(0) -> amnesiac, primary(1) -> acceptedCommit, primary(2) -> holder(),
      primary(3) -> holder(), primary(4) -> unknown)) shouldBe Decision.Commit

  test("a secondary pool's object below its read threshold discards as lost"):
    val two = withSecondary
    val primaryOk = Map(primary(0) -> amnesiac, primary(1) -> acceptedCommit, primary(2) -> acceptedCommit,
      primary(3) -> acceptedCommit, primary(4) -> unknown)
    ExpiryTally.decide(two, primaryOk ++ Map(secondary(0) -> holder(), secondary(1) -> holder(),
      secondary(2) -> unknown)) shouldBe Decision.Commit
    ExpiryTally.decide(two, primaryOk ++ Map(secondary(0) -> holder(), secondary(1) -> unknown,
      secondary(2) -> unknown)) shouldBe Decision.Discard(dataLost = true)
    // A silent secondary store may hold the update: hold
    ExpiryTally.decide(two, primaryOk ++ Map(secondary(0) -> holder(), secondary(1) -> unknown)) shouldBe
      Decision.Hold(undecidable = false)

  // ---- Hold (G) ----

  test("silent stores hold"):
    ExpiryTally.decide(txd, Map()) shouldBe Decision.Hold(undecidable = false)
    // Two provable non-voters with three stores silent: under the old rules this held; now no tally of five stores
    // with two non-voters reaches the write threshold of four, so it discards (E)
    ExpiryTally.decide(txd, Map(primary(0) -> unknown, primary(1) -> unknown)) shouldBe
      Decision.Discard(dataLost = false)
    // An amnesiac store and three commit voters, one store silent: the silent one may have accepted abort, and
    // the amnesiac one too (two non-commit votes would make an abort-certain tally)
    ExpiryTally.decide(txd, Map(primary(0) -> amnesiac, primary(1) -> holder(), primary(2) -> holder(),
      primary(3) -> holder())) shouldBe Decision.Hold(undecidable = false)

  test("a rebuilt leader that accepted commit with every store a commit voter commits"):
    // Replication(3, 2): its leader, which accepted commit, was rebuilt. Under the old rules this was undecidable (a
    // commit chosen by stores 0 and 1, or an abort by 0 and 2). By L1 no abort was ever accepted: stores 1 and 2 voted
    // commit, so no tally had more than w - t = 1 non-commit votes. Commit by votes (A)
    val r32 = txdWith(Replication(3, 2))
    ExpiryTally.decide(r32, Map(primary(0) -> amnesiac, primary(1) -> acceptedCommit, primary(2) -> holder())) shouldBe
      Decision.Commit

  // ---- A Tx that already knows the outcome (resolved, waiting for a TxFinalized that may never come) ----

  private def cached(status: TransactionStatus.Value) = outcome(status)
  private def heldOutcome(status: TransactionStatus.Value) =
    Some(TxStatus(status, finalized = false, late = false, holdsAcceptorState = true))

  test("a known-outcome Tx with more than width - writeThreshold non-holders concludes"):
    ExpiryTally.decide(txd, Map(primary(0) -> cached(TransactionStatus.Committed),
      primary(1) -> cached(TransactionStatus.Committed), primary(2) -> unknown), known = Some(true)) shouldBe
      Decision.Discard(dataLost = false)
    ExpiryTally.decide(txd, Map(primary(0) -> cached(TransactionStatus.Aborted), primary(1) -> unknown),
      known = Some(false)) shouldBe Decision.Discard(dataLost = false)

  test("a known-outcome Tx holds while too few non-holders show T finalized"):
    ExpiryTally.decide(txd, Map(primary(0) -> heldOutcome(TransactionStatus.Committed),
      primary(1) -> heldOutcome(TransactionStatus.Committed), primary(2) -> holder(),
      primary(3) -> cached(TransactionStatus.Committed)), known = Some(true)) shouldBe
      Decision.Hold(undecidable = false)
    // An amnesiac store is never a non-holder: it may have forgotten T undecided
    ExpiryTally.decide(txd, Map(primary(0) -> amnesiac, primary(1) -> unknown), known = Some(true)) shouldBe
      Decision.Hold(undecidable = false)

  test("a known commit still adopts a finalized or late commit"):
    ExpiryTally.decide(txd, Map(primary(0) -> outcome(TransactionStatus.Committed, finalized = true)),
      known = Some(true)) shouldBe Decision.Adopt(true, true)
    // A known abort adopts nothing: it concludes or holds by the classification
    ExpiryTally.decide(txd, Map(primary(0) -> outcome(TransactionStatus.Committed, finalized = true)),
      known = Some(false)) shouldBe Decision.Hold(undecidable = false)
