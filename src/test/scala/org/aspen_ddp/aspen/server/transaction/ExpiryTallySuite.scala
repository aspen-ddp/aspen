package org.aspen_ddp.aspen.server.transaction

import org.aspen_ddp.aspen.common.ida.{IDA, ReedSolomon, Replication}
import org.aspen_ddp.aspen.common.network.TxStatusResponse.TxStatus
import org.aspen_ddp.aspen.common.objects.{DataObjectPointer, ObjectId}
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.transaction.{DataUpdate, DataUpdateOperation, TransactionDescription,
  TransactionStatus}
import org.aspen_ddp.aspen.server.store.FrontendSuite
import org.aspen_ddp.aspen.server.transaction.ExpiryTally.Decision
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.util.UUID

/** The pure decision of an expiring Tx from its peers' status replies (ExpiryTally rules 1-4). The primary pool is
  * ReedSolomon(5, 3, 4): width 5, read threshold 3, write threshold 4, so `width - writeThreshold` is 1. T overwrites
  * one object of it
  */
class ExpiryTallySuite extends AnyFunSuite with Matchers:
  import FrontendSuite.{op1, otherPoolId, poolId, rev0}

  private val rs = ReedSolomon(5, 3, 4)

  private def txdWith(primary: IDA, others: Map[org.aspen_ddp.aspen.common.pool.PoolId, IDA] = Map(),
                      extra: List[DataUpdate] = Nil): TransactionDescription =
    FrontendSuite.mktxd().copy(primaryObjectIDA = primary, poolIDAMap = Map(poolId -> primary) ++ others,
      requirements = DataUpdate(op1, rev0, DataUpdateOperation.Overwrite) :: extra)

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

  // ---- Rule 2: commit ----

  test("commit accepts beyond width - writeThreshold and commit voters at the read threshold commit"):
    // G1: the leader, which also accepted commit, was rebuilt; store 4 never heard of T
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

  // ---- Rule 3: discard ----

  test("too few commit voters to restore the update discard, as lost if the commit may have been chosen"):
    // Three commit accepts and an amnesiac: a commit may be chosen, but only two stores hold its update
    ExpiryTally.decide(txd, Map(primary(0) -> amnesiac, primary(1) -> acceptedCommit, primary(2) -> acceptedCommit,
      primary(3) -> holder(voted = false, accepted = Some(true)), primary(4) -> unknown)) shouldBe
      Decision.Discard(dataLost = true)
    // No commit can have been chosen: discarded, and nothing was lost
    ExpiryTally.decide(txd, Map(primary(0) -> unknown, primary(1) -> holder(), primary(2) -> holder(),
      primary(3) -> unknown, primary(4) -> unknown)) shouldBe Decision.Discard(dataLost = false)

  test("stuck holders that accepted nothing discard"):
    // HorizonIntegrationSuite "stuck holders short of a quorum all discard": no accept, so no commit was chosen, and
    // frozen holders can never choose one. Three voters hold the update, its read threshold
    ExpiryTally.decide(txd, Map(primary(0) -> holder(), primary(1) -> holder(), primary(2) -> holder(),
      primary(3) -> unknown, primary(4) -> unknown)) shouldBe Decision.Discard(dataLost = false)
    // Five holders that accepted nothing: nothing was or can be chosen
    ExpiryTally.decide(txd, (0 until 5).map(i => primary(i) -> holder()).toMap) shouldBe
      Decision.Discard(dataLost = false)

  test("one commit accept with a rebuilt store discards: no commit can have been chosen"):
    // A chosen commit has four acceptors: at most store 1 and the amnesiac store 0 could be among them
    ExpiryTally.decide(txd, Map(primary(0) -> amnesiac, primary(1) -> acceptedCommit, primary(2) -> holder(),
      primary(3) -> holder(), primary(4) -> unknown)) shouldBe Decision.Discard(dataLost = false)

  test("a secondary pool's object below its read threshold discards as lost"):
    val other = ObjectId(new UUID(0, 99))
    val two = txdWith(rs, Map(otherPoolId -> Replication(3, 2)),
      List(DataUpdate(DataObjectPointer(other, otherPoolId, Array[Byte]()), rev0, DataUpdateOperation.Overwrite)))
    val primaryOk = Map(primary(0) -> amnesiac, primary(1) -> acceptedCommit, primary(2) -> acceptedCommit,
      primary(3) -> acceptedCommit, primary(4) -> unknown)
    ExpiryTally.decide(two, primaryOk ++ Map(secondary(0) -> holder(), secondary(1) -> holder(),
      secondary(2) -> unknown)) shouldBe Decision.Commit
    ExpiryTally.decide(two, primaryOk ++ Map(secondary(0) -> holder(), secondary(1) -> unknown,
      secondary(2) -> unknown)) shouldBe Decision.Discard(dataLost = true)
    // A silent secondary store may hold the update: hold
    ExpiryTally.decide(two, primaryOk ++ Map(secondary(0) -> holder(), secondary(1) -> unknown)) shouldBe
      Decision.Hold(undecidable = false)

  // ---- Rule 4: hold ----

  test("silent stores hold"):
    ExpiryTally.decide(txd, Map()) shouldBe Decision.Hold(undecidable = false)
    ExpiryTally.decide(txd, Map(primary(0) -> unknown, primary(1) -> unknown)) shouldBe
      Decision.Hold(undecidable = false)
    // One accept and one silent store: the silent one may have accepted commit too
    ExpiryTally.decide(txd, Map(primary(0) -> amnesiac, primary(1) -> acceptedCommit, primary(2) -> holder(),
      primary(3) -> holder())) shouldBe Decision.Hold(undecidable = false)

  test("a rebuilt store beyond the pool's tolerance, every store reported, is undecidable"):
    // Replication(3, 2) has no margin above its read threshold (2): its leader, which accepted commit, was rebuilt.
    // A commit chosen by stores 0 and 1, or an abort by 0 and 2 that store 2 has since forgotten: no tally tells them
    // apart
    val r32 = txdWith(Replication(3, 2))
    ExpiryTally.decide(r32, Map(primary(0) -> amnesiac, primary(1) -> acceptedCommit, primary(2) -> holder())) shouldBe
      Decision.Hold(undecidable = true)

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
