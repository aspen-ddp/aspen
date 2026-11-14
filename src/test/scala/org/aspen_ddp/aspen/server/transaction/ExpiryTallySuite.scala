package org.aspen_ddp.aspen.server.transaction

import org.aspen_ddp.aspen.common.ida.ReedSolomon
import org.aspen_ddp.aspen.common.network.TxStatusResponse.TxStatus
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.transaction.TransactionStatus
import org.aspen_ddp.aspen.server.store.FrontendSuite
import org.aspen_ddp.aspen.server.transaction.ExpiryTally.Decision
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** The pure decision of an expiring Tx from its peers' status replies (horizon design section 2, rules 1-4). The primary
  * pool's IDA has width 5 and write threshold 3, so more than 2 non-holders discard
  */
class ExpiryTallySuite extends AnyFunSuite with Matchers:
  import FrontendSuite.{otherPoolId, poolId}

  private val rs = ReedSolomon(5, 3, 3)
  private val txd = FrontendSuite.mktxd().copy(primaryObjectIDA = rs, poolIDAMap = Map(poolId -> rs))

  private def primary(i: Int): StoreId = StoreId(poolId, i.toByte)
  private def secondary(i: Int): StoreId = StoreId(otherPoolId, i.toByte)

  private val unknown: Option[TxStatus] = None
  private val holder: Option[TxStatus] = Some(TxStatus(TransactionStatus.Unresolved, finalized = false, late = false,
    holdsAcceptorState = true))
  private val muted: Option[TxStatus] = Some(TxStatus(TransactionStatus.Unresolved, finalized = false, late = false,
    holdsAcceptorState = false))
  private def outcome(status: TransactionStatus.Value, finalized: Boolean = false, late: Boolean = false) =
    Some(TxStatus(status, finalized, late, holdsAcceptorState = false))

  test("any outcome-bearing reply is adopted"):
    ExpiryTally.decide(txd, Map(primary(0) -> unknown, primary(1) -> unknown, primary(2) -> unknown,
      secondary(0) -> outcome(TransactionStatus.Committed, late = true))) shouldBe Decision.Adopt(true, true)
    ExpiryTally.decide(txd, Map(primary(0) -> outcome(TransactionStatus.Committed, finalized = true))) shouldBe
      Decision.Adopt(true, true)
    ExpiryTally.decide(txd, Map(primary(0) -> holder, primary(1) -> outcome(TransactionStatus.Committed))) shouldBe
      Decision.Adopt(true, false)
    ExpiryTally.decide(txd, Map(primary(0) -> holder, primary(1) -> outcome(TransactionStatus.Aborted))) match
      case Decision.Adopt(committed, _) => committed shouldBe false
      case other => fail(s"Expected an aborted Adopt, got $other")
    // A holder's Tx that resolved reports its outcome with its acceptor state: still an outcome
    ExpiryTally.decide(txd, Map(primary(0) -> Some(TxStatus(TransactionStatus.Aborted, finalized = false,
      late = false, holdsAcceptorState = true)))) shouldBe Decision.Adopt(false, false)

  test("three explicit primary-pool non-holders discard"):
    ExpiryTally.decide(txd, Map(primary(0) -> unknown, primary(1) -> unknown, primary(2) -> unknown)) shouldBe
      Decision.Discard

  test("two non-holders and three silent stores hold"):
    ExpiryTally.decide(txd, Map(primary(0) -> unknown, primary(1) -> unknown)) shouldBe Decision.Hold
    ExpiryTally.decide(txd, Map()) shouldBe Decision.Hold

  test("holders that cannot form a quorum with silent stores discard"):
    // The user's corner case: two stuck holders show T in progress to each other, and three stores never can
    ExpiryTally.decide(txd, Map(primary(0) -> holder, primary(1) -> holder, primary(2) -> unknown,
      primary(3) -> unknown, primary(4) -> unknown)) shouldBe Decision.Discard

  test("three holders keep holding"):
    ExpiryTally.decide(txd, Map(primary(0) -> holder, primary(1) -> holder, primary(2) -> holder,
      primary(3) -> unknown, primary(4) -> unknown)) shouldBe Decision.Hold
    // Two holders and one silent store can still form a quorum
    ExpiryTally.decide(txd, Map(primary(0) -> holder, primary(1) -> holder,
      primary(3) -> unknown, primary(4) -> unknown)) shouldBe Decision.Hold

  test("non-holders in a secondary pool do not count"):
    ExpiryTally.decide(txd, Map(primary(0) -> unknown, primary(1) -> unknown, secondary(0) -> unknown,
      secondary(1) -> unknown, secondary(2) -> unknown)) shouldBe Decision.Hold

  test("a muted Tx's reply counts as a non-holder"):
    ExpiryTally.decide(txd, Map(primary(0) -> muted, primary(1) -> unknown, primary(2) -> unknown)) shouldBe
      Decision.Discard
    ExpiryTally.decide(txd, Map(primary(0) -> muted, primary(1) -> unknown, primary(2) -> holder)) shouldBe
      Decision.Hold

  // ---- A Tx that already knows the outcome (resolved, waiting for a TxFinalized that may never come) ----

  private def cached(status: TransactionStatus.Value) = outcome(status)
  private def heldOutcome(status: TransactionStatus.Value) =
    Some(TxStatus(status, finalized = false, late = false, holdsAcceptorState = true))

  test("a known-outcome Tx with cache-only replies discards"):
    // Re-adopting a known, unfinalized outcome would change nothing: cache-only replies are non-holders
    ExpiryTally.decide(txd, Map(primary(0) -> cached(TransactionStatus.Committed),
      primary(1) -> cached(TransactionStatus.Committed), primary(2) -> unknown), known = Some(true)) shouldBe
      Decision.Discard
    ExpiryTally.decide(txd, Map(primary(0) -> cached(TransactionStatus.Aborted),
      primary(1) -> unknown, primary(2) -> unknown), known = Some(false)) shouldBe Decision.Discard

  test("a known-outcome Tx with acceptor-state holders holds"):
    ExpiryTally.decide(txd, Map(primary(0) -> heldOutcome(TransactionStatus.Committed),
      primary(1) -> heldOutcome(TransactionStatus.Committed), primary(2) -> holder,
      primary(3) -> cached(TransactionStatus.Committed), primary(4) -> unknown), known = Some(true)) shouldBe
      Decision.Hold

  test("a known commit still adopts a finalized or late commit"):
    ExpiryTally.decide(txd, Map(primary(0) -> outcome(TransactionStatus.Committed, finalized = true)),
      known = Some(true)) shouldBe Decision.Adopt(true, true)
    // A known abort adopts nothing: it concludes or holds by the classification
    ExpiryTally.decide(txd, Map(primary(0) -> outcome(TransactionStatus.Committed, finalized = true)),
      known = Some(false)) shouldBe Decision.Hold
