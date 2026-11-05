package org.aspen_ddp.aspen.client.internal.transaction

import java.util.UUID

import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.transaction.TransactionId
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class MissedUpdateReportSuite extends AnyFunSuite with Matchers:
  private val pool = PoolId(new UUID(0, 1))
  private val s0 = StoreId(pool, 0)
  private val s1 = StoreId(pool, 1)
  private val tx0 = TransactionId(new UUID(0, 2))
  private val tx1 = TransactionId(new UUID(0, 3))

  test("nothing is reported without marks, even for transactions that had none") {
    val report = new MissedUpdateFinalizationAction.MissedUpdateReport
    report.recordClean()

    report.take() shouldBe None
  }

  test("only the first marks of a report ask for it to be scheduled") {
    val report = new MissedUpdateFinalizationAction.MissedUpdateReport

    report.recordMarks(tx0, List(s0), 1, 300) shouldBe true
    report.recordMarks(tx1, List(s0), 1, 300) shouldBe false

    report.take() should not be empty
    report.recordMarks(tx1, List(s0), 1, 300) shouldBe true
  }

  test("a report totals its marks and the next one starts empty") {
    val report = new MissedUpdateFinalizationAction.MissedUpdateReport
    report.recordClean()
    report.recordMarks(tx0, List(s0, s0, s1), 2, 300)
    report.recordMarks(tx0, List(s0), 0, 500)
    report.recordMarks(tx1, List(s1), 1, 1000)

    val text = report.take().get
    text should include("5 marks (3 with no reply from the store, 2 with a commit error)")
    text should include("from 2 transactions in 3 executions")
    text should include("Transactions with no marks: 1")
    text should include("average 600ms, max 1000ms")
    text should include(s"Marks per store: $s0=3 $s1=2")

    report.take() shouldBe None
  }
