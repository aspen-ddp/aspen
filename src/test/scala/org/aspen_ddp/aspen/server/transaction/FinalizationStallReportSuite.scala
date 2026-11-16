package org.aspen_ddp.aspen.server.transaction

import org.aspen_ddp.aspen.common.transaction.TransactionId
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.util.UUID
import scala.concurrent.duration.*

class FinalizationStallReportSuite extends AnyFunSuite with Matchers:
  import TransactionDriverSuite.ds0

  private def stall(seconds: Int, failures: Int = 0, err: Option[Throwable] = None) =
    FinalizationStall(TransactionId(UUID.randomUUID()), seconds.seconds, failures, err)

  test("nothing is reported without stalls") {
    val report = new FinalizationStallReport(ds0)
    report.report(Nil, 0L) should be (None)
    report.report(Nil, 1.hour.toNanos) should be (None)
  }

  test("the first stall is reported at once, then at most once per period while stalls remain") {
    val report = new FinalizationStallReport(ds0)
    val s = List(stall(61))
    report.report(s, 10L).isDefined should be (true)
    report.report(s, 10L + FinalizationStallReport.Every.toNanos - 1) should be (None)
    report.report(s, 10L + FinalizationStallReport.Every.toNanos).isDefined should be (true)
    // Cleared stalls reset the period: a new stall is reported at once
    report.report(Nil, 10L + FinalizationStallReport.Every.toNanos + 1) should be (None)
    report.report(s, 10L + FinalizationStallReport.Every.toNanos + 2).isDefined should be (true)
  }

  test("the line names the store, the count and the oldest stall with its failures, and caps the others") {
    val oldest = stall(300, failures = 7, err = Some(new Exception("marker gave up")))
    val others = (1 to 15).map(i => stall(61 + i)).toList
    val line = new FinalizationStallReport(ds0).report(others ++ (oldest :: Nil), 0L).get
    line should startWith ("FINALIZATION STALLED")
    line should include (ds0.toString)
    line should include ("16 committed transactions")
    line should include (s"oldest ${oldest.transactionId}")
    line should include ("7 finalizer failures")
    line should include ("marker gave up")
    line should include ("and 5 more")
    others.sortBy(_.age).reverse.take(10).count(s => line.contains(s.transactionId.toString)) should be (10)
  }
