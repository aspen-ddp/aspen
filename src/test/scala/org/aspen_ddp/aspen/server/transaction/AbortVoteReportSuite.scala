package org.aspen_ddp.aspen.server.transaction

import java.util.UUID

import org.aspen_ddp.aspen.common.objects.ObjectId
import org.aspen_ddp.aspen.common.transaction.{LocalTimeError, RevisionMismatch, TransactionCollision, TransactionId}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class AbortVoteReportSuite extends AnyFunSuite with Matchers:
  private val ms = 1000000L
  private val o1 = ObjectId(new UUID(0, 1))
  private val o2 = ObjectId(new UUID(0, 2))
  private val tx1 = TransactionId(new UUID(0, 3))
  private val tx2 = TransactionId(new UUID(0, 4))

  private def report(): (AbortVoteReport, () => List[String]) =
    var reports = List[String]()
    (new AbortVoteReport(1000 * ms, s => reports = s :: reports), () => reports.reverse)

  test("votes are totalled by reason once the period ends") {
    val (r, reports) = report()
    r.record(tx1, List(Some(o1) -> RevisionMismatch(), Some(o2) -> RevisionMismatch()), now = 0)
    r.record(tx2, List(Some(o1) -> RevisionMismatch(), Some(o2) -> TransactionCollision(tx1)), now = 10 * ms)
    reports() shouldBe Nil

    r.record(tx2, List(None -> LocalTimeError()), now = 1000 * ms)
    val text = reports().head
    text should include("Abort votes over 1000ms: 3.")
    text should include(s"RevisionMismatch: 3 from 2 transactions on 2 objects (most: ${o1.uuid}=2 ${o2.uuid}=1)")
    text should include("TransactionCollision: 1 from 1 transactions on 1 objects")
    text should include("LocalTimeError: 1 from 1 transactions;")
  }

  test("the next report starts empty") {
    val (r, reports) = report()
    r.record(tx1, List(Some(o1) -> RevisionMismatch()), now = 0)
    r.record(tx1, List(Some(o1) -> RevisionMismatch()), now = 1000 * ms)
    r.record(tx2, List(Some(o2) -> RevisionMismatch()), now = 2000 * ms)
    reports().last should include("RevisionMismatch: 1 from 1 transactions on 1 objects")
  }
