package org.aspen_ddp.aspen.common.transaction

import org.aspen_ddp.aspen.server.store.Frontend
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.concurrent.duration.*

class TransactionHorizonSuite extends AnyFunSuite with Matchers:

  test("the client gives up one MaxStartLead past the horizon"):
    TransactionHorizon.clientGiveUp(20.seconds) should be(22.seconds)
    TransactionHorizon.clientGiveUp(500.millis) should be(2500.millis)

  test("Frontend.MaxStartLead is the horizon's"):
    Frontend.MaxStartLead should be(TransactionHorizon.MaxStartLead)

  test("the status cache outlasts the horizon, the start lead and the idle self-drive delay"):
    TransactionHorizon.statusCacheDuration(TransactionHorizon.Horizon, 1.second) should be(30.seconds)
    TransactionHorizon.statusCacheDuration(TransactionHorizon.Horizon, 500.millis) should be(26.seconds)
