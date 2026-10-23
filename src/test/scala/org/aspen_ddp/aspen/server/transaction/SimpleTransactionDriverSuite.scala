package org.aspen_ddp.aspen.server.transaction

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.concurrent.duration.*

class SimpleTransactionDriverSuite extends AnyFunSuite with Matchers {

  // A live driver re-broadcasts only every maxDelay, so a shorter failure threshold would have
  // the other stores start competing drivers for a transaction whose leader is merely slow.
  test("factory waits longer than the driver's re-broadcast interval before declaring a driver failed") {
    val maxDelay: Duration = 3.seconds
    val factory = SimpleTransactionDriver.factory(100.milliseconds, maxDelay)

    factory.failedDriverDuration should be > maxDelay
  }
}
