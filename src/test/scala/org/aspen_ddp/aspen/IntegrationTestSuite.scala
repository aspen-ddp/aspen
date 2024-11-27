package org.aspen_ddp.aspen

import java.util.concurrent.TimeoutException

import org.aspen_ddp.aspen.client.AspenClient
import org.aspen_ddp.aspen.common.objects.KeyValueObjectPointer
import org.scalatest.{BeforeAndAfter, FutureOutcome}
import org.scalatest.funsuite.AsyncFunSuite
import org.scalatest.matchers.should.Matchers

import scala.concurrent.{Await, Future}
import scala.concurrent.duration.{Duration, MILLISECONDS}

class IntegrationTestSuite  extends AsyncFunSuite with Matchers { //with BeforeAndAfter {
  var net: TestNetwork = _
  var client: AspenClient = _
  var radicle: KeyValueObjectPointer = _
  var testName: String = "NO_TEST"

  /*
  after {
    try {
      Await.result(net.waitForTransactionsToComplete(), Duration(5000, MILLISECONDS))
    } catch {
      case e: TimeoutException =>
        println(s"TEST LEFT TRANSACTIONS UNFINISHED: $testName")
        net.printTransactionStatus()
        throw e
    }

    net = null
    client = null
    radicle = null
    testName = "NO_TEST"
  }*/
  def subFixtureSetup(): Unit = {}
  def subFixtureTeardown(): Unit = ()

  override def withFixture(test: NoArgAsyncTest): FutureOutcome = {
    net = new TestNetwork
    client = net.client
    testName = test.name
    radicle = net.radicle
    client.setSystemAttribute("unittest.name", test.name)

    subFixtureSetup()

    complete {
      super.withFixture(test)
    } lastly {
      try {
        Await.result(net.waitForTransactionsToComplete(), Duration(5000, MILLISECONDS))
      } catch {
        case e: TimeoutException =>
          println(s"TEST LEFT TRANSACTIONS UNFINISHED: $testName")
          net.printTransactionStatus()
          throw e
      }

      subFixtureTeardown()
      client.shutdown()

      net = null
      client = null
      radicle = null
      testName = "NO_TEST"
    }
  }

  def waitForTransactionsToComplete(): Future[Unit] = net.waitForTransactionsToComplete()

  def handleEvents(): Unit = net.handleEvents()

  def waitForIt(errMsg: String)(fn: => Boolean): Future[Unit] = Future {

    var count = 0
    while (!fn && count < 500) {
      count += 1
      Thread.sleep(5)
    }

    if (count >= 500)
      throw new Exception(errMsg)
  }

}
