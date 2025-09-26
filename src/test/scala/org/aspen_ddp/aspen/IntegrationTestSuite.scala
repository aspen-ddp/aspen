package org.aspen_ddp.aspen

import org.aspen_ddp.aspen.client.AspenClient
import org.aspen_ddp.aspen.common.objects.KeyValueObjectPointer
import org.scalatest.{FutureOutcome, Tag, compatible}
import org.scalatest.funsuite.AsyncFunSuite
import org.scalatest.matchers.should.Matchers

import scala.concurrent.{Await, Future}
import scala.concurrent.duration.{Duration, MILLISECONDS}

class IntegrationTestSuite  extends AsyncFunSuite with Matchers { //with BeforeAndAfter {
  var net: TestNetwork = scala.compiletime.uninitialized
  var client: AspenClient = scala.compiletime.uninitialized
  var radicle: KeyValueObjectPointer = scala.compiletime.uninitialized
  var testName: String = "NO_TEST"

  def subFixtureSetup(): Unit = {}
  def subFixtureTeardown(): Unit = ()

  inline def atest(testName: String, testTags: Tag*)(testFun: => Future[compatible.Assertion]): Unit = {
    test(testName, testTags*):
      testFun.flatMap: result =>
        net.waitForTransactionsToComplete().map: _ =>
          result
  }

  override def withFixture(test: NoArgAsyncTest): FutureOutcome = {
    net = new TestNetwork(executionContext)
    client = net.client
    testName = test.name
    radicle = net.radicle
    client.setSystemAttribute("unittest.name", test.name)

    subFixtureSetup()

    complete {
      super.withFixture(test)
    } lastly {

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

}
