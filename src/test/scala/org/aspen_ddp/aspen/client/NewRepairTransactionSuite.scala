package org.aspen_ddp.aspen.client

import org.aspen_ddp.aspen.IntegrationTestSuite

import scala.concurrent.Future

class NewRepairTransactionSuite extends IntegrationTestSuite:

  test("an ordinary transaction tracks missed updates"):
    Future.successful:
      client.newTransaction().missedUpdateTrackingEnabled shouldBe true

  test("a repair transaction does not track missed updates"):
    Future.successful:
      client.newRepairTransaction().missedUpdateTrackingEnabled shouldBe false
