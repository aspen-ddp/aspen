package org.aspen_ddp.aspen.compute.systemtask

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.util.UUID

class SystemTaskMessageSuite extends AnyFunSuite with Matchers:

  test("NewSystemTaskAdded round-trips"):
    val m = NewSystemTaskAdded(UUID.randomUUID())
    SystemTaskMessage.decode(SystemTaskMessage.encode(m)) shouldBe m

  test("SystemTaskComplete round-trips"):
    val m = SystemTaskComplete(UUID.randomUUID())
    SystemTaskMessage.decode(SystemTaskMessage.encode(m)) shouldBe m
