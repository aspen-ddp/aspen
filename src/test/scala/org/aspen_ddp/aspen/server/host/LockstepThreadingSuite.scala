package org.aspen_ddp.aspen.server.host

import org.aspen_ddp.aspen.common.metadata.StorageDeviceId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.pool.PoolId
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.util.UUID
import scala.collection.mutable.ListBuffer
import scala.concurrent.ExecutionContext

class LockstepThreadingSuite extends AnyFunSuite with Matchers:

  private def threading = new LockstepThreading(ExecutionContext.parasitic)

  test("tasks run in posting order across dispatch and controlEvents"):
    val t = threading
    val order = ListBuffer[String]()
    t.dispatch.execute(() => order += "A")
    t.controlEvents.execute(() => order += "B")
    t.dispatch.execute(() => order += "C")
    t.drain()
    order.toList should be(List("A", "B", "C"))

  test("nothing runs until drain"):
    val t = threading
    var ran = false
    t.dispatch.execute(() => ran = true)
    ran should be(false)
    t.hasQueued should be(true)
    t.drain()
    ran should be(true)
    t.hasQueued should be(false)

  test("tasks posted while draining run in the same drain"):
    val t = threading
    val order = ListBuffer[String]()
    t.controlEvents.execute: () =>
      order += "first"
      t.dispatch.execute(() => order += "second")
    t.drain()
    order.toList should be(List("first", "second"))

  test("assign returns an inline executor"):
    val t = threading
    val exec = t.assign(StoreId(PoolId(new UUID(0, 1)), 0), StorageDeviceId(new UUID(0, 2)))
    var ranOn: Option[Thread] = None
    exec.execute(() => ranOn = Some(Thread.currentThread))
    ranOn should be(Some(Thread.currentThread))

  test("a throwing task does not stop the queue"):
    val t = threading
    var ran = false
    t.dispatch.execute(() => throw new RuntimeException("expected by the test"))
    t.dispatch.execute(() => ran = true)
    t.drain()
    ran should be(true)
