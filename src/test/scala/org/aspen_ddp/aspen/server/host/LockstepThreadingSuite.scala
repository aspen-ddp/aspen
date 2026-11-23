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

  test("a throwing task propagates from drain and later tasks stay queued"):
    val t = threading
    val boom = new RuntimeException("expected by the test")
    var ran = false
    t.dispatch.execute(() => throw boom)
    t.dispatch.execute(() => ran = true)
    val thrown = intercept[RuntimeException](t.drain())
    thrown should be theSameInstanceAs boom
    ran should be(false)
    t.drain()
    ran should be(true)

  test("device jobs run on the FIFO in order"):
    val t = threading
    val io = t.newDeviceIo(os.pwd / "dev-a")
    val order = ListBuffer[String]()
    val h1 = io.submit(JobKind.Scan, None)({ order += "one"; 1 })
    val h2 = io.submit(JobKind.Markers, None)({ order += "two"; 2 })
    order shouldBe empty
    h1.future.isCompleted should be(false)
    t.drain()
    order.toList should be(List("one", "two"))
    h1.future.value.map(_.get) should be(Some(1))
    h2.future.value.map(_.get) should be(Some(2))
    io.close()

  test("a held path keeps its jobs until released"):
    val t = threading
    val p = os.pwd / "dev-held"
    val q = os.pwd / "dev-other"
    val io = t.newDeviceIo(p)
    val other = t.newDeviceIo(q)
    t.hold(p)
    var ranHeld = false
    var ranOther = false
    val h = io.submit(JobKind.Scan, None)({ ranHeld = true })
    other.submit(JobKind.Scan, None)({ ranOther = true })
    t.drain()
    ranHeld should be(false)
    ranOther should be(true)
    t.held(p) should be(List(JobKind.Scan))
    t.release(p)
    t.drain()
    ranHeld should be(true)
    h.future.isCompleted should be(true)
    t.held(p) should be(Nil)
    io.close()
    other.close()
