package org.aspen_ddp.aspen.common.util

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.duration.*
import scala.concurrent.{ExecutionContext, Future, Promise}

/** Tests for the single-flight guard behind scheduleNonConcurrentPollingTask.
 *
 *  The guard exists to stop polling reads from piling up while a store is offline, so every test
 *  here has to hold that line: "the next tick ran" is only ever correct when nothing is actually
 *  outstanding. The clock and the stall hook are injected rather than real so a stall threshold
 *  measured in minutes can be tested without sleeping through one.
 */
class SingleFlightSuite extends AnyFunSuite with Matchers:

  // Same-thread execution context: callbacks run inline, so every assertion below is about
  // ordering the guard controls rather than about scheduler timing.
  given ExecutionContext = ExecutionContext.parasitic

  /** A clock the test advances by hand. */
  class FakeClock:
    private var nanos: Long = 0L
    def read(): Long = nanos
    def advance(d: FiniteDuration): Unit = nanos += d.toNanos

  test("a pending invocation suppresses later ticks"):
    val calls = new AtomicInteger(0)
    val p = Promise[Unit]()
    val clock = new FakeClock

    val run = boundedSingleFlight("test", stallAfter = 10.minutes, clock = clock.read):
      calls.incrementAndGet()
      p.future

    run()
    run()
    run()
    calls.get should be(1)

  test("completion re-arms the guard"):
    val calls = new AtomicInteger(0)
    var p = Promise[Unit]()
    val clock = new FakeClock

    val run = boundedSingleFlight("test", stallAfter = 10.minutes, clock = clock.read):
      calls.incrementAndGet()
      p.future

    run()
    calls.get should be(1)
    p.success(())
    p = Promise[Unit]()
    run()
    calls.get should be(2)

  test("a failed Future releases the guard"):
    // Mode (b): the old implementation released only from fn.foreach, so a failure left the
    // flag set forever while nothing was outstanding.
    val calls = new AtomicInteger(0)
    val clock = new FakeClock

    val run = boundedSingleFlight("test", stallAfter = 10.minutes, clock = clock.read):
      calls.incrementAndGet()
      Future.failed(new Exception("boom"))

    run()
    run()
    run()
    calls.get should be(3)

  test("a synchronous throw releases the guard"):
    // Mode (a): fn used to be evaluated inside the monitor after the flag was set, so a throw
    // unwound past the release and wedged the task permanently -- silently, because
    // schedulePeriodic swallows it into a dropped Future.
    val calls = new AtomicInteger(0)
    val clock = new FakeClock

    val run = boundedSingleFlight("test", stallAfter = 10.minutes, clock = clock.read):
      calls.incrementAndGet()
      throw new Exception("boom")

    run()
    run()
    run()
    calls.get should be(3)

  test("no stall is reported before the threshold elapses"):
    val stalls = List.newBuilder[SingleFlightStall]
    val clock = new FakeClock
    val p = Promise[Unit]()

    val run = boundedSingleFlight("test", stallAfter = 10.minutes,
                                  clock = clock.read, onStall = s => stalls += s):
      p.future

    run()
    clock.advance(9.minutes)
    run()
    stalls.result() should be(empty)

  test("a suppressed tick past the threshold reports a stall with the suppressed count"):
    // Mode (c): the guard cannot tell a genuinely slow read from an orphaned promise, so it
    // must not guess -- but it must say something. This is the alarm that turns an invisible
    // permanent wedge into a diagnosable one.
    val stalls = List.newBuilder[SingleFlightStall]
    val clock = new FakeClock
    val p = Promise[Unit]()

    val run = boundedSingleFlight("usage-update", stallAfter = 10.minutes,
                                  clock = clock.read, onStall = s => stalls += s):
      p.future

    run()
    clock.advance(5.minutes)
    run() // suppressed, below threshold, no alarm
    clock.advance(6.minutes)
    run() // suppressed, past threshold, alarm

    val reported = stalls.result()
    reported.size should be(1)
    reported.head.name should be("usage-update")
    reported.head.outstandingFor should be(11.minutes)
    reported.head.inFlight should be(1)
    // Both suppressed ticks count, not just the one that tripped the alarm.
    reported.head.suppressedTicks should be(2)
    reported.head.startedExtra should be(false)

  test("the suppressed count resets once nothing is outstanding"):
    val stalls = List.newBuilder[SingleFlightStall]
    val clock = new FakeClock
    var p = Promise[Unit]()

    val run = boundedSingleFlight("test", stallAfter = 10.minutes,
                                  clock = clock.read, onStall = s => stalls += s):
      p.future

    run()
    clock.advance(11.minutes)
    run()
    p.success(())

    p = Promise[Unit]()
    run()
    clock.advance(11.minutes)
    run()

    stalls.result().map(_.suppressedTicks) should be(List(1, 1))

  test("at the default cap a stalled invocation is reported but never joined"):
    // maxInFlight = 1 must stay bit-for-bit the old behaviour: one read against an offline
    // store, however long the outage runs. This is the assertion that protects every existing
    // call site from the new parameter.
    val calls = new AtomicInteger(0)
    val clock = new FakeClock
    val p = Promise[Unit]()

    val run = boundedSingleFlight("test", stallAfter = 10.minutes, clock = clock.read):
      calls.incrementAndGet()
      p.future

    run()
    clock.advance(24.hours)
    run()
    run()
    calls.get should be(1)

  test("above a cap of one, each extra invocation costs a full stall interval"):
    val calls = new AtomicInteger(0)
    val clock = new FakeClock

    val run = boundedSingleFlight("test", stallAfter = 10.minutes, maxInFlight = 3,
                                  clock = clock.read):
      calls.incrementAndGet()
      Promise[Unit]().future // never completes

    run()
    calls.get should be(1)

    clock.advance(10.minutes)
    run()
    calls.get should be(2)

    // Gated on the newest invocation, not the oldest: the oldest is 10 minutes past the
    // threshold here, but the newest has only just started, so this tick is dropped.
    run()
    calls.get should be(2)

    clock.advance(10.minutes)
    run()
    calls.get should be(3)

  test("the cap holds however long the outage runs"):
    // The whole argument for a cap over a timeout. A timeout's worst case scales with the
    // outage; this one does not.
    val calls = new AtomicInteger(0)
    val clock = new FakeClock

    val run = boundedSingleFlight("test", stallAfter = 5.minutes, maxInFlight = 3,
                                  clock = clock.read):
      calls.incrementAndGet()
      Promise[Unit]().future // never completes

    // A day offline at a 20-second poll.
    (1 to 4320).foreach: _ =>
      run()
      clock.advance(20.seconds)

    calls.get should be(3)

  test("an extra invocation reports startedExtra and the count in flight"):
    val stalls = List.newBuilder[SingleFlightStall]
    val clock = new FakeClock

    val run = boundedSingleFlight("device-check", stallAfter = 10.minutes, maxInFlight = 2,
                                  clock = clock.read, onStall = s => stalls += s):
      Promise[Unit]().future

    run()
    clock.advance(11.minutes)
    run()

    val reported = stalls.result()
    reported.size should be(1)
    reported.head.startedExtra should be(true)
    reported.head.inFlight should be(1) // before the extra started
    reported.head.outstandingFor should be(11.minutes)

  test("a completed invocation frees a slot under the cap"):
    val calls = new AtomicInteger(0)
    val clock = new FakeClock
    val first = Promise[Unit]()

    val run = boundedSingleFlight("test", stallAfter = 10.minutes, maxInFlight = 2,
                                  clock = clock.read):
      if calls.incrementAndGet() == 1 then first.future else Promise[Unit]().future

    run()
    clock.advance(11.minutes)
    run() // extra admitted, 2 in flight, at the cap
    calls.get should be(2)

    run()
    calls.get should be(2) // capped

    first.success(())
    clock.advance(11.minutes)
    run() // a slot freed, and the survivor is well past the threshold
    calls.get should be(3)

  test("maxInFlight below one is rejected"):
    an[IllegalArgumentException] should be thrownBy:
      boundedSingleFlight("test", maxInFlight = 0)(Future.unit)

  test("fn is evaluated outside the lock"):
    // A re-entrant call from within fn would deadlock if the monitor were held across it. The
    // re-entrant call is correctly suppressed -- the outer invocation is still outstanding.
    val calls = new AtomicInteger(0)
    val clock = new FakeClock
    var run: () => Unit = () => ()

    run = boundedSingleFlight("test", stallAfter = 10.minutes, clock = clock.read):
      if calls.incrementAndGet() == 1 then
        run()
      Future.unit

    run()
    calls.get should be(1)
