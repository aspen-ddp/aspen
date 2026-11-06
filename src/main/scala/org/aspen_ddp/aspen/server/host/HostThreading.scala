package org.aspen_ddp.aspen.server.host

import org.aspen_ddp.aspen.common.metadata.StorageDeviceId
import org.aspen_ddp.aspen.common.store.StoreId
import scribe.Logging

import java.util.concurrent.Executor
import scala.concurrent.ExecutionContext

/** How a host runs its stores and its device-management control thread.
 *
 *  Device management (the classes in `server.host` that DeviceControl wires together) runs on a dedicated
 *  single-threaded ExecutionContext, the control context (`control`). Every Future combinator in that code names the
 *  control context, so its callback runs on the control thread, and that code needs no locks: each handler and
 *  callback runs to completion before the next one starts, and all mutable device-management state is read and
 *  written only on that thread. This is deliberately unlike ordinary Scala Future usage, where callbacks run on a
 *  shared pool and shared state needs locks. It is an actor-style "single owner" model built from plain Futures, and
 *  it needs these rules:
 *  1. **Device-management classes must not have a general-purpose `given ExecutionContext` in scope.** The only
 *     `given` is the control context, so a callback with no context named does not compile into "runs somewhere on a
 *     pool". Work that must run elsewhere (an FSIF open on `FsifOpenEc`, client transactions) names its context
 *     explicitly, and its result comes back through a control-context callback.
 *  2. **Never block the control thread on a Future** (`Await`, `.get` on a Java future, a `LinkedBlockingQueue.take`
 *     waiting for another thread's answer). The thread would deadlock against its own callbacks. Slow local disk
 *     work (marker files, directory moves, fsyncs through `DurableFiles`) is allowed there, because those operations
 *     are rare.
 *  3. **State owned by the control thread is never read from other threads.** Other threads that need an answer
 *     either post a request and receive a Future, or read an immutable snapshot that the control thread publishes
 *     through a `@volatile` field (see `HostStoreView` below).
 *  4. **Store state is never touched from the control thread,** and the reverse also holds.
 *
 *  Rule 4 is enforced by ownership (spec section 5): before `StoreRouter.install` and after a store's close has
 *  completed, its backend and directory belong to the control side; in between, the Store and its backend belong to
 *  its executor. The control side may then read only the backend's immutable fields (`storeId`, `path`,
 *  `crlSaveFile`) and FSIF's `@volatile` `hasFailed` and `isWriteFrozen`; anything else is posted to the executor
 *  (`StoreRouter.onStore`, or the close sequence in StoreLoader) and its result returned through a Future whose
 *  callbacks name the control context. SingleThreadThreading and LockstepThreading run stores inline, on the thread
 *  that posts to them, so under them the hop costs nothing and the order is the event loop's.
 *
 *  `HostStoreView` is that snapshot: DeviceControl.publishView republishes it after every change to the state it
 *  reflects. Callbacks that are not Futures (timers, FSIF's onFatal, the FSIF open on fsifOpenEc) run on whatever
 *  thread invokes them, so each posts to `controlEvents` before touching control state; BackgroundControlScheduler
 *  does this for delayed work. Every place that deliberately leaves the control context says, in a comment, where the
 *  work runs and how its result comes back.
 *
 *  Three implementations:
 *  - StoreThreadPool (production, `cmdline/Main.scala`): `aspen-host-control` runs device management, and stores
 *    run on at most `maxThreads` daemon `aspen-store-N` threads, created lazily and placed least-loaded first.
 *  - SingleThreadThreading: one thread, `aspen-host-control`, runs everything posted, in posting order, as the
 *    host's event loop did.
 *  - LockstepThreading (regression tests, `TestNetwork` and every test Host): posted work waits on one FIFO until the
 *    test drains it, and Future callbacks run on the test's own ExecutionContext, which is single-threaded and runs
 *    on the test thread, so the single-owner rule still holds.
 *
 *  See sections 2 and 6 of docs/superpowers/specs/2026-11-05-host-refactor-design.md. */
trait HostThreading:
  /** Where every device-management Future callback runs. Single-threaded. */
  def control: ExecutionContext
  /** Where control events are posted (production: the control thread; lockstep: the FIFO). */
  def controlEvents: Executor
  /** Entry hop for work arriving from a foreign thread: network, I/O completion, timers, CRL callbacks. StoreRouter
   *  runs its store lookup here, then hands the work to the store's executor.
   *  StoreThreadPool: runs inline on the calling thread, which hands the work to the store's executor itself.
   *  SingleThreadThreading: enqueues on the control thread, where stores still run. Lockstep: enqueues on the FIFO. */
  def dispatch: Executor
  /** Binds a store being loaded to an executor. Called on the control thread.
   *  StoreThreadPool: a store thread with the fewest assigned stores, ties going to the lowest index; a new thread is
   *  created only when every existing one has a store and fewer than `maxThreads` exist. SingleThreadThreading and
   *  Lockstep: runs the store's work inline on the thread that posts it. StoreThreadPool throws AlreadyAssigned,
   *  changing nothing, for a store whose earlier slot `release` has not freed yet (its close is still running). */
  def assign(storeId: StoreId, deviceId: StorageDeviceId): StoreExecutor
  /** Called on the control thread once a store's close has completed. StoreThreadPool: frees the store's slot on
   *  its thread; threads are never stopped before `shutdown`. */
  def release(storeId: StoreId): Unit
  /** Begins running posted work; until then it waits. Lockstep: nothing to start, as only a drain runs work. */
  def start(): Unit
  def shutdown(): Unit

/** A single-threaded FIFO executor for one or more stores. */
trait StoreExecutor extends ExecutionContext

object HostThreading extends Logging:
  /** Thrown by `assign` for a store that still holds a slot: its earlier close has not called `release` yet. The
   *  LoadStore handler fails that load with it. */
  final case class AlreadyAssigned(storeId: StoreId)
    extends Exception(s"store $storeId is already assigned to an executor: its earlier close has not released it")

  /** A posted task that names itself in the log line written when it throws. */
  trait Named extends Runnable:
    /** Must not throw: it is called inside the catch. */
    def name: String

  /** Runs one posted task, logging and swallowing anything it throws. Production only: LockstepThreading lets a
   *  throw propagate to the test.
   *
   *  Swallowing and continuing is deliberate even though Aspen is crash-only: a single poisoned task must not take
   *  down an otherwise-healthy host, and tasks run one at a time with no loop state carried between them. Left to
   *  escape, the throw would end the loop thread with no log line at all, and the host would go silently deaf while
   *  still appearing to run.
   *
   *  The trade is that a throw part-way through a handler leaves whatever that handler had already mutated
   *  half-updated, and LoadStore, Repair and ShutdownStore each carry a Promise completed by the handler or by work
   *  the handler starts, so that promise stays uncompleted and its awaiter hangs forever. Neither is new -- a dead
   *  loop left both too -- but a dead loop hung everything at once and so was obvious, whereas now one caller hangs
   *  inside a host that still looks healthy. This log line is the only signal that it happened.
   *
   *  Only the task's name (by default its class name) is interpolated: this call is inside the catch, so a throwing
   *  toString on the task's payload would escape and kill the loop anyway. The stack trace names the handler. */
  private[host] def runLogged(task: Runnable): Unit =
    try task.run()
    catch
      case t: Throwable =>
        val name = task match
          case n: Named => n.name
          case _ => task.getClass.getSimpleName
        logger.error(s"Unhandled exception processing event $name", t)

  /** Runs each task on the calling thread, which is already the one the store belongs on. A throw propagates to the
   *  caller: the task that posted it, which SingleThreadThreading's loop logs, or a lockstep drain. */
  private[host] object InlineStoreExecutor extends StoreExecutor:
    override def execute(runnable: Runnable): Unit = runnable.run()
    override def reportFailure(cause: Throwable): Unit = logger.error("Unhandled exception in a store task", cause)
