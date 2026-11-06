package org.aspen_ddp.aspen.server.host

import org.aspen_ddp.aspen.common.metadata.StorageDeviceId
import org.aspen_ddp.aspen.common.store.StoreId
import scribe.Logging

import java.util.concurrent.Executor
import scala.concurrent.ExecutionContext

/** How a host runs its stores and its device-management control thread.
 *
 *  Two implementations:
 *  - SingleThreadThreading (production, `cmdline/Main.scala`): one thread, `aspen-host-control`, runs everything
 *    posted, in posting order, as the host's event loop did.
 *  - LockstepThreading (regression tests, `TestNetwork` and every test Host): posted work waits on one FIFO until the
 *    test drains it, and Future callbacks run on the test's own ExecutionContext.
 *
 *  See section 6 of docs/superpowers/specs/2026-11-05-host-refactor-design.md for the lockstep rules. */
trait HostThreading:
  /** Where every device-management Future callback runs. Single-threaded. */
  def control: ExecutionContext
  /** Where control events are posted (production: the control thread; lockstep: the FIFO). */
  def controlEvents: Executor
  /** Entry hop for work arriving from a foreign thread: network, I/O completion, timers, CRL callbacks.
   *  Production: runs inline. Lockstep: enqueues on the FIFO. */
  def dispatch: Executor
  /** Binds a store being loaded to an executor. Called on the control thread. */
  def assign(storeId: StoreId, deviceId: StorageDeviceId): StoreExecutor
  /** Called on the control thread once a store's close has completed. */
  def release(storeId: StoreId): Unit
  /** Begins running posted work; until then it waits. Lockstep: nothing to start, as only a drain runs work. */
  def start(): Unit
  def shutdown(): Unit

/** A single-threaded FIFO executor for one or more stores. */
trait StoreExecutor extends ExecutionContext

object HostThreading extends Logging:
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
