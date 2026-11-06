package org.aspen_ddp.aspen.server.host

import org.aspen_ddp.aspen.common.metadata.StorageDeviceId
import org.aspen_ddp.aspen.common.store.StoreId

import java.util.concurrent.Executor
import scala.concurrent.ExecutionContext

/** The production threading (`cmdline/Main.scala`): device management on one control thread, stores on a pool of
 *  at most `maxThreads` store threads (spec section 3).
 *
 *  - `control` and `controlEvents` are one non-daemon thread, `aspen-host-control`, as in SingleThreadThreading.
 *  - `dispatch` runs each task inline on the calling thread (network, I/O completion, timer or CRL callback), which
 *    routes it straight to the store's executor without a hop through the control thread.
 *  - Store threads, `aspen-store-N`, are daemon threads created lazily: `assign` adds one only when every existing
 *    thread already has a store and fewer than `maxThreads` exist, so a host never runs more store threads than it
 *    has ever had stores loaded at once. Otherwise the store goes to the thread with the fewest assigned stores, ties going to the
 *    lowest index. `release` frees the store's slot; threads are never stopped before `shutdown`.
 *
 *  A store thread starts as soon as `assign` creates it rather than waiting for `start`: `assign` is called on the
 *  control thread, which itself runs only after `start`, so in a host the two orders cannot differ.
 *
 *  The placement bookkeeping is read and written only by `assign` and `release`, which are called on the control
 *  thread (or a test's thread), so it needs no lock. `loops` is volatile only because `shutdown` may be called from
 *  another thread.
 *
 *  Control tasks queued before `shutdown` still run after it, and one of them may load a store. `assign` then returns
 *  a loop that is already shut down, so the store's work is dropped (with SerialLoop's warning) and no thread is left
 *  running that nothing would stop. */
class StoreThreadPool(maxThreads: Int = Runtime.getRuntime.availableProcessors()) extends HostThreading:
  require(maxThreads >= 1, s"maxThreads must be at least 1: $maxThreads")

  private val controlLoop = new SerialLoop("aspen-host-control", daemon = false)

  private val inline: Executor = task => task.run()

  @volatile private var shut = false
  @volatile private var loops = Vector.empty[SerialLoop]
  /** Every store loop this pool has created, including those created already shut down. Testing hook. */
  @volatile private var created = Vector.empty[SerialLoop]
  /** Stores assigned to each loop, by loop index. */
  private var counts = Vector.empty[Int]
  /** The loop index each assigned store runs on. */
  private var placement = Map.empty[StoreId, Int]

  override def control: ExecutionContext = controlLoop
  override def controlEvents: Executor = controlLoop
  override def dispatch: Executor = inline

  override def assign(storeId: StoreId, deviceId: StorageDeviceId): StoreExecutor =
    require(!placement.contains(storeId), s"$storeId is already assigned")
    if shut then
      // Never started, so no thread is left behind; work posted to it is dropped
      val dead = new SerialLoop(s"aspen-store-${loops.size}", daemon = true)
      dead.shutdown()
      created = created :+ dead
      dead
    else
      val index =
        if counts.forall(_ >= 1) && loops.size < maxThreads then
          val loop = new SerialLoop(s"aspen-store-${loops.size}", daemon = true)
          loop.start()
          created = created :+ loop
          loops = loops :+ loop
          // A shutdown racing this assign: it either saw the new loop in `loops` or is seen here (both volatile)
          if shut then
            loop.shutdown()
          counts = counts :+ 0
          loops.size - 1
        else
          counts.indices.minBy(i => (counts(i), i))
      counts = counts.updated(index, counts(index) + 1)
      placement += storeId -> index
      loops(index)

  override def release(storeId: StoreId): Unit =
    placement.get(storeId).foreach: index =>
      placement -= storeId
      counts = counts.updated(index, counts(index) - 1)

  override def start(): Unit = controlLoop.start()

  override def shutdown(): Unit =
    shut = true
    controlLoop.shutdown()
    loops.foreach(_.shutdown())

  private[host] def threadCount: Int = loops.size
  private[host] def createdLoops: Vector[SerialLoop] = created
  private[host] def assignedCounts: Vector[Int] = counts
