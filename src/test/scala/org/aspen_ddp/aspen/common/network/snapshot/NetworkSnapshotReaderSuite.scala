package org.aspen_ddp.aspen.common.network.snapshot

import org.aspen_ddp.aspen.common.HLCTimestamp
import org.aspen_ddp.aspen.common.network.{CachedHost, CachedStore, MetadataManager, MetadataManagerFixture,
  NetworkCacheContent, RecordingNetworkCache}
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.util.BackgroundTaskManager
import org.aspen_ddp.aspen.common.util.BackgroundTaskManager.ScheduledTask
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.collection.mutable
import scala.concurrent.duration.*
import scala.concurrent.{ExecutionContext, Future, Promise}

/** NetworkSnapshotReader against a real MetadataManager, a scripted fetch and a BackgroundTaskManager that only
 *  records what it is asked to schedule. ExecutionContext.parasitic runs every continuation inline, so a read
 *  whose fetch has completed has finished by the time start() or a fired task returns.
 */
class NetworkSnapshotReaderSuite extends AnyFunSuite
    with Matchers
    with MetadataManagerFixture:

  private given ExecutionContext = ExecutionContext.parasitic

  private val period = 24.hours
  private val minute: Long = 60 * 1000L
  private val T: Long = 1_000_000_000L
  private def at(ms: Long) = HLCTimestamp(ms << 16)

  private val store0 = StoreId(unknownPoolId, 0.toByte)

  private def remote(ms: Long, address: String = "1.2.3.4") =
    CachedHost(remoteHostId, "remote_host", address, 1, 2, 3, at(ms))

  private def seeded(ms: Long) =
    new RecordingNetworkCache(NetworkCacheContent(
      List(CachedStore(store0, remoteHostId, at(ms))), List(remote(ms))))

  /** One scheduling request: `random` for scheduleRandomlyWithinWindow, whose `delay` is the window */
  private final class Requested(val delay: Duration, val random: Boolean, fn: => Unit) extends ScheduledTask:
    @volatile var cancelled = false
    def cancel(): Unit = cancelled = true
    def fire(): Unit = fn

  private final class RecordingTasks extends BackgroundTaskManager(ExecutionContext.parasitic):
    val requested: mutable.ListBuffer[Requested] = mutable.ListBuffer()

    override def schedule(delay: Duration)(fn: => Unit): ScheduledTask =
      val r = new Requested(delay, false, fn)
      requested += r
      r

    override def scheduleRandomlyWithinWindow(window: Duration)(fn: => Unit): ScheduledTask =
      val r = new Requested(window, true, fn)
      requested += r
      r

  /** A fetch answering from `answers` in order, repeating the last one */
  private final class ScriptedFetch(answers: (() => Future[Option[(HostSnapshot, StoreSnapshot)]])*):
    var calls = 0
    def apply(): Future[Option[(HostSnapshot, StoreSnapshot)]] =
      val a = answers(math.min(calls, answers.size - 1))
      calls += 1
      a()

  private def some(h: HostSnapshot, s: StoreSnapshot): () => Future[Option[(HostSnapshot, StoreSnapshot)]] =
    () => Future.successful(Some((h, s)))

  private val none: () => Future[Option[(HostSnapshot, StoreSnapshot)]] = () => Future.successful(None)

  private def newReader(mgr: MetadataManager[MetadataManager.HostEntry], fetch: ScriptedFetch, tasks: RecordingTasks,
                        retired: mutable.ListBuffer[MetadataManager.HostEntry] = mutable.ListBuffer()) =
    new NetworkSnapshotReader[MetadataManager.HostEntry](mgr, () => fetch(), tasks, e => retired += e, period)

  private val emptySnapshots = some(HostSnapshot(at(T), Nil), StoreSnapshot(at(T), Nil))

  test("an empty cache reads immediately"):
    val (mgr, _, _) = newManager(new RecordingNetworkCache())
    val fetch = ScriptedFetch(emptySnapshots)
    val tasks = new RecordingTasks

    newReader(mgr, fetch, tasks).start()

    fetch.calls should be(1)
    tasks.requested.size should be(1)
    tasks.requested.head.random should be(true)

  private val failed: () => Future[Option[(HostSnapshot, StoreSnapshot)]] =
    () => Future.failed(new RuntimeException("read failed"))

  private def retries(tasks: RecordingTasks): List[Requested] = tasks.requested.filterNot(_.random).toList

  test("a failed empty-cache startup read retries after 1, 5 and 30 minutes, then leaves it to the schedule"):
    val (mgr, _, _) = newManager(new RecordingNetworkCache())
    val fetch = ScriptedFetch(failed)
    val tasks = new RecordingTasks
    newReader(mgr, fetch, tasks).start()

    fetch.calls should be(1)
    retries(tasks).map(_.delay) should be(List(1.minute))
    retries(tasks).last.fire()
    retries(tasks).map(_.delay) should be(List(1.minute, 5.minutes))
    retries(tasks).last.fire()
    retries(tasks).map(_.delay) should be(List(1.minute, 5.minutes, 30.minutes))
    retries(tasks).last.fire()

    fetch.calls should be(4)
    retries(tasks).size should be(3)
    tasks.requested.count(_.random) should be(1)

  test("startup retries end once a read succeeds"):
    val (mgr, _, _) = newManager(new RecordingNetworkCache())
    val fetch = ScriptedFetch(failed,
      some(HostSnapshot(at(T), List(remote(T))), StoreSnapshot(at(T), List(CachedStore(store0, remoteHostId, at(T))))))
    val tasks = new RecordingTasks
    newReader(mgr, fetch, tasks).start()

    retries(tasks).head.fire()

    fetch.calls should be(2)
    retries(tasks).size should be(1)
    mgr.peekHostEntry(remoteHostId).map(_.address) should be(Some("1.2.3.4"))

  test("a pending startup retry is skipped once the scheduled read has succeeded"):
    val (mgr, _, _) = newManager(new RecordingNetworkCache())
    val fetch = ScriptedFetch(failed, emptySnapshots)
    val tasks = new RecordingTasks
    newReader(mgr, fetch, tasks).start()
    val retry = retries(tasks).head

    tasks.requested.find(_.random).get.fire()
    fetch.calls should be(2)
    retry.fire()

    fetch.calls should be(2)
    retries(tasks).map(_.delay) should be(List(1.minute, period))

  test("a seeded cache does not retry a failed read early"):
    val (mgr, _, _) = newManager(seeded(T))
    val fetch = ScriptedFetch(failed)
    val tasks = new RecordingTasks
    newReader(mgr, fetch, tasks).start()

    tasks.requested.head.fire()

    retries(tasks).map(_.delay) should be(List(period))

  test("stop cancels a pending startup retry"):
    val (mgr, _, _) = newManager(new RecordingNetworkCache())
    val fetch = ScriptedFetch(failed)
    val tasks = new RecordingTasks
    val reader = newReader(mgr, fetch, tasks)
    reader.start()

    reader.stop()

    retries(tasks).head.cancelled should be(true)
    retries(tasks).head.fire()
    fetch.calls should be(1)

  test("a seeded cache waits for its random slot"):
    val (mgr, _, _) = newManager(seeded(T))
    val fetch = ScriptedFetch(emptySnapshots)
    val tasks = new RecordingTasks

    newReader(mgr, fetch, tasks).start()

    fetch.calls should be(0)
    tasks.requested.size should be(1)
    val slot = tasks.requested.head
    slot.random should be(true)
    // scheduleRandomlyWithinWindow draws nextInt(window.length) in the window's own unit: milliseconds give a
    // slot anywhere in the day rather than on one of 24 hour boundaries
    slot.delay.unit should be(MILLISECONDS)
    slot.delay.length should be(period.toMillis)

  test("each read schedules the next one period later"):
    val (mgr, _, _) = newManager(seeded(T))
    val fetch = ScriptedFetch(emptySnapshots)
    val tasks = new RecordingTasks
    newReader(mgr, fetch, tasks).start()

    tasks.requested.head.fire()

    fetch.calls should be(1)
    tasks.requested.size should be(2)
    tasks.requested(1).random should be(false)
    tasks.requested(1).delay should be(period)

    tasks.requested(1).fire()

    fetch.calls should be(2)
    tasks.requested.size should be(3)
    tasks.requested(2).delay should be(period)

  test("a snapshot no newer than the last applied is skipped"):
    val cache = seeded(T - 11 * minute)
    val (mgr, _, _) = newManager(cache)
    val old = mgr.peekHostEntry(remoteHostId).get
    val retired = mutable.ListBuffer[MetadataManager.HostEntry]()
    val fetch = ScriptedFetch(some(HostSnapshot(at(T), Nil), StoreSnapshot(at(T), Nil)))
    val tasks = new RecordingTasks
    newReader(mgr, fetch, tasks, retired).start()

    tasks.requested.head.fire()
    tasks.requested(1).fire()

    fetch.calls should be(2)
    // Every applySnapshot call writes the cache exactly once
    cache.bulkUpdates.size should be(1)
    retired.toList should be(List(old))

  test("each snapshot is skipped independently"):
    // Remote is held at T - 30min; a host snapshot scanned at T - 15min that omitted it would drop it
    val cache = seeded(T - 30 * minute)
    val (mgr, _, _) = newManager(cache)
    val fetch = ScriptedFetch(
      some(HostSnapshot(at(T - 15 * minute), List(remote(T - 30 * minute))), StoreSnapshot(at(T - 15 * minute), Nil)),
      // Same host scanStart, now omitting remote: skipped, so remote is not dropped. The store snapshot is newer.
      some(HostSnapshot(at(T - 15 * minute), Nil),
        StoreSnapshot(at(T), List(CachedStore(store0, otherHostId, at(T))))))
    val tasks = new RecordingTasks
    newReader(mgr, fetch, tasks).start()

    tasks.requested.head.fire()
    tasks.requested(1).fire()

    cache.bulkUpdates.size should be(2)
    mgr.peekHostEntry(remoteHostId).map(_.address) should be(Some("1.2.3.4"))
    mgr.peekStoreHost(store0) should be(Some(otherHostId))
    cache.removedHosts should be(empty)

  test("a Zero snapshot does not block the first real one"):
    val cache = new RecordingNetworkCache()
    val (mgr, _, _) = newManager(cache)
    val fetch = ScriptedFetch(
      some(HostSnapshot(HLCTimestamp.Zero, Nil), StoreSnapshot(HLCTimestamp.Zero, Nil)),
      some(HostSnapshot(at(T), List(remote(T))), StoreSnapshot(at(T), List(CachedStore(store0, remoteHostId, at(T))))))
    val tasks = new RecordingTasks
    newReader(mgr, fetch, tasks).start()

    tasks.requested.head.fire()

    fetch.calls should be(2)
    mgr.peekHostEntry(remoteHostId).map(_.address) should be(Some("1.2.3.4"))
    mgr.peekStoreHost(store0) should be(Some(remoteHostId))

  test("no service entry is a quiet no-op"):
    val cache = seeded(T)
    val (mgr, _, _) = newManager(cache)
    val held = mgr.peekHostEntry(remoteHostId).get
    val fetch = ScriptedFetch(none)
    val tasks = new RecordingTasks
    newReader(mgr, fetch, tasks).start()

    tasks.requested.head.fire()

    fetch.calls should be(1)
    cache.bulkUpdates should be(empty)
    mgr.peekHostEntry(remoteHostId).get should be theSameInstanceAs held
    tasks.requested.size should be(2)
    tasks.requested(1).delay should be(period)

  test("a failed fetch keeps the schedule"):
    val (mgr, _, _) = newManager(seeded(T))
    val fetch = ScriptedFetch(
      () => Future.failed(new RuntimeException("read failed")),
      () => throw new RuntimeException("fetch threw"),
      emptySnapshots)
    val tasks = new RecordingTasks
    newReader(mgr, fetch, tasks).start()

    tasks.requested.head.fire()
    tasks.requested.size should be(2)
    tasks.requested(1).fire()
    tasks.requested.size should be(3)
    tasks.requested(2).fire()

    fetch.calls should be(3)
    tasks.requested.size should be(4)
    tasks.requested(3).delay should be(period)

  test("stop cancels the schedule"):
    val cache = seeded(T)
    val (mgr, _, _) = newManager(cache)
    val inFlight = Promise[Option[(HostSnapshot, StoreSnapshot)]]()
    val fetch = ScriptedFetch(() => inFlight.future)
    val tasks = new RecordingTasks
    val reader = newReader(mgr, fetch, tasks)
    reader.start()

    tasks.requested.head.fire()
    reader.stop()
    tasks.requested.head.cancelled should be(true)

    // The read in flight completes after stop: nothing is applied and nothing more is scheduled
    inFlight.success(Some((HostSnapshot(at(T + 1), Nil), StoreSnapshot(at(T + 1), Nil))))

    cache.bulkUpdates should be(empty)
    tasks.requested.size should be(1)
