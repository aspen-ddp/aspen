package org.aspen_ddp.aspen.server.store

import java.util.UUID
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicLong

import org.aspen_ddp.aspen.common.{DataBuffer, HLCTimestamp}
import org.aspen_ddp.aspen.common.objects.{Key, Metadata, ObjectId, ObjectRefcount, ObjectRevision, ObjectType, Value}
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.transaction.TransactionId
import org.aspen_ddp.aspen.server.store.cache.{HostObjectCache, StoreObjectCache}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration.*
import scala.language.implicitConversions

object HostObjectCacheSuite {

  val Overhead: Long = HostObjectCache.EntryOverhead

  val store0: StoreId = StoreId(PoolId(new UUID(0, 9)), 0)
  val store1: StoreId = StoreId(PoolId(new UUID(0, 9)), 1)
  val store2: StoreId = StoreId(PoolId(new UUID(0, 9)), 2)

  /** Runs each posted task at once, counting them */
  class Inline extends ExecutionContext {
    var posted = 0
    def execute(r: Runnable): Unit =
      posted += 1
      r.run()
    def reportFailure(t: Throwable): Unit = throw t
  }

  /** Counts the tasks posted through it and those that have finished running */
  class Counting(ec: ExecutionContext) extends ExecutionContext {
    val submitted = new AtomicLong(0)
    val finished = new AtomicLong(0)
    def execute(r: Runnable): Unit =
      submitted.incrementAndGet()
      ec.execute(() => try r.run() finally finished.incrementAndGet())
    def reportFailure(t: Throwable): Unit = ec.reportFailure(t)
  }

  /** Records posted tasks so a test decides when they run */
  class Queue extends ExecutionContext {
    var tasks: Vector[Runnable] = Vector()
    def execute(r: Runnable): Unit = tasks :+= r
    def runAll(): Unit =
      while tasks.nonEmpty do
        val t = tasks.head
        tasks = tasks.tail
        t.run()
    def reportFailure(t: Throwable): Unit = throw t
  }

  def mkobj(n: Int, dataSize: Int = 0): ObjectState = new ObjectState(
    ObjectId(new UUID(0, n)),
    Metadata(ObjectRevision(TransactionId(new UUID(0, 2))), ObjectRefcount(1, 1), HLCTimestamp(1)),
    ObjectType.Data,
    DataBuffer(new Array[Byte](dataSize))
  )

  def mkobjs(): (ObjectState, ObjectState, ObjectState, ObjectState) =
    (mkobj(1), mkobj(2), mkobj(3), mkobj(4))
}

class HostObjectCacheSuite extends AnyFunSuite with Matchers {
  import HostObjectCacheSuite._

  private var now = 0L
  private val clock = () => { now += 1; now }

  private def mk(capacity: Long): (HostObjectCache, StoreObjectCache) =
    val hc = new HostObjectCache(capacity, clock)
    (hc, hc.forStore(store0))

  test("evicts least recently used first") {
    val (hc, c) = mk(3 * Overhead)
    val (o1, o2, o3, o4) = mkobjs()

    c.insert(o1); c.insert(o2); c.insert(o3)
    c.get(o1.objectId) should be(Some(o1))
    c.insert(o4)

    c.get(o2.objectId) shouldBe None
    c.get(o1.objectId) should be(Some(o1))
    c.get(o3.objectId) should be(Some(o3))
    c.get(o4.objectId) should be(Some(o4))
    hc.sizeInBytes should be(3 * Overhead)
    c.sizeInBytes should be(3 * Overhead)
    c.entryCount should be(3)
  }

  test("evicts as many as needed to fit a large one") {
    val (hc, c) = mk(4 * Overhead)
    val (o1, o2, o3, o4) = mkobjs()

    c.insert(o1); c.insert(o2); c.insert(o3); c.insert(o4)
    val big = mkobj(5, dataSize = 2 * Overhead.toInt)
    c.insert(big)

    c.get(o1.objectId) shouldBe None
    c.get(o2.objectId) shouldBe None
    c.get(o3.objectId) shouldBe None
    c.get(o4.objectId) should be(Some(o4))
    c.get(big.objectId) should be(Some(big))
    hc.sizeInBytes should be(4 * Overhead)
  }

  test("an object larger than the capacity is kept until the next insert") {
    val (hc, c) = mk(2 * Overhead)
    val big = mkobj(1, dataSize = 4 * Overhead.toInt)

    c.insert(big)
    c.get(big.objectId) should be(Some(big))
    hc.sizeInBytes should be(5 * Overhead)

    val o2 = mkobj(2)
    c.insert(o2)
    c.get(big.objectId) shouldBe None
    c.get(o2.objectId) should be(Some(o2))
    hc.sizeInBytes should be(Overhead)
  }

  test("re-inserting re-measures") {
    val (hc, c) = mk(100 * Overhead)
    val o1 = mkobj(1)

    c.insert(o1)
    hc.sizeInBytes should be(Overhead)

    o1.data = DataBuffer(new Array[Byte](1000))
    c.insert(o1)
    hc.sizeInBytes should be(Overhead + 1000)
    c.entryCount should be(1)

    c.remove(o1.objectId)
    hc.sizeInBytes should be(0)
    c.entryCount should be(0)
  }

  test("key-value objects count their decoded content") {
    val o = mkobj(1)
    val kv = new ObjectState(o.objectId, o.metadata, ObjectType.KeyValue, DataBuffer.Empty)
    val emptySize = HostObjectCache.estimateSize(kv)

    kv.kvState.get.content += Key(Array[Byte](1, 2, 3)) ->
      new ValueState(Value(new Array[Byte](10)), o.metadata.revision, HLCTimestamp(1), None)

    HostObjectCache.estimateSize(kv) should be(emptySize + 3 + 10 + HostObjectCache.KeyValueEntryOverhead)
  }

  test("pinned states are counted but never evicted") {
    val (hc, c) = mk(2 * Overhead)
    val (o1, o2, o3, _) = mkobjs()

    c.insert(o1)
    c.pin(o1)
    c.pinnedCount should be(1)
    c.entryCount should be(0)
    c.insert(o2)
    c.insert(o3)

    c.get(o1.objectId) should be(Some(o1))
    c.get(o3.objectId) should be(Some(o3))
    c.get(o2.objectId) shouldBe None
    hc.sizeInBytes should be(2 * Overhead)
  }

  test("unpinning returns a cached state at the most recently used end") {
    val (_, c) = mk(3 * Overhead)
    val (o1, o2, o3, o4) = mkobjs()

    c.insert(o1); c.insert(o2); c.insert(o3)
    c.pin(o1)
    c.unpin(o1)
    c.pinnedCount should be(0)
    c.insert(o4)

    c.get(o2.objectId) shouldBe None
    c.get(o1.objectId) should be(Some(o1))
  }

  test("unpinning an uncached state stops counting it") {
    val (hc, c) = mk(10 * Overhead)
    val x = mkobj(1)

    c.pin(x)
    hc.sizeInBytes should be(Overhead)
    c.sizeInBytes should be(Overhead)
    c.get(x.objectId) shouldBe None
    c.unpin(x)
    hc.sizeInBytes should be(0)
    c.get(x.objectId) shouldBe None
  }

  test("unpinning a discarded state stops counting it") {
    val (hc, c) = mk(10 * Overhead)
    val a = mkobj(1)

    c.insert(a)
    c.pin(a)
    a.reloadRequired = true
    c.unpin(a)

    c.get(a.objectId) shouldBe None
    hc.sizeInBytes should be(0)
  }

  test("pinning twice is a no-op") {
    val (hc, c) = mk(10 * Overhead)
    val a = mkobj(1)

    c.pin(a)
    c.pin(a)
    hc.sizeInBytes should be(Overhead)
    c.pinnedCount should be(1)
  }

  test("inserting a pinned state marks it cached") {
    val (hc, c) = mk(10 * Overhead)
    val a = mkobj(1)

    c.pin(a)
    a.data = DataBuffer(new Array[Byte](100))
    c.insert(a)
    hc.sizeInBytes should be(Overhead + 100)
    c.entryCount should be(0)
    c.get(a.objectId) should be(Some(a))
    c.unpin(a)
    c.entryCount should be(1)
    hc.sizeInBytes should be(Overhead + 100)
  }

  test("stores share one host total but not objects") {
    val hc = new HostObjectCache(10 * Overhead, clock)
    val c0 = hc.forStore(store0)
    val c1 = hc.forStore(store1)
    val s0 = mkobj(1)
    val s1 = mkobj(1)

    c0.insert(s0)
    c1.insert(s1)
    c0.get(s0.objectId).get should be theSameInstanceAs s0
    c1.get(s1.objectId).get should be theSameInstanceAs s1
    hc.sizeInBytes should be(2 * Overhead)
    c0.sizeInBytes should be(Overhead)
  }

  test("clearing a view uncounts it and ends it") {
    val hc = new HostObjectCache(10 * Overhead, clock)
    val c0 = hc.forStore(store0)
    val c1 = hc.forStore(store1)
    val (o1, o2, o3, o4) = mkobjs()

    c0.insert(o1)
    c0.insert(o2)
    c0.pin(o3)
    c1.insert(o4)
    hc.sizeInBytes should be(4 * Overhead)

    c0.clear()
    hc.sizeInBytes should be(Overhead)
    c0.get(o1.objectId) shouldBe None
    c0.insert(o1)
    c0.pin(o2)
    c0.get(o1.objectId) shouldBe None
    hc.sizeInBytes should be(Overhead)
    c1.get(o4.objectId) should be(Some(o4))
  }

  test("a second view of a store leaves the earlier one counted and working until it is cleared") {
    val hc = new HostObjectCache(10 * Overhead, clock)
    val old = hc.forStore(store0)
    val other = hc.forStore(store1)
    val (o1, o2, o3, _) = mkobjs()

    old.insert(o1)
    old.insert(o2)
    other.insert(o3)

    // As a reload over a loaded store makes it: the old store may still be running its queued tasks
    val fresh = hc.forStore(store0)
    hc.viewCount should be(3)
    hc.sizeInBytes should be(3 * Overhead)
    fresh.get(o1.objectId) shouldBe None
    old.get(o1.objectId) should be(Some(o1))
    old.remove(o2.objectId)
    hc.sizeInBytes should be(2 * Overhead)

    // Clearing the old view uncounts only its own entries and leaves the new view registered
    old.clear()
    hc.viewCount should be(2)
    hc.sizeInBytes should be(Overhead)
    fresh.insert(o1)
    hc.sizeInBytes should be(2 * Overhead)
    fresh.get(o1.objectId) should be(Some(o1))
  }

  // Cross-store tests use a cap of 32 entries: a shed then frees down to 31 (the cap less a 1/32 slack of one entry)
  private val Cap32 = 32 * Overhead

  private def objs(from: Int, until: Int): IndexedSeq[ObjectState] = (from until until).map(mkobj(_))

  test("a hot store's inserts evict a cold store's oldest entries") {
    val hc = new HostObjectCache(Cap32, clock)
    val coldQueue = new Queue
    val cold = hc.forStore(store0, coldQueue)
    val hot = hc.forStore(store1)
    val cs = objs(1, 5)
    val hs = objs(100, 129)

    cs.foreach(cold.insert)
    hs.foreach(hot.insert) // 33 entries: over by one

    coldQueue.tasks.size shouldBe 1
    coldQueue.runAll()
    // Freed down to the low-water mark of 31 entries: the two oldest
    cold.get(cs(0).objectId) shouldBe None
    cold.get(cs(1).objectId) shouldBe None
    cold.get(cs(2).objectId) should be(Some(cs(2)))
    hc.sizeInBytes shouldBe 31 * Overhead
    hs.foreach(h => hot.get(h.objectId) should be(Some(h)))
  }

  test("a store evicts its own entries when they are the oldest") {
    val hc = new HostObjectCache(Cap32, clock)
    val qa = new Queue
    val qb = new Queue
    val a = hc.forStore(store0, qa)
    val b = hc.forStore(store1, qb)
    val bs = objs(1, 5)

    b.insert(bs(0)); b.insert(bs(1))
    objs(100, 129).foreach(a.insert) // 31 entries
    b.insert(bs(2))
    b.insert(bs(3)) // 33: over by one

    qa.tasks shouldBe empty
    qb.tasks shouldBe empty
    // Local eviction is exact to the cap
    b.get(bs(0).objectId) shouldBe None
    b.get(bs(1).objectId) should be(Some(bs(1)))
    a.entryCount shouldBe 29
    hc.sizeInBytes shouldBe Cap32
  }

  test("only one shed is pending per victim") {
    val hc = new HostObjectCache(Cap32, clock)
    val coldQueue = new Queue
    val cold = hc.forStore(store0, coldQueue)
    val hot = hc.forStore(store1)

    objs(1, 5).foreach(cold.insert)
    objs(100, 130).foreach(hot.insert) // 34 entries: over at the 33rd and the 34th

    coldQueue.tasks.size shouldBe 1
    coldQueue.runAll()
    cold.entryCount shouldBe 1
    hot.entryCount shouldBe 30
    hc.sizeInBytes shouldBe 31 * Overhead
  }

  test("a shed that arrives after the pressure passed evicts nothing") {
    val hc = new HostObjectCache(Cap32, clock)
    val coldQueue = new Queue
    val cold = hc.forStore(store0, coldQueue)
    val hot = hc.forStore(store1)

    objs(1, 3).foreach(cold.insert)
    objs(100, 131).foreach(hot.insert) // 33 entries
    coldQueue.tasks.size shouldBe 1
    hot.clear()
    coldQueue.runAll()

    cold.entryCount shouldBe 2
    hc.sizeInBytes shouldBe 2 * Overhead
  }

  test("a shed queued for a cleared view does nothing") {
    val hc = new HostObjectCache(Cap32, clock)
    val coldQueue = new Queue
    val cold = hc.forStore(store0, coldQueue)
    val hot = hc.forStore(store1)

    objs(1, 3).foreach(cold.insert)
    objs(100, 133).foreach(hot.insert) // 35 entries
    coldQueue.tasks.size shouldBe 1
    cold.clear()
    hc.sizeInBytes shouldBe 33 * Overhead
    coldQueue.runAll()

    // Still over, but a cleared view neither evicts nor passes the pressure on
    hc.sizeInBytes shouldBe 33 * Overhead
    hot.entryCount shouldBe 33
  }

  test("a victim with too few entries passes the rest of the pressure on") {
    val hc = new HostObjectCache(Cap32, clock)
    val qa = new Queue
    val qb = new Queue
    val a = hc.forStore(store0, qa)
    val b = hc.forStore(store1, qb)
    val c = hc.forStore(store2)
    val a1 = mkobj(1)
    val bs = objs(10, 14)
    val cs = objs(100, 129)

    a.insert(a1)
    bs.foreach(b.insert)
    cs.foreach(c.insert) // 34 entries

    qa.tasks.size shouldBe 1
    qb.tasks shouldBe empty
    qa.runAll()
    a.entryCount shouldBe 0
    qb.tasks.size shouldBe 1
    qb.runAll()

    b.get(bs(0).objectId) shouldBe None
    b.get(bs(1).objectId) shouldBe None
    b.get(bs(2).objectId) should be(Some(bs(2)))
    cs.foreach(o => c.get(o.objectId) should be(Some(o)))
    hc.sizeInBytes shouldBe 31 * Overhead
  }

  test("sustained pressure posts one shed per slack's worth of inserts") {
    // Cap of 320 entries: a shed frees down to 310, so the next ten inserts need no shed
    val hc = new HostObjectCache(320 * Overhead, clock)
    val inline = new Inline
    val cold = hc.forStore(store0, inline)
    val hot = hc.forStore(store1)

    objs(1, 321).foreach(cold.insert) // exactly at the cap
    objs(1000, 1110).foreach(hot.insert)

    // Inserts 1, 12, 23, ..., 100 go over: 10 sheds of 11 entries each, rather than one per insert
    inline.posted shouldBe 10
    cold.entryCount shouldBe 210
    hot.entryCount shouldBe 110
    hc.sizeInBytes shouldBe 320 * Overhead
  }

  test("pinned states never make a store a victim") {
    val hc = new HostObjectCache(2 * Overhead, clock)
    val coldQueue = new Queue
    val cold = hc.forStore(store0, coldQueue)
    val hot = hc.forStore(store1)
    val c1 = mkobj(1)
    val h1 = mkobj(11); val h2 = mkobj(12)

    cold.insert(c1)
    cold.pin(c1)
    hot.insert(h1); hot.insert(h2)

    coldQueue.tasks shouldBe empty
    cold.get(c1.objectId) should be(Some(c1))
    hot.get(h1.objectId) shouldBe None
    hot.get(h2.objectId) should be(Some(h2))
    hc.sizeInBytes shouldBe 2 * Overhead
  }

  test("stores on their own threads keep the host total consistent") {
    val cap = 1000 * Overhead
    val ticks = new AtomicLong(0)
    // Strictly increasing across threads, so the prefilled view's entries are always older than later ones
    val hc = new HostObjectCache(cap, () => ticks.incrementAndGet())
    val poolA = Executors.newSingleThreadExecutor()
    val poolB = Executors.newSingleThreadExecutor()
    try
      val ecA = ExecutionContext.fromExecutor(poolA)
      val ecB = ExecutionContext.fromExecutor(poolB)
      // The views post sheds through counting wrappers; the test's own tasks bypass them
      val shedsA = new Counting(ecA)
      val shedsB = new Counting(ecB)
      val a = hc.forStore(store0, shedsA)
      val b = hc.forStore(store1, shedsB)

      def await(f: Future[Unit]): Unit = Await.result(f, 60.seconds)

      def insertAll(ec: ExecutionContext, view: StoreObjectCache, from: Int, until: Int): Future[Unit] =
        Future((from until until).foreach(n => ec.execute(() => view.insert(mkobj(n)))))(using ec)
          .flatMap(_ => Future(())(using ec))(using ec)

      // A fills the cache alone, so B's first over-budget insert finds A's head oldest and posts it a shed
      await(insertAll(ecA, a, 0, 1000))
      val doneA = insertAll(ecA, a, 1000, 10000)
      val doneB = insertAll(ecB, b, 100000, 110000)
      await(doneA)
      await(doneB)

      // Quiescent once two consecutive snapshots, each taken after a drain of both threads, are equal and show every
      // posted shed finished: no shed ran or was posted between them, and nothing else posts any more
      def drain(): Unit =
        await(Future(())(using ecA))
        await(Future(())(using ecB))
      def snapshot(): (Long, Long, Long, Long) =
        (shedsA.submitted.get, shedsA.finished.get, shedsB.submitted.get, shedsB.finished.get)
      drain()
      var prev = snapshot()
      var rounds = 0
      var quiescent = false
      while !quiescent do
        drain()
        val cur = snapshot()
        quiescent = cur == prev && cur._1 == cur._2 && cur._3 == cur._4
        prev = cur
        rounds += 1
        rounds should be < 1000

      (shedsA.finished.get + shedsB.finished.get) should be > 0L
      hc.sizeInBytes shouldBe (a.sizeInBytes + b.sizeInBytes)
      hc.sizeInBytes should be <= (cap + Overhead * 2)
    finally
      poolA.shutdown()
      poolB.shutdown()
  }
}
