package org.aspen_ddp.aspen.bench

import java.nio.ByteBuffer
import java.util.UUID
import java.util.concurrent.{Executors, CountDownLatch}
import java.util.concurrent.atomic.AtomicLong

import scala.concurrent.{Await, ExecutionContext}
import scala.concurrent.duration.*
import scala.util.Random

import org.aspen_ddp.aspen.common.{DataBuffer, HLCTimestamp}
import org.aspen_ddp.aspen.common.metadata.StorageDeviceId
import org.aspen_ddp.aspen.common.objects.{DataObjectPointer, Metadata, ObjectId, ObjectRefcount, ObjectRevision,
  ObjectType}
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.transaction.TransactionId
import org.aspen_ddp.aspen.server.StorageDeviceConfig
import org.aspen_ddp.aspen.server.host.{DeviceIo, FsifJournalDisk, ThreadedDeviceIo}
import org.aspen_ddp.aspen.server.store.backend.{Commit, CommitState, Completion, CompletionHandler, Read, UpdateForm}
import org.aspen_ddp.aspen.server.store.backend.fsif.{FsifBackend, FsifLayout}

/** A runnable benchmark, not a suite (docs/host-threading-measurement.md, "FSIF device journal"). S FSIF stores on one
  * ThreadedDeviceIo sharing its FSIF device journal (production default sizing, created by FsifJournalDisk.ensure as a
  * device's first FSIF store create does), driven like the vdisk randrw job through a 16-wide pool: each op is a 50/50
  * mix of a "write" (a ranged write committed on every store) and a "read" (a read on `readStores` stores), `depth` ops
  * outstanding. `dir` must be on the file system being measured (not tmpfs).
  *
  * args: dir dataThreads serialThreads stores depth seconds [readStores] [writeBytes]
  *
  * Run: java -cp "$(sbt -batch 'export Test/fullClasspath' | tail -1)" org.aspen_ddp.aspen.bench.FsifDeviceBench \
  *   $PWD/target/fsifbench 1 1 16 8 10
  */
object FsifDeviceBench:
  def main(a: Array[String]): Unit =
    val dir = os.Path(a(0))
    val dataThreads = a(1).toInt
    val serialThreads = a(2).toInt
    val stores = a(3).toInt
    val depth = a(4).toInt
    val seconds = a(5).toInt
    val readStores = if a.length > 6 then a(6).toInt else 10
    val writeBytes = if a.length > 7 then a(7).toInt else 512
    val sliceSize = 64 * 1024
    val objects = 2000
    val layout = FsifLayout(sliceSize, 4096)
    val poolId = PoolId(new UUID(1, 1))
    os.remove.all(dir)
    os.makeDir.all(dir)
    val dev = new ThreadedDeviceIo(dir, DeviceIo.Config(dataThreads = dataThreads, serialThreads = serialThreads))
    val created = System.nanoTime()
    val journal = FsifJournalDisk.ensure(dev, dir,
      StorageDeviceConfig(StorageDeviceId(UUID.randomUUID()), UUID.randomUUID()))
    println(f"journal created in ${(System.nanoTime() - created) / 1e9}%.2fs")
    val driver = ExecutionContext.fromExecutorService(Executors.newSingleThreadExecutor())
    def sp(x: Int) = ByteBuffer.allocate(4).putInt(x).array()
    val ids = Array.tabulate(objects)(i => ObjectId(new UUID(i + 1, ~i)))
    def ptr(x: Int) = DataObjectPointer(ids(x), poolId, sp(x))
    val rev = new AtomicLong(0)
    def md() =
      val r = rev.incrementAndGet()
      Metadata(ObjectRevision(TransactionId(new UUID(r, 6))), ObjectRefcount(1, 1), HLCTimestamp(r))

    // Completion bookkeeping, all on `driver`
    val waiting = scala.collection.mutable.HashMap[(Int, ObjectId), scala.collection.mutable.Queue[() => Unit]]()
    final class Handler(s: Int) extends CompletionHandler:
      def complete(op: Completion): Unit =
        val id = op match
          case c: Commit => c.objectId
          case r: Read => r.objectId
        driver.execute: () =>
          waiting.get((s, id)).foreach(q => if q.nonEmpty then q.dequeue()())

    val backends = (0 until stores).map: s =>
      val sid = StoreId(poolId, s.toByte)
      val p = dir / s"store-$s"
      FsifBackend.initializeStore(p, layout)
      val b = FsifBackend.open(sid, p, layout, dev.dataEc, journal, t => println(s"FATAL $s $t"),
        serialEc = Some(dev.serialEc), readGate = Some(dev))
      b.setCompletionHandler(new Handler(s))
      b
    val data = Array.fill(objects)(new Array[Byte](sliceSize))

    def onAll(targets: Seq[Int], x: Int)(issue: Int => Unit)(done: () => Unit): Unit =
      var left = targets.size
      targets.foreach: s =>
        waiting.getOrElseUpdate((s, ids(x)), scala.collection.mutable.Queue()).enqueue: () =>
          left -= 1
          if left == 0 then done()
        issue(s)

    // Allocate every object on every store
    val allocated = new CountDownLatch(1)
    driver.execute: () =>
      var left = objects
      (0 until objects).foreach: x =>
        val st = CommitState(ptr(x), md(), ObjectType.Data, DataBuffer(data(x)), allocating = true)
        onAll(0 until stores, x)(s => backends(s).commit(st, TransactionId(new UUID(0, x))))(() =>
          left -= 1
          if left == 0 then allocated.countDown())
    allocated.await()

    val rng = new Random(42)
    val writes = new AtomicLong
    val reads = new AtomicLong
    val writeLat = new AtomicLong
    val readLat = new AtomicLong
    val busy = scala.collection.mutable.HashSet[Int]()
    val end = System.nanoTime() + seconds * 1000000000L
    val finished = new CountDownLatch(depth)
    def startOp(): Unit =
      if System.nanoTime() > end then finished.countDown()
      else
        var x = rng.nextInt(objects)
        while busy.contains(x) do x = rng.nextInt(objects)
        busy += x
        val t0 = System.nanoTime()
        if rng.nextBoolean() then
          val off = rng.nextInt(sliceSize - writeBytes)
          val old = data(x)
          val nd = old.clone()
          rng.nextBytes(java.util.Arrays.copyOfRange(nd, off, off + writeBytes))
          data(x) = nd
          val st = CommitState(ptr(x), md(), ObjectType.Data, DataBuffer(nd),
            form = UpdateForm.Ranges(old.length, List((off, writeBytes))))
          val tx = TransactionId(UUID.randomUUID())
          onAll(0 until stores, x)(s => backends(s).commit(st, tx)): () =>
            writes.incrementAndGet(); writeLat.addAndGet(System.nanoTime() - t0); busy -= x; startOp()
        else
          val targets = rng.shuffle((0 until stores).toList).take(readStores)
          onAll(targets, x)(s => backends(s).read(ptr(x))): () =>
            reads.incrementAndGet(); readLat.addAndGet(System.nanoTime() - t0); busy -= x; startOp()
    val start = System.nanoTime()
    driver.execute(() => (0 until depth).foreach(_ => startOp()))
    finished.await()
    val secs = (System.nanoTime() - start) / 1e9
    val w = writes.get(); val r = reads.get()
    println(f"data=$dataThreads%2d serial=$serialThreads stores=$stores depth=$depth  ops/s=${(w + r) / secs}%8.1f  " +
      f"writes/s=${w / secs}%7.1f wLat=${writeLat.get / 1e6 / math.max(1, w)}%6.2fms  " +
      f"reads/s=${r / secs}%7.1f rLat=${readLat.get / 1e6 / math.max(1, r)}%6.2fms")
    backends.foreach(b => Await.ready(b.close(), 30.seconds))
    journal.close()
    dev.close()
    driver.shutdownNow()
    System.exit(0)
