package org.aspen_ddp.aspen.server.host

import org.aspen_ddp.aspen.common.metadata.StorageDeviceId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.pool.PoolId
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.util.UUID
import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}
import java.util.concurrent.{ConcurrentLinkedQueue, CountDownLatch, TimeUnit}
import scala.jdk.CollectionConverters.*

class StoreThreadPoolSuite extends AnyFunSuite with Matchers:

  private val Device = StorageDeviceId(new UUID(0, 1))
  private val Pool = PoolId(new UUID(0, 2))

  private def store(n: Int): StoreId = StoreId(Pool, n.toByte)

  private def withPool(maxThreads: Int)(f: StoreThreadPool => Unit): Unit =
    val t = new StoreThreadPool(maxThreads)
    t.start()
    try f(t) finally t.shutdown()

  test("threads are created lazily"):
    withPool(4): t =>
      t.assign(store(1), Device)
      t.assign(store(2), Device)
      t.threadCount should be(2)

  test("never more threads than maxThreads"):
    withPool(4): t =>
      for n <- 0 until 10 do t.assign(store(n), Device)
      t.threadCount should be(4)
      t.assignedCounts.sorted should be(Vector(2, 2, 3, 3))

  test("least-loaded placement"):
    withPool(2): t =>
      val s1 = t.assign(store(1), Device)
      t.assign(store(2), Device)
      t.assign(store(3), Device)
      t.release(store(1))
      val s4 = t.assign(store(4), Device)
      (s4 eq s1) should be(true)
      // s3 and s4 on one loop, s2 on the other
      t.assignedCounts should be(Vector(2, 1))

  test("release frees a slot for the next store"):
    withPool(1): t =>
      for _ <- 0 until 100 do
        t.assign(store(1), Device)
        t.release(store(1))
      t.threadCount should be(1)
      t.assignedCounts should be(Vector(0))

  test("store threads are daemon threads named aspen-store-N"):
    withPool(4): t =>
      val seen = new AtomicReference[(String, Boolean)]()
      val ran = new CountDownLatch(1)
      t.assign(store(1), Device).execute: () =>
        seen.set((Thread.currentThread.getName, Thread.currentThread.isDaemon))
        ran.countDown()
      ran.await(10, TimeUnit.SECONDS) should be(true)
      seen.get should be(("aspen-store-0", true))

  test("tasks of one store never overlap and run in posting order"):
    val Stores = 16
    val Posters = 8
    val TasksPerPoster = 2000
    withPool(4): t =>
      val executors = (0 until Stores).map(n => t.assign(store(n), Device))
      val inUse = Vector.fill(Stores)(new AtomicBoolean(false))
      val overlapSeen = new AtomicBoolean(false)
      // Per store: (poster, sequence number) in the order the store ran them
      val records = Vector.fill(Stores)(new ConcurrentLinkedQueue[(Int, Int)]())
      val done = new CountDownLatch(Posters * TasksPerPoster)
      val posters = (0 until Posters).map: poster =>
        new Thread(() =>
          for seq <- 0 until TasksPerPoster do
            val s = (poster + seq) % Stores
            executors(s).execute: () =>
              if !inUse(s).compareAndSet(false, true) then
                overlapSeen.set(true)
              records(s).add((poster, seq))
              inUse(s).set(false)
              done.countDown())
      posters.foreach(_.start())
      posters.foreach(_.join())
      done.await(30, TimeUnit.SECONDS) should be(true)
      overlapSeen.get should be(false)
      for s <- 0 until Stores; poster <- 0 until Posters do
        val seqs = records(s).asScala.toList.filter(_._1 == poster).map(_._2)
        seqs should be(seqs.sorted)
        seqs.distinct.size should be(seqs.size)
      records.map(_.size).sum should be(Posters * TasksPerPoster)

  test("a throwing task does not kill its store thread"):
    withPool(1): t =>
      val ex = t.assign(store(1), Device)
      val ran = new CountDownLatch(1)
      ex.execute(() => throw new RuntimeException("expected by the test"))
      ex.execute(() => ran.countDown())
      ran.await(10, TimeUnit.SECONDS) should be(true)
