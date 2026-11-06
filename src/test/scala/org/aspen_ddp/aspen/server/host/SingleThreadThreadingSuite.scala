package org.aspen_ddp.aspen.server.host

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.util.concurrent.{ConcurrentLinkedQueue, CountDownLatch, TimeUnit}
import scala.jdk.CollectionConverters.*

class SingleThreadThreadingSuite extends AnyFunSuite with Matchers:

  private val Posters = 8
  private val TasksPerPoster = 125 // 1,000 tasks in all

  private def withThreading(f: SingleThreadThreading => Unit): Unit =
    val t = new SingleThreadThreading()
    t.start()
    try f(t) finally t.shutdown()

  /** Posts TasksPerPoster tasks from each of Posters threads, cycling through dispatch, controlEvents and control.
   *  Each task records (poster, sequence number, thread name). Returns once every task has run. */
  private def postFromManyThreads(t: SingleThreadThreading): List[(Int, Int, String)] =
    val records = new ConcurrentLinkedQueue[(Int, Int, String)]()
    val done = new CountDownLatch(Posters * TasksPerPoster)
    val posters = (0 until Posters).map: poster =>
      new Thread(() =>
        for seq <- 0 until TasksPerPoster do
          val task: Runnable = () =>
            records.add((poster, seq, Thread.currentThread.getName))
            done.countDown()
          seq % 3 match
            case 0 => t.dispatch.execute(task)
            case 1 => t.controlEvents.execute(task)
            case _ => t.control.execute(task))
    posters.foreach(_.start())
    posters.foreach(_.join())
    done.await(10, TimeUnit.SECONDS) should be(true)
    records.asScala.toList

  test("all work runs on one thread named aspen-host-control"):
    withThreading: t =>
      val records = postFromManyThreads(t)
      records.size should be(Posters * TasksPerPoster)
      records.map(_._3).toSet should be(Set("aspen-host-control"))

  test("per-poster FIFO order"):
    withThreading: t =>
      val records = postFromManyThreads(t)
      for poster <- 0 until Posters do
        records.filter(_._1 == poster).map(_._2) should be((0 until TasksPerPoster).toList)

  test("a throwing task does not stop the queue"):
    withThreading: t =>
      val ran = new CountDownLatch(1)
      t.dispatch.execute(() => throw new RuntimeException("expected by the test"))
      t.dispatch.execute(() => ran.countDown())
      ran.await(10, TimeUnit.SECONDS) should be(true)
