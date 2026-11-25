package org.aspen_ddp.aspen.server.store.backend.fsif

import java.util.concurrent.ConcurrentLinkedQueue

import scala.concurrent.ExecutionContext

/** Queues runnables; they run only when the test calls runOne/runAll */
final class ManualEc(report: Throwable => Unit = t => throw t) extends ExecutionContext:
  private val queue = new ConcurrentLinkedQueue[Runnable]()
  @volatile var wrap: Runnable => Runnable = r => r
  def execute(r: Runnable): Unit = queue.add(r)
  def reportFailure(t: Throwable): Unit = report(t)
  def pending: Int = queue.size
  def runOne(): Unit = Option(queue.poll()).foreach(r => wrap(r).run())
  /** Includes tasks queued by the tasks it runs */
  def runAll(): Unit =
    while !queue.isEmpty do runOne()
