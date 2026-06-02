package org.aspen_ddp.aspen.common.util

import java.util.concurrent.LinkedBlockingQueue

class EvictingQueue[A](val capacity: Int):
  private val queue = new LinkedBlockingQueue[A](capacity)

  def enqueue(element: A): Unit =
    // Thread-safe: Keep removing the head until the new item successfully fits
    while !queue.offer(element) do
      queue.poll() // Drops the oldest element

  def dequeue(): Option[A] = Option(queue.poll())
  def size: Int = queue.size()
  def isEmpty: Boolean = queue.size() == 0

