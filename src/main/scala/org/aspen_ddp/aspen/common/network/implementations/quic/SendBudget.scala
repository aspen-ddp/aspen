package org.aspen_ddp.aspen.common.network.implementations.quic

import java.util.concurrent.atomic.AtomicLong

/** The host-wide send budget that all of a `QuicNet`'s connections draw from (see `QuicConfig`). Connections add what
  * they queue and subtract it once written or dropped. Thread-safe.
  */
final class SendBudget(val capacity: Long):
  private val used = new AtomicLong(0)

  /** Adds `n` bytes, or returns them when negative */
  def add(n: Long): Unit = used.addAndGet(n): Unit

  def usedBytes: Long = used.get()

  def hasRoom: Boolean = used.get() < capacity
