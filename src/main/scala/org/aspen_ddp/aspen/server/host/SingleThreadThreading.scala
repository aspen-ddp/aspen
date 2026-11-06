package org.aspen_ddp.aspen.server.host

import org.aspen_ddp.aspen.common.metadata.StorageDeviceId
import org.aspen_ddp.aspen.common.store.StoreId

import java.util.concurrent.Executor
import scala.concurrent.ExecutionContext

/** The host's former production threading, as its event loop ran: one thread, `aspen-host-control`, runs everything
 *  posted through `control`, `controlEvents` and `dispatch`, one task at a time in posting order. Stores run inline,
 *  as their work already runs on that thread.
 *
 *  It has no production user (`Main` passes StoreThreadPool). It is kept as a debugging and comparison option, to
 *  run a host with all its work serialized on one thread, and for tests.
 *
 *  Work posted before `start` waits for it. The thread is not a daemon: `shutdown` stops it once the work already
 *  queued has run, and anything posted after that is dropped. */
class SingleThreadThreading extends HostThreading:
  private val loop = new SerialLoop("aspen-host-control", daemon = false)

  override def control: ExecutionContext = loop
  override def controlEvents: Executor = loop
  override def dispatch: Executor = loop
  override def assign(storeId: StoreId, deviceId: StorageDeviceId): StoreExecutor = HostThreading.InlineStoreExecutor
  override def release(storeId: StoreId): Unit = ()

  override def start(): Unit = loop.start()

  override def shutdown(): Unit = loop.shutdown()
