package org.aspen_ddp.aspen.compute

import org.aspen_ddp.aspen.common.network.ServiceMessage

trait DurableService:
  /** Called by the executor when this host loses its lease.
   *  The service must stop operating immediately. */
  def shutdown(): Unit

  /** Called by the executor when a ServiceMessage arrives for this service.
   *  Default is a no-op; services that consume messages override this. */
  def receiveMessage(msg: ServiceMessage): Unit = ()
