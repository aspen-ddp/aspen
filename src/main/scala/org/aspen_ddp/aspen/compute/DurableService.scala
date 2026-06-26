package org.aspen_ddp.aspen.compute

trait DurableService:
  /** Called by the executor when this host loses its lease.
   *  The service must stop operating immediately. */
  def shutdown(): Unit
