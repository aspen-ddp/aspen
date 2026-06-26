package org.aspen_ddp.aspen.compute

import java.util.UUID
import org.aspen_ddp.aspen.common.objects.Key

import scala.concurrent.Future

trait DurableServiceExecutor:
  /** Idempotent. Creates state KVO and an immediately-claimable TKVL entry.
   *  No-op if the service UUID is already registered. */
  def registerService(
    typeUUID: UUID,
    serviceUUID: UUID,
    initialState: Map[Key, Array[Byte]]
  ): Future[Unit]

  /** Removes the TKVL entry. Calls shutdown() on the service if this host owns it. */
  def unregisterService(serviceUUID: UUID): Future[Unit]
