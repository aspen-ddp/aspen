package org.aspen_ddp.aspen.server.store

import org.aspen_ddp.aspen.common.HLCTimestamp
import org.aspen_ddp.aspen.common.objects.ObjectPointer
import org.aspen_ddp.aspen.common.store.StoreId

import scala.util.Try

/** Verifies a quarantined object (Frontend.quarantine): repairs the store's copy from a read of the object through its
  * pool, and answers Success(true) once the repair is complete for a read at least `minTimestamp` (the store's copy
  * then matches a pool state no older than the transaction that quarantined it, exactly what repair requires before
  * dropping an ErrorTree entry for that transaction), Success(false) when it is stale or deferred, or a Failure.
  * `answer` may be called from any thread: the Frontend posts it to its own executor (Frontend.post).
  *
  * Stores have no client, so the Host supplies this (Host.quarantineVerifier, through RepairService.verifyObject)
  */
trait QuarantineVerifier:
  def verify(storeId: StoreId, pointer: ObjectPointer, minTimestamp: HLCTimestamp, answer: Try[Boolean] => Unit): Unit

object QuarantineVerifier:
  /** For tests and stores without a host: never answers */
  val Never: QuarantineVerifier = (_, _, _, _) => ()
