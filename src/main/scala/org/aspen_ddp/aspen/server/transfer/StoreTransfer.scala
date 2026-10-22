package org.aspen_ddp.aspen.server.transfer

import org.aspen_ddp.aspen.common.HLCTimestamp
import org.aspen_ddp.aspen.common.store.StoreId

import scribe.Logging

import java.util.UUID
import scala.concurrent.{Future, Promise}

object StoreTransfer:
  /** Present in a store directory while the store is being transferred to another device;
   *  hosts do not load a store carrying this marker. */
  val TransferOutMarkerFile: String = "transferring-out"

  /** Per-device directory that incoming stores are extracted into before being moved into place */
  val StagingDirectory: String = "transferring-in"

/** Moves whole store directories between hosts. Each transfer attempt is identified by a
 *  `(StoreId, transferUUID)` pair; the receiver accepts data only for its current registration. */
trait StoreTransferService:
  /** Registers to receive `storeId` for attempt `transferUUID`. The store is extracted into
   *  `destinationDirectory / storeId.directoryName`, replacing anything already there. The future
   *  completes once the full archive has arrived and been extracted, and fails if the attempt fails
   *  or is superseded by a later call for the same store. */
  def beginTransferIn(storeId: StoreId, transferUUID: UUID, destinationDirectory: os.Path): Future[Unit]

  def beginTransferOut(storeId: StoreId, transferUUID: UUID, sourceDirectory: os.Path,
                       destHost: String, destPort: Int, timestamp: HLCTimestamp): Unit

  def isTransferringOut(storeId: StoreId): Boolean

  def transferringOutStores: Set[StoreId]

object StoreTransferService:
  /** For hosts built without a transfer port (TestNetwork, harness defaults). beginTransferIn
   *  returns a Future that never completes; beginTransferOut logs a warning and does nothing. */
  object Disabled extends StoreTransferService with Logging:
    def beginTransferIn(storeId: StoreId, transferUUID: UUID, destinationDirectory: os.Path): Future[Unit] =
      Promise[Unit]().future

    def beginTransferOut(storeId: StoreId, transferUUID: UUID, sourceDirectory: os.Path,
                         destHost: String, destPort: Int, timestamp: HLCTimestamp): Unit =
      logger.warn(s"Store transfer is disabled on this host. Ignoring transfer out of store $storeId")

    def isTransferringOut(storeId: StoreId): Boolean = false

    def transferringOutStores: Set[StoreId] = Set.empty
