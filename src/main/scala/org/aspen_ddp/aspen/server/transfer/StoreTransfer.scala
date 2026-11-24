package org.aspen_ddp.aspen.server.transfer

import org.aspen_ddp.aspen.common.HLCTimestamp
import org.aspen_ddp.aspen.common.store.StoreId

import scribe.Logging

import java.util.UUID
import scala.concurrent.{Future, Promise}

object StoreTransfer:
  /** Per-device directory that incoming stores are extracted into before being moved into place: each receive attempt
   *  stages under its own `transferring-in/<transferUUID>` (host.StoreTransfers.stagingOf) */
  val StagingDirectory: String = "transferring-in"

/** Where a transfer in's directory work for one device runs (StoreTransferService.beginTransferIn): the device's
 *  own management lane, so a hung device stalls only its own transfers and no caller thread ever waits on its disk.
 *  Thread-safe: the service calls `run` from the caller's thread (a cancel, a supersession) and from its connection
 *  threads (a failed receive). */
trait TransferLane:
  /** The root of the device the attempt stages on: the key of the service's per-device directory lock */
  def device: os.Path
  /** Runs `job` on the device's lane. The Future completes once it has run, and fails without running it if the job is
   *  cancelled before it starts */
  def run(job: => Unit): Future[Unit]

/** Moves whole store directories between hosts. Each transfer attempt is identified by a
 *  `(StoreId, transferUUID)` pair; the receiver accepts data only for its current registration. */
trait StoreTransferService:
  /** Registers to receive `storeId` for attempt `transferUUID`. The store is extracted into
   *  `destinationDirectory / storeId.directoryName`, replacing anything already there. The future
   *  completes once the full archive has arrived and been extracted, and fails if the attempt fails
   *  or is superseded by a later call for the same store. Every removal of what the attempt extracted runs on `lane`
   *  (the device's), never on the caller's thread. */
  def beginTransferIn(storeId: StoreId, transferUUID: UUID, destinationDirectory: os.Path,
                      lane: TransferLane): Future[Unit]

  /** Drops the registration for attempt `transferUUID` of `storeId`, if it is still the current
   *  one: any live connection and extractor are aborted and the attempt's future fails at once, and the removal of the
   *  partially extracted directory is one job on the attempt's lane, whose completion the returned Future reports
   *  (Future.unit for any other attempt, which is left alone). Takes no lock and touches no disk on the caller's
   *  thread. */
  def cancelTransferIn(storeId: StoreId, transferUUID: UUID): Future[Unit]

  def beginTransferOut(storeId: StoreId, transferUUID: UUID, sourceDirectory: os.Path,
                       destHost: String, destPort: Int, timestamp: HLCTimestamp): Unit

  /** Aborts the active send of `storeId`, if any. It is no longer transferring out afterwards. */
  def cancelTransferOut(storeId: StoreId): Unit

  def isTransferringOut(storeId: StoreId): Boolean

  def transferringOutStores: Set[StoreId]

object StoreTransferService:
  /** For hosts built without a transfer port (TestNetwork, harness defaults). beginTransferIn
   *  returns a Future that never completes; beginTransferOut logs a warning and does nothing. */
  object Disabled extends StoreTransferService with Logging:
    def beginTransferIn(storeId: StoreId, transferUUID: UUID, destinationDirectory: os.Path,
                        lane: TransferLane): Future[Unit] =
      Promise[Unit]().future

    def cancelTransferIn(storeId: StoreId, transferUUID: UUID): Future[Unit] = Future.unit

    def beginTransferOut(storeId: StoreId, transferUUID: UUID, sourceDirectory: os.Path,
                         destHost: String, destPort: Int, timestamp: HLCTimestamp): Unit =
      logger.warn(s"Store transfer is disabled on this host. Ignoring transfer out of store $storeId")

    def cancelTransferOut(storeId: StoreId): Unit = ()

    def isTransferringOut(storeId: StoreId): Boolean = false

    def transferringOutStores: Set[StoreId] = Set.empty
