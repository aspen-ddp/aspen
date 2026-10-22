package org.aspen_ddp.aspen.server.transfer

import org.aspen_ddp.aspen.client.AspenClient
import org.aspen_ddp.aspen.common.{DataBuffer, HLCTimestamp}
import org.aspen_ddp.aspen.common.metadata.{HostId, StorageDeviceId}
import org.aspen_ddp.aspen.common.store.StoreId

import scribe.Logging

import java.nio.file.Path
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

/** Receiving side of a store transfer. `complete` resolves when all data has arrived and
 *  the store is in place. */
trait StoreTransferIn:
  val storeId: StoreId
  val transferUUID: UUID
  def complete: Future[Unit]
  def dataReceived(db: DataBuffer): Unit

/** Sending side of a store transfer. `complete` resolves when all data has been sent. */
trait StoreTransferOut:
  val storeId: StoreId
  val transferUUID: UUID
  val timestamp: HLCTimestamp
  def complete: Future[Unit]
  def abort(): Unit

/** Constructs the in/out halves of a store transfer. The default `Filesystem` factory
 *  produces the real (filesystem + `jar` subprocess) implementations. Tests inject an
 *  in-memory factory that skips disk/process work. */
trait StoreTransferFactory:
  def createTransferIn(client: AspenClient,
                       storeId: StoreId,
                       storageDeviceId: StorageDeviceId,
                       devicePath: Path): StoreTransferIn

  def createTransferOut(client: AspenClient,
                        fromDevice: StorageDeviceId,
                        devicePath: Path,
                        storeId: StoreId,
                        toHost: HostId,
                        toDevice: StorageDeviceId,
                        timestamp: HLCTimestamp,
                        transferUUID: UUID): StoreTransferOut

object StoreTransferFactory:
  object Filesystem extends StoreTransferFactory:
    def createTransferIn(client: AspenClient,
                         storeId: StoreId,
                         storageDeviceId: StorageDeviceId,
                         devicePath: Path): StoreTransferIn =
      new TransferringIn(client, storeId, storageDeviceId, devicePath)

    def createTransferOut(client: AspenClient,
                          fromDevice: StorageDeviceId,
                          devicePath: Path,
                          storeId: StoreId,
                          toHost: HostId,
                          toDevice: StorageDeviceId,
                          timestamp: HLCTimestamp,
                          transferUUID: UUID): StoreTransferOut =
      new TransferringOut(client, fromDevice, devicePath, storeId, toHost, toDevice, timestamp, transferUUID)
