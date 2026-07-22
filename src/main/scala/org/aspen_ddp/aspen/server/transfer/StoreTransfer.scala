package org.aspen_ddp.aspen.server.transfer

import org.aspen_ddp.aspen.client.AspenClient
import org.aspen_ddp.aspen.common.{DataBuffer, HLCTimestamp}
import org.aspen_ddp.aspen.common.metadata.{HostId, StorageDeviceId}
import org.aspen_ddp.aspen.common.store.StoreId

import java.nio.file.Path
import java.util.UUID
import scala.concurrent.Future

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
