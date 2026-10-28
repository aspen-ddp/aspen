package org.aspen_ddp.aspen.common.metadata

import org.aspen_ddp.aspen.client.KeyValueObjectState
import org.aspen_ddp.aspen.codec
import org.aspen_ddp.aspen.common.metadata.HostId
import org.aspen_ddp.aspen.common.network.Codec
import org.aspen_ddp.aspen.common.objects.Key
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.util.YamlFormat.{Format, FormatError}

import java.util.UUID

final case class StorageDeviceId(uuid: UUID) extends AnyVal

object StorageDeviceId:
  val BootstrapStorageDeviceId: StorageDeviceId = fixed_ids.BootstrapStorageDeviceId

  object YStorageDeviceId extends Format[StorageDeviceId]:
    override def format(o: Object): StorageDeviceId = o match
      case v: java.lang.String =>
        try
          StorageDeviceId(UUID.fromString(v))
        catch
          case t: Throwable => throw new FormatError(s"Invalid UUID: $t")
      case _ => throw new FormatError(s"String Required")


object StorageDeviceState:

  private [aspen] val StateKey = Key(Array[Byte](0))

  def apply(buff: Array[Byte]): StorageDeviceState = Codec.decode(codec.StorageDeviceState.parseFrom(buff))
  
  def apply(kvos: KeyValueObjectState): StorageDeviceState = 
    StorageDeviceState(kvos.contents(StateKey).value.bytes)
  
  enum StoreStatus:
    case Initializing, Active, TransferringIn, TransferringOut, Rebuilding, Offline

  /** Operational is the normal state. ShuttingDown and Shutdown bracket a planned physical move
   *  of the device. Failed is the one-way tombstone written by failStorageDevice. */
  enum DeviceStatus:
    case Operational, ShuttingDown, Shutdown, Failed

  /** `transferId` identifies one logical transfer and is shared by the source's TransferringOut
   *  and the destination's TransferringIn entries. It is carried in the on-disk transfer markers
   *  so a copy left behind by an earlier, aborted transfer is never mistaken for this one's. */
  case class StoreEntry(status: StoreStatus,
                        transferDevice: Option[StorageDeviceId],
                        transferId: Option[UUID] = None)


case class StorageDeviceState(storageDeviceId: StorageDeviceId,
                              hostId: HostId,
                              currentUsage: Long,
                              totalSize: Long,
                              stores: Map[StoreId, StorageDeviceState.StoreEntry],
                              storageDeviceSet: StorageDeviceSetId,
                              status: StorageDeviceState.DeviceStatus = StorageDeviceState.DeviceStatus.Operational):
  
  def encode(): Array[Byte] = Codec.encode(this).toByteArray

  /** True once an operator has declared this device dead. A tombstoned device still exists and
   *  is still readable -- it holds the list of stores awaiting reconstruction -- but it is no
   *  longer a member of any set or host, and is never a legal destination for a store.
   *
   *  The status is authoritative. The tombstone also zeroes both ids, but a zeroed id on an
   *  otherwise Operational device does not make it failed. */
  def isFailed: Boolean = status == StorageDeviceState.DeviceStatus.Failed

  def isOperational: Boolean = status == StorageDeviceState.DeviceStatus.Operational

  /** True while the device is being, or has been, shelved for a planned physical move. */
  def isShutDownOrShuttingDown: Boolean =
    status == StorageDeviceState.DeviceStatus.ShuttingDown ||
    status == StorageDeviceState.DeviceStatus.Shutdown

  /** The status to write for a store that is coming into service on this device: Active, or
   *  Offline while the device is ShuttingDown or Shutdown.
   *
   *  Every transaction that would make an entry Active -- a store creation, a rebuild or transfer
   *  in finishing, a transfer out rolled back -- reads this state under a KeyRevision and writes
   *  what this returns. That is what keeps a shut-down device from ever holding an Active entry:
   *  a write ordered after the shutdown request sees ShuttingDown and writes Offline, and one
   *  ordered before it is flipped to Offline by the request itself. Only a restart or adoption
   *  turns those Offline entries Active again.
   */
  def serviceStatus: StorageDeviceState.StoreStatus =
    if isShutDownOrShuttingDown then StorageDeviceState.StoreStatus.Offline
    else StorageDeviceState.StoreStatus.Active

  /** Every Active store becomes Offline; all other entries are untouched. */
  def withActiveStoresOffline: StorageDeviceState =
    remapStores(StorageDeviceState.StoreStatus.Active, StorageDeviceState.StoreStatus.Offline)

  /** Every Offline store becomes Active; all other entries are untouched. */
  def withOfflineStoresActive: StorageDeviceState =
    remapStores(StorageDeviceState.StoreStatus.Offline, StorageDeviceState.StoreStatus.Active)

  private def remapStores(from: StorageDeviceState.StoreStatus,
                          to: StorageDeviceState.StoreStatus): StorageDeviceState =
    this.copy(stores = stores.map: (storeId, entry) =>
      if entry.status == from then storeId -> entry.copy(status = to) else storeId -> entry)

  def setHost(hostId: HostId): StorageDeviceState =
    this.copy(hostId=hostId)

  def setStoreEntry(storeId: StoreId,
                    status: StorageDeviceState.StoreStatus,
                    transferDevice: Option[StorageDeviceId]): StorageDeviceState =
    this.copy(stores=stores + (storeId -> StorageDeviceState.StoreEntry(status, transferDevice)))

  def removeStore(storeId: StoreId): StorageDeviceState =
    this.copy(stores=stores - storeId)

  /** The state this device should hold once a transfer of `storeId` onto `failedDestination` has
   *  been abandoned because that destination was declared dead: the entry goes back to Active and
   *  forgets the destination.
   *
   *  That is the whole of the repair. The source never gave up its copy and the pool never
   *  stopped naming it -- a transfer leaves the pool alone until it completes -- so putting the
   *  entry back is what makes the store look ordinary again to everything that keys off Active:
   *  MigratePoolToSetDurableTask's inFlight, SetRebalanceDurableTask's Phase.InFlight, and
   *  rebalancing's ownedStores. Nothing else about the transfer needs undoing at this level.
   *
   *  On a device that is shutting down or shut down the entry goes back to Offline instead; see
   *  serviceStatus. The source's copy is already closed behind its transfer-out marker, which is
   *  exactly how the shutdown leaves its other Offline stores.
   *
   *  None when this device is not, or is no longer, the source of that particular transfer. Both
   *  callers read this state inside a transaction that carries a KeyRevision on StateKey, so a
   *  None is the loser of a race re-reading after the winner already restored the entry --
   *  writing nothing is then the correct outcome rather than a clobber, and an empty transaction
   *  commits immediately.
   */
  def restoreAbandonedTransferSource(storeId: StoreId,
                                     failedDestination: StorageDeviceId): Option[StorageDeviceState] =
    stores.get(storeId)
      .filter: entry =>
        entry.status == StorageDeviceState.StoreStatus.TransferringOut &&
        entry.transferDevice.contains(failedDestination)
      .map: _ =>
        setStoreEntry(storeId, serviceStatus, None)