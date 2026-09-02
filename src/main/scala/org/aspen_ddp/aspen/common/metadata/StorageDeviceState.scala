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
    case Initializing, Active, TransferringIn, TransferringOut, Rebuilding

  case class StoreEntry(status: StoreStatus,
                        transferDevice: Option[StorageDeviceId])


case class StorageDeviceState(storageDeviceId: StorageDeviceId,
                              hostId: HostId,
                              currentUsage: Long,
                              totalSize: Long,
                              stores: Map[StoreId, StorageDeviceState.StoreEntry],
                              storageDeviceSet: StorageDeviceSetId):
  
  def encode(): Array[Byte] = Codec.encode(this).toByteArray

  /** True once an operator has declared this device dead. A tombstoned device still exists and
   *  is still readable -- it holds the list of stores awaiting reconstruction -- but it is no
   *  longer a member of any set or host, and is never a legal destination for a store.
   *
   *  `||` rather than `&&`: a half-written tombstone must read as failed, not as healthy. */
  def isFailed: Boolean =
    hostId == fixed_ids.FailedHostId || storageDeviceId == fixed_ids.FailedStorageDeviceId

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
        setStoreEntry(storeId, StorageDeviceState.StoreStatus.Active, None)