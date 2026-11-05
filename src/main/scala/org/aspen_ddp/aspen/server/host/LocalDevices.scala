package org.aspen_ddp.aspen.server.host

import org.aspen_ddp.aspen.common.metadata.StorageDeviceId
import org.aspen_ddp.aspen.common.store.StoreId
import scribe.Logging

class LocalStorageDeviceState(val storageDeviceId: StorageDeviceId,
                              val devicePath: os.Path,
                              val configFile: os.Path):
  var loadedStores: Set[StoreId] = Set()
  var offlineStores: Set[StoreId] = Set()

  def totalSize: Long = configFile.toIO.getTotalSpace
  def currentUsage: Long = totalSize - configFile.toIO.getFreeSpace

/** The storage devices loaded on this host.
 *
 *  tryLoadDevice is the only writer that adds entries in production. Entries are removed when a
 *  device is shut down and unloaded, so a lookup miss means a device that is not loaded right
 *  now: one never seen, one rejected by the load decision, or one unloaded by a shutdown. Tests
 *  inject entries directly via RecordingHost.injectLoadedDevice. */
class LocalDevices(lock: AnyRef) extends Logging:
  var devices: Map[StorageDeviceId, LocalStorageDeviceState] = Map()

  def devicePath(id: StorageDeviceId): Option[os.Path] = devices.get(id).map(_.devicePath)

  /** Reclaims the store copy a transfer just delivered onto a device that turned out to be
   *  tombstoned.
   *
   *  Only reached with TransferOutcome.SourceRestored, which means the pool still names the
   *  source and the source still holds the data -- so there is nothing here worth keeping. Its
   *  transferred-in marker keeps tryLoadStore from loading it, but nothing else would ever
   *  reclaim it, since reconcileDeviceState ignores a failed device outright. Were it loaded, and
   *  the source happened to live on this same host, the load would replace the source's entry in
   *  `stores`, which is keyed by StoreId alone.
   *
   *  Nothing is unloaded because nothing was loaded: the load is downstream of the decision.
   */
  def discardStoreDirectory(storeId: StoreId,
                                    deviceId: StorageDeviceId,
                                    reason: String): Unit = lock.synchronized {
    devices.get(deviceId) match
      case None =>
        // The operation completed on a device this process never loaded, or one a shutdown has
        // since unloaded. Log rather than assume.
        logger.warn(s"Cannot discard the copy of store $storeId on unloaded device $deviceId")

      case Some(sds) =>
        val storePath = sds.devicePath / storeId.directoryName
        if os.exists(storePath) then
          logger.warn(s"Discarding the copy of store $storeId on device $deviceId. " +
                      s"$reason: $storePath")
          try
            os.remove.all(storePath)
          catch
            case t: Throwable =>
              // The store is not loaded, so nothing serves it now. The cost of the leak is a
              // directory on a dead device that a restart would load and then never route to.
              logger.error(s"Failed to delete the discarded store copy $storePath. Error: $t")
  }
