package org.aspen_ddp.aspen.server.host

import org.aspen_ddp.aspen.common.metadata.StorageDeviceId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.server.crl.{CrashRecoveryLog, CrlAdmission}
import scribe.Logging

import scala.concurrent.Future

/** A device loaded on this host. `crl` is the device's own CRL, opened before any of its stores loads: every store on
 *  the device saves to, recovers from and deletes from it. */
class LocalStorageDeviceState(val storageDeviceId: StorageDeviceId,
                              val devicePath: os.Path,
                              val configFile: os.Path,
                              val crl: CrashRecoveryLog,
                              val admission: CrlAdmission = CrlAdmission.Unlimited):
  var loadedStores: Set[StoreId] = Set()
  var offlineStores: Set[StoreId] = Set()
  /** Stores the reconcile's Missing Stores pass declared lost on this device, while their entry stays Active here.
   *  Its rebuild is requested again at each reconcile even once the directory is back, since nothing else would serve
   *  or rebuild such a store (rebuild-process T14). Held in memory only: a restart's load serves a store that is back. */
  var declaredMissing: Set[StoreId] = Set()

  def totalSize: Long = configFile.toIO.getTotalSpace
  def currentUsage: Long = totalSize - configFile.toIO.getFreeSpace

/** Runs only on the control thread. See HostThreading.scala.
 *
 *  The storage devices loaded on this host. Whoever changes `devices` calls DeviceControl.publishView afterwards.
 *
 *  tryLoadDevice is the only writer that adds entries in production. Entries are removed when a
 *  device is shut down and unloaded, so a lookup miss means a device that is not loaded right
 *  now: one never seen, one rejected by the load decision, or one unloaded by a shutdown. Tests
 *  inject entries directly via ControlFixture.injectLoadedDevice. */
class LocalDevices extends Logging:
  var devices: Map[StorageDeviceId, LocalStorageDeviceState] = Map()

  /** Paths of unloaded devices whose CRL is still shutting down, each with that shutdown. A device is not loaded
   *  again from such a path until its entry is removed (DeviceShutdown.unloadDevice): two CRLs open over the same
   *  files would corrupt them. */
  var closingCrls: Map[os.Path, Future[Unit]] = Map()

  /** Whether `sds` is still the loaded state of its device (not unloaded, nor replaced by a later load) */
  def isLoaded(sds: LocalStorageDeviceState): Boolean =
    devices.get(sds.storageDeviceId).exists(_ eq sds)

  /** Deletes a copy that never was its store's owner copy and holds nothing its source lacks, so it needs no retiring
   *  (StoreCopies): the copy a transfer delivered onto a device that turned out to be tombstoned
   *  (TransferOutcome.SourceRestored: the pool still names the source, which still holds the data), or one a rebuild
   *  built from its peers on a device the tree does not say owns it (RebuildOutcome.Discard). Its transferred-in or
   *  rebuild-complete marker keeps tryLoadStore from loading it, but nothing else would ever reclaim it, since
   *  reconcileDeviceState ignores a failed device outright. Were it loaded, and the source happened to live on this
   *  same host, the load would replace the source's entry in `stores`, which is keyed by StoreId alone.
   *
   *  Nothing is unloaded because nothing was loaded: the load is downstream of the decision.
   */
  def discardStoreDirectory(storeId: StoreId,
                            deviceId: StorageDeviceId,
                            reason: String): Unit =
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
          // Rename-first: a failure leaves the copy whole, its marker included, so a restart never loads it.
          // The store is not loaded, so nothing serves it now
          StoreCopies.delete(sds.devicePath, storePath, reason)
