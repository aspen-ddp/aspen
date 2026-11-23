package org.aspen_ddp.aspen.server.host

import org.aspen_ddp.aspen.common.metadata.{StorageDeviceId, StorageDeviceState}
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.server.FileStat
import org.aspen_ddp.aspen.server.crl.{CrashRecoveryLog, CrlAdmission}
import scribe.Logging

import scala.concurrent.Future

/** A device loaded on this host. `crl` is the device's own CRL, opened before any of its stores loads: every store on
 *  the device saves to, recovers from and deletes from it. */
class LocalStorageDeviceState(val storageDeviceId: StorageDeviceId,
                              val devicePath: os.Path,
                              val configFile: os.Path,
                              val crl: CrashRecoveryLog,
                              /** The device's I/O lanes (`DeviceIos.forPath`) */
                              val io: DeviceIo,
                              val admission: CrlAdmission = CrlAdmission.Unlimited):
  /** The device's copy registry (HostCopies), attached at load. Updated on the control thread only */
  val copies: DeviceCopies = new DeviceCopies(storageDeviceId, devicePath, s => onCopyChange(s))
  /** Called after every change to `copies` (HostCopies.observer, for tests) */
  var onCopyChange: StoreId => Unit = _ => ()
  /** Called after every write of `reconciledEntries`, with the stores it held before and holds now (HostCopies) */
  var onReconciled: (Set[StoreId], Set[StoreId]) => Unit = (_, _) => ()
  /** Stores the reconcile's Missing Stores pass declared lost on this device, while their entry stays Active here.
   *  Its rebuild is requested again at each reconcile even once the directory is back, since nothing else would serve
   *  or rebuild such a store (rebuild-process). Held in memory only: a restart's load serves a store that is back. */
  var declaredMissing: Set[StoreId] = Set()
  /** The store entries the device's latest reconcile read from its StorageDeviceState, None before its first. A load
   *  retry serves only a store whose entry here is Active or Offline (StoreLoader.retryFailedLoads) */
  def reconciledEntries: Option[Map[StoreId, StorageDeviceState.StoreEntry]] = reconciled

  def reconciledEntries_=(entries: Option[Map[StoreId, StorageDeviceState.StoreEntry]]): Unit =
    val before = reconciled.map(_.keySet).getOrElse(Set())
    reconciled = entries
    onReconciled(before, entries.map(_.keySet).getOrElse(Set()))

  private var reconciled: Option[Map[StoreId, StorageDeviceState.StoreEntry]] = None

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
   *  built from its peers on a device the tree does not say owns it (RebuildOutcome.Discard). Its place in arrived/
   *  or rebuilt/ keeps tryLoadStore from loading it, but nothing else would ever reclaim it, since
   *  reconcileDeviceState ignores a failed device outright. Were it loaded, and the source happened to live on this
   *  same host, the load would replace the source's entry in `stores`, which is keyed by StoreId alone.
   *
   *  Nothing is unloaded because nothing was loaded: the load is downstream of the decision. Returns the device whose
   *  copy was deleted, for the copy registry.
   */
  def discardStoreDirectory(storeId: StoreId,
                            deviceId: StorageDeviceId,
                            reason: String): Option[LocalStorageDeviceState] =
    devices.get(deviceId) match
      case None =>
        // The operation completed on a device this process never loaded, or one a shutdown has
        // since unloaded. Log rather than assume.
        logger.warn(s"Cannot discard the copy of store $storeId on unloaded device $deviceId")
        None

      case Some(sds) =>
        // No record: no copy of the store on the device. A stat that fails is no absence (FileStat): the delete is
        // attempted, and fails rather than report it gone
        sds.copies.pathOf(storeId) match
          case Some(storePath) if !FileStat.existence(storePath).contains(false) =>
            logger.warn(s"Discarding the copy of store $storeId on device $deviceId. " +
                        s"$reason: $storePath")
            // Rename-first: a failure leaves the copy whole where it is, so a restart never loads it.
            // The store is not loaded, so nothing serves it now
            if StoreCopies.delete(sds.devicePath, storePath, reason) then Some(sds) else None
          case _ =>
            None
