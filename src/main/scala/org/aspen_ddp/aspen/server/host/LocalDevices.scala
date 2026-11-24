package org.aspen_ddp.aspen.server.host

import org.aspen_ddp.aspen.common.metadata.{StorageDeviceId, StorageDeviceState}
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

  /** The device's (currentUsage, totalSize) in bytes, as its latest `Usage` job measured them (ReconcileDisk.usage,
   *  DeviceReconciler.pollUsage); None until its first measurement is applied. Set on the control thread only */
  var usage: Option[(Long, Long)] = None

  /** The device's current health (DeviceHealthJudge), Initial at load. Written on the control thread only;
   *  @volatile because the health publish reads it on the client context */
  @volatile var health: DeviceHealth = DeviceHealth.Initial
  /** The health last written to the device's record, None until known (the health publish). Control thread only */
  var publishedHealth: Option[DeviceHealth] = None
  /** A health publish is in flight. Control thread only */
  var healthPublishing: Boolean = false
  /** The judge has logged the probe's current error, until a later attempt succeeds. Control thread only */
  var probeErrorLogged: Boolean = false

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
