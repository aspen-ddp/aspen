package org.aspen_ddp.aspen.server.host

import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.server.{FileStat, StoreConfig}

import scala.util.Try

/** The disk reads of a device check and of the periodic usage poll, as plain functions of paths. Each runs as one job
 *  on the device's management lane (DeviceReconciler: `ReconcileInputs` and `Usage`, at most one of each outstanding
 *  per device); the control thread decides from the values they return. They touch no control state. */
object ReconcileDisk:

  /** The stats of one copy path: its directory and its `config.yaml`, each as FileStat.existence (None where the stat
   *  fails) */
  final case class PathStats(dir: Option[Boolean], config: Option[Boolean])

  /** Every disk-derived input of one device's reconcile, read in one job.
   *  `recorded`: the path of each store's copy record when the job was submitted, the paths stat'd; a store whose
   *  record names another path at the apply (or that gained or lost its record) is skipped that round
   *  (DeviceReconciler.reconcileStores), never acted on from these values. `listing`: the device's copies with their
   *  phase parameters (CopyLayout.list), for HostCopies.reread and its rebuild-finish check. `stats`: the stats of
   *  every recorded path and of every copy the listing found outside pending-delete/. `staging`: the directories under
   *  `rebuilding/` (RebuildBookkeepingDisk.listStaging), for StoreRebuilds.sweepUnlistedStaging */
  final case class ReconcileDiskInputs(recorded: Map[StoreId, os.Path], listing: Try[CopyLayout.Listing],
                                       stats: Map[os.Path, PathStats], staging: Try[List[os.Path]] = Try(Nil)):
    /** Whether `storeId`'s record names, at the apply, the path it named when these were gathered (`now`, None for no
     *  record): the stats are its copy's */
    def current(storeId: StoreId, now: Option[os.Path]): Boolean = recorded.get(storeId) == now

  /** A device check's disk inputs for the device at `devicePath`, whose copy records name `recorded` */
  def gather(devicePath: os.Path, recorded: Map[StoreId, os.Path]): ReconcileDiskInputs =
    val listing = Try(CopyLayout.list(devicePath))
    val listed = listing.toOption.toList.flatMap(_.copies).filter(_.phase != Phase.Retired).map(_.path)
    val stats = (recorded.values ++ listed).toSet.iterator.map: p =>
      p -> PathStats(FileStat.existence(p), FileStat.existence(p / StoreConfig.configFilename))
    val staging = Try(RebuildBookkeepingDisk.listStaging(devicePath))
    ReconcileDiskInputs(recorded, listing, stats.toMap, staging)

  /** The device's (currentUsage, totalSize) in bytes, as the file system holding `configFile` reports them */
  def usage(configFile: os.Path): (Long, Long) =
    val f = configFile.toIO
    val total = f.getTotalSpace
    (total - f.getFreeSpace, total)
