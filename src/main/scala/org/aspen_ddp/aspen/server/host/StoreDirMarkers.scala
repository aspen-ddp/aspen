package org.aspen_ddp.aspen.server.host

import org.aspen_ddp.aspen.server.FileStat
import org.aspen_ddp.aspen.server.rebuild.RebuildingStore
import org.aspen_ddp.aspen.server.transfer.StoreTransfer

/** The marker files a store directory can carry, and the one place that says which wins when several are present.
 *  Every test is FileStat's: a stat that fails is never taken for an absence. */
object StoreDirMarkers:

  /** A marker that keeps a store from being loaded. Declared in priority order: `blocker` returns the first present.
   *   - RebuildRequired: its CRL state is gone, so it awaits a rebuild
   *   - TransferredIn: it arrived from another host and awaits its pool update
   *   - TransferOut: it is marked for transfer out; its state travels in crl_save.log
   *   - RebuildComplete: a finished rebuild whose entry flip may not have committed */
  enum Marker(val fileName: String):
    case RebuildRequired extends Marker(RebuildingStore.RebuildRequiredMarkerFile)
    case TransferredIn extends Marker(StoreTransfer.TransferredInMarkerFile)
    case TransferOut extends Marker(StoreTransfer.TransferOutMarkerFile)
    case RebuildComplete extends Marker(RebuildingStore.RebuildCompleteMarkerFile)

  /** Whether `storePath` carries `marker`. Throws the stat's error when that cannot be told */
  def has(storePath: os.Path, marker: Marker): Boolean =
    FileStat.exists(storePath / marker.fileName)

  /** Whether `storePath` is known to carry `marker`: a stat that fails answers false, for a caller that waits for a
   *  later pass, whose load stats it again */
  def knownPresent(storePath: os.Path, marker: Marker): Boolean =
    FileStat.existence(storePath / marker.fileName).contains(true)

  /** The highest-priority marker in `storePath`, if any. Throws the stat's error when that cannot be told */
  def blocker(storePath: os.Path): Option[Marker] =
    Marker.values.find(has(storePath, _))
