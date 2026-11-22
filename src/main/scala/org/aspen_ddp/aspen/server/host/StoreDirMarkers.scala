package org.aspen_ddp.aspen.server.host

import org.aspen_ddp.aspen.server.FileStat
import org.aspen_ddp.aspen.server.transfer.StoreTransfer

/** The marker files a store directory can carry. Every test is FileStat's: a stat that fails is never taken for an
 *  absence. */
object StoreDirMarkers:

  /** A marker that keeps a store from being loaded:
   *   - TransferredIn: it arrived from another host and awaits its pool update */
  enum Marker(val fileName: String):
    case TransferredIn extends Marker(StoreTransfer.TransferredInMarkerFile)

  /** Whether `storePath` carries `marker`. Throws the stat's error when that cannot be told */
  def has(storePath: os.Path, marker: Marker): Boolean =
    FileStat.exists(storePath / marker.fileName)

  /** Whether `storePath` is known to carry `marker`: a stat that fails answers false, for a caller that waits for a
   *  later pass, whose load stats it again */
  def knownPresent(storePath: os.Path, marker: Marker): Boolean =
    FileStat.existence(storePath / marker.fileName).contains(true)
