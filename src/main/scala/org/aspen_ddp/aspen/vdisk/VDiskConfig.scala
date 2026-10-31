package org.aspen_ddp.aspen.vdisk

import scala.concurrent.duration.*

/** Tuning of a served virtual disk.
  *
  * @param dirtyHardLimit bytes of uncommitted write data at which writers are throttled
  * @param dirtySoftLimit bytes of uncommitted write data above which writeback skips the coalescing window
  * @param coalesceWindow how long a block's dirty data waits for more writes before it is written back. FLUSH and
  *                       FUA bypass it
  * @param maxInFlightDataTx most data transactions in flight at once
  * @param maxMapTxBytes initial content bytes a single map transaction allocates, beyond its first block
  * @param cleanCacheBytes bound on cached committed data
  * @param maxReadahead largest readahead past a sequential read
  */
final case class VDiskConfig(dirtyHardLimit: Long = 256L * 1024 * 1024,
                             dirtySoftLimit: Long = 64L * 1024 * 1024,
                             coalesceWindow: FiniteDuration = 20.millis,
                             maxInFlightDataTx: Int = 64,
                             maxMapTxBytes: Long = 32L * 1024 * 1024,
                             cleanCacheBytes: Long = 512L * 1024 * 1024,
                             maxReadahead: Int = 1024 * 1024):
  require(dirtySoftLimit <= dirtyHardLimit,
    s"dirtySoftLimit ($dirtySoftLimit) may not exceed dirtyHardLimit ($dirtyHardLimit)")
  require(maxInFlightDataTx >= 1, s"maxInFlightDataTx ($maxInFlightDataTx) must be at least 1")
