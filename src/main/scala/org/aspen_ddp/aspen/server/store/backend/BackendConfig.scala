package org.aspen_ddp.aspen.server.store.backend

import org.aspen_ddp.aspen.common.ida.IDA

sealed abstract class BackendConfig:
  val backendName: String

  /** The backend's own slice limit, in bytes, or `None` for no limit. See `StoragePoolState.effectiveMaxSliceSize`
    * for how it combines with the pool's limit. Most code should use the pool's derived `maxObjectSize` instead.
    */
  def maxSliceSize: Option[Int]

/** @param maxSliceSize Optional limit, in bytes, on the slice a store using this backend will accept. Chosen at pool
  *                     creation and combined with the pool's own maxSliceSize as min(pool, backend) to give each
  *                     store's effective limit. Most code should use the derived maxObjectSize instead.
  */
case class RocksDBConfig(maxSliceSize: Option[Int] = None) extends BackendConfig:
  val backendName: String = "rocksdb"

/** FSIF: every slice lives in a fixed-size segment of one flat file, addressed by an external index in
  * `[0, segmentCount)` carried in the storePointer. Both are pool-wide, since every store of the pool shares one
  * index space.
  *
  * @param segmentPayloadSize the largest slice, in bytes, a segment holds. It is the backend's `maxSliceSize`.
  * @param segmentCount       the number of external indices (the allocation bitmap's size)
  */
case class FsifConfig(segmentPayloadSize: Int, segmentCount: Int) extends BackendConfig:
  require(segmentPayloadSize > 0, s"segmentPayloadSize must be positive: $segmentPayloadSize")
  require(segmentCount > 0, s"segmentCount must be positive: $segmentCount")

  val backendName: String = "fsif"

  def maxSliceSize: Option[Int] = Some(segmentPayloadSize)

object FsifConfig:
  /** The config whose segments are the smallest that hold the slices of a `maxObjectSize`-byte object under `ida`
    *
    * @throws IllegalArgumentException if `ida` cannot encode `maxObjectSize` bytes into slices of at most
    *                                  `Int.MaxValue` bytes
    */
  def forObjectSize(ida: IDA, maxObjectSize: Int, segmentCount: Int): FsifConfig =
    val size = ida.minSliceSizeFor(maxObjectSize)
    require(size <= Int.MaxValue, s"a $maxObjectSize-byte object needs $size-byte slices under $ida: too large")
    FsifConfig(size.toInt, segmentCount)
