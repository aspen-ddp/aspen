package org.aspen_ddp.aspen.vdisk

import com.github.blemale.scaffeine.{Cache, Scaffeine}

import java.util.Arrays
import scala.concurrent.ExecutionContext

/** Known committed bytes of one block object, as sorted non-overlapping (offset, bytes) pieces. Immutable: merging
  * builds a new block, and the piece arrays are never mutated once held, so a block may be shared freely.
  */
final class CleanBlock private (val pieces: Vector[(Int, Array[Byte])]):

  val bytes: Long = pieces.foldLeft(0L)((n, p) => n + p._2.length)

  /** This block with `data` at `offset` replacing the parts of the pieces it overlaps. Takes ownership of `data` */
  def merge(offset: Int, data: Array[Byte]): CleanBlock =
    if data.isEmpty then
      this
    else
      val end = offset + data.length
      val b = Vector.newBuilder[(Int, Array[Byte])]
      var inserted = false
      def insert(): Unit =
        if !inserted then
          b += ((offset, data))
          inserted = true
      pieces.foreach: (o, d) =>
        val e = o + d.length
        if e <= offset then
          b += ((o, d))
        else if o >= end then
          insert()
          b += ((o, d))
        else
          if o < offset then
            b += ((o, Arrays.copyOfRange(d, 0, offset - o)))
          insert()
          if e > end then
            b += ((end, Arrays.copyOfRange(d, end - o, d.length)))
      insert()
      CleanBlock(b.result())

object CleanBlock:
  val empty: CleanBlock = CleanBlock(Vector.empty)


/** Committed block content held by the owner, bounded to `maxBytes` of piece data. Entries are evicted whole, by
  * the cache's size policy. Maintenance runs on the calling thread, so the bound holds once each call returns.
  * Thread-safe; `VirtualDisk` calls it under its monitor, which keeps a merge consistent with the block's revision.
  */
final class CleanCache(maxBytes: Long):

  private val cache: Cache[Long, CleanBlock] = Scaffeine()
    .executor(ExecutionContext.parasitic)
    .maximumWeight(maxBytes)
    .weigher[Long, CleanBlock]((_, block) => math.min(block.bytes, Int.MaxValue).toInt)
    .build[Long, CleanBlock]()

  def get(index: Long): Option[CleanBlock] = cache.getIfPresent(index)

  /** Adds `bytes` at `offset` to the block's entry, replacing what it overlaps. Takes ownership of `bytes` */
  def merge(index: Long, offset: Int, bytes: Array[Byte]): Unit =
    if bytes.nonEmpty then
      cache.asMap().updateWith(index):
        case Some(block) => Some(block.merge(offset, bytes))
        case None => Some(CleanBlock.empty.merge(offset, bytes))

  def invalidate(index: Long): Unit = cache.invalidate(index)

  def clear(): Unit = cache.invalidateAll()
