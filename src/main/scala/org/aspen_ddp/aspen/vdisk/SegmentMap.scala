package org.aspen_ddp.aspen.vdisk

import org.aspen_ddp.aspen.client.RangedWrite

import java.util.TreeMap
import scala.jdk.CollectionConverters.*

/** Immutable view of the pieces held by a SegmentMap. Pieces are sorted by offset and do not overlap. The piece
  * arrays are never mutated after being created, so a snapshot may be shared freely.
  */
final class SegmentSnapshot(val pieces: Vector[(Int, Array[Byte])], val maxSeq: Long):

  /** The highest byte covered plus one, or 0 when empty */
  def end: Int = pieces.lastOption.map((o, b) => o + b.length).getOrElse(0)

  /** Copies the covered bytes into `target`, which holds the block bytes starting at `targetOffsetInBlock`.
    * Bytes of `target` that no piece covers are left untouched
    */
  def overlay(target: Array[Byte], targetOffsetInBlock: Int): Unit =
    val targetEnd = targetOffsetInBlock + target.length
    pieces.foreach: (offset, data) =>
      val from = math.max(offset, targetOffsetInBlock)
      val to = math.min(offset + data.length, targetEnd)
      if from < to then
        System.arraycopy(data, from - offset, target, from - targetOffsetInBlock, to - from)

  /** One write per piece. Adjacent pieces touch but never overlap */
  def writes: List[RangedWrite] = pieces.toList.map((o, b) => RangedWrite(o, b))


/** Non-overlapping (offset, bytes, seq) pieces within one block. Newer puts replace the overlapped parts of older
  * pieces. Not thread-safe; the owner serializes access. Piece arrays are never mutated once stored, so
  * snapshots can share them.
  */
final class SegmentMap:

  private final case class Piece(offset: Int, data: Array[Byte], seq: Long):
    def end: Int = offset + data.length

  private val pieces = new TreeMap[Int, Piece]()
  private var held = 0L

  def bytes: Long = held

  def isEmpty: Boolean = pieces.isEmpty

  def maxSeq: Long = pieces.values.asScala.foldLeft(0L)((m, p) => math.max(m, p.seq))

  def put(offset: Int, data: Array[Byte], seq: Long): Unit =
    if data.length > 0 then
      val end = offset + data.length
      // Remove every piece overlapping [offset, end), keeping the parts outside it
      val first = Option(pieces.floorEntry(offset)).filter(_.getValue.end > offset).map(_.getKey).getOrElse(offset)
      val overlapped = pieces.subMap(first, true, end, false).values.asScala.toList
      overlapped.foreach: p =>
        remove(p)
        if p.offset < offset then
          add(Piece(p.offset, java.util.Arrays.copyOfRange(p.data, 0, offset - p.offset), p.seq))
        if p.end > end then
          add(Piece(end, java.util.Arrays.copyOfRange(p.data, end - p.offset, p.data.length), p.seq))
      add(Piece(offset, data.clone(), seq))

  def snapshot: SegmentSnapshot =
    val ps = pieces.values.asScala.map(p => (p.offset, p.data)).toVector
    SegmentSnapshot(ps, maxSeq)

  /** Drops every piece whose seq is <= `seq` */
  def removeUpTo(seq: Long): Unit =
    pieces.values.asScala.filter(_.seq <= seq).toList.foreach(remove)

  private def add(p: Piece): Unit =
    pieces.put(p.offset, p)
    held += p.data.length

  private def remove(p: Piece): Unit =
    pieces.remove(p.offset)
    held -= p.data.length
