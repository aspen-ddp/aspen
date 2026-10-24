package org.aspen_ddp.aspen.client

import org.aspen_ddp.aspen.common.objects.{DataObjectPointer, ObjectRevision}

import java.util.Arrays

/** Overwrites the object's bytes at `offset` with `data`. Bytes beyond the current end of the object extend it */
final case class RangedWrite(offset: Int, data: Array[Byte]):

  def end: Long = offset.toLong + data.length

  override def equals(other: Any): Boolean = other match
    case that: RangedWrite => offset == that.offset && Arrays.equals(data, that.data)
    case _ => false

  override def hashCode: Int = 31 * Arrays.hashCode(data) + offset


/** The object state a ranged write needs: the object's revision, its logical length, and the current bytes
  * under every range that will be written. The revision becomes the revision requirement of the update, so a
  * stale state aborts the transaction.
  *
  * `pieces` holds (offset, bytes) pairs of known content, sorted by offset and lying within the object.
  */
final class RangedWriteState private[aspen] (
  val pointer: DataObjectPointer,
  val revision: ObjectRevision,
  val length: Int,
  pieces: List[(Int, Array[Byte])]):

  /** The existing bytes in [offset, offset + len) clipped to the object length. Throws IllegalArgumentException
    * if this state does not hold all of them
    */
  def oldBytes(offset: Int, len: Int): Array[Byte] =
    require(offset >= 0 && len >= 0, s"Invalid range offset $offset length $len")
    val end = math.min(offset.toLong + len, length.toLong).toInt
    if offset >= end then
      Array.emptyByteArray
    else
      val result = new Array[Byte](end - offset)
      var pos = offset
      while pos < end do
        pieces.find((o, b) => o <= pos && pos < o + b.length) match
          case None =>
            throw new IllegalArgumentException(s"Ranged write state for ${pointer.id} does not hold byte $pos")
          case Some((o, b)) =>
            val n = math.min(end, o + b.length) - pos
            System.arraycopy(b, pos - o, result, pos - offset, n)
            pos += n
      result


object RangedWriteState:

  /** Uses the full content of a normal read. No further read is needed */
  def fromObject(state: DataObjectState): RangedWriteState =
    val data = state.data.getByteArray
    new RangedWriteState(state.pointer, state.revision, data.length, List((0, data)))

  /** Builds a state from byte ranges read at a single revision of an object of `length` bytes.
    * Each piece is (offset, bytes read) and must lie within the object
    */
  private[aspen] def fromRanges(
    pointer: DataObjectPointer,
    revision: ObjectRevision,
    length: Int,
    pieces: List[(Int, Array[Byte])]): RangedWriteState =
    require(length >= 0, s"Invalid object length $length")
    pieces.foreach: (o, b) =>
      require(o >= 0 && o.toLong + b.length <= length,
        s"Piece at $o of ${b.length} bytes lies outside an object of $length bytes")
    new RangedWriteState(pointer, revision, length, pieces.sortBy(_._1))
