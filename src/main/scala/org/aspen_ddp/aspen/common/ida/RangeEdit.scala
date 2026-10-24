package org.aspen_ddp.aspen.common.ida

import java.util.Arrays

/** One write of newBytes at logical offset of an object. oldBytes holds the current content under
  * the write that lies within the current object: max(0, min(newBytes.length, currentLength - offset))
  * bytes, empty when the write begins at or beyond the current end.
  */
final case class RangeEdit(offset: Int, newBytes: Array[Byte], oldBytes: Array[Byte]):

  def end: Long = offset.toLong + newBytes.length

  /** The XOR delta for logical byte pos, which must fall within this edit */
  def deltaAt(pos: Int): Byte =
    val j = pos - offset
    if j < oldBytes.length then
      (newBytes(j) ^ oldBytes(j)).toByte
    else
      newBytes(j)

  override def equals(other: Any): Boolean = other match
    case that: RangeEdit =>
      offset == that.offset && Arrays.equals(newBytes, that.newBytes) && Arrays.equals(oldBytes, that.oldBytes)
    case _ => false

  override def hashCode: Int = 31 * (31 * Arrays.hashCode(newBytes) + Arrays.hashCode(oldBytes)) + offset


object RangeEdit:

  /** Validates the edits against an object of currentLength bytes and returns them sorted by offset
    * along with the resulting object length
    */
  def validate(currentLength: Int, edits: List[RangeEdit]): (List[RangeEdit], Long) =
    require(currentLength >= 0, s"Invalid current length $currentLength")
    require(edits.nonEmpty, "Ranged write requires at least one edit")
    val sorted = edits.sortBy(_.offset)
    sorted.foldLeft(0L) { (prevEnd, e) =>
      require(e.offset >= 0, s"Invalid edit offset ${e.offset}")
      require(e.newBytes.length > 0, s"Empty edit at offset ${e.offset}")
      require(e.offset >= prevEnd, s"Edit at ${e.offset} overlaps a previous edit")
      val overlap = math.max(0L, math.min(e.newBytes.length.toLong, currentLength.toLong - e.offset))
      require(e.oldBytes.length == overlap,
        s"Edit at ${e.offset} carries ${e.oldBytes.length} old bytes. Expected $overlap")
      e.end
    }
    (sorted, math.max(currentLength.toLong, sorted.last.end))
