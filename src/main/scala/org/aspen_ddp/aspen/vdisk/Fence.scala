package org.aspen_ddp.aspen.vdisk

import java.nio.ByteBuffer
import java.util.UUID

/** Contents of a disk's fence object. Each owner takeover increments the epoch; an owner that has been fenced is
  * detected by the abort of any transaction that shared-locks the fence revision it observed.
  */
final case class FenceState(epoch: Long, owner: UUID):

  def toArray: Array[Byte] =
    val bb = ByteBuffer.allocate(FenceState.EncodedSize)
    bb.putLong(epoch)
    bb.putLong(owner.getMostSignificantBits)
    bb.putLong(owner.getLeastSignificantBits)
    bb.array()

object FenceState:
  val EncodedSize: Int = 24

  val Initial: FenceState = FenceState(0L, new UUID(0L, 0L))

  def fromArray(arr: Array[Byte]): FenceState =
    require(arr.length == EncodedSize, s"fence state must be $EncodedSize bytes, not ${arr.length}")
    val bb = ByteBuffer.wrap(arr)
    val epoch = bb.getLong()
    val msb = bb.getLong()
    val lsb = bb.getLong()
    FenceState(epoch, new UUID(msb, lsb))
