package org.aspen_ddp.aspen.compute

import org.aspen_ddp.aspen.common.HLCTimestamp
import org.aspen_ddp.aspen.common.objects.{KeyValueObjectPointer, ObjectPointer}

import java.nio.ByteBuffer
import java.util.UUID

object ServiceEntry:
  val UnclaimedHostId: UUID = new UUID(0, 0)

  def decode(bytes: Array[Byte]): ServiceEntry =
    val bb = ByteBuffer.wrap(bytes)
    val typeUUID = new UUID(bb.getLong(), bb.getLong())
    val hostId   = new UUID(bb.getLong(), bb.getLong())
    val leaseExpiry = HLCTimestamp(bb.getLong())
    val ptrBytes = new Array[Byte](bb.remaining())
    bb.get(ptrBytes)
    ObjectPointer(ptrBytes) match
      case p: KeyValueObjectPointer =>
        ServiceEntry(typeUUID, hostId, leaseExpiry, p)
      case other =>
        throw new IllegalArgumentException(
          s"ServiceEntry statePointer must be KeyValueObjectPointer, got ${other.objectType}")

final case class ServiceEntry(
  typeUUID: UUID,
  hostId: UUID,
  leaseExpiry: HLCTimestamp,
  statePointer: KeyValueObjectPointer
):
  def encode(): Array[Byte] =
    val ptrBytes = statePointer.toArray
    val bb = ByteBuffer.allocate(16 + 16 + 8 + ptrBytes.length)
    bb.putLong(typeUUID.getMostSignificantBits)
    bb.putLong(typeUUID.getLeastSignificantBits)
    bb.putLong(hostId.getMostSignificantBits)
    bb.putLong(hostId.getLeastSignificantBits)
    bb.putLong(leaseExpiry.asLong)
    bb.put(ptrBytes)
    bb.array()

  def isExpired: Boolean = leaseExpiry < HLCTimestamp.now

  def isClaimed: Boolean = hostId != ServiceEntry.UnclaimedHostId
