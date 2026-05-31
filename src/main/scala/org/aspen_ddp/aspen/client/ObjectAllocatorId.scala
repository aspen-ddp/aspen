package org.aspen_ddp.aspen.client

import org.aspen_ddp.aspen.common.allocation_group.AllocationGroupId
import org.aspen_ddp.aspen.common.pool.PoolId

import java.nio.ByteBuffer
import java.util.UUID

object ObjectAllocatorId:
  def apply(arr: Array[Byte]): ObjectAllocatorId =
    require(arr.length == 17)
    val bb = ByteBuffer.wrap(arr)
    val code = bb.get()
    val msb = bb.getLong()
    val lsb = bb.getLong()
    val uuid = UUID(msb, lsb)
    code match
      case ObjectAllocatorType.Pool.code => PoolObjectAllocatorId(PoolId(uuid))
      case ObjectAllocatorType.Group.code => GroupObjectAllocatorId(AllocationGroupId(uuid))
      case _ => throw Exception(s"Unknown Allocator Type Code $code")

sealed abstract class ObjectAllocatorId:
  def uuid: UUID
  def allocatorType: ObjectAllocatorType

  def toArray: Array[Byte] =
    val bb = ByteBuffer.allocate(17)
    bb.put(allocatorType.code)
    bb.putLong(0, uuid.getMostSignificantBits)
    bb.putLong(8, uuid.getLeastSignificantBits)
    bb.array()

case class PoolObjectAllocatorId(poolId: PoolId) extends ObjectAllocatorId:
  def uuid: UUID = poolId.uuid
  def allocatorType: ObjectAllocatorType = ObjectAllocatorType.Pool

case class GroupObjectAllocatorId(groupId: AllocationGroupId) extends ObjectAllocatorId:
  def uuid: UUID = groupId.uuid
  def allocatorType: ObjectAllocatorType = ObjectAllocatorType.Group
