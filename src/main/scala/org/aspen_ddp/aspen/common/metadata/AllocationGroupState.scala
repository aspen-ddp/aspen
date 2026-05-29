package org.aspen_ddp.aspen.common.metadata

import org.aspen_ddp.aspen.client.DataObjectState
import org.aspen_ddp.aspen.codec
import org.aspen_ddp.aspen.common.allocation_group.AllocationGroupId
import org.aspen_ddp.aspen.common.network.Codec

import java.util.UUID

object AllocationGroupState:

  enum MemberType:
    case Pool, Group

  case class Member(memberType: MemberType,
                    uuid: UUID,
                    maxObjectSize: Option[Int],
                    currentUsage: Long,
                    maximumSize: Long)

  def apply(cfg: Array[Byte]): AllocationGroupState = Codec.decode(codec.AllocationGroupState.parseFrom(cfg))
  def apply(dos: DataObjectState): AllocationGroupState = apply(dos.data.getByteArray)


final case class AllocationGroupState(
                                     groupId: AllocationGroupId,
                                     level: Int,
                                     name: String,
                                     members: List[AllocationGroupState.Member],
                                     parentGroups: List[AllocationGroupId]
                                     ):

  def toBytes: Array[Byte] = Codec.encode(this).toByteArray

  def currentUsage: Long = members.foldLeft(0L)((acc, m) => acc + m.currentUsage)
  def maximumSize: Long = members.foldLeft(0L)((acc, m) => acc + m.maximumSize)
  
  def newCurrentUsage(uuid: UUID, newUsage: Long): Long =
    members.foldLeft(0L): (acc, m) =>
      val sz = if m.uuid == uuid then newUsage else m.currentUsage
      acc + sz
  
  /** Returns the smallest maximumObjectSize for all members */
  def maximumObjectSize: Option[Int] =
    members.foldLeft[Option[Int]](None): (acc, m) => 
      acc match
        case None => m.maxObjectSize 
        case Some(min) => 
          if m.maxObjectSize.getOrElse(Int.MaxValue) < min then 
            m.maxObjectSize 
          else 
            acc
      
          
