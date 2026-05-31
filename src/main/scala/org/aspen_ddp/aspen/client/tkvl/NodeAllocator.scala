package org.aspen_ddp.aspen.client.tkvl

import java.nio.ByteBuffer
import java.util.UUID
import org.aspen_ddp.aspen.client.{AspenClient, ObjectAllocator, ObjectAllocatorId}
import org.aspen_ddp.aspen.client.internal.allocation.PoolObjectAllocator
import org.aspen_ddp.aspen.common.Radicle
import org.aspen_ddp.aspen.common.pool.PoolId

import scala.concurrent.{ExecutionContext, Future}

object NodeAllocator:
  val encodedTierSize: Int = ObjectAllocatorId.encodedSize + 4

  def apply(client: AspenClient, bb: ByteBuffer): NodeAllocator =
    var ntiers: Int = bb.get()
    var tiers: List[(ObjectAllocatorId, Int)] = Nil

    while ntiers > 0 do
      ntiers -= 1
      val allocatorId = ObjectAllocatorId(bb)
      val maxNodeSize = bb.getInt()
      tiers = (allocatorId, maxNodeSize) :: tiers

    new NodeAllocator(client, tiers.reverse)


class NodeAllocator(val client:AspenClient, config: Seq[(ObjectAllocatorId, Int)]):

  val tiers: Array[(ObjectAllocatorId, Int)] = config.toArray

  def encodedSize: Int = 1 + (NodeAllocator.encodedTierSize * tiers.length)

  def getAllocatorForTier(tier: Int): Future[ObjectAllocator] =
    val allocatorId = if tier < tiers.length then tiers(tier)._1 else tiers.last._1
    client.getAllocator(allocatorId)

  def getMaxNodeSize(tier: Int): Int =
    if tier < tiers.length then tiers(tier)._2 else tiers.last._2

  def encodeInto(bb:ByteBuffer): Unit =
    bb.put(tiers.length.toByte)
    tiers.foreach: tier =>
      tier._1.encodeInto(bb)
      bb.putInt(tier._2)
