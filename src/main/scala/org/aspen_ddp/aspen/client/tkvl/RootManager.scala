package org.aspen_ddp.aspen.client.tkvl

import org.aspen_ddp.aspen.client.{ObjectAllocator, Transaction}
import org.aspen_ddp.aspen.common.objects.{AllocationRevisionGuard, Key, KeyOrdering, KeyValueObjectPointer, Value}

import scala.concurrent.Future

trait RootManager {

  /** Returns (numTiers, keyOrdering, rootNode) */
  def getRootNode(): Future[(Int, KeyOrdering, Option[KeyValueListNode])]

  def createInitialNode(contents: Map[Key,Value])(using tx: Transaction): Future[AllocationRevisionGuard]

  def getAllocatorForTier(tier: Int): Future[ObjectAllocator]

  def getMaxNodeSize(tier: Int): Future[Int]

  def prepareRootUpdate(newTier: Int, newRoot: KeyValueObjectPointer)(using tx: Transaction): Future[Unit]

  def getRootRevisionGuard(): Future[AllocationRevisionGuard]

  def encode(): Array[Byte]

  def typeId: RootManagerTypeId
}
