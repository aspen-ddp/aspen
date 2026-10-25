package org.aspen_ddp.aspen.client.tkvl

import org.aspen_ddp.aspen.client.{ObjectAllocator, Transaction}
import org.aspen_ddp.aspen.common.objects.{Key, KeyOrdering, KeyValueObjectPointer, ObjectPointer, Value}

import scala.concurrent.{ExecutionContext, Future}

trait RootManager {

  def getRoot(): Future[Root]
  
  /** Returns (numTiers, keyOrdering, rootNode) */
  def getRootNode(): Future[(Int, KeyOrdering, Option[ListNode])]

  /** getRootNode for trees whose nodes are KeyValueObjects. */
  final def getKeyValueRootNode(): Future[(Int, KeyOrdering, Option[KeyValueListNode])] =
    given ExecutionContext = ExecutionContext.parasitic
    getRootNode().map: (tier, ordering, onode) =>
      val kvNode = onode.map:
        case kvn: KeyValueListNode => kvn
        case other => throw new IllegalStateException(s"Expected a KeyValueListNode but found ${other.getClass.getName}")
      (tier, ordering, kvNode)

  /** Implementations must only commit if the tree is still empty at commit time.
   *  TieredKeyValueList.set relies on this in place of key requirements.
   */
  def createInitialNode(contents: Map[Key,Value])(using tx: Transaction): Future[Unit]

  def getAllocatorForTier(tier: Int): Future[ObjectAllocator]

  def getMaxNodeSize(tier: Int): Future[Int]

  def prepareRootUpdate(newTier: Int, onewRoot: Option[ObjectPointer])(using tx: Transaction): Future[Unit]

  def encode(): Array[Byte]

  def typeId: RootManagerTypeId
}
