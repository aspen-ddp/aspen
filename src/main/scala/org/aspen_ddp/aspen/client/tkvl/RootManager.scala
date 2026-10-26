package org.aspen_ddp.aspen.client.tkvl

import org.aspen_ddp.aspen.client.{ConflictingRequirements, MultipleDataUpdatesToObject, ObjectAllocator, Transaction}
import org.aspen_ddp.aspen.common.objects.{Key, KeyOrdering, KeyValueObjectPointer, ObjectPointer, Value}

import scala.concurrent.{ExecutionContext, Future}

trait RootManager {

  /** Holds the root and, for trees of tier >= 1, the root node. Consulted by `getRoot`, `getRootNode`,
   *  `getAllocatorForTier` and `getMaxNodeSize`, never by root writes.
   */
  def cache: TierCache

  def getRoot(): Future[Root]
  
  /** Returns (numTiers, keyOrdering, rootNode) */
  def getRootNode(): Future[(Int, KeyOrdering, Option[ListNode])]

  /** getRootNode, bypassing the cache. The result refreshes the cache. */
  def getFreshRootNode(): Future[(Int, KeyOrdering, Option[ListNode])]

  /** getRootNode for trees whose nodes are KeyValueObjects. */
  final def getKeyValueRootNode(): Future[(Int, KeyOrdering, Option[KeyValueListNode])] =
    given ExecutionContext = ExecutionContext.parasitic
    getRootNode().map: (tier, ordering, onode) =>
      val kvNode = onode.map:
        case kvn: KeyValueListNode => kvn
        case other => throw new IllegalStateException(s"Expected a KeyValueListNode but found ${other.getClass.getName}")
      (tier, ordering, kvNode)

  /** Implementations must only commit if the tree is still empty at commit time. Contents too large for one tier-0
   *  node are divided among a list of nodes (see `NodeEncoding.allocateChain`), each after the first added to tier 1
   *  by a `SplitFinalizationAction`.
   *  TieredKeyValueList.set relies on this in place of key requirements. Fails with RootAlreadyModified, aborting
   *  the transaction, if the transaction already updates the root.
   */
  def createInitialNode(contents: Map[Key,Value])(using tx: Transaction): Future[Unit]

  def getAllocatorForTier(tier: Int): Future[ObjectAllocator]

  def getMaxNodeSize(tier: Int): Future[Int]

  def prepareRootUpdate(newTier: Int, onewRoot: Option[ObjectPointer])(using tx: Transaction): Future[Unit]

  def encode(): Array[Byte]

  def typeId: RootManagerTypeId
}

object RootManager:

  /** The cached root unless `fresh` or nothing is cached. Otherwise runs `read` and caches its result. */
  def cachedRead(cache: TierCache, fresh: Boolean)(read: => Future[RootState])
                (using ExecutionContext): Future[RootState] =
    val cached = if fresh then None else cache.root
    cached match
      case Some(rs) => Future.successful(rs)
      case None =>
        read.map: rs =>
          cache.putRoot(rs)
          rs

  /** Runs `update`, a `createInitialNode` write of `host`, the object holding the tree's root. The transaction
    * rejects it if it already updates `host`, as when an earlier update of the same transaction already created the
    * initial node. The new node is staged by then, so this aborts the transaction with `RootAlreadyModified` rather
    * than leave the node unreferenced.
    */
  def updateInitialRoot(host: ObjectPointer, tx: Transaction)(update: => Unit): Unit =
    try
      update
    catch
      case _: MultipleDataUpdatesToObject | _: ConflictingRequirements =>
        tx.abortAndThrow(RootAlreadyModified(host))
