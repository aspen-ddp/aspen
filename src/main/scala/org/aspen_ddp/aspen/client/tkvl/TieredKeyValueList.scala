package org.aspen_ddp.aspen.client.tkvl

import org.aspen_ddp.aspen.client.{AspenClient, InvalidObject, Transaction}
import org.aspen_ddp.aspen.client.KeyValueObjectState.ValueState
import org.aspen_ddp.aspen.common.objects.{Key, KeyOrdering, KeyValueObjectPointer, ObjectId, ObjectRevision, Value}
import org.aspen_ddp.aspen.common.transaction.KeyValueUpdate
import scribe.Logging

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success}

class TieredKeyValueList(val client: AspenClient,
                         val rootManager: RootManager) extends Logging {

  given ExecutionContext = client.clientContext

  import TieredKeyValueList._

  def get(key: Key): Future[Option[ValueState]] = {
    rootManager.getRootNode().flatMap { t =>
      val (tier, ordering, onode) = t
      onode match {
        case None => Future.successful(None)
        case Some(node) =>
          fetchContainingNode(client, tier, 0, ordering, key, node, Set()).map {
            case Left(_) => throw new BrokenTree()
            case Right(node) => node.contents.get(key)
          }
      }
    }
  }

  def getContainingNode(key: Key): Future[Option[TieredKeyValueListNode]] = {
    rootManager.getRootNode().flatMap { t =>
      val (tier, ordering, onode) = t
      onode match {
        case None => Future.successful(None)
        case Some(node) =>
          fetchContainingNode(client, tier, 0, ordering, key, node, Set()).map {
            case Left(_) => throw new BrokenTree()
            case Right(node) => Some(new TieredKeyValueListNode(this, node))
          }
      }
    }
  }
  
  def deleteTree(odeleteKVPair: Option[(Key, ValueState) => Future[Unit]]): Future[Unit] =
    def fetchRootNodes(path: List[(Int, KeyValueListNode)]): Future[List[(Int, KeyValueListNode)]] =
      val (tier, node) = path.head
      if tier == 0 then
        Future.successful(path)
      else
        node.contents.get(Key.AbsoluteMinimum) match 
          case None => Future.successful(path)
          case Some(vs) =>
            val downPtr = KeyValueObjectPointer(vs.value.bytes)
            client.readOptional(downPtr).flatMap:
              case None => Future.successful(path)
              case Some(kvos) =>
                val downlp = KeyValueListPointer(Key.AbsoluteMinimum, downPtr)
                val downNode = KeyValueListNode(client, downlp, node.ordering, kvos)
                val nextTierDown = tier - 1
                fetchRootNodes((nextTierDown, downNode) :: path)

    def rdelete(path: List[(Int, KeyValueListNode)]): Future[Unit] = 
      if path.isEmpty then
        Future.unit
      else 
        val (tier, node) = path.head
        val odkv = if tier == 0 then odeleteKVPair else None
        val kvlp = KeyValueListPointer(Key.AbsoluteMinimum, node.pointer)

        for 
          _ <- KeyValueListNode.deleteList(client, kvlp, node.ordering, odkv)
          _ <- rdelete(path.tail)
        yield 
          ()
    
    for
      (tier, ordering, onode) <- rootManager.getRootNode()
      
      _ <- onode match
        case None => Future.unit
        case Some(node) =>
          for
            path <- fetchRootNodes(List((tier, node)))
            _ <- rdelete(path)
            tx = client.newTransaction()
            _ <- rootManager.prepareRootUpdate(0, None)(using tx)
            _ <- tx.commit()
          yield
            ()
    yield
      ()
      
  /**
   * Splits the tree into two trees at the supplied key. All keys comparing
   * less than splitAtKey will remain in the original tree, the rest will be
   * moved to a new tree of the same depth as the original. The resulting
   * Root object will be the root of the new tree. If the inclusive parameter
   * is set to true, the key exactly matching splitAtKey will be kept in
   * the original tree. Otherwise it will wind up in the new tree
   *
   * The splitting operation is completed in a single transaction but note
   * that race conditions with outstanding split/join finalization actions
   * could cause errant content to appear in the original (left) tree.
   * Use with caution.
   */
  def splitTree(splitAtKey: Key, inclusive: Boolean = false)
               (using t: Transaction): Future[Root] = {

    def rinsert(tier: Int,
                ordering: KeyOrdering,
                rpath: List[KeyValueListNode],
                downPointer: KeyValueObjectPointer): Future[KeyValueObjectPointer] =
      if rpath.isEmpty then
        Future.successful(downPointer)
      else
        val odown = if tier > 0 then Some(downPointer) else None
        for
          alloc <- rootManager.getAllocatorForTier(tier)
          down <- rpath.head.splitAt(ordering, splitAtKey, inclusive, odown, alloc)
          kvp <- rinsert(tier + 1, ordering, rpath.tail, down.pointer)
        yield
          kvp

    // Returns the new tree's tier along with its root. The path has one node per tier, and it
    // can be a tier shorter than `tier` if a tier reduction raced the descent.
    def nonEmpty(tier: Int,
                 ordering: KeyOrdering,
                 root: KeyValueListNode): Future[(Int, KeyValueObjectPointer)] =
      for
        e <- fetchContainingNodePath(client, tier, 0, ordering, splitAtKey, root, Set())

        rpath = e match
          case Left(_) => throw new BrokenTree()
          case Right(p) => p

        // There will always be a tier0 entry, split that then recurse through
        // the full path
        alloc <- rootManager.getAllocatorForTier(0)

        down <- rpath.head.splitAt(ordering, splitAtKey, inclusive, None, alloc)

        kvp <- rinsert(1, ordering, rpath.tail, down.pointer)
      yield
        (rpath.length - 1, kvp)

    for
      oldRoot <- rootManager.getRoot()
      (tier, ordering, oroot) <- rootManager.getRootNode()
      (newTier, ptr) <- oroot match
        case Some(root) => nonEmpty(tier, ordering, root)
        case None =>
          for
            _ <- rootManager.createInitialNode(Map())
            alloc <- rootManager.getAllocatorForTier(0)
            ptr <- alloc.allocateKeyValueObject(Map[Key, Value]())
          yield
            (tier, ptr)
    yield
      Root(newTier, ordering, Some(ptr), oldRoot.nodeAllocator)
  }

  def set(key: Key,
          value: Value,
          requirement: Option[Either[Boolean, ObjectRevision]] = None)
         (using t: Transaction): Future[Unit] = {

    def onSplit(newMinimum: Key, newNode: KeyValueObjectPointer): Future[Unit] = {
      SplitFinalizationAction.addToTransaction(rootManager, 1, newMinimum, newNode, t)
      Future.unit
    }
    def empty(tier: Int, ordering: KeyOrdering): Future[Unit] = {
      // `requirement` is intentionally dropped. createInitialNode only commits
      // if the tree is still empty, in which case the key cannot exist and a
      // DoesNotExist requirement is already satisfied.
      rootManager.createInitialNode(Map(key -> value)).map(_=>())
    }
    def nonEmpty(tier: Int, ordering: KeyOrdering, root: KeyValueListNode): Future[Unit] = {
      for
        alloc <- rootManager.getAllocatorForTier(0)
        maxNodeSize <- rootManager.getMaxNodeSize(0)

        e <- fetchContainingNode(client, tier, 0, ordering, key, root, Set())

        node = e match
          case Left(_) => throw new BrokenTree()
          case Right(n) => n

        _ <- node.insert(key, value, maxNodeSize, alloc, onSplit, requirement)
      yield
        ()
    }

    rootManager.getRootNode().flatMap { t =>
      val (tier, ordering, oroot) = t
      oroot match {
        case None => empty(tier, ordering)
        case Some(root) => nonEmpty(tier, ordering, root)
      }
    }
  }

  def delete(key: Key)(using t: Transaction): Future[Unit] = {
    def onJoin(delMinimum: Key, delNode: KeyValueObjectPointer): Future[Unit] = {
      JoinFinalizationAction.addToTransaction(rootManager, 1, delMinimum, delNode, t)
      Future.unit
    }
    deleteFromContainingNode(key, _.delete(key, onJoin))
  }

  def delete(key: Key, 
             requiredRevision: Option[ObjectRevision],
             requirements: List[KeyValueUpdate.KeyRequirement])(using t: Transaction): Future[Unit] = {
    def onJoin(delMinimum: Key, delNode: KeyValueObjectPointer): Future[Unit] = {
      JoinFinalizationAction.addToTransaction(rootManager, 1, delMinimum, delNode, t)
      Future.unit
    }
    deleteFromContainingNode(key, _.delete(key, requiredRevision, requirements, onJoin))
  }

  /** Descends to the tier-0 node containing `key` and hands it to `del`.
   *
   *  Deleting a node's last key joins it with its right neighbour, which `del` reads. If a racing
   *  join already absorbed that neighbour, the containing node was read before the join and the
   *  read fails with InvalidObject. `del` adds nothing to the transaction before that read, so
   *  the delete restarts from a fresh root.
   */
  private def deleteFromContainingNode(key: Key, del: KeyValueListNode => Future[Unit]): Future[Unit] =

    def attempt(restarts: Int): Future[Unit] =
      rootManager.getRootNode().flatMap: t =>
        val (tier, ordering, oroot) = t
        oroot match
          case None => Future.unit
          case Some(root) =>
            for
              e <- fetchContainingNode(client, tier, 0, ordering, key, root, Set())
              node = e match
                case Left(_) => throw new BrokenTree()
                case Right(n) => n
              _ <- del(node).recoverWith:
                case _: InvalidObject if restarts < MaxJoinRaceRestarts => attempt(restarts + 1)
            yield
              ()

    attempt(0)

  def foldLeft[B](z: B)(fn: (B, Map[Key, ValueState]) => B): Future[B] = {

    def nonEmpty(tier: Int, ordering: KeyOrdering, root: KeyValueListNode): Future[B] = {
      for {
        e <- fetchContainingNode(client, tier, 0, ordering, Key.AbsoluteMinimum, root, Set())
        node = e match {
          case Left(_) => throw new BrokenTree()
          case Right(n) => n
        }
        result <- node.foldLeft(z)(fn)
      } yield {
        result
      }
    }
    rootManager.getRootNode().flatMap { t =>
      val (tier, ordering, oroot) = t
      oroot match {
        case None => Future.successful(z)
        case Some(root) => nonEmpty(tier, ordering, root)
      }
    }
  }

  /** Descends to the tier-0 node containing `startKey` and hands it to `walk`.
   *
   *  The three tier-0 walks below differ only in which node-level method they delegate to;
   *  sharing the descent keeps that the only difference between them. Each keeps its own name
   *  so the node-level diagnostics still report the method the caller invoked.
   */
  private def walkFromContainingNode(startKey: Key,
                                     walk: KeyValueListNode => Future[Unit]): Future[Unit] =

    def nonEmpty(tier: Int, ordering: KeyOrdering, root: KeyValueListNode): Future[Unit] =
      for
        e <- fetchContainingNode(client, tier, 0, ordering, startKey, root, Set())
        node = e match
          case Left(_) => throw new BrokenTree()
          case Right(n) => n
        _ <- walk(node)
      yield
        ()

    rootManager.getRootNode().flatMap: t =>
      val (tier, ordering, oroot) = t
      oroot match
        case None => Future.unit
        case Some(root) => nonEmpty(tier, ordering, root)

  /** Visit every entry in the tree, in ascending key order.
   *
   *  Exactly `foreachFrom(Key.AbsoluteMinimum, fn)` at both this level and the node level.
   *
   *  A failing `fn` is logged and the walk continues to the next key. A failing read fails the
   *  returned future.
   */
  def foreach(fn: (KeyValueListNode, Key, ValueState) => Future[Unit]): Future[Unit] =
    walkFromContainingNode(Key.AbsoluteMinimum, _.foreach(fn))

  /** Visit every entry in `[minKey, maxKey)`, in ascending key order. */
  def foreachInRange(minKey: Key,
                     maxKey: Key,
                     fn: (KeyValueListNode, Key, ValueState) => Future[Unit]): Future[Unit] =
    walkFromContainingNode(minKey, _.foreachInRange(minKey, maxKey, fn))

  /** Visit every entry at or above `minKey`, in ascending key order. The open-ended
   *  counterpart to foreachInRange, used to resume an interrupted walk from a checkpointed
   *  key.
   *
   *  The bound is inclusive: if `minKey` is still present it is visited again. A consumer that
   *  checkpoints the last key it finished therefore re-processes exactly that one key on
   *  resume, so whatever it does per key must be idempotent. That is the safe side of the
   *  trade - an exclusive bound would turn a crash between doing the work and recording the
   *  checkpoint into work that is permanently skipped.
   */
  def foreachFrom(minKey: Key,
                  fn: (KeyValueListNode, Key, ValueState) => Future[Unit]): Future[Unit] =
    walkFromContainingNode(minKey, _.foreachFrom(minKey, fn))
}

object TieredKeyValueList {

  /*
   Returns Left if the tree is broken or Right with a list of nodes for each
   tier in the path down to the requested node in reverse order. The top of
   the tree will be the last node in the list
   */
  private[tkvl] def fetchContainingNode(client: AspenClient,
                                        currentTier: Int,
                                        targetTier: Int,
                                        ordering: KeyOrdering,
                                        target: Key,
                                        currentNode: KeyValueListNode,
                                        initialBlacklist: Set[ObjectId],
                                        reversePath: List[KeyValueListNode] = Nil): Future[Either[Set[ObjectId], KeyValueListNode]] = {
    given ExecutionContext = client.clientContext

    fetchContainingNodePath(client, currentTier, targetTier, ordering, target,
      currentNode, initialBlacklist).map {
      case Left(l) => Left(l)
      case Right(rpath) => Right(rpath.head)
    }
  }

  /** How many times a traversal restarts after reading a node that a racing join deleted.
   *
   *  A restart only fails again if yet another join races it, so exhausting this means the
   *  tree really does point at a deleted node.
   */
  private[tkvl] val MaxJoinRaceRestarts = 10

  /*
   A join deletes the right-hand node in the same transaction that rewrites the left node's
   right pointer, so a right scan that read the left node just before the join reads a deleted
   node and fails with InvalidObject. Upper-tier reads are already tolerant of this (fetchNode
   blacklists them), so any InvalidObject here comes from the target-tier scan. currentNode is
   the leftmost node of its tier, which a join never deletes, so refreshing it and descending
   again sees the list as the join left it.

   Tier reduction does delete the root, though, so the refresh fails if one ran after the caller
   read it. The deleted root held only its AbsoluteMinimum child, which became the new root, so
   the restart descends from that child one tier down instead.
   */
  private[tkvl] def fetchContainingNodePath(client: AspenClient,
                                            currentTier: Int,
                                            targetTier: Int,
                                            ordering: KeyOrdering,
                                            target: Key,
                                            currentNode: KeyValueListNode,
                                            initialBlacklist: Set[ObjectId],
                                            reversePath: List[KeyValueListNode] = Nil): Future[Either[Set[ObjectId], List[KeyValueListNode]]] = {

    given ExecutionContext = client.clientContext

    def leftmostChild(stale: KeyValueListNode): Future[KeyValueListNode] =
      stale.contents.get(Key.AbsoluteMinimum) match
        case None => Future.failed(new BrokenTree())
        case Some(vs) =>
          val lp = KeyValueListPointer(Key.AbsoluteMinimum, KeyValueObjectPointer(vs.value.bytes))
          client.read(lp.pointer, s"TKVL restart below deleted root ${stale.pointer.id}").map: kvos =>
            KeyValueListNode(client, lp, ordering, kvos)

    def attempt(tier: Int,
                start: KeyValueListNode,
                restarts: Int): Future[Either[Set[ObjectId], List[KeyValueListNode]]] =
      descend(client, tier, targetTier, ordering, target, start, initialBlacklist, reversePath).recoverWith:
        case _: InvalidObject if restarts < MaxJoinRaceRestarts =>
          start.refresh().map(n => (tier, n)).recoverWith:
            case _: InvalidObject if tier > targetTier => leftmostChild(start).map(n => (tier - 1, n))
          .flatMap((t, n) => attempt(t, n, restarts + 1))

    attempt(currentTier, currentNode, 0)
  }

  private def descend(client: AspenClient,
                      currentTier: Int,
                      targetTier: Int,
                      ordering: KeyOrdering,
                      target: Key,
                      currentNode: KeyValueListNode,
                      initialBlacklist: Set[ObjectId],
                      reversePath: List[KeyValueListNode]): Future[Either[Set[ObjectId], List[KeyValueListNode]]] = {

    given ExecutionContext = client.clientContext

    if (currentTier == targetTier) {
      // Once we're on the right tier, we can rely on consistent right pointers to scan to the
      // containing node
      currentNode.fetchContainingNode(target).map(n => Right(n :: reversePath))
    } else {

      val p = Promise[Either[Set[ObjectId], List[KeyValueListNode]]]()

      def rtry(candidates: List[(Key, KeyValueObjectPointer)], blacklist: Set[ObjectId]): Unit = {
        if (candidates.isEmpty) {
          p.success(Left(blacklist + currentNode.pointer.id))
        } else {
          fetchNode(client, ordering, targetTier, candidates.head._1, candidates.head._2, blacklist) foreach {
            case Left(blklst) => rtry(candidates.tail, blklst)
            case Right(next) => descend(client, currentTier-1, targetTier, ordering, target,
              next, blacklist, currentNode :: reversePath).onComplete {
              case Failure(err) => p.failure(err)
              case Success(Left(blklst)) => p.success(Left(blklst + next.pointer.id))
              case Success(Right(rpath)) => p.success(Right(rpath))
            }
          }
        }
      }

      val initialCandidates = currentNode.contents.iterator.
        filter(t => ordering.compare(t._1, target) <= 0).
        map(t => t._1 -> KeyValueObjectPointer(t._2.value.bytes)).
        filter(t => !initialBlacklist.contains(t._2.id)).
        toList.
        sortWith((l, r) => ordering.compare(l._1, r._1) < 0).
        reverse

      rtry(initialCandidates, initialBlacklist)

      p.future
    }

  }

  private def fetchNode(client: AspenClient,
                        ordering: KeyOrdering,
                        targetTier: Int,
                        minimum: Key,
                        pointer: KeyValueObjectPointer,
                        blacklist: Set[ObjectId]): Future[Either[Set[ObjectId], KeyValueListNode]] = {

    given ExecutionContext = client.clientContext

    if (blacklist.contains(pointer.id)) {
      Future.successful(Left(blacklist))
    } else {
      val p = Promise[Either[Set[ObjectId], KeyValueListNode]]()

      client.read(pointer, s"TKVL read of tier $targetTier, key: $minimum") onComplete {
        case Failure(_) => p.success(Left(blacklist + pointer.id))

        case Success(kvos) =>

          val tail = kvos.right.map(right => KeyValueListPointer(right.bytes))

          val node = new KeyValueListNode(client, kvos.ida, pointer, ordering, minimum,
            kvos.revision, kvos.refcount, kvos.contents, tail)
          p.success(Right(node))
      }

      p.future
    }
  }

}
