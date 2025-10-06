package org.aspen_ddp.aspen.client.tkvl

import org.aspen_ddp.aspen.client.{AspenClient, Transaction}
import org.aspen_ddp.aspen.client.KeyValueObjectState.ValueState
import org.aspen_ddp.aspen.common.objects.{Key, KeyOrdering, KeyValueObjectPointer, ObjectId, ObjectRevision, ObjectRevisionGuard, Value}
import org.aspen_ddp.aspen.common.transaction.KeyValueUpdate
import org.apache.logging.log4j.scala.Logging

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

    def nonEmpty(tier: Int,
                 ordering: KeyOrdering,
                 root: KeyValueListNode): Future[KeyValueObjectPointer] =
      for
        e <- fetchContainingNodePath(client, tier, 0, ordering, splitAtKey, root, Set())

        rpath = e match
          case Left(_) => throw new BrokenTree()
          case Right(p) => p

        // There will always be a tier0 entry, split that then recurse through
        // the full path
        alloc <- rootManager.getAllocatorForTier(0)

        down <- rpath.head.splitAt(ordering, splitAtKey, inclusive, None, alloc)

        kvp <- rinsert(0, ordering, rpath, down.pointer)
      yield
        kvp

    for
      oldRoot <- rootManager.getRoot()
      (tier, ordering, oroot) <- rootManager.getRootNode()
      ptr <- oroot match
        case Some(root) => nonEmpty(tier, ordering, root)
        case None =>
          for
            allocGuard <- rootManager.createInitialNode(Map())
            alloc <- rootManager.getAllocatorForTier(0)
            ptr <- alloc.allocateKeyValueObject(allocGuard, Map())
          yield
            ptr
    yield
      Root(tier, ordering, Some(ptr), oldRoot.nodeAllocator)
  }

  def set(key: Key,
          value: Value,
          requirement: Option[Either[Boolean, ObjectRevision]] = None)
         (using t: Transaction): Future[Unit] = {

    def onSplit(newMinimum: Key, newNode: KeyValueObjectPointer): Future[Unit] = {
      SplitFinalizationAction.addToTransaction(rootManager, 1, newMinimum, newNode, t)
      Future.successful(())
    }
    def empty(tier: Int, ordering: KeyOrdering): Future[Unit] = {
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
      Future.successful(())
    }
    def nonEmpty(tier: Int, ordering: KeyOrdering, root: KeyValueListNode): Future[Unit] = {
      for {
        e <- fetchContainingNode(client, tier, 0, ordering, key, root, Set())
        node = e match {
          case Left(_) => throw new BrokenTree()
          case Right(n) => n
        }
        _ <- node.delete(key, onJoin)
      } yield {
        ()
      }
    }

    rootManager.getRootNode().flatMap { t =>
      val (tier, ordering, oroot) = t
      oroot match {
        case None => Future.successful(())
        case Some(root) => nonEmpty(tier, ordering, root)
      }
    }
  }

  def delete(key: Key, 
             requiredRevision: Option[ObjectRevision],
             requirements: List[KeyValueUpdate.KeyRequirement])(using t: Transaction): Future[Unit] = {
    def onJoin(delMinimum: Key, delNode: KeyValueObjectPointer): Future[Unit] = {
      JoinFinalizationAction.addToTransaction(rootManager, 1, delMinimum, delNode, t)
      Future.successful(())
    }

    def nonEmpty(tier: Int, ordering: KeyOrdering, root: KeyValueListNode): Future[Unit] = {
      for {
        e <- fetchContainingNode(client, tier, 0, ordering, key, root, Set())
        node = e match {
          case Left(_) => throw new BrokenTree()
          case Right(n) => n
        }
        _ <- node.delete(key, requiredRevision, requirements, onJoin)
      } yield {
        ()
      }
    }

    rootManager.getRootNode().flatMap { t =>
      val (tier, ordering, oroot) = t
      oroot match {
        case None => Future.successful(())
        case Some(root) => nonEmpty(tier, ordering, root)
      }
    }
  }

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

  def foreach(fn: (KeyValueListNode, Key, ValueState) => Future[Unit]): Future[Unit] =

    def nonEmpty(tier: Int, ordering: KeyOrdering, root: KeyValueListNode): Future[Unit] =
      for
        e <- fetchContainingNode(client, tier, 0, ordering, Key.AbsoluteMinimum, root, Set())
        node = e match
          case Left(_) => throw new BrokenTree()
          case Right(n) => n
        _ <- node.foreach(fn)
      yield
        ()

    rootManager.getRootNode().flatMap: t =>
      val (tier, ordering, oroot) = t
      oroot match
        case None => Future.successful(())
        case Some(root) => nonEmpty(tier, ordering, root)

  def foreachInRange(minKey: Key,
                     maxKey: Key,
                     fn: (KeyValueListNode, Key, ValueState) => Future[Unit]): Future[Unit] =

    def nonEmpty(tier: Int, ordering: KeyOrdering, root: KeyValueListNode): Future[Unit] =
      for
        e <- fetchContainingNode(client, tier, 0, ordering, minKey, root, Set())
        node = e match
          case Left(_) => throw new BrokenTree()
          case Right(n) => n
        _ <- node.foreachInRange(minKey, maxKey, fn)
      yield
        ()

    rootManager.getRootNode().flatMap: t =>
      val (tier, ordering, oroot) = t
      oroot match
        case None => Future.successful(())
        case Some(root) => nonEmpty(tier, ordering, root)
}

object TieredKeyValueList {

  /*
   Returns Left if the tree is broken or Right with a list of hosts for each
   tier in the path down to the requested host in reverse order. The top of
   the tree will be the last host in the list
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

  private[tkvl] def fetchContainingNodePath(client: AspenClient,
                                            currentTier: Int,
                                            targetTier: Int,
                                            ordering: KeyOrdering,
                                            target: Key,
                                            currentNode: KeyValueListNode,
                                            initialBlacklist: Set[ObjectId],
                                            reversePath: List[KeyValueListNode] = Nil): Future[Either[Set[ObjectId], List[KeyValueListNode]]] = {

    given ExecutionContext = client.clientContext

    if (currentTier == targetTier) {
      // Once we're on the right tier, we can rely on consistent right pointers to scan to the
      // containing host
      currentNode.fetchContainingNode(target).map(n => Right(n :: reversePath))
    } else {

      val p = Promise[Either[Set[ObjectId], List[KeyValueListNode]]]()

      def rtry(candidates: List[(Key, KeyValueObjectPointer)], blacklist: Set[ObjectId]): Unit = {
        if (candidates.isEmpty) {
          p.success(Left(blacklist + currentNode.pointer.id))
        } else {
          fetchNode(client, ordering, targetTier, candidates.head._1, candidates.head._2, blacklist) foreach {
            case Left(blklst) => rtry(candidates.tail, blklst)
            case Right(next) => fetchContainingNodePath(client, currentTier-1, targetTier, ordering, target,
              next, blacklist, reversePath).foreach {
              case Left(blklst) => p.success(Left(blklst + next.pointer.id))
              case Right(rpath) => p.success(Right(rpath))
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

          val node = new KeyValueListNode(client, pointer, ordering, minimum,
            kvos.revision, kvos.refcount, kvos.contents, tail)
          p.success(Right(node))
      }

      p.future
    }
  }

}
