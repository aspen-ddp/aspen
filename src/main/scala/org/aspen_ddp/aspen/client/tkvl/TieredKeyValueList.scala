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

  implicit val ec: ExecutionContext = client.clientContext

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

  def set(key: Key,
          value: Value,
          requirement: Option[Either[Boolean, ObjectRevision]] = None)
         (implicit t: Transaction): Future[Unit] = {
    logger.trace("Beginning TKVL Set Operation")
    def onSplit(newMinimum: Key, newNode: KeyValueObjectPointer): Future[Unit] = {
      SplitFinalizationAction.addToTransaction(rootManager, 1, newMinimum, newNode, t)
      Future.successful(())
    }
    def empty(tier: Int, ordering: KeyOrdering): Future[Unit] = {
      logger.trace("Creating Initial TKVL Node")
      rootManager.createInitialNode(Map(key -> value)).map(_=>())
    }
    def nonEmpty(tier: Int, ordering: KeyOrdering, root: KeyValueListNode): Future[Unit] = {
      logger.trace("Non Empty TKVL Tree")
      for {
        alloc <- rootManager.getAllocatorForTier(0)
        _=logger.trace("Got allocator for tree")
        maxNodeSize <- rootManager.getMaxNodeSize(0)
        _=logger.trace("Got max node size")
        e <- fetchContainingNode(client, tier, 0, ordering, key, root, Set())
        _=logger.trace(s"Got containing node $e")
        node = e match {
          case Left(_) => throw new BrokenTree()
          case Right(n) => n
        }
        _ <- node.insert(key, value, maxNodeSize, alloc, onSplit, requirement)
      } yield {
        ()
      }
    }

    rootManager.getRootNode().flatMap { t =>
      val (tier, ordering, oroot) = t
      oroot match {
        case None => empty(tier, ordering)
        case Some(root) => nonEmpty(tier, ordering, root)
      }
    }
  }

  def delete(key: Key)(implicit t: Transaction): Future[Unit] = {
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
             requirements: List[KeyValueUpdate.KeyRequirement])(implicit t: Transaction): Future[Unit] = {
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

  private[tkvl] def fetchContainingNode(client: AspenClient,
                                        currentTier: Int,
                                        targetTier: Int,
                                        ordering: KeyOrdering,
                                        target: Key,
                                        currentNode: KeyValueListNode,
                                        initialBlacklist: Set[ObjectId]): Future[Either[Set[ObjectId], KeyValueListNode]] = {

    implicit val ec: ExecutionContext = client.clientContext

    if (currentTier == targetTier) {
      // Once we're on the right tier, we can rely on consistent right pointers to scan to the
      // containing node
      currentNode.fetchContainingNode(target).map(n => Right(n))
    } else {

      val p = Promise[Either[Set[ObjectId], KeyValueListNode]]()

      def rtry(candidates: List[(Key, KeyValueObjectPointer)], blacklist: Set[ObjectId]): Unit = {
        if (candidates.isEmpty) {
          p.success(Left(blacklist + currentNode.pointer.id))
        } else {
          fetchNode(client, ordering, targetTier, candidates.head._1, candidates.head._2, blacklist) foreach {
            case Left(blklst) => rtry(candidates.tail, blklst)
            case Right(next) => fetchContainingNode(client, currentTier-1, targetTier, ordering, target,
              next, blacklist).foreach {
              case Left(blklst) => p.success(Left(blklst + next.pointer.id))
              case Right(targetNode) => p.success(Right(targetNode))
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

    implicit val ec: ExecutionContext = client.clientContext

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
