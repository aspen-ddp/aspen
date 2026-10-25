package org.aspen_ddp.aspen.client.tkvl

import java.nio.{ByteBuffer, ByteOrder}
import java.util.UUID

import org.aspen_ddp.aspen.client.{AspenClient, ObjectAllocator, RegisteredTypeFactory, Transaction}
import org.aspen_ddp.aspen.common.objects.{Insert, Key, KeyOrdering, KeyValueObjectPointer, ObjectPointer, ObjectRevision, Value}
import org.aspen_ddp.aspen.common.transaction.KeyValueUpdate

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success}

class KVObjectRootManager(val client: AspenClient,
                          val treeKey: Key,
                          val pointer: KeyValueObjectPointer) extends RootManager {

  import KVObjectRootManager._

  given ExecutionContext = client.clientContext

  def typeId: RootManagerTypeId = RootManagerTypeId(typeUUID)

  def getRoot(): Future[Root] = getRData().map(_.root)
  
  /** Returns (numTiers, keyOrdering, rootNode) */
  private def getRData(): Future[RData] = {
    val p = Promise[RData]()

    client.read(pointer, s"Object hosting TKVL Root for tree $treeKey").onComplete {
      case Failure(err) => p.failure(err)
      case Success(container) =>

        container.contents.get(treeKey) match {
          case None => p.failure(new InvalidRoot)
          case Some(v) => try {
            val root = Root(client, v.value.bytes)

            root.orootObject match {
              case None => p.success(RData(root, v.revision, None))
              case Some(rootObject) =>
                NodeEncoding(root.kind).read(client, rootObject, Key.AbsoluteMinimum, root.ordering,
                  s"Root node for TKVL tree $treeKey").onComplete {
                  case Failure(err) =>
                    // Tree has been deleted but the root has not yet been updated
                    // Return an empty tree root
                    p.success(RData(root.copy(tier=0, orootObject=None), v.revision, None))
                    
                  case Success(node) =>
                    p.success(RData(root, v.revision, Some(node)))
                }
            }
          } catch {
            case err: Throwable =>
              p.failure(new InvalidRoot)
          }
        }
    }

    p.future
  }

  def getTree(): Future[TieredKeyValueList] = getRData().map { rd =>
    new TieredKeyValueList(client, this)
  }

  def getAllocatorForTier(tier: Int): Future[ObjectAllocator] = getRData().flatMap { rd =>
    rd.root.nodeAllocator.getAllocatorForTier(tier, rd.root.kind)
  }

  def getRootNode(): Future[(Int, KeyOrdering, Option[ListNode])] = getRData().map { rd =>
    (rd.root.tier, rd.root.ordering, rd.onode)
  }

  def getMaxNodeSize(tier: Int): Future[Int] = getRData().map { rd =>
    rd.root.nodeAllocator.getMaxNodeSize(tier)
  }

  def encode(): Array[Byte] = {
    val arr = new Array[Byte]( 4 + treeKey.bytes.length + pointer.encodedSize )
    val bb = ByteBuffer.wrap(arr)
    bb.order(ByteOrder.BIG_ENDIAN)
    bb.putInt(treeKey.bytes.length)
    bb.put(treeKey.bytes)
    pointer.encodeInto(bb)
    arr
  }

  def prepareRootUpdate(newTier: Int, onewRoot: Option[ObjectPointer])(using tx: Transaction): Future[Unit] = {
    getRData().map { rd =>
      val data = rd.root.copy(tier=newTier, orootObject=onewRoot).encode()
      val reqs = KeyValueUpdate.KeyRevision(treeKey, rd.rootRevision) :: Nil
      val ops = Insert(treeKey, data) :: Nil
      
      tx.update(pointer, None, None, reqs, ops)
    }
  }

  def createInitialNode(contents: Map[Key,Value])(using tx: Transaction): Future[Unit] = {
    for {
      RData(root, rootRevision, onode) <- getRData()

      // Check for a race condition where multiple concurrent attempts to
      // create the initial node might clash with each other. Done before the
      // allocation below so a loser stages nothing.
      _ = if onode.isDefined then
            tx.abortAndThrow(new Exception("Initial TKVL node already exists."))

      alloc <- root.nodeAllocator.getAllocatorForTier(0, root.kind)
      rptr <- NodeEncoding(root.kind).allocate(alloc, contents)
    } yield {
      val newRoot = Root(0, root.ordering, Some(rptr), root.nodeAllocator, root.kind)
      // Pin the revision at which the tree was observed to be empty. Callers
      // such as TieredKeyValueList.set rely on this to make key requirements
      // unnecessary for the initial node's contents.
      val kreqs = KeyValueUpdate.KeyRevision(treeKey, rootRevision) :: Nil
      tx.update(pointer, None, None, kreqs, Insert(treeKey, newRoot.encode()) :: Nil)
      tx.result.map { _ =>
        new KVObjectRootManager(client, treeKey, pointer)
      }
    }
  }
}

object KVObjectRootManager extends RootManagerFactory {

  private case class RData(root: Root, rootRevision: ObjectRevision, onode: Option[ListNode])

  val typeUUID: UUID = UUID.fromString("CE36789D-42F1-43F9-9464-E9B44419D8C4")

  /**
   * @return future to a new KVObjectManager. Future completes after the
   *         transaction commits
   */
  def setNewRoot(client: AspenClient,
                 hostingObject: KeyValueObjectPointer,
                 rootKey: Key,
                 requireKeyNotExist: Boolean,
                 root: Root)(using tx:Transaction): Future[KVObjectRootManager] =

    given ExecutionContext = client.clientContext

    val kreqs = if requireKeyNotExist then
      KeyValueUpdate.DoesNotExist(rootKey) :: Nil
    else
      Nil
    tx.update(hostingObject, None, None, kreqs, Insert(rootKey, root.encode()) :: Nil)
    tx.result.map:  _ =>
      new KVObjectRootManager(client, rootKey, hostingObject)


  def createRootManager(client: AspenClient, data: Array[Byte]): KVObjectRootManager = {
    val bb = ByteBuffer.wrap(data)
    bb.order(ByteOrder.BIG_ENDIAN)
    val klen = bb.getInt()
    val karr = new Array[Byte](klen)
    bb.get(karr)
    val ptr = KeyValueObjectPointer(bb)
    new KVObjectRootManager(client, Key(karr), ptr)
  }

  /** Outter future is for ready to commit transaction. Inner future is to the root manager after successful
    * transaction completion.
    *
    * Note that the key must not already exist within the object
    */
  def createNewTree(client: AspenClient,
                    pointer: KeyValueObjectPointer,
                    key: Key,
                    ordering: KeyOrdering,
                    nodeAllocator: NodeAllocator,
                    initialContent: Map[Key,Value])(using tx: Transaction): Future[Future[KVObjectRootManager]] =

    given ExecutionContext = client.clientContext

    // The root entry must always be written: it is what makes the tree exist, and its DoesNotExist
    // requirement is what guards against clobbering an existing tree. An empty tree needs no root node
    // object, so Root.orootObject is None until the first insert.
    if initialContent.isEmpty then
      nodeAllocator.validate().map: _ =>
        setNewRoot(client, pointer, key, true, Root(0, ordering, None, nodeAllocator))
    else
      for
        _ <- nodeAllocator.validate()
        alloc <- nodeAllocator.getAllocatorForTier(0)
        rptr <- alloc.allocateKeyValueObject(initialContent)
      yield
        setNewRoot(client, pointer, key, true, Root(0, ordering, Some(rptr), nodeAllocator))
}