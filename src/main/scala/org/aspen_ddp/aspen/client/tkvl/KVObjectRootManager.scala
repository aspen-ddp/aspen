package org.aspen_ddp.aspen.client.tkvl

import java.nio.{ByteBuffer, ByteOrder}
import java.util.UUID

import org.aspen_ddp.aspen.client.{AspenClient, ObjectAllocator, ObjectReader, RegisteredTypeFactory, Transaction}
import org.aspen_ddp.aspen.common.objects.{Insert, Key, KeyOrdering, KeyValueObjectPointer, ObjectPointer, ObjectRevision, Value}
import org.aspen_ddp.aspen.common.transaction.KeyValueUpdate

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success}

/** `readWith` reads the root's host object and the root node (and so, through the nodes, every read of a walk from
  * the root); the client when None. A reader with settled reads (AspenClient.settledReader) makes the tree's reads
  * settled. Writes always read with the client (`forWrites`). Not encoded: a decoded root manager reads with its
  * client
  */
class KVObjectRootManager(val client: AspenClient,
                          val treeKey: Key,
                          val pointer: KeyValueObjectPointer,
                          val cache: TierCache = TierCache.default(),
                          readWith: Option[ObjectReader] = None) extends RootManager {

  import KVObjectRootManager._

  private val reader: ObjectReader = readWith.getOrElse(client)

  given ExecutionContext = client.clientContext

  def typeId: RootManagerTypeId = RootManagerTypeId(typeUUID)

  def getRoot(): Future[Root] = getRData().map(_.root)

  override lazy val forWrites: RootManager = readWith match
    case None => this
    case Some(_) => new KVObjectRootManager(client, treeKey, pointer, TierCache.invalidateOnly(cache))
  
  private def getRData(fresh: Boolean = false): Future[RootState] =
    RootManager.cachedRead(cache, fresh)(readRData())

  private def readRData(): Future[RootState] = {
    val p = Promise[RootState]()

    reader.read(pointer, s"Object hosting TKVL Root for tree $treeKey").onComplete {
      case Failure(err) => p.failure(err)
      case Success(container) =>

        container.contents.get(treeKey) match {
          case None => p.failure(new InvalidRoot)
          case Some(v) => try {
            val root = Root(client, v.value.bytes)

            root.orootObject match {
              case None => p.success(RootState(root, v.revision, None))
              case Some(rootObject) =>
                NodeEncoding(root.kind).read(reader, rootObject, Key.AbsoluteMinimum, root.ordering,
                  s"Root node for TKVL tree $treeKey").onComplete {
                  case Failure(err) =>
                    // Tree has been deleted but the root has not yet been updated
                    // Return an empty tree root
                    p.success(RootState(root.copy(tier=0, orootObject=None), v.revision, None, cacheable = false))
                    
                  case Success(node) =>
                    p.success(RootState(root, v.revision, Some(node)))
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

  def readStoredRoot(): Future[Root] =
    reader.read(pointer, s"Object hosting TKVL Root for tree $treeKey").map: container =>
      container.contents.get(treeKey) match
        case None => throw new InvalidRoot
        case Some(v) =>
          try
            Root(client, v.value.bytes)
          catch
            case cause: Throwable => throw new InvalidRoot(cause)

  def getTree(): Future[TieredKeyValueList] = getRData().map { rd =>
    new TieredKeyValueList(client, this)
  }

  /** The tree as a DataObjectTKVL. Fails with `WrongTreeKind` if it is not a DataObjects tree. */
  def getDataObjectTree(): Future[DataObjectTKVL] = DataObjectTKVL.open(client, this)

  def getAllocatorForTier(tier: Int): Future[ObjectAllocator] = getRData().flatMap { rd =>
    rd.root.nodeAllocator.getAllocatorForTier(tier, rd.root.kind)
  }

  def getRootNode(): Future[(Int, KeyOrdering, Option[ListNode])] = getRData().map { rd =>
    (rd.root.tier, rd.root.ordering, rd.node)
  }

  def getFreshRootNode(): Future[(Int, KeyOrdering, Option[ListNode])] = getRData(fresh = true).map { rd =>
    (rd.root.tier, rd.root.ordering, rd.node)
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
    getRData(fresh = true).map { rd =>
      val data = rd.root.copy(tier=newTier, orootObject=onewRoot).encode()
      val reqs = KeyValueUpdate.KeyRevision(treeKey, rd.revision) :: Nil
      val ops = Insert(treeKey, data) :: Nil
      
      tx.update(pointer, None, None, reqs, ops)
    }
  }

  def createInitialNode(contents: Map[Key,Value])(using tx: Transaction): Future[Unit] = {
    def onSplit(newMinimum: Key, newNode: ObjectPointer): Future[Unit] = {
      SplitFinalizationAction.addToTransaction(this, 1, newMinimum, newNode, tx)
      Future.unit
    }

    for {
      RootState(root, rootRevision, onode, _) <- getRData(fresh = true)

      // Check for a race condition where multiple concurrent attempts to
      // create the initial node might clash with each other. Done before the
      // allocation below so a loser stages nothing.
      _ = if onode.isDefined then
            tx.abortAndThrow(new Exception("Initial TKVL node already exists."))

      alloc <- root.nodeAllocator.getAllocatorForTier(0, root.kind)
      rptr <- NodeEncoding(root.kind).allocateChain(alloc, root.ordering, contents,
        root.nodeAllocator.getMaxNodeSize(0), onSplit)
    } yield {
      val newRoot = Root(0, root.ordering, Some(rptr), root.nodeAllocator, root.kind)
      // Pin the revision at which the tree was observed to be empty. Callers
      // such as TieredKeyValueList.set rely on this to make key requirements
      // unnecessary for the initial node's contents.
      val kreqs = KeyValueUpdate.KeyRevision(treeKey, rootRevision) :: Nil
      RootManager.updateInitialRoot(pointer, tx):
        tx.update(pointer, None, None, kreqs, Insert(treeKey, newRoot.encode()) :: Nil)
      tx.result.map { _ =>
        new KVObjectRootManager(client, treeKey, pointer)
      }
    }
  }
}

object KVObjectRootManager extends RootManagerFactory {

  val typeUUID: UUID = UUID.fromString("CE36789D-42F1-43F9-9464-E9B44419D8C4")

  /** Stores `root` under `rootKey` of `hostingObject`, which must not hold the key (`DoesNotExist`)
   * @return future to a new KVObjectManager. Future completes after the
   *         transaction commits
   */
  def setNewRoot(client: AspenClient,
                 hostingObject: KeyValueObjectPointer,
                 rootKey: Key,
                 root: Root)(using tx:Transaction): Future[KVObjectRootManager] =

    given ExecutionContext = client.clientContext

    tx.update(hostingObject, None, None, KeyValueUpdate.DoesNotExist(rootKey) :: Nil,
      Insert(rootKey, root.encode()) :: Nil)
    tx.result.map:  _ =>
      new KVObjectRootManager(client, rootKey, hostingObject)


  def createRootManager(client: AspenClient, data: Array[Byte]): KVObjectRootManager = {
    val bb = ByteBuffer.wrap(data)
    bb.order(ByteOrder.BIG_ENDIAN)
    val klen = bb.getInt()
    val karr = new Array[Byte](klen)
    bb.get(karr)
    val ptr = KeyValueObjectPointer(bb)
    new KVObjectRootManager(client, Key(karr), ptr, NoCache)
  }

  /** Outter future is for ready to commit transaction. Inner future is to the root manager after successful
    * transaction completion.
    *
    * Note that the key must not already exist within the object. The root is hosted in `pointer` under `key` for
    * either `kind`; the nodes, including the initial one holding `initialContent`, are objects of that kind.
    */
  def createNewTree(client: AspenClient,
                    pointer: KeyValueObjectPointer,
                    key: Key,
                    ordering: KeyOrdering,
                    nodeAllocator: NodeAllocator,
                    initialContent: Map[Key,Value],
                    kind: TreeKind = TreeKind.KeyValueObjects)(using tx: Transaction): Future[Future[KVObjectRootManager]] =

    given ExecutionContext = client.clientContext

    // The root entry must always be written: it is what makes the tree exist, and its DoesNotExist
    // requirement is what guards against clobbering an existing tree. An empty tree needs no root node
    // object, so Root.orootObject is None until the first insert.
    Root.create(client, ordering, nodeAllocator, initialContent, kind).map: root =>
      setNewRoot(client, pointer, key, root)
}