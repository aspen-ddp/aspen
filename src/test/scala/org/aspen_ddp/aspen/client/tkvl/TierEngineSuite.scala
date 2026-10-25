package org.aspen_ddp.aspen.client.tkvl

import java.util.UUID

import org.aspen_ddp.aspen.IntegrationTestSuite
import org.aspen_ddp.aspen.client.{ObjectAllocator, Transaction}
import org.aspen_ddp.aspen.common.Radicle
import org.aspen_ddp.aspen.common.objects.{ByteArrayKeyOrdering, DataObjectPointer, Key, KeyOrdering, KeyValueObjectPointer, ObjectId, ObjectPointer, ObjectRefcount, ObjectRevision, Value}
import org.aspen_ddp.aspen.common.pool.PoolId

import scala.concurrent.Future

class TierEngineSuite extends IntegrationTestSuite:

  private val poolId = PoolId(new UUID(0, 5))
  private val theKey = Key(Array[Byte](1))
  private val theValue = Value(Array[Byte](2))

  /** A tier-0 node with no right link, so a descent to it reads nothing */
  private class StubNode(val pointer: ObjectPointer) extends ListNode:
    def minimum: Key = Key.AbsoluteMinimum
    def maximum: Option[Key] = None
    def link: Option[NodeLink] = None
    def ordering: KeyOrdering = ByteArrayKeyOrdering
    def revision: ObjectRevision = ObjectRevision.Null
    def refcount: ObjectRefcount = ObjectRefcount(0, 1)
    def entries: List[(Key, Value)] = List(theKey -> theValue)
    def get(k: Key): Option[Value] = entries.toMap.get(k)
    def size: Int = entries.size
    def keyInRange(k: Key): Boolean = true

  /** Serves `node` as a tier-0 root. Nothing else is used by the reads below. */
  private class StubRootManager(node: ListNode) extends RootManager:
    def getRoot(): Future[Root] = Future.failed(new NotImplementedError)
    def getRootNode(): Future[(Int, KeyOrdering, Option[ListNode])] =
      Future.successful((0, node.ordering, Some(node)))
    def createInitialNode(contents: Map[Key, Value])(using tx: Transaction): Future[Unit] =
      Future.failed(new NotImplementedError)
    def getAllocatorForTier(tier: Int): Future[ObjectAllocator] = Future.failed(new NotImplementedError)
    def getMaxNodeSize(tier: Int): Future[Int] = Future.failed(new NotImplementedError)
    def prepareRootUpdate(newTier: Int, onewRoot: Option[ObjectPointer])(using tx: Transaction): Future[Unit] =
      Future.failed(new NotImplementedError)
    def encode(): Array[Byte] = Array()
    def typeId: RootManagerTypeId = RootManagerTypeId(new UUID(0, 0))

  test("TreeKind.of maps each pointer type to its tree kind"):
    assert(TreeKind.of(KeyValueObjectPointer(ObjectId(new UUID(1, 2)), poolId)) == TreeKind.KeyValueObjects)
    assert(TreeKind.of(DataObjectPointer(ObjectId(new UUID(1, 2)), poolId)) == TreeKind.DataObjects)

  atest("TierEngine uses the encoding of a KeyValueObjects root"):
    val stub = new StubNode(KeyValueObjectPointer(ObjectId(new UUID(1, 2)), poolId))
    new TierEngine(client, new StubRootManager(stub)).get(theKey).map: v =>
      assert(v.map(_.bytes.toList) == Some(theValue.bytes.toList))

  atest("TierEngine picks the encoding from a DataObjects root's kind"):
    // NodeEncoding(DataObjects) is not implemented yet, so choosing it is observable as this failure
    val stub = new StubNode(DataObjectPointer(ObjectId(new UUID(1, 2)), poolId))
    recoverToSucceededIf[UnsupportedOperationException]:
      new TierEngine(client, new StubRootManager(stub)).get(theKey)

  atest("TierEngine.get descends a multi-tier KeyValueObjects tree"):
    val treeKey = Key(Array[Byte](0))
    def insert(tree: TieredKeyValueList, i: Int): Future[Unit] =
      val tx = client.newTransaction()
      tree.set(Key(i), Value(new Array[Byte](100)))(using tx).flatMap(_ => tx.commit().map(_ => ()))

    for
      ikvos <- client.read(radicle)
      pool <- client.getStoragePool(Radicle.poolId)
      tx0 = client.newTransaction()
      ptr <- pool.allocator.allocateKeyValueObject()(using tx0)
      _ = tx0.lockRevision(radicle, ikvos.revision)
      _ <- tx0.commit()
      _ <- waitForTransactionsToComplete()

      nodeAllocator = new SinglePoolNodeAllocator(client, Radicle.poolId, 500)
      tx1 = client.newTransaction()
      froot <- KVObjectRootManager.createNewTree(client, ptr, treeKey, ByteArrayKeyOrdering, nodeAllocator,
        Map())(using tx1)
      _ <- tx1.commit()
      root <- froot
      tree <- root.getTree()
      _ <- (1 to 30).foldLeft(Future.unit)((f, i) => f.flatMap(_ => insert(tree, i)))
      _ <- waitForTransactionsToComplete()

      (numTiers, _, _) <- root.getRootNode()
      engine = new TierEngine(client, root)
      found <- Future.sequence((1 to 30).map(i => engine.get(Key(i))))
      missing <- engine.get(Key(99))
    yield
      assert(numTiers > 0)
      assert(found.forall(_.isDefined))
      assert(missing.isEmpty)
