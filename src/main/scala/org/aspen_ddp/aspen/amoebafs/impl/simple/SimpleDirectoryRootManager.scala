package org.aspen_ddp.aspen.amoebafs.impl.simple

import scala.language.implicitConversions
import java.nio.{ByteBuffer, ByteOrder}
import java.util.UUID
import org.aspen_ddp.aspen.client.tkvl.*
import org.aspen_ddp.aspen.client.{AspenClient, DataObjectState, ObjectAllocator, RegisteredTypeFactory, Transaction}
import org.aspen_ddp.aspen.common.objects.*
import org.aspen_ddp.aspen.amoebafs.DirectoryInode

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success}

class SimpleDirectoryRootManager(client: AspenClient,
                                 inodePointer: DataObjectPointer,
                                 val cache: TierCache = TierCache.default()) extends RootManager {

  import SimpleDirectoryRootManager._

  given ExecutionContext = client.clientContext

  override def typeId: RootManagerTypeId = RootManagerTypeId(typeUUID)

  def getRoot(): Future[Root] = getRData().map(_.root)
  
  private def getRData(fresh: Boolean = false, oInodeDos: Option[DataObjectState] = None): Future[RootState] =
    RootManager.cachedRead(cache, fresh)(readRData(oInodeDos))

  private def readRData(oInodeDos: Option[DataObjectState]): Future[RootState] = {
    val p = Promise[RootState]()

    val fInodeDos = oInodeDos match
      case None => client.read(inodePointer)
      case Some(idos) => Future.successful(idos)

    fInodeDos.onComplete {
      case Failure(err) => p.failure(err)
      case Success(inodeDos) =>
        // The decode must stay inside the try. Outside it, a malformed inode throws into the
        // executor and leaves p uncompleted, hanging the caller's Future forever.
        try {
          val inode = DirectoryInode(client, inodeDos.data)
          val root = inode.contents

          root.rootKeyValuePointer match {
            case None => p.success(RootState(root, inodeDos.revision, None))
            case Some(rootObject) =>
              client.read(rootObject).onComplete {
                case Failure(err) =>
                  // Tree has been deleted but the root has not yet been updated
                  // Return an empty tree root
                  p.success(RootState(root.copy(tier=0, orootObject=None), inodeDos.revision, None))
                  
                case Success(rootKvos) =>
                  val rootLp = KeyValueListPointer(Key.AbsoluteMinimum, rootObject)
                  val node = KeyValueListNode(client, rootLp, root.ordering, rootKvos)

                  p.success(RootState(root, inodeDos.revision, Some(node)))
              }
          }
        } catch {
          case cause: Throwable => p.failure(new InvalidRoot(cause))
        }

    }

    p.future
  }

  def getTree(): Future[TieredKeyValueList] = getRData().map { rd =>
    new TieredKeyValueList(client, this)
  }

  def getAllocatorForTier(tier: Int): Future[ObjectAllocator] = getRData().flatMap { rd =>
    rd.root.nodeAllocator.getAllocatorForTier(tier)
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

  override def encode(): Array[Byte] = {
    val arr = new Array[Byte]( inodePointer.encodedSize )
    val bb = ByteBuffer.wrap(arr)
    bb.order(ByteOrder.BIG_ENDIAN)
    inodePointer.encodeInto(bb)
    arr
  }

  override def prepareRootUpdate(newTier: Int, 
                                 onewRoot: Option[ObjectPointer])(using tx: Transaction): Future[Unit] = {
    val p = Promise[Unit]()

    client.read(inodePointer).onComplete {
      case Failure(err) => p.failure(err)
      case Success(inodeDos) =>
        val inode = DirectoryInode(client, inodeDos.data)

        val root = inode.contents

        val nextRoot = root.copy(tier = newTier, orootObject = onewRoot)
        val newInode = inode.setContentTree(nextRoot)

        tx.overwrite(inodePointer, inodeDos.revision, newInode.toArray)
        p.success(())
    }

    p.future
  }

  override def createInitialNode(contents: Map[Key,Value])(using tx: Transaction): Future[Unit] = {
    for {
      dos <- client.read(inodePointer)
      RootState(root, _, onode) <- getRData(fresh = true, Some(dos))

      // Check for a race condition where multiple concurrent attempts to
      // create the initial node might clash with each other. Done before the
      // allocation below so a loser stages nothing.
      _ = if onode.isDefined then
            tx.abortAndThrow(new Exception("Initial TKVL node already exists."))

      alloc <- root.nodeAllocator.getAllocatorForTier(0)
      rptr <- alloc.allocateKeyValueObject(contents)
    } yield {
      val dinode = DirectoryInode(client, dos.data)
      val newRoot = Root(0, root.ordering, Some(rptr), root.nodeAllocator)
      val newInode = dinode.setContentTree(newRoot)

      RootManager.updateInitialRoot(inodePointer, tx):
        tx.overwrite(inodePointer, dos.revision, newInode.toArray)
    }
  }
}

object SimpleDirectoryRootManager extends RootManagerFactory {
  val typeUUID: UUID = UUID.fromString("52887CBE-0D2B-43C8-80FA-999DA177392D")

  def apply(client: AspenClient, bb: ByteBuffer): SimpleDirectoryRootManager = {
    new SimpleDirectoryRootManager(client, DataObjectPointer(bb), NoCache)
  }

  override def createRootManager(client: AspenClient, data: Array[Byte]): SimpleDirectoryRootManager = {
    val bb = ByteBuffer.wrap(data)
    bb.order(ByteOrder.BIG_ENDIAN)
    SimpleDirectoryRootManager(client, bb)
  }
}
