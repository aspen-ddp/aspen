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
                                 inodePointer: DataObjectPointer) extends RootManager {

  import SimpleDirectoryRootManager._

  given ExecutionContext = client.clientContext

  override def typeId: RootManagerTypeId = RootManagerTypeId(typeUUID)

  def getRoot(): Future[Root] = getRData().map(_.root)
  
  private def getRData(oInodeDos: Option[DataObjectState] = None): Future[RData] = {
    val p = Promise[RData]()

    val fInodeDos = oInodeDos match
      case None => client.read(inodePointer)
      case Some(idos) => Future.successful(idos)

    fInodeDos.onComplete {
      case Failure(err) => p.failure(err)
      case Success(inodeDos) =>
        val inode = DirectoryInode(client, inodeDos.data)
        val root = inode.contents
        try {
          root.orootObject match {
            case None => p.success(RData(root, inodeDos.revision, None))
            case Some(rootObject) =>
              client.read(rootObject).onComplete {
                case Failure(err) => p.failure(err)
                case Success(rootKvos) =>

                  val rootLp = KeyValueListPointer(Key.AbsoluteMinimum, rootObject)
                  val node = KeyValueListNode(client, rootLp, root.ordering, rootKvos)

                  p.success(RData(root, inodeDos.revision, Some(node)))
              }
          }
        } catch {
          case _: Throwable => p.failure(new InvalidRoot)
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

  def getRootNode(): Future[(Int, KeyOrdering, Option[KeyValueListNode])] = getRData().map { rd =>
    (rd.root.tier, rd.root.ordering, rd.onode)
  }

  def getMaxNodeSize(tier: Int): Future[Int] = getRData().map { rd =>
    rd.root.nodeAllocator.getMaxNodeSize(tier)
  }

  override def getRootRevisionGuard(): Future[AllocationRevisionGuard] = {

    getRData().map { rd =>
      ObjectRevisionGuard(inodePointer, rd.rootRevision)
    }
  }

  override def encode(): Array[Byte] = {
    val arr = new Array[Byte]( inodePointer.encodedSize )
    val bb = ByteBuffer.wrap(arr)
    bb.order(ByteOrder.BIG_ENDIAN)
    inodePointer.encodeInto(bb)
    arr
  }

  override def prepareRootUpdate(newTier: Int,
                                 newRoot: KeyValueObjectPointer)(using tx: Transaction): Future[Unit] = {
    val p = Promise[Unit]()

    client.read(inodePointer).onComplete {
      case Failure(err) => p.failure(err)
      case Success(inodeDos) =>
        val inode = DirectoryInode(client, inodeDos.data)

        val root = inode.contents

        val nextRoot = root.copy(tier = newTier, orootObject = Some(newRoot))
        val newInode = inode.setContentTree(nextRoot)

        tx.overwrite(inodePointer, inodeDos.revision, newInode.toArray)
        p.success(())
    }

    p.future
  }

  override def createInitialNode(contents: Map[Key,Value])(using tx: Transaction): Future[AllocationRevisionGuard] = {
    for {
      dos <- client.read(inodePointer)
      RData(root, _, onode) <- getRData(Some(dos))
      alloc <- root.nodeAllocator.getAllocatorForTier(0)
      rptr <- alloc.allocateKeyValueObject(ObjectRevisionGuard(inodePointer, dos.revision), contents)
    } yield {
      val dinode = DirectoryInode(client, dos.data)
      val newRoot = Root(0, root.ordering, Some(rptr), root.nodeAllocator)
      val newInode = dinode.setContentTree(newRoot)
      
      tx.overwrite(inodePointer, dos.revision, newInode.toArray)

      onode.foreach: _ =>
        // Check for a race condition where multiple concurrent attempts to
        // create the initial node might clash with each other
        tx.invalidateTransaction(new Exception("Initial TKVL node already exists."))
      
      ObjectRevisionGuard(inodePointer, dos.revision)
    }
  }
}

object SimpleDirectoryRootManager extends RegisteredTypeFactory with RootManagerFactory {
  val typeUUID: UUID = UUID.fromString("52887CBE-0D2B-43C8-80FA-999DA177392D")

  private case class RData(root: Root, rootRevision: ObjectRevision, onode: Option[KeyValueListNode])

  def apply(client: AspenClient, bb: ByteBuffer): SimpleDirectoryRootManager = {
    new SimpleDirectoryRootManager(client, DataObjectPointer(bb))
  }

  override def createRootManager(client: AspenClient, data: Array[Byte]): SimpleDirectoryRootManager = {
    val bb = ByteBuffer.wrap(data)
    bb.order(ByteOrder.BIG_ENDIAN)
    SimpleDirectoryRootManager(client, bb)
  }
}
