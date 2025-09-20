package org.aspen_ddp.aspen.amoebafs.impl.simple

import scala.language.implicitConversions
import java.nio.{ByteBuffer, ByteOrder}
import java.util.UUID
import org.aspen_ddp.aspen.client.tkvl.*
import org.aspen_ddp.aspen.client.{AspenClient, DataObjectState, ObjectAllocator, RegisteredTypeFactory, Transaction}
import org.aspen_ddp.aspen.common.objects.*
import org.aspen_ddp.aspen.amoebafs.FileInode

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success}

class SimpleFileRootManager(client: AspenClient,
                            inodePointer: DataObjectPointer) extends RootManager:

  import SimpleFileRootManager._

  given ExecutionContext = client.clientContext

  override def typeId: RootManagerTypeId = RootManagerTypeId(typeUUID)

  private def getRoot(oInodeDos: Option[DataObjectState] = None): Future[RData] = 
    val p = Promise[RData]()
    
    val fInodeDos = oInodeDos match 
      case None => client.read(inodePointer)
      case Some(idos) => Future.successful(idos)

    fInodeDos.onComplete:
      case Failure(err) => p.failure(err)
      case Success(inodeDos) =>
        val inode = FileInode(client, inodeDos.data)
        val root = inode.contents
        try 
          root.orootObject match 
            case None => p.success(RData(root, inodeDos.revision, None))
            case Some(rootObject) =>
              client.read(rootObject).onComplete:
                case Failure(err) => p.failure(err)
                case Success(rootKvos) =>
                  val rootLp = KeyValueListPointer(Key.AbsoluteMinimum, rootObject)
                  val node = KeyValueListNode(client, rootLp, root.ordering, rootKvos)
                  p.success(RData(root, inodeDos.revision, Some(node)))
        catch 
          case _: Throwable => p.failure(new InvalidRoot)
    
    p.future
  

  def getTree(): Future[TieredKeyValueList] = Future.successful(new TieredKeyValueList(client, this))

  def getAllocatorForTier(tier: Int): Future[ObjectAllocator] = 
    getRoot().flatMap: rd =>
      rd.root.nodeAllocator.getAllocatorForTier(tier)
  
  def getRootNode(): Future[(Int, KeyOrdering, Option[KeyValueListNode])] = 
    getRoot().map: rd =>
      (rd.root.tier, rd.root.ordering, rd.onode)

  def getMaxNodeSize(tier: Int): Future[Int] = 
    getRoot().map: rd =>
      rd.root.nodeAllocator.getMaxNodeSize(tier)

  override def getRootRevisionGuard(): Future[AllocationRevisionGuard] = 
    getRoot().map: rd =>
      ObjectRevisionGuard(inodePointer, rd.rootRevision)

  override def encode(): Array[Byte] =
    val arr = new Array[Byte]( inodePointer.encodedSize )
    val bb = ByteBuffer.wrap(arr)
    bb.order(ByteOrder.BIG_ENDIAN)
    inodePointer.encodeInto(bb)
    arr

  override def prepareRootUpdate(newTier: Int,
                                 newRoot: KeyValueObjectPointer)(using tx: Transaction): Future[Unit] =
    val p = Promise[Unit]()

    client.read(inodePointer).onComplete:
      case Failure(err) => p.failure(err)
      case Success(inodeDos) =>
        val inode = FileInode(client, inodeDos.data)

        val root = inode.contents

        val nextRoot = root.copy(tier = newTier, orootObject = Some(newRoot))
        val newInode = inode.setContentTree(nextRoot)

        tx.overwrite(inodePointer, inodeDos.revision, newInode.toArray)
        p.success(())

    p.future

  override def createInitialNode(contents: Map[Key,Value])(using tx: Transaction): Future[AllocationRevisionGuard] =
    for
      dos <- client.read(inodePointer)
      RData(root, _, onode) <- getRoot(Some(dos))
      alloc <- root.nodeAllocator.getAllocatorForTier(0)
      rptr <- alloc.allocateKeyValueObject(ObjectRevisionGuard(inodePointer, dos.revision), contents)
    yield 
      val finode = FileInode(client, dos.data)
      val newRoot = Root(0, root.ordering, Some(rptr), root.nodeAllocator)
      val newInode = finode.setContentTree(newRoot)
      
      tx.overwrite(inodePointer, dos.revision, newInode.toArray)
      
      onode.foreach: _ =>
        // Check for a race condition where multiple concurrent attempts to
        // create the initial node might clash with each other
        tx.invalidateTransaction(new Exception("Initial TKVL node already exists."))
      
      ObjectRevisionGuard(inodePointer, dos.revision)
      

object SimpleFileRootManager extends RegisteredTypeFactory with RootManagerFactory:
  val typeUUID: UUID = UUID.fromString("51936423-bbd1-4500-b60d-b4c557b69f24")

  private case class RData(root: Root, rootRevision: ObjectRevision, onode: Option[KeyValueListNode])

  def apply(client: AspenClient, bb: ByteBuffer): SimpleFileRootManager =
    new SimpleFileRootManager(client, DataObjectPointer(bb))

  override def createRootManager(client: AspenClient, data: Array[Byte]): SimpleFileRootManager =
    val bb = ByteBuffer.wrap(data)
    bb.order(ByteOrder.BIG_ENDIAN)
    SimpleFileRootManager(client, bb)
  

