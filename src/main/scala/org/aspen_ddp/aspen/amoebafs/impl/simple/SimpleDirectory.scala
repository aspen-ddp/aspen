package org.aspen_ddp.aspen.amoebafs.impl.simple

import scala.language.implicitConversions
import org.aspen_ddp.aspen.client.KeyValueObjectState.ValueState
import org.aspen_ddp.aspen.client.{StopRetrying, Transaction}
import org.aspen_ddp.aspen.client.tkvl.TieredKeyValueList
import org.aspen_ddp.aspen.common.objects.{Key, ObjectRevision, Value}
import org.aspen_ddp.aspen.amoebafs.error.{DirectoryEntryDoesNotExist, DirectoryEntryExists, DirectoryNotEmpty}
import org.aspen_ddp.aspen.amoebafs.{BaseFile, Directory, DirectoryEntry, DirectoryInode, DirectoryPointer, FileSystem, InodePointer}
import org.apache.logging.log4j.scala.Logging

import scala.concurrent.Future

class SimpleDirectory(override val pointer: DirectoryPointer,
                      cachedInodeRevision: ObjectRevision,
                      initialInode: DirectoryInode,
                      fs: FileSystem) extends SimpleBaseFile(pointer, cachedInodeRevision, initialInode, fs) with Directory with Logging {

  def tree: TieredKeyValueList = {
    val rootManager = new SimpleDirectoryRootManager(fs.client, pointer.pointer)
    new TieredKeyValueList(fs.client, rootManager)
  }

  override def isEmpty(): Future[Boolean] = {
    tree.foldLeft(true)( (s, t) => s && t.isEmpty)
  }

  override def getContents(): Future[List[DirectoryEntry]] = {

    def scan(sum: List[DirectoryEntry], m: Map[Key, ValueState]): List[DirectoryEntry] = {
      m.foldLeft(sum) { (s, t) =>
        DirectoryEntry(t._1.stringValue, InodePointer(t._2.value.bytes)) :: s
      }
    }

    tree.foldLeft(List[DirectoryEntry]())(scan).map(l => l.reverse)
  }

  override def getEntry(name: String): Future[Option[InodePointer]] = {
    for {
      ovs <- tree.get(Key(name))
    } yield {
      ovs.map(vs => InodePointer(vs.value.bytes))
    }
  }

  override def prepareInsert(name: String,
                             pointer: InodePointer,
                             incref: Boolean)(implicit tx: Transaction): Future[Unit] = {
    val fcheck = tree.getContainingNode(name).map { onode =>
      onode.map { node =>
        if (node.contains(name))
          prepareDelete(name, true)
      }
    }

    val fincref = if (incref) {
      fs.readInode(pointer) map { t =>
        val (finode, _, frevision) = t
        val updatedInode = finode.update(links=Some(inode.links+1))
        tx.overwrite(pointer.pointer, frevision, updatedInode.toArray)
      }
    } else {
      Future.successful(())
    }

    for {
      _ <- fcheck
      _ <- fincref
      _ <- tree.set(Key(name), Value(pointer.toArray), Some(Left(true)))
    } yield ()
  }

  override def prepareDelete(name: String, decref: Boolean)(implicit tx: Transaction): Future[Unit] = {
    val key = Key(name)

    def onptr(ovs: Option[ValueState]): Future[Unit] = ovs match {
      case None => Future.failed(DirectoryEntryDoesNotExist(pointer, name))
      case Some(vs) =>
        val fptr = InodePointer(vs.value.bytes)
        val fcheck = fptr match {
          case dptr: DirectoryPointer =>
            for {
              d <- fs.loadDirectory(dptr)
              (_, revision) <- d.getInode()
              isEmpty <- d.isEmpty()
            } yield {
              if (!isEmpty) {
                throw  DirectoryNotEmpty(dptr)
              } else {
                // Use this to ensure that no entries are added to the directory
                // while we're in the process of deleting it
                tx.bumpVersion(d.pointer.pointer, revision)
              }
            }
          case _ => Future.successful(())
        }

        for {
          _ <- fcheck
          _ <- tree.delete(key)
          _ <- if (decref) {
            UnlinkFileTask.prepareTask(fs, fptr)
          } else {
            Future.successful(())
          }
        } yield {
          tx.result.map(_=>())
        }
    }

    for {
      ovs <- tree.get(key)
      _ <- onptr(ovs)
    } yield ()
  }

  override def prepareRename(oldName: String, newName: String)(implicit tx: Transaction): Future[Unit] = {
    val oldKey = Key(oldName)
    val newKey = Key(newName)
    val tr = tree
    for {
      oa <- tr.getContainingNode(oldKey)
      ob <- tr.getContainingNode(newKey)
      _ <- (oa, ob) match {
        case (Some(a), Some(b)) =>
          if (b.contains(newKey))
            throw DirectoryEntryExists(this.pointer, newName)
          if (!a.contains(oldKey)) {
            Future.failed(DirectoryEntryDoesNotExist(pointer, oldKey.stringValue))
          } else {
            if (a.nodeUUID == b.nodeUUID)
              a.rename(oldKey, newKey)
            else {
              Future.sequence(List(a.delete(oldKey), b.set(newKey, a.get(oldKey).get.value)))
            }
          }
        case _ => Future.failed(DirectoryEntryDoesNotExist(pointer, oldName))
      }
    } yield {

    }
  }

  override def prepareHardLink(name: String, file: BaseFile)(implicit tx: Transaction): Future[Unit] = {
    prepareInsert(name, file.pointer)
  }

  /** Ensures the directory is empty and that all resources are cleaned up if the transaction successfully commits
    */
  override def prepareForDirectoryDeletion()(implicit tx: Transaction): Future[Unit] = {
    // TODO: protect against race conditions during directory deletion
    getContents().map { contents =>
      if (contents.nonEmpty)
        throw DirectoryNotEmpty(pointer)
    }
  }


}
