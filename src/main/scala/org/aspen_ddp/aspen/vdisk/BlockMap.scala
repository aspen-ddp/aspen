package org.aspen_ddp.aspen.vdisk

import org.aspen_ddp.aspen.client.Transaction
import org.aspen_ddp.aspen.client.tkvl.{DataObjectTKVL, TKVLInsert}
import org.aspen_ddp.aspen.common.objects.{DataObjectPointer, Key, Value}

import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}

/** The disk's block index -> block object map. The owner is the map's only writer, so every entry it has looked up
  * or committed, absence included, stays valid in memory and no index is looked up over the network twice.
  * Thread-safe.
  */
final class BlockMap(tree: DataObjectTKVL):

  private given ExecutionContext = tree.client.clientContext

  private val known = mutable.HashMap[Long, Option[DataObjectPointer]]()
  private val pending = mutable.HashMap[Long, Future[Option[DataObjectPointer]]]()

  def lookup(index: Long): Future[Option[DataObjectPointer]] = synchronized:
    known.get(index) match
      case Some(r) => Future.successful(r)
      case None => pending.getOrElseUpdate(index, fetch(index))

  /** Reads the entry from the tree, bypassing what is held in memory, and holds the result */
  def refresh(index: Long): Future[Option[DataObjectPointer]] =
    tree.get(Key(index)).map: ov =>
      val r = ov.map(v => DataObjectPointer(v.bytes))
      synchronized:
        known(index) = r
      r

  def prepare(inserts: List[(Long, DataObjectPointer)], deletes: List[Long])(using Transaction): Future[Unit] =
    tree.prepareUpdates(
      inserts.map((index, ptr) => TKVLInsert(Key(index), Value(ptr.toArray), requireDoesNotExist = true)),
      deletes.map(Key(_)))

  /** Records the effect of a committed `prepare` */
  def recordCommitted(inserts: List[(Long, DataObjectPointer)], deletes: List[Long]): Unit = synchronized:
    inserts.foreach((index, ptr) => known(index) = Some(ptr))
    deletes.foreach(index => known(index) = None)

  private def fetch(index: Long): Future[Option[DataObjectPointer]] =
    val f = tree.get(Key(index)).map(_.map(v => DataObjectPointer(v.bytes)))
    f.onComplete: r =>
      synchronized:
        pending.remove(index)
        // A commit recorded while the read was outstanding is newer than what the read saw
        r.foreach(ptr => if !known.contains(index) then known(index) = ptr)
    f.map(r => synchronized(known.getOrElse(index, r)))
