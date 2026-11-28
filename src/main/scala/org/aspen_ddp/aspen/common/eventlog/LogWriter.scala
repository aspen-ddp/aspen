package org.aspen_ddp.aspen.common.eventlog

import org.aspen_ddp.aspen.client.{AppendPosition, AspenClient, DataObjectState, ObjectAllocator, Transaction}
import org.aspen_ddp.aspen.client.tkvl.{DataObjectTKVL, TKVLInsert}
import org.aspen_ddp.aspen.common.{DataBuffer, HLCTimestamp}
import org.aspen_ddp.aspen.common.ida.{RandomAccessReedSolomon, Replication}
import org.aspen_ddp.aspen.common.objects.{DataObjectPointer, Key, Value}
import scribe.Logging

import scala.concurrent.{ExecutionContext, Future}

/** A tail object as read for one move transaction: appended to when its pool's IDA supports appends, else rewritten
  * whole. cap is the hard cap (maxObjectSize) of the pool it lives in.
  */
enum LoadedTail:
  case Appendable(position: AppendPosition, cap: Option[Int])
  case Rewritable(state: DataObjectState, cap: Option[Int])

  def length: Int = this match
    case Appendable(p, _) => p.length
    case Rewritable(s, _) => s.size

  def maxSize: Option[Int] = this match
    case Appendable(_, c) => c
    case Rewritable(_, c) => c

/** The full object a late batch is merged into, as read: its planner view and the state its rewrite overwrites */
final case class LoadedFull(view: FullView, state: DataObjectState)

/** Applies a LogPlan to a Transaction */
object LogWriter extends Logging:

  /** Reads a tail the way it will be written: append support is decided by the pool the object lives in */
  def load(client: AspenClient, tail: DataObjectPointer): Future[LoadedTail] =
    given ExecutionContext = client.clientContext
    client.getStoragePool(tail.poolId).flatMap: pool =>
      pool.ida match
        case _: Replication | _: RandomAccessReedSolomon =>
          client.readAppendPosition(tail, "event log tail").map(LoadedTail.Appendable(_, pool.maxObjectSize))
        case _ =>
          loadWhole(client, tail)

  /** Reads a tail's whole content, as a plan that may compact needs it. Written by overwriting, whatever its IDA. */
  def loadWhole(client: AspenClient, tail: DataObjectPointer): Future[LoadedTail.Rewritable] =
    given ExecutionContext = client.clientContext
    for
      pool <- client.getStoragePool(tail.poolId)
      state <- client.read(tail, "event log tail")
    yield LoadedTail.Rewritable(state, pool.maxObjectSize)

  /** Reads the full object filed under `key` in loggedEvents, `nextKey` being the following key there (None when
    * it is the last). Fails when the object does not match the tree as read: its header names another key, or an
    * upper bound that is not `nextKey`. A split or rekey committed between the tree read and this one does that,
    * and planning against the stale tree could merge records outside the object's range; the attempt is retried.
    */
  def loadFull(client: AspenClient,
               key: HLCTimestamp,
               pointer: DataObjectPointer,
               nextKey: Option[HLCTimestamp]): Future[LoadedFull] =
    given ExecutionContext = client.clientContext
    for
      pool <- client.getStoragePool(pointer.poolId)
      state <- client.read(pointer, "event log full object")
    yield
      val (header, records) = FullObject.decode(state.data.getByteArray)
      if header.filedKey != key || header.upperBound.exists(ub => !nextKey.contains(ub)) then
        throw new IllegalStateException(s"Event log full object ${pointer.id} ($header) does not match loggedEvents " +
          s"as read (key $key, next key $nextKey)")
      LoadedFull(FullView(key, header.upperBound, nextKey, records, pool.maxObjectSize), state)

  /** Stages the plan's writes.
    *
    * Every tail is written at most once: all of a plan's records for one tail go into one new frame. A Compact
    * allocates its full objects from `allocator` and overwrites every tail, with the frame of the records the plan
    * appends to it after the compaction, or empty; it requires every tail to be Rewritable (read whole), since the
    * compaction's records came from those reads. A RewriteFull overwrites `late`'s object (at the revision it was
    * read at) with its first part and allocates the rest from `allocator`. Every loggedEvents change of the plan,
    * the compaction's new keys and the rewrite's, goes into one `prepareUpdates` batch, so each tree node is updated
    * once. A Drop needs no write (its record is consumed, its replacement placed by another step); one whose
    * replacement fit nowhere is logged.
    */
  def apply(plan: LogPlan,
            tails: Vector[LoadedTail],
            late: Option[LoadedFull],
            allocator: ObjectAllocator,
            logged: DataObjectTKVL)(using tx: Transaction): Future[Unit] =
    given ExecutionContext = allocator.client.clientContext

    val compaction = plan.steps.collect { case c: LogStep.Compact => c }
    require(compaction.size <= 1, "a plan compacts at most once")
    val rewrite = plan.steps.collect { case r: LogStep.RewriteFull => r }
    require(rewrite.size <= 1, "a plan rewrites at most one full object")
    rewrite.headOption.foreach: r =>
      require(late.exists(_.view.key == r.oldKey), s"the rewritten full object ${r.oldKey} was not loaded")

    plan.steps.foreach:
      case LogStep.Drop(r, false) =>
        logger.warn(s"Event log: dropped a record (type id ${r.typeId & 0xFFFF}, timestamp ${r.timestamp}) whose " +
          "DroppedEvent replacement fit nowhere; it is not logged")
      case _ => ()

    val byTail = plan.steps
      .collect { case LogStep.AppendToTail(i, rs) => i -> rs }
      .groupMapReduce(_._1)(_._2)(_ ++ _)

    compaction.headOption match
      case None =>
        byTail.toVector.sortBy(_._1).foreach: (i, records) =>
          val frame = LogRecords.encodeFrame(records)
          tails(i) match
            case LoadedTail.Appendable(position, _) =>
              tx.append(position, DataBuffer(frame))
            case LoadedTail.Rewritable(state, _) =>
              tx.overwrite(state.pointer, state.revision, DataBuffer(state.data.getByteArray ++ frame))

      case Some(_) =>
        val states = tails.map:
          case LoadedTail.Rewritable(state, _) => state
          case LoadedTail.Appendable(p, _) =>
            throw new IllegalStateException(s"Compaction needs every tail read whole; tail ${p.pointer.id} was not")
        states.zipWithIndex.foreach: (state, i) =>
          val content = byTail.get(i).fold(Array.emptyByteArray)(LogRecords.encodeFrame)
          tx.overwrite(state.pointer, state.revision, DataBuffer(content))

    rewrite.headOption.foreach: r =>
      val (header, records) = r.objects.head
      val state = late.get.state
      tx.overwrite(state.pointer, state.revision, DataBuffer(FullObject.encode(header, records)))

    val newObjects = rewrite.flatMap(_.objects.tail) ++ compaction.flatMap(_.objects)
    for
      pointers <- allocateAll(newObjects, allocator)
      (inserts, deletes) = treeChanges(rewrite.headOption, late, newObjects.map(_._1).zip(pointers))
      _ <- if inserts.isEmpty && deletes.isEmpty then Future.unit else logged.prepareUpdates(inserts, deletes)
    yield ()

  /** Allocates the objects sequentially, so allocations are staged in order */
  private def allocateAll(objects: Vector[(FullObjectHeader, Vector[LoggedRecord])], allocator: ObjectAllocator)
                         (using Transaction, ExecutionContext): Future[Vector[DataObjectPointer]] =
    objects.foldLeft(Future.successful(Vector.empty[DataObjectPointer])): (acc, obj) =>
      val (header, records) = obj
      acc.flatMap: pointers =>
        allocator.allocateDataObject(DataBuffer(FullObject.encode(header, records))).map(pointers :+ _)

  /** The loggedEvents batch: every new object inserted under its filedKey; a rekeyed rewrite files the rewritten
    * object under its new key and removes the old one. When a new part is filed under the old key, its put replaces
    * the old entry instead of a delete and an insert of the same key.
    */
  private def treeChanges(rewrite: Option[LogStep.RewriteFull],
                          late: Option[LoadedFull],
                          allocated: Vector[(FullObjectHeader, DataObjectPointer)]): (List[TKVLInsert], List[Key]) =
    val oldKey = rewrite.map(_.oldKey)
    val rekeyedTo = rewrite.map(_.objects.head._1.filedKey).filterNot(oldKey.contains)
    val rekey = rekeyedTo.toList.map: k =>
      TKVLInsert(LogKey(k), Value(late.get.state.pointer.toArray), requireDoesNotExist = true)
    val news = allocated.toList.map: (header, ptr) =>
      val replacesOld = rekeyedTo.isDefined && oldKey.contains(header.filedKey)
      TKVLInsert(LogKey(header.filedKey), Value(ptr.toArray), requireDoesNotExist = !replacesOld)
    val deletes =
      if rekeyedTo.isDefined && !allocated.exists(a => oldKey.contains(a._1.filedKey)) then oldKey.toList.map(LogKey(_))
      else Nil
    (rekey ++ news, deletes)
