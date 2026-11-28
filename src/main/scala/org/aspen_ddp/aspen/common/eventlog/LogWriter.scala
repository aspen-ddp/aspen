package org.aspen_ddp.aspen.common.eventlog

import org.aspen_ddp.aspen.client.{AppendPosition, AspenClient, DataObjectState, ObjectAllocator, Transaction}
import org.aspen_ddp.aspen.client.tkvl.{DataObjectTKVL, TKVLInsert}
import org.aspen_ddp.aspen.common.DataBuffer
import org.aspen_ddp.aspen.common.ida.{RandomAccessReedSolomon, Replication}
import org.aspen_ddp.aspen.common.objects.{DataObjectPointer, Value}
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

  /** Stages the plan's writes.
    *
    * Every tail is written at most once: all of a plan's records for one tail go into one new frame. A Compact
    * allocates its full objects from `allocator`, inserts their pointers into `logged` in one batch and overwrites
    * every tail, with the frame of the records the plan appends to it after the compaction, or empty; it requires
    * every tail to be Rewritable (read whole), since the compaction's records came from those reads. A Drop needs no
    * write (its record is consumed, its replacement placed by another step); one whose replacement fit nowhere is
    * logged. Fails with NotImplementedError, before staging anything, for a plan with a full-object rewrite.
    */
  def apply(plan: LogPlan,
            tails: Vector[LoadedTail],
            allocator: ObjectAllocator,
            logged: DataObjectTKVL)(using tx: Transaction): Future[Unit] =
    given ExecutionContext = allocator.client.clientContext

    plan.steps.foreach:
      case _: LogStep.RewriteFull => throw new NotImplementedError("Event log full-object rewrites are not supported yet")
      case _ => ()

    val compaction = plan.steps.collect { case c: LogStep.Compact => c }
    require(compaction.size <= 1, "a plan compacts at most once")

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
        Future.unit

      case Some(compact) =>
        val states = tails.map:
          case LoadedTail.Rewritable(state, _) => state
          case LoadedTail.Appendable(p, _) =>
            throw new IllegalStateException(s"Compaction needs every tail read whole; tail ${p.pointer.id} was not")
        states.zipWithIndex.foreach: (state, i) =>
          val content = byTail.get(i).fold(Array.emptyByteArray)(LogRecords.encodeFrame)
          tx.overwrite(state.pointer, state.revision, DataBuffer(content))

        // Sequentially, so allocations are staged in plan order
        val allocated = compact.objects.foldLeft(Future.successful(Vector.empty[TKVLInsert])): (acc, obj) =>
          val (header, records) = obj
          acc.flatMap: inserts =>
            allocator.allocateDataObject(DataBuffer(FullObject.encode(header, records))).map: ptr =>
              inserts :+ TKVLInsert(LogKey(header.filedKey), Value(ptr.toArray), requireDoesNotExist = true)
        allocated.flatMap(inserts => logged.prepareUpdates(inserts.toList, Nil))
