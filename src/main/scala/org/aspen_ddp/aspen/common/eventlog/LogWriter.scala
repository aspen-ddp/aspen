package org.aspen_ddp.aspen.common.eventlog

import org.aspen_ddp.aspen.client.{AppendPosition, AspenClient, DataObjectState, Transaction}
import org.aspen_ddp.aspen.common.DataBuffer
import org.aspen_ddp.aspen.common.ida.{RandomAccessReedSolomon, Replication}
import org.aspen_ddp.aspen.common.objects.DataObjectPointer

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
object LogWriter:

  /** Reads a tail the way it will be written: append support is decided by the pool the object lives in */
  def load(client: AspenClient, tail: DataObjectPointer): Future[LoadedTail] =
    given ExecutionContext = client.clientContext
    client.getStoragePool(tail.poolId).flatMap: pool =>
      pool.ida match
        case _: Replication | _: RandomAccessReedSolomon =>
          client.readAppendPosition(tail, "event log tail").map(LoadedTail.Appendable(_, pool.maxObjectSize))
        case _ =>
          client.read(tail, "event log tail").map(LoadedTail.Rewritable(_, pool.maxObjectSize))

  /** Stages the plan's writes. Every tail is written at most once: all of a plan's records for one tail go into one
    * new frame. Fails with NotImplementedError, before staging anything, for a plan with compaction, full-object
    * rewrites or drops, which are not supported yet.
    */
  def apply(plan: LogPlan, tails: Vector[LoadedTail])(using tx: Transaction): Unit =
    plan.steps.foreach:
      case _: LogStep.AppendToTail => ()
      case other => throw new NotImplementedError(s"Event log step not supported yet: ${other.getClass.getSimpleName}")

    val byTail = plan.steps
      .collect { case LogStep.AppendToTail(i, rs) => i -> rs }
      .groupMapReduce(_._1)(_._2)(_ ++ _)

    byTail.toVector.sortBy(_._1).foreach: (i, records) =>
      val frame = LogRecords.encodeFrame(records)
      tails(i) match
        case LoadedTail.Appendable(position, _) =>
          tx.append(position, DataBuffer(frame))
        case LoadedTail.Rewritable(state, _) =>
          tx.overwrite(state.pointer, state.revision, DataBuffer(state.data.getByteArray ++ frame))
