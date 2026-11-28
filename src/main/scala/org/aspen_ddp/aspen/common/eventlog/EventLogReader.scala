package org.aspen_ddp.aspen.common.eventlog

import org.aspen_ddp.aspen.client.{AspenClient, InvalidObject}
import org.aspen_ddp.aspen.client.tkvl.{DataObjectTKVL, KVObjectRootManager}
import org.aspen_ddp.aspen.common.HLCTimestamp
import org.aspen_ddp.aspen.common.objects.{DataObjectPointer, Key, KeyValueObjectPointer, Value}
import scribe.Logging

import java.util.UUID
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

object EventLogReader:

  /** Attempts after the first that a read makes before failing */
  val MaxRestarts: Int = 5

  /** The smallest timestamp: a read from it includes every entry */
  val Earliest: HLCTimestamp = HLCTimestamp(Long.MinValue)

  /** The log as read did not hang together: something moved records between the reads. The read restarts. */
  private final class Inconsistent(msg: String) extends Exception(msg)

/** Reads a range of the system event log without omitting any event logged before the read began.
  *
  * An attempt reads incomingEvents (entries logged but not yet moved by the service), then the state object (tails,
  * typeIds), then every tail, then walks loggedEvents from the full object covering `from`, reading each object as
  * the walk reaches it. Entries only move forward along that order (incoming -> tails -> full objects), so a move
  * committing between two of the reads yields duplicates (an entry read before and again after it moved), never a
  * gap. Undecodable incoming values are skipped; the service deletes them.
  *
  * A split or rekey moves records between full objects, so the walk's keys may be stale by the time an object is
  * read. Every object's header is checked against the walk: its filedKey must be the key it was found under, and an
  * upper bound must be the next key the walk found (the object is the last when the walk found none). A split sets
  * the rewritten object's upper bound to the new part's key and a rekey changes the filedKey, so either, committed
  * between the walk and the object read, fails the check. The walk's first key must be the one it was started from,
  * so a rekey that deletes it between finding it and walking fails too. So does an object read as InvalidObject or
  * a record whose type id the state read does not know. The read then restarts from the state object, at most MaxRestarts times,
  * and fails with IllegalStateException after that.
  *
  * Each instance holds test seams and counts its restarts, so instances are not shared between reads in tests.
  */
final class EventLogReader(client: AspenClient) extends Logging:

  import EventLogReader.*

  private given ExecutionContext = client.clientContext

  /** Runs between the tail reads and the tree walk, once per attempt */
  private[eventlog] var testAfterTailsRead: () => Future[Unit] = () => Future.unit

  /** Runs between finding the walk's start (`getContainingNode`) and walking from it, once per attempt */
  private[eventlog] var testAfterContainingNode: () => Future[Unit] = () => Future.unit

  /** Runs once per attempt, after the walk read the tree node holding the first object read and before that read */
  private[eventlog] var testBeforeFirstObjectRead: () => Future[Unit] = () => Future.unit

  /** Attempts restarted so far */
  @volatile private[eventlog] var restarts: Int = 0

  /** Entries with timestamps in `[from, to]`, sorted by timestamp, at most `limit` of them; duplicates possible */
  def read(from: HLCTimestamp, to: Option[HLCTimestamp], limit: Option[Int]): Future[Vector[EventLogEntry]] =
    limit.foreach(n => require(n >= 0, s"limit must not be negative: $n"))
    def attempt(n: Int): Future[Vector[EventLogEntry]] =
      Future.delegate(readOnce(from, to, limit)).recoverWith:
        case err @ (_: Inconsistent | _: InvalidObject) =>
          if n >= MaxRestarts then
            Future.failed(new IllegalStateException(
              s"Event log read gave up after $MaxRestarts restarts; the log kept changing under it", err))
          else
            logger.debug(s"Event log read restarting: $err")
            restarts += 1
            attempt(n + 1)
    if limit.contains(0) then Future.successful(Vector.empty) else attempt(0)

  private def inRange(from: HLCTimestamp, to: Option[HLCTimestamp])(ts: HLCTimestamp): Boolean =
    ts >= from && to.forall(ts <= _)

  /** The decodable entries of incomingEvents in the range */
  private def readIncoming(ptr: KeyValueObjectPointer,
                           from: HLCTimestamp,
                           to: Option[HLCTimestamp]): Future[Vector[EventLogEntry]] =
    SystemEventLog.incomingTree(client, ptr).foldLeft(Vector.empty[EventLogEntry]): (acc, contents) =>
      acc ++ contents.valuesIterator.flatMap: vs =>
        try Some(EventLogEntry.decode(vs.value.bytes)).filter(e => inRange(from, to)(e.timestamp))
        catch
          case _: Throwable => None

  private def readOnce(from: HLCTimestamp,
                       to: Option[HLCTimestamp],
                       limit: Option[Int]): Future[Vector[EventLogEntry]] =
    for
      ptr <- SystemEventLog.statePointer(client)
      incoming <- readIncoming(ptr, from, to)
      kvos <- client.read(ptr, "event log state")
      typeIds = EventLogState.decodeTypeIds(kvos.contents(EventLogState.TypeIdsKey).value.bytes)
      tails = EventLogState.decodeTails(kvos.contents(EventLogState.TailsKey).value.bytes)
      tailStates <- Future.traverse(tails.pointers)(p => client.read(p, "event log tail"))
      _ <- testAfterTailsRead()
      tailRecords = tailStates.flatMap(s => LogRecords.decodeFrames(s.data.getByteArray))
        .filter(r => inRange(from, to)(r.timestamp))
      // A fresh root manager per attempt, so a restart never walks cached nodes
      logged <- KVObjectRootManager(client, EventLogState.LoggedKey, ptr).getDataObjectTree()
      fullRecords <- walk(logged, from, to, limit)
    yield
      val merged: Vector[Either[LoggedRecord, EventLogEntry]] =
        (incoming.map(Right(_)) ++ (tailRecords ++ fullRecords).map(Left(_)))
          .sortBy(_.fold(_.timestamp, _.timestamp))
      val uuidOf: Map[Short, UUID] = typeIds.map(_.swap)
      limit.fold(merged)(merged.take).map:
        case Right(e) => e
        case Left(r) =>
          uuidOf.get(r.typeId) match
            case Some(uuid) => EventLogEntry(r.timestamp, uuid, r.data)
            case None => throw new Inconsistent(s"type id ${r.typeId & 0xFFFF} is not in the typeIds read")

  /** The in-range records of every full object that may hold one.
    *
    * The walk starts at the largest key at or below `from` in the tier-0 node containing it. Only when that node
    * holds none (`from` precedes every key, or the node's minimum entry was deleted by a rekey) does it start at the
    * beginning of the tree, skipping objects until the last one at or below `from`. A walk from a found key must
    * visit that key first: a rekey committed in between deletes it, and the walk would otherwise start at the next
    * object and never read the one covering `from`. An object is read when the walk
    * reaches the key after it, which its upper bound is checked against; the last is read with no next key once the
    * walk ends. The walk stops, leaving the object found last unread, at a key past `to`, or once `limit` records
    * timestamped before that key are in hand: every later object's records are at or after its key, so they would
    * sort after those and be cut by the limit.
    */
  private def walk(logged: DataObjectTKVL,
                   from: HLCTimestamp,
                   to: Option[HLCTimestamp],
                   limit: Option[Int]): Future[Vector[LoggedRecord]] =
    val gathered = ArrayBuffer.empty[LoggedRecord]
    var pending: Option[(HLCTimestamp, DataObjectPointer)] = None
    var done = false
    var failure: Option[Throwable] = None
    var seamRun = false
    var visited = false

    def readObject(key: HLCTimestamp, pointer: DataObjectPointer, next: Option[HLCTimestamp]): Future[Unit] =
      val seam =
        if seamRun then
          Future.unit
        else
          seamRun = true
          testBeforeFirstObjectRead()
      for
        _ <- seam
        state <- client.read(pointer, "event log full object")
      yield
        val (header, records) = FullObject.decode(state.data.getByteArray)
        if header.filedKey != key || header.upperBound.exists(ub => !next.contains(ub)) then
          throw new Inconsistent(s"full object ${pointer.id} ($header) does not match loggedEvents as walked " +
            s"(key $key, next key $next)")
        gathered ++= records.filter(r => inRange(from, to)(r.timestamp))

    def limitReached(nextKey: HLCTimestamp): Boolean = limit.exists: n =>
      gathered.size >= n && gathered.iterator.map(_.timestamp).toVector.sorted.apply(n - 1) < nextKey

    // The walk logs and skips a failing visit, so a failure is recorded here and ends the walk instead
    def visit(floor: Option[Key])(k: Key, v: Value): Future[Unit] =
      Future.delegate:
        val key = LogKey.decode(k)
        val pointer = DataObjectPointer(v.bytes)
        val first = !visited
        visited = true
        if first && floor.exists(f => LogKey.decode(f) != key) then
          throw new Inconsistent(s"loggedEvents walk from ${floor.map(LogKey.decode)} first found $key")
        else if key <= from then
          pending = Some(key -> pointer)
          Future.unit
        else
          val prior = pending.fold(Future.unit)((pk, pp) => readObject(pk, pp, Some(key)))
          prior.map: _ =>
            if to.exists(key > _) || limitReached(key) then
              done = true
            else
              pending = Some(key -> pointer)
      .transform:
        case Success(_) => Success(())
        case Failure(err) =>
          failure = Some(err)
          done = true
          Success(())

    for
      onode <- logged.getContainingNode(LogKey(from))
      floor = onode.flatMap(_.entries.map(_._1).takeWhile(k => LogKey.decode(k) <= from).lastOption)
      _ <- testAfterContainingNode()
      _ <- logged.foreachFrom(floor.getOrElse(Key.AbsoluteMinimum), visit(floor), () => done)
      _ <- failure match
        case Some(err) => Future.failed(err)
        case None if floor.isDefined && !visited =>
          Future.failed(new Inconsistent(s"loggedEvents walk from ${floor.map(LogKey.decode)} found nothing"))
        case None if done => Future.unit
        case None => pending.fold(Future.unit)((pk, pp) => readObject(pk, pp, None))
    yield gathered.toVector
