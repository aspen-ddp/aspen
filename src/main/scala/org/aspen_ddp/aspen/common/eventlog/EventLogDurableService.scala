package org.aspen_ddp.aspen.common.eventlog

import org.aspen_ddp.aspen.client.{AspenClient, KeyValueObjectState, Transaction}
import org.aspen_ddp.aspen.client.KeyValueObjectState.ValueState
import org.aspen_ddp.aspen.client.tkvl.{DataObjectTKVL, KVObjectRootManager, TieredKeyValueList}
import org.aspen_ddp.aspen.common.HLCTimestamp
import org.aspen_ddp.aspen.common.network.ServiceMessage
import org.aspen_ddp.aspen.common.objects.{Insert, Key, KeyValueObjectPointer, ObjectRevision}
import org.aspen_ddp.aspen.common.transaction.KeyValueUpdate
import org.aspen_ddp.aspen.common.util.BackgroundTaskManager.{NoTask, ScheduledTask}
import org.aspen_ddp.aspen.compute.{DurableService, DurableServiceFactory}
import scribe.Logging

import java.util.{IdentityHashMap, UUID}
import scala.concurrent.duration.{Duration, DurationInt}
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

object EventLogDurableService extends DurableServiceFactory with Logging:

  val ServiceTypeUUID: UUID = UUID.fromString("5c0e8f43-7b1d-4a2e-9d36-0f4b8a1c2e57")
  val ServiceUUID: UUID     = UUID.fromString("a93d27e6-41c8-4f0b-b5e2-6d17c9f083a4")

  val typeUUID: UUID = ServiceTypeUUID

  val DefaultPollPeriod: Duration = 15.seconds

  /** Soft target size of a full log object; the log allocator's maxObjectSize, when smaller, caps it */
  val DefaultTargetObjectSize: Int = 2 * 1024 * 1024

  /** Bounds of one move batch, so a transaction stays reasonably sized */
  val MaxBatchEntries: Int = 1000
  val MaxBatchBytes: Int = 1024 * 1024

  /** Failed move transactions in a row after which a scan gives up until the next poll or nudge */
  val MaxConsecutiveFailures: Int = 10

  /** Largest type id: ids are 16 bits, read unsigned */
  val MaxTypeId: Int = 0xFFFF

  // Test seams, keyed by client because suites run in parallel in one JVM; weak keys vanish with their client
  private val testPollPeriods =
    java.util.Collections.synchronizedMap(new java.util.WeakHashMap[AspenClient, Duration]())
  private val testTargetObjectSizes =
    java.util.Collections.synchronizedMap(new java.util.WeakHashMap[AspenClient, Integer]())

  private[aspen] def setTestPollPeriod(client: AspenClient, period: Duration): Unit =
    testPollPeriods.put(client, period)

  private[aspen] def setTestTargetObjectSize(client: AspenClient, size: Int): Unit =
    testTargetObjectSizes.put(client, size)

  private def pollPeriodFor(client: AspenClient): Duration =
    Option(testPollPeriods.get(client)).getOrElse(DefaultPollPeriod)

  private def targetObjectSizeFor(client: AspenClient): Int =
    Option(testTargetObjectSizes.get(client)).map(_.intValue).getOrElse(DefaultTargetObjectSize)

  override def createService(client: AspenClient,
                             statePointer: KeyValueObjectPointer,
                             state: KeyValueObjectState): DurableService =
    new EventLogDurableService(client, statePointer, pollPeriodFor(client), targetObjectSizeFor(client))

  /** How one batch attempt ended */
  private enum BatchResult:
    case Moved, Empty, Stuck

/** Moves entries from incomingEvents into the log objects, one bounded batch per transaction.
  *
  * Every move transaction deletes the moved incoming keys at their read KeyRevision (its pin) and checks the tails and
  * the state keys it writes, so two instances overlapping after a lease handoff never both commit the same entry: the
  * loser aborts and its next batch re-reads. Scans run one at a time; a nudge arriving during a scan makes it scan
  * again once done.
  *
  * Nudges are best-effort; polling covers lost ones.
  */
class EventLogDurableService(val client: AspenClient,
                             val statePointer: KeyValueObjectPointer,
                             pollPeriod: Duration,
                             targetObjectSize: Int) extends DurableService with Logging:

  import EventLogDurableService.*

  private given ExecutionContext = client.clientContext

  private val incoming: TieredKeyValueList = SystemEventLog.incomingTree(client, statePointer)
  private val loggedRoot = KVObjectRootManager(client, EventLogState.LoggedKey, statePointer)

  private var scanning = false
  private var rescan = false
  private var pollTask: ScheduledTask = NoTask
  @volatile private var stopped = false

  requestScan()
  pollTask = client.backgroundTaskManager.scheduleNonConcurrentPollingTask("event-log-move", pollPeriod):
    requestScan()

  override def shutdown(): Unit = synchronized:
    stopped = true
    pollTask.cancel()

  override def receiveMessage(msg: ServiceMessage): Unit =
    try
      EventLogMessage.decode(msg.encodedContent) match
        case Nudge => requestScan()
        case ReloadObservers => ()
    catch
      case err: Throwable => logger.warn(s"EventLogDurableService: undecodable service message: $err")

  /** Starts a scan, or marks the running one to scan again when it is done */
  private def requestScan(): Future[Unit] =
    val start = synchronized:
      if stopped then
        false
      else if scanning then
        rescan = true
        false
      else
        scanning = true
        true
    if start then runScans() else Future.unit

  private def runScans(): Future[Unit] =
    Future.delegate(scan(0)).recover:
      case err => logger.warn(s"EventLogDurableService scan failed: $err")
    .flatMap: _ =>
      val again = synchronized:
        if rescan && !stopped then
          rescan = false
          true
        else
          scanning = false
          false
      if again then runScans() else Future.unit

  /** Moves batches until incomingEvents is empty, nothing more can move, or too many attempts in a row failed */
  private def scan(failures: Int): Future[Unit] =
    if stopped then
      Future.unit
    else
      Future.delegate(moveBatch()).transformWith:
        case Success(BatchResult.Moved) => scan(0)
        case Success(_) => Future.unit
        case Failure(err) =>
          if failures + 1 >= MaxConsecutiveFailures then
            logger.warn(s"EventLogDurableService: $MaxConsecutiveFailures move attempts failed in a row, last: $err")
            Future.unit
          else
            logger.debug(s"EventLogDurableService: move attempt failed, retrying: $err")
            scan(failures + 1)

  private final case class Incoming(key: Key, revision: ObjectRevision, entry: Option[EventLogEntry])

  /** Reads up to one batch of incomingEvents, in key order */
  private def readBatch(): Future[Vector[Incoming]] =
    val batch = Vector.newBuilder[Incoming]
    var count = 0
    var bytes = 0L
    incoming.foreach(
      (_, key, vs) =>
        count += 1
        bytes += vs.value.bytes.length
        val entry =
          try Some(EventLogEntry.decode(vs.value.bytes))
          catch
            case err: Throwable =>
              logger.error(s"EventLogDurableService: deleting undecodable incoming entry $key: $err")
              None
        batch += Incoming(key, vs.revision, entry)
        Future.unit,
      () => count >= MaxBatchEntries || bytes >= MaxBatchBytes
    ).map(_ => batch.result())

  private def firstFullKey(logged: DataObjectTKVL): Future[Option[HLCTimestamp]] =
    var first: Option[Key] = None
    logged.foreach(
      (k, _) =>
        if first.isEmpty then first = Some(k)
        Future.unit,
      () => first.isDefined
    ).map(_ => first.map(LogKey.decode))

  private def moveBatch(): Future[BatchResult] =
    for
      kvos <- client.read(statePointer, "event log service state")
      batch <- readBatch()
      result <- if batch.isEmpty then Future.successful(BatchResult.Empty) else moveRead(kvos, batch)
    yield result

  private def moveRead(kvos: KeyValueObjectState, batch: Vector[Incoming]): Future[BatchResult] =
    def value(k: Key): ValueState = kvos.contents(k)
    val typeIds = EventLogState.decodeTypeIds(value(EventLogState.TypeIdsKey).value.bytes)
    val tails = EventLogState.decodeTails(value(EventLogState.TailsKey).value.bytes)
    val tailFloor = EventLogState.decodeTailFloor(value(EventLogState.TailFloorKey).value.bytes)
    val allocatorId = EventLogState.decodeAllocator(value(EventLogState.AllocatorKey).value.bytes)

    // DroppedEvent always has an id before any batch is planned, so drop replacements can name it
    val needed = (batch.flatMap(_.entry.map(_.typeUUID)) :+ DroppedEvent.TypeUUID).distinct.filterNot(typeIds.contains)
    val next = if typeIds.isEmpty then 0 else typeIds.values.map(_ & 0xFFFF).max + 1
    if next + needed.size - 1 > MaxTypeId then
      logger.error(s"EventLogDurableService: event type ids exhausted (${typeIds.size} assigned, " +
        s"${needed.size} more needed); no entries are moved")
      Future.successful(BatchResult.Stuck)
    else
      val ids: EventLogState.TypeIds = typeIds ++ needed.zipWithIndex.map((u, i) => u -> (next + i).toShort)
      val uuidOf: Map[Short, UUID] = ids.map(_.swap)
      val droppedId = ids(DroppedEvent.TypeUUID)

      val keyOf = new IdentityHashMap[LoggedRecord, Incoming]()
      val records = batch.flatMap: in =>
        in.entry.map: e =>
          val r = LoggedRecord(e.timestamp, ids(e.typeUUID), e.data)
          keyOf.put(r, in)
          r
      val undecodable = batch.filter(_.entry.isEmpty)

      def dropReplacement(r: LoggedRecord): LoggedRecord =
        val d = DroppedEvent.entryFor(EventLogEntry(r.timestamp, uuidOf(r.typeId), r.data),
          DropReason.ExceededMaximumLogEntrySize)
        LoggedRecord(d.timestamp, droppedId, d.data)

      for
        loaded <- Future.traverse(tails.pointers)(LogWriter.load(client, _))
        logged <- loggedRoot.getDataObjectTree()
        first <- firstFullKey(logged)
        allocator <- client.getAllocator(allocatorId)
        result <-
          val cap = allocator.maxObjectSize
          val plan = LogPlanner.plan(PlanInput(
            batch = records,
            tails = loaded.zipWithIndex.map((t, i) => TailView(i, t.length, t.maxSize)),
            current = tails.current,
            tailRecords = None,
            tailFloor = tailFloor,
            firstFullKey = first,
            lateTarget = None,
            target = cap.fold(targetObjectSize)(math.min(targetObjectSize, _)),
            newObjectCap = cap,
            dropReplacement = dropReplacement))

          val moved = plan.consumed.map(keyOf.get) ++ undecodable
          if moved.isEmpty then
            logger.warn(s"EventLogDurableService: ${records.size} incoming entries cannot be placed")
            Future.successful(BatchResult.Stuck)
          else
            client.transact: tx =>
              given Transaction = tx
              LogWriter(plan, loaded)

              var reqs = List.empty[KeyValueUpdate.KeyRequirement]
              var ops = List.empty[Insert]
              if ids.size != typeIds.size then
                reqs ::= KeyValueUpdate.KeyRevision(EventLogState.TypeIdsKey, value(EventLogState.TypeIdsKey).revision)
                ops ::= Insert(EventLogState.TypeIdsKey, EventLogState.encodeTypeIds(ids))
              if plan.newCurrent != tails.current then
                reqs ::= KeyValueUpdate.KeyRevision(EventLogState.TailsKey, value(EventLogState.TailsKey).revision)
                ops ::= Insert(EventLogState.TailsKey,
                  EventLogState.encodeTails(tails.copy(current = plan.newCurrent)))
              if ops.nonEmpty then
                tx.update(statePointer, None, None, reqs, ops)

              incoming.prepareUpdates(Nil, Nil, moved.toList.map(in => in.key -> in.revision))
            .map(_ => BatchResult.Moved)
      yield result
