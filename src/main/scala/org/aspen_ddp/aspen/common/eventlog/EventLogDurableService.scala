package org.aspen_ddp.aspen.common.eventlog

import org.aspen_ddp.aspen.client.{AspenClient, KeyValueObjectState, Transaction}
import org.aspen_ddp.aspen.client.KeyValueObjectState.ValueState
import org.aspen_ddp.aspen.client.tkvl.{DataObjectTKVL, KVObjectRootManager, TieredKeyValueList}
import org.aspen_ddp.aspen.common.HLCTimestamp
import org.aspen_ddp.aspen.common.network.ServiceMessage
import org.aspen_ddp.aspen.common.objects.{DataObjectPointer, Insert, Key, KeyValueObjectPointer, ObjectRevision,
  Value}
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
  * A batch goes into the tails. When no tail has room for one of its records, the same transaction compacts: every
  * tail's records are sorted into new full objects inserted into loggedEvents, the tails emptied (and refilled with
  * the rest of the batch) and tailFloor advanced. An entry too large for any log object is replaced by a
  * DroppedEvent with its timestamp.
  *
  * With full objects present, a record at or below tailFloor is late. The batch's oldest late record picks the full
  * object covering it (filed under the largest key at or below it, else the first), which is read and rewritten in
  * the same transaction with every late record of the batch in its range merged in: split at timestamp boundaries
  * when it outgrows the target (the new parts allocated and inserted into loggedEvents), rekeyed when a record
  * precedes the first object. Late records outside that range wait for a later batch.
  *
  * Every move transaction deletes the moved incoming keys at their read KeyRevision (its pin) and checks the tails and
  * the state keys it writes, so two instances overlapping after a lease handoff never both commit the same entry: the
  * loser aborts and its next batch re-reads. Scans run one at a time; a nudge arriving during a scan makes it scan
  * again once done.
  *
  * Each batch reads AllocatorKey: new full objects come from the allocator it names. Tails allocated from another
  * (Tails.allocator) are replaced at the next compaction: new tails from the current allocator take the compaction's
  * refill, and the old ones are refcount-decremented in the same transaction.
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
  // Rebuilt whenever LoggedKey's revision changes, so the root's NodeAllocator, which setSystemLogAllocator rewrites,
  // is never used from a stale cache. Only moveBatch's scan uses it, one at a time.
  private var loggedRoot = KVObjectRootManager(client, EventLogState.LoggedKey, statePointer)
  private var loggedRootRevision: Option[ObjectRevision] = None

  private def loggedRootFor(kvos: KeyValueObjectState): KVObjectRootManager =
    val revision = kvos.contents(EventLogState.LoggedKey).revision
    if !loggedRootRevision.contains(revision) then
      loggedRoot = KVObjectRootManager(client, EventLogState.LoggedKey, statePointer)
      loggedRootRevision = Some(revision)
    loggedRoot

  private var scanning = false
  private var rescan = false
  private var pollTask: ScheduledTask = NoTask
  @volatile private var stopped = false

  // The observers are loaded before the first scan, so events it moves are observed
  private var loadedRevision: Option[ObjectRevision] = None
  private var loadedTimestamp: Option[HLCTimestamp] = None
  private var observers: List[EventObserver] = Nil

  loadObservers().onComplete(_ => requestScan())
  pollTask = client.backgroundTaskManager.scheduleNonConcurrentPollingTask("event-log-move", pollPeriod):
    loadObservers()
    requestScan()

  override def shutdown(): Unit = synchronized:
    stopped = true
    pollTask.cancel()

  override def receiveMessage(msg: ServiceMessage): Unit =
    try
      EventLogMessage.decode(msg.encodedContent) match
        case Nudge => requestScan()
        case ReloadObservers => loadObservers()
    catch
      case err: Throwable => logger.warn(s"EventLogDurableService: undecodable service message: $err")

  /** Reads the state object and rebuilds the observers when ObserversKey's revision is not the one last loaded */
  private def loadObservers(): Future[Unit] =
    Future.delegate(client.read(statePointer, "event log observers")).map: kvos =>
      applyObservers(kvos)
    .recover:
      case err => logger.warn(s"EventLogDurableService: observer reload failed: $err")

  /** Replaces the observer set with the one in `kvos` unless an equal or newer one is installed. Observer lists are
    * ordered by the write timestamp of ObserversKey's value, so a read that started earlier but finishes later
    * (a poll racing a batch) never replaces a newer set. The check, build and swap are one critical section, so
    * factories run once per installed set.
    */
  private def applyObservers(kvos: KeyValueObjectState): Unit = synchronized:
    val vs = kvos.contents(EventLogState.ObserversKey)
    if loadedTimestamp.forall(_ < vs.timestamp) then
      val built = EventLogState.decodeObservers(vs.value.bytes).flatMap: o =>
        client.typeRegistry.getType[EventObserverFactory](o.factoryType) match
          case None =>
            logger.warn(s"EventLogDurableService: no EventObserverFactory registered for ${o.factoryType}; skipped")
            None
          case Some(f) =>
            try Some(f.createEventObserver(o.factoryType, o.config))
            catch
              case err: Throwable =>
                logger.warn(s"EventLogDurableService: observer of ${o.factoryType} could not be created: $err")
                None
      loadedTimestamp = Some(vs.timestamp)
      loadedRevision = Some(vs.revision)
      observers = built

  /** The revision of ObserversKey's value that the installed observers were built from */
  private[aspen] def loadedObserversRevision: Option[ObjectRevision] = synchronized(loadedRevision)

  /** Calls the observers of each event moved by a committed transaction, in timestamp order. Observers must be fast;
    * an exception from one is logged and does not affect the others or the log.
    */
  private def deliver(entries: Vector[EventLogEntry]): Unit =
    val current = synchronized(observers)
    if current.nonEmpty then
      entries.sortBy(_.timestamp).foreach: entry =>
        try
          client.decodeSystemEvent(entry).foreach: event =>
            current.foreach: o =>
              if o.observedTypes.forall(_.contains(event.typeUUID)) then
                try o.observe(event)
                catch
                  case err: Throwable => logger.warn(s"EventLogDurableService: observer failed: $err")
        catch
          case err: Throwable => logger.warn(s"EventLogDurableService: event could not be decoded: $err")

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

  /** Loads the full object covering ts: the one filed under the largest key at or below ts, else the first.
    *
    * Late records sit near tailFloor, at the end of an ever-growing tree, so the search starts from the tier-0 node
    * containing ts: from its largest key at or below ts, walking on only to read the following key (one node, or
    * two when that key is in the next node). Only when that node holds no key at or below ts (ts before every key,
    * or the node's minimum entry deleted by a rekey) does it walk from the start of the tree.
    */
  private def loadCovering(logged: DataObjectTKVL, ts: HLCTimestamp): Future[LoadedFull] =
    var covering: Option[(HLCTimestamp, DataObjectPointer)] = None
    var next: Option[HLCTimestamp] = None
    def visit(k: Key, v: Value): Future[Unit] =
      if next.isEmpty then
        val key = LogKey.decode(k)
        if covering.isEmpty || key <= ts then
          covering = Some(key -> DataObjectPointer(v.bytes))
        else
          next = Some(key)
      Future.unit
    for
      onode <- logged.getContainingNode(LogKey(ts))
      floor = onode.flatMap(_.entries.map(_._1).takeWhile(k => LogKey.decode(k) <= ts).lastOption)
      _ <- logged.foreachFrom(floor.getOrElse(Key.AbsoluteMinimum), visit, () => next.isDefined)
      full <- covering match
        case Some((key, ptr)) => LogWriter.loadFull(client, key, ptr, next)
        case None => Future.failed(new IllegalStateException("loggedEvents is empty, so no record can be late"))
    yield full

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
    // Tails from an older allocator move to the current one at the next compaction
    val migrateTails = tails.allocator != allocatorId

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

      def planWith(loaded: Vector[LoadedTail],
                   tailRecords: Option[Vector[LoggedRecord]],
                   first: Option[HLCTimestamp],
                   late: Option[LoadedFull],
                   cap: Option[Int]): LogPlan =
        // While the tails await migration, each tail's cap is also the new pool's: a compacting plan refills new
        // tails from the current allocator, a plan that does not appends to the old ones, and the plan may be either
        def tailCap(t: LoadedTail): Option[Int] =
          if migrateTails then (t.maxSize.toList ++ cap.toList).minOption else t.maxSize
        LogPlanner.plan(PlanInput(
          batch = records,
          tails = loaded.zipWithIndex.map((t, i) => TailView(i, t.length, tailCap(t))),
          current = tails.current,
          tailRecords = tailRecords,
          tailFloor = tailFloor,
          firstFullKey = first,
          lateTarget = late.map(_.view),
          target = cap.fold(targetObjectSize)(math.min(targetObjectSize, _)),
          newObjectCap = cap,
          dropReplacement = dropReplacement))

      /** The plan from the tails' lengths alone. When an in-order record is left over, no tail had room for it, so
        * the tails are read whole and the batch planned again, now able to compact them.
        */
      def planBatch(first: Option[HLCTimestamp],
                    late: Option[LoadedFull],
                    cap: Option[Int]): Future[(LogPlan, Vector[LoadedTail])] =
        for
          loaded <- Future.traverse(tails.pointers)(LogWriter.load(client, _))
          plan = planWith(loaded, None, first, late, cap)
          inOrderLeft = plan.consumed.size < records.size && {
            val taken = java.util.Collections.newSetFromMap(new IdentityHashMap[LoggedRecord, java.lang.Boolean]())
            plan.consumed.foreach(taken.add)
            records.exists(r => !taken.contains(r) && (first.isEmpty || r.timestamp > tailFloor))
          }
          result <-
            if !inOrderLeft then
              Future.successful((plan, loaded))
            else
              Future.traverse(tails.pointers)(LogWriter.loadWhole(client, _)).map: whole =>
                val tailRecords = whole.flatMap(t => LogRecords.decodeFrames(t.state.data.getByteArray))
                (planWith(whole, Some(tailRecords), first, late, cap), whole)
        yield result

      for
        logged <- loggedRootFor(kvos).getDataObjectTree()
        first <- firstFullKey(logged)
        // The covering object of the oldest late record; the plan merges every late record in its range
        oldestLate = first.flatMap(_ => records.iterator.map(_.timestamp).filter(_ <= tailFloor).minOption)
        late <- oldestLate.fold(Future.successful(Option.empty[LoadedFull])):
                  ts => loadCovering(logged, ts).map(Some(_))
        allocator <- client.getAllocator(allocatorId)
        (plan, loaded) <- planBatch(first, late, allocator.maxObjectSize)
        result <-
          val moved = plan.consumed.map(keyOf.get) ++ undecodable
          if moved.isEmpty then
            logger.warn(s"EventLogDurableService: ${records.size} incoming entries cannot be placed")
            Future.successful(BatchResult.Stuck)
          else
            val compaction = plan.steps.collectFirst { case c: LogStep.Compact => c }
            client.transact: tx =>
              given Transaction = tx

              // Every move checks TailsKey, so an instance holding stale state cannot append after another advanced
              // `current` or compacted; its transaction aborts and its next batch re-reads. It checks TailFloorKey
              // too, since that decides which records are late and where they go
              val tailsReq = KeyValueUpdate.KeyRevision(EventLogState.TailsKey, value(EventLogState.TailsKey).revision)
              val floorReq =
                KeyValueUpdate.KeyRevision(EventLogState.TailFloorKey, value(EventLogState.TailFloorKey).revision)
              var reqs = List[KeyValueUpdate.KeyRequirement](tailsReq, floorReq)
              var ops = List.empty[Insert]
              if ids.size != typeIds.size then
                reqs ::= KeyValueUpdate.KeyRevision(EventLogState.TypeIdsKey, value(EventLogState.TypeIdsKey).revision)
                ops ::= Insert(EventLogState.TypeIdsKey, EventLogState.encodeTypeIds(ids))
              compaction.foreach: c =>
                ops ::= Insert(EventLogState.TailFloorKey, EventLogState.encodeTailFloor(c.newTailFloor))

              for
                newTails <- LogWriter(plan, loaded, late, allocator, logged, migrateTails)
                _ <- incoming.prepareUpdates(Nil, Nil, moved.toList.map(in => in.key -> in.revision))
              yield
                val nextTails = newTails match
                  case Some(pointers) => Some(EventLogState.Tails(pointers, plan.newCurrent, allocatorId))
                  case None if plan.newCurrent != tails.current || compaction.isDefined =>
                    Some(tails.copy(current = plan.newCurrent))
                  case None => None
                nextTails.foreach: t =>
                  ops ::= Insert(EventLogState.TailsKey, EventLogState.encodeTails(t))
                tx.update(statePointer, None, None, reqs, ops)
            .map: _ =>
              // The observer list read with this batch's state is current for events moved by it
              try
                applyObservers(kvos)
                val drops = new IdentityHashMap[LoggedRecord, java.lang.Boolean]()
                plan.steps.foreach:
                  case LogStep.Drop(r, placed) => drops.put(r, placed)
                  case _ =>
                deliver(plan.consumed.flatMap: r =>
                  val placed = drops.get(r)
                  if placed == null then
                    Some(EventLogEntry(r.timestamp, uuidOf(r.typeId), r.data))
                  else if placed.booleanValue then
                    val d = dropReplacement(r)
                    Some(EventLogEntry(d.timestamp, uuidOf(d.typeId), d.data))
                  else
                    None)
              catch
                case err: Throwable => logger.warn(s"EventLogDurableService: observer delivery failed: $err")
              BatchResult.Moved
      yield result
