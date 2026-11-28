package org.aspen_ddp.aspen.common.eventlog

import org.aspen_ddp.aspen.client.{PoolObjectAllocatorId, Transaction}
import org.aspen_ddp.aspen.client.tkvl.{NodeAllocator, Root}
import org.aspen_ddp.aspen.common.{HLCTimestamp, Radicle}
import org.aspen_ddp.aspen.common.objects.{Insert, KeyValueObjectPointer}
import org.aspen_ddp.aspen.common.transaction.KeyValueUpdate

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Random

/** The public reader: range reads over full objects and tails that restart rather than omit when racing the service */
class EventLogReaderSuite extends EventLogSuiteBase:

  private val Target = 16 * 1024

  /** Timestamps step by Spacing, so late events can fall between logged ones */
  private val Spacing = 10L

  private val rnd = new Random(11)

  /** Incompressible, so record sizes and frame bounds are exact */
  private def data(n: Int): Array[Byte] =
    val a = new Array[Byte](n)
    rnd.nextBytes(a)
    a

  private def recordSize(n: Int): Int = LogRecords.recordSize(LoggedRecord(HLCTimestamp.now, 0, new Array[Byte](n)))

  /** Entries that fill the tails once (the next one that does not fit compacts them) */
  private def perRound: Int =
    val perTail = Iterator.from(1).takeWhile(n => LogRecords.maxFrameSize(n * recordSize(100)) <= Target / 10).max
    perTail * EventLogState.TailCount

  private final case class Logged(base: Long, round: Int, entries: Vector[EventLogEntry], objects: Vector[FullView])

  /** Two compactions' worth of full objects and half a round in the tails, the service left running */
  private def logged(): Future[Logged] =
    given ExecutionContext = executionContext
    val base = HLCTimestamp.now.asLong
    val round = perRound
    val entries = (0 until 2 * round + round / 2).toVector
      .map(i => EventLogEntry(HLCTimestamp(base + Spacing * i), typeA, data(100)))
    for
      ptr <- SystemEventLog.statePointer(client)
      _ <- logInGroups(entries, 25)
      _ = makeService(ptr, targetSize = Target)
      _ <- awaitIncomingEmpty()
      objs <- fullObjects()
      inTails <- tailEntries()
    yield
      objs.size shouldBe 2
      inTails should not be empty
      Logged(base, round, entries, objs)

  private def logTogether(es: Seq[EventLogEntry]): Future[Unit] =
    client.transact: tx =>
      given Transaction = tx
      tx.replaySafe("event log entries may be duplicated")
      client.prepareSystemEventLogEntries(es)

  /** Runs `f` the first time it is called only */
  private def once(f: => Future[Unit]): () => Future[Unit] =
    var ran = false
    () =>
      if ran then
        Future.unit
      else
        ran = true
        f

  atest("readSystemEvents returns logged events in timestamp order across full objects and tails"):
    given ExecutionContext = executionContext
    for
      l <- logged()
      all <- client.readSystemEvents()
      from = l.entries(l.round - 3).timestamp
      to = l.entries(2 * l.round + 2).timestamp
      ranged <- client.readSystemEvents(from, Some(to))
      fromOnly <- client.readSystemEvents(l.entries(2 * l.round + 1).timestamp)
      limited <- client.readSystemEvents(from, Some(to), limit = Some(7))
      crossing <- client.readSystemEvents(from, limit = Some(l.round + 5))
      first5 <- client.readSystemEvents(limit = Some(5))
      none <- client.readSystemEvents(limit = Some(0))
      // A from between two logged timestamps and a to before every one
      between <- client.readSystemEvents(HLCTimestamp(from.asLong + 1), Some(to))
      before <- client.readSystemEvents(to = Some(HLCTimestamp(l.base - 1)))
    yield
      all shouldBe l.entries
      ranged shouldBe l.entries.filter(e => e.timestamp >= from && e.timestamp <= to)
      fromOnly shouldBe l.entries.drop(2 * l.round + 1)
      limited shouldBe l.entries.filter(_.timestamp >= from).take(7)
      crossing shouldBe l.entries.filter(_.timestamp >= from).take(l.round + 5)
      first5 shouldBe l.entries.take(5)
      none shouldBe empty
      between shouldBe l.entries.filter(e => e.timestamp > from && e.timestamp <= to)
      before shouldBe empty

  atest("a reader that read the tree before a split restarts instead of omitting"):
    given ExecutionContext = executionContext
    for
      l <- logged()
      // Enough late events into the first object that it splits; its records move to a newly allocated object.
      // Of a type the log already knows, so the reader's typeIds cover them and only the split can restart it
      held = l.objects(0).records.size
      late = (0 until held).map(k => EventLogEntry(HLCTimestamp(l.base + Spacing * k + 5), typeA, data(100)))
      reader = new EventLogReader(client)
      _ = reader.testBeforeFirstObjectRead = once:
        for
          _ <- logTogether(late)
          _ <- awaitIncomingEmpty()
          objs <- fullObjects()
        yield
          objs.size should be > l.objects.size
          objs(0).state.pointer shouldBe l.objects(0).state.pointer
          objs(0).header.upperBound shouldBe defined
      result <- reader.read(EventLogReader.Earliest, None, None)
    yield
      result shouldBe (l.entries ++ late).sortBy(_.timestamp)
      reader.restarts shouldBe 1

  atest("a reader that read the tree before a rekey restarts instead of omitting"):
    given ExecutionContext = executionContext
    def fits(raw: Long): Boolean = FullObject.overhead + LogRecords.maxFrameSize(raw.toInt) <= Target
    for
      l <- logged()
      // A full object's worth of events before the first object: it is rekeyed to the oldest and rewritten with
      // them, and its old records move to a new object filed under its old key
      count = Iterator.from(1).takeWhile(n => fits(n.toLong * recordSize(100))).max
      late = (0 until count).map(k => EventLogEntry(HLCTimestamp(l.base - Spacing * (count - k)), typeA, data(100)))
      reader = new EventLogReader(client)
      _ = reader.testBeforeFirstObjectRead = once:
        for
          _ <- logTogether(late)
          _ <- awaitIncomingEmpty()
          objs <- fullObjects()
        yield
          objs.size shouldBe l.objects.size + 1
          LogKey.decode(objs(0).key) shouldBe late.head.timestamp
          objs(1).key shouldBe l.objects(0).key
          objs(1).state.pointer should not be l.objects(0).state.pointer
      result <- reader.read(EventLogReader.Earliest, None, None)
    yield
      result shouldBe (late ++ l.entries).sortBy(_.timestamp)
      reader.restarts shouldBe 1

  atest("a reader whose walk start is rekeyed away restarts instead of skipping the covering object"):
    given ExecutionContext = executionContext
    for
      l <- logged()
      // Inside the first object's range, so the walk starts at its key, which the rekey then deletes
      from = l.entries(5).timestamp
      late = EventLogEntry(HLCTimestamp(l.base - 5), typeA, data(10))
      reader = new EventLogReader(client)
      _ = reader.testAfterContainingNode = once:
        for
          _ <- logTogether(Seq(late))
          _ <- awaitIncomingEmpty()
          objs <- fullObjects()
        yield
          // Rekeyed without a split: same object, new key, the old key gone
          objs.size shouldBe l.objects.size
          LogKey.decode(objs(0).key) shouldBe late.timestamp
          objs(0).state.pointer shouldBe l.objects(0).state.pointer
      result <- reader.read(from, None, None)
    yield
      result shouldBe l.entries.filter(_.timestamp >= from)
      reader.restarts shouldBe 1

  atest("a reader racing compaction may duplicate but never omits"):
    given ExecutionContext = executionContext
    for
      l <- logged()
      tailed <- tailEntries()
      // A round more fills the tails, so the service compacts what the reader already read in them
      more = (0 until l.round).toVector.map: i =>
        EventLogEntry(HLCTimestamp(l.base + Spacing * (l.entries.size + i)), typeA, data(100))
      reader = new EventLogReader(client)
      _ = reader.testAfterTailsRead = once:
        for
          _ <- logInGroups(more, 25)
          _ <- awaitIncomingEmpty()
          objs <- fullObjects()
        yield objs.size should be > l.objects.size
      result <- reader.read(EventLogReader.Earliest, None, None)
    yield
      reader.restarts shouldBe 0
      result.map(_.timestamp) shouldBe result.map(_.timestamp).sorted
      val counts = multiset(result)
      // The tails' records were read there and again in the new full object; the rest once
      tailed.foreach(e => counts.getOrElse(e, 0) shouldBe 2)
      l.entries.filterNot(tailed.toSet).foreach(e => counts.getOrElse(e, 0) shouldBe 1)
      result.toSet.subsetOf((l.entries ++ more).toSet) shouldBe true

  atest("an entry moved from incomingEvents to a tail between those two reads is still returned"):
    given ExecutionContext = executionContext
    val base = HLCTimestamp.now.asLong
    val entries = (0 until 5).toVector.map(i => EventLogEntry(HLCTimestamp(base + Spacing * i), typeA, data(20)))
    for
      ptr <- SystemEventLog.statePointer(client)
      // No service runs yet, so the reader finds the entries in incomingEvents
      _ <- logInGroups(entries, 2)
      reader = new EventLogReader(client)
      _ = reader.testAfterIncomingRead = once:
        makeService(ptr)
        for
          _ <- awaitIncomingEmpty()
          tailed <- tailEntries()
        yield multiset(tailed) shouldBe multiset(entries)
      result <- reader.read(EventLogReader.Earliest, None, None)
    yield
      reader.restarts shouldBe 0
      // Read in incomingEvents, then again in a tail: a duplicate (DESIGN.md section 7, path 2), never a gap
      multiset(result) shouldBe entries.map(_ -> 2).toMap

  /** Gives the incomingEvents tree `size`-byte nodes, so a few entries span several of them */
  private def shrinkIncomingNodes(ptr: KeyValueObjectPointer, size: Int): Future[Unit] =
    given ExecutionContext = executionContext
    client.transact: tx =>
      client.read(ptr).map: kvos =>
        val vs = kvos.contents(EventLogState.IncomingKey)
        val root = Root(client, vs.value.bytes)
        val small = new NodeAllocator(client, List((PoolObjectAllocatorId(Radicle.poolId), size)))
        tx.update(ptr, None, None, List(KeyValueUpdate.KeyRevision(EventLogState.IncomingKey, vs.revision)),
          List(Insert(EventLogState.IncomingKey, root.copy(nodeAllocator = small).encode())))

  private def incomingNodes(ptr: KeyValueObjectPointer): Future[Int] =
    given ExecutionContext = executionContext
    SystemEventLog.incomingTree(client, ptr).foreachNode((_, _) => Future.unit).map(_.visited)

  atest("a reader walking incomingEvents while the service drains it, removing nodes, does not restart"):
    given ExecutionContext = executionContext
    val base = HLCTimestamp.now.asLong
    val entries = (0 until 40).toVector.map(i => EventLogEntry(HLCTimestamp(base + Spacing * i), typeA, data(100)))
    for
      ptr <- SystemEventLog.statePointer(client)
      _ <- shrinkIncomingNodes(ptr, 1024)
      _ <- logInGroups(entries, 5)
      before <- incomingNodes(ptr)
      reader = new EventLogReader(client)
      // After the walk's first entry, the service moves every entry and the emptied nodes are removed: the walk's
      // next node read fails with InvalidObject, and the walk resumes after the last key it visited
      _ = reader.testDuringIncomingWalk = once:
        makeService(ptr)
        for
          _ <- awaitIncomingEmpty()
          _ <- waitForTransactionsToComplete()
          _ <- awaitUntil("the emptied incomingEvents nodes to be removed")(incomingNodes(ptr).map(_ == 1))
        yield ()
      result <- reader.read(EventLogReader.Earliest, None, None)
    yield
      before should be > 2
      reader.restarts shouldBe 0
      entries.foreach(e => result should contain (e))
      result.toSet shouldBe entries.toSet

  atest("entries logged but not yet moved by the service are returned"):
    given ExecutionContext = executionContext
    val base = HLCTimestamp.now.asLong
    val entries = (0 until 5).toVector.map(i => EventLogEntry(HLCTimestamp(base + Spacing * i), typeA, data(20)))
    for
      // No service runs, so the entries stay in incomingEvents
      _ <- logInGroups(entries.reverse, 2)
      waiting <- incomingEntries()
      all <- client.readSystemEvents()
      ranged <- client.readSystemEvents(entries(1).timestamp, Some(entries(3).timestamp))
      limited <- client.readSystemEvents(limit = Some(2))
    yield
      multiset(waiting) shouldBe multiset(entries)
      all shouldBe entries
      ranged shouldBe entries.slice(1, 4)
      limited shouldBe entries.take(2)

  atest("a read from the default start includes timestamps below zero"):
    given ExecutionContext = executionContext
    val negative = EventLogEntry(HLCTimestamp(Long.MinValue + 1), typeA, data(20))
    for
      _ <- logInGroups(Seq(negative))
      all <- client.readSystemEvents()
    yield all shouldBe Vector(negative)

  atest("decodeSystemEvent returns None for an unregistered type and a DroppedEvent for its type"):
    val entry = EventLogEntry(HLCTimestamp.now, UUID.randomUUID(), Array[Byte](1, 2, 3))
    val dropped = DroppedEvent.entryFor(entry, DropReason.ExceededMaximumLogEntrySize)
    client.decodeSystemEvent(entry) shouldBe None
    Future.successful:
      client.decodeSystemEvent(dropped) shouldBe
        Some(DroppedEvent(entry.timestamp, entry.typeUUID, DropReason.ExceededMaximumLogEntrySize))
