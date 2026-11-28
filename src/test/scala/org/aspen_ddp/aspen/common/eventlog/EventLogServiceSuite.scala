package org.aspen_ddp.aspen.common.eventlog

import org.aspen_ddp.aspen.{IntegrationTestSuite, PinAudit}
import org.aspen_ddp.aspen.client.{RegisteredTypeFactory, Transaction}
import org.aspen_ddp.aspen.client.tkvl.{KVObjectRootManager, TreeKind}
import org.aspen_ddp.aspen.common.HLCTimestamp
import org.aspen_ddp.aspen.common.metadata.HostId
import org.aspen_ddp.aspen.common.network.ServiceMessage
import org.aspen_ddp.aspen.common.objects.{Key, KeyValueObjectPointer}
import org.aspen_ddp.aspen.compute.impl.SimpleDurableServiceExecutor

import java.util.UUID
import scala.concurrent.duration.{Duration, HOURS, MILLISECONDS}
import scala.concurrent.{ExecutionContext, Future, Promise}

class EventLogServiceSuite extends IntegrationTestSuite:

  override def userTypeFactories: List[RegisteredTypeFactory] = List(EventLogDurableService)

  protected val TargetSize: Int = 64 * 1024
  protected val PollPeriod: Duration = Duration(100, MILLISECONDS)

  private var executors: List[SimpleDurableServiceExecutor] = Nil
  private var services: List[EventLogDurableService] = Nil

  // Runs on failure and timeout too, so a failed test cannot leave an executor or service running
  override def subFixtureSetup(): Unit =
    executors = Nil
    services = Nil
    EventLogDurableService.setTestPollPeriod(client, PollPeriod)
    EventLogDurableService.setTestTargetObjectSize(client, TargetSize)

  override def subFixtureTeardown(): Unit =
    executors.foreach(_.shutdown())
    services.foreach(_.shutdown())
    executors = Nil
    services = Nil

  protected def makeExecutor(): SimpleDurableServiceExecutor =
    val exec = new SimpleDurableServiceExecutor(
      client, HostId.BootstrapHostId, client.backgroundTaskManager,
      leaseDuration        = Duration(30000, MILLISECONDS),
      renewalInterval      = Duration(1000, MILLISECONDS),
      minScanInterval      = Duration(10, MILLISECONDS),
      maxScanInterval      = Duration(20, MILLISECONDS),
      claimDelayPerService = Duration(0, MILLISECONDS))
    executors ::= exec
    exec

  protected def makeService(ptr: KeyValueObjectPointer, pollPeriod: Duration = PollPeriod): EventLogDurableService =
    val s = new EventLogDurableService(client, ptr, pollPeriod, TargetSize)
    services ::= s
    s

  private val typeA = UUID.randomUUID()
  private val typeB = UUID.randomUUID()
  private val typeC = UUID.randomUUID()

  /** Distinct timestamps, so the order records take in a frame is fully determined */
  protected def entry(i: Int, t: UUID = typeA): EventLogEntry =
    EventLogEntry(HLCTimestamp(HLCTimestamp.now.asLong + i), t, s"entry-$i-${UUID.randomUUID()}".getBytes)

  protected final case class StateView(typeIds: EventLogState.TypeIds,
                                       tails: EventLogState.Tails,
                                       tailFloor: HLCTimestamp)

  protected def readState(): Future[StateView] =
    given ExecutionContext = executionContext
    for
      ptr <- SystemEventLog.statePointer(client)
      kvos <- client.read(ptr)
    yield
      def bytes(k: Key) = kvos.contents(k).value.bytes
      StateView(
        EventLogState.decodeTypeIds(bytes(EventLogState.TypeIdsKey)),
        EventLogState.decodeTails(bytes(EventLogState.TailsKey)),
        EventLogState.decodeTailFloor(bytes(EventLogState.TailFloorKey)))

  protected def incomingEntries(): Future[Vector[EventLogEntry]] =
    given ExecutionContext = executionContext
    for
      ptr <- SystemEventLog.statePointer(client)
      all <- SystemEventLog.incomingTree(client, ptr).foldLeft(Vector.empty[EventLogEntry]): (acc, contents) =>
        acc ++ contents.values.map(vs => EventLogEntry.decode(vs.value.bytes))
    yield all

  /** Every entry held by the tails, type ids expanded */
  protected def tailEntries(): Future[Vector[EventLogEntry]] =
    given ExecutionContext = executionContext
    for
      st <- readState()
      contents <- Future.traverse(st.tails.pointers)(p => client.read(p))
    yield
      val uuidOf = st.typeIds.map(_.swap)
      contents.flatMap(c => LogRecords.decodeFrames(c.data.getByteArray))
        .map(r => EventLogEntry(r.timestamp, uuidOf(r.typeId), r.data))

  protected def multiset(es: Seq[EventLogEntry]): Map[EventLogEntry, Int] = es.groupBy(identity).map((e, v) => e -> v.size)

  /** Poll `cond` until it holds. Fails the test after `timeout` */
  protected def awaitUntil(what: String, timeout: Duration = Duration(60000, MILLISECONDS))
                          (cond: => Future[Boolean]): Future[Unit] =
    given ExecutionContext = executionContext
    val deadline = System.nanoTime() + timeout.toNanos
    def loop(): Future[Unit] =
      cond.flatMap: ok =>
        if ok then Future.unit
        else if System.nanoTime() > deadline then Future.failed(new AssertionError(s"timed out waiting for: $what"))
        else
          val p = Promise[Unit]()
          client.backgroundTaskManager.schedule(Duration(20, MILLISECONDS))(p.success(()))
          p.future.flatMap(_ => loop())
    loop()

  protected def awaitIncomingEmpty(): Future[Unit] =
    given ExecutionContext = executionContext
    awaitUntil("incomingEvents to empty")(incomingEntries().map(_.isEmpty))

  atest("Bootstrap registers the event log with empty state"):
    given ExecutionContext = executionContext
    for
      ptr <- SystemEventLog.statePointer(client)
      st <- readState()
      incoming <- incomingEntries()
      loggedRoot <- KVObjectRootManager(client, EventLogState.LoggedKey, ptr).getRoot()
      tails <- Future.traverse(st.tails.pointers)(p => client.read(p))
    yield
      st.tails.pointers.size shouldBe EventLogState.TailCount
      st.tails.current shouldBe 0
      st.typeIds shouldBe empty
      st.tailFloor shouldBe HLCTimestamp.Zero
      incoming shouldBe empty
      loggedRoot.kind shouldBe TreeKind.DataObjects
      loggedRoot.orootObject shouldBe None
      tails.map(_.size) shouldBe Vector.fill(EventLogState.TailCount)(0)

  atest("logged events reach the tails and leave incomingEvents"):
    given ExecutionContext = executionContext
    makeExecutor()
    val types = Vector(typeA, typeB, typeC)
    val entries = (0 until 50).map(i => entry(i, types(i % 3)))
    val audit = new PinAudit(client)
    for
      _ <- entries.foldLeft(Future.unit)((f, e) => f.flatMap(_ => client.logSystemEvent(e)))
      _ <- awaitIncomingEmpty()
      _ = audit.stop()
      logged <- tailEntries()
      st <- readState()
    yield
      multiset(logged) shouldBe multiset(entries)
      audit.replaySafeWith("event log entries may be duplicated").size shouldBe entries.size
      audit.unpinned shouldBe empty
      st.typeIds.keySet shouldBe Set(typeA, typeB, typeC, DroppedEvent.TypeUUID)
      st.typeIds.values.map(_ & 0xFFFF).toSet shouldBe Set(0, 1, 2, 3)

  atest("prepareSystemEventLogEntries inserts several entries in one transaction"):
    given ExecutionContext = executionContext
    val entries = (0 until 5).map(i => entry(i))
    val audit = new PinAudit(client)
    for
      _ <- client.transact: tx =>
             given Transaction = tx
             tx.replaySafe("event log entries may be duplicated")
             client.prepareSystemEventLogEntries(entries)
      _ = audit.stop()
      incoming <- incomingEntries()
    yield
      audit.replaySafeWith("event log entries may be duplicated").size shouldBe 1
      multiset(incoming) shouldBe multiset(entries)

  atest("a nudge moves entries without waiting for the poll"):
    given ExecutionContext = executionContext
    EventLogDurableService.setTestPollPeriod(client, Duration(1, HOURS))
    val exec = makeExecutor()
    var delivered = 0
    // TestNetwork captures host messages instead of delivering them, so forward the service's nudges by hand
    def deliverNudges(): Unit =
      val nudges = net.capturedHostMessages.collect:
        case m: ServiceMessage if m.serviceUUID == EventLogDurableService.ServiceUUID => m
      nudges.drop(delivered).foreach(exec.deliverMessage)
      delivered = nudges.size
    def moved(e: EventLogEntry): Future[Unit] =
      awaitUntil("the entry to reach a tail"):
        deliverNudges()
        tailEntries().map(_.contains(e))
    val first = entry(1)
    val second = entry(2)
    for
      _ <- awaitUntil("the event log service to be claimed"):
             client.getServiceHost(EventLogDurableService.ServiceUUID).map(_.isDefined)
      // The service scans once when it starts; the second entry is logged after that scan moved the first one, so
      // only its nudge can move it within the hour-long poll period
      _ <- client.logSystemEvent(first)
      _ <- moved(first)
      _ <- client.logSystemEvent(second)
      _ <- moved(second)
      incoming <- incomingEntries()
    yield
      delivered should be >= 2
      incoming shouldBe empty

  atest("two overlapping service instances never lose or double-move an entry"):
    given ExecutionContext = executionContext
    val entries = (0 until 200).map(i => entry(i, if i % 2 == 0 then typeA else typeB))
    def logGroup(group: Seq[EventLogEntry]): Future[Unit] =
      client.transact: tx =>
        given Transaction = tx
        tx.replaySafe("event log entries may be duplicated")
        client.prepareSystemEventLogEntries(group)
    for
      ptr <- SystemEventLog.statePointer(client)
      _ = makeService(ptr)
      _ = makeService(ptr)
      _ <- entries.grouped(10).foldLeft(Future.unit)((f, g) => f.flatMap(_ => logGroup(g)))
      _ <- awaitIncomingEmpty()
      logged <- tailEntries()
      st <- readState()
    yield
      multiset(logged) shouldBe multiset(entries)
      st.typeIds.keySet shouldBe Set(typeA, typeB, DroppedEvent.TypeUUID)
