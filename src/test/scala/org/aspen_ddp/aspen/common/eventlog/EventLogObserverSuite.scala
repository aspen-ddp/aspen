package org.aspen_ddp.aspen.common.eventlog

import org.aspen_ddp.aspen.client.{RegisteredTypeFactory, Transaction}
import org.aspen_ddp.aspen.common.HLCTimestamp
import org.aspen_ddp.aspen.common.objects.{Insert, ObjectRevision}
import org.aspen_ddp.aspen.common.transaction.KeyValueUpdate

import java.util.UUID
import java.util.concurrent.{ConcurrentHashMap, CopyOnWriteArrayList}
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters.*

/** Event types and an observer factory recording what it sees, per config, for the observer suite */
object ObserverTestTypes:
  val EvA: UUID = UUID.fromString("0e1a4c52-83a7-4c3e-9d10-6b2f5a7c1001")
  val EvB: UUID = UUID.fromString("0e1a4c52-83a7-4c3e-9d10-6b2f5a7c1002")

  /** Config (as a string) to the events its observers saw, in call order */
  val seen = new ConcurrentHashMap[String, CopyOnWriteArrayList[Event]]()
  def seenBy(config: String): Vector[Event] =
    Option(seen.get(config)).map(_.asScala.toVector).getOrElse(Vector.empty)

  final case class TestEvent(timestamp: HLCTimestamp, typeUUID: UUID) extends Event

  class TestEventFactory(val typeUUID: UUID) extends EventFactory:
    def createEvent(e: EventLogEntry): Event = TestEvent(e.timestamp, e.typeUUID)

  object FactoryA extends TestEventFactory(EvA)
  object FactoryB extends TestEventFactory(EvB)

  /** Config: "<name>|<mode>" with mode A (observes EvA only), ALL (None) or THROW (all, throws) */
  object ObserverFactory extends EventObserverFactory:
    val typeUUID: UUID = UUID.fromString("0e1a4c52-83a7-4c3e-9d10-6b2f5a7c1003")
    def createEventObserver(t: UUID, configData: Array[Byte]): EventObserver =
      val config = new String(configData)
      val mode = config.split('|')(1)
      val buffer = seen.computeIfAbsent(config, _ => new CopyOnWriteArrayList[Event]())
      new EventObserver:
        def observedTypes: Option[Set[UUID]] = if mode == "A" then Some(Set(EvA)) else None
        def observe(e: Event): Unit =
          buffer.add(e)
          if mode == "THROW" then throw new RuntimeException("observer failure")

class EventLogObserverSuite extends EventLogSuiteBase:
  import ObserverTestTypes.*

  override def userTypeFactories: List[RegisteredTypeFactory] =
    List(EventLogDurableService, FactoryA, FactoryB, ObserverFactory)

  private def config(mode: String): String = s"${UUID.randomUUID()}|$mode"

  private def add(c: String): Future[Unit] = client.addSystemEventObserver(ObserverFactory.typeUUID, c.getBytes)

  private def ev(i: Int, t: UUID): EventLogEntry =
    EventLogEntry(HLCTimestamp(HLCTimestamp.now.asLong + i), t, Array[Byte](i.toByte))

  private def awaitSeen(c: String, n: Int): Future[Unit] =
    awaitUntil(s"$n events seen by $c")(Future.successful(seenBy(c).size >= n))

  atest("an added observer sees events of its observed types only"):
    given ExecutionContext = executionContext
    val c = config("A")
    val es = Vector(ev(0, EvA), ev(1, EvB), ev(2, EvA), ev(3, EvB))
    for
      _ <- add(c)
      ptr <- SystemEventLog.statePointer(client)
      _ <- logInGroups(es)
      _ = makeService(ptr)
      _ <- awaitIncomingEmpty()
      _ <- awaitSeen(c, 2)
    yield seenBy(c).map(_.timestamp) shouldBe Vector(es(0).timestamp, es(2).timestamp)

  atest("an observer with observedTypes None sees all"):
    given ExecutionContext = executionContext
    val c = config("ALL")
    val es = Vector(ev(0, EvA), ev(1, EvB), ev(2, EvA))
    for
      _ <- add(c)
      ptr <- SystemEventLog.statePointer(client)
      _ <- logInGroups(es)
      _ = makeService(ptr)
      _ <- awaitSeen(c, 3)
    yield seenBy(c).map(_.timestamp) shouldBe es.map(_.timestamp)

  atest("removing an observer stops delivery"):
    given ExecutionContext = executionContext
    val c = config("ALL")
    val first = ev(0, EvA)
    val second = ev(1, EvA)
    for
      ptr <- SystemEventLog.statePointer(client)
      _ <- add(c)
      _ <- add(c)
      listed <- client.listSystemEventObservers()
      _ = makeService(ptr)
      _ <- logInGroups(Vector(first))
      _ <- awaitSeen(c, 1)
      _ <- client.removeSystemEventObserver(ObserverFactory.typeUUID, c.getBytes)
      _ <- client.removeSystemEventObserver(ObserverFactory.typeUUID, c.getBytes)
      after <- client.listSystemEventObservers()
      _ <- logInGroups(Vector(second))
      _ <- awaitUntil("second event moved")(tailEntries().map(_.exists(_.timestamp == second.timestamp)))
    yield
      listed shouldBe List(EventLogState.ObserverConfig(ObserverFactory.typeUUID, c.getBytes))
      after shouldBe empty
      seenBy(c).map(_.timestamp) shouldBe Vector(first.timestamp)

  atest("a lost reload message is covered by the poll"):
    given ExecutionContext = executionContext
    val c = config("ALL")
    for
      ptr <- SystemEventLog.statePointer(client)
      _ = makeService(ptr)
      kvos <- client.read(ptr)
      rev = kvos.contents(EventLogState.ObserversKey).revision
      // Written directly: no reload message is sent
      _ <- client.transact: tx =>
             tx.update(ptr, None, None, List(KeyValueUpdate.KeyRevision(EventLogState.ObserversKey, rev)),
               List(Insert(EventLogState.ObserversKey, EventLogState.encodeObservers(
                 List(EventLogState.ObserverConfig(ObserverFactory.typeUUID, c.getBytes))))))
             Future.unit
      // An observer active after a poll observes events moved afterwards
      _ <- logInGroups(Vector(ev(0, EvA)))
      _ <- awaitSeen(c, 1)
    yield succeed

  atest("adding an unregistered factory type fails"):
    given ExecutionContext = executionContext
    client.addSystemEventObserver(UUID.randomUUID(), Array[Byte](1)).failed.map: err =>
      err shouldBe a[IllegalArgumentException]

  atest("an observer that throws does not stop the log or other observers"):
    given ExecutionContext = executionContext
    val bad = config("THROW")
    val good = config("ALL")
    val es = Vector(ev(0, EvA), ev(1, EvB), ev(2, EvA))
    for
      _ <- add(bad)
      _ <- add(good)
      ptr <- SystemEventLog.statePointer(client)
      _ <- logInGroups(es)
      _ = makeService(ptr)
      _ <- awaitIncomingEmpty()
      _ <- awaitSeen(good, 3)
      logged <- tailEntries()
    yield
      seenBy(good).map(_.timestamp) shouldBe es.map(_.timestamp)
      seenBy(bad).size shouldBe 3
      logged.map(_.timestamp).toSet shouldBe es.map(_.timestamp).toSet
