package org.aspen_ddp.aspen.common.eventlog

import org.aspen_ddp.aspen.client.{AspenClient, ObjectAllocatorId, Transaction}
import org.aspen_ddp.aspen.client.tkvl.{KVObjectRootManager, Root, TKVLInsert, TieredKeyValueList}
import org.aspen_ddp.aspen.common.{HLCTimestamp, Radicle}
import org.aspen_ddp.aspen.common.objects.{DataObjectPointer, Key, KeyValueObjectPointer, Value}
import org.aspen_ddp.aspen.compute.ServiceEntry
import scribe.Logging

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

/** Client-side operations of the system event log, behind the `AspenClient` methods, and the Bootstrap state. */
object SystemEventLog extends Logging:

  /** The initial contents of the service's KV state object, written by Bootstrap. Every key is present, so readers
    * never handle an absent one.
    */
  private[aspen] def initialState(incoming: Root,
                                  logged: Root,
                                  tails: Vector[DataObjectPointer],
                                  allocator: ObjectAllocatorId): Map[Key, Array[Byte]] =
    Map(
      EventLogState.IncomingKey -> incoming.encode(),
      EventLogState.LoggedKey -> logged.encode(),
      EventLogState.TypeIdsKey -> EventLogState.encodeTypeIds(Map.empty),
      EventLogState.ObserversKey -> EventLogState.encodeObservers(Nil),
      EventLogState.AllocatorKey -> EventLogState.encodeAllocator(allocator),
      EventLogState.TailsKey -> EventLogState.encodeTails(EventLogState.Tails(tails, 0, allocator)),
      EventLogState.TailFloorKey -> EventLogState.encodeTailFloor(HLCTimestamp.Zero))

  // ServiceEntry.statePointer never changes, so it is looked up once per client; weak keys vanish with their client
  private val statePointers =
    java.util.Collections.synchronizedMap(new java.util.WeakHashMap[AspenClient, KeyValueObjectPointer]())

  /** The service state object's pointer, from the services tree */
  private[aspen] def statePointer(client: AspenClient): Future[KeyValueObjectPointer] =
    given ExecutionContext = client.clientContext
    Option(statePointers.get(client)) match
      case Some(ptr) => Future.successful(ptr)
      case None =>
        val tkvl = TieredKeyValueList(client, KVObjectRootManager(client, Radicle.ServicesTreeKey, Radicle.pointer))
        tkvl.get(Key(EventLogDurableService.ServiceUUID)).map:
          case Some(vs) =>
            val ptr = ServiceEntry.decode(vs.value.bytes).statePointer
            statePointers.put(client, ptr)
            ptr
          case None => throw new IllegalStateException("EventLogDurableService is not registered")

  private[aspen] def incomingTree(client: AspenClient, statePointer: KeyValueObjectPointer): TieredKeyValueList =
    TieredKeyValueList(client, KVObjectRootManager(client, EventLogState.IncomingKey, statePointer))

  def prepareEntries(client: AspenClient, es: Seq[EventLogEntry])(using tx: Transaction): Future[Unit] =
    given ExecutionContext = client.clientContext
    val inserts = es.toList.map(e => TKVLInsert(Key(UUID.randomUUID()), Value(e.encode()), requireDoesNotExist = true))
    tx.result.foreach(_ => nudge(client))
    statePointer(client).flatMap(ptr => incomingTree(client, ptr).prepareUpdates(inserts, Nil))

  def logSystemEvent(client: AspenClient, e: EventLogEntry): Future[Unit] =
    given ExecutionContext = client.clientContext
    client.transact: tx =>
      given Transaction = tx
      tx.replaySafe("event log entries may be duplicated")
      prepareEntries(client, List(e))

  /** Best-effort: the service's poll picks the entries up anyway */
  private def nudge(client: AspenClient): Unit =
    given ExecutionContext = client.clientContext
    try
      client.sendServiceMessage(EventLogDurableService.ServiceUUID, EventLogMessage.encode(Nudge)).failed.foreach:
        err => logger.debug(s"Event log nudge failed: $err")
    catch
      case err: Throwable => logger.debug(s"Event log nudge failed: $err")
