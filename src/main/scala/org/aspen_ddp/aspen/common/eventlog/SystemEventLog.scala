package org.aspen_ddp.aspen.common.eventlog

import org.aspen_ddp.aspen.client.{AspenClient, ObjectAllocatorId, ObjectReader, Transaction}
import org.aspen_ddp.aspen.client.tkvl.{KVObjectRootManager, NodeAllocator, Root, TKVLInsert, TieredKeyValueList,
  TreeKind}
import org.aspen_ddp.aspen.common.{HLCTimestamp, Radicle}
import org.aspen_ddp.aspen.common.eventlog.EventLogState.ObserverConfig
import org.aspen_ddp.aspen.common.objects.{DataObjectPointer, Insert, Key, KeyValueObjectPointer, Value}
import org.aspen_ddp.aspen.common.transaction.KeyValueUpdate
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

  /** The service state object's pointer, from the services tree, read with `readWith` (the client when None) when not cached */
  private[aspen] def statePointer(client: AspenClient,
                                  readWith: Option[ObjectReader] = None): Future[KeyValueObjectPointer] =
    given ExecutionContext = client.clientContext
    Option(statePointers.get(client)) match
      case Some(ptr) => Future.successful(ptr)
      case None =>
        val tkvl = TieredKeyValueList(client,
          KVObjectRootManager(client, Radicle.ServicesTreeKey, Radicle.pointer, readWith = readWith))
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

  private def readObservers(client: AspenClient)(using ExecutionContext) =
    for
      ptr <- statePointer(client)
      kvos <- client.read(ptr, "event log observers")
      vs = kvos.contents(EventLogState.ObserversKey)
    yield (ptr, vs.revision, EventLogState.decodeObservers(vs.value.bytes))

  def listObservers(client: AspenClient): Future[List[ObserverConfig]] =
    given ExecutionContext = client.clientContext
    readObservers(client).map(_._3)

  /** Rewrites the observer list with `change`. An unchanged list commits no transaction. The write requires the
    * revision read, so concurrent changes retry rather than overwrite each other.
    */
  private def modifyObservers(client: AspenClient, change: List[ObserverConfig] => List[ObserverConfig]): Future[Unit] =
    given ExecutionContext = client.clientContext
    readObservers(client).flatMap: (_, _, current) =>
      if change(current) == current then
        Future.unit
      else
        client.transactUntilSuccessful: tx =>
          readObservers(client).map: (ptr, revision, latest) =>
            // When a concurrent change made this one redundant the unchanged list is rewritten, still revision
            // checked: the write is this transaction's pin
            tx.update(ptr, None, None, List(KeyValueUpdate.KeyRevision(EventLogState.ObserversKey, revision)),
              List(Insert(EventLogState.ObserversKey, EventLogState.encodeObservers(change(latest)))))
        .map(_ => reloadObservers(client))

  def addObserver(client: AspenClient, factoryType: UUID, config: Array[Byte]): Future[Unit] =
    if client.typeRegistry.getType[EventObserverFactory](factoryType).isEmpty then
      Future.failed(new IllegalArgumentException(s"No EventObserverFactory registered for $factoryType"))
    else
      val o = ObserverConfig(factoryType, config)
      modifyObservers(client, l => if l.contains(o) then l else l :+ o)

  def removeObserver(client: AspenClient, factoryType: UUID, config: Array[Byte]): Future[Unit] =
    val o = ObserverConfig(factoryType, config)
    modifyObservers(client, _.filterNot(_ == o))

  /** Points the log at allocator `id`: it must exist (`getAllocator`). One transaction rewrites the state's
    * AllocatorKey and, in the loggedEvents Root under LoggedKey, only the NodeAllocator: every tier now allocates from
    * `id`, at the existing tier-0 node size, lowered to `id`'s node size limit when that is smaller. The tree's nodes
    * stay where they are; only new ones go to `id`. Both keys are revision checked (the pin). New full objects come
    * from `id` from the service's next batch on; the tails move at its next compaction. When `id`'s object size cap
    * is smaller, that compaction turns tail records larger than it into DroppedEvents (DESIGN.md section 8).
    */
  def setAllocator(client: AspenClient, id: ObjectAllocatorId): Future[Unit] =
    given ExecutionContext = client.clientContext
    for
      allocator <- client.getAllocator(id)
      ptr <- statePointer(client)
      _ <- client.transactUntilSuccessful: tx =>
             for
               kvos <- client.read(ptr, "event log state")
               loggedVs = kvos.contents(EventLogState.LoggedKey)
               root = Root(client, loggedVs.value.bytes)
               size = NodeAllocator.fittingNodeSize(root.nodeAllocator.maxNodeSize(0),
                        NodeAllocator.nodeSizeLimit(allocator, TreeKind.DataObjects))
               nodeAllocator = new NodeAllocator(client, List((id, size)))
               _ <- nodeAllocator.validate(TreeKind.DataObjects)
             yield
               val allocatorVs = kvos.contents(EventLogState.AllocatorKey)
               tx.update(ptr, None, None,
                 List(KeyValueUpdate.KeyRevision(EventLogState.LoggedKey, loggedVs.revision),
                   KeyValueUpdate.KeyRevision(EventLogState.AllocatorKey, allocatorVs.revision)),
                 List(Insert(EventLogState.LoggedKey, root.copy(nodeAllocator = nodeAllocator).encode()),
                   Insert(EventLogState.AllocatorKey, EventLogState.encodeAllocator(id))))
    yield nudge(client)

  /** Best-effort: the service's poll covers a lost message */
  private def reloadObservers(client: AspenClient): Unit =
    given ExecutionContext = client.clientContext
    try
      client.sendServiceMessage(EventLogDurableService.ServiceUUID, EventLogMessage.encode(ReloadObservers)).failed
        .foreach(err => logger.debug(s"Event log observer reload message failed: $err"))
    catch
      case err: Throwable => logger.debug(s"Event log observer reload message failed: $err")

  /** Best-effort: the service's poll picks the entries up anyway */
  private def nudge(client: AspenClient): Unit =
    given ExecutionContext = client.clientContext
    try
      client.sendServiceMessage(EventLogDurableService.ServiceUUID, EventLogMessage.encode(Nudge)).failed.foreach:
        err => logger.debug(s"Event log nudge failed: $err")
    catch
      case err: Throwable => logger.debug(s"Event log nudge failed: $err")
