package org.aspen_ddp.aspen.common.network.snapshot

import org.aspen_ddp.aspen.client.{AspenClient, KeyValueObjectState, Transaction}
import org.aspen_ddp.aspen.client.tkvl.{KVObjectRootManager, TieredKeyValueList}
import org.aspen_ddp.aspen.common.{DataBuffer, HLCTimestamp, Radicle}
import org.aspen_ddp.aspen.common.network.{CachedHost, CachedStore, ServiceMessage}
import org.aspen_ddp.aspen.common.objects.{DataObjectPointer, Insert, Key, KeyValueObjectPointer}
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.transaction.KeyValueUpdate
import org.aspen_ddp.aspen.common.util.BackgroundTaskManager.ScheduledTask
import org.aspen_ddp.aspen.compute.{DurableService, DurableServiceFactory, ServiceEntry}
import scribe.Logging

import java.nio.ByteBuffer
import java.util.UUID
import scala.concurrent.duration.{Duration, FiniteDuration, HOURS, MINUTES}
import scala.concurrent.{ExecutionContext, Future}

/** Periodically writes two zstd compressed DataObjects listing every host endpoint and every
  * store->host mapping. Nodes read them to catch up on network changes they never heard about.
  */
object NetworkCachingService extends DurableServiceFactory with Logging:

  val ServiceTypeUUID: UUID = UUID.fromString("3e8a5c71-94d2-4b06-a7f3-c15d20b9e684")
  val ServiceUUID: UUID     = UUID.fromString("a62f0d93-7b4e-4c18-8d55-0e9b3f71c2a8")

  val typeUUID: UUID = ServiceTypeUUID

  val HostSnapshotKey: Key  = Key(Array[Byte](0))
  val StoreSnapshotKey: Key = Key(Array[Byte](1))
  val LastRunKey: Key       = Key(Array[Byte](2))

  val DefaultRunPeriod: Duration  = Duration(24, HOURS)
  val DefaultPollPeriod: Duration = Duration(10, MINUTES)

  /** Test seam: per-client (poll, run) period overrides. Weak-keyed by client for the same
   *  reason as RebalancingDurableService's: suites share a JVM and a static would leak. */
  private val testPeriods =
    java.util.Collections.synchronizedMap(new java.util.WeakHashMap[AspenClient, (Duration, Duration)]())

  private[aspen] def setTestPeriods(client: AspenClient, poll: Duration, run: Duration): Unit =
    testPeriods.put(client, (poll, run))

  private def periodsFor(client: AspenClient): (Duration, Duration) =
    Option(testPeriods.get(client)).getOrElse((DefaultPollPeriod, DefaultRunPeriod))

  override def createService(client: AspenClient,
                             statePointer: KeyValueObjectPointer,
                             state: KeyValueObjectState): DurableService =
    val (poll, run) = periodsFor(client)
    new NetworkCachingService(client, statePointer, poll, run)

  def encodeLastRun(ts: HLCTimestamp): Array[Byte] =
    ByteBuffer.allocate(8).putLong(ts.asLong).array()

  def decodeLastRun(bytes: Array[Byte]): HLCTimestamp =
    HLCTimestamp(ByteBuffer.wrap(bytes).getLong())

  /** The initial contents of the service's KV state object, written by Bootstrap. */
  def initialServiceState(hostSnapshot: DataObjectPointer,
                          storeSnapshot: DataObjectPointer): Map[Key, Array[Byte]] =
    Map(
      HostSnapshotKey -> hostSnapshot.toArray,
      StoreSnapshotKey -> storeSnapshot.toArray,
      LastRunKey -> encodeLastRun(HLCTimestamp.Zero)
    )

  def emptyHostSnapshot: Array[Byte] =
    NetworkSnapshot.encodeHosts(HostSnapshot(HLCTimestamp.Zero, Nil))

  def emptyStoreSnapshot: Array[Byte] =
    NetworkSnapshot.encodeStores(StoreSnapshot(HLCTimestamp.Zero, Nil))

  /** The service state object's pointer via the services TKVL, or None if no entry exists. */
  private def findServiceStatePointer(client: AspenClient): Future[Option[KeyValueObjectPointer]] =
    given ExecutionContext = client.clientContext
    val tkvl = TieredKeyValueList(client, KVObjectRootManager(client, Radicle.ServicesTreeKey, Radicle.pointer))
    tkvl.get(Key(ServiceUUID)).map:
      _.map(vs => ServiceEntry.decode(vs.value.bytes).statePointer)

  /** The most recently written snapshots, or None when the services tree has no entry for this
   *  service. */
  def readSnapshots(client: AspenClient): Future[Option[(HostSnapshot, StoreSnapshot)]] =
    given ExecutionContext = client.clientContext
    findServiceStatePointer(client).flatMap:
      case None => Future.successful(None)
      case Some(statePtr) =>
        for
          kvos <- client.read(statePtr)
          hosts <- client.read(DataObjectPointer(kvos.contents(HostSnapshotKey).value.bytes))
          stores <- client.read(DataObjectPointer(kvos.contents(StoreSnapshotKey).value.bytes))
        yield
          Some((NetworkSnapshot.decodeHosts(hosts.data.getByteArray),
                NetworkSnapshot.decodeStores(stores.data.getByteArray)))


class NetworkCachingService(val client: AspenClient,
                            val statePointer: KeyValueObjectPointer,
                            pollPeriod: Duration,
                            runPeriod: Duration) extends DurableService with Logging:

  import NetworkCachingService.*

  private given ExecutionContext = client.clientContext

  @volatile private var stopped = false

  // stallAfter is an hour because a run reads every host and pool sequentially, which on a large
  // system may legitimately take far longer than a poll period.
  private val pollTask: ScheduledTask =
    client.backgroundTaskManager.scheduleNonConcurrentPollingTask(
      "network-caching-poll", pollPeriod, stallAfter = FiniteDuration(1, HOURS)):
      poll()

  override def shutdown(): Unit = synchronized:
    stopped = true
    pollTask.cancel()

  override def receiveMessage(msg: ServiceMessage): Unit = ()

  private def poll(): Future[Unit] =
    if stopped then Future.unit
    else
      client.read(statePointer)
        .flatMap: kvos =>
          val lastRun = decodeLastRun(kvos.contents(LastRunKey).value.bytes)
          // A lastRun in the future (clock skew) yields a negative difference and simply
          // defers the run until wall time catches up.
          if (HLCTimestamp.now - lastRun) >= runPeriod then run() else Future.unit
        .recover:
          case err => logger.warn(s"NetworkCachingService run failed: $err")

  /** Scan, then write. Any failed read aborts the run without writing anything: a partial
   *  snapshot stamped with a scanStart would let readers treat missing entries as authoritative. */
  private def run(): Future[Unit] =
    val scanStart = HLCTimestamp.now
    for
      hosts <- scanHosts()
      stores <- scanStores()
      _ <- if stopped then Future.unit else write(scanStart, hosts, stores)
    yield ()

  private def sequentially[A, B](items: List[A])(fn: A => Future[B]): Future[List[B]] =
    items.foldLeft(Future.successful(List.empty[B])): (acc, item) =>
      acc.flatMap(done => fn(item).map(b => b :: done))
    .map(_.reverse)

  private def scanHosts(): Future[List[CachedHost]] =
    for
      entries <- client.listHosts()
      hosts <- sequentially(entries): (_, hostId) =>
                 client.getHostStateAndTimestamp(hostId).map: (hs, ts) =>
                   CachedHost(hs.hostId, hs.name, hs.address, hs.dataPort, hs.httpPort,
                              hs.storeTransferPort, ts)
    yield hosts

  private def scanStores(): Future[List[CachedStore]] =
    for
      entries <- client.listStoragePools()
      perPool <- sequentially(entries): (_, poolId) =>
                   client.getStoragePoolStateAndTimestamp(poolId).map: (ps, ts) =>
                     ps.stores.toList.zipWithIndex.map: (entry, index) =>
                       CachedStore(StoreId(poolId, index.toByte), entry.hostId, ts)
    yield perPool.flatten

  /** Overwrite both snapshot objects and record the run in a single transaction. The objects are
   *  overwritten in place so their pointers, held in the state object, never change. */
  private def write(scanStart: HLCTimestamp,
                    hosts: List[CachedHost],
                    stores: List[CachedStore]): Future[Unit] =
    val hostBytes = NetworkSnapshot.encodeHosts(HostSnapshot(scanStart, hosts))
    val storeBytes = NetworkSnapshot.encodeStores(StoreSnapshot(scanStart, stores))
    client.transactUntilSuccessful: tx =>
      given Transaction = tx
      for
        kvos <- client.read(statePointer)
        hostPtr = DataObjectPointer(kvos.contents(HostSnapshotKey).value.bytes)
        storePtr = DataObjectPointer(kvos.contents(StoreSnapshotKey).value.bytes)
        hostDos <- client.read(hostPtr)
        storeDos <- client.read(storePtr)
      yield
        tx.overwrite(hostPtr, hostDos.revision, DataBuffer(hostBytes))
        tx.overwrite(storePtr, storeDos.revision, DataBuffer(storeBytes))
        val reqs = KeyValueUpdate.KeyRevision(LastRunKey, kvos.contents(LastRunKey).revision) :: Nil
        val ops = Insert(LastRunKey, encodeLastRun(scanStart)) :: Nil
        tx.update(statePointer, None, None, reqs, ops)
