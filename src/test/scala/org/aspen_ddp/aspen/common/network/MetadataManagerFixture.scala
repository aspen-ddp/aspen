package org.aspen_ddp.aspen.common.network

import org.aspen_ddp.aspen.TestNetwork
import org.aspen_ddp.aspen.client.internal.network.Messenger as ClientMessenger
import org.aspen_ddp.aspen.common.{HLCTimestamp, Radicle}
import org.aspen_ddp.aspen.common.ida.Replication
import org.aspen_ddp.aspen.common.metadata.{BootstrapConfig, HostId, HostState, StorageDeviceId, StorageDeviceSetId, StoragePoolState}
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.util.EvictingQueue
import org.aspen_ddp.aspen.server.store.backend.RocksDBConfig
import org.scalatest.{BeforeAndAfterAll, Suite}

import java.util.UUID
import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future, Promise}

/** An AspenClient whose lookups never resolve on their own: it records what it was asked
 *  about and hands back a Promise the test completes when it chooses.
 *
 *  The recording is what makes "this call started no lookup" observable without reaching into
 *  MetadataManager's private state -- getHostStateAndTimestamp and getStoragePoolStateAndTimestamp are the only things
 *  startHostLookup and startPoolLookup do that a caller can see.
 *
 *  ExecutionContext.parasitic runs the continuation inline rather than on a pool thread: on the
 *  thread that completes the Promise, or on the registering thread if the Promise is already
 *  complete. Either way the MetadataManager callback that installs (or, on failure, drops) the
 *  entry has finished by the time the test's next line runs, which removes all waiting.
 *
 *  Lock-ordering invariant: never complete a promise this class vends -- lookupPromise,
 *  poolLookupPromise or bootstrapConfigPromise -- while holding this object's monitor. Because
 *  parasitic runs the MetadataManager continuation inline on the completing thread, doing so
 *  would take the manager's lock while holding the client's lock, inverting the established
 *  order (manager → client) and risking deadlock.
 *
 *  Everything else is inherited from TestNetwork.TClient purely so this file does not have to
 *  stub the whole AspenClient surface. No read, transaction or message ever leaves it: the
 *  messenger discards sends and the radicle pointer is never dereferenced.
 */
class LookupRecordingClient extends TestNetwork.TClient(
    ExecutionContext.parasitic,
    ClientMessenger.None,
    Radicle.pointer,
    Replication(1, 1)):

  /** Every hostId getHostStateAndTimestamp was called with, in call order. */
  val lookups: mutable.ListBuffer[HostId] = mutable.ListBuffer[HostId]()

  /** Every poolId getStoragePoolStateAndTimestamp was called with, in call order. */
  val poolLookups: mutable.ListBuffer[PoolId] = mutable.ListBuffer[PoolId]()

  private var promises: Map[HostId, Promise[HostState]] = Map()
  private var poolPromises: Map[PoolId, Promise[StoragePoolState]] = Map()
  private var lookupFailures: Map[HostId, Throwable] = Map()
  private var lookupTimestamps: Map[HostId, HLCTimestamp] = Map()
  private var poolLookupTimestamps: Map[PoolId, HLCTimestamp] = Map()
  private var poolLookupFailures: Map[PoolId, Throwable] = Map()
  private var bootstrapConfigAttempts: Int = 0
  private var bootstrapConfigPromises: Map[Int, Promise[String]] = Map()
  private var bootstrapConfigThrownAttempts: Set[Int] = Set()
  private var bootstrapConfigFailure: Option[Throwable] = None

  /** The Promise backing `hostId`'s lookup, created on first use. Callable before or after the
   *  lookup itself so a test can complete it either way round. */
  def lookupPromise(hostId: HostId): Promise[HostState] = synchronized:
    promises.get(hostId) match
      case Some(p) => p
      case None =>
        val p = Promise[HostState]()
        promises += hostId -> p
        p

  /** The Promise backing `poolId`'s lookup, created on first use. */
  def poolLookupPromise(poolId: PoolId): Promise[StoragePoolState] = synchronized:
    poolPromises.get(poolId) match
      case Some(p) => p
      case None =>
        val p = Promise[StoragePoolState]()
        poolPromises += poolId -> p
        p

  /** Makes getHostStateAndTimestamp throw `err` synchronously for `hostId` rather than returning a future.
   *
   *  The call is still recorded in `lookups` before the throw, which is what lets a test tell a
   *  retried lookup from a wedged one: both leave the host unresolved, and only the call count
   *  distinguishes them. */
  def failLookupWith(hostId: HostId, err: Throwable): Unit = synchronized:
    lookupFailures += hostId -> err

  def clearLookupFailure(hostId: HostId): Unit = synchronized:
    lookupFailures -= hostId

  /** Makes getStoragePoolStateAndTimestamp throw `err` synchronously for `poolId`. Recorded in `poolLookups`
   *  before the throw, for the same reason failLookupWith records first. */
  def failPoolLookupWith(poolId: PoolId, err: Throwable): Unit = synchronized:
    poolLookupFailures += poolId -> err

  def clearPoolLookupFailure(poolId: PoolId): Unit = synchronized:
    poolLookupFailures -= poolId

  /** The timestamp `hostId`'s lookup reports its host object was written at. Zero unless set. */
  def setLookupTimestamp(hostId: HostId, timestamp: HLCTimestamp): Unit = synchronized:
    lookupTimestamps += hostId -> timestamp

  /** The timestamp `poolId`'s lookup reports its pool object was written at. Zero unless set. */
  def setPoolLookupTimestamp(poolId: PoolId, timestamp: HLCTimestamp): Unit = synchronized:
    poolLookupTimestamps += poolId -> timestamp

  override def getHostStateAndTimestamp(hostId: HostId): Future[(HostState, HLCTimestamp)] = synchronized:
    lookups += hostId
    lookupFailures.get(hostId) match
      case Some(err) => throw err
      case None =>
        lookupPromise(hostId).future.map(state => (state, synchronized(lookupTimestamps.getOrElse(hostId, HLCTimestamp.Zero))))(
          using ExecutionContext.parasitic)

  override def getStoragePoolStateAndTimestamp(poolId: PoolId): Future[(StoragePoolState, HLCTimestamp)] = synchronized:
    poolLookups += poolId
    poolLookupFailures.get(poolId) match
      case Some(err) => throw err
      case None =>
        poolLookupPromise(poolId).future.map(state => (state, synchronized(poolLookupTimestamps.getOrElse(poolId, HLCTimestamp.Zero))))(
          using ExecutionContext.parasitic)

  /** How many times getBootstrapConfig has been called.
   *
   *  Counting attempts rather than inspecting refreshingBootstrapConfig is what makes guard
   *  release observable from outside the manager: a released guard lets the next
   *  dropStoreMapping start a second fetch, a wedged one does not.
   */
  def bootstrapConfigFetches: Int = synchronized:
    bootstrapConfigAttempts

  /** The Promise backing the nth fetch, 1-based, created on first use. Callable before or after
   *  the fetch itself so a test can complete it either way round.
   *
   *  Throws for an attempt that was answered by failBootstrapConfigWith rather than by a Future.
   *  That attempt has no promise and never will, so handing back a fresh one would give the test
   *  something it could complete to no effect -- a silent no-op that surfaces as an unrelated
   *  assertion failure much later.
   */
  def bootstrapConfigPromise(attempt: Int): Promise[String] = synchronized:
    if bootstrapConfigThrownAttempts.contains(attempt) then
      throw new IllegalArgumentException(
        s"bootstrap config fetch $attempt was armed to throw, so it has no promise")

    bootstrapConfigPromises.get(attempt) match
      case Some(p) => p
      case None =>
        val p = Promise[String]()
        bootstrapConfigPromises += attempt -> p
        p

  /** Makes getBootstrapConfig throw rather than return a Future. Still counts as an attempt --
   *  the manager did call it.
   *
   *  Unlike failLookupWith, whose throw startHostLookup absorbs, where this one ends up depends
   *  on which kind it is. refreshBootstrapConfig turns a NonFatal throw into its Failure branch:
   *  the guard is released, the throw is logged, and nothing escapes dropStoreMapping. A fatal
   *  one is released past rather than absorbed -- it propagates out of dropStoreMapping, so a
   *  test arming one has to intercept it.
   */
  def failBootstrapConfigWith(err: Throwable): Unit = synchronized:
    bootstrapConfigFailure = Some(err)

  def clearBootstrapConfigFailure(): Unit = synchronized:
    bootstrapConfigFailure = None

  override def getBootstrapConfig(): Future[String] = synchronized:
    bootstrapConfigAttempts += 1
    bootstrapConfigFailure match
      case Some(err) =>
        bootstrapConfigThrownAttempts += bootstrapConfigAttempts
        throw err
      case None => bootstrapConfigPromise(bootstrapConfigAttempts).future


/** A NetworkImplInterface that drains what MetadataManager hands it, the way ZMQNet does, and
 *  records where each message ended up.
 *
 *  Draining rather than peeking is deliberate: ZMQNet's createHostEntry and storeResolved both
 *  empty the EvictingQueue they are given before returning, so a double that left messages in
 *  place would not model the handoff it is here to observe.
 *
 *  Lock-ordering note: MetadataManager calls both methods while holding its own monitor -- the
 *  constructor path included, since loadInitialBootstrapConfig takes it -- so the recorder's
 *  monitor is never acquired before the manager's. Tests read the recordings without holding the
 *  manager's lock, which keeps that order intact.
 */
class RecordingNetworkImpl extends MetadataManager.NetworkImplInterface[MetadataManager.HostEntry]:

  private val storeResolutionsBuffer: mutable.ListBuffer[(HostId, StoreId)] = mutable.ListBuffer[(HostId, StoreId)]()

  private var delivered: Map[HostId, List[Message]] = Map()

  private var createFailures: Map[HostId, Throwable] = Map()

  private var storeResolvedFailures: Map[HostId, Throwable] = Map()

  /** Every (hostId, storeId) pair storeResolved was called with, in call order. */
  def resolutions: List[(HostId, StoreId)] = synchronized:
    storeResolutionsBuffer.toList

  /** Messages drained on `hostId`'s behalf, in arrival order. Empty if it received none. */
  def deliveredTo(hostId: HostId): List[Message] =
    synchronized:
      delivered.getOrElse(hostId, Nil)

  /** Makes createHostEntry throw for `hostId` -- after draining and recording, because that is
   *  where the real failure lives: ZMQNet's createHostEntry empties the queue and enqueues
   *  NewHostAvailable before reaching the wakeIoThread() call that can actually fail. A double
   *  that threw before draining would model a failure that cannot happen. */
  def throwOnCreateHostEntry(hostId: HostId, err: Throwable): Unit = synchronized:
    createFailures += hostId -> err

  /** Makes storeResolved throw for `hostId`, after draining and recording. ZMQNet's storeResolved
   *  empties the queue and enqueues ProcessPendingMessages before its wakeIoThread() call, which
   *  is the one that can fail -- on a socket closed by CLI teardown. */
  def throwOnStoreResolved(hostId: HostId, err: Throwable): Unit = synchronized:
    storeResolvedFailures += hostId -> err

  /** Caller holds this object's monitor. */
  private def drain(hostId: HostId, queuedMessages: EvictingQueue[Message]): Unit =
    val buf = mutable.ListBuffer[Message]()
    var omsg = queuedMessages.dequeue()
    while omsg.isDefined do
      omsg.foreach(buf.append)
      omsg = queuedMessages.dequeue()
    // Recording nothing for an empty queue keeps delivered's key set meaning "hosts that
    // actually received a message". MetadataManager's constructor hands createHostEntry an
    // empty queue for every bootstrap host, which would otherwise show up as a delivery.
    if buf.nonEmpty then
      delivered += hostId -> (delivered.getOrElse(hostId, Nil) ++ buf.toList)

  def createHostEntry(hostId: HostId,
                      name: String,
                      address: String,
                      dataPort: Int,
                      httpPort: Int,
                      storeTransferPort: Int,
                      queuedMessages: EvictingQueue[Message]): MetadataManager.HostEntry =
    synchronized:
      drain(hostId, queuedMessages)
      createFailures.get(hostId) match
        case Some(err) => throw err
        case None => ()
    new MetadataManager.HostEntry(hostId, name, address, dataPort, httpPort, storeTransferPort)

  def storeResolved(hostEntry: MetadataManager.HostEntry,
                    storeId: StoreId,
                    queuedMessages: EvictingQueue[Message]): Unit =
    synchronized:
      storeResolutionsBuffer += hostEntry.hostId -> storeId
      drain(hostEntry.hostId, queuedMessages)
      storeResolvedFailures.get(hostEntry.hostId) match
        case Some(err) => throw err
        case None => ()


/** An in-memory NetworkCache serving a preset load() and recording every write. */
class RecordingNetworkCache(preset: NetworkCacheContent = NetworkCacheContent(Nil, Nil)) extends NetworkCache:
  val storePuts: mutable.ListBuffer[CachedStore] = mutable.ListBuffer()
  val storeRemoves: mutable.ListBuffer[StoreId] = mutable.ListBuffer()
  val hostPuts: mutable.ListBuffer[CachedHost] = mutable.ListBuffer()
  val removedHosts: mutable.ListBuffer[HostId] = mutable.ListBuffer()
  val removedStores: mutable.ListBuffer[StoreId] = mutable.ListBuffer()
  val bulkUpdates: mutable.ListBuffer[(NetworkCacheContent, List[StoreId], List[HostId])] = mutable.ListBuffer()

  @volatile var throwOnWrite: Boolean = false

  private def check(): Unit =
    if throwOnWrite then throw new RuntimeException("cache write failed")

  def load(): NetworkCacheContent = preset
  def putStore(e: CachedStore): Unit = synchronized:
    storePuts += e
    check()
  def removeStore(storeId: StoreId): Unit = synchronized:
    storeRemoves += storeId
    check()
  def putHost(e: CachedHost): Unit = synchronized:
    hostPuts += e
    check()
  def removeHost(hostId: HostId): Unit = synchronized:
    removedHosts += hostId
    check()
  def bulkUpdate(content: NetworkCacheContent, removedStores: Iterable[StoreId] = Nil,
                 removedHosts: Iterable[HostId] = Nil): Unit = synchronized:
    bulkUpdates += ((content, removedStores.toList, removedHosts.toList))
  def close(): Unit = ()

  def writeCount: Int = synchronized(storePuts.size + storeRemoves.size + hostPuts.size)


/** A MetadataManager over a temp bootstrap config naming exactly one host, plus the ids of a
 *  host and a pool that config does not name -- reaching either requires a lookup. */
trait MetadataManagerFixture extends BeforeAndAfterAll:
  this: Suite =>

  protected val systemId: UUID = UUID.fromString("55555555-5555-5555-5555-555555555555")
  protected val poolId: PoolId = PoolId(UUID.fromString("66666666-6666-6666-6666-666666666666"))

  /** Present in the bootstrap config, so MetadataManager's constructor resolves it with no
   *  lookup at all. */
  protected val bootstrapHostId: HostId = HostId(UUID.fromString("77777777-7777-7777-7777-777777777777"))

  /** Absent from the bootstrap config: reaching it requires a lookup. */
  protected val remoteHostId: HostId = HostId(UUID.fromString("88888888-8888-8888-8888-888888888888"))

  /** Absent from the bootstrap config: reaching a store in it requires a pool lookup. */
  protected val unknownPoolId: PoolId = PoolId(UUID.fromString("99999999-9999-9999-9999-999999999999"))

  protected val remoteHostState: HostState =
    HostState(remoteHostId, "remote_host", "10.0.0.9", 6000, 6001, 6002, Set())

  /** A second host absent from the bootstrap config. Two distinct unknown hosts are what make it
   *  observable that each rescued store queue gets its own PendingHostLookup rather than sharing
   *  one across the pool's stores. */
  protected val otherHostId: HostId = HostId(UUID.fromString("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"))

  protected val otherHostState: HostState =
    HostState(otherHostId, "other_host", "10.0.0.10", 7000, 7001, 7002, Set())

  /** A StoragePoolState placing one store on each of `hostIds`, store index matching position.
   *
   *  Only the store entries matter, and each only for its hostId. MetadataManager reads
   *  poolState.stores and builds every StoreId from the pool id it was asked about, never from
   *  poolState.poolId -- so even that argument is filler, supplied for readability at the call
   *  site. Everything else is placeholder detail the manager never looks at.
   */
  protected def poolStateWith(targetPoolId: PoolId, hostIds: HostId*): StoragePoolState =
    val stores = hostIds.map: hostId =>
      StoragePoolState.StoreEntry(hostId, StorageDeviceId(UUID.randomUUID()))
    StoragePoolState(
      targetPoolId,
      "test_pool",
      Replication(hostIds.size, hostIds.size),
      None,
      stores.toArray,
      RocksDBConfig(),
      StorageDeviceSetId(UUID.randomUUID()))

  private var templateConfig: String = scala.compiletime.uninitialized

  override protected def beforeAll(): Unit =
    super.beforeAll()
    templateConfig = bootstrapConfigYaml(
      Replication(1, 1),
      List(HostState(bootstrapHostId, "bootstrap_host", "127.0.0.1", 5000, 5001, 5002, Set())),
      List(StoreId(poolId, 0.toByte) -> bootstrapHostId))

  /** A bootstrap config over the fixture's systemId.
   *
   *  BootstrapConfig.Config rejects a config whose total store count differs from the IDA width,
   *  so callers must keep `ida.width` equal to the number of entries in `storeMap`.
   */
  protected def bootstrapConfigYaml(ida: Replication,
                                    hostStates: List[HostState],
                                    storeMap: List[(StoreId, HostId)]): String =
    BootstrapConfig.generateBootstrapConfig(systemId, ida, hostStates, storeMap,
      org.aspen_ddp.aspen.TestTLS.caCertificatePem)

  /** newManager, plus a recorder holding the yaml of every onBootstrapConfigRefreshed call, in
   *  order.
   *
   *  The callback records before it runs `afterRecord`, so a test that makes the callback throw
   *  can still see that it was reached -- without that, a callback that was never invoked would
   *  pass a "throwing callback does no harm" test vacuously.
   */
  protected def newManagerWithRecorder(afterRecord: String => Unit = _ => ()):
      (MetadataManager[MetadataManager.HostEntry], LookupRecordingClient, RecordingNetworkImpl, mutable.ListBuffer[String]) =
    val recorded = mutable.ListBuffer[String]()
    val impl = new RecordingNetworkImpl
    val client = new LookupRecordingClient
    val mgr = new MetadataManager[MetadataManager.HostEntry](
      BootstrapConfig.parseBootstrapConfig(templateConfig),
      (yaml, _) =>
        recorded += yaml
        afterRecord(yaml),
      impl)
    mgr.setAspenClient(client)
    (mgr, client, impl, recorded)

  /** A MetadataManager over a bootstrap config naming exactly one host, wired to a fresh
   *  recording client and a fresh RecordingNetworkImpl. Returning the impl is what lets a suite
   *  assert that a message actually reached a host rather than merely that a lookup was started. */
  protected def newManager(): (MetadataManager[MetadataManager.HostEntry], LookupRecordingClient, RecordingNetworkImpl) =
    val (mgr, client, impl, _) = newManagerWithRecorder()
    (mgr, client, impl)

  /** newManager over `cache`, optionally naming this process's own host */
  protected def newManager(cache: NetworkCache, selfHostId: Option[HostId] = None):
      (MetadataManager[MetadataManager.HostEntry], LookupRecordingClient, RecordingNetworkImpl) =
    val impl = new RecordingNetworkImpl
    val client = new LookupRecordingClient
    val mgr = new MetadataManager[MetadataManager.HostEntry](
      BootstrapConfig.parseBootstrapConfig(templateConfig),
      (_, _) => (),
      impl,
      cache,
      selfHostId)
    mgr.setAspenClient(client)
    (mgr, client, impl)
