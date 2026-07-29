package org.aspen_ddp.aspen.common.network

import org.aspen_ddp.aspen.TestNetwork
import org.aspen_ddp.aspen.client.internal.network.Messenger as ClientMessenger
import org.aspen_ddp.aspen.common.Radicle
import org.aspen_ddp.aspen.common.ida.Replication
import org.aspen_ddp.aspen.common.metadata.{BootstrapConfig, HostId, HostState, StoragePoolState}
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.util.EvictingQueue
import org.scalatest.{BeforeAndAfterAll, Suite}

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import java.util.UUID
import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future, Promise}

/** An AspenClient whose lookups never resolve on their own: it records what it was asked
 *  about and hands back a Promise the test completes when it chooses.
 *
 *  The recording is what makes "this call started no lookup" observable without reaching into
 *  MetadataManager's private state -- getHostState and getStoragePoolState are the only things
 *  startHostLookup and startPoolLookup do that a caller can see.
 *
 *  ExecutionContext.parasitic runs the continuation inline rather than on a pool thread: on the
 *  thread that completes the Promise, or on the registering thread if the Promise is already
 *  complete. Either way the MetadataManager callback that installs (or, on failure, drops) the
 *  entry has finished by the time the test's next line runs, which removes all waiting.
 *
 *  Lock-ordering invariant: never complete a promise returned by lookupPromise or
 *  poolLookupPromise while holding this object's monitor. Because parasitic runs the
 *  MetadataManager continuation inline on the completing thread, doing so would take the
 *  manager's lock while holding the client's lock, inverting the established order
 *  (manager → client) and risking deadlock.
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

  /** Every hostId getHostState was called with, in call order. */
  val lookups: mutable.ListBuffer[HostId] = mutable.ListBuffer[HostId]()

  /** Every poolId getStoragePoolState was called with, in call order. */
  val poolLookups: mutable.ListBuffer[PoolId] = mutable.ListBuffer[PoolId]()

  private var promises: Map[HostId, Promise[HostState]] = Map()
  private var poolPromises: Map[PoolId, Promise[StoragePoolState]] = Map()

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

  override def getHostState(hostId: HostId): Future[HostState] = synchronized:
    lookups += hostId
    lookupPromise(hostId).future

  override def getStoragePoolState(poolId: PoolId): Future[StoragePoolState] = synchronized:
    poolLookups += poolId
    poolLookupPromise(poolId).future


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

  private var tempDir: Path = scala.compiletime.uninitialized
  private var bootstrapConfigFile: os.Path = scala.compiletime.uninitialized

  override protected def beforeAll(): Unit =
    super.beforeAll()
    tempDir = Files.createTempDirectory("aspen-metadata-manager")
    val f = tempDir.resolve("aspen-bootstrap-config.yaml")
    val yaml = BootstrapConfig.generateBootstrapConfig(
      systemId,
      Replication(1, 1),
      List(HostState(bootstrapHostId, "bootstrap_host", "127.0.0.1", 5000, 5001, 5002, Set())),
      List(StoreId(poolId, 0.toByte) -> bootstrapHostId))
    Files.write(f, yaml.getBytes(StandardCharsets.UTF_8))
    bootstrapConfigFile = os.Path(f)

  override protected def afterAll(): Unit =
    try os.remove.all(os.Path(tempDir))
    catch case _: Throwable => ()
    finally super.afterAll()

  /** A MetadataManager over the fixture's bootstrap config, wired to a fresh recording client.
   *  The NetworkImplInterface is the smallest thing that satisfies the type: it builds a plain
   *  HostEntry and ignores store resolution, neither of which the suites here touch. */
  protected def newManager(): (MetadataManager[MetadataManager.HostEntry], LookupRecordingClient) =
    val impl = new MetadataManager.NetworkImplInterface[MetadataManager.HostEntry]:
      def createHostEntry(hostId: HostId,
                          name: String,
                          address: String,
                          dataPort: Int,
                          cncPort: Int,
                          storeTransferPort: Int,
                          queuedMessages: EvictingQueue[Message]): MetadataManager.HostEntry =
        new MetadataManager.HostEntry(hostId, name, address, dataPort, cncPort, storeTransferPort)

      def storeResolved(hostEntry: MetadataManager.HostEntry,
                        storeId: StoreId,
                        queuedMessages: EvictingQueue[Message]): Unit = ()

    val client = new LookupRecordingClient
    val mgr = new MetadataManager[MetadataManager.HostEntry](bootstrapConfigFile, impl)
    mgr.setAspenClient(client)
    (mgr, client)
