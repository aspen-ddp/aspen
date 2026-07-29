package org.aspen_ddp.aspen.common.network

import org.aspen_ddp.aspen.TestNetwork
import org.aspen_ddp.aspen.client.internal.network.Messenger as ClientMessenger
import org.aspen_ddp.aspen.common.Radicle
import org.aspen_ddp.aspen.common.ida.Replication
import org.aspen_ddp.aspen.common.metadata.{BootstrapConfig, HostId, HostState}
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.util.EvictingQueue
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import java.util.UUID
import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future, Promise}

/** An AspenClient whose getHostState never resolves on its own: it records the host it was
 *  asked about and hands back a Promise the test completes when it chooses.
 *
 *  The recording is what makes "peekHostEntry starts no lookup" observable without reaching
 *  into MetadataManager's private state -- getHostState is the only thing startHostLookup does
 *  that a caller can see.
 *
 *  ExecutionContext.parasitic runs every continuation on the thread that completes the Promise,
 *  so the MetadataManager callback that installs (or, on failure, drops) the host entry has
 *  finished by the time success()/failure() returns. That removes all waiting from these tests.
 *
 *  Everything else is inherited from TestNetwork.TClient purely so this file does not have to
 *  stub the whole AspenClient surface. No read, transaction or message ever leaves it: the
 *  messenger discards sends and the radicle pointer is never dereferenced.
 */
private class LookupRecordingClient extends TestNetwork.TClient(
    ExecutionContext.parasitic,
    ClientMessenger.None,
    Radicle.pointer,
    Replication(1, 1)):

  /** Every hostId getHostState was called with, in call order. */
  val lookups: mutable.ListBuffer[HostId] = mutable.ListBuffer[HostId]()

  private var promises: Map[HostId, Promise[HostState]] = Map()

  /** The Promise backing `hostId`'s lookup, created on first use. Callable before or after the
   *  lookup itself so a test can complete it either way round. */
  def lookupPromise(hostId: HostId): Promise[HostState] = synchronized:
    promises.get(hostId) match
      case Some(p) => p
      case None =>
        val p = Promise[HostState]()
        promises += hostId -> p
        p

  override def getHostState(hostId: HostId): Future[HostState] = synchronized:
    lookups += hostId
    lookupPromise(hostId).future


class MetadataManagerPeekHostEntrySuite extends AnyFunSuite with Matchers with BeforeAndAfterAll:

  private val systemId = UUID.fromString("55555555-5555-5555-5555-555555555555")
  private val poolId = PoolId(UUID.fromString("66666666-6666-6666-6666-666666666666"))

  /** Present in the bootstrap config, so MetadataManager's constructor resolves it with no
   *  lookup at all. */
  private val bootstrapHostId = HostId(UUID.fromString("77777777-7777-7777-7777-777777777777"))

  /** Absent from the bootstrap config: reaching it requires a lookup. */
  private val remoteHostId = HostId(UUID.fromString("88888888-8888-8888-8888-888888888888"))

  private val remoteHostState =
    HostState(remoteHostId, "remote_host", "10.0.0.9", 6000, 6001, 6002, Set())

  private var tempDir: Path = scala.compiletime.uninitialized
  private var bootstrapConfigFile: os.Path = scala.compiletime.uninitialized

  override def beforeAll(): Unit =
    tempDir = Files.createTempDirectory("aspen-metadata-manager")
    val f = tempDir.resolve("aspen-bootstrap-config.yaml")
    val yaml = BootstrapConfig.generateBootstrapConfig(
      systemId,
      Replication(1, 1),
      List(HostState(bootstrapHostId, "bootstrap_host", "127.0.0.1", 5000, 5001, 5002, Set())),
      List(StoreId(poolId, 0.toByte) -> bootstrapHostId))
    Files.write(f, yaml.getBytes(StandardCharsets.UTF_8))
    bootstrapConfigFile = os.Path(f)

  override def afterAll(): Unit =
    try os.remove.all(os.Path(tempDir))
    catch case _: Throwable => ()

  /** A MetadataManager over the suite's bootstrap config, wired to a fresh recording client.
   *  The NetworkImplInterface is the smallest thing that satisfies the type: it builds a plain
   *  HostEntry and ignores store resolution, neither of which peekHostEntry touches. */
  private def newManager(): (MetadataManager[MetadataManager.HostEntry], LookupRecordingClient) =
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

  test("peekHostEntry returns a bootstrap host without starting a lookup"):
    val (mgr, client) = newManager()

    mgr.peekHostEntry(bootstrapHostId).map(_.name) should be(Some("bootstrap_host"))
    client.lookups.toList should be(empty)

  test("peekHostEntry returns None for an unknown host and starts no lookup"):
    val (mgr, client) = newManager()

    mgr.peekHostEntry(remoteHostId) should be(None)
    mgr.peekHostEntry(remoteHostId) should be(None)
    client.lookups.toList should be(empty)

    // The contrast that gives the assertion above its meaning: getHostEntry answers None just
    // the same, but does it by starting the lookup peekHostEntry deliberately does not.
    mgr.getHostEntry(remoteHostId) should be(None)
    client.lookups.toList should be(List(remoteHostId))

  test("peekHostEntry returns None while a lookup is pending"):
    val (mgr, client) = newManager()

    mgr.getHostEntry(remoteHostId) should be(None)
    // The Promise is deliberately left uncompleted, so the entry is still Left(PendingHostLookup).
    mgr.peekHostEntry(remoteHostId) should be(None)

    // And peeking at a pending entry does not start a second lookup either.
    client.lookups.toList should be(List(remoteHostId))

  test("peekHostEntry returns the entry once the lookup resolves"):
    val (mgr, client) = newManager()

    mgr.getHostEntry(remoteHostId) should be(None)
    client.lookupPromise(remoteHostId).success(remoteHostState)

    val entry = mgr.peekHostEntry(remoteHostId)
    entry.map(_.hostId) should be(Some(remoteHostId))
    entry.map(_.name) should be(Some("remote_host"))
    entry.map(_.address) should be(Some("10.0.0.9"))
    client.lookups.toList should be(List(remoteHostId))

  test("a failed lookup returns the host to the never-looked-up state"):
    val (mgr, client) = newManager()

    mgr.getHostEntry(remoteHostId) should be(None)
    client.lookupPromise(remoteHostId).failure(new NoSuchElementException("no such host"))

    // peekHostEntry's scaladoc rests on this: a failure drops the entry rather than leaving it
    // pending, so a poll-only caller keeps seeing None and has to time out.
    mgr.peekHostEntry(remoteHostId) should be(None)

    // Dropped rather than remembered as failed, so the next getHostEntry starts a fresh lookup.
    mgr.getHostEntry(remoteHostId) should be(None)
    client.lookups.toList should be(List(remoteHostId, remoteHostId))
