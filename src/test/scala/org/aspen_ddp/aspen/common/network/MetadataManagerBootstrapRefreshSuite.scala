package org.aspen_ddp.aspen.common.network

import org.aspen_ddp.aspen.common.ida.Replication
import org.aspen_ddp.aspen.common.metadata.StorageDeviceId
import org.aspen_ddp.aspen.common.store.StoreId
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.util.UUID

/** What a bootstrap config refresh delivers, and what it costs when it goes wrong.
 *
 *  refreshingBootstrapConfig is a plain Boolean, so a refresh that ends without clearing it
 *  disables every later refresh for the life of the process. dropStoreMapping routes bootstrap
 *  stores to refreshBootstrapConfig and non-bootstrap stores to `stores -= storeId`, so a wedged
 *  flag does not degrade to the fallback -- it drops the correction entirely.
 *
 *  The flag is private and ExecutionContext.parasitic reports a throw from an onComplete callback
 *  rather than rethrowing it, so nothing here can observe either the flag or an escaping
 *  exception directly. A test claiming the guard was released instead makes a second
 *  dropStoreMapping call and asserts a second fetch was attempted: a released guard permits one,
 *  a wedged guard does not.
 *
 *  Beyond guard release the tests carry two further claims. Where the fetched config ends up:
 *  parsing it before writing is what keeps a rejected config off disk, and mapping from the
 *  parsed result rather than from a re-read is what keeps a failed write from discarding a good
 *  config. And what a refresh does to the store maps: it installs the store -> host mappings the
 *  config names *and* adopts those stores as bootstrap stores, so a store the original config did
 *  not name is corrected by a later refresh rather than dropped by dropStoreMapping and left to a
 *  pool lookup that routes back through the bootstrap stores themselves. That adoption is
 *  add-only, so a store the fetched config omits keeps both its mapping and its bootstrap status.
 *
 *  Every refresh here is driven through dropStoreMapping. receivedUnknownStoreFromHost is the
 *  other public path that reaches refreshBootstrapConfig for a bootstrap store; the two differ
 *  only in how they decide to call it, so dropStoreMapping stands in for both.
 */
class MetadataManagerBootstrapRefreshSuite extends AnyFunSuite
    with Matchers
    with MetadataManagerFixture:

  /** The one store the fixture's bootstrap config names. */
  private val bootstrapStoreId: StoreId = StoreId(poolId, 0.toByte)

  /** Absent from the fixture's bootstrap config; a refreshed config can introduce it. */
  private val secondStoreId: StoreId = StoreId(poolId, 1.toByte)

  private def nudge(): Message =
    CheckStorageDevice(remoteHostId, ClientId.Null, StorageDeviceId(UUID.randomUUID()))

  test("a bootstrap config fetch that fails leaves the refresh retryable"):
    val (mgr, client, _) = newManager()

    mgr.dropStoreMapping(bootstrapStoreId)
    client.bootstrapConfigFetches should be(1)

    // Pre-fix the release lives in a Future.foreach, which runs on Success only, so this
    // completion never reaches it and the guard stays true forever.
    client.bootstrapConfigPromise(1).failure(new RuntimeException("read failed"))

    mgr.dropStoreMapping(bootstrapStoreId)
    client.bootstrapConfigFetches should be(2)

  test("a getBootstrapConfig call that throws leaves the refresh retryable"):
    val (mgr, client, _) = newManager()

    client.failBootstrapConfigWith(new RuntimeException("getBootstrapConfig exploded"))

    // Pre-fix this throw propagates straight out of dropStoreMapping -- past the guard it just
    // set -- and into whatever called it, typically the network IO thread.
    mgr.dropStoreMapping(bootstrapStoreId)
    client.bootstrapConfigFetches should be(1)

    client.clearBootstrapConfigFailure()

    mgr.dropStoreMapping(bootstrapStoreId)
    client.bootstrapConfigFetches should be(2)

  test("a refresh already in flight coalesces a second request"):
    val (mgr, client, _) = newManager()

    // This passes against the unfixed code: coalescing is the flag's actual purpose and the one
    // behaviour it gets right. It is here so the rewrite cannot quietly drop it.
    mgr.dropStoreMapping(bootstrapStoreId)
    mgr.dropStoreMapping(bootstrapStoreId)

    client.bootstrapConfigFetches should be(1)

  test("a fetched config that does not parse leaves the file unchanged and the refresh retryable"):
    val (mgr, client, _, configFile) = newManagerWithConfigFile()

    val before = os.read(configFile)

    mgr.dropStoreMapping(bootstrapStoreId)

    // Well-formed YAML that BootstrapConfig.Config rejects: two stores against an IDA of width
    // one. A syntactically broken string would do too, but this exercises the same validation a
    // real mis-generated config would trip.
    val unparseable = bootstrapConfigYaml(
      Replication(1, 1),
      List(remoteHostState),
      List(bootstrapStoreId -> remoteHostId, secondStoreId -> remoteHostId))

    client.bootstrapConfigPromise(1).success(unparseable)

    // Pre-fix the string is written before anything parses it, so the on-disk config is replaced
    // by one the process cannot read back -- and a restart then fails at construction.
    os.read(configFile) should be(before)

    mgr.dropStoreMapping(bootstrapStoreId)
    client.bootstrapConfigFetches should be(2)

  test("a config that cannot be written is still applied in memory"):
    val (mgr, client, _, configFile) = newManagerWithConfigFile()

    mgr.dropStoreMapping(bootstrapStoreId)

    // atomicWrite creates its temp file in the target's parent directory, so removing the
    // directory makes the write fail with NoSuchFileException whatever the suite's privileges.
    os.remove.all(configFile / os.up)

    client.bootstrapConfigPromise(1).success(bootstrapConfigYaml(
      Replication(1, 1),
      List(remoteHostState),
      List(bootstrapStoreId -> remoteHostId)))

    // The store now routes to remoteHostId, which has no HostEntry, so reaching it starts a
    // lookup and parks the message. Pre-fix the mapping came from a re-read of the file, and
    // removing the directory takes the file with it, so that re-read raised
    // FileNotFoundException and the apply never reached the mapping at all -- the store still
    // mapped to bootstrapHostId and this returned Some. Mapping from the parsed config is what
    // makes the write and the mapping independent.
    mgr.getHostEntryOrQueueMessage(bootstrapStoreId, nudge()) should be(None)
    client.lookups.toList should be(List(remoteHostId))

    mgr.dropStoreMapping(bootstrapStoreId)
    client.bootstrapConfigFetches should be(2)

  test("a fetched config that parses is written through to disk"):
    val (mgr, client, _, configFile) = newManagerWithConfigFile()

    mgr.dropStoreMapping(bootstrapStoreId)

    val fresh = bootstrapConfigYaml(
      Replication(1, 1),
      List(remoteHostState),
      List(bootstrapStoreId -> remoteHostId))

    client.bootstrapConfigPromise(1).success(fresh)

    os.read(configFile) should be(fresh)

  test("a fetched config that is not YAML at all leaves the file unchanged and the refresh retryable"):
    val (mgr, client, _, configFile) = newManagerWithConfigFile()

    val before = os.read(configFile)

    mgr.dropStoreMapping(bootstrapStoreId)

    // Not a FormatError: SnakeYAML raises ParserException, a plain RuntimeException. This is the
    // case loadYamlString's scaladoc warns about -- the reason applyBootstrapConfig's caller
    // catches NonFatal rather than FormatError. Nothing else in the tree covers it.
    client.bootstrapConfigPromise(1).success("bootstrap-hosts: [unclosed\n")

    os.read(configFile) should be(before)

    mgr.dropStoreMapping(bootstrapStoreId)
    client.bootstrapConfigFetches should be(2)

  test("a successful refresh remaps the store and adopts new bootstrap stores"):
    val (mgr, client, _) = newManager()

    mgr.isBootstrapStore(bootstrapStoreId) should be(true)
    mgr.isBootstrapStore(secondStoreId) should be(false)

    mgr.dropStoreMapping(bootstrapStoreId)

    // The refreshed config moves the existing store to remoteHostId and adds a second one there.
    // Two stores means the IDA width must be two -- BootstrapConfig.Config rejects a mismatch.
    client.bootstrapConfigPromise(1).success(bootstrapConfigYaml(
      Replication(2, 2),
      List(remoteHostState),
      List(bootstrapStoreId -> remoteHostId, secondStoreId -> remoteHostId)))

    mgr.getHostEntryOrQueueMessage(bootstrapStoreId, nudge()) should be(None)
    client.lookups.toList should be(List(remoteHostId))

    // Pre-fix bootstrapStores is written once, by the constructor, so the newly named store is
    // treated as an ordinary store: dropStoreMapping would drop its mapping and the next send
    // would start a pool lookup -- against the bootstrap pool, whose state is only readable
    // through the bootstrap stores themselves.
    mgr.isBootstrapStore(secondStoreId) should be(true)

    // The consequence, driven rather than described: dropping the second store's mapping now
    // takes the refresh branch. The first refresh released the guard, so a second fetch is
    // permitted. Pre-fix this took `stores -= storeId` instead and the count stayed at one.
    mgr.dropStoreMapping(secondStoreId)
    client.bootstrapConfigFetches should be(2)

  test("a refresh that omits a store leaves its mapping and its bootstrap status intact"):
    val (mgr, client, _) = newManager()

    mgr.dropStoreMapping(bootstrapStoreId)

    // A config naming only the second store. One store means an IDA of width one.
    client.bootstrapConfigPromise(1).success(bootstrapConfigYaml(
      Replication(1, 1),
      List(remoteHostState),
      List(secondStoreId -> remoteHostId)))

    // mapBootstrapStores is add-only, so the omitted store keeps both halves of what the
    // original config gave it. Lose the mapping and the next send starts a pool lookup; lose the
    // bootstrap status and the next dropStoreMapping drops the mapping rather than refreshing,
    // arriving at the same place -- a lookup against the pool whose state is only readable
    // through the bootstrap stores themselves.
    mgr.isBootstrapStore(bootstrapStoreId) should be(true)

    // Still mapped to bootstrapHostId, which the constructor gave a host entry, so this
    // resolves outright rather than parking the message behind a lookup.
    mgr.getHostEntryOrQueueMessage(bootstrapStoreId, nudge()) should not be None
    client.lookups.toList should be(Nil)
