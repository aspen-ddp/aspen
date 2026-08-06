package org.aspen_ddp.aspen.common.network

import org.aspen_ddp.aspen.common.store.StoreId
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** What a bootstrap config refresh costs when it goes wrong.
 *
 *  refreshingBootstrapConfig is a plain Boolean, so a refresh that ends without clearing it
 *  disables every later refresh for the life of the process. dropStoreMapping routes bootstrap
 *  stores to refreshBootstrapConfig and non-bootstrap stores to `stores -= storeId`, so a wedged
 *  flag does not degrade to the fallback -- it drops the correction entirely.
 *
 *  The flag is private and ExecutionContext.parasitic reports a throw from an onComplete callback
 *  rather than rethrowing it, so nothing here can observe either the flag or an escaping
 *  exception directly. Each test instead makes a second dropStoreMapping call and asserts a
 *  second fetch was attempted: a released guard permits one, a wedged guard does not.
 *
 *  Every test drives the refresh through dropStoreMapping(bootstrapStoreId).
 *  receivedUnknownStoreFromHost is the other public path that reaches refreshBootstrapConfig for
 *  a bootstrap store; the two differ only in how they decide to call it, so dropStoreMapping
 *  stands in for both.
 */
class MetadataManagerBootstrapRefreshSuite extends AnyFunSuite
    with Matchers
    with MetadataManagerFixture:

  /** The one store the fixture's bootstrap config names. */
  private val bootstrapStoreId: StoreId = StoreId(poolId, 0.toByte)

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

  test("a throw while applying the fetched config leaves the refresh retryable"):
    val (mgr, client, _, path) = newManagerWithConfigFile()

    mgr.dropStoreMapping(bootstrapStoreId)
    client.bootstrapConfigFetches should be(1)

    // Removes the parent directory, so atomicWrite's createTempFile and the re-read both throw.
    os.remove.all(path / os.up)
    client.bootstrapConfigPromise(1).success("not even valid yaml")

    mgr.dropStoreMapping(bootstrapStoreId)
    client.bootstrapConfigFetches should be(2)

  test("a refresh already in flight coalesces a second request"):
    val (mgr, client, _) = newManager()

    // This passes against the unfixed code: coalescing is the flag's actual purpose and the one
    // behaviour it gets right. It is here so the rewrite cannot quietly drop it.
    mgr.dropStoreMapping(bootstrapStoreId)
    mgr.dropStoreMapping(bootstrapStoreId)

    client.bootstrapConfigFetches should be(1)
