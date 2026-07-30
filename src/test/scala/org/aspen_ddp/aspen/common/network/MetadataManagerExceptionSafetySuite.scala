package org.aspen_ddp.aspen.common.network

import org.aspen_ddp.aspen.common.metadata.StorageDeviceId
import org.aspen_ddp.aspen.common.store.StoreId
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.util.UUID

/** What a throw costs in MetadataManager's lookup paths.
 *
 *  Every test here fails one call and asserts on what survived. Two failure shapes recur:
 *
 *    - A lookup that throws must leave its subject *retryable*, not merely unresolved. The
 *      distinction is invisible in the resolved/unresolved state alone -- a wedged entry and a
 *      cleared one both report "not resolved" -- so each test proves it by making a second call
 *      and asserting a second lookup was started.
 *    - A throw in the pool handoff loop must cost only its own store.
 *
 *  Note that ExecutionContext.parasitic swallows a throw from an onComplete callback: a promise
 *  completion returns normally even when the continuation it ran threw. Nothing here can assert
 *  on an exception escaping a completion, and the pre-fix bugs were invisible for that reason.
 *
 *  The message type is incidental -- MetadataManager never inspects a message, only the address
 *  it was sent to -- so these reuse the nudge the other suites use.
 */
class MetadataManagerExceptionSafetySuite extends AnyFunSuite
    with Matchers
    with MetadataManagerFixture:

  private def nudge(): Message =
    CheckStorageDevice(remoteHostId, ClientId.Null, StorageDeviceId(UUID.randomUUID()))

  test("a host lookup call that throws leaves the host retryable"):
    val (mgr, client, impl) = newManager()

    client.failLookupWith(remoteHostId, new RuntimeException("getHostState exploded"))

    val msg1 = nudge()
    // Pre-fix this throw escapes getHostEntryOrQueueMessage and can propagate into the send loop,
    // taking down the IO thread if that loop has no guard of its own. Post-fix a synchronous
    // throw is just another way for a lookup to fail: logged, entry removed, None returned.
    mgr.getHostEntryOrQueueMessage(remoteHostId, msg1) should be(None)
    client.lookups.toList should be(List(remoteHostId))

    // The pending entry was removed rather than left behind. Left at Left(phl) it would never
    // resolve -- no continuation exists -- so this would stay true for the life of the process
    // and awaitPendingMessagesSent could only ever time out.
    mgr.hasParkedMessages should be(false)

    client.clearLookupFailure(remoteHostId)

    val msg2 = nudge()
    // Retryable, not merely un-wedged: the second send starts a second lookup instead of parking
    // behind the dead one.
    mgr.getHostEntryOrQueueMessage(remoteHostId, msg2) should be(None)
    client.lookups.toList should be(List(remoteHostId, remoteHostId))

    client.lookupPromise(remoteHostId).success(remoteHostState)

    // The failed lookup dropped its message rather than carrying it into the retry -- the same
    // cost a lookup that fails by returning a failed Future imposes.
    impl.deliveredTo(remoteHostId) should be(List(msg2))
