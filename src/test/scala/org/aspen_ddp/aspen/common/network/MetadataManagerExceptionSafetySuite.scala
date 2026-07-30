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
 *  Note that ExecutionContext.parasitic reports a throw from an onComplete callback rather than
 *  rethrowing it: a promise completion returns normally even when the continuation it ran threw.
 *  Nothing here can assert on an exception escaping a completion, and the pre-fix bugs were
 *  invisible for that reason.
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

  test("a host entry that fails to build leaves the host retryable"):
    val (mgr, client, impl) = newManager()

    impl.throwOnCreateHostEntry(remoteHostId, new RuntimeException("createHostEntry exploded"))

    val msg = nudge()
    mgr.getHostEntryOrQueueMessage(remoteHostId, msg) should be(None)
    client.lookups.toList should be(List(remoteHostId))

    // parasitic runs the continuation inline on this thread and reports a throw from an onComplete
    // callback rather than rethrowing it, so a completion returns normally even when the
    // continuation threw -- which is why nothing here can assert on an exception. Post-fix nothing
    // escapes to be reported: the guard consumes it. That silence is the whole problem pre-fix,
    // where nothing fails and the entry simply never advances from Left to Right.
    client.lookupPromise(remoteHostId).success(remoteHostState)

    // The entry did not reach Right -- createHostEntry never returned one to install. A Left reads
    // as None too, so this only rules out a Right.
    mgr.peekHostEntry(remoteHostId) should be(None)

    // Unlike a lookup that throws, this failure loses nothing: createHostEntry drained the queue
    // before it failed, exactly as ZMQNet's does before wakeIoThread(). Asserted here rather than
    // at the end because both sends share one promise, already completed above -- the retry below
    // resolves inline and delivers its own message too, which would mask what happened to this one.
    impl.deliveredTo(remoteHostId) should be(List(msg))

    // ...and it did not stay at Left either. Pre-fix it did, so this send parks behind a pending
    // lookup nothing will ever resolve. Post-fix the host is back to never-looked-up and the send
    // starts a fresh lookup. This assertion is the one that separates the two states.
    mgr.getHostEntryOrQueueMessage(remoteHostId, nudge()) should be(None)
    client.lookups.toList should be(List(remoteHostId, remoteHostId))

  test("a pool lookup call that throws leaves the pool retryable"):
    val (mgr, client, impl) = newManager()

    val store0 = StoreId(unknownPoolId, 0.toByte)
    client.failPoolLookupWith(unknownPoolId, new RuntimeException("getStoragePoolState exploded"))

    val msg1 = nudge()
    mgr.getHostEntryOrQueueMessage(store0, msg1) should be(None)
    client.poolLookups.toList should be(List(unknownPoolId))

    // A pendingPoolLookups entry no continuation will remove parks every later message for every
    // store in the pool, forever -- one wedge covering a whole pool rather than one host.
    mgr.hasParkedMessages should be(false)

    client.clearPoolLookupFailure(unknownPoolId)

    val msg2 = nudge()
    mgr.getHostEntryOrQueueMessage(store0, msg2) should be(None)
    client.poolLookups.toList should be(List(unknownPoolId, unknownPoolId))

    // The failed lookup dropped its message: the catch discards the ppl and every store queue on
    // it before any can be rescued. Contrasts with a failed createHostEntry, which loses nothing
    // because the network implementation drains the queue before throwing.
    client.poolLookupPromise(unknownPoolId).success(poolStateWith(unknownPoolId, bootstrapHostId))
    impl.deliveredTo(bootstrapHostId) should be(List(msg2))

  test("a throwing storeResolved costs only its own store, not the rest of the pool"):
    val (mgr, client, impl) = newManager()

    val store0 = StoreId(unknownPoolId, 0.toByte)
    val store1 = StoreId(unknownPoolId, 1.toByte)
    val msg0 = nudge()
    val msg1 = nudge()

    mgr.getHostEntryOrQueueMessage(store0, msg0) should be(None)
    mgr.getHostEntryOrQueueMessage(store1, msg1) should be(None)

    // bootstrapHostId is already Right(...) from the constructor, so store 0 takes the
    // storeResolved branch -- the loop's one remaining throw site now that startHostLookup no
    // longer throws. In ZMQNet the throw comes from wakeIoThread() sending on a socket that CLI
    // teardown already closed.
    impl.throwOnStoreResolved(bootstrapHostId, new RuntimeException("wakeIoThread exploded"))

    // Store 0 is listed first so there is a tail to lose. Pre-fix the throw escapes foreach and
    // store 1 gets neither its stores mapping nor its rescued queue.
    client.poolLookupPromise(unknownPoolId).success(
      poolStateWith(unknownPoolId, bootstrapHostId, remoteHostId))

    // Store 0's own messages were consumed before the throw, as they are in ZMQNet: the queue is
    // drained and ProcessPendingMessages enqueued before the wake fails.
    impl.deliveredTo(bootstrapHostId) should be(List(msg0))

    // Store 1's queue was still rescued onto a host lookup.
    client.lookups.toList should be(List(remoteHostId))
    client.lookupPromise(remoteHostId).success(remoteHostState)
    impl.deliveredTo(remoteHostId) should be(List(msg1))
    mgr.hasParkedMessages should be(false)

    // Both stores' mappings survived -- the throwing store's too, which is what "outside the try"
    // buys. A later send to either resolves straight through the host path rather than starting a
    // second pool lookup.
    mgr.getHostEntryOrQueueMessage(store0, nudge()).map(_.hostId) should be(Some(bootstrapHostId))
    mgr.getHostEntryOrQueueMessage(store1, nudge()).map(_.hostId) should be(Some(remoteHostId))
    client.poolLookups.toList should be(List(unknownPoolId))
