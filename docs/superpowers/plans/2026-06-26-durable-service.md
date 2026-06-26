# Durable Service Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement a lease-based, cluster-wide singleton service framework that ensures long-running services stay running on exactly one host at all times, with automatic recovery when a host fails.

**Architecture:** A new `ServicesTreeKey` in `Radicle` roots a `TieredKeyValueList` mapping each service UUID to a `ServiceEntry` (typeUUID, hostId, leaseExpiry, stateObjectPointer). `SimpleDurableServiceExecutor` runs on every host; it periodically scans the tree looking for expired leases, races to claim them using revision-based transactions, and renews owned leases every 5 seconds. Load is spread by delaying claim attempts proportionally to how many services a host already owns.

**Tech Stack:** Scala 3, existing Aspen transaction/TKVL/BackgroundTaskManager infrastructure, ScalaTest AsyncFunSuite integration tests.

---

## File Map

| Action | Path | Responsibility |
|--------|------|---------------|
| Create | `src/main/scala/org/aspen_ddp/aspen/compute/DurableService.scala` | `DurableService` trait |
| Create | `src/main/scala/org/aspen_ddp/aspen/compute/DurableServiceFactory.scala` | `DurableServiceFactory` trait |
| Create | `src/main/scala/org/aspen_ddp/aspen/compute/DurableServiceExecutor.scala` | `DurableServiceExecutor` trait |
| Create | `src/main/scala/org/aspen_ddp/aspen/compute/ServiceEntry.scala` | TKVL value encoding/decoding |
| Create | `src/main/scala/org/aspen_ddp/aspen/compute/impl/SimpleDurableServiceExecutor.scala` | Full executor implementation |
| Modify | `src/main/scala/org/aspen_ddp/aspen/common/Radicle.scala` | Add `ServicesTreeKey = Key(Array[Byte](9))` |
| Modify | `src/main/scala/org/aspen_ddp/aspen/server/store/Bootstrap.scala` | Create empty services TKVL, store root under `ServicesTreeKey` |
| Modify | `src/main/scala/org/aspen_ddp/aspen/server/StoreManager.scala` | Construct and shut down `SimpleDurableServiceExecutor` |
| Modify | `src/test/scala/org/aspen_ddp/aspen/TestNetwork.scala` | Expose `serviceExecutor` for test assertions |
| Create | `src/test/scala/org/aspen_ddp/aspen/compute/DurableServiceSuite.scala` | Integration tests |

---

## Task 1: Core traits

**Files:**
- Create: `src/main/scala/org/aspen_ddp/aspen/compute/DurableService.scala`
- Create: `src/main/scala/org/aspen_ddp/aspen/compute/DurableServiceFactory.scala`
- Create: `src/main/scala/org/aspen_ddp/aspen/compute/DurableServiceExecutor.scala`

- [ ] **Step 1: Create DurableService.scala**

```scala
package org.aspen_ddp.aspen.compute

trait DurableService:
  /** Called by the executor when this host loses its lease.
   *  The service must stop operating immediately. */
  def shutdown(): Unit
```

- [ ] **Step 2: Create DurableServiceFactory.scala**

```scala
package org.aspen_ddp.aspen.compute

import org.aspen_ddp.aspen.client.{AspenClient, KeyValueObjectState, RegisteredTypeFactory}
import org.aspen_ddp.aspen.common.objects.KeyValueObjectPointer

trait DurableServiceFactory extends RegisteredTypeFactory:
  /** Construct and immediately start a service instance from persisted state.
   *  The service begins operating upon return. */
  def createService(
    client: AspenClient,
    statePointer: KeyValueObjectPointer,
    state: KeyValueObjectState
  ): DurableService
```

- [ ] **Step 3: Create DurableServiceExecutor.scala**

```scala
package org.aspen_ddp.aspen.compute

import java.util.UUID
import org.aspen_ddp.aspen.common.objects.Key

import scala.concurrent.Future

trait DurableServiceExecutor:
  /** Idempotent. Creates state KVO and an immediately-claimable TKVL entry.
   *  No-op if the service UUID is already registered. */
  def registerService(
    typeUUID: UUID,
    serviceUUID: UUID,
    initialState: Map[Key, Array[Byte]]
  ): Future[Unit]

  /** Removes the TKVL entry. Calls shutdown() on the service if this host owns it. */
  def unregisterService(serviceUUID: UUID): Future[Unit]
```

- [ ] **Step 4: Compile**

```bash
sbt compile
```

Expected: clean compile.

- [ ] **Step 5: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/compute/DurableService.scala \
        src/main/scala/org/aspen_ddp/aspen/compute/DurableServiceFactory.scala \
        src/main/scala/org/aspen_ddp/aspen/compute/DurableServiceExecutor.scala
git commit -m "Add DurableService, DurableServiceFactory, DurableServiceExecutor traits"
```

---

## Task 2: ServiceEntry encoding/decoding

**Files:**
- Create: `src/main/scala/org/aspen_ddp/aspen/compute/ServiceEntry.scala`
- Test: `src/test/scala/org/aspen_ddp/aspen/compute/DurableServiceSuite.scala`

The TKVL value for each service is a fixed-layout binary blob:

```
Offset  Size  Field
     0    16  typeUUID  (MSB, LSB)
    16    16  hostId    (MSB, LSB); all-zeros = unclaimed
    32     8  leaseExpiry as HLCTimestamp.asLong
    40     *  stateObjectPointer (KeyValueObjectPointer.toArray)
```

`leaseExpiry = HLCTimestamp.Zero` means immediately claimable (used at registration).

- [ ] **Step 1: Write a failing test**

Create `src/test/scala/org/aspen_ddp/aspen/compute/DurableServiceSuite.scala`:

```scala
package org.aspen_ddp.aspen.compute

import org.aspen_ddp.aspen.common.{HLCTimestamp, Radicle}
import org.aspen_ddp.aspen.common.metadata.HostId
import org.aspen_ddp.aspen.common.objects.KeyValueObjectPointer
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.util.UUID

class DurableServiceSuite extends AnyFunSuite with Matchers:

  test("ServiceEntry round-trip encode/decode"):
    val typeUUID = UUID.randomUUID()
    val hostId = UUID.randomUUID()
    val leaseExpiry = HLCTimestamp.now
    val statePointer = KeyValueObjectPointer(Radicle.objectId, Radicle.poolId)

    val entry = ServiceEntry(typeUUID, hostId, leaseExpiry, statePointer)
    val decoded = ServiceEntry.decode(entry.encode())

    decoded.typeUUID shouldBe typeUUID
    decoded.hostId shouldBe hostId
    decoded.leaseExpiry shouldBe leaseExpiry
    decoded.statePointer.id shouldBe statePointer.id
    decoded.statePointer.poolId shouldBe statePointer.poolId

  test("ServiceEntry zero hostId represents unclaimed"):
    val entry = ServiceEntry(
      typeUUID = UUID.randomUUID(),
      hostId = new UUID(0, 0),
      leaseExpiry = HLCTimestamp.Zero,
      statePointer = KeyValueObjectPointer(Radicle.objectId, Radicle.poolId)
    )
    val decoded = ServiceEntry.decode(entry.encode())
    decoded.hostId shouldBe new UUID(0, 0)
    decoded.leaseExpiry shouldBe HLCTimestamp.Zero
```

- [ ] **Step 2: Run test — expect compile failure**

```bash
sbt 'testOnly *DurableServiceSuite -- -z "round-trip"'
```

Expected: compile error — `ServiceEntry` not found.

- [ ] **Step 3: Create ServiceEntry.scala**

```scala
package org.aspen_ddp.aspen.compute

import org.aspen_ddp.aspen.common.HLCTimestamp
import org.aspen_ddp.aspen.common.objects.{KeyValueObjectPointer, ObjectPointer}

import java.nio.ByteBuffer
import java.util.UUID

object ServiceEntry:
  val UnclamedHostId: UUID = new UUID(0, 0)

  def decode(bytes: Array[Byte]): ServiceEntry =
    val bb = ByteBuffer.wrap(bytes)
    val typeUUID = new UUID(bb.getLong(), bb.getLong())
    val hostId   = new UUID(bb.getLong(), bb.getLong())
    val leaseExpiry = HLCTimestamp(bb.getLong())
    val ptrBytes = new Array[Byte](bb.remaining())
    bb.get(ptrBytes)
    ServiceEntry(typeUUID, hostId, leaseExpiry, ObjectPointer(ptrBytes).asInstanceOf[KeyValueObjectPointer])

final case class ServiceEntry(
  typeUUID: UUID,
  hostId: UUID,
  leaseExpiry: HLCTimestamp,
  statePointer: KeyValueObjectPointer
):
  def encode(): Array[Byte] =
    val ptrBytes = statePointer.toArray
    val bb = ByteBuffer.allocate(16 + 16 + 8 + ptrBytes.length)
    bb.putLong(typeUUID.getMostSignificantBits)
    bb.putLong(typeUUID.getLeastSignificantBits)
    bb.putLong(hostId.getMostSignificantBits)
    bb.putLong(hostId.getLeastSignificantBits)
    bb.putLong(leaseExpiry.asLong)
    bb.put(ptrBytes)
    bb.array()

  def isExpired: Boolean = leaseExpiry < HLCTimestamp.now

  def isClaimed: Boolean = hostId != ServiceEntry.UnclamedHostId
```

- [ ] **Step 4: Run tests — expect pass**

```bash
sbt 'testOnly *DurableServiceSuite'
```

Expected: 2 tests pass.

- [ ] **Step 5: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/compute/ServiceEntry.scala \
        src/test/scala/org/aspen_ddp/aspen/compute/DurableServiceSuite.scala
git commit -m "Add ServiceEntry encoding/decoding for services TKVL values"
```

---

## Task 3: Radicle key + Bootstrap services tree

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/common/Radicle.scala`
- Modify: `src/main/scala/org/aspen_ddp/aspen/server/store/Bootstrap.scala`

- [ ] **Step 1: Add ServicesTreeKey to Radicle.scala**

In `Radicle.scala`, after `StorageDeviceSetsTreeKey`:

```scala
  private[aspen] val StorageDeviceSetsTreeKey = Key(Array[Byte](8))
  private[aspen] val ServicesTreeKey           = Key(Array[Byte](9))
```

- [ ] **Step 2: Add services TKVL to Bootstrap.scala**

In `Bootstrap.initialize`, after the `storageDeviceSetsTree` block and before the `radicleContent` list:

```scala
    val servicesTree = allocateTree(ByteArrayKeyOrdering)
```

Then add it to `radicleContent`:

```scala
    val radicleContent: List[(Key, Array[Byte])] = List(
      Radicle.BootstrapConfigKey          -> bootstrapConfig.getBytes(StandardCharsets.UTF_8),
      Radicle.SystemIdKey                 -> uuid2byte(aspenSystemId),
      Radicle.ObjectRegistryKey           -> objectRegistryTree.encode(),
      Radicle.NamespacedRegistryKey       -> namespacedRegistryTree.encode(),
      Radicle.StoragePoolsTreeKey         -> storagePoolsTree.encode(),
      Radicle.AllocationGroupsTreeKey     -> allocationGroupsTree.encode(),
      Radicle.HostsTreeKey                -> hostsTree.encode(),
      Radicle.StorageDevicesTreeKey       -> storageDevicesTree.encode(),
      Radicle.StorageDeviceSetsTreeKey    -> storageDeviceSetsTree.encode(),
      Radicle.ServicesTreeKey             -> servicesTree.encode(),
    )
```

- [ ] **Step 3: Run all tests**

```bash
sbt test
```

Expected: all existing tests pass. Any test that bootstraps (e.g. integration tests using `TestNetwork`) now creates a services TKVL. Bootstrap tests that check the Radicle content must include the new key — fix any that fail.

- [ ] **Step 4: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/common/Radicle.scala \
        src/main/scala/org/aspen_ddp/aspen/server/store/Bootstrap.scala
git commit -m "Add ServicesTreeKey to Radicle and create empty services TKVL in Bootstrap"
```

---

## Task 4: SimpleDurableServiceExecutor skeleton

**Files:**
- Create: `src/main/scala/org/aspen_ddp/aspen/compute/impl/SimpleDurableServiceExecutor.scala`

This task creates the class with its constructor, internal state, TKVL reference, and `shutdown()` method. Scan and renewal logic are added in later tasks.

- [ ] **Step 1: Create the file**

```scala
package org.aspen_ddp.aspen.compute.impl

import org.aspen_ddp.aspen.client.{AspenClient, KeyValueObjectState, Transaction}
import org.aspen_ddp.aspen.client.tkvl.{KVObjectRootManager, TieredKeyValueList}
import org.aspen_ddp.aspen.common.{HLCTimestamp, Radicle}
import org.aspen_ddp.aspen.common.metadata.HostId
import org.aspen_ddp.aspen.common.objects.{Key, KeyValueObjectPointer, Value}
import org.aspen_ddp.aspen.common.util.{BackgroundTaskManager, ignoreExtraCallsWhileRunning}
import org.aspen_ddp.aspen.common.util.BackgroundTaskManager.ScheduledTask
import org.aspen_ddp.aspen.compute.{DurableService, DurableServiceExecutor, ServiceEntry}
import scribe.Logging

import java.util.UUID
import java.util.concurrent.ThreadLocalRandom
import scala.concurrent.duration.*
import scala.concurrent.{ExecutionContext, Future}

class SimpleDurableServiceExecutor(
  val client: AspenClient,
  val hostId: HostId,
  val backgroundTasks: BackgroundTaskManager,
  val leaseDuration: Duration        = Duration(15, SECONDS),
  val renewalInterval: Duration      = Duration(5, SECONDS),
  val minScanInterval: Duration      = Duration(10, SECONDS),
  val maxScanInterval: Duration      = Duration(30, SECONDS),
  val claimDelayPerService: Duration = Duration(500, MILLISECONDS)
) extends DurableServiceExecutor with Logging:

  given ExecutionContext = client.clientContext

  // serviceUUID -> (running instance, renewal timer)
  private var ownedServices: Map[UUID, (DurableService, ScheduledTask)] = Map.empty

  // Task that fires the next scan (one-shot, rescheduled after each scan completes)
  private var scanTask: ScheduledTask = ScheduledTask.NoTask

  protected val servicesTkvl: TieredKeyValueList =
    TieredKeyValueList(client, KVObjectRootManager(client, Radicle.ServicesTreeKey, Radicle.pointer))

  // Start the first scan
  scheduleScan()

  /** Cancels all timers and shuts down all owned services. Must be called on host shutdown. */
  def shutdown(): Unit = synchronized:
    scanTask.cancel()
    ownedServices.foreach: (_, (service, renewalTimer)) =>
      renewalTimer.cancel()
      service.shutdown()
    ownedServices = Map.empty

  protected def scheduleScan(): Unit =
    val rangeMillis = maxScanInterval.toMillis - minScanInterval.toMillis
    val delayMillis = minScanInterval.toMillis + ThreadLocalRandom.current().nextLong(rangeMillis)
    scanTask = backgroundTasks.schedule(Duration(delayMillis, MILLISECONDS)):
      doScan()

  protected def doScan(): Unit = ()  // filled in Task 6

  protected def startRenewalTimer(serviceUUID: UUID): ScheduledTask =
    backgroundTasks.schedulePeriodic(renewalInterval):
      doRenewal(serviceUUID)

  protected def doRenewal(serviceUUID: UUID): Unit = ()  // filled in Task 7

  override def registerService(
    typeUUID: UUID,
    serviceUUID: UUID,
    initialState: Map[Key, Array[Byte]]
  ): Future[Unit] = Future.unit  // filled in Task 5

  override def unregisterService(serviceUUID: UUID): Future[Unit] = Future.unit  // filled in Task 9
```

Note: `ScheduledTask.NoTask` does not yet exist. Add it to `BackgroundTaskManager` in the next step.

- [ ] **Step 2: Add NoTask sentinel to BackgroundTaskManager**

In `BackgroundTaskManager.scala`, inside `object BackgroundTaskManager`, add after `ShutdownTask`:

```scala
  object NoTask extends ScheduledTask:
    def cancel(): Unit = ()
```

- [ ] **Step 3: Compile**

```bash
sbt compile
```

Expected: clean compile.

- [ ] **Step 4: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/compute/impl/SimpleDurableServiceExecutor.scala \
        src/main/scala/org/aspen_ddp/aspen/common/util/BackgroundTaskManager.scala
git commit -m "Add SimpleDurableServiceExecutor skeleton with TKVL ref and timer management"
```

---

## Task 5: registerService

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/compute/impl/SimpleDurableServiceExecutor.scala`
- Modify: `src/test/scala/org/aspen_ddp/aspen/compute/DurableServiceSuite.scala`

- [ ] **Step 1: Write a failing integration test**

Add to `DurableServiceSuite.scala`. This test needs a full `TestNetwork`. Change the suite to extend `IntegrationTestSuite`:

```scala
package org.aspen_ddp.aspen.compute

import org.aspen_ddp.aspen.IntegrationTestSuite
import org.aspen_ddp.aspen.client.internal.allocation.PoolObjectAllocator
import org.aspen_ddp.aspen.common.{HLCTimestamp, Radicle}
import org.aspen_ddp.aspen.common.metadata.HostId
import org.aspen_ddp.aspen.common.objects.{Key, KeyValueObjectPointer, ObjectPointer, Value}
import org.aspen_ddp.aspen.common.util.BackgroundTaskManager
import org.aspen_ddp.aspen.compute.impl.SimpleDurableServiceExecutor
import org.scalatest.funsuite.{AnyFunSuite, AsyncFunSuite}
import org.scalatest.matchers.should.Matchers

import java.util.UUID
import scala.concurrent.duration.*
import scala.concurrent.{ExecutionContext, Future, Promise}

// Keep the pure unit tests in a separate non-integration suite
class ServiceEntrySpec extends AnyFunSuite with Matchers:

  test("ServiceEntry round-trip encode/decode"):
    val typeUUID = UUID.randomUUID()
    val hostId = UUID.randomUUID()
    val leaseExpiry = HLCTimestamp(12345678L)
    val statePointer = KeyValueObjectPointer(Radicle.objectId, Radicle.poolId)
    val entry = ServiceEntry(typeUUID, hostId, leaseExpiry, statePointer)
    val decoded = ServiceEntry.decode(entry.encode())
    decoded.typeUUID shouldBe typeUUID
    decoded.hostId shouldBe hostId
    decoded.leaseExpiry shouldBe leaseExpiry
    decoded.statePointer.id shouldBe statePointer.id
    decoded.statePointer.poolId shouldBe statePointer.poolId

  test("ServiceEntry zero hostId represents unclaimed"):
    val entry = ServiceEntry(UUID.randomUUID(), new UUID(0, 0), HLCTimestamp.Zero,
      KeyValueObjectPointer(Radicle.objectId, Radicle.poolId))
    val decoded = ServiceEntry.decode(entry.encode())
    decoded.hostId shouldBe new UUID(0, 0)
    decoded.leaseExpiry shouldBe HLCTimestamp.Zero


class DurableServiceSuite extends IntegrationTestSuite:

  val testHostId: HostId = HostId(UUID.fromString("00000000-0000-0000-0000-000000000001"))

  def makeExecutor(): SimpleDurableServiceExecutor =
    new SimpleDurableServiceExecutor(
      client, testHostId, client.backgroundTaskManager,
      leaseDuration        = Duration(200, MILLISECONDS),
      renewalInterval      = Duration(50, MILLISECONDS),
      minScanInterval      = Duration(50, MILLISECONDS),
      maxScanInterval      = Duration(100, MILLISECONDS),
      claimDelayPerService = Duration(0, MILLISECONDS)
    )

  atest("registerService creates a claimable TKVL entry"):
    given ExecutionContext = executionContext
    val exec = makeExecutor()
    val typeUUID = UUID.randomUUID()
    val svcUUID  = UUID.randomUUID()
    for
      _ <- exec.registerService(typeUUID, svcUUID, Map.empty)
      vs <- exec.servicesTkvl.get(Key(svcUUID))
    yield
      exec.shutdown()
      vs shouldBe defined
      val entry = ServiceEntry.decode(vs.get.value.bytes)
      entry.typeUUID shouldBe typeUUID
      entry.hostId shouldBe ServiceEntry.UnclamedHostId
      entry.leaseExpiry shouldBe HLCTimestamp.Zero

  atest("registerService is idempotent"):
    given ExecutionContext = executionContext
    val exec = makeExecutor()
    val typeUUID = UUID.randomUUID()
    val svcUUID  = UUID.randomUUID()
    for
      _ <- exec.registerService(typeUUID, svcUUID, Map.empty)
      _ <- exec.registerService(typeUUID, svcUUID, Map.empty)  // second call must not throw
      vs <- exec.servicesTkvl.get(Key(svcUUID))
    yield
      exec.shutdown()
      vs shouldBe defined
```

Note: `client.backgroundTaskManager` must be accessible. Check `TestNetwork.TClient` — it exposes `val backgroundTaskManager`. If it is `private`, expose it via `def backgroundTaskManager: BackgroundTaskManager` on `AspenClient` (or add a cast in the test).

- [ ] **Step 2: Run test — expect failure**

```bash
sbt 'testOnly *DurableServiceSuite -- -z "registerService creates"'
```

Expected: test fails because `registerService` returns `Future.unit` without writing anything.

- [ ] **Step 3: Implement registerService**

In `SimpleDurableServiceExecutor.scala`, replace the `registerService` stub:

```scala
override def registerService(
  typeUUID: UUID,
  serviceUUID: UUID,
  initialState: Map[Key, Array[Byte]]
): Future[Unit] =
  import org.aspen_ddp.aspen.client.internal.allocation.PoolObjectAllocator
  import org.aspen_ddp.aspen.common.objects.Value
  import org.aspen_ddp.aspen.common.pool.PoolId

  val serviceKey = Key(serviceUUID)

  servicesTkvl.get(serviceKey).flatMap:
    case Some(_) => Future.unit  // already registered
    case None =>
      client.transactUntilSuccessful[Unit]: tx =>
        given Transaction = tx
        for
          pool      <- client.getStoragePool(Radicle.poolId)
          allocator  = new PoolObjectAllocator(client, pool)
          statePtr  <- allocator.allocateKeyValueObject(initialState.map((k, v) => k -> Value(v)))(using tx)
        yield
          val entry = ServiceEntry(typeUUID, ServiceEntry.UnclamedHostId, HLCTimestamp.Zero, statePtr)
          servicesTkvl.set(serviceKey, Value(entry.encode()), Some(Left(true)))(using tx)
      .recoverWith:
        // KeyAlreadyExists means a concurrent registerService won the race — treat as success
        case _: org.aspen_ddp.aspen.common.objects.KeyAlreadyExists => Future.unit
```

`Some(Left(true))` is the `DoesNotExist` requirement used by `MetadataTree.preparePut` — the set fails if the key already exists, which `transactUntilSuccessful` retries, but on retry `servicesTkvl.get` returns `Some(_)` and we short-circuit. The `recoverWith` catches any residual `KeyAlreadyExists` on the off-chance the short-circuit race is lost.

- [ ] **Step 4: Run tests**

```bash
sbt 'testOnly *DurableServiceSuite'
```

Expected: all tests in the suite pass.

- [ ] **Step 5: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/compute/impl/SimpleDurableServiceExecutor.scala \
        src/test/scala/org/aspen_ddp/aspen/compute/DurableServiceSuite.scala
git commit -m "Implement registerService: create state KVO and claimable TKVL entry"
```

---

## Task 6: Scan loop + claim attempt

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/compute/impl/SimpleDurableServiceExecutor.scala`
- Modify: `src/test/scala/org/aspen_ddp/aspen/compute/DurableServiceSuite.scala`

- [ ] **Step 1: Write a failing test**

Add to `DurableServiceSuite`:

```scala
  atest("executor claims an unclaimed service on scan"):
    given ExecutionContext = executionContext

    val typeUUID = UUID.randomUUID()
    val svcUUID  = UUID.randomUUID()
    val claimed  = Promise[KeyValueObjectState]()

    // Factory that completes the promise when createService is called
    val factory = new DurableServiceFactory:
      val typeUUID: UUID = DurableServiceSuite.this.typeUUID  // placeholder; reassign below
      def createService(c: AspenClient, ptr: KeyValueObjectPointer, state: KeyValueObjectState): DurableService =
        claimed.trySuccess(state)
        new DurableService { def shutdown(): Unit = () }
```

Because `typeUUID` is a `val` on the trait, construct as an anonymous class with the outer `typeUUID`:

```scala
    val claimedPromise = Promise[Unit]()
    val fTypeUUID = UUID.randomUUID()

    val factory = new DurableServiceFactory:
      val typeUUID: UUID = fTypeUUID
      def createService(c: AspenClient, ptr: KeyValueObjectPointer, state: KeyValueObjectState): DurableService =
        claimedPromise.trySuccess(())
        new DurableService { def shutdown(): Unit = () }

    // Register factory so the executor can find it
    // NOTE: TypeRegistry is immutable — we need TestNetwork to include this factory.
    // Simplest approach: add factory to the test client's registry via a mutable wrapper,
    // OR create a test subclass that overrides typeRegistry.
    // Use the approach shown in SteppedDurableTaskSuite: TestNetwork builds
    // a TypeRegistry from the static factory lists. For this test, we register the
    // factory at construction time via TestNetwork's user-supplied factories parameter.
    // See Task 6 Step 2 for the TestNetwork extension approach.
```

**Practical approach**: `TestNetwork.TClient.typeRegistry` is constructed from static lists. Add a `userFactories: List[RegisteredTypeFactory] = Nil` parameter to `TestNetwork` and include them in the registry. Then pass the test factory:

In `TestNetwork.scala`, change the `TestNetwork` class signature to:

```scala
class TestNetwork(executionContext: ExecutionContext,
                  userTypeFactories: List[org.aspen_ddp.aspen.client.RegisteredTypeFactory] = Nil)
```

And in `TClient.typeRegistry`:

```scala
    val typeRegistry: TypeRegistry = TypeRegistry(
      org.aspen_ddp.aspen.common.TypeFactories.factories,
      org.aspen_ddp.aspen.client.TypeFactories.factories,
      org.aspen_ddp.aspen.server.TypeFactories.factories,
      userTypeFactories
    )
```

Then the test creates the network with:

```scala
override def subFixtureSetup(): Unit =
  // recreate net with the test factory included
  net = new TestNetwork(executionContext, List(factory))
  client = net.client
  radicle = net.radicle
```

Wait — `IntegrationTestSuite.withFixture` already creates `TestNetwork`. A simpler pattern: override `withFixture` or `subFixtureSetup` is already called before the test. Use it:

Revised test structure in `DurableServiceSuite`:

```scala
  private var testFactory: DurableServiceFactory = scala.compiletime.uninitialized
  private var claimedPromise: Promise[Unit]       = scala.compiletime.uninitialized

  override def subFixtureSetup(): Unit =
    claimedPromise = Promise[Unit]()
    testFactory = new DurableServiceFactory:
      val typeUUID: UUID = UUID.fromString("AAAAAAAA-0000-0000-0000-000000000001")
      def createService(c: AspenClient, ptr: KeyValueObjectPointer, state: KeyValueObjectState): DurableService =
        claimedPromise.trySuccess(())
        new DurableService { def shutdown(): Unit = () }
    net = new TestNetwork(executionContext, List(testFactory))
    client = net.client
    radicle = net.radicle
```

Now the actual test:

```scala
  atest("executor claims an unclaimed service on scan"):
    given ExecutionContext = executionContext
    val svcUUID = UUID.randomUUID()
    val exec = makeExecutor()
    for
      _       <- exec.registerService(testFactory.typeUUID, svcUUID, Map.empty)
      _       <- claimedPromise.future   // wait for factory.createService to be called
      vs      <- exec.servicesTkvl.get(Key(svcUUID))
    yield
      exec.shutdown()
      vs shouldBe defined
      val entry = ServiceEntry.decode(vs.get.value.bytes)
      entry.hostId shouldBe testHostId.uuid
      entry.leaseExpiry > HLCTimestamp.Zero shouldBe true
```

- [ ] **Step 2: Update TestNetwork to accept user factories**

In `TestNetwork.scala`:

Change `class TestNetwork(executionContext: ExecutionContext)` to:

```scala
class TestNetwork(executionContext: ExecutionContext,
                  userTypeFactories: List[org.aspen_ddp.aspen.client.RegisteredTypeFactory] = Nil)
```

In `TClient`, change `typeRegistry` to:

```scala
    val typeRegistry: TypeRegistry = TypeRegistry(
      org.aspen_ddp.aspen.common.TypeFactories.factories,
      org.aspen_ddp.aspen.client.TypeFactories.factories,
      org.aspen_ddp.aspen.server.TypeFactories.factories,
      userTypeFactories
    )
```

Also expose `backgroundTaskManager` on `AspenClient` if not already present. Check `AspenClient.scala` for an existing `def backgroundTaskManager`. If absent, add it to the trait and provide the implementation in `TClient`:

```scala
// In AspenClient trait:
def backgroundTaskManager: BackgroundTaskManager

// In TClient:
override def backgroundTaskManager: BackgroundTaskManager = this.backgroundTaskManager  // already a field
```

- [ ] **Step 3: Run test — expect failure**

```bash
sbt 'testOnly *DurableServiceSuite -- -z "executor claims"'
```

Expected: test times out because `doScan()` is a no-op.

- [ ] **Step 4: Implement the scan loop**

Replace `doScan()` and `attemptClaim(...)` in `SimpleDurableServiceExecutor.scala`:

```scala
  import scala.collection.mutable.ListBuffer

  private val scanFn: () => Unit = ignoreExtraCallsWhileRunning:
    val entries = ListBuffer[(Key, ServiceEntry, org.aspen_ddp.aspen.common.objects.ObjectRevision)]()

    servicesTkvl.foreach: (_, key, vs) =>
      entries += ((key, ServiceEntry.decode(vs.value.bytes), vs.revision))
      Future.unit
    .map: _ =>
      val now        = HLCTimestamp.now
      val tkvlUUIDs  = entries.map((k, _, _) => byte2uuid(k.bytes)).toSet
      val currentOwned = synchronized { ownedServices.keySet }

      // Unregistration check (Task 8 — leave as no-op for now, filled in Task 8)

      // Expired lease detection
      val candidates = entries.filter: (key, entry, _) =>
        val svcUUID = byte2uuid(key.bytes)
        entry.leaseExpiry < now && !currentOwned.contains(svcUUID)

      candidates.foreach: (key, entry, revision) =>
        val delay = synchronized:
          val n = ownedServices.size
          if n == 0 then Duration.Zero
          else Duration(ThreadLocalRandom.current().nextLong(n * claimDelayPerService.toMillis), MILLISECONDS)
        backgroundTasks.schedule(delay):
          attemptClaim(key, entry.typeUUID, entry.statePointer, revision)

      scheduleScan()

  protected def doScan(): Unit = scanFn()

  private def byte2uuid(bytes: Array[Byte]): UUID =
    org.aspen_ddp.aspen.common.util.byte2uuid(bytes)

  private def attemptClaim(
    serviceKey: Key,
    typeUUID: UUID,
    statePointer: KeyValueObjectPointer,
    oldRevision: org.aspen_ddp.aspen.common.objects.ObjectRevision
  ): Unit =
    client.typeRegistry.getType[DurableServiceFactory](typeUUID) match
      case None =>
        logger.warn(s"No DurableServiceFactory registered for typeUUID $typeUUID — cannot claim service")
      case Some(factory) =>
        val newExpiry = HLCTimestamp(HLCTimestamp.now.asLong + (leaseDuration.toMillis << 16))
        val newEntry  = ServiceEntry(typeUUID, hostId.uuid, newExpiry, statePointer)
        client.transactUntilSuccessful[Unit]: tx =>
          given Transaction = tx
          servicesTkvl.set(serviceKey, Value(newEntry.encode()), Some(Right(oldRevision)))(using tx)
        .foreach: _ =>
          client.read(statePointer).foreach: state =>
            val svcUUID = byte2uuid(serviceKey.bytes)
            val service = factory.createService(client, statePointer, state)
            val timer   = startRenewalTimer(svcUUID)
            synchronized:
              ownedServices += svcUUID -> (service, timer)
```

Note: `client.typeRegistry.getType[DurableServiceFactory](typeUUID)` requires a `getType` method on `TypeRegistry`. Check its API. Looking at SimpleTaskExecutor: `client.typeRegistry.getType[DurableTaskFactory](taskType)`. Confirm that API is available and use the same pattern.

- [ ] **Step 5: Run tests**

```bash
sbt 'testOnly *DurableServiceSuite'
```

Expected: all tests pass (scan fires, finds unclaimed entry, claims it, calls factory).

- [ ] **Step 6: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/compute/impl/SimpleDurableServiceExecutor.scala \
        src/test/scala/org/aspen_ddp/aspen/TestNetwork.scala \
        src/test/scala/org/aspen_ddp/aspen/compute/DurableServiceSuite.scala \
        src/main/scala/org/aspen_ddp/aspen/client/AspenClient.scala
git commit -m "Implement scan loop and claim attempt; extend TestNetwork with userTypeFactories"
```

---

## Task 7: Lease renewal timers

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/compute/impl/SimpleDurableServiceExecutor.scala`
- Modify: `src/test/scala/org/aspen_ddp/aspen/compute/DurableServiceSuite.scala`

- [ ] **Step 1: Write a failing test**

Add to `DurableServiceSuite`:

```scala
  atest("executor renews lease before expiry"):
    given ExecutionContext = executionContext
    val svcUUID = UUID.randomUUID()
    val exec = makeExecutor()
    for
      _  <- exec.registerService(testFactory.typeUUID, svcUUID, Map.empty)
      _  <- claimedPromise.future                // wait for initial claim
      v0 <- exec.servicesTkvl.get(Key(svcUUID)) // capture first leaseExpiry
      _  <- Future { Thread.sleep(150) }         // wait 3× renewalInterval
      v1 <- exec.servicesTkvl.get(Key(svcUUID)) // check leaseExpiry updated
    yield
      exec.shutdown()
      v0 shouldBe defined
      v1 shouldBe defined
      val e0 = ServiceEntry.decode(v0.get.value.bytes)
      val e1 = ServiceEntry.decode(v1.get.value.bytes)
      e1.leaseExpiry > e0.leaseExpiry shouldBe true
```

- [ ] **Step 2: Run test — expect failure**

```bash
sbt 'testOnly *DurableServiceSuite -- -z "renews lease"'
```

Expected: `e1.leaseExpiry` equals `e0.leaseExpiry` because `doRenewal` is still a no-op.

- [ ] **Step 3: Implement doRenewal**

Replace the `doRenewal` stub in `SimpleDurableServiceExecutor.scala`:

```scala
  protected def doRenewal(serviceUUID: UUID): Unit =
    val serviceKey = Key(serviceUUID)

    def attempt(): Unit =
      servicesTkvl.get(serviceKey).foreach:
        case None =>
          // Service was unregistered; scan will clean up ownedServices on next cycle
          ()
        case Some(vs) =>
          val entry = ServiceEntry.decode(vs.value.bytes)
          val now   = HLCTimestamp.now

          if entry.hostId != hostId.uuid then
            handleLeaseLoss(serviceUUID)
          else if entry.leaseExpiry < now then
            handleLeaseLoss(serviceUUID)
          else
            val newExpiry = HLCTimestamp(now.asLong + (leaseDuration.toMillis << 16))
            val newEntry  = entry.copy(leaseExpiry = newExpiry)
            val tx = client.newTransaction()
            given Transaction = tx
            servicesTkvl.set(serviceKey, Value(newEntry.encode()), Some(Right(vs.revision)))(using tx)
            tx.commit().failed.foreach: _ =>
              attempt()  // retry from a fresh read so we pick up any ownership changes

    attempt()

  private def handleLeaseLoss(serviceUUID: UUID): Unit = synchronized:
    ownedServices.get(serviceUUID).foreach: (service, timer) =>
      timer.cancel()
      service.shutdown()
    ownedServices -= serviceUUID
```

- [ ] **Step 4: Run tests**

```bash
sbt 'testOnly *DurableServiceSuite'
```

Expected: all pass. Renewal test sees `e1.leaseExpiry > e0.leaseExpiry`.

- [ ] **Step 5: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/compute/impl/SimpleDurableServiceExecutor.scala \
        src/test/scala/org/aspen_ddp/aspen/compute/DurableServiceSuite.scala
git commit -m "Implement lease renewal timers with automatic lease-loss detection"
```

---

## Task 8: Lease loss detection + unregistration detection during scan

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/compute/impl/SimpleDurableServiceExecutor.scala`
- Modify: `src/test/scala/org/aspen_ddp/aspen/compute/DurableServiceSuite.scala`

- [ ] **Step 1: Write failing tests**

Add to `DurableServiceSuite`:

```scala
  atest("executor calls shutdown when another host overwrites the TKVL entry"):
    given ExecutionContext = executionContext
    val svcUUID      = UUID.randomUUID()
    val shutdownDone = Promise[Unit]()

    // Override testFactory to capture shutdown
    val capturingFactory = new DurableServiceFactory:
      val typeUUID: UUID = testFactory.typeUUID
      def createService(c: AspenClient, ptr: KeyValueObjectPointer, state: KeyValueObjectState): DurableService =
        claimedPromise.trySuccess(())
        new DurableService:
          def shutdown(): Unit = shutdownDone.trySuccess(())

    net = new TestNetwork(executionContext, List(capturingFactory))
    client = net.client
    radicle = net.radicle

    val exec = makeExecutor()
    for
      _  <- exec.registerService(capturingFactory.typeUUID, svcUUID, Map.empty)
      _  <- claimedPromise.future               // wait for claim
      // Overwrite TKVL entry with a different hostId to simulate another host taking over
      vs <- exec.servicesTkvl.get(Key(svcUUID))
      fakeEntry = ServiceEntry.decode(vs.get.value.bytes).copy(hostId = UUID.randomUUID())
      tx  = client.newTransaction()
      _  <- exec.servicesTkvl.set(Key(svcUUID), Value(fakeEntry.encode()))(using tx)
      _  <- tx.commit()
      _  <- shutdownDone.future                  // wait for shutdown() call
    yield
      exec.shutdown()
      succeed

  atest("executor calls shutdown when service is removed from TKVL during scan"):
    given ExecutionContext = executionContext
    val svcUUID      = UUID.randomUUID()
    val shutdownDone = Promise[Unit]()

    val capturingFactory = new DurableServiceFactory:
      val typeUUID: UUID = testFactory.typeUUID
      def createService(c: AspenClient, ptr: KeyValueObjectPointer, state: KeyValueObjectState): DurableService =
        claimedPromise.trySuccess(())
        new DurableService:
          def shutdown(): Unit = shutdownDone.trySuccess(())

    net = new TestNetwork(executionContext, List(capturingFactory))
    client = net.client
    radicle = net.radicle

    val exec = makeExecutor()
    for
      _  <- exec.registerService(capturingFactory.typeUUID, svcUUID, Map.empty)
      _  <- claimedPromise.future              // wait for claim
      _  <- exec.unregisterService(svcUUID)   // remove from TKVL (Task 9)
      _  <- shutdownDone.future               // scan must detect removal and call shutdown
    yield
      exec.shutdown()
      succeed
```

- [ ] **Step 2: Run tests — expect failure**

```bash
sbt 'testOnly *DurableServiceSuite -- -z "calls shutdown"'
```

Expected: both tests time out waiting for their promises.

- [ ] **Step 3: Add unregistration check to the scan loop**

In the `scanFn` body, after building `tkvlUUIDs`, fill in the unregistration check block:

```scala
      // Unregistration check
      synchronized:
        val removed = currentOwned.filter(svcUUID => !tkvlUUIDs.contains(svcUUID))
        removed.foreach: svcUUID =>
          ownedServices.get(svcUUID).foreach: (service, timer) =>
            timer.cancel()
            service.shutdown()
          ownedServices -= svcUUID
```

The lease-loss path (renewal revision mismatch) is already handled by `handleLeaseLoss` inside `doRenewal` from Task 7.

- [ ] **Step 4: Run tests — expect pass**

```bash
sbt 'testOnly *DurableServiceSuite'
```

Expected: all pass. Both shutdown tests complete.

- [ ] **Step 5: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/compute/impl/SimpleDurableServiceExecutor.scala \
        src/test/scala/org/aspen_ddp/aspen/compute/DurableServiceSuite.scala
git commit -m "Add unregistration detection in scan loop; lease-loss in renewal handles revision mismatch"
```

---

## Task 9: unregisterService

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/compute/impl/SimpleDurableServiceExecutor.scala`
- Modify: `src/test/scala/org/aspen_ddp/aspen/compute/DurableServiceSuite.scala`

- [ ] **Step 1: Write a failing test**

Add to `DurableServiceSuite`:

```scala
  atest("unregisterService removes TKVL entry and shuts down owned service"):
    given ExecutionContext = executionContext
    val svcUUID = UUID.randomUUID()
    val exec    = makeExecutor()
    for
      _   <- exec.registerService(testFactory.typeUUID, svcUUID, Map.empty)
      _   <- claimedPromise.future
      _   <- exec.unregisterService(svcUUID)
      vs  <- exec.servicesTkvl.get(Key(svcUUID))
    yield
      exec.shutdown()
      vs shouldBe None

  atest("unregisterService on unowned service just removes TKVL entry"):
    given ExecutionContext = executionContext
    val svcUUID = UUID.randomUUID()
    val exec    = makeExecutor()
    for
      // Register with a different typeUUID that has no factory — entry stays unclaimed
      _ <- exec.registerService(UUID.randomUUID(), svcUUID, Map.empty)
      _ <- Future { Thread.sleep(200) }  // give scan time to run (and fail to find factory)
      _ <- exec.unregisterService(svcUUID)
      vs <- exec.servicesTkvl.get(Key(svcUUID))
    yield
      exec.shutdown()
      vs shouldBe None
```

- [ ] **Step 2: Run test — expect failure**

```bash
sbt 'testOnly *DurableServiceSuite -- -z "unregisterService removes"'
```

Expected: `vs` is still `Some(...)` because `unregisterService` returns `Future.unit`.

- [ ] **Step 3: Implement unregisterService**

Replace the stub in `SimpleDurableServiceExecutor.scala`:

```scala
  override def unregisterService(serviceUUID: UUID): Future[Unit] =
    // Shut down and remove from ownedServices if this host owns it
    synchronized:
      ownedServices.get(serviceUUID).foreach: (service, timer) =>
        timer.cancel()
        service.shutdown()
      ownedServices -= serviceUUID

    val serviceKey = Key(serviceUUID)

    servicesTkvl.get(serviceKey).flatMap:
      case None => Future.unit  // already gone
      case Some(_) =>
        client.transactUntilSuccessful[Unit]: tx =>
          given Transaction = tx
          servicesTkvl.delete(serviceKey)(using tx)
```

- [ ] **Step 4: Run tests**

```bash
sbt 'testOnly *DurableServiceSuite'
```

Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/compute/impl/SimpleDurableServiceExecutor.scala \
        src/test/scala/org/aspen_ddp/aspen/compute/DurableServiceSuite.scala
git commit -m "Implement unregisterService: remove TKVL entry and shutdown owned service"
```

---

## Task 10: StoreManager wiring + timer cleanup

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/server/StoreManager.scala`
- Modify: `src/main/scala/org/aspen_ddp/aspen/client/AspenClient.scala` (if `backgroundTaskManager` not yet in trait)
- Modify: `src/test/scala/org/aspen_ddp/aspen/TestNetwork.scala`

All timers inside `SimpleDurableServiceExecutor` must be cancelled before the test `BackgroundTaskManager` shuts down. Failure to do so causes callbacks to fire after host shutdown, producing intermittent errors in subsequent tests.

- [ ] **Step 1: Add backgroundTaskManager to AspenClient if missing**

Open `src/main/scala/org/aspen_ddp/aspen/client/AspenClient.scala`. If `def backgroundTaskManager: BackgroundTaskManager` is not in the trait, add it. Ensure `TestNetwork.TClient` and any production `AspenClient` implementations provide it.

- [ ] **Step 2: Wire SimpleDurableServiceExecutor into StoreManager**

In `StoreManager.scala`:

Add import:
```scala
import org.aspen_ddp.aspen.compute.impl.SimpleDurableServiceExecutor
```

Add a field near the other executor fields:

```scala
  private val serviceExecutorPromise: Promise[SimpleDurableServiceExecutor] = Promise()
```

At the end of `initializeTaskExecutor()` (after `taskExecutorPromise.success(executor)`), add:

```scala
                    val serviceExec = new SimpleDurableServiceExecutor(
                      client, hostId, backgroundTasks
                    )
                    serviceExecutorPromise.success(serviceExec)
```

Add a getter:

```scala
  def getServiceExecutor(): Future[SimpleDurableServiceExecutor] = serviceExecutorPromise.future
```

In the `shutdown()` method of `StoreManager` (where other cleanup happens), add:

```scala
    serviceExecutorPromise.future.foreach(_.shutdown())
```

- [ ] **Step 3: Ensure TestNetwork shuts down the executor**

`TestNetwork.TClient.shutdown()` calls `backgroundTaskManager.shutdown(Duration(50, MILLISECONDS))`. `SimpleDurableServiceExecutor.shutdown()` cancels all timers before that. The executor is created by `StoreManager` in production but NOT in `TestNetwork` (which bypasses StoreManager entirely). Therefore, tests that create a `SimpleDurableServiceExecutor` directly must call `exec.shutdown()` before the test ends — which all tests in `DurableServiceSuite` already do via `exec.shutdown()` in the `yield` block.

Double-check that every test in `DurableServiceSuite` calls `exec.shutdown()` before returning `succeed`. Any test that returns early (e.g. on a failed assertion) must also shut down. Use a `try/finally` guard if needed.

- [ ] **Step 4: Run all tests**

```bash
sbt test
```

Expected: full test suite passes. No intermittent timer-callback errors.

- [ ] **Step 5: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/server/StoreManager.scala \
        src/main/scala/org/aspen_ddp/aspen/client/AspenClient.scala \
        src/test/scala/org/aspen_ddp/aspen/TestNetwork.scala
git commit -m "Wire SimpleDurableServiceExecutor into StoreManager; ensure timer cleanup on shutdown"
```

---

## Self-Review Notes

**Spec coverage check:**
- Core traits ✓ (Task 1)
- ServiceEntry encoding ✓ (Task 2)
- Radicle key + Bootstrap ✓ (Task 3)
- registerService idempotent ✓ (Task 5)
- Scan with randomized interval ✓ (Task 6 — `ignoreExtraCallsWhileRunning` + randomized schedule)
- Claim with load-weighted delay ✓ (Task 6 — `claimDelayPerService × ownedServices.size`)
- Revision-based claim atomicity ✓ (Task 6 — `Some(Right(oldRevision))`)
- Lease renewal every 5s ✓ (Task 7)
- Lease loss → shutdown ✓ (Task 7 — `handleLeaseLoss` in `doRenewal`)
- Unregistration detection during scan ✓ (Task 8)
- unregisterService ✓ (Task 9)
- StoreManager wiring ✓ (Task 10)
- **Timer cleanup on host shutdown ✓** (Task 4 `shutdown()` + Task 10 reminder + test discipline)
- `DoesNotExist` concurrency safety in registerService ✓ (Task 5 — `Some(Left(true))` + `recoverWith`)
