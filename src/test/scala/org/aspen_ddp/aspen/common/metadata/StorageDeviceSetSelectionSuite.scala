package org.aspen_ddp.aspen.common.metadata

import org.aspen_ddp.aspen.client.AllocationError
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.util.UUID
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration.*
import scala.util.Random

object StorageDeviceSetSelectionSuite:
  given ExecutionContext = ExecutionContext.global
  val timeout: FiniteDuration = 5.seconds

  def dev(): StorageDeviceId = StorageDeviceId(UUID.randomUUID())

  /** A level-0 set containing the given devices. */
  def leaf(devices: List[StorageDeviceId]): StorageDeviceSetState =
    StorageDeviceSetState(
      setId = StorageDeviceSetId(UUID.randomUUID()),
      name = "leaf",
      level = 0,
      parent = None,
      memberDevices = devices,
      memberSets = Nil,
      assignedPools = Nil
    )

  /** A higher-level set over the given member sets. */
  def upper(level: Int, members: List[StorageDeviceSetState]): StorageDeviceSetState =
    StorageDeviceSetState(
      setId = StorageDeviceSetId(UUID.randomUUID()),
      name = "upper",
      level = level,
      parent = None,
      memberDevices = Nil,
      memberSets = members.map(_.setId),
      assignedPools = Nil
    )

  /** Build a lookup over every supplied state. */
  def lookupFor(states: StorageDeviceSetState*): StorageDeviceSetId => Future[StorageDeviceSetState] =
    val m = states.map(s => s.setId -> s).toMap
    id => Future.successful(m(id))

  /** Lookup that fails if ever called (level-0 tests never recurse). */
  val noLookup: StorageDeviceSetId => Future[StorageDeviceSetState] =
    _ => Future.failed(new RuntimeException("lookup should not be called"))

class StorageDeviceSetSelectionSuite extends AnyFunSuite with Matchers:
  import StorageDeviceSetSelectionSuite.{*, given}

  test("level 0: numStores < members yields distinct subset"):
    val devices = List.fill(5)(dev())
    val set = leaf(devices)
    val result = Await.result(set.selectDevices(3, Set.empty, noLookup, new Random(1)), timeout)
    result.length should be(3)
    result.distinct.length should be(3)
    result.toSet.subsetOf(devices.toSet) should be(true)

  test("level 0: numStores == members yields a full permutation"):
    val devices = List.fill(4)(dev())
    val set = leaf(devices)
    val result = Await.result(set.selectDevices(4, Set.empty, noLookup, new Random(2)), timeout)
    result.length should be(4)
    result.toSet should be(devices.toSet)

  test("level 0: numStores > members reuses minimally and evenly"):
    val devices = List.fill(3)(dev())
    val set = leaf(devices)
    val result = Await.result(set.selectDevices(7, Set.empty, noLookup, new Random(3)), timeout)
    result.length should be(7)
    val counts = result.groupBy(identity).view.mapValues(_.size).values.toList
    counts.max should be(3) // ceil(7/3)
    counts.min should be(2) // floor(7/3)
    counts.size should be(3) // every device used

  test("level 0: exclude is honored at numStores == 1"):
    val devices = List.fill(4)(dev())
    val set = leaf(devices)
    val excluded = devices.take(3).toSet
    val result = Await.result(set.selectDevices(1, excluded, noLookup, new Random(4)), timeout)
    result should be(List(devices(3)))

  test("level 0: falls back to a duplicate when all devices are excluded"):
    val devices = List.fill(3)(dev())
    val set = leaf(devices)
    val result = Await.result(set.selectDevices(1, devices.toSet, noLookup, new Random(5)), timeout)
    result.length should be(1)
    devices should contain(result.head)

  test("level 0: identical seed produces identical results"):
    val devices = List.fill(6)(dev())
    val set = leaf(devices)
    val a = Await.result(set.selectDevices(4, Set.empty, noLookup, new Random(123)), timeout)
    val b = Await.result(set.selectDevices(4, Set.empty, noLookup, new Random(123)), timeout)
    a should be(b)

  test("numStores <= 0 returns empty list"):
    val set = leaf(List.fill(3)(dev()))
    Await.result(set.selectDevices(0, Set.empty, noLookup, new Random(6)), timeout) should be(Nil)

  test("level 0: empty member devices fails with AllocationError"):
    val set = leaf(Nil)
    assertThrows[AllocationError]:
      Await.result(set.selectDevices(2, Set.empty, noLookup, new Random(7)), timeout)
