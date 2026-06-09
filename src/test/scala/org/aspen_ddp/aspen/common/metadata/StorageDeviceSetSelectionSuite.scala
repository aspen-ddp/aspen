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

  test("level 1: distributes evenly across member sets (counts differ by <= 1)"):
    val leaves = List.fill(3)(leaf(List.fill(4)(dev())))
    val root = upper(1, leaves)
    val deviceToLeaf: Map[StorageDeviceId, StorageDeviceSetId] =
      leaves.flatMap(l => l.memberDevices.map(_ -> l.setId)).toMap

    val result = Await.result(
      root.selectDevices(7, Set.empty, lookupFor(leaves*), new Random(11)), timeout)

    result.length should be(7)
    val perLeaf = result.groupBy(deviceToLeaf).view.mapValues(_.size).values.toList
    (perLeaf.max - perLeaf.min) should be <= 1

  test("level 1: no duplicates when each member set has enough distinct devices"):
    val leaves = List.fill(3)(leaf(List.fill(4)(dev())))
    val root = upper(1, leaves)

    // numStores=6, 3 member sets => ceil(6/3)=2 visits each, each leaf has 4 devices.
    val result = Await.result(
      root.selectDevices(6, Set.empty, lookupFor(leaves*), new Random(12)), timeout)

    result.length should be(6)
    result.distinct.length should be(6)

  test("level 1: duplicates only from an exhausted member set, distribution still even"):
    val a1 = dev()
    val small = leaf(List(a1))            // only 1 device
    val big = leaf(List.fill(5)(dev()))   // plenty
    val root = upper(1, List(small, big))
    val deviceToLeaf: Map[StorageDeviceId, StorageDeviceSetId] =
      (small.memberDevices ++ big.memberDevices).map { d =>
        d -> (if d == a1 then small.setId else big.setId)
      }.toMap

    // numStores=4, 2 member sets => 2 visits each. `small` is revisited but has 1 device.
    val result = Await.result(
      root.selectDevices(4, Set.empty, lookupFor(small, big), new Random(13)), timeout)

    result.length should be(4)
    result.count(_ == a1) should be(2)        // the only possible duplicate, from `small`
    result.distinct.length should be(3)
    val perLeaf = result.groupBy(deviceToLeaf).view.mapValues(_.size).toMap
    perLeaf(small.setId) should be(2)
    perLeaf(big.setId) should be(2)

  test("level 2: chains down through nested sets to leaf devices"):
    val leavesA = List.fill(2)(leaf(List.fill(3)(dev())))
    val leavesB = List.fill(2)(leaf(List.fill(3)(dev())))
    val branchA = upper(1, leavesA)
    val branchB = upper(1, leavesB)
    val root = upper(2, List(branchA, branchB))
    val allDevices = (leavesA ++ leavesB).flatMap(_.memberDevices).toSet
    val lookup = lookupFor((leavesA ++ leavesB ++ List(branchA, branchB))*)

    val result = Await.result(root.selectDevices(4, Set.empty, lookup, new Random(14)), timeout)

    result.length should be(4)
    result.toSet.subsetOf(allDevices) should be(true)

  test("level 1: empty member sets fails with AllocationError"):
    val root = upper(1, Nil)
    assertThrows[AllocationError]:
      Await.result(root.selectDevices(2, Set.empty, noLookup, new Random(15)), timeout)
