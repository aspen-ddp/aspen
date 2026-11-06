package org.aspen_ddp.aspen.server.host

import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.server.host.StoreBalancer.{Config, Move, Placed}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.util.UUID

class StoreBalancerSuite extends AnyFunSuite with Matchers:

  private val Pool = PoolId(new UUID(0, 2))
  private def store(n: Int): StoreId = StoreId(Pool, n.toByte)
  private def placed(n: Int, thread: Int, load: Double, movable: Boolean = true) =
    Placed(store(n), thread, load, movable)

  private val config = Config(target = 0.65, consolidateBelow = 0.5, movesPerCheck = 2)
  private def calm(threads: Int) = Vector.fill(threads)(false)

  test("a new store goes to the lowest-index thread below target and not congested, else the least loaded"):
    val stores = List(placed(1, 0, 0.7), placed(2, 1, 0.3), placed(3, 2, 0.1))
    StoreBalancer.placeNew(3, stores, calm(3), config) should be(1)
    StoreBalancer.placeNew(3, stores, Vector(false, true, false), config) should be(2)
    StoreBalancer.placeNew(2, List(placed(1, 0, 0.9), placed(2, 1, 0.8)), calm(2), config) should be(1)
    StoreBalancer.placeNew(2, Nil, calm(2), config) should be(0)

  test("a thread is needed only when one is congested and none is empty"):
    val twoBusy = List(placed(1, 0, 0.9), placed(2, 1, 0.2))
    StoreBalancer.needsThread(2, twoBusy, Vector(true, false)) should be(true)
    StoreBalancer.needsThread(2, twoBusy, calm(2)) should be(false)
    // However busy, an uncongested pool is keeping up
    StoreBalancer.needsThread(1, List(placed(1, 0, 1.0), placed(2, 0, 0.9)), calm(1)) should be(false)
    StoreBalancer.needsThread(3, twoBusy, Vector(true, false, false)) should be(false)

  test("a thread over target gives its busiest store that fits to a busy thread with room, not an empty one"):
    val stores = List(placed(1, 0, 0.5), placed(2, 0, 0.3), placed(3, 1, 0.1))
    // Thread 2 is empty: an uncongested thread over target is still keeping up, so it is not used
    StoreBalancer.plan(3, stores, calm(3), config) should be(List(Move(store(1), 0, 1)))

  test("a congested thread may give a store to an empty thread"):
    val stores = List(placed(1, 0, 0.5), placed(2, 0, 0.4))
    StoreBalancer.plan(2, stores, calm(2), config) should be(Nil)
    StoreBalancer.plan(2, stores, Vector(true, false), config) should be(List(Move(store(1), 0, 1)))

  test("congestion alone, below target, also relieves the thread"):
    val stores = List(placed(1, 0, 0.3), placed(2, 0, 0.2), placed(3, 1, 0.1))
    StoreBalancer.plan(2, stores, Vector(true, false), config) should be(List(Move(store(1), 0, 1)))

  test("a congested thread never receives a store"):
    val stores = List(placed(1, 0, 0.5), placed(2, 0, 0.3), placed(3, 1, 0.1))
    StoreBalancer.plan(2, stores, Vector(false, true), config) should be(Nil)

  test("a thread with one store is never relieved, and no move just swaps which thread is busy"):
    StoreBalancer.plan(2, List(placed(1, 0, 0.95)), Vector(true, false), config) should be(Nil)
    // Moving the 0.5 store would leave thread 1 at 0.6 against thread 0's 0.55 before: no better
    val stores = List(placed(1, 0, 0.5), placed(2, 0, 0.05), placed(3, 1, 0.1))
    StoreBalancer.plan(2, stores, Vector(true, false), config) should be(List(Move(store(2), 0, 1)))

  test("with nothing to relieve, the highest busy thread's stores move down onto threads with room"):
    val stores = List(placed(1, 0, 0.2), placed(2, 1, 0.1), placed(3, 2, 0.0), placed(4, 2, 0.15))
    // Thread 2's stores go first, least loaded first, each to thread 0 while it stays at most consolidateBelow
    StoreBalancer.plan(3, stores, calm(3), config) should be(List(Move(store(3), 2, 0), Move(store(4), 2, 0)))
    StoreBalancer.plan(3, stores, calm(3), config.copy(movesPerCheck = 10)) should be(
      List(Move(store(3), 2, 0), Move(store(4), 2, 0), Move(store(2), 1, 0)))

  test("consolidation stops at consolidateBelow and skips congested threads"):
    val stores = List(placed(1, 0, 0.4), placed(2, 1, 0.2))
    StoreBalancer.plan(2, stores, calm(2), config) should be(Nil)
    val light = List(placed(1, 0, 0.1), placed(2, 1, 0.2))
    StoreBalancer.plan(2, light, Vector(true, false), config) should be(Nil)

  test("a check that relieves does not also consolidate"):
    // Thread 0 is over target; thread 2's store could otherwise move down to thread 1
    val stores = List(placed(1, 0, 0.5), placed(2, 0, 0.3), placed(3, 1, 0.1), placed(4, 2, 0.05))
    StoreBalancer.plan(3, stores, calm(3), config.copy(movesPerCheck = 10)) should be(List(Move(store(1), 0, 1)))

  test("stores that are not movable stay"):
    val stores = List(placed(1, 0, 0.5, movable = false), placed(2, 0, 0.3), placed(3, 1, 0.1))
    StoreBalancer.plan(2, stores, calm(2), config) should be(List(Move(store(2), 0, 1)))
    val pinned = List(placed(1, 1, 0.1, movable = false), placed(2, 0, 0.1))
    StoreBalancer.plan(2, pinned, calm(2), config) should be(Nil)

  test("plan does nothing with one thread"):
    StoreBalancer.plan(1, List(placed(1, 0, 0.9), placed(2, 0, 0.9)), Vector(true), config) should be(Nil)
