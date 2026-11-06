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

  private val config = Config(imbalance = 0.3, movesPerCheck = 2)

  test("a new store goes to the least loaded thread, then the one with fewest stores, then the lowest index"):
    StoreBalancer.placeNew(3, List(placed(1, 0, 0.5), placed(2, 1, 0.2), placed(3, 2, 0.4))) should be(1)
    StoreBalancer.placeNew(3, List(placed(1, 0, 0.0), placed(2, 0, 0.0), placed(3, 1, 0.0))) should be(2)
    StoreBalancer.placeNew(2, Nil) should be(0)

  test("overloaded compares the average thread load with growAbove"):
    val c = Config(growAbove = 0.7)
    StoreBalancer.overloaded(2, List(placed(1, 0, 0.8), placed(2, 1, 0.7)), c) should be(true)
    StoreBalancer.overloaded(2, List(placed(1, 0, 1.0), placed(2, 1, 0.3)), c) should be(false)

  test("plan moves the store that best evens out the busiest and least busy threads"):
    // Gap 0.6: the 0.3 store halves it exactly; the 0.5 store would leave a 0.4 gap the other way
    val stores = List(placed(1, 0, 0.5), placed(2, 0, 0.3), placed(3, 0, 0.1), placed(4, 1, 0.3))
    StoreBalancer.plan(2, stores, config, force = false) should be(List(Move(store(2), 0, 1)))

  test("plan leaves threads within imbalance alone unless forced"):
    val stores = List(placed(1, 0, 0.3), placed(2, 0, 0.2), placed(3, 1, 0.25))
    StoreBalancer.plan(2, stores, config, force = false) should be(Nil)
    // Forced (a thread was just added): gap 0.25, and the 0.2 store brings the two loads to 0.3 and 0.45
    StoreBalancer.plan(2, stores, config, force = true) should be(List(Move(store(2), 0, 1)))

  test("plan never moves a store that would leave the pair no better"):
    // A single store as busy as the gap: moving it only swaps which thread is busy
    StoreBalancer.plan(2, List(placed(1, 0, 0.9)), config, force = true) should be(Nil)

  test("plan skips stores that are not movable"):
    val stores = List(placed(1, 0, 0.4, movable = false), placed(2, 0, 0.4))
    StoreBalancer.plan(2, stores, config, force = false) should be(List(Move(store(2), 0, 1)))
    StoreBalancer.plan(2, List(placed(1, 0, 0.4, movable = false), placed(2, 0, 0.4, movable = false)), config,
      force = false) should be(Nil)

  test("plan makes at most movesPerCheck moves and moves a store at most once"):
    // A new, empty third thread and six equal stores on thread 0: each move takes one store from the busiest thread
    val stores = (1 to 6).map(n => placed(n, 0, 0.2)) :+ placed(7, 1, 0.6)
    val moves = StoreBalancer.plan(3, stores, config.copy(movesPerCheck = 10), force = true)
    moves.map(_.storeId).distinct.size should be(moves.size)
    val after = stores.map(s => moves.find(_.storeId == s.storeId).fold(s)(m => s.copy(thread = m.to)))
    StoreBalancer.threadLoads(3, after).map(l => math.round(l * 10) / 10.0) should be(Vector(0.6, 0.6, 0.6))
    StoreBalancer.plan(3, stores, config.copy(movesPerCheck = 1), force = true).size should be(1)

  test("plan does nothing with one thread"):
    StoreBalancer.plan(1, List(placed(1, 0, 0.9), placed(2, 0, 0.9)), config, force = true) should be(Nil)
