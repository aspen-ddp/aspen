package org.aspen_ddp.aspen.server.host

import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.transaction.TransactionHorizon
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.util.UUID
import scala.concurrent.duration.{FiniteDuration, MILLISECONDS}

class JoinCeilingSuite extends AnyFunSuite with Matchers:

  private val lead = TransactionHorizon.MaxStartLead.toMillis
  private val step = FiniteDuration(100, MILLISECONDS)
  private val storeA = StoreId(PoolId(new UUID(0, 7)), 0)
  private val storeB = StoreId(PoolId(new UUID(0, 7)), 1)

  /** In memory; a write or removal throws while its flag is set */
  private class Cell(initial: Option[Long] = None) extends JoinCeiling.Persistence:
    var value: Option[Long] = initial
    var writes = 0
    var failWrites = false
    var failRemoves = false
    def read(): Option[Long] = value
    def write(ceiling: Long): Unit =
      if failWrites then throw new java.io.IOException("write failed")
      writes += 1
      value = Some(ceiling)
    def remove(): Unit =
      if failRemoves then throw new java.io.IOException("remove failed")
      value = None

  test("a renewal persists the lead plus the step past now, and only ever raises the ceiling"):
    var now = 10_000L
    val cell = new Cell()
    val ceiling = new JoinCeiling(Some(cell), step, () => now)
    ceiling.current shouldBe None
    ceiling.mayJoin(Long.MaxValue - 1) shouldBe true
    ceiling.renew()
    cell.value shouldBe Some(now + lead + step.toMillis)
    ceiling.current shouldBe cell.value
    // A clock stepped back lowers nothing, and writes nothing
    now -= 5_000
    ceiling.renew()
    cell.writes shouldBe 1
    ceiling.current shouldBe Some(10_000L + lead + step.toMillis)

  test("a store joins only transactions started below the persisted ceiling"):
    val ceiling = new JoinCeiling(Some(new Cell()), step, () => 0L)
    ceiling.renew()
    val c = ceiling.current.get
    ceiling.mayJoin(c - 1) shouldBe true
    ceiling.mayJoin(c) shouldBe false

  test("a store's first load in a process is bounded by the ceiling the last process left, a reload by the one now"):
    var now = 50_000L
    val ceiling = new JoinCeiling(Some(new Cell(Some(20_000L))), step, () => now)
    ceiling.atStart shouldBe Some(20_000L)
    ceiling.renew()
    ceiling.boundAtLoad(storeA) shouldBe Some(20_000L)
    ceiling.boundAtLoad(storeB) shouldBe Some(20_000L)
    now += 1_000
    ceiling.renew()
    ceiling.boundAtLoad(storeA) shouldBe Some(now + lead + step.toMillis)

  test("a renewal that fails removes the ceiling, and the process gates and bounds nothing more"):
    val cell = new Cell(Some(1_000L))
    val ceiling = new JoinCeiling(Some(cell), step, () => 5_000L)
    cell.failWrites = true
    ceiling.renew()
    cell.value shouldBe None
    ceiling.current shouldBe None
    ceiling.mayJoin(1_000_000L) shouldBe true
    ceiling.boundAtLoad(storeA) shouldBe Some(1_000L) // the last process's joins are still below it
    ceiling.boundAtLoad(storeA) shouldBe None
    cell.failWrites = false
    ceiling.renew()
    cell.writes shouldBe 0

  test("a renewal that can neither write nor remove the ceiling keeps gating below it"):
    val cell = new Cell(Some(1_000L))
    val ceiling = new JoinCeiling(Some(cell), step, () => 5_000L)
    cell.failWrites = true
    cell.failRemoves = true
    ceiling.renew()
    ceiling.current shouldBe Some(1_000L)
    ceiling.mayJoin(1_000L) shouldBe false
    cell.failWrites = false
    ceiling.renew()
    ceiling.current shouldBe Some(5_000L + lead + step.toMillis)

  test("the file persists the ceiling, and one that cannot be read bounds nothing"):
    val dir = os.temp.dir(prefix = "aspen-join-ceiling", deleteOnExit = false)
    try
      val file = new JoinCeiling.FilePersistence(dir)
      file.read() shouldBe None
      new JoinCeiling(Some(file), step, () => 7_000L).renew()
      new JoinCeiling(Some(new JoinCeiling.FilePersistence(dir))).atStart shouldBe Some(7_000L + lead + step.toMillis)
      os.write.over(dir / JoinCeiling.FileName, "garbage")
      new JoinCeiling(Some(new JoinCeiling.FilePersistence(dir))).atStart shouldBe None
      file.remove()
      os.exists(dir / JoinCeiling.FileName) shouldBe false
    finally os.remove.all(dir)

  test("an unbounded ceiling gates and bounds nothing"):
    JoinCeiling.Unbounded.renew()
    JoinCeiling.Unbounded.current shouldBe None
    JoinCeiling.Unbounded.mayJoin(Long.MaxValue) shouldBe true
    JoinCeiling.Unbounded.boundAtLoad(storeA) shouldBe None
