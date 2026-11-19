package org.aspen_ddp.aspen.server.crl

import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.util.UUID
import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicLong

class CrlAdmissionSuite extends AnyFunSuite with Matchers:

  test("each tier is admitted below its own ceiling"):
    val a = new CrlAdmission(800)
    a.ceiling(AdmissionTier.Data) shouldBe 600
    a.ceiling(AdmissionTier.Metadata) shouldBe 700
    a.ceiling(AdmissionTier.Bootstrap) shouldBe 800
    a.tryAdmit(600, AdmissionTier.Data) shouldBe true
    a.full(AdmissionTier.Data) shouldBe true
    a.tryAdmit(1, AdmissionTier.Data) shouldBe false
    a.tryAdmit(100, AdmissionTier.Metadata) shouldBe true
    a.full(AdmissionTier.Metadata) shouldBe true
    a.tryAdmit(1, AdmissionTier.Metadata) shouldBe false
    a.full(AdmissionTier.Bootstrap) shouldBe false
    a.tryAdmit(100, AdmissionTier.Bootstrap) shouldBe true
    a.tryAdmit(1, AdmissionTier.Bootstrap) shouldBe false
    a.release(250)
    a.tryAdmit(1, AdmissionTier.Data) shouldBe true

  test("raiseLimit only raises the limit, and ceilings follow it"):
    val l = 800L
    val a = new CrlAdmission(l)
    a.raiseLimit(2 * l)
    a.limitBytes shouldBe 2 * l
    a.ceiling(AdmissionTier.Data) shouldBe 2 * l - 2 * l / 4
    a.raiseLimit(l)
    a.limitBytes shouldBe 2 * l
    a.ceiling(AdmissionTier.Data) shouldBe 2 * l - 2 * l / 4

  test("an unlimited admission admits every tier"):
    val a = CrlAdmission.Unlimited
    AdmissionTier.values.foreach: t =>
      a.ceiling(t) should be > Long.MaxValue / 2
      a.tryAdmit(1L << 40, t) shouldBe true

  test("a pool's tier"):
    import org.aspen_ddp.aspen.common.metadata.fixed_ids
    import org.aspen_ddp.aspen.common.pool.PoolId
    AdmissionTier.forPool(fixed_ids.BootstrapPoolId, isMetadataPool = true) shouldBe AdmissionTier.Bootstrap
    AdmissionTier.forPool(fixed_ids.InitialMetadataPoolId, isMetadataPool = true) shouldBe AdmissionTier.Metadata
    AdmissionTier.forPool(PoolId(java.util.UUID.randomUUID()), isMetadataPool = false) shouldBe AdmissionTier.Data

  test("admits below the limit and refuses at it"):
    val a = new CrlAdmission(100)
    a.tryAdmit(60) shouldBe true
    // Still below the limit, so one transaction may cross it
    a.tryAdmit(60) shouldBe true
    a.admittedBytes shouldBe 120
    a.tryAdmit(1) shouldBe false
    a.admittedBytes shouldBe 120

    val exact = new CrlAdmission(100)
    exact.tryAdmit(100) shouldBe true
    exact.tryAdmit(1) shouldBe false

  test("release makes room"):
    val a = new CrlAdmission(100)
    a.tryAdmit(100) shouldBe true
    a.tryAdmit(10) shouldBe false
    a.release(100)
    a.admittedBytes shouldBe 0
    a.tryAdmit(10) shouldBe true
    a.admittedBytes shouldBe 10

  test("concurrent admits from many threads never exceed limit + one transaction"):
    val limit = 10000L
    val size = 7L
    val a = new CrlAdmission(limit)
    val admitted = new AtomicLong(0)
    val start = new CountDownLatch(1)
    val threads = (1 to 16).map: _ =>
      val t = new Thread(() =>
        start.await()
        for _ <- 1 to 2000 do
          if a.tryAdmit(size) then admitted.addAndGet(size))
      t.start()
      t
    start.countDown()
    threads.foreach(_.join())

    a.admittedBytes shouldBe admitted.get
    a.admittedBytes should be >= limit
    a.admittedBytes should be < (limit + size)

  test("unlimited admits everything"):
    val a = CrlAdmission.Unlimited
    a.tryAdmit(Long.MaxValue / 4) shouldBe true
    a.tryAdmit(Long.MaxValue / 4) shouldBe true

  test("a loaded state is charged, retained for its store, only while the total stays at most limit + slack"):
    val s0 = StoreId(PoolId(new UUID(3, 1)), 0)
    val a = new CrlAdmission(100)
    a.charge(60)
    a.tryChargeLoaded(s0, 51, slack = 10) shouldBe false
    a.admittedBytes shouldBe 60
    a.retainedBytes(s0) shouldBe 0
    a.tryChargeLoaded(s0, 50, slack = 10) shouldBe true
    a.admittedBytes shouldBe 110
    a.retainedBytes(s0) shouldBe 50
    // What is already retained for the store is the same records, saved again in place: left out of the check
    a.tryChargeLoaded(s0, 50, slack = 10) shouldBe true
    a.admittedBytes shouldBe 160
    a.releaseRetained(s0)
    a.admittedBytes shouldBe 60

  test("a loaded state's check does not overflow for an unlimited admission"):
    val s0 = StoreId(PoolId(new UUID(3, 1)), 0)
    val a = CrlAdmission.Unlimited
    a.tryChargeLoaded(s0, 1000, slack = Long.MaxValue) shouldBe true
    a.admittedBytes shouldBe 1000

  test("a retained charge read earlier is released only while nothing was retained or released for its store since"):
    val s0 = StoreId(PoolId(new UUID(3, 1)), 0)
    val a = new CrlAdmission(1000)
    a.chargeRetained(s0, 40)
    val unchanged = a.retainedCharge(s0)
    unchanged.map(_.bytes) shouldBe Some(40)
    a.releaseRetained(s0, unchanged.get)
    a.admittedBytes shouldBe 0
    a.retainedBytes(s0) shouldBe 0
    a.retainedCharge(s0) shouldBe None

  test("a retained charge read before a load charged its state leaves both charges counted"):
    val s0 = StoreId(PoolId(new UUID(3, 1)), 0)
    val a = new CrlAdmission(1000)
    a.chargeRetained(s0, 40)
    val before = a.retainedCharge(s0).get
    a.tryChargeLoaded(s0, 70, slack = 10) shouldBe true
    a.releaseRetained(s0, before)
    a.admittedBytes shouldBe 110
    a.retainedBytes(s0) shouldBe 110
    // The load's recovery releases all of it, once it has counted the records itself
    a.releaseRetained(s0)
    a.admittedBytes shouldBe 0

  test("a retained charge is matched by identity, not by size"):
    val s0 = StoreId(PoolId(new UUID(3, 1)), 0)
    val a = new CrlAdmission(1000)
    a.chargeRetained(s0, 40)
    val stale = a.retainedCharge(s0).get
    a.releaseRetained(s0)
    a.chargeRetained(s0, 40)
    a.releaseRetained(s0, stale)
    a.admittedBytes shouldBe 40
    a.retainedBytes(s0) shouldBe 40
