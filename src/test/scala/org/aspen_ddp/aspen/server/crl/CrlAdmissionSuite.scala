package org.aspen_ddp.aspen.server.crl

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

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
