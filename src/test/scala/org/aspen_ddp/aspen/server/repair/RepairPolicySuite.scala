package org.aspen_ddp.aspen.server.repair

import org.aspen_ddp.aspen.IntegrationTestSuite
import org.aspen_ddp.aspen.common.metadata.{HostId, HostState}
import org.aspen_ddp.aspen.common.pool.PoolId

import scala.concurrent.Future
import scala.concurrent.duration.{Duration, MINUTES, SECONDS}

class RepairPolicySuite extends IntegrationTestSuite:

  test("the repair policy defaults preserve the CLI repair timings"):
    Future.successful:
      RepairPolicy.Default.scanIntervalFloor shouldBe Duration(30, SECONDS)
      RepairPolicy.Default.scanIntervalCap shouldBe Duration(15, MINUTES)
      RepairPolicy.Default.minErrorEntryAgeForDeletion shouldBe Duration(60, SECONDS)
      HostRepairLimits.Default.maxConcurrentStoreScans shouldBe 4

  test("a repair policy survives a codec round trip"):
    Future.successful:
      val p = RepairPolicy(Duration(5, SECONDS), Duration(90, SECONDS), Duration(120, SECONDS))
      RepairPolicy.decode(p.encode()) shouldBe p

  test("host repair limits survive a codec round trip"):
    Future.successful:
      val l = HostRepairLimits(11)
      HostRepairLimits.decode(l.encode()) shouldBe l

  // proto3 scalars read as zero when never written, which is also what a record written before
  // a field was added looks like. Zero must mean "use the default", not "scan every 0 seconds".
  test("unset fields in an encoded policy fall back to the defaults"):
    Future.successful:
      RepairPolicy.decode(Array[Byte]()) shouldBe RepairPolicy.Default
      HostRepairLimits.decode(Array[Byte]()) shouldBe HostRepairLimits.Default

  test("a pool with no repair policy key reads as the default policy"):
    RepairPolicy.read(client, PoolId.BootstrapPoolId).map: p =>
      p shouldBe RepairPolicy.Default

  test("a host with no repair limits key reads as the default limits"):
    HostRepairLimits.read(client, HostId.BootstrapHostId).map: l =>
      l shouldBe HostRepairLimits.Default
