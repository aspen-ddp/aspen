package org.aspen_ddp.aspen.server.host

import org.aspen_ddp.aspen.IntegrationTestSuite
import org.aspen_ddp.aspen.common.metadata.StorageDeviceId
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.server.{DurableFiles, Host}
import org.aspen_ddp.aspen.server.crl.CrashRecoveryLog
import org.aspen_ddp.aspen.server.rebuild.RebuildingStore

import java.util.UUID
import scala.concurrent.Future

/** CopyMoves: every phase change of a copy is one move, the target phase's parameter file first, then a rename forced
 *  in both directories (spec "Uniform move and Unsynced"). Until a phase is converted these drive the one phase directory there is,
 *  Retired, from a flat Lost copy */
class CopyMovesSuite extends IntegrationTestSuite with ControlFixture:

  private val s = StoreId(PoolId(UUID.fromString("33333333-3333-3333-3333-333333333333")), 0)

  private def withDevice(): (Host, LocalStorageDeviceState, os.Path) =
    val hostRoot = newHostDir()
    val dev = writeDevice(hostRoot, "dev0", deviceA)
    os.makeDir.all(CrashRecoveryLog.deviceCrlDir(dev))
    CopyFixtures.placeCopy(dev, s, Phase.Lost)
    val host = newHost(hostRoot, verifyCopyRegistry = true)
    host.testingOnlyHandleEvents()
    (host, loadedDevices(host)(deviceA), dev)

  private def record(host: Host, storeId: StoreId): Option[CopyRecord] =
    host.control.copyRegistry.device(deviceA).flatMap(_.copies.get(storeId))

  private def scratch(): os.Path =
    val d = os.temp.dir(prefix = "aspen-copy-moves", deleteOnExit = false)
    tempRoots += d
    d

  private def injected(): java.io.IOException = new java.io.IOException("injected failure")

  test("a move writes the target phase's parameter before the rename and records the phase"):
    val (host, local, dev) = withDevice()
    val from = CopyFixtures.copyPath(dev, s).get
    var renames = List.empty[(os.Path, os.Path)]
    val rename = host.control.moves.rename
    host.control.moves.rename = (a, b) =>
      renames ::= (a, b)
      CopyLayout.param(Phase.Retired).foreach(p => CopyLayout.readParam(a, Phase.Retired) should be(p))
      rename(a, b)
    host.control.moves.move("test", local, s, Phase.Retired) should be(MoveResult.Durable)
    host.testingOnlyHandleEvents()
    renames.map(_._1) should be(List(from))
    CopyFixtures.retiredCopies(dev, s) should be(renames.map(_._2))
    CopyFixtures.copyPath(dev, s) should be(None)
    // A durable move to Retired takes the copy out of the registry (P3); marker files left behind are stripped (R-P2)
    record(host, s) should be(None)
    os.list(renames.head._2).map(_.last).toSet should be(Set(org.aspen_ddp.aspen.server.StoreConfig.configFilename))

    // A target phase that carries a parameter has it in place, durably, before the rename
    val tdir = scratch()
    val a = tdir / "a"
    val b = tdir / "b"
    os.makeDir.all(a / "copy")
    os.makeDir.all(b)
    var seen = Option.empty[String]
    host.control.moves.rename = (x, y) =>
      seen = Some(CopyLayout.readParam(x, Phase.Outbound("")))
      rename(x, y)
    host.control.moves.relocate(a / "copy", b / "copy", fromFlat = false, toFlat = false,
      Phase.Outbound("t-7")) should be(MoveResult.Durable)
    seen should be(Some("t-7"))
    CopyLayout.readParam(b / "copy", Phase.Outbound("")) should be("t-7")

  test("a copy leaving a phase directory for the flat path is stripped of markers first, or not moved"):
    val (host, _, _) = withDevice()
    val tdir = scratch()
    val copy = tdir / "a" / "copy"
    os.makeDir.all(copy)
    os.write(copy / RebuildingStore.RebuildCompleteMarkerFile, "")
    host.control.moves.syncDirectory = d => if d == copy then throw injected()
    host.control.moves.relocate(copy, tdir / "flat", fromFlat = false, toFlat = true, Phase.Live) shouldBe
      a[MoveResult.NotMoved]
    os.isDir(copy) should be(true)
    host.control.moves.syncDirectory = DurableFiles.syncDirectory
    host.control.moves.relocate(copy, tdir / "flat", fromFlat = false, toFlat = true, Phase.Live) should be(
      MoveResult.Durable)
    os.list(tdir / "flat") should be(Nil)

  test("a sync that fails after the rename leaves the record unsynced and the next resync clears it"):
    val (host, local, dev) = withDevice()
    val target = dev / CopyLayout.dirOf(Phase.Retired).get
    var failures = 1
    host.control.moves.syncDirectory = d =>
      if d == target && failures > 0 then
        failures -= 1
        throw injected()
      DurableFiles.syncDirectory(d)
    host.control.moves.move("test", local, s, Phase.Retired) shouldBe a[MoveResult.Unsynced]
    host.testingOnlyHandleEvents()
    record(host, s).map(r => (r.phase, r.unsynced)) should be(Some((Phase.Retired, true)))
    CopyFixtures.retiredCopies(dev, s).size should be(1)
    host.control.moves.resync(local) should be(List(s))
    host.testingOnlyHandleEvents()
    // Synced, the retired copy leaves the registry as a durable move's does
    record(host, s) should be(None)
    host.control.moves.resync(local) should be(Nil)

  // Review finding (fix round 1): a move writes only its target phase's parameter file, so a failed rename (or a crash
  // before it) leaves the source reading its own parameter. No param-carrying phase is converted yet: driven through
  // relocate, from a copy in a phase directory whose own Rebuilt file names its episode, to Outbound
  test("a failed move to a phase with a parameter leaves the source's own parameter as it was"):
    val (host, _, _) = withDevice()
    val before = record(host, s)
    val tdir = scratch()
    val copy = tdir / "rebuilt" / "copy"
    os.makeDir.all(copy)
    os.makeDir.all(tdir / "outbound")
    os.write(copy / CopyLayout.paramFile(Phase.Rebuilt("")).get, "episode-e")
    host.control.moves.rename = (_, _) => throw injected()
    host.control.moves.relocate(copy, tdir / "outbound" / "copy", fromFlat = false, toFlat = false,
      Phase.Outbound("transfer-t")) shouldBe a[MoveResult.NotMoved]
    host.testingOnlyHandleEvents()
    os.isDir(copy) should be(true)
    CopyLayout.readParam(copy, Phase.Rebuilt("")) should be("episode-e")
    // The target's file written ahead of the rename is inert where the copy still is
    CopyLayout.readParam(copy, Phase.Outbound("")) should be("transfer-t")
    record(host, s) should be(before)
    host.control.copyRegistry.takeRegistryErrors() should be(Vector())

  test("a rename that fails changes nothing"):
    val (host, local, dev) = withDevice()
    val before = record(host, s)
    host.control.moves.rename = (_, _) => throw injected()
    host.control.moves.move("test", local, s, Phase.Retired) shouldBe a[MoveResult.NotMoved]
    host.testingOnlyHandleEvents()
    CopyFixtures.phaseOf(dev, s) should be(Some(Phase.Lost))
    CopyFixtures.retiredCopies(dev, s) should be(Nil)
    record(host, s) should be(before)
    host.control.copyRegistry.takeRegistryErrors() should be(Vector())

  test("DurableFiles.move throws MoveNotForced only after a landed rename"):
    val tdir = scratch()
    val source = tdir / "source"
    val target = tdir / "dir" / "target"
    os.makeDir.all(source)
    os.makeDir.all(target / os.up)
    val thrown = intercept[DurableFiles.MoveNotForced]:
      DurableFiles.move(source, target, sync = _ => throw injected())
    os.exists(target) should be(true)
    os.exists(source) should be(false)
    thrown.source should be(source)
    thrown.target should be(target)
    // A rename that fails is no MoveNotForced: nothing moved
    val missing = intercept[Exception](DurableFiles.move(tdir / "gone", tdir / "elsewhere"))
    missing.isInstanceOf[DurableFiles.MoveNotForced] should be(false)
