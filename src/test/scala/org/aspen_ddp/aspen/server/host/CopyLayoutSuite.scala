package org.aspen_ddp.aspen.server.host

import org.aspen_ddp.aspen.FileBasedTests
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId

import java.util.UUID

class CopyLayoutSuite extends FileBasedTests:

  private val s = StoreId(PoolId(UUID.fromString("22222222-2222-2222-2222-222222222222")), 3)

  test("copy names round-trip and carry the store id"):
    val u = UUID.randomUUID()
    CopyLayout.parseName(CopyLayout.copyName(s, u)) should be(Some((s, u)))
    CopyLayout.copyName(s, u) should be(s"${s.directoryName}.$u")
    CopyLayout.parseName(s.directoryName) should be(None)
    CopyLayout.parseName(s"${s.directoryName}.not-a-uuid") should be(None)

  test("every phase maps to the spec's directory once converted"):
    // Each task that converts a phase flips its row to Some
    val table = List(
      (Phase.Live, "live", false),
      (Phase.Arrived("t"), "arrived", false),
      (Phase.Outbound("t"), "outbound", false),
      (Phase.Rebuilt("e"), "rebuilt", false),
      (Phase.Lost, "lost", false),
      (Phase.Retired, "pending-delete", true))
    table.foreach: (phase, name, converted) =>
      withClue(phase):
        CopyLayout.dirOf(phase) should be(if converted then Some(name) else None)
    val u = UUID.randomUUID()
    CopyLayout.path(tdir, s, Some(u), Phase.Retired) should be(tdir / "pending-delete" / CopyLayout.copyName(s, u))
    CopyLayout.path(tdir, s, None, Phase.Lost) should be(tdir / s.directoryName)
    CopyLayout.path(tdir, s, Some(u), Phase.Live) should be(tdir / s.directoryName)

  test("param is the phase's parameter"):
    CopyLayout.param(Phase.Arrived("t")) should be(Some("t"))
    CopyLayout.param(Phase.Outbound("")) should be(Some(""))
    CopyLayout.param(Phase.Rebuilt("e")) should be(Some("e"))
    CopyLayout.param(Phase.Live) should be(None)
    CopyLayout.param(Phase.Lost) should be(None)

  test("readParam reads an absent file as empty and throws a failed stat"):
    val copy = tdir / "copy"
    os.makeDir.all(copy)
    CopyLayout.readParam(copy) should be("")
    os.write(copy / CopyLayout.PhaseParamFile, "t-1")
    CopyLayout.readParam(copy) should be("t-1")
    // A parent without search permission: the stat fails (EACCES), which is no absence
    val locked = tdir / "locked"
    os.makeDir.all(locked / "copy")
    os.perms.set(locked, "rw-------")
    try an[java.io.IOException] should be thrownBy CopyLayout.readParam(locked / "copy")
    finally os.perms.set(locked, "rwx------")

  test("a child whose name is not a copy name is reported, not listed"):
    val dev = tdir / "dev"
    val retired = CopyFixtures.placeCopy(dev, s, Phase.Retired)
    val live = CopyFixtures.placeCopy(dev, StoreId(s.poolId, 4))
    // Each converted phase directory holds the operator's strays
    val strays = List(CopyLayout.dirOf(Phase.Retired).get).flatMap: d =>
      os.makeDir.all(dev / d / "stray")
      os.write(dev / d / "notes.txt", "")
      List(dev / d / "stray", dev / d / "notes.txt")
    val listing = CopyLayout.list(dev)
    listing.strays.toSet should be(strays.toSet)
    listing.copies.map(f => (f.storeId, f.phase, f.path)).toSet should be(Set(
      (s, Phase.Retired, retired),
      (StoreId(s.poolId, 4), Phase.Live, live)))
    listing.copies.find(_.phase == Phase.Retired).flatMap(_.copyId) should be(CopyLayout.parseName(retired.last).map(_._2))

  test("a flat directory counts as a copy by its store's name, and scratch areas never do"):
    val dev = tdir / "dev"
    CopyFixtures.placeCopy(dev, s)
    // A copy that lost its config is still a copy: the Missing Stores pass rebuilds its store, a retire takes it
    os.makeDir.all(dev / StoreId(s.poolId, 5).directoryName)
    // A file is no copy
    os.write(dev / StoreId(s.poolId, 6).directoryName, "")
    List("creating", "deleting", "transferring-in", "rebuilding", "crl", "crl.creating").foreach(d =>
      os.makeDir.all(dev / d))
    CopyLayout.list(dev).copies.map(_.storeId).toSet should be(Set(s, StoreId(s.poolId, 5)))

  test("a failed listing throws"):
    val dev = tdir / "dev"
    CopyFixtures.placeCopy(dev, s, Phase.Retired)
    val dir = dev / CopyLayout.dirOf(Phase.Retired).get
    os.perms.set(dir, "-wx------")
    try an[java.io.IOException] should be thrownBy CopyLayout.list(dev)
    finally os.perms.set(dir, "rwx------")
