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
      (Phase.Outbound("t"), "outbound", true),
      (Phase.Rebuilt("e"), "rebuilt", false),
      (Phase.Lost, "lost", true),
      (Phase.Retired, "pending-delete", true))
    table.foreach: (phase, name, converted) =>
      withClue(phase):
        CopyLayout.dirOf(phase) should be(if converted then Some(name) else None)
    val u = UUID.randomUUID()
    CopyLayout.path(tdir, s, Some(u), Phase.Retired) should be(tdir / "pending-delete" / CopyLayout.copyName(s, u))
    CopyLayout.path(tdir, s, Some(u), Phase.Lost) should be(tdir / "lost" / CopyLayout.copyName(s, u))
    CopyLayout.path(tdir, s, Some(u), Phase.Outbound("t")) should be(tdir / "outbound" / CopyLayout.copyName(s, u))
    CopyLayout.path(tdir, s, None, Phase.Rebuilt("e")) should be(tdir / s.directoryName)
    CopyLayout.path(tdir, s, Some(u), Phase.Live) should be(tdir / s.directoryName)

  test("param is the phase's parameter"):
    CopyLayout.param(Phase.Arrived("t")) should be(Some("t"))
    CopyLayout.param(Phase.Outbound("")) should be(Some(""))
    CopyLayout.param(Phase.Rebuilt("e")) should be(Some("e"))
    CopyLayout.param(Phase.Live) should be(None)
    CopyLayout.param(Phase.Lost) should be(None)

  test("each phase's parameter has its own file"):
    CopyLayout.paramFile(Phase.Arrived("t")) should be(Some("phase-param.arrived"))
    CopyLayout.paramFile(Phase.Outbound("t")) should be(Some("phase-param.outbound"))
    CopyLayout.paramFile(Phase.Rebuilt("e")) should be(Some("phase-param.rebuilt"))
    List(Phase.Live, Phase.Lost, Phase.Retired).map(CopyLayout.paramFile) should be(List(None, None, None))

  test("readParam reads an absent file as empty and throws a failed stat"):
    val copy = tdir / "copy"
    os.makeDir.all(copy)
    CopyLayout.readParam(copy, Phase.Rebuilt("")) should be("")
    os.write(copy / CopyLayout.paramFile(Phase.Rebuilt("")).get, "e-1")
    // Each phase reads only its own file
    CopyLayout.readParam(copy, Phase.Rebuilt("")) should be("e-1")
    CopyLayout.readParam(copy, Phase.Outbound("")) should be("")
    CopyLayout.readParam(copy, Phase.Live) should be("")
    // A parent without search permission: the stat fails (EACCES), which is no absence
    val locked = tdir / "locked"
    os.makeDir.all(locked / "copy")
    os.perms.set(locked, "rw-------")
    try an[java.io.IOException] should be thrownBy CopyLayout.readParam(locked / "copy", Phase.Rebuilt(""))
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

  test("an outbound copy's phase is read from its own parameter file, and a failed read throws"):
    val dev = tdir / "dev"
    val sending = CopyFixtures.placeCopy(dev, s, Phase.Outbound("t-1"))
    val parked = CopyFixtures.placeCopy(dev, StoreId(s.poolId, 7), Phase.Outbound(""))
    // Another phase's parameter file is inert there, and a parked copy may have none
    os.write(sending / CopyLayout.paramFile(Phase.Arrived("")).get, "t-other")
    os.remove(parked / CopyLayout.paramFile(Phase.Outbound("")).get)
    CopyLayout.list(dev).copies.map(f => (f.storeId, f.phase, f.path)).toSet should be(Set(
      (s, Phase.Outbound("t-1"), sending),
      (StoreId(s.poolId, 7), Phase.Outbound(""), parked)))
    CopyLayout.phaseAt(sending) should be(Some(Phase.Outbound("t-1")))
    CopyLayout.knownPhase(sending) should be(Some(Phase.Outbound("t-1")))
    // The copy without search permission: its parameter file cannot be stat'd
    os.perms.set(sending, "rw-------")
    try
      an[java.io.IOException] should be thrownBy CopyLayout.list(dev)
      CopyLayout.knownPhase(sending) should be(None)
    finally os.perms.set(sending, "rwx------")
