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

  test("every phase maps to the spec's directory"):
    val table = List(
      (Phase.Live, "live"),
      (Phase.Arrived("t"), "arrived"),
      (Phase.Outbound("t"), "outbound"),
      (Phase.Rebuilt("e"), "rebuilt"),
      (Phase.Parked, "parked"),
      (Phase.Lost, "lost"),
      (Phase.Retired, "pending-delete"))
    val u = UUID.randomUUID()
    table.foreach: (phase, name) =>
      withClue(phase):
        CopyLayout.dirOf(phase) should be(name)
        CopyLayout.isPhaseDirectory(name) should be(true)
        CopyLayout.path(tdir, s, u, phase) should be(tdir / name / CopyLayout.copyName(s, u))
        CopyLayout.isLivePath(CopyLayout.path(tdir, s, u, phase)) should be(phase == Phase.Live)
    CopyLayout.isLivePath(tdir / s.directoryName) should be(false)
    val fresh = CopyLayout.freshLivePath(tdir, s)
    fresh / os.up should be(tdir / "live")
    CopyLayout.parseName(fresh.last).map(_._1) should be(Some(s))

  test("locate finds a copy's phase by its location, and None where no phase directory holds it"):
    val dev = tdir / "dev-locate"
    val u = UUID.randomUUID()
    CopyLayoutDisk.locate(dev, s, u) should be(None)
    os.makeDir.all(CopyLayout.path(dev, s, u, Phase.Outbound("")))
    os.write(CopyLayout.path(dev, s, u, Phase.Outbound("")) / CopyLayout.paramFile(Phase.Outbound("")).get, "t-1")
    CopyLayoutDisk.locate(dev, s, u) should be(Some(Phase.Outbound("t-1")))
    CopyLayoutDisk.locate(dev, s, UUID.randomUUID()) should be(None)
    // A phase directory without search permission: its stat fails, which is no absence
    os.perms.set(dev / "outbound", "rw-------")
    try an[java.io.IOException] should be thrownBy CopyLayoutDisk.locate(dev, s, u)
    finally os.perms.set(dev / "outbound", "rwx------")

  test("param is the phase's parameter"):
    CopyLayout.param(Phase.Arrived("t")) should be(Some("t"))
    CopyLayout.param(Phase.Outbound("")) should be(Some(""))
    CopyLayout.param(Phase.Rebuilt("e")) should be(Some("e"))
    CopyLayout.param(Phase.Live) should be(None)
    CopyLayout.param(Phase.Lost) should be(None)
    CopyLayout.param(Phase.Parked) should be(None)

  test("each phase's parameter has its own file"):
    CopyLayout.paramFile(Phase.Arrived("t")) should be(Some("phase-param.arrived"))
    CopyLayout.paramFile(Phase.Outbound("t")) should be(Some("phase-param.outbound"))
    CopyLayout.paramFile(Phase.Rebuilt("e")) should be(Some("phase-param.rebuilt"))
    List(Phase.Live, Phase.Parked, Phase.Lost, Phase.Retired).map(CopyLayout.paramFile) should be(List(None, None, None, None))

  test("readParam reads an absent file as empty and throws a failed stat"):
    val copy = tdir / "copy"
    os.makeDir.all(copy)
    CopyLayoutDisk.readParam(copy, Phase.Rebuilt("")) should be("")
    os.write(copy / CopyLayout.paramFile(Phase.Rebuilt("")).get, "e-1")
    // Each phase reads only its own file
    CopyLayoutDisk.readParam(copy, Phase.Rebuilt("")) should be("e-1")
    CopyLayoutDisk.readParam(copy, Phase.Outbound("")) should be("")
    CopyLayoutDisk.readParam(copy, Phase.Live) should be("")
    // A parent without search permission: the stat fails (EACCES), which is no absence
    val locked = tdir / "locked"
    os.makeDir.all(locked / "copy")
    os.perms.set(locked, "rw-------")
    try an[java.io.IOException] should be thrownBy CopyLayoutDisk.readParam(locked / "copy", Phase.Rebuilt(""))
    finally os.perms.set(locked, "rwx------")

  test("a child whose name is not a copy name is reported, not listed"):
    val dev = tdir / "dev"
    val retired = CopyFixtures.placeCopy(dev, s, Phase.Retired)
    val live = CopyFixtures.placeCopy(dev, StoreId(s.poolId, 4))
    // Each phase directory holds the operator's strays
    val strays = List(CopyLayout.dirOf(Phase.Retired), CopyLayout.dirOf(Phase.Live)).flatMap: d =>
      os.makeDir.all(dev / d / "stray")
      os.write(dev / d / "notes.txt", "")
      List(dev / d / "stray", dev / d / "notes.txt")
    val listing = CopyLayoutDisk.list(dev)
    listing.strays.toSet should be(strays.toSet)
    listing.copies.map(f => (f.storeId, f.phase, f.path)).toSet should be(Set(
      (s, Phase.Retired, retired),
      (StoreId(s.poolId, 4), Phase.Live, live)))
    listing.copies.find(_.phase == Phase.Retired).map(_.copyId) should be(CopyLayout.parseName(retired.last).map(_._2))

  test("only a phase directory's children are copies: a store-named directory at the device root and scratch areas never are"):
    val dev = tdir / "dev"
    val live = CopyFixtures.placeCopy(dev, s)
    // A copy that lost its config is still a copy: the Missing Stores pass rebuilds its store, a retire takes it
    val unconfigured = CopyFixtures.placeCopy(dev, StoreId(s.poolId, 5), configured = false)
    // The stage-1 flat path is nothing now
    os.makeDir.all(dev / StoreId(s.poolId, 6).directoryName)
    List("creating", "deleting", "transferring-in", "rebuilding", "crl", "crl.creating").foreach(d =>
      os.makeDir.all(dev / d / CopyLayout.copyName(StoreId(s.poolId, 8), UUID.randomUUID())))
    val listing = CopyLayoutDisk.list(dev)
    listing.copies.map(f => (f.storeId, f.phase, f.path)).toSet should be(Set(
      (s, Phase.Live, live), (StoreId(s.poolId, 5), Phase.Live, unconfigured)))
    listing.strays should be(Nil)

  test("a file named like a copy is a stray, never a copy, and a child whose stat fails is listed"):
    val dev = tdir / "dev-files"
    val real = CopyFixtures.placeCopy(dev, s)
    val file = CopyFixtures.freshCopyPath(dev, s)
    os.write(file, "")
    // A copy name linking into a directory without search permission: its stat fails (EACCES)
    val closed = tdir / "closed-files"
    os.makeDir.all(closed / "inside")
    val unstat = CopyFixtures.freshCopyPath(dev, StoreId(s.poolId, 9))
    os.symlink(unstat, closed / "inside")
    os.perms.set(closed, "rw-------")
    try
      val listing = CopyLayoutDisk.list(dev)
      listing.strays should be(List(file))
      listing.copies.map(_.path).toSet should be(Set(real, unstat))
    finally os.perms.set(closed, "rwx------")

  test("no phase is read from a former marker file: a live/ copy is Live whatever it carries"):
    val dev = tdir / "dev-markers"
    val copies = List("rebuild-required" -> "", "transferring-out" -> "t", "rebuild-complete" -> UUID.randomUUID().toString,
      "transferred-in" -> "t1").zipWithIndex.map: (marker, i) =>
        val copy = CopyFixtures.placeCopy(dev, StoreId(s.poolId, i.toByte))
        os.write(copy / marker._1, marker._2)
        copy
    CopyLayoutDisk.list(dev).copies.map(f => (f.phase, f.path)).toSet should be(copies.map(Phase.Live -> _).toSet)

  test("a failed listing throws"):
    val dev = tdir / "dev"
    CopyFixtures.placeCopy(dev, s, Phase.Retired)
    val dir = dev / CopyLayout.dirOf(Phase.Retired)
    os.perms.set(dir, "-wx------")
    try an[java.io.IOException] should be thrownBy CopyLayoutDisk.list(dev)
    finally os.perms.set(dir, "rwx------")

  test("a rebuilt copy's episode is read from its own parameter file, and a parked copy's phase is its location"):
    val dev = tdir / "dev-rebuilt"
    val rebuilt = CopyFixtures.placeCopy(dev, s, Phase.Rebuilt("e-1"))
    val parked = CopyFixtures.placeCopy(dev, StoreId(s.poolId, 7), Phase.Parked)
    // Another phase's parameter file is inert in each
    os.write(rebuilt / CopyLayout.paramFile(Phase.Outbound("")).get, "t-other")
    os.write(parked / CopyLayout.paramFile(Phase.Rebuilt("")).get, "e-other")
    os.write(parked / "rebuild-complete", "e-other")
    CopyLayoutDisk.list(dev).copies.map(f => (f.storeId, f.phase, f.path)).toSet should be(Set(
      (s, Phase.Rebuilt("e-1"), rebuilt),
      (StoreId(s.poolId, 7), Phase.Parked, parked)))
    rebuilt / os.up should be(dev / "rebuilt")
    parked / os.up should be(dev / "parked")
    CopyLayoutDisk.phaseAt(rebuilt) should be(Some(Phase.Rebuilt("e-1")))
    os.perms.set(rebuilt, "rw-------")
    try
      an[java.io.IOException] should be thrownBy CopyLayoutDisk.list(dev)
      an[java.io.IOException] should be thrownBy CopyLayoutDisk.phaseAt(rebuilt)
    finally os.perms.set(rebuilt, "rwx------")

  test("an outbound copy's phase is read from its own parameter file, and a failed read throws"):
    val dev = tdir / "dev"
    val sending = CopyFixtures.placeCopy(dev, s, Phase.Outbound("t-1"))
    val parked = CopyFixtures.placeCopy(dev, StoreId(s.poolId, 7), Phase.Outbound(""))
    // Another phase's parameter file is inert there, and an outbound copy may have none
    os.write(sending / CopyLayout.paramFile(Phase.Arrived("")).get, "t-other")
    os.remove(parked / CopyLayout.paramFile(Phase.Outbound("")).get)
    CopyLayoutDisk.list(dev).copies.map(f => (f.storeId, f.phase, f.path)).toSet should be(Set(
      (s, Phase.Outbound("t-1"), sending),
      (StoreId(s.poolId, 7), Phase.Outbound(""), parked)))
    CopyLayoutDisk.phaseAt(sending) should be(Some(Phase.Outbound("t-1")))
    // The copy without search permission: its parameter file cannot be stat'd
    os.perms.set(sending, "rw-------")
    try
      an[java.io.IOException] should be thrownBy CopyLayoutDisk.list(dev)
      an[java.io.IOException] should be thrownBy CopyLayoutDisk.phaseAt(sending)
    finally os.perms.set(sending, "rwx------")
