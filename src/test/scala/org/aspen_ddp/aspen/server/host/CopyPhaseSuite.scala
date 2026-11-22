package org.aspen_ddp.aspen.server.host

import java.util.UUID

import org.aspen_ddp.aspen.server.rebuild.RebuildingStore
import org.aspen_ddp.aspen.server.transfer.StoreTransfer
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class CopyPhaseSuite extends AnyFunSuite with Matchers:

  private val TO = StoreTransfer.TransferOutMarkerFile
  private val TI = StoreTransfer.TransferredInMarkerFile
  private val RC = RebuildingStore.RebuildCompleteMarkerFile

  private def withStore(markers: (String, String)*)(f: os.Path => Unit): Unit =
    val dir = os.temp.dir()
    try
      markers.foreach((name, content) => os.write(dir / name, content))
      f(dir)
    finally
      os.perms.set(dir, "rwx------")
      os.remove.all(dir)

  test("no marker is Live"):
    withStore(): dir =>
      CopyPhase.read(dir) should be(Phase.Live)
      CopyPhase.known(dir) should be(Some(Phase.Live))

  test("each single marker maps to its phase with its content"):
    val u = UUID.randomUUID()
    withStore(TI -> " t1\n")(dir => CopyPhase.read(dir) should be(Phase.Arrived("t1")))
    withStore(TO -> "")(dir => CopyPhase.read(dir) should be(Phase.Outbound("")))
    withStore(TO -> u.toString)(dir => CopyPhase.read(dir) should be(Phase.Outbound(u.toString)))
    withStore(RC -> u.toString)(dir => CopyPhase.read(dir) should be(Phase.Rebuilt(u.toString)))
    withStore(RC -> "")(dir => CopyPhase.read(dir) should be(Phase.Rebuilt("")))

  test("content is kept verbatim: garbage is distinct from empty"):
    withStore(TO -> " xyz\n")(dir => CopyPhase.read(dir) should be(Phase.Outbound("xyz")))
    withStore(RC -> "xyz")(dir => CopyPhase.read(dir) should be(Phase.Rebuilt("xyz")))
    CopyPhase.read(os.temp.dir()) should not be(Phase.Rebuilt("xyz"))
    Phase.Rebuilt("xyz") should not be(Phase.Rebuilt(""))
    Phase.Outbound("xyz") should not be(Phase.Outbound(""))

  test("precedence is TO > TI > RC, and Lost is never read from markers"):
    val u = UUID.randomUUID()
    withStore("rebuild-required" -> "")(dir => CopyPhase.read(dir) should be(Phase.Live))
    withStore(TO -> "", TI -> "t1")(dir => CopyPhase.read(dir) should be(Phase.Outbound("")))
    withStore(TI -> "t1", RC -> u.toString)(dir => CopyPhase.read(dir) should be(Phase.Arrived("t1")))
    withStore(TO -> "", RC -> u.toString)(dir => CopyPhase.read(dir) should be(Phase.Outbound("")))

  test("a failed stat throws and known answers None"):
    withStore(RC -> ""): dir =>
      os.perms.set(dir, "rw-------")
      an[java.io.IOException] should be thrownBy CopyPhase.read(dir)
      CopyPhase.known(dir) should be(None)
