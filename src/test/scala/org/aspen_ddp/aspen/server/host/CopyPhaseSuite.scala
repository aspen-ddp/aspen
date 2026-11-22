package org.aspen_ddp.aspen.server.host

import java.util.UUID

import org.aspen_ddp.aspen.server.rebuild.RebuildingStore
import org.aspen_ddp.aspen.server.transfer.StoreTransfer
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class CopyPhaseSuite extends AnyFunSuite with Matchers:

  private val RR = RebuildingStore.RebuildRequiredMarkerFile
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
    withStore(TO -> "")(dir => CopyPhase.read(dir) should be(Phase.Outbound(None)))
    withStore(TO -> u.toString)(dir => CopyPhase.read(dir) should be(Phase.Outbound(Some(u))))
    withStore(RC -> u.toString)(dir => CopyPhase.read(dir) should be(Phase.Rebuilt(Some(u))))
    withStore(RC -> "")(dir => CopyPhase.read(dir) should be(Phase.Rebuilt(None)))
    withStore(RR -> "")(dir => CopyPhase.read(dir) should be(Phase.Lost))

  test("precedence is RR > TO > TI > RC"):
    val u = UUID.randomUUID()
    withStore(RR -> "", TO -> "")(dir => CopyPhase.read(dir) should be(Phase.Lost))
    withStore(TO -> "", TI -> "t1")(dir => CopyPhase.read(dir) should be(Phase.Outbound(None)))
    withStore(TI -> "t1", RC -> u.toString)(dir => CopyPhase.read(dir) should be(Phase.Arrived("t1")))
    withStore(TO -> "", RC -> u.toString)(dir => CopyPhase.read(dir) should be(Phase.Outbound(None)))

  test("a failed stat throws and known answers None"):
    withStore(RC -> ""): dir =>
      os.perms.set(dir, "rw-------")
      an[java.io.IOException] should be thrownBy CopyPhase.read(dir)
      CopyPhase.known(dir) should be(None)
