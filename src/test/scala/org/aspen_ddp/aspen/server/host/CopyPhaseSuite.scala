package org.aspen_ddp.aspen.server.host

import java.util.UUID

import org.aspen_ddp.aspen.server.transfer.StoreTransfer
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class CopyPhaseSuite extends AnyFunSuite with Matchers:

  private val TI = StoreTransfer.TransferredInMarkerFile
  private val RC = "rebuild-complete"

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

  test("the transferred-in marker maps to Arrived with its content"):
    withStore(TI -> " t1\n")(dir => CopyPhase.read(dir) should be(Phase.Arrived("t1")))
    withStore(TI -> "")(dir => CopyPhase.read(dir) should be(Phase.Arrived("")))

  test("content is kept verbatim: garbage is distinct from empty"):
    withStore(TI -> " xyz\n")(dir => CopyPhase.read(dir) should be(Phase.Arrived("xyz")))
    Phase.Arrived("xyz") should not be(Phase.Arrived(""))
    Phase.Rebuilt("xyz") should not be(Phase.Rebuilt(""))
    Phase.Outbound("xyz") should not be(Phase.Outbound(""))

  test("Lost, Outbound, Rebuilt and Parked are never read from markers"):
    val u = UUID.randomUUID()
    withStore("rebuild-required" -> "")(dir => CopyPhase.read(dir) should be(Phase.Live))
    withStore("transferring-out" -> "t")(dir => CopyPhase.read(dir) should be(Phase.Live))
    withStore(RC -> u.toString)(dir => CopyPhase.read(dir) should be(Phase.Live))
    withStore(TI -> "t1", RC -> u.toString)(dir => CopyPhase.read(dir) should be(Phase.Arrived("t1")))

  test("a failed stat throws and known answers None"):
    withStore(TI -> ""): dir =>
      os.perms.set(dir, "rw-------")
      an[java.io.IOException] should be thrownBy CopyPhase.read(dir)
      CopyPhase.known(dir) should be(None)
