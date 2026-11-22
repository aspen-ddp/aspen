package org.aspen_ddp.aspen.server.host

import java.util.UUID

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** CopyPhase reads no marker any more (stage 2, Task 5): a flat copy is Live, whatever files it carries. Task 6 deletes
 *  the reader */
class CopyPhaseSuite extends AnyFunSuite with Matchers:

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

  test("parameters are kept verbatim: garbage is distinct from empty"):
    Phase.Arrived("xyz") should not be(Phase.Arrived(""))
    Phase.Rebuilt("xyz") should not be(Phase.Rebuilt(""))
    Phase.Outbound("xyz") should not be(Phase.Outbound(""))

  test("no phase is read from a former marker file"):
    val u = UUID.randomUUID()
    withStore("rebuild-required" -> "")(dir => CopyPhase.read(dir) should be(Phase.Live))
    withStore("transferring-out" -> "t")(dir => CopyPhase.read(dir) should be(Phase.Live))
    withStore("rebuild-complete" -> u.toString)(dir => CopyPhase.read(dir) should be(Phase.Live))
    withStore("transferred-in" -> "t1")(dir => CopyPhase.read(dir) should be(Phase.Live))
