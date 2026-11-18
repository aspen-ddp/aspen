package org.aspen_ddp.aspen.server.crl.simple

import org.scalatest.funsuite.AnyFunSuite

class LivePiecesSuite extends AnyFunSuite:

  test("A serial stays oldest until every piece added under it is removed") {
    val live = new LivePieces
    assert(live.oldest.isEmpty)
    live.add(5)
    live.add(3)
    live.add(3)
    live.remove(3)
    assert(live.oldest == Some(3L))
    live.remove(3)
    assert(live.oldest == Some(5L))
    live.remove(5)
    assert(live.oldest.isEmpty)
  }

  test("Removing a serial no piece holds is a fault") {
    val live = new LivePieces
    live.add(1)
    intercept[IllegalArgumentException](live.remove(2))
  }
