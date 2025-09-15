package org.aspen_ddp.aspen.common.paxos

import org.scalatest._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class LearnerSuite extends AnyFunSuite with Matchers:

  test("Basic Resolution"):

    val l = new Learner(3, 2)

    l.receiveAccepted(Accepted(0, ProposalId(1, 0), true)) should be(None)
    l.receiveAccepted(Accepted(1, ProposalId(1, 0), true)) should be(Some(true))

    val b = l.peerBitset

    b.get(0) should be(true)
    b.get(1) should be(true)
    b.get(2) should be(false)

  test("Ignore duplicates"):

    val l = new Learner(3, 2)

    l.receiveAccepted(Accepted(0, ProposalId(1, 0), true)) should be(None)
    l.receiveAccepted(Accepted(0, ProposalId(1, 0), true)) should be(None)
    l.receiveAccepted(Accepted(0, ProposalId(1, 0), true)) should be(None)
    l.receiveAccepted(Accepted(1, ProposalId(1, 0), true)) should be(Some(true))

    val b = l.peerBitset

    b.get(0) should be(true)
    b.get(1) should be(true)
    b.get(2) should be(false)

  test("Add peer bits after resolution"):

    val l = new Learner(3, 2)

    l.receiveAccepted(Accepted(0, ProposalId(1, 0), true)) should be(None)
    l.receiveAccepted(Accepted(1, ProposalId(1, 0), true)) should be(Some(true))

    var b = l.peerBitset

    b.get(0) should be(true)
    b.get(1) should be(true)
    b.get(2) should be(false)

    l.receiveAccepted(Accepted(2, ProposalId(1, 0), true)) should be(Some(true))

    b = l.peerBitset

    b.get(0) should be(true)
    b.get(1) should be(true)
    b.get(2) should be(true)

  test("Watch only highest round"):

    val l = new Learner(3, 2)

    l.receiveAccepted(Accepted(0, ProposalId(5, 0), true)) should be(None)
    l.receiveAccepted(Accepted(1, ProposalId(1, 0), true)) should be(None)
    l.receiveAccepted(Accepted(2, ProposalId(1, 0), true)) should be(None)
    l.receiveAccepted(Accepted(2, ProposalId(5, 0), true)) should be(Some(true))

    val b = l.peerBitset

    b.get(0) should be(true)
    b.get(1) should be(false)
    b.get(2) should be(true)

  test("Clear lower round state on higher round seen"):

    val l = new Learner(3, 2)

    l.receiveAccepted(Accepted(0, ProposalId(1, 0), true)) should be(None)

    var b = l.peerBitset

    b.get(0) should be(true)
    b.get(1) should be(false)
    b.get(2) should be(false)

    l.receiveAccepted(Accepted(1, ProposalId(5, 0), true)) should be(None)

    b = l.peerBitset

    b.get(0) should be(false)
    b.get(1) should be(true)
    b.get(2) should be(false)

    l.receiveAccepted(Accepted(2, ProposalId(5, 0), true)) should be(Some(true))

    b = l.peerBitset

    b.get(0) should be(false)
    b.get(1) should be(true)
    b.get(2) should be(true)