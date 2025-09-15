package org.aspen_ddp.aspen.common.paxos

import org.scalatest._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class AcceptorSuite extends AnyFunSuite with Matchers:

  test("Recover state"):
    val istate = PersistentState(Some(ProposalId(1, 1)), Some((ProposalId(2, 2), false)))

    val a = new Acceptor(0, istate)

    a.persistentState should be(istate)

  test("Promise first"):
    val a = new Acceptor(0)

    a.receivePrepare(Prepare(ProposalId(1, 1))) should be(Right(Promise(0, ProposalId(1, 1), None)))

    a.persistentState should be(PersistentState(Some(ProposalId(1, 1)), None))

  test("Promise higher"):
    val a = new Acceptor(0)

    a.receivePrepare(Prepare(ProposalId(1, 1))) should be(Right(Promise(0, ProposalId(1, 1), None)))
    a.receivePrepare(Prepare(ProposalId(2, 2))) should be(Right(Promise(0, ProposalId(2, 2), None)))
    a.persistentState should be(PersistentState(Some(ProposalId(2, 2)), None))

  test("Nack promise lower"):
    val a = new Acceptor(0)

    a.receivePrepare(Prepare(ProposalId(1, 1))) should be(Right(Promise(0, ProposalId(1, 1), None)))
    a.receivePrepare(Prepare(ProposalId(2, 2))) should be(Right(Promise(0, ProposalId(2, 2), None)))
    a.persistentState should be(PersistentState(Some(ProposalId(2, 2)), None))

    a.receivePrepare(Prepare(ProposalId(1, 0))) should be(Left(Nack(0, ProposalId(1, 0), ProposalId(2, 2))))

  test("Accept first"):
    val a = new Acceptor(0)

    a.receiveAccept(Accept(ProposalId(1, 1), true)) should be(Right(Accepted(0, ProposalId(1, 1), true)))

    a.persistentState should be(PersistentState(Some(ProposalId(1, 1)), Some((ProposalId(1, 1), true))))

  test("Accept higher"):
    val a = new Acceptor(0)

    a.receiveAccept(Accept(ProposalId(1, 1), true)) should be(Right(Accepted(0, ProposalId(1, 1), true)))
    a.receiveAccept(Accept(ProposalId(2, 1), false)) should be(Right(Accepted(0, ProposalId(2, 1), false)))
    a.persistentState should be(PersistentState(Some(ProposalId(2, 1)), Some((ProposalId(2, 1), false))))

  test("Nack accept lower"):
    val a = new Acceptor(0)

    a.receiveAccept(Accept(ProposalId(2, 1), false)) should be(Right(Accepted(0, ProposalId(2, 1), false)))
    a.receiveAccept(Accept(ProposalId(1, 0), false)) should be(Left(Nack(0, ProposalId(1, 0), ProposalId(2, 1))))
    a.persistentState should be(PersistentState(Some(ProposalId(2, 1)), Some((ProposalId(2, 1), false))))