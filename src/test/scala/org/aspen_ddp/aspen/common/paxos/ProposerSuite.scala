package org.aspen_ddp.aspen.common.paxos

import org.scalatest._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class ProposerSuite extends AnyFunSuite with Matchers {
  test("Update Proposal ID") {
	  val p = new Proposer(0, 3, 2)
	  
	  p.currentPrepareMessage().proposalId should be (ProposalId.initialProposal(0))
	  
	  p.updateHighestProposalId(ProposalId(4,1))
	  
	  p.nextRound()
	  
	  p.currentPrepareMessage().proposalId should be (ProposalId(5,0))
	}
  
  test("Prepare quorum ignores duplicate promises") {
    val p = new Proposer(0, 3, 2)
    
    p.prepareQuorumReached should be (false)
    p.receivePromise(Promise(0, ProposalId(1,0), None))
    p.prepareQuorumReached should be (false)
    p.receivePromise(Promise(0, ProposalId(1,0), None))
    p.prepareQuorumReached should be (false)
    p.receivePromise(Promise(1, ProposalId(1,0), None))
    p.prepareQuorumReached should be (true)
  }
  
  test("Next round discards previous state") {
    val p = new Proposer(0, 3, 2)
    p.setLocalProposal(true)
    p.prepareQuorumReached should be (false)
    p.receivePromise(Promise(0, ProposalId(1,0), None))
    p.prepareQuorumReached should be (false)
    p.receivePromise(Promise(0, ProposalId(1,0), None))
    p.prepareQuorumReached should be (false)
    p.receivePromise(Promise(1, ProposalId(1,0), None))
    p.prepareQuorumReached should be (true)
    p.currentAcceptMessage() should be (Some(Accept(ProposalId(1,0), true)))
    
    p.nextRound()
    p.prepareQuorumReached should be (false)
    p.currentAcceptMessage() should be (None)
  }
  
  test("Nack handling") {
    val p = new Proposer(0, 3, 2)
    p.setLocalProposal(true)
    p.nextRound()
    p.prepareQuorumReached should be (false)
    p.receivePromise(Promise(0, ProposalId(2,0), None))
    p.prepareQuorumReached should be (false)
    p.receiveNack(Nack(2, ProposalId(2,0), ProposalId(1,0))) should be (false)
    p.prepareQuorumReached should be (false)
    p.numNacks should be (1)
    p.numPromises should be (1)
    p.receivePromise(Promise(1, ProposalId(2,0), None))
    p.prepareQuorumReached should be (true)
    p.currentAcceptMessage() should be (Some(Accept(ProposalId(2,0), true)))
    
    p.nextRound()
    p.prepareQuorumReached should be (false)
    p.currentAcceptMessage() should be (None)
    p.numNacks should be (0)
    p.receiveNack(Nack(2, ProposalId(3,0), ProposalId(4,0))) should be (false)
    p.receiveNack(Nack(2, ProposalId(3,0), ProposalId(4,0))) should be (false)
    p.receiveNack(Nack(1, ProposalId(3,0), ProposalId(4,0))) should be (true)
  }
  
  test("Leadership acquisition before local value proposal") {
    val p = new Proposer(0, 3, 2)
    
    p.currentAcceptMessage() should be (None)
    p.receivePromise(Promise(0, ProposalId(1,0), None))
    p.currentAcceptMessage() should be (None)
    p.receivePromise(Promise(1, ProposalId(1,0), None))
    p.currentAcceptMessage() should be (None)
    p.setLocalProposal(true)
    p.currentAcceptMessage() should be (Some(Accept(ProposalId(1,0), true)))
  }
  
  test("Leadership acquisition after local value proposal") {
    val p = new Proposer(0, 3, 2)
    p.setLocalProposal(true)
    p.currentAcceptMessage() should be (None)
    p.receivePromise(Promise(0, ProposalId(1,0), None))
    p.currentAcceptMessage() should be (None)
    p.receivePromise(Promise(1, ProposalId(1,0), None))
    p.currentAcceptMessage() should be (Some(Accept(ProposalId(1,0), true)))
  }
  
  test("Leadership acquisition with already accepted proposals") {
    val p = new Proposer(0, 3, 2)
    p.nextRound()
    p.currentPrepareMessage().proposalId should be (ProposalId(2,0))
    
    p.receivePromise(Promise(0, ProposalId(2,0), None))
    p.currentAcceptMessage() should be (None)
    p.receivePromise(Promise(1, ProposalId(2,0), Some((ProposalId(1,1),false))))
    p.currentAcceptMessage() should be (Some(Accept(ProposalId(2,0), false)))
  }
  
  test("Leadership acquisition with already accepted proposals overrides local proposal") {
    val p = new Proposer(0, 3, 2)
    p.setLocalProposal(true)
    
    p.nextRound()
    p.currentPrepareMessage().proposalId should be (ProposalId(2,0))
    
    p.receivePromise(Promise(0, ProposalId(2,0), None))
    p.currentAcceptMessage() should be (None)
    p.receivePromise(Promise(1, ProposalId(2,0), Some((ProposalId(1,1),false))))
    p.currentAcceptMessage() should be (Some(Accept(ProposalId(2,0), false)))
  }
}