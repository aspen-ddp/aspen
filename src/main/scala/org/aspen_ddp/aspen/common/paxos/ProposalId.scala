package org.aspen_ddp.aspen.common.paxos

case class ProposalId(number: Int, peer: Byte) extends Ordered[ProposalId] {
  def nextProposal = ProposalId(number+1, peer)

  def compare(rhs: ProposalId): Int = if (number == rhs.number)
    peer - rhs.peer
  else
    number - rhs.number
}

object ProposalId {
  def initialProposal(peer: Byte) = ProposalId(1, peer)
}
