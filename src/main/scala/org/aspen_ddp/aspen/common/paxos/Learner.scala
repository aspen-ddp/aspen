package org.aspen_ddp.aspen.common.paxos

class Learner( val numPeers: Int, val quorumSize: Int ):

  require(quorumSize >= numPeers/2 + 1)

  private var highestProposalId: Option[ProposalId] = None
  private val peersAccepted = new java.util.BitSet(numPeers)
  private var resolvedValue: Option[Boolean] = None

  def finalValue: Option[Boolean] = resolvedValue
  def peerBitset: java.util.BitSet = peersAccepted.clone().asInstanceOf[java.util.BitSet]

  def receiveAccepted(m: Accepted): Option[Boolean] = resolvedValue match
    case Some(v) =>
      if m.proposalId >= highestProposalId.get then
        // If a peer gained permission to send an Accept message for a proposal id higher than the
        // one we saw achieve resolution, the peer must have learned of the consensual value. We can
        // therefore update our peer map to show that this peer has correctly accepted the final value
        peersAccepted.set(m.fromPeer)
      Some(v)

    case None =>
      if highestProposalId.isEmpty then
        highestProposalId = Some(m.proposalId)

      if m.proposalId > highestProposalId.get then
        peersAccepted.clear()
        highestProposalId = Some(m.proposalId)

      if m.proposalId == highestProposalId.get then
        peersAccepted.set(m.fromPeer)

      if peersAccepted.cardinality() >= quorumSize then
        resolvedValue = Some(m.proposalValue)

      resolvedValue
