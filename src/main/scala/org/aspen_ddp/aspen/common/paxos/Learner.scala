package org.aspen_ddp.aspen.common.paxos

class Learner( val numPeers: Int, val quorumSize: Int ) {

  require(quorumSize >= numPeers/2 + 1)

  private[this] var highestProposalId: Option[ProposalId] = None
  private[this] val peersAccepted = new java.util.BitSet(numPeers)
  private[this] var resolvedValue: Option[Boolean] = None

  def finalValue: Option[Boolean] = resolvedValue
  def peerBitset: java.util.BitSet = peersAccepted.clone().asInstanceOf[java.util.BitSet]

  def receiveAccepted(m: Accepted): Option[Boolean] = resolvedValue match {
    case Some(v) =>
      if (m.proposalId >= highestProposalId.get) {
        // If a peer gained permission to send an Accept message for a proposal id higher than the
        // one we saw achieve resolution, the peer must have learned of the consensual value. We can
        // therefore update our peer map to show that this peer has correctly accepted the final value
        peersAccepted.set(m.fromPeer)
      }
      Some(v)

    case None =>
      if (highestProposalId.isEmpty)
        highestProposalId = Some(m.proposalId)

      if (m.proposalId > highestProposalId.get) {
        peersAccepted.clear()
        highestProposalId = Some(m.proposalId)
      }

      if (m.proposalId == highestProposalId.get)
        peersAccepted.set(m.fromPeer)

      if (peersAccepted.cardinality() >= quorumSize)
        resolvedValue = Some(m.proposalValue)

      resolvedValue
  }
}
