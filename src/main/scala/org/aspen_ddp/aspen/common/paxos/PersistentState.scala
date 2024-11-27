package org.aspen_ddp.aspen.common.paxos

case class PersistentState(
                            promised: Option[ProposalId],
                            accepted: Option[(ProposalId, Boolean)])

object PersistentState {
  def initial: PersistentState = PersistentState(None, None)
}
