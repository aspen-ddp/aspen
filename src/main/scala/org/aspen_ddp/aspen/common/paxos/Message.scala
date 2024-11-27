package org.aspen_ddp.aspen.common.paxos

sealed abstract class Message

case class Prepare(proposalId: ProposalId) extends Message

case class Nack(fromPeer:Byte,
                proposalId: ProposalId,
                promisedProposalId: ProposalId) extends Message

case class Promise(fromPeer: Byte,
                   proposalId: ProposalId,
                   lastAccepted: Option[(ProposalId, Boolean)]) extends Message

case class Accept(proposalId: ProposalId,
                  proposalValue: Boolean) extends Message

case class Accepted(fromPeer: Byte,
                    proposalId: ProposalId,
                    proposalValue: Boolean) extends Message
