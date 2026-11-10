package org.aspen_ddp.aspen.common.paxos

class Proposer(
                val peerId: Byte,
                val numPeers: Int,
                val quorumSize: Int):

  require(quorumSize >= numPeers/2 + 1)

  private var localProposal: Option[Boolean] = None
  private var proposalId = ProposalId.initialProposal(peerId)
  private var highestProposalId = proposalId
  private var highestAccepted: Option[(ProposalId, Boolean)] = None

  private val promisesReceived = new java.util.BitSet(numPeers)
  private val nacksReceived = new java.util.BitSet(numPeers)
  private var roundHasAcceptedCommit = false
  private var provenCommit = false

  def prepareQuorumReached: Boolean = promisesReceived.cardinality() >= quorumSize

  /** True when the promises of the current round reach a quorum and none of them carries an accepted commit.
    * Accepted values reported in earlier rounds do not count, and neither does an accepted abort
    */
  def prepareQuorumWithoutAcceptedCommit: Boolean = prepareQuorumReached && !roundHasAcceptedCommit

  /** The commit is known to have been chosen (a proof). Commit was then chosen at some ballot c, and every value
    * accepted at c or higher is commit, so an accepted abort predates c and is never adopted. From now on the
    * proposal is commit when a promise of the current round carries an accepted commit, and none otherwise: neither
    * the local proposal nor a value from an earlier round counts. Cannot be undone
    */
  def commitProven(): Unit = provenCommit = true

  def haveLocalProposal: Boolean = localProposal.isDefined

  def numPromises: Int = promisesReceived.cardinality()
  def numNacks: Int = nacksReceived.cardinality()

  def maySendAccept: Boolean = prepareQuorumReached && (highestAccepted.isDefined || localProposal.isDefined)

  def proposalValue: Option[Boolean] =
    if prepareQuorumReached && provenCommit then
      Option.when(roundHasAcceptedCommit)(true)
    else if prepareQuorumReached then
      highestAccepted match
        case Some(t) => Some(t._2)
        case None => localProposal match
          case Some(v) => Some(v)
          case None => None
    else
      None

  /** Sets the proposal value for this node. Once set, it cannot be unset. Subsequent calls are ignored.*/
  def setLocalProposal(value:Boolean): Unit = if localProposal.isEmpty then localProposal = Some(value)

  /** Used to reduce the chance of receiving Nacks to our Prepare messages */
  def updateHighestProposalId(pid: ProposalId): Unit = if pid > highestProposalId then highestProposalId = pid

  /** Abandons the current Paxos round and prepares for the next.
    *
    * After calling this method the current Prepare message will contain a ProposalID higher than any previously
    * seen and all internal state tracking will be reset to represent the new round.
    */
  def nextRound(): Unit =
    proposalId = ProposalId(highestProposalId.number+1, peerId)
    highestProposalId = proposalId
    promisesReceived.clear()
    nacksReceived.clear()
    roundHasAcceptedCommit = false

  def currentProposalId: ProposalId = proposalId

  def currentPrepareMessage(): Prepare = Prepare(proposalId)

  /** Return value contains an Accept message only if we have reached the quorum threshold and we
    *  have a value to propose.
    *
    *  The value may come either from the local proposal or from a peer via the Paxos proposal requirement
    */
  def currentAcceptMessage(): Option[Accept] = proposalValue.map(v => Accept(proposalId,v))

  /** Returns true if this Nack eliminates the possibility of success for the
    * current proposal id. Returns false if success is still possible.
    */
  def receiveNack(nack: Nack) : Boolean =
    updateHighestProposalId(nack.promisedProposalId)

    if nack.proposalId == proposalId then
      nacksReceived.set(nack.fromPeer)

    nacksReceived.cardinality() > numPeers - quorumSize

  def receivePromise(promise: Promise): Unit =
    updateHighestProposalId(promise.proposalId)

    if promise.proposalId == proposalId then
      promisesReceived.set(promise.fromPeer)
      if promise.lastAccepted.exists(_._2) then roundHasAcceptedCommit = true

      promise.lastAccepted.foreach: t =>
        highestAccepted match
          case Some(highest) => if t._1 > highest._1 then highestAccepted = Some(t)
          case None => highestAccepted = Some(t)