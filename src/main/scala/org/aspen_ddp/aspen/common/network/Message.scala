package org.aspen_ddp.aspen.common.network

import org.aspen_ddp.aspen.client.HostId

import java.util.UUID
import org.aspen_ddp.aspen.common.{DataBuffer, HLCTimestamp}
import org.aspen_ddp.aspen.common.objects.{AllocationRevisionGuard, ObjectId, ObjectPointer, ObjectRefcount, ObjectRevision, ObjectType, ReadError, ReadType}
import org.aspen_ddp.aspen.common.paxos.ProposalId
import org.aspen_ddp.aspen.common.store.{StoreId, StorePointer}
import org.aspen_ddp.aspen.common.transaction.{ObjectUpdate, PreTransactionOpportunisticRebuild, TransactionDescription, TransactionDisposition, TransactionId, TransactionStatus}

sealed abstract class Message

case class HostHeartbeat(hostId: HostId) extends Message

sealed abstract class ClientRequest extends Message {
  val toStore: StoreId
  val fromClient: ClientId
}

sealed abstract class ClientResponse extends Message {
  val toClient: ClientId
  val fromStore: StoreId
}

sealed abstract class TxMessage extends Message {
  val to: StoreId
  val from: StoreId
  val transactionId: TransactionId
}

final case class Allocate(
                           toStore: StoreId,
                           fromClient: ClientId,
                           newObjectId: ObjectId,
                           objectType: ObjectType.Value,
                           objectSize: Option[Int],
                           initialRefcount: ObjectRefcount,
                           objectData: DataBuffer,
                           timestamp: HLCTimestamp,
                           allocationTransactionId: TransactionId,
                           revisionGuard: AllocationRevisionGuard
                         ) extends ClientRequest {

  override def equals(other: Any): Boolean = other match {
    case rhs: Allocate => toStore == rhs.toStore && fromClient == rhs.fromClient &&
      objectSize == rhs.objectSize && objectData.compareTo(rhs.objectData) == 0 &&
      initialRefcount == rhs.initialRefcount && timestamp.compareTo(rhs.timestamp) == 0 &&
      allocationTransactionId == rhs.allocationTransactionId &&
      revisionGuard == rhs.revisionGuard
    case _ => false
  }

  override def toString: String = f"Allocate from $fromClient to $toStore type $objectType objectId $newObjectId allocationTx $allocationTransactionId"
}

final case class AllocateResponse( toClient: ClientId,
                                   fromStore: StoreId,
                                   allocationTransactionId: TransactionId,
                                   newObjectId: ObjectId,
                                   result: Option[StorePointer]) extends ClientResponse:
  override def toString: String = f"AllocateResponse from $fromStore to $toClient tx $allocationTransactionId objId $newObjectId"

final case class Read(
                       toStore: StoreId,
                       fromClient: ClientId,
                       readUUID: UUID,
                       objectPointer: ObjectPointer,
                       readType: ReadType) extends ClientRequest:
  override def toString: String = f"Read toStore $toStore from $fromClient uuid $readUUID object ${objectPointer.id}"

final case class ReadResponse( toClient: ClientId,
                               fromStore: StoreId,
                               readUUID: UUID,
                               readTime: HLCTimestamp,
                               result: Either[ReadError.Value, ReadResponse.CurrentState]) extends ClientResponse:
  override def toString: String = f"ReadResponse toClient $toClient from $fromStore uuid $readUUID"

object ReadResponse {
  case class CurrentState(
                           revision: ObjectRevision,
                           refcount: ObjectRefcount,
                           timestamp: HLCTimestamp,
                           sizeOnStore: Int,
                           objectData: Option[DataBuffer],
                           lockedWriteTransactions: Set[TransactionId]) {

    override def equals(other: Any): Boolean = other match {
      case rhs: CurrentState =>

        val dmatch = (objectData, rhs.objectData) match {
          case (Some(lhs), Some(r)) => lhs.compareTo(r) == 0
          case (None, None) => true
          case _ => false
        }

        revision == rhs.revision && refcount == rhs.refcount && dmatch && lockedWriteTransactions == rhs.lockedWriteTransactions

      case _ => false
    }
  }
}

final case class OpportunisticRebuild(
                                       toStore: StoreId,
                                       fromClient: ClientId,
                                       pointer: ObjectPointer,
                                       revision: ObjectRevision,
                                       refcount: ObjectRefcount,
                                       timestamp: HLCTimestamp,
                                       data: DataBuffer) extends ClientRequest:
  override def toString: String = f"OpportunisticRebuild to $toStore from $fromClient object ${pointer.id}"

final case class TransactionCompletionQuery(
                                             toStore: StoreId,
                                             fromClient: ClientId,
                                             queryUUID: UUID,
                                             transactionId: TransactionId) extends ClientRequest:
  override def toString: String = f"TransactionCompletionQuery to $toStore from $fromClient query $queryUUID tx $transactionId"

final case class TransactionCompletionResponse(
                                                toClient: ClientId,
                                                fromStore: StoreId,
                                                queryUUID: UUID,
                                                isComplete: Boolean) extends ClientResponse:
  override def toString: String = f"TransactionCompletionResponse to $toClient from $fromStore query $queryUUID isComplete $isComplete"

final case class TransactionResolved(
                                      toClient: ClientId,
                                      fromStore: StoreId,
                                      transactionId: TransactionId,
                                      committed: Boolean) extends ClientResponse:
  override def toString: String = f"TransactionResolved to $toClient from $fromStore tx $transactionId committed $committed"

final case class TransactionFinalized(
                                       toClient: ClientId,
                                       fromStore: StoreId,
                                       transactionId: TransactionId,
                                       committed: Boolean) extends ClientResponse:
  override def toString: String = f"TransactionFinalized to $toClient from $fromStore tx $transactionId committed $committed"


final case class TxPrepare(
                            to: StoreId,
                            from: StoreId,
                            txd: TransactionDescription,
                            proposalId: ProposalId,
                            objectUpdates: List[ObjectUpdate],
                            preTxRebuilds: List[PreTransactionOpportunisticRebuild]) extends TxMessage:

  override val transactionId: TransactionId = txd.transactionId

  override def toString: String = f"TxPrepare to $to from $from tx $transactionId proposalId $proposalId"


final case class TxPrepareResponse(
                                    to: StoreId,
                                    from: StoreId,
                                    transactionId: TransactionId,
                                    response: Either[TxPrepareResponse.Nack, TxPrepareResponse.Promise],
                                    proposalId: ProposalId,
                                    disposition: TransactionDisposition.Value,
                                    collisions: List[TransactionId]) extends TxMessage:
  override def toString: String = f"TxPrepareResponse to $to from $from tx $transactionId proposalId $proposalId disposition $disposition response $response"

object TxPrepareResponse {
  case class Nack(promisedId: ProposalId)
  case class Promise(lastAccepted: Option[(ProposalId,Boolean)])
}

final case class TxAccept(
                           to: StoreId,
                           from: StoreId,
                           transactionId: TransactionId,
                           proposalId: ProposalId,
                           value: Boolean) extends TxMessage:
  override def toString: String = f"TxAccept to $to from $from tx $transactionId proposalId $proposalId value $value"

final case class TxAcceptResponse(
                                   to: StoreId,
                                   from: StoreId,
                                   transactionId: TransactionId,
                                   proposalId: ProposalId,
                                   response: Either[TxAcceptResponse.Nack, TxAcceptResponse.Accepted]) extends TxMessage:
  override def toString: String = f"TxAcceptResponse to $to from $from tx $transactionId proposalId $proposalId response $response"

object TxAcceptResponse {
  case class Nack(promisedId: ProposalId)
  case class Accepted(value: Boolean)
}

final case class TxResolved(
                             to: StoreId,
                             from: StoreId,
                             transactionId: TransactionId,
                             committed: Boolean) extends TxMessage:
  override def toString: String = f"TxResolved to $to from $from tx $transactionId committed $committed"

final case class TxCommitted(
                              to: StoreId,
                              from: StoreId,
                              transactionId: TransactionId,
                              // List of object UUIDs that could not be committed due to transaction requirement errors
                              objectCommitErrors: List[ObjectId]) extends TxMessage:
  override def toString: String = f"TxCommitted to $to from $from tx $transactionId errors $objectCommitErrors"

final case class TxFinalized(
                              to: StoreId,
                              from: StoreId,
                              transactionId: TransactionId,
                              committed: Boolean) extends TxMessage:
  override def toString: String = f"TxFinalized to $to from $from tx $transactionId committed $committed"

final case class TxHeartbeat(
                              to: StoreId,
                              from: StoreId,
                              transactionId: TransactionId) extends TxMessage:
  override def toString: String = f"TxHeartbeat to $to from $from tx $transactionId"

final case class TxStatusRequest(
                                  to: StoreId,
                                  from: StoreId,
                                  transactionId: TransactionId,
                                  requestUUID: UUID) extends TxMessage:
  override def toString: String = f"TxStatusRequest to $to from $from tx $transactionId request $requestUUID"

final case class TxStatusResponse(
                                   to: StoreId,
                                   from: StoreId,
                                   transactionId: TransactionId,
                                   requestUUID: UUID,
                                   status: Option[TxStatusResponse.TxStatus]) extends TxMessage:
  override def toString: String = f"TxStatusResponse to $to from $from tx $transactionId request $requestUUID status $status"

object TxStatusResponse {
  case class TxStatus(status: TransactionStatus.Value, finalized: Boolean)
}

