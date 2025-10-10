package org.aspen_ddp.aspen.common.network

import com.google.protobuf.ByteString
import org.aspen_ddp.aspen.client.{Host, HostId, StorageDevice, StorageDeviceId, StoragePool}
import org.apache.logging.log4j.scala.Logging
import org.aspen_ddp.aspen.codec
import org.aspen_ddp.aspen.codec.ObjectReadError
import org.aspen_ddp.aspen.common.{DataBuffer, HLCTimestamp}
import org.aspen_ddp.aspen.common.ida.{IDA, ReedSolomon, Replication}
import org.aspen_ddp.aspen.common.objects.{ByteArrayKeyOrdering, ByteRange, DataObjectPointer, FullObject, IntegerKeyOrdering, Key, KeyOrdering, KeyRange, KeyRevisionGuard, KeyValueObjectPointer, LargestKeyLessThan, LargestKeyLessThanOrEqualTo, LexicalKeyOrdering, MetadataOnly, ObjectId, ObjectPointer, ObjectRefcount, ObjectRevision, ObjectRevisionGuard, ObjectType, ReadError, SingleKey}
import org.aspen_ddp.aspen.common.paxos.{PersistentState, ProposalId}
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.{StoreId, StorePointer}
import org.aspen_ddp.aspen.common.transaction.KeyValueUpdate.KeyRevision
import org.aspen_ddp.aspen.common.transaction.{DataUpdate, DataUpdateOperation, FinalizationActionId, KeyValueUpdate, LocalTimeRequirement, ObjectUpdate, PreTransactionOpportunisticRebuild, RefcountUpdate, RevisionLock, SerializedFinalizationAction, TransactionDescription, TransactionDisposition, TransactionId, TransactionRequirement, TransactionStatus, VersionBump}
import org.aspen_ddp.aspen.server.cnc
import org.aspen_ddp.aspen.server.cnc.{NewStore, ShutdownStore, TransferStore}
import org.aspen_ddp.aspen.server.crl.{AllocationRecoveryState, TransactionRecoveryState}
import org.aspen_ddp.aspen.server.store.backend.{BackendConfig, RocksDBConfig}

import java.nio.{ByteBuffer, ByteOrder}
import scala.jdk.CollectionConverters.*
import java.util.UUID

object Codec extends Logging:

  def encodeUUID(o: UUID): codec.UUID =
    codec.UUID.newBuilder()
      .setMostSigBits(o.getMostSignificantBits)
      .setLeastSigBits(o.getLeastSignificantBits)
      .build

  def decodeUUID(m: codec.UUID): UUID =
    val msb = m.getMostSigBits
    val lsb = m.getLeastSigBits
    new UUID(msb, lsb)


  def encodeKeyComparison(o: KeyOrdering): codec.KeyComparison = o match
    case ByteArrayKeyOrdering => codec.KeyComparison.KEY_COMPARISON_BYTE_ARRAY
    case IntegerKeyOrdering => codec.KeyComparison.KEY_COMPARISON_INTEGER
    case LexicalKeyOrdering => codec.KeyComparison.KEY_COMPARISON_LEXICAL

  def decodeKeyComparison(m: codec.KeyComparison): KeyOrdering = m match
    case codec.KeyComparison.KEY_COMPARISON_BYTE_ARRAY => ByteArrayKeyOrdering
    case codec.KeyComparison.KEY_COMPARISON_INTEGER => IntegerKeyOrdering
    case codec.KeyComparison.KEY_COMPARISON_LEXICAL => LexicalKeyOrdering
    case f => throw new EncodingError(f"Invalid key comparison: $f")


  def encode(o: Replication): codec.Replication =
    codec.Replication.newBuilder()
      .setWidth(o.width)
      .setWriteThreshold(o.writeThreshold)
      .build

  def decode(m: codec.Replication): Replication =
    Replication(m.getWidth, m.getWriteThreshold)


  def encode(o: ReedSolomon): codec.ReedSolomon =
    codec.ReedSolomon.newBuilder()
      .setWidth(o.width)
      .setRestoreThreshold(o.restoreThreshold)
      .setWriteThreshold(o.writeThreshold)
      .build

  def decode(m: codec.ReedSolomon): ReedSolomon =
    ReedSolomon(m.getWidth, m.getRestoreThreshold, m.getWriteThreshold)


  def encode(o: IDA): codec.IDA =
    val builder = codec.IDA.newBuilder()
    o match
      case r: Replication => builder.setReplication(encode(r))
      case r: ReedSolomon => builder.setReedSolomon(encode(r))
    builder.build

  def decode(m: codec.IDA): IDA =
    if m.hasReplication then
      decode(m.getReplication)
    else if m.hasReedSolomon then
      decode(m.getReedSolomon)
    else
      throw new EncodingError("Unknown IDA")


  def encode(o: ObjectRevision): codec.ObjectRevision =
    codec.ObjectRevision.newBuilder()
      .setUuid(encodeUUID(o.lastUpdateTxUUID))
      .build

  def decode(m: codec.ObjectRevision): ObjectRevision =
    ObjectRevision(TransactionId(decodeUUID(m.getUuid)))


  def encode(o: ObjectRefcount): codec.ObjectRefcount =
    codec.ObjectRefcount.newBuilder()
      .setUpdateSerial(o.updateSerial)
      .setRefcount(o.count)
      .build

  def decode(m: codec.ObjectRefcount): ObjectRefcount =
    ObjectRefcount(m.getUpdateSerial.toInt, m.getRefcount)


  def encode(o: StorePointer): codec.StorePointer =
    codec.StorePointer.newBuilder()
      .setStoreIndex(o.poolIndex)
      .setData(ByteString.copyFrom(o.data))
      .build

  def decode(m: codec.StorePointer): StorePointer =
    StorePointer(m.getStoreIndex.toByte, m.getData.toByteArray)


  def encodeObjectType(o: ObjectType.Value): codec.ObjectType = o match
    case ObjectType.Data => codec.ObjectType.OBJECT_TYPE_DATA
    case ObjectType.KeyValue => codec.ObjectType.OBJECT_TYPE_KEYVALUE

  def decodeObjectType(m: codec.ObjectType): ObjectType.Value = m match
    case codec.ObjectType.OBJECT_TYPE_DATA => ObjectType.Data
    case codec.ObjectType.OBJECT_TYPE_KEYVALUE => ObjectType.KeyValue
    case t => throw new EncodingError(f"Invalid ObjecType: $t")


  def encode(o: ObjectPointer): codec.ObjectPointer =
    val builder = codec.ObjectPointer.newBuilder()
      .setUuid(encodeUUID(o.id.uuid))
      .setPoolUuid(encodeUUID(o.poolId.uuid))
      .setIda(encode(o.ida))
      .setObjectType(encodeObjectType(o.objectType))

    o.size.foreach: size =>
      builder.setSize(size)

    o.storePointers.foreach: ptr =>
      builder.addStorePointers(encode(ptr))

    builder.build

  def decode(m: codec.ObjectPointer): ObjectPointer =
    val uuid = decodeUUID(m.getUuid)
    val poolUuid = decodeUUID(m.getPoolUuid)
    val ida = decode(m.getIda)
    val objectType = decodeObjectType(m.getObjectType)
    val osize = if m.getSize == 0 then None else Some(m.getSize)
    val storePointers = m.getStorePointersList.asScala.map(sp => decode(sp)).toArray

    objectType match
      case ObjectType.Data => new DataObjectPointer(ObjectId(uuid), PoolId(poolUuid), osize, ida, storePointers)
      case ObjectType.KeyValue => new KeyValueObjectPointer(ObjectId(uuid), PoolId(poolUuid), osize, ida, storePointers)


  def encodeTransactionStatus(o: TransactionStatus.Value): codec.TransactionStatus = o match
    case TransactionStatus.Unresolved => codec.TransactionStatus.TRANSACTION_STATUS_UNRESOLVED
    case TransactionStatus.Committed => codec.TransactionStatus.TRANSACTION_STATUS_COMMITTED
    case TransactionStatus.Aborted => codec.TransactionStatus.TRANSACTION_STATUS_ABORT

  def decodeTransactionStatus(m: codec.TransactionStatus): TransactionStatus.Value = m match
    case codec.TransactionStatus.TRANSACTION_STATUS_UNRESOLVED => TransactionStatus.Unresolved
    case codec.TransactionStatus.TRANSACTION_STATUS_COMMITTED => TransactionStatus.Committed
    case codec.TransactionStatus.TRANSACTION_STATUS_ABORT => TransactionStatus.Aborted
    case f => throw new EncodingError(f"Invalid Transaction Status: $f")


  def encodeTransactionDisposition(o: TransactionDisposition.Value): codec.TransactionDisposition = o match
    case TransactionDisposition.Undetermined => codec.TransactionDisposition.TRANSACTION_DISPOSITION_UNDETERMINED
    case TransactionDisposition.VoteCommit => codec.TransactionDisposition.TRANSACTION_DISPOSITION_VOTE_COMMIT
    case TransactionDisposition.VoteAbort => codec.TransactionDisposition.TRANSACTION_DISPOSITION_VOTE_ABORT

  def decodeTransactionDisposition(m: codec.TransactionDisposition): TransactionDisposition.Value = m match
    case codec.TransactionDisposition.TRANSACTION_DISPOSITION_UNDETERMINED => TransactionDisposition.Undetermined
    case codec.TransactionDisposition.TRANSACTION_DISPOSITION_VOTE_COMMIT => TransactionDisposition.VoteCommit
    case codec.TransactionDisposition.TRANSACTION_DISPOSITION_VOTE_ABORT => TransactionDisposition.VoteAbort
    case f => throw new EncodingError(f"Invalid Transaction Disposition: $f")


  def encodeDataUpdateOperation(o: DataUpdateOperation.Value): codec.DataUpdateOperation = o match
    case DataUpdateOperation.Append => codec.DataUpdateOperation.DATA_UPDATE_OPERATION_APPEND
    case DataUpdateOperation.Overwrite => codec.DataUpdateOperation.DATA_UPDATE_OPERATION_OVERWRITE

  def decodeDataUpdateOperation(m: codec.DataUpdateOperation): DataUpdateOperation.Value = m match
    case codec.DataUpdateOperation.DATA_UPDATE_OPERATION_APPEND => DataUpdateOperation.Append
    case codec.DataUpdateOperation.DATA_UPDATE_OPERATION_OVERWRITE => DataUpdateOperation.Overwrite
    case f => throw new EncodingError(f"Invalid DataUpdateOperation: $f")


  def encode(o: DataUpdate): codec.DataUpdate =
    codec.DataUpdate.newBuilder()
      .setObjectPointer(encode(o.objectPointer))
      .setRequiredRevision(encode(o.requiredRevision))
      .setOperation(encodeDataUpdateOperation(o.operation))
      .build
    
  def decode(m: codec.DataUpdate): DataUpdate =
    DataUpdate(decode(m.getObjectPointer),
      decode(m.getRequiredRevision),
      decodeDataUpdateOperation(m.getOperation))


  def encode(o: RefcountUpdate): codec.RefcountUpdate =
    codec.RefcountUpdate.newBuilder()
      .setObjectPointer(encode(o.objectPointer))
      .setRequiredRefcount(encode(o.requiredRefcount))
      .setNewRefcount(encode(o.newRefcount))
      .build

  def decode(m: codec.RefcountUpdate): RefcountUpdate =
    RefcountUpdate(decode(m.getObjectPointer),
      decode(m.getRequiredRefcount),
      decode(m.getNewRefcount))


  def encode(o: VersionBump): codec.VersionBump =
    codec.VersionBump.newBuilder()
      .setObjectPointer(encode(o.objectPointer))
      .setRequiredRevision(encode(o.requiredRevision))
      .build

  def decode(m: codec.VersionBump): VersionBump =
    VersionBump(decode(m.getObjectPointer),
      decode(m.getRequiredRevision))


  def encode(o: RevisionLock): codec.RevisionLock =
    codec.RevisionLock.newBuilder()
      .setObjectPointer(encode(o.objectPointer))
      .setRequiredRevision(encode(o.requiredRevision))
      .build

  def decode(m: codec.RevisionLock): RevisionLock =
    RevisionLock(decode(m.getObjectPointer),
      decode(m.getRequiredRevision))


  def encode(o: KeyValueUpdate.KeyRequirement): codec.KVReq =
    val builder = codec.KVReq.newBuilder()
    builder.setKey(ByteString.copyFrom(o.key.bytes))

    val req = o match
      case r: KeyValueUpdate.KeyRevision =>
        builder.setRevision(encode(r.revision))
        codec.KeyRequirement.KEY_REQUIREMENT_KEY_REVISION

      case r: KeyValueUpdate.KeyObjectRevision =>
        builder.setRevision(encode(r.revision))
        codec.KeyRequirement.KEY_REQUIREMENT_KEY_OBJECT_REVISION

      case r: KeyValueUpdate.WithinRange =>
        builder.setComparison(encodeKeyComparison(r.ordering))
        codec.KeyRequirement.KEY_REQUIREMENT_WITHIN_RANGE

      case _: KeyValueUpdate.Exists => codec.KeyRequirement.KEY_REQUIREMENT_EXISTS
      case _: KeyValueUpdate.MayExist => codec.KeyRequirement.KEY_REQUIREMENT_MAY_EXIST
      case _: KeyValueUpdate.DoesNotExist => codec.KeyRequirement.KEY_REQUIREMENT_DOES_NOT_EXIST

      case r: KeyValueUpdate.TimestampEquals =>
        builder.setTimestamp(r.timestamp.asLong)
        codec.KeyRequirement.KEY_REQUIREMENT_TIMESTAMP_EQUALS

      case r: KeyValueUpdate.TimestampLessThan =>
        builder.setTimestamp(r.timestamp.asLong)
        codec.KeyRequirement.KEY_REQUIREMENT_TIMESTAMP_LESS_THAN

      case r: KeyValueUpdate.TimestampGreaterThan =>
        builder.setTimestamp(r.timestamp.asLong)
        codec.KeyRequirement.KEY_REQUIREMENT_TIMESTAMP_GREATER_THAN

    builder.setRequirement(req)
    builder.build

  def decode(m: codec.KVReq): KeyValueUpdate.KeyRequirement =
    val key = Key(m.getKey.toByteArray)
    val timestamp = HLCTimestamp(m.getTimestamp)

    m.getRequirement match
      case codec.KeyRequirement.KEY_REQUIREMENT_EXISTS => KeyValueUpdate.Exists(key)
      case codec.KeyRequirement.KEY_REQUIREMENT_MAY_EXIST => KeyValueUpdate.MayExist(key)
      case codec.KeyRequirement.KEY_REQUIREMENT_DOES_NOT_EXIST => KeyValueUpdate.DoesNotExist(key)
      case codec.KeyRequirement.KEY_REQUIREMENT_TIMESTAMP_EQUALS => KeyValueUpdate.TimestampEquals(key, timestamp)
      case codec.KeyRequirement.KEY_REQUIREMENT_TIMESTAMP_LESS_THAN => KeyValueUpdate.TimestampLessThan(key, timestamp)
      case codec.KeyRequirement.KEY_REQUIREMENT_TIMESTAMP_GREATER_THAN => KeyValueUpdate.TimestampGreaterThan(key, timestamp)
      case codec.KeyRequirement.KEY_REQUIREMENT_KEY_REVISION =>
        val rev = decode(m.getRevision)
        KeyValueUpdate.KeyRevision(key, rev)
      case codec.KeyRequirement.KEY_REQUIREMENT_KEY_OBJECT_REVISION =>
        val rev = decode(m.getRevision)
        KeyValueUpdate.KeyObjectRevision(key, rev)
      case codec.KeyRequirement.KEY_REQUIREMENT_WITHIN_RANGE =>
        val ord = decodeKeyComparison(m.getComparison)
        KeyValueUpdate.WithinRange(key, ord)
      case f => throw new EncodingError(f"Invalid KeyRequirement: $f")


  def encode(o: KeyRevision): codec.KeyRevision =
    codec.KeyRevision.newBuilder()
      .setKey(ByteString.copyFrom(o.key.bytes))
      .setRevision(encode(o.revision))
      .build

  def decode(m: codec.KeyRevision): KeyRevision =
    KeyRevision(Key(m.getKey.toByteArray), decode(m.getRevision))


  def encode(o: KeyValueUpdate): codec.KeyValueUpdate =
    val builder = codec.KeyValueUpdate.newBuilder()
      .setObjectPointer(encode(o.objectPointer))

    o.requiredRevision.foreach: rev =>
      builder.setRequiredRevision(encode(rev))

    o.contentLock.foreach: lck =>
      lck.fullContents.foreach: kr =>
        builder.addContentLock(encode(kr))

    o.requirements.foreach: kr =>
      builder.addRequirements(encode(kr))

    builder.build

  def decode(m: codec.KeyValueUpdate): KeyValueUpdate =
    val objectPointer = decode(m.getObjectPointer).asInstanceOf[KeyValueObjectPointer]

    val requiredRevision = if m.hasRequiredRevision then
      Some(decode(m.getRequiredRevision))
    else
      None

    val contentLock = if m.getContentLockCount == 0 then
      None
    else
      Some(KeyValueUpdate.FullContentLock(m.getContentLockList.asScala.map(decode).toList))

    val requirements = m.getRequirementsList.asScala.map(decode).toList

    KeyValueUpdate(objectPointer, requiredRevision, contentLock, requirements)


  def encodeLocalTimeRequirementEnum(o: LocalTimeRequirement.Requirement.Value): codec.LocalTimeRequirementEnum = o match
    case LocalTimeRequirement.Requirement.LessThan => codec.LocalTimeRequirementEnum.LOCAL_TIME_REQUIREMENT_ENUM_LESS_THAN
    case LocalTimeRequirement.Requirement.GreaterThan => codec.LocalTimeRequirementEnum.LOCAL_TIME_REQUIREMENT_ENUM_GREATER_THAN
    case LocalTimeRequirement.Requirement.Equals => codec.LocalTimeRequirementEnum.LOCAL_TIME_REQUIREMENT_ENUM_EQUALS

  def decodeLocalTimeRequirementEnum(m: codec.LocalTimeRequirementEnum): LocalTimeRequirement.Requirement.Value = m match
    case codec.LocalTimeRequirementEnum.LOCAL_TIME_REQUIREMENT_ENUM_LESS_THAN => LocalTimeRequirement.Requirement.LessThan
    case codec.LocalTimeRequirementEnum.LOCAL_TIME_REQUIREMENT_ENUM_GREATER_THAN => LocalTimeRequirement.Requirement.GreaterThan
    case codec.LocalTimeRequirementEnum.LOCAL_TIME_REQUIREMENT_ENUM_EQUALS => LocalTimeRequirement.Requirement.Equals
    case f => throw new EncodingError(f"Invalid LocalTimeRequirementEnum: $f")


  def encode(o: LocalTimeRequirement): codec.LocalTimeRequirement =
    codec.LocalTimeRequirement.newBuilder()
      .setTimestamp(o.timestamp.asLong)
      .setRequirement(encodeLocalTimeRequirementEnum(o.tsRequirement))
      .build

  def decode(m: codec.LocalTimeRequirement): LocalTimeRequirement =
    LocalTimeRequirement(HLCTimestamp(m.getTimestamp), decodeLocalTimeRequirementEnum(m.getRequirement))


  def encode(o: TransactionRequirement): codec.TransactionRequirement =
    val builder = codec.TransactionRequirement.newBuilder()
    o match
      case tr: DataUpdate => builder.setDataUpdate(encode(tr))
      case tr: RefcountUpdate => builder.setRefcountUpdate(encode(tr))
      case tr: VersionBump => builder.setVersionBump(encode(tr))
      case tr: RevisionLock => builder.setRevisionLock(encode(tr))
      case tr: KeyValueUpdate => builder.setKvUpdate(encode(tr))
      case tr: LocalTimeRequirement => builder.setLocaltime(encode(tr))
    builder.build

  def decode(m: codec.TransactionRequirement): TransactionRequirement =
    if m.hasDataUpdate then decode(m.getDataUpdate)
    else if m.hasRefcountUpdate then decode(m.getRefcountUpdate)
    else if m.hasVersionBump then decode(m.getVersionBump)
    else if m.hasRevisionLock then decode(m.getRevisionLock)
    else if m.hasKvUpdate then decode(m.getKvUpdate)
    else if m.hasLocaltime then decode(m.getLocaltime)
    else throw new EncodingError("Unknown Transaction Requirement")


  def encode(o: SerializedFinalizationAction): codec.SerializedFinalizationAction =
    codec.SerializedFinalizationAction.newBuilder()
      .setTypeUuid(encodeUUID(o.typeId.uuid))
      .setData(ByteString.copyFrom(o.data))
      .build

  def decode(m: codec.SerializedFinalizationAction): SerializedFinalizationAction =
    SerializedFinalizationAction(FinalizationActionId(decodeUUID(m.getTypeUuid)), m.getData.toByteArray)


  def encode(o: StoreId): codec.StoreId =
    codec.StoreId.newBuilder()
      .setStoragePoolUuid(encodeUUID(o.poolId.uuid))
      .setStoragePoolIndex(o.poolIndex)
      .build()

  def decode(m: codec.StoreId): StoreId =
    StoreId(PoolId(decodeUUID(m.getStoragePoolUuid)), m.getStoragePoolIndex.toByte)


  def encode(o: TransactionDescription): codec.TransactionDescription =
    val builder = codec.TransactionDescription.newBuilder()

    builder.setTransactionUuid(encodeUUID(o.transactionId.uuid))
    builder.setStartTimestamp(o.startTimestamp.asLong)
    builder.setPrimaryObject(encode(o.primaryObject))
    builder.setDesignatedLeaderUid(o.designatedLeaderUID)
    o.requirements.foreach: tr =>
      builder.addRequirements(encode(tr))
    o.finalizationActions.foreach: sfa =>
      builder.addFinalizationActions(encode(sfa))
    o.originatingClient.foreach: clientId =>
      builder.setOriginatingClient(encodeUUID(clientId.uuid))
    o.notifyOnResolution.foreach: storeId =>
      builder.addNotifyOnResolution(encode(storeId))
    o.notes.foreach: s =>
      builder.addNotes(s)

    builder.build
  def decode(m: codec.TransactionDescription): TransactionDescription =
    val txuuid = TransactionId(decodeUUID(m.getTransactionUuid))
    val startTs = HLCTimestamp(m.getStartTimestamp)
    val primaryObj = decode(m.getPrimaryObject)
    val designatedLeader = m.getDesignatedLeaderUid.toByte
    val requirements = m.getRequirementsList.asScala.map(decode).toList
    val serializedFas = m.getFinalizationActionsList.asScala.map(decode).toList
    val origClient = if m.hasOriginatingClient then Some(ClientId(decodeUUID(m.getOriginatingClient))) else None
    val notifyOnRes = m.getNotifyOnResolutionList.asScala.map(decode).toList
    val notes = m.getNotesList.asScala.toList

    TransactionDescription(txuuid, startTs, primaryObj, designatedLeader, requirements,
      serializedFas, origClient, notifyOnRes, notes)


  def encode(o: ProposalId): codec.ProposalId =
    codec.ProposalId.newBuilder()
      .setNumber(o.number)
      .setUid(o.peer)
      .build

  def decode(m: codec.ProposalId): ProposalId =
    ProposalId(m.getNumber, m.getUid.toByte)


  def encode(o: TxPrepare): codec.TxPrepare =
    codec.TxPrepare.newBuilder()
      .setTo(encode(o.to))
      .setFrom(encode(o.from))
      .setTxd(encode(o.txd))
      .setProposalId(encode(o.proposalId))
      .build

  def decode(m: codec.TxPrepare,
             objectUpdates: List[ObjectUpdate],
             preTxRebuilds: List[PreTransactionOpportunisticRebuild]): TxPrepare =
    TxPrepare(decode(m.getTo), decode(m.getFrom), decode(m.getTxd), decode(m.getProposalId),
      objectUpdates, preTxRebuilds)


  def encode(o: TxPrepareResponse): codec.TxPrepareResponse =
    val builder = codec.TxPrepareResponse.newBuilder()

    builder.setTo(encode(o.to))
    builder.setFrom(encode(o.from))
    builder.setTransactionUuid(encodeUUID(o.transactionId.uuid))
    builder.setProposalId(encode(o.proposalId))
    val prt = o.response match
      case Left(nack) =>
        builder.setPromisedId(encode(nack.promisedId))
        codec.TxPrepareResponseType.TX_PREPARE_RESPONSE_TYPE_NACK
      case Right(promise) =>
        promise.lastAccepted.foreach: (pid, value) =>
          builder.setLastAcceptedId(encode(pid))
          builder.setLastAcceptedValue(value)
        codec.TxPrepareResponseType.TX_PREPARE_RESPONSE_TYPE_PROMISE
    builder.setResponseType(prt)
    builder.setDisposition(encodeTransactionDisposition(o.disposition))

    val arr = new Array[Byte](o.collisions.size * 16)

    val bb = ByteBuffer.wrap(arr)
    bb.order(ByteOrder.BIG_ENDIAN)
    o.collisions.foreach: id =>
      bb.putLong(id.uuid.getMostSignificantBits)
      bb.putLong(id.uuid.getLeastSignificantBits)

    builder.setTransactionCollisions(ByteString.copyFrom(arr))

    builder.build

  def decode(m: codec.TxPrepareResponse): TxPrepareResponse =
    val to = decode(m.getTo)
    val from = decode(m.getFrom)
    val txid = TransactionId(decodeUUID(m.getTransactionUuid))
    val response = m.getResponseType match
      case codec.TxPrepareResponseType.TX_PREPARE_RESPONSE_TYPE_NACK =>
        Left(TxPrepareResponse.Nack(decode(m.getPromisedId)))
      case codec.TxPrepareResponseType.TX_PREPARE_RESPONSE_TYPE_PROMISE =>
        val la = if m.hasLastAcceptedId then
          Some((decode(m.getLastAcceptedId), m.getLastAcceptedValue))
        else
          None
        Right(TxPrepareResponse.Promise(la))
      case f => throw new EncodingError("Unknown Prepare Response Type")
    val proposalId = decode(m.getProposalId)
    val disposition = decodeTransactionDisposition(m.getDisposition)
    var collisions = List[TransactionId]()

    val bb = m.getTransactionCollisions.asReadOnlyByteBuffer()
    bb.order(ByteOrder.BIG_ENDIAN)
    while (bb.remaining() != 0) {
      val msb = bb.getLong()
      val lsb = bb.getLong()
      collisions = TransactionId(new UUID(msb, lsb)) :: collisions
    }

    TxPrepareResponse(to, from, txid, response, proposalId, disposition, collisions)


  def encode(o: TxAccept): codec.TxAccept =
    codec.TxAccept.newBuilder()
      .setTo(encode(o.to))
      .setFrom(encode(o.from))
      .setTransactionUuid(encodeUUID(o.transactionId.uuid))
      .setProposalId(encode(o.proposalId))
      .setValue(o.value)
      .build

  def decode(m: codec.TxAccept): TxAccept =
    TxAccept(decode(m.getTo), decode(m.getFrom), TransactionId(decodeUUID(m.getTransactionUuid)),
      decode(m.getProposalId), m.getValue)


  def encode(o: TxAcceptResponse): codec.TxAcceptResponse =
    val builder = codec.TxAcceptResponse.newBuilder()
      .setTo(encode(o.to))
      .setFrom(encode(o.from))
      .setTransactionUuid(encodeUUID(o.transactionId.uuid))
      .setProposalId(encode(o.proposalId))

      o.response match
        case Left(nack) =>
          builder.setIsNack(true)
          builder.setPromisedId(encode(nack.promisedId))
        case Right(accepted) =>
          builder.setValue(accepted.value)

      builder.build

  def decode(m: codec.TxAcceptResponse): TxAcceptResponse =
    val response = if m.getIsNack then
      Left(TxAcceptResponse.Nack(decode(m.getPromisedId)))
    else
      Right(TxAcceptResponse.Accepted(m.getValue))

    TxAcceptResponse(decode(m.getTo), decode(m.getFrom), TransactionId(decodeUUID(m.getTransactionUuid)),
      decode(m.getProposalId), response)


  def encode(o: TxResolved): codec.TxResolved =
    codec.TxResolved.newBuilder()
      .setTo(encode(o.to))
      .setFrom(encode(o.from))
      .setTransactionUuid(encodeUUID(o.transactionId.uuid))
      .setCommitted(o.committed)
      .build

  def decode(m: codec.TxResolved): TxResolved =
    TxResolved(decode(m.getTo), decode(m.getFrom), TransactionId(decodeUUID(m.getTransactionUuid)),
      m.getCommitted)


  def encode(o: TxCommitted): codec.TxCommitted =
    val builder = codec.TxCommitted.newBuilder()
      .setTo(encode(o.to))
      .setFrom(encode(o.from))
      .setTransactionUuid(encodeUUID(o.transactionId.uuid))

    val arr = new Array[Byte](o.objectCommitErrors.size * 16)

    val bb = ByteBuffer.wrap(arr)
    bb.order(ByteOrder.BIG_ENDIAN)
    o.objectCommitErrors.foreach: id =>
      bb.putLong(id.uuid.getMostSignificantBits)
      bb.putLong(id.uuid.getLeastSignificantBits)

    builder.setObjectCommitErrors(ByteString.copyFrom(arr))

    builder.build

  def decode(m: codec.TxCommitted): TxCommitted =

    var commitErrors = List[ObjectId]()

    val bb = m.getObjectCommitErrors.asReadOnlyByteBuffer()
    bb.order(ByteOrder.BIG_ENDIAN)
    while (bb.remaining() != 0) {
      val msb = bb.getLong()
      val lsb = bb.getLong()
      commitErrors = ObjectId(new UUID(msb, lsb)) :: commitErrors
    }

    TxCommitted(decode(m.getTo), decode(m.getFrom), TransactionId(decodeUUID(m.getTransactionUuid)),
      commitErrors)


  def encode(o: TxFinalized): codec.TxFinalized =
    codec.TxFinalized.newBuilder()
      .setTo(encode(o.to))
      .setFrom(encode(o.from))
      .setTransactionUuid(encodeUUID(o.transactionId.uuid))
      .setCommitted(o.committed)
      .build

  def decode(m: codec.TxFinalized): TxFinalized =
    TxFinalized(decode(m.getTo), decode(m.getFrom), TransactionId(decodeUUID(m.getTransactionUuid)),
      m.getCommitted)


  def encode(o: TxHeartbeat): codec.TxHeartbeat =
    codec.TxHeartbeat.newBuilder()
      .setTo(encode(o.to))
      .setFrom(encode(o.from))
      .setTransactionUuid(encodeUUID(o.transactionId.uuid))
      .build

  def decode(m: codec.TxHeartbeat): TxHeartbeat =
    TxHeartbeat(decode(m.getTo), decode(m.getFrom), TransactionId(decodeUUID(m.getTransactionUuid)))


  def encode(o: TxStatusRequest): codec.TxStatusRequest =
    codec.TxStatusRequest.newBuilder()
      .setTo(encode(o.to))
      .setFrom(encode(o.from))
      .setTransactionUuid(encodeUUID(o.transactionId.uuid))
      .setRequestUuid(encodeUUID(o.requestUUID))
      .build

  def decode(m: codec.TxStatusRequest): TxStatusRequest =
    TxStatusRequest(decode(m.getTo), decode(m.getFrom), TransactionId(decodeUUID(m.getTransactionUuid)),
      decodeUUID(m.getRequestUuid))


  def encode(o: TxStatusResponse): codec.TxStatusResponse =
    val builder = codec.TxStatusResponse.newBuilder()
      .setTo(encode(o.to))
      .setFrom(encode(o.from))
      .setTransactionUuid(encodeUUID(o.transactionId.uuid))
      .setRequestUuid(encodeUUID(o.requestUUID))

    val status = o.status match
      case None =>
        builder.setHaveStatus(false)

      case Some(stat) =>
        builder.setHaveStatus(true)
        builder.setIsFinalized(stat.finalized)
        builder.setStatus(encodeTransactionStatus(stat.status))

    builder.build

  def decode(m: codec.TxStatusResponse): TxStatusResponse =
    val status = if m.getHaveStatus then
      Some(TxStatusResponse.TxStatus(decodeTransactionStatus(m.getStatus), m.getIsFinalized))
    else
      None

    TxStatusResponse(decode(m.getTo), decode(m.getFrom), TransactionId(decodeUUID(m.getTransactionUuid)),
      decodeUUID(m.getRequestUuid), status)


  def encodeReadError(o: ReadError.Value): codec.ObjectReadError = o match
    case ReadError.ObjectMismatch => codec.ObjectReadError.OBJECT_READ_ERROR_OBJECT_MISMATCH
    case ReadError.ObjectNotFound => codec.ObjectReadError.OBJECT_READ_ERROR_OBJECT_NOT_FOUND
    case ReadError.StoreNotFound => codec.ObjectReadError.OBJECT_READ_ERROR_STORE_NOT_FOUND
    case ReadError.CorruptedObject => codec.ObjectReadError.OBJECT_READ_ERROR_CORRUPTED_OBJECT

  def decodeReadError(m: codec.ObjectReadError): ReadError.Value = m match
    case codec.ObjectReadError.OBJECT_READ_ERROR_OBJECT_MISMATCH => ReadError.ObjectMismatch
    case codec.ObjectReadError.OBJECT_READ_ERROR_OBJECT_NOT_FOUND => ReadError.ObjectNotFound
    case codec.ObjectReadError.OBJECT_READ_ERROR_STORE_NOT_FOUND => ReadError.StoreNotFound
    case codec.ObjectReadError.OBJECT_READ_ERROR_CORRUPTED_OBJECT => ReadError.CorruptedObject
    case f => throw new EncodingError(f"Invalid ObjectReadError: $f")


  def encode(o: Read): codec.Read =
    val builder = codec.Read.newBuilder()

    builder.setToStore(encode(o.toStore))
    builder.setFromClient(encodeUUID(o.fromClient.uuid))
    builder.setReadUuid(encodeUUID(o.readUUID))
    builder.setObjectPointer(encode(o.objectPointer))

    val readType = o.readType match
      case _: MetadataOnly =>
        codec.ReadType.READ_TYPE_METADATA_ONLY
      case _: FullObject =>
        codec.ReadType.READ_TYPE_FULL_OBJECT
      case rt: ByteRange =>
        builder.setOffset(rt.offset)
        builder.setLength(rt.length)
        codec.ReadType.READ_TYPE_BYTE_RANGE
      case rt: SingleKey =>
        builder.setKey(ByteString.copyFrom(rt.key.bytes))
        builder.setComparison(encodeKeyComparison(rt.ordering))
        codec.ReadType.READ_TYPE_SINGLE_KEY
      case rt: LargestKeyLessThan =>
        builder.setKey(ByteString.copyFrom(rt.key.bytes))
        builder.setComparison(encodeKeyComparison(rt.ordering))
        codec.ReadType.READ_TYPE_LARGEST_KEY_LESS_THAN
      case rt: LargestKeyLessThanOrEqualTo =>
        builder.setKey(ByteString.copyFrom(rt.key.bytes))
        builder.setComparison(encodeKeyComparison(rt.ordering))
        codec.ReadType.READ_TYPE_LARGEST_KEY_LESS_THAN_OR_EQUAL_TO
      case rt: KeyRange =>
        builder.setMin(ByteString.copyFrom(rt.minimum.bytes))
        builder.setMax(ByteString.copyFrom(rt.maximum.bytes))
        builder.setComparison(encodeKeyComparison(rt.ordering))
        codec.ReadType.READ_TYPE_KEY_RANGE

      builder.setReadType(readType)

    builder.build

  def decode(m: codec.Read): Read =
    val toStore = decode(m.getToStore)
    val from = ClientId(decodeUUID(m.getFromClient))
    val readUuid = decodeUUID(m.getReadUuid)
    val objPtr = decode(m.getObjectPointer)
    val readType = m.getReadType match
      case codec.ReadType.READ_TYPE_METADATA_ONLY => MetadataOnly()
      case codec.ReadType.READ_TYPE_FULL_OBJECT => FullObject()
      case codec.ReadType.READ_TYPE_BYTE_RANGE => ByteRange(m.getOffset, m.getLength)
      case codec.ReadType.READ_TYPE_SINGLE_KEY => SingleKey(Key(m.getKey.toByteArray), decodeKeyComparison(m.getComparison))
      case codec.ReadType.READ_TYPE_LARGEST_KEY_LESS_THAN => LargestKeyLessThan(Key(m.getKey.toByteArray), decodeKeyComparison(m.getComparison))
      case codec.ReadType.READ_TYPE_LARGEST_KEY_LESS_THAN_OR_EQUAL_TO => LargestKeyLessThanOrEqualTo(Key(m.getKey.toByteArray), decodeKeyComparison(m.getComparison))
      case codec.ReadType.READ_TYPE_KEY_RANGE => KeyRange(Key(m.getMin.toByteArray), Key(m.getMax.toByteArray), decodeKeyComparison(m.getComparison))
      case f => throw new EncodingError(f"Invalid Read Type: $f")

    Read(toStore, from, readUuid, objPtr, readType)


  def encode(o: ReadResponse): codec.ReadResponse =
    val builder = codec.ReadResponse.newBuilder()
    builder.setFromStore(encode(o.fromStore))
    builder.setReadUuid(encodeUUID(o.readUUID))
    builder.setReadTime(o.readTime.asLong)

    o.result match
      case Left(err) =>
        builder.setHaveData(false)
        builder.setReadError(encodeReadError(err))

      case Right(r) =>
        builder.setHaveData(true)
        builder.setRevision(encode(r.revision))
        builder.setRefcount(encode(r.refcount))
        builder.setSizeOnStore(r.sizeOnStore)
        builder.setTimestamp(r.timestamp.asLong)

        r.objectData.foreach: data =>
          builder.setObjectData(ByteString.copyFrom(data.asReadOnlyBuffer()))

        val arr = new Array[Byte](r.lockedWriteTransactions.size * 16)
        val bb = ByteBuffer.wrap(arr)
        bb.order(ByteOrder.BIG_ENDIAN)
        r.lockedWriteTransactions.foreach: id =>
          bb.putLong(id.uuid.getMostSignificantBits)
          bb.putLong(id.uuid.getLeastSignificantBits)

        builder.setLockedWriteTransactions(ByteString.copyFrom(arr))

    builder.build

  def decode(m: codec.ReadResponse): ReadResponse =
    val fromStore = decode(m.getFromStore)
    val toClient = ClientId(decodeUUID(m.getToClient))
    val readUuid = decodeUUID(m.getReadUuid)
    val readTime = HLCTimestamp(m.getReadTime)
    val result = if ! m.getHaveData then
      Left(decodeReadError(m.getReadError))
    else
      val revision = decode(m.getRevision)
      val refcount = decode(m.getRefcount)
      val timestamp = HLCTimestamp(m.getTimestamp)
      val sizeOnStore = m.getSizeOnStore
      val data = if m.getObjectData.size == 0 then
        None
      else
        Some(DataBuffer(m.getObjectData.toByteArray))

      var lockedTx = List[TransactionId]()

      val bb = m.getLockedWriteTransactions.asReadOnlyByteBuffer()
      bb.order(ByteOrder.BIG_ENDIAN)
      while (bb.remaining() != 0) {
        val msb = bb.getLong()
        val lsb = bb.getLong()
        lockedTx = TransactionId(new UUID(msb, lsb)) :: lockedTx
      }

      Right(ReadResponse.CurrentState(revision, refcount, timestamp, sizeOnStore, data, lockedTx.toSet))

    ReadResponse(toClient, fromStore, readUuid, readTime, result)


  def encode(o: OpportunisticRebuild): codec.OpportunisticRebuild =
    codec.OpportunisticRebuild.newBuilder()
      .setToStore(encode(o.toStore))
      .setFromClient(encodeUUID(o.fromClient.uuid))
      .setPointer(encode(o.pointer))
      .setRevision(encode(o.revision))
      .setRefcount(encode(o.refcount))
      .setTimestamp(o.timestamp.asLong)
      .setData(ByteString.copyFrom(o.data.asReadOnlyBuffer()))
      .build

  def decode(m: codec.OpportunisticRebuild): OpportunisticRebuild =
    val toStore = decode(m.getToStore)
    val fromClient = ClientId(decodeUUID(m.getFromClient))
    val pointer = decode(m.getPointer)
    val revision = decode(m.getRevision)
    val refcount = decode(m.getRefcount)
    val timestamp = HLCTimestamp(m.getTimestamp)
    val data = DataBuffer(m.getData.toByteArray)
    OpportunisticRebuild(toStore, fromClient, pointer, revision, refcount, timestamp, data)


  def encode(o: TransactionCompletionQuery): codec.TransactionCompletionQuery =
    codec.TransactionCompletionQuery.newBuilder()
      .setToStore(encode(o.toStore))
      .setFromClient(encodeUUID(o.fromClient.uuid))
      .setQueryUuid(encodeUUID(o.queryUUID))
      .setTransactionUuid(encodeUUID(o.transactionId.uuid))
      .build

  def decode(m: codec.TransactionCompletionQuery): TransactionCompletionQuery =
    val toStore = decode(m.getToStore)
    val fromClient = ClientId(decodeUUID(m.getFromClient))
    val queryUuid = decodeUUID(m.getQueryUuid)
    val txid = TransactionId(decodeUUID(m.getTransactionUuid))
    TransactionCompletionQuery(toStore, fromClient, queryUuid, txid)


  def encode(o: TransactionCompletionResponse): codec.TransactionCompletionResponse =
    codec.TransactionCompletionResponse.newBuilder()
      .setToClient(encodeUUID(o.toClient.uuid))
      .setFromStore(encode(o.fromStore))
      .setQueryUuid(encodeUUID(o.queryUUID))
      .setIsComplete(o.isComplete)
      .build

  def decode(m: codec.TransactionCompletionResponse): TransactionCompletionResponse =
    val fromStore = decode(m.getFromStore)
    val toClient = ClientId(decodeUUID(m.getToClient))
    val queryUuid = decodeUUID(m.getQueryUuid)
    val isComplete = m.getIsComplete
    TransactionCompletionResponse(toClient, fromStore, queryUuid, isComplete)


  def encode(o: TransactionResolved): codec.TransactionResolved =
    codec.TransactionResolved.newBuilder()
      .setToClient(encodeUUID(o.toClient.uuid))
      .setFromStore(encode(o.fromStore))
      .setTransactionUuid(encodeUUID(o.transactionId.uuid))
      .setCommitted(o.committed)
      .build

  def decode(m: codec.TransactionResolved): TransactionResolved =
    val fromStore = decode(m.getFromStore)
    val toClient = ClientId(decodeUUID(m.getToClient))
    val transactionId = TransactionId(decodeUUID(m.getTransactionUuid))
    val committed = m.getCommitted
    TransactionResolved(toClient, fromStore, transactionId, committed)


  def encode(o: TransactionFinalized): codec.TransactionFinalized =
    codec.TransactionFinalized.newBuilder()
      .setToClient(encodeUUID(o.toClient.uuid))
      .setFromStore(encode(o.fromStore))
      .setTransactionUuid(encodeUUID(o.transactionId.uuid))
      .setCommitted(o.committed)
      .build

  def decode(m: codec.TransactionFinalized): TransactionFinalized =
    val fromStore = decode(m.getFromStore)
    val toClient = ClientId(decodeUUID(m.getToClient))
    val transactionId = TransactionId(decodeUUID(m.getTransactionUuid))
    val committed = m.getCommitted
    TransactionFinalized(toClient, fromStore, transactionId, committed)


  def encode(o: Allocate): codec.Allocate =
    val builder = codec.Allocate.newBuilder()
      .setToStore(encode(o.toStore))
      .setFromClient(encodeUUID(o.fromClient.uuid))
      .setNewObjectUuid(encodeUUID(o.newObjectId.uuid))
      .setObjectType(encodeObjectType(o.objectType))
      .setObjectSize(o.objectSize.getOrElse(0))
      .setInitialRefcount(encode(o.initialRefcount))
      .setObjectData(ByteString.copyFrom(o.objectData.asReadOnlyBuffer()))
      .setTimestamp(o.timestamp.asLong)
      .setAllocationTransactionUuid(encodeUUID(o.allocationTransactionId.uuid))
      .setAllocatingObject(encode(o.revisionGuard.pointer))

    o.revisionGuard match
      case g: ObjectRevisionGuard => builder.setAllocatingObjectRevision(encode(g.requiredRevision))
      case g: KeyRevisionGuard =>
        val kvreq = KeyValueUpdate.KeyObjectRevision(g.key, g.keyRevision)
        builder.setAllocatingObjectKeyRequirement(encode(kvreq))

    builder.build

  def decode(m: codec.Allocate): Allocate =
    val toStore = decode(m.getToStore)
    val fromClient = ClientId(decodeUUID(m.getFromClient))
    val newObjectId = ObjectId(decodeUUID(m.getNewObjectUuid))
    val objectType = decodeObjectType(m.getObjectType)
    val objectSize = if m.getObjectSize == 0 then None else Some(m.getObjectSize)
    val initialRefcount = decode(m.getInitialRefcount)
    val objectData = DataBuffer(m.getObjectData.toByteArray)
    val timestamp = HLCTimestamp(m.getTimestamp)
    val allocTxId = TransactionId(decodeUUID(m.getAllocationTransactionUuid))
    val allocObj = decode(m.getAllocatingObject)
    val revisionGuard = if m.hasAllocatingObjectRevision then
      ObjectRevisionGuard(allocObj, decode(m.getAllocatingObjectRevision))
    else
      val kvreq = decode(m.getAllocatingObjectKeyRequirement)
      val rev = kvreq match
        case kor: KeyValueUpdate.KeyObjectRevision => kor.revision
        case _ => throw new EncodingError("Only KeyObjectRevisions are allowed")
      KeyRevisionGuard(allocObj.asInstanceOf[KeyValueObjectPointer], kvreq.key, rev)

    Allocate(toStore, fromClient, newObjectId, objectType, objectSize, initialRefcount,
      objectData, timestamp, allocTxId, revisionGuard)


  def encode(o: AllocateResponse): codec.AllocateResponse =
    val builder = codec.AllocateResponse.newBuilder()
      .setToClient(encodeUUID(o.toClient.uuid))
      .setFromStore(encode(o.fromStore))
      .setAllocationTransactionUuid(encodeUUID(o.allocationTransactionId.uuid))
      .setNewObjectUuid(encodeUUID(o.newObjectId.uuid))

    o.result.foreach: ptr =>
      builder.setAllocateStorePointer(encode(ptr))

    builder.build

  def decode(m: codec.AllocateResponse): AllocateResponse =
    val toClient = ClientId(decodeUUID(m.getToClient))
    val fromStore = decode(m.getFromStore)
    val allocTxId = TransactionId(decodeUUID(m.getAllocationTransactionUuid))
    val newObjId = ObjectId(decodeUUID(m.getNewObjectUuid))
    val storePointer = if m.hasAllocateStorePointer then Some(decode(m.getAllocateStorePointer)) else None

    AllocateResponse(toClient, fromStore, allocTxId, newObjId, storePointer)


  def encode(o: HostHeartbeat): codec.HostHeartbeat =
    codec.HostHeartbeat.newBuilder()
      .setFromHostId(encodeUUID(o.hostId.uuid))
      .build

  def decode(m: codec.HostHeartbeat): HostHeartbeat =
    HostHeartbeat(HostId(decodeUUID(m.getFromHostId)))
    
  // ----------------------- Non Network Messages -----------------------

  def encode(o: ObjectUpdate): codec.ObjectUpdate =
    val builder = codec.ObjectUpdate.newBuilder()
      .setObjectId(encodeUUID(o.objectId.uuid))
      .setData(ByteString.copyFrom(o.data.asReadOnlyBuffer()))

    builder.build

  def decode(m: codec.ObjectUpdate): ObjectUpdate =
    val oid = ObjectId(decodeUUID(m.getObjectId))
    val objectData = DataBuffer(m.getData.toByteArray)
    
    ObjectUpdate(oid, objectData)


  def encode(o: PersistentState): codec.PersistentState =
    val builder = codec.PersistentState.newBuilder()
    
    o.promised.foreach: p =>
      builder.setPromised(encode(p))
    o.accepted.foreach: (pid, value) =>
      builder.setAcceptedProposalId(encode(pid))
      builder.setAcceptedValue(value)
      
    builder.build

  def decode(m: codec.PersistentState): PersistentState =
    val promised = if m.hasPromised then
      Some(decode(m.getPromised))
    else
      None
      
    val accepted = if m.hasAcceptedProposalId then
      Some((decode(m.getAcceptedProposalId), m.getAcceptedValue))
    else
      None

    PersistentState(promised, accepted)


  def encode(o: TransactionRecoveryState): codec.TransactionRecoveryState =
    val builder = codec.TransactionRecoveryState.newBuilder()

    builder.setStoreId(encode(o.storeId))
    builder.setSerializedTxd(ByteString.copyFrom(o.serializedTxd.asReadOnlyBuffer()))
    o.objectUpdates.foreach: ou =>
      builder.addObjectUpdates(encode(ou))
    builder.setDisposition(encodeTransactionDisposition(o.disposition))
    builder.setStatus(encodeTransactionStatus(o.status))
    builder.setPaxosAcceptorState(encode(o.paxosAcceptorState))

    builder.build

  def decode(m: codec.TransactionRecoveryState): TransactionRecoveryState =
    val storeId = decode(m.getStoreId)
    val serializedTxd = DataBuffer(m.getSerializedTxd.toByteArray)
    val objectUpdates = m.getObjectUpdatesList.asScala.map(decode).toList
    val disposition = decodeTransactionDisposition(m.getDisposition)
    val status = decodeTransactionStatus(m.getStatus)
    val paxosAcceptorState = decode(m.getPaxosAcceptorState)
    
    TransactionRecoveryState(storeId, serializedTxd, objectUpdates, disposition, status, paxosAcceptorState)
    
  
  def encode(o: AllocationRecoveryState): codec.AllocationRecoveryState =
    val builder = codec.AllocationRecoveryState.newBuilder()

    builder.setStoreId(encode(o.storeId))
    builder.setStorePointer(encode(o.storePointer))
    builder.setNewObjectId(encodeUUID(o.newObjectId.uuid))
    builder.setObjectType(encodeObjectType(o.objectType))
    o.objectSize.foreach: sz =>
      builder.setObjectSize(sz)
    builder.setObjectData(ByteString.copyFrom(o.objectData.asReadOnlyBuffer()))
    builder.setInitialRefcount(encode(o.initialRefcount))
    builder.setTimestamp(o.timestamp.asLong)
    builder.setTransactionUuid(encodeUUID(o.allocationTransactionId.uuid))
    builder.setSerializedRevisionGuard(ByteString.copyFrom(o.serializedRevisionGuard.asReadOnlyBuffer()))
    
    builder.build

  def decode(m: codec.AllocationRecoveryState): AllocationRecoveryState =
    val storeId = decode(m.getStoreId)
    val storePointer = decode(m.getStorePointer)
    val newObjectId = ObjectId(decodeUUID(m.getNewObjectId))
    val objectType = decodeObjectType(m.getObjectType)
    val objectSize = if m.getObjectSize == 0 then None else Some(m.getObjectSize)
    val objectData = DataBuffer(m.getObjectData.toByteArray)
    val initialRefcount = decode(m.getInitialRefcount)
    val timestamp = HLCTimestamp(m.getTimestamp)
    val transactionId = TransactionId(decodeUUID(m.getTransactionUuid))
    val serializedRevisionGuard = DataBuffer(m.getSerializedRevisionGuard.toByteArray)

    AllocationRecoveryState(storeId, storePointer, newObjectId, objectType, objectSize, objectData,
      initialRefcount, timestamp, transactionId, serializedRevisionGuard)


  def encode(o: StoragePool.StoreEntry): codec.PoolStoreEntry =
    codec.PoolStoreEntry.newBuilder()
      .setHostId(encodeUUID(o.hostId.uuid))
      .setStorageDeviceId(encode(o.storageDeviceId))
      .build

  def decode(m: codec.PoolStoreEntry): StoragePool.StoreEntry =
    val hostId = HostId(decodeUUID(m.getHostId))
    val storageDeviceId = decode(m.getStorageDeviceId)
    StoragePool.StoreEntry(hostId, storageDeviceId)


  def encode(o: StoragePool.Config): codec.PoolConfig =
    val builder = codec.PoolConfig.newBuilder()

    builder.setPoolId(encodeUUID(o.poolId.uuid))
    builder.setName(o.name)
    builder.setDefaultIDA(encode(o.defaultIDA))
    builder.setMaxObjectSize(o.maxObjectSize.getOrElse(0))

    o.stores.foreach: storeEntry =>
      builder.addStores(encode(storeEntry))

    builder.build

  def decode(m: codec.PoolConfig): StoragePool.Config =
    val poolId = PoolId(decodeUUID(m.getPoolId))
    val name = m.getName
    val defaultIDA = decode(m.getDefaultIDA)
    val maxObjectSize = if m.getMaxObjectSize == 0 then None else Some(m.getMaxObjectSize)
    val stores = m.getStoresList.asScala.map(decode).toArray

    StoragePool.Config(poolId, name, defaultIDA, maxObjectSize, stores)


  def encode(o: Host): codec.Host =
    val builder = codec.Host.newBuilder()

    builder.setHostId(encodeUUID(o.hostId.uuid))
    builder.setName(o.name)
    builder.setAddress(o.address)
    builder.setDataPort(o.dataPort)
    builder.setCncPort(o.cncPort)
    builder.setStoreTransferPort(o.storeTransferPort)

    o.storageDevices.foreach { deviceId =>
      builder.addStorageDevices(encode(deviceId))
    }

    builder.build

  def decode(m: codec.Host): Host =
    val hostId = HostId(decodeUUID(m.getHostId))
    val name = m.getName
    val address = m.getAddress
    val dataPort = m.getDataPort
    val cncPort = m.getCncPort
    val storeTransferPort = m.getStoreTransferPort
    val storageDevices = m.getStorageDevicesList.asScala.map(decode).toSet

    Host(hostId, name, address, dataPort, cncPort, storeTransferPort, storageDevices)

  // CnC Messages -----------------------------------------------------------------

  def encodeBackendConfig(o: BackendConfig): codec.BackendConfig = o match
    case RocksDBConfig() => codec.BackendConfig.BACKEND_CONFIG_ROCKS_DB

  def decodeBackendConfig(m: codec.BackendConfig): BackendConfig = m match
    case codec.BackendConfig.BACKEND_CONFIG_ROCKS_DB => RocksDBConfig()
    case f => throw new EncodingError(f"Invalid Backend Config: $f")


  def encode(o: NewStore): codec.NewStore =
    val builder = codec.NewStore.newBuilder()

    builder.setStoreId(encode(o.storeId))
    builder.setBackendConfig(encodeBackendConfig(o.backendType))

    builder.build

  def decode(m: codec.NewStore): NewStore =
    val storeId = decode(m.getStoreId)
    val backendType = decodeBackendConfig(m.getBackendConfig)

    NewStore(storeId, backendType)


  def encode(o: ShutdownStore): codec.ShutdownStore =
    val builder = codec.ShutdownStore.newBuilder()

    builder.setStoreId(encode(o.storeId))

    builder.build

  def decode(m: codec.ShutdownStore): ShutdownStore =
    val storeId = decode(m.getStoreId)

    ShutdownStore(storeId)


  def encode(o: TransferStore): codec.TransferStore =
    val builder = codec.TransferStore.newBuilder()

    builder.setStoreId(encode(o.storeId))
    builder.setToHost(encodeUUID(o.toHost.uuid))

    builder.build

  def decode(m: codec.TransferStore): TransferStore =
    val storeId = decode(m.getStoreId)
    val toHost = HostId(decodeUUID(m.getToHost))

    TransferStore(storeId, toHost)


  def encode(o: cnc.Error): codec.CnCError =
    val builder = codec.CnCError.newBuilder()

    builder.setMessage(o.message)

    builder.build

  def decode(m: codec.CnCError): cnc.Error =
    cnc.Error(m.getMessage)

  // Storage Device Messages -----------------------------------------------------------------

  def encode(o: StorageDeviceId): codec.StorageDeviceId =
    codec.StorageDeviceId.newBuilder()
      .setDeviceUuid(encodeUUID(o.uuid))
      .build

  def decode(m: codec.StorageDeviceId): StorageDeviceId =
    StorageDeviceId(decodeUUID(m.getDeviceUuid))


  def encodeStorageDeviceStoreStatus(o: StorageDevice.StoreStatus): codec.StorageDeviceStoreStatus = o match
    case StorageDevice.StoreStatus.Initializing => codec.StorageDeviceStoreStatus.STORAGE_DEVICE_STORE_STATUS_INITIALIZING
    case StorageDevice.StoreStatus.Active => codec.StorageDeviceStoreStatus.STORAGE_DEVICE_STORE_STATUS_ACTIVE
    case StorageDevice.StoreStatus.TransferringIn => codec.StorageDeviceStoreStatus.STORAGE_DEVICE_STORE_STATUS_TRANSFERRING_IN
    case StorageDevice.StoreStatus.TransferringOut => codec.StorageDeviceStoreStatus.STORAGE_DEVICE_STORE_STATUS_TRANSFERRING_OUT
    case StorageDevice.StoreStatus.Rebuilding => codec.StorageDeviceStoreStatus.STORAGE_DEVICE_STORE_STATUS_REBUILDING

  def decodeStorageDeviceStoreStatus(m: codec.StorageDeviceStoreStatus): StorageDevice.StoreStatus = m match
    case codec.StorageDeviceStoreStatus.STORAGE_DEVICE_STORE_STATUS_INITIALIZING => StorageDevice.StoreStatus.Initializing
    case codec.StorageDeviceStoreStatus.STORAGE_DEVICE_STORE_STATUS_ACTIVE => StorageDevice.StoreStatus.Active
    case codec.StorageDeviceStoreStatus.STORAGE_DEVICE_STORE_STATUS_TRANSFERRING_IN => StorageDevice.StoreStatus.TransferringIn
    case codec.StorageDeviceStoreStatus.STORAGE_DEVICE_STORE_STATUS_TRANSFERRING_OUT => StorageDevice.StoreStatus.TransferringOut
    case codec.StorageDeviceStoreStatus.STORAGE_DEVICE_STORE_STATUS_REBUILDING => StorageDevice.StoreStatus.Rebuilding
    case f => throw new EncodingError(f"Invalid StorageDeviceStoreStatus: $f")


  def encode(o: StorageDevice.StoreEntry): codec.StorageDeviceStoreEntry =
    val builder = codec.StorageDeviceStoreEntry.newBuilder()
      .setStatus(encodeStorageDeviceStoreStatus(o.status))

    o.transferDevice.foreach: device =>
      builder.setTransferDevice(encode(device))

    builder.build

  def decode(m: codec.StorageDeviceStoreEntry): StorageDevice.StoreEntry =
    val status = decodeStorageDeviceStoreStatus(m.getStatus)
    val transferDevice = if m.hasTransferDevice then
      Some(decode(m.getTransferDevice))
    else
      None

    StorageDevice.StoreEntry(status, transferDevice)


  def encode(o: StorageDevice): codec.StorageDevice =
    val builder = codec.StorageDevice.newBuilder()
      .setStorageDeviceId(encode(o.storageDeviceId))
    
    builder.setHostId(encodeUUID(o.hostId.uuid))

    o.stores.foreach: (storeId, storeEntry) =>
      val keyValue = codec.StorageDeviceStoreKeyValue.newBuilder()
        .setStoreId(ByteString.copyFrom(storeId.toBytes))
        .setEntry(encode(storeEntry))
        .build()
      builder.addStores(keyValue)

    builder.build

  def decode(m: codec.StorageDevice): StorageDevice =
    val storageDeviceId = decode(m.getStorageDeviceId)
    val hostId = HostId(decodeUUID(m.getHostId))
    val stores = m.getStoresList.asScala.map: keyValue =>
      val storeId = StoreId(keyValue.getStoreId.toByteArray)
      val entry = decode(keyValue.getEntry)
      storeId -> entry
    .toMap

    new StorageDevice(storageDeviceId, hostId, stores)


