package org.aspen_ddp.aspen.common.network

import com.google.protobuf.ByteString
import org.aspen_ddp.aspen.client.StoragePool
import org.apache.logging.log4j.scala.Logging
import org.aspen_ddp.aspen.codec
import org.aspen_ddp.aspen.common.{DataBuffer, HLCTimestamp}
import org.aspen_ddp.aspen.common.ida.{IDA, ReedSolomon, Replication}
import org.aspen_ddp.aspen.common.metadata.{HostId, HostState, StorageDeviceId, StorageDeviceState, StoragePoolState}
import org.aspen_ddp.aspen.common.objects.{ByteArrayKeyOrdering, ByteRange, DataObjectPointer, FullObject, IntegerKeyOrdering, Key, KeyOrdering, KeyRange, KeyValueObjectPointer, LargestKeyLessThan, LargestKeyLessThanOrEqualTo, LexicalKeyOrdering, MetadataOnly, ObjectId, ObjectPointer, ObjectRefcount, ObjectRevision, ObjectType, ReadError, SingleKey}
import org.aspen_ddp.aspen.common.paxos.{PersistentState, ProposalId}
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.transaction.KeyValueUpdate.KeyRevision
import org.aspen_ddp.aspen.common.transaction.{DataUpdate, DataUpdateOperation, FinalizationActionId, KeyValueUpdate, LocalTimeRequirement, ObjectUpdate, PreTransactionOpportunisticRebuild, RefcountUpdate, RevisionLock, SerializedFinalizationAction, TransactionDescription, TransactionDisposition, TransactionId, TransactionRequirement, TransactionStatus, VersionBump}
import org.aspen_ddp.aspen.server.cnc
import org.aspen_ddp.aspen.server.cnc.{NewStore, ShutdownStore, TransferStore}
import org.aspen_ddp.aspen.server.crl.TransactionRecoveryState
import org.aspen_ddp.aspen.server.store.backend.{BackendConfig, RocksDBConfig}

import java.nio.{ByteBuffer, ByteOrder}
import java.util.UUID

object Codec extends Logging:

  def encodeUUID(o: UUID): codec.UUID =
    codec.UUID(
      mostSigBits = o.getMostSignificantBits,
      leastSigBits = o.getLeastSignificantBits
    )

  def decodeUUID(m: codec.UUID): UUID =
    new UUID(m.mostSigBits, m.leastSigBits)


  def encodeKeyComparison(o: KeyOrdering): codec.KeyComparison = o match
    case ByteArrayKeyOrdering => codec.KeyComparison.KEY_COMPARISON_BYTE_ARRAY
    case IntegerKeyOrdering => codec.KeyComparison.KEY_COMPARISON_INTEGER
    case LexicalKeyOrdering => codec.KeyComparison.KEY_COMPARISON_LEXICAL

  def decodeKeyComparison(m: codec.KeyComparison): KeyOrdering = m match
    case codec.KeyComparison.KEY_COMPARISON_BYTE_ARRAY => ByteArrayKeyOrdering
    case codec.KeyComparison.KEY_COMPARISON_INTEGER => IntegerKeyOrdering
    case codec.KeyComparison.KEY_COMPARISON_LEXICAL => LexicalKeyOrdering
    case codec.KeyComparison.Unrecognized(v) => throw new EncodingError(f"Invalid key comparison: $v")


  def encode(o: Replication): codec.Replication =
    codec.Replication(
      width = o.width,
      writeThreshold = o.writeThreshold
    )

  def decode(m: codec.Replication): Replication =
    Replication(m.width, m.writeThreshold)


  def encode(o: ReedSolomon): codec.ReedSolomon =
    codec.ReedSolomon(
      width = o.width,
      restoreThreshold = o.restoreThreshold,
      writeThreshold = o.writeThreshold
    )

  def decode(m: codec.ReedSolomon): ReedSolomon =
    ReedSolomon(m.width, m.restoreThreshold, m.writeThreshold)


  def encode(o: IDA): codec.IDA =
    val ida = o match
      case r: Replication => codec.IDA.Ida.Replication(encode(r))
      case r: ReedSolomon => codec.IDA.Ida.ReedSolomon(encode(r))
    codec.IDA(ida = ida)

  def decode(m: codec.IDA): IDA =
    m.ida match
      case codec.IDA.Ida.Replication(r) => decode(r)
      case codec.IDA.Ida.ReedSolomon(r) => decode(r)
      case codec.IDA.Ida.Empty => throw new EncodingError("Unknown IDA")


  def encode(o: ObjectRevision): codec.ObjectRevision =
    codec.ObjectRevision(uuid = Some(encodeUUID(o.lastUpdateTxUUID)))

  def decode(m: codec.ObjectRevision): ObjectRevision =
    ObjectRevision(TransactionId(decodeUUID(m.uuid.get)))


  def encode(o: ObjectRefcount): codec.ObjectRefcount =
    codec.ObjectRefcount(
      updateSerial = o.updateSerial,
      refcount = o.count
    )

  def decode(m: codec.ObjectRefcount): ObjectRefcount =
    ObjectRefcount(m.updateSerial.toInt, m.refcount)


  def encodeObjectType(o: ObjectType.Value): codec.ObjectType = o match
    case ObjectType.Data => codec.ObjectType.OBJECT_TYPE_DATA
    case ObjectType.KeyValue => codec.ObjectType.OBJECT_TYPE_KEYVALUE

  def decodeObjectType(m: codec.ObjectType): ObjectType.Value = m match
    case codec.ObjectType.OBJECT_TYPE_DATA => ObjectType.Data
    case codec.ObjectType.OBJECT_TYPE_KEYVALUE => ObjectType.KeyValue
    case codec.ObjectType.Unrecognized(v) => throw new EncodingError(f"Invalid ObjectType: $v")


  def encode(o: ObjectPointer): codec.ObjectPointer =
    codec.ObjectPointer(
      uuid = Some(encodeUUID(o.id.uuid)),
      poolUuid = Some(encodeUUID(o.poolId.uuid)),
      objectType = encodeObjectType(o.objectType),
      storePointer = ByteString.copyFrom(o.storePointer)
    )

  def decode(m: codec.ObjectPointer): ObjectPointer =
    val uuid = decodeUUID(m.uuid.get)
    val poolUuid = decodeUUID(m.poolUuid.get)
    val objectType = decodeObjectType(m.objectType)
    val storePointer = m.storePointer.toByteArray

    objectType match
      case ObjectType.Data => DataObjectPointer(ObjectId(uuid), PoolId(poolUuid), storePointer)
      case ObjectType.KeyValue => KeyValueObjectPointer(ObjectId(uuid), PoolId(poolUuid), storePointer)


  def encodeTransactionStatus(o: TransactionStatus.Value): codec.TransactionStatus = o match
    case TransactionStatus.Unresolved => codec.TransactionStatus.TRANSACTION_STATUS_UNRESOLVED
    case TransactionStatus.Committed => codec.TransactionStatus.TRANSACTION_STATUS_COMMITTED
    case TransactionStatus.Aborted => codec.TransactionStatus.TRANSACTION_STATUS_ABORT

  def decodeTransactionStatus(m: codec.TransactionStatus): TransactionStatus.Value = m match
    case codec.TransactionStatus.TRANSACTION_STATUS_UNRESOLVED => TransactionStatus.Unresolved
    case codec.TransactionStatus.TRANSACTION_STATUS_COMMITTED => TransactionStatus.Committed
    case codec.TransactionStatus.TRANSACTION_STATUS_ABORT => TransactionStatus.Aborted
    case codec.TransactionStatus.Unrecognized(v) => throw new EncodingError(f"Invalid Transaction Status: $v")


  def encodeTransactionDisposition(o: TransactionDisposition.Value): codec.TransactionDisposition = o match
    case TransactionDisposition.Undetermined => codec.TransactionDisposition.TRANSACTION_DISPOSITION_UNDETERMINED
    case TransactionDisposition.VoteCommit => codec.TransactionDisposition.TRANSACTION_DISPOSITION_VOTE_COMMIT
    case TransactionDisposition.VoteAbort => codec.TransactionDisposition.TRANSACTION_DISPOSITION_VOTE_ABORT

  def decodeTransactionDisposition(m: codec.TransactionDisposition): TransactionDisposition.Value = m match
    case codec.TransactionDisposition.TRANSACTION_DISPOSITION_UNDETERMINED => TransactionDisposition.Undetermined
    case codec.TransactionDisposition.TRANSACTION_DISPOSITION_VOTE_COMMIT => TransactionDisposition.VoteCommit
    case codec.TransactionDisposition.TRANSACTION_DISPOSITION_VOTE_ABORT => TransactionDisposition.VoteAbort
    case codec.TransactionDisposition.Unrecognized(v) => throw new EncodingError(f"Invalid Transaction Disposition: $v")


  def encodeDataUpdateOperation(o: DataUpdateOperation.Value): codec.DataUpdateOperation = o match
    case DataUpdateOperation.Append => codec.DataUpdateOperation.DATA_UPDATE_OPERATION_APPEND
    case DataUpdateOperation.Overwrite => codec.DataUpdateOperation.DATA_UPDATE_OPERATION_OVERWRITE

  def decodeDataUpdateOperation(m: codec.DataUpdateOperation): DataUpdateOperation.Value = m match
    case codec.DataUpdateOperation.DATA_UPDATE_OPERATION_APPEND => DataUpdateOperation.Append
    case codec.DataUpdateOperation.DATA_UPDATE_OPERATION_OVERWRITE => DataUpdateOperation.Overwrite
    case codec.DataUpdateOperation.Unrecognized(v) => throw new EncodingError(f"Invalid DataUpdateOperation: $v")


  def encode(o: DataUpdate): codec.DataUpdate =
    codec.DataUpdate(
      objectPointer = Some(encode(o.objectPointer)),
      requiredRevision = Some(encode(o.requiredRevision)),
      operation = encodeDataUpdateOperation(o.operation)
    )

  def decode(m: codec.DataUpdate): DataUpdate =
    DataUpdate(decode(m.objectPointer.get),
      decode(m.requiredRevision.get),
      decodeDataUpdateOperation(m.operation))


  def encode(o: RefcountUpdate): codec.RefcountUpdate =
    codec.RefcountUpdate(
      objectPointer = Some(encode(o.objectPointer)),
      requiredRefcount = Some(encode(o.requiredRefcount)),
      newRefcount = Some(encode(o.newRefcount))
    )

  def decode(m: codec.RefcountUpdate): RefcountUpdate =
    RefcountUpdate(decode(m.objectPointer.get),
      decode(m.requiredRefcount.get),
      decode(m.newRefcount.get))


  def encode(o: VersionBump): codec.VersionBump =
    codec.VersionBump(
      objectPointer = Some(encode(o.objectPointer)),
      requiredRevision = Some(encode(o.requiredRevision))
    )

  def decode(m: codec.VersionBump): VersionBump =
    VersionBump(decode(m.objectPointer.get),
      decode(m.requiredRevision.get))


  def encode(o: RevisionLock): codec.RevisionLock =
    codec.RevisionLock(
      objectPointer = Some(encode(o.objectPointer)),
      requiredRevision = Some(encode(o.requiredRevision))
    )

  def decode(m: codec.RevisionLock): RevisionLock =
    RevisionLock(decode(m.objectPointer.get),
      decode(m.requiredRevision.get))


  def encode(o: KeyValueUpdate.KeyRequirement): codec.KVReq =
    val key = ByteString.copyFrom(o.key.bytes)

    o match
      case r: KeyValueUpdate.KeyRevision =>
        codec.KVReq(
          requirement = codec.KeyRequirement.KEY_REQUIREMENT_KEY_REVISION,
          key = key,
          revision = Some(encode(r.revision))
        )

      case r: KeyValueUpdate.KeyObjectRevision =>
        codec.KVReq(
          requirement = codec.KeyRequirement.KEY_REQUIREMENT_KEY_OBJECT_REVISION,
          key = key,
          revision = Some(encode(r.revision))
        )

      case r: KeyValueUpdate.WithinRange =>
        codec.KVReq(
          requirement = codec.KeyRequirement.KEY_REQUIREMENT_WITHIN_RANGE,
          key = key,
          comparison = encodeKeyComparison(r.ordering)
        )

      case _: KeyValueUpdate.Exists =>
        codec.KVReq(
          requirement = codec.KeyRequirement.KEY_REQUIREMENT_EXISTS,
          key = key
        )

      case _: KeyValueUpdate.MayExist =>
        codec.KVReq(
          requirement = codec.KeyRequirement.KEY_REQUIREMENT_MAY_EXIST,
          key = key
        )

      case _: KeyValueUpdate.DoesNotExist =>
        codec.KVReq(
          requirement = codec.KeyRequirement.KEY_REQUIREMENT_DOES_NOT_EXIST,
          key = key
        )

      case r: KeyValueUpdate.TimestampEquals =>
        codec.KVReq(
          requirement = codec.KeyRequirement.KEY_REQUIREMENT_TIMESTAMP_EQUALS,
          key = key,
          timestamp = r.timestamp.asLong
        )

      case r: KeyValueUpdate.TimestampLessThan =>
        codec.KVReq(
          requirement = codec.KeyRequirement.KEY_REQUIREMENT_TIMESTAMP_LESS_THAN,
          key = key,
          timestamp = r.timestamp.asLong
        )

      case r: KeyValueUpdate.TimestampGreaterThan =>
        codec.KVReq(
          requirement = codec.KeyRequirement.KEY_REQUIREMENT_TIMESTAMP_GREATER_THAN,
          key = key,
          timestamp = r.timestamp.asLong
        )

  def decode(m: codec.KVReq): KeyValueUpdate.KeyRequirement =
    val key = Key(m.key.toByteArray)
    val timestamp = HLCTimestamp(m.timestamp)

    m.requirement match
      case codec.KeyRequirement.KEY_REQUIREMENT_EXISTS => KeyValueUpdate.Exists(key)
      case codec.KeyRequirement.KEY_REQUIREMENT_MAY_EXIST => KeyValueUpdate.MayExist(key)
      case codec.KeyRequirement.KEY_REQUIREMENT_DOES_NOT_EXIST => KeyValueUpdate.DoesNotExist(key)
      case codec.KeyRequirement.KEY_REQUIREMENT_TIMESTAMP_EQUALS => KeyValueUpdate.TimestampEquals(key, timestamp)
      case codec.KeyRequirement.KEY_REQUIREMENT_TIMESTAMP_LESS_THAN => KeyValueUpdate.TimestampLessThan(key, timestamp)
      case codec.KeyRequirement.KEY_REQUIREMENT_TIMESTAMP_GREATER_THAN => KeyValueUpdate.TimestampGreaterThan(key, timestamp)
      case codec.KeyRequirement.KEY_REQUIREMENT_KEY_REVISION =>
        val rev = decode(m.revision.get)
        KeyValueUpdate.KeyRevision(key, rev)
      case codec.KeyRequirement.KEY_REQUIREMENT_KEY_OBJECT_REVISION =>
        val rev = decode(m.revision.get)
        KeyValueUpdate.KeyObjectRevision(key, rev)
      case codec.KeyRequirement.KEY_REQUIREMENT_WITHIN_RANGE =>
        val ord = decodeKeyComparison(m.comparison)
        KeyValueUpdate.WithinRange(key, ord)
      case codec.KeyRequirement.Unrecognized(v) => throw new EncodingError(f"Invalid KeyRequirement: $v")


  def encode(o: KeyRevision): codec.KeyRevision =
    codec.KeyRevision(
      key = ByteString.copyFrom(o.key.bytes),
      revision = Some(encode(o.revision))
    )

  def decode(m: codec.KeyRevision): KeyRevision =
    KeyRevision(Key(m.key.toByteArray), decode(m.revision.get))


  def encode(o: KeyValueUpdate): codec.KeyValueUpdate =
    codec.KeyValueUpdate(
      objectPointer = Some(encode(o.objectPointer)),
      requiredRevision = o.requiredRevision.map(encode),
      contentLock = o.contentLock.map(lck => lck.fullContents.map(encode)).getOrElse(Seq.empty),
      requirements = o.requirements.map(encode)
    )

  def decode(m: codec.KeyValueUpdate): KeyValueUpdate =
    val objectPointer = decode(m.objectPointer.get).asInstanceOf[KeyValueObjectPointer]

    val requiredRevision = m.requiredRevision.map(decode)

    val contentLock = if m.contentLock.isEmpty then
      None
    else
      Some(KeyValueUpdate.FullContentLock(m.contentLock.map(decode).toList))

    val requirements = m.requirements.map(decode).toList

    KeyValueUpdate(objectPointer, requiredRevision, contentLock, requirements)


  def encodeLocalTimeRequirementEnum(o: LocalTimeRequirement.Requirement.Value): codec.LocalTimeRequirementEnum = o match
    case LocalTimeRequirement.Requirement.LessThan => codec.LocalTimeRequirementEnum.LOCAL_TIME_REQUIREMENT_ENUM_LESS_THAN
    case LocalTimeRequirement.Requirement.GreaterThan => codec.LocalTimeRequirementEnum.LOCAL_TIME_REQUIREMENT_ENUM_GREATER_THAN
    case LocalTimeRequirement.Requirement.Equals => codec.LocalTimeRequirementEnum.LOCAL_TIME_REQUIREMENT_ENUM_EQUALS

  def decodeLocalTimeRequirementEnum(m: codec.LocalTimeRequirementEnum): LocalTimeRequirement.Requirement.Value = m match
    case codec.LocalTimeRequirementEnum.LOCAL_TIME_REQUIREMENT_ENUM_LESS_THAN => LocalTimeRequirement.Requirement.LessThan
    case codec.LocalTimeRequirementEnum.LOCAL_TIME_REQUIREMENT_ENUM_GREATER_THAN => LocalTimeRequirement.Requirement.GreaterThan
    case codec.LocalTimeRequirementEnum.LOCAL_TIME_REQUIREMENT_ENUM_EQUALS => LocalTimeRequirement.Requirement.Equals
    case codec.LocalTimeRequirementEnum.Unrecognized(v) => throw new EncodingError(f"Invalid LocalTimeRequirementEnum: $v")


  def encode(o: LocalTimeRequirement): codec.LocalTimeRequirement =
    codec.LocalTimeRequirement(
      timestamp = o.timestamp.asLong,
      requirement = encodeLocalTimeRequirementEnum(o.tsRequirement)
    )

  def decode(m: codec.LocalTimeRequirement): LocalTimeRequirement =
    LocalTimeRequirement(HLCTimestamp(m.timestamp), decodeLocalTimeRequirementEnum(m.requirement))


  def encode(o: TransactionRequirement): codec.TransactionRequirement =
    o match
      case tr: DataUpdate => codec.TransactionRequirement(dataUpdate = Some(encode(tr)))
      case tr: RefcountUpdate => codec.TransactionRequirement(refcountUpdate = Some(encode(tr)))
      case tr: VersionBump => codec.TransactionRequirement(versionBump = Some(encode(tr)))
      case tr: RevisionLock => codec.TransactionRequirement(revisionLock = Some(encode(tr)))
      case tr: KeyValueUpdate => codec.TransactionRequirement(kvUpdate = Some(encode(tr)))
      case tr: LocalTimeRequirement => codec.TransactionRequirement(localtime = Some(encode(tr)))

  def decode(m: codec.TransactionRequirement): TransactionRequirement =
    m.dataUpdate.map(decode)
      .orElse(m.refcountUpdate.map(decode))
      .orElse(m.versionBump.map(decode))
      .orElse(m.revisionLock.map(decode))
      .orElse(m.kvUpdate.map(decode))
      .orElse(m.localtime.map(decode))
      .getOrElse(throw new EncodingError("Unknown Transaction Requirement"))


  def encode(o: SerializedFinalizationAction): codec.SerializedFinalizationAction =
    codec.SerializedFinalizationAction(
      typeUuid = Some(encodeUUID(o.typeId.uuid)),
      data = ByteString.copyFrom(o.data)
    )

  def decode(m: codec.SerializedFinalizationAction): SerializedFinalizationAction =
    SerializedFinalizationAction(FinalizationActionId(decodeUUID(m.typeUuid.get)), m.data.toByteArray)


  def encode(o: StoreId): codec.StoreId =
    codec.StoreId(
      storagePoolUuid = Some(encodeUUID(o.poolId.uuid)),
      storagePoolIndex = o.poolIndex
    )

  def decode(m: codec.StoreId): StoreId =
    StoreId(PoolId(decodeUUID(m.storagePoolUuid.get)), m.storagePoolIndex.toByte)


  def encode(o: TransactionDescription): codec.TransactionDescription =
    codec.TransactionDescription(
      transactionUuid = Some(encodeUUID(o.transactionId.uuid)),
      startTimestamp = o.startTimestamp.asLong,
      primaryObject = Some(encode(o.primaryObject)),
      designatedLeaderUid = o.designatedLeaderUID,
      requirements = o.requirements.map(encode),
      finalizationActions = o.finalizationActions.map(encode),
      originatingClient = o.originatingClient.map(c => encodeUUID(c.uuid)),
      notifyOnResolution = o.notifyOnResolution.map(encode),
      notes = o.notes,
      poolIdas = o.poolIDAMap.map { (poolId, ida) =>
        codec.PoolIDA(
          poolId = Some(encodeUUID(poolId.uuid)),
          ida = Some(encode(ida))
        )
      }.toSeq,
      allocatingObjects = o.allocatingObjects.map(oid => encodeUUID(oid.uuid)).toSeq
    )

  def decode(m: codec.TransactionDescription): TransactionDescription =
    val txuuid = TransactionId(decodeUUID(m.transactionUuid.get))
    val startTs = HLCTimestamp(m.startTimestamp)
    val primaryObj = decode(m.primaryObject.get)
    val designatedLeader = m.designatedLeaderUid.toByte
    val requirements = m.requirements.map(decode).toList
    val serializedFas = m.finalizationActions.map(decode).toList
    val origClient = m.originatingClient.map(c => ClientId(decodeUUID(c)))
    val notifyOnRes = m.notifyOnResolution.map(decode).toList
    val notes = m.notes.toList
    val poolIDAMap = m.poolIdas.map { pida =>
      PoolId(decodeUUID(pida.poolId.get)) -> decode(pida.ida.get)
    }.toMap
    val primaryObjectIDA = poolIDAMap(primaryObj.poolId)
    val allocatingObjects = m.allocatingObjects.map(u => ObjectId(decodeUUID(u))).toSet

    TransactionDescription(txuuid, startTs, primaryObj, designatedLeader, requirements,
      serializedFas, origClient, notifyOnRes, notes, primaryObjectIDA, poolIDAMap, allocatingObjects)


  def encode(o: ProposalId): codec.ProposalId =
    codec.ProposalId(
      number = o.number,
      uid = o.peer
    )

  def decode(m: codec.ProposalId): ProposalId =
    ProposalId(m.number, m.uid.toByte)


  def encode(o: TxPrepare): codec.TxPrepare =
    codec.TxPrepare(
      to = Some(encode(o.to)),
      from = Some(encode(o.from)),
      txd = Some(encode(o.txd)),
      proposalId = Some(encode(o.proposalId))
    )

  def decode(m: codec.TxPrepare,
             objectUpdates: List[ObjectUpdate],
             preTxRebuilds: List[PreTransactionOpportunisticRebuild]): TxPrepare =
    TxPrepare(decode(m.to.get), decode(m.from.get), decode(m.txd.get), decode(m.proposalId.get),
      objectUpdates, preTxRebuilds)


  def encode(o: TxPrepareResponse): codec.TxPrepareResponse =
    val (responseType, promisedId, lastAcceptedId, lastAcceptedValue) = o.response match
      case Left(nack) =>
        (codec.TxPrepareResponseType.TX_PREPARE_RESPONSE_TYPE_NACK,
          Some(encode(nack.promisedId)), None, false)
      case Right(promise) =>
        promise.lastAccepted match
          case Some((pid, value)) =>
            (codec.TxPrepareResponseType.TX_PREPARE_RESPONSE_TYPE_PROMISE,
              None, Some(encode(pid)), value)
          case None =>
            (codec.TxPrepareResponseType.TX_PREPARE_RESPONSE_TYPE_PROMISE,
              None, None, false)

    val arr = new Array[Byte](o.collisions.size * 16)
    val bb = ByteBuffer.wrap(arr)
    bb.order(ByteOrder.BIG_ENDIAN)
    o.collisions.foreach: id =>
      bb.putLong(id.uuid.getMostSignificantBits)
      bb.putLong(id.uuid.getLeastSignificantBits)

    codec.TxPrepareResponse(
      to = Some(encode(o.to)),
      from = Some(encode(o.from)),
      transactionUuid = Some(encodeUUID(o.transactionId.uuid)),
      responseType = responseType,
      proposalId = Some(encode(o.proposalId)),
      promisedId = promisedId,
      lastAcceptedId = lastAcceptedId,
      lastAcceptedValue = lastAcceptedValue,
      disposition = encodeTransactionDisposition(o.disposition),
      transactionCollisions = ByteString.copyFrom(arr)
    )

  def decode(m: codec.TxPrepareResponse): TxPrepareResponse =
    val to = decode(m.to.get)
    val from = decode(m.from.get)
    val txid = TransactionId(decodeUUID(m.transactionUuid.get))
    val response = m.responseType match
      case codec.TxPrepareResponseType.TX_PREPARE_RESPONSE_TYPE_NACK =>
        Left(TxPrepareResponse.Nack(decode(m.promisedId.get)))
      case codec.TxPrepareResponseType.TX_PREPARE_RESPONSE_TYPE_PROMISE =>
        val la = m.lastAcceptedId.map(pid => (decode(pid), m.lastAcceptedValue))
        Right(TxPrepareResponse.Promise(la))
      case codec.TxPrepareResponseType.Unrecognized(_) =>
        throw new EncodingError("Unknown Prepare Response Type")
    val proposalId = decode(m.proposalId.get)
    val disposition = decodeTransactionDisposition(m.disposition)
    var collisions = List[TransactionId]()

    val bb = m.transactionCollisions.asReadOnlyByteBuffer()
    bb.order(ByteOrder.BIG_ENDIAN)
    while (bb.remaining() != 0) {
      val msb = bb.getLong()
      val lsb = bb.getLong()
      collisions = TransactionId(new UUID(msb, lsb)) :: collisions
    }

    TxPrepareResponse(to, from, txid, response, proposalId, disposition, collisions)


  def encode(o: TxAccept): codec.TxAccept =
    codec.TxAccept(
      to = Some(encode(o.to)),
      from = Some(encode(o.from)),
      transactionUuid = Some(encodeUUID(o.transactionId.uuid)),
      proposalId = Some(encode(o.proposalId)),
      value = o.value
    )

  def decode(m: codec.TxAccept): TxAccept =
    TxAccept(decode(m.to.get), decode(m.from.get), TransactionId(decodeUUID(m.transactionUuid.get)),
      decode(m.proposalId.get), m.value)


  def encode(o: TxAcceptResponse): codec.TxAcceptResponse =
    val (isNack, promisedId, value) = o.response match
      case Left(nack) =>
        (true, Some(encode(nack.promisedId)), false)
      case Right(accepted) =>
        (false, None, accepted.value)

    codec.TxAcceptResponse(
      to = Some(encode(o.to)),
      from = Some(encode(o.from)),
      transactionUuid = Some(encodeUUID(o.transactionId.uuid)),
      proposalId = Some(encode(o.proposalId)),
      isNack = isNack,
      promisedId = promisedId,
      value = value
    )

  def decode(m: codec.TxAcceptResponse): TxAcceptResponse =
    val response = if m.isNack then
      Left(TxAcceptResponse.Nack(decode(m.promisedId.get)))
    else
      Right(TxAcceptResponse.Accepted(m.value))

    TxAcceptResponse(decode(m.to.get), decode(m.from.get), TransactionId(decodeUUID(m.transactionUuid.get)),
      decode(m.proposalId.get), response)


  def encode(o: TxResolved): codec.TxResolved =
    codec.TxResolved(
      to = Some(encode(o.to)),
      from = Some(encode(o.from)),
      transactionUuid = Some(encodeUUID(o.transactionId.uuid)),
      committed = o.committed
    )

  def decode(m: codec.TxResolved): TxResolved =
    TxResolved(decode(m.to.get), decode(m.from.get), TransactionId(decodeUUID(m.transactionUuid.get)),
      m.committed)


  def encode(o: TxUnknownStore): codec.TxUnknownStore =
    codec.TxUnknownStore(
      to = Some(encode(o.to)),
      from = Some(encode(o.from)),
      transactionUuid = Some(encodeUUID(o.transactionId.uuid))
    )

  def decode(m: codec.TxUnknownStore): TxUnknownStore =
    TxUnknownStore(decode(m.to.get), decode(m.from.get), TransactionId(decodeUUID(m.transactionUuid.get)))


  def encode(o: TxCommitted): codec.TxCommitted =
    val arr = new Array[Byte](o.objectCommitErrors.size * 16)
    val bb = ByteBuffer.wrap(arr)
    bb.order(ByteOrder.BIG_ENDIAN)
    o.objectCommitErrors.foreach: id =>
      bb.putLong(id.uuid.getMostSignificantBits)
      bb.putLong(id.uuid.getLeastSignificantBits)

    codec.TxCommitted(
      to = Some(encode(o.to)),
      from = Some(encode(o.from)),
      transactionUuid = Some(encodeUUID(o.transactionId.uuid)),
      objectCommitErrors = ByteString.copyFrom(arr)
    )

  def decode(m: codec.TxCommitted): TxCommitted =
    var commitErrors = List[ObjectId]()

    val bb = m.objectCommitErrors.asReadOnlyByteBuffer()
    bb.order(ByteOrder.BIG_ENDIAN)
    while (bb.remaining() != 0) {
      val msb = bb.getLong()
      val lsb = bb.getLong()
      commitErrors = ObjectId(new UUID(msb, lsb)) :: commitErrors
    }

    TxCommitted(decode(m.to.get), decode(m.from.get), TransactionId(decodeUUID(m.transactionUuid.get)),
      commitErrors)


  def encode(o: TxFinalized): codec.TxFinalized =
    codec.TxFinalized(
      to = Some(encode(o.to)),
      from = Some(encode(o.from)),
      transactionUuid = Some(encodeUUID(o.transactionId.uuid)),
      committed = o.committed
    )

  def decode(m: codec.TxFinalized): TxFinalized =
    TxFinalized(decode(m.to.get), decode(m.from.get), TransactionId(decodeUUID(m.transactionUuid.get)),
      m.committed)


  def encode(o: TxHeartbeat): codec.TxHeartbeat =
    codec.TxHeartbeat(
      to = Some(encode(o.to)),
      from = Some(encode(o.from)),
      transactionUuid = Some(encodeUUID(o.transactionId.uuid))
    )

  def decode(m: codec.TxHeartbeat): TxHeartbeat =
    TxHeartbeat(decode(m.to.get), decode(m.from.get), TransactionId(decodeUUID(m.transactionUuid.get)))


  def encode(o: TxStatusRequest): codec.TxStatusRequest =
    codec.TxStatusRequest(
      to = Some(encode(o.to)),
      from = Some(encode(o.from)),
      transactionUuid = Some(encodeUUID(o.transactionId.uuid)),
      requestUuid = Some(encodeUUID(o.requestUUID))
    )

  def decode(m: codec.TxStatusRequest): TxStatusRequest =
    TxStatusRequest(decode(m.to.get), decode(m.from.get), TransactionId(decodeUUID(m.transactionUuid.get)),
      decodeUUID(m.requestUuid.get))


  def encode(o: TxStatusResponse): codec.TxStatusResponse =
    val (haveStatus, isFinalized, status) = o.status match
      case None => (false, false, codec.TransactionStatus.TRANSACTION_STATUS_UNRESOLVED)
      case Some(stat) => (true, stat.finalized, encodeTransactionStatus(stat.status))

    codec.TxStatusResponse(
      to = Some(encode(o.to)),
      from = Some(encode(o.from)),
      transactionUuid = Some(encodeUUID(o.transactionId.uuid)),
      requestUuid = Some(encodeUUID(o.requestUUID)),
      haveStatus = haveStatus,
      isFinalized = isFinalized,
      status = status
    )

  def decode(m: codec.TxStatusResponse): TxStatusResponse =
    val status = if m.haveStatus then
      Some(TxStatusResponse.TxStatus(decodeTransactionStatus(m.status), m.isFinalized))
    else
      None

    TxStatusResponse(decode(m.to.get), decode(m.from.get), TransactionId(decodeUUID(m.transactionUuid.get)),
      decodeUUID(m.requestUuid.get), status)


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
    case codec.ObjectReadError.Unrecognized(v) => throw new EncodingError(f"Invalid ObjectReadError: $v")


  def encode(o: Read): codec.Read =
    val (readType, key, min, max, comparison, offset, length) = o.readType match
      case _: MetadataOnly =>
        (codec.ReadType.READ_TYPE_METADATA_ONLY, ByteString.EMPTY, ByteString.EMPTY, ByteString.EMPTY,
          codec.KeyComparison.KEY_COMPARISON_BYTE_ARRAY, 0, 0)
      case _: FullObject =>
        (codec.ReadType.READ_TYPE_FULL_OBJECT, ByteString.EMPTY, ByteString.EMPTY, ByteString.EMPTY,
          codec.KeyComparison.KEY_COMPARISON_BYTE_ARRAY, 0, 0)
      case rt: ByteRange =>
        (codec.ReadType.READ_TYPE_BYTE_RANGE, ByteString.EMPTY, ByteString.EMPTY, ByteString.EMPTY,
          codec.KeyComparison.KEY_COMPARISON_BYTE_ARRAY, rt.offset, rt.length)
      case rt: SingleKey =>
        (codec.ReadType.READ_TYPE_SINGLE_KEY, ByteString.copyFrom(rt.key.bytes), ByteString.EMPTY, ByteString.EMPTY,
          encodeKeyComparison(rt.ordering), 0, 0)
      case rt: LargestKeyLessThan =>
        (codec.ReadType.READ_TYPE_LARGEST_KEY_LESS_THAN, ByteString.copyFrom(rt.key.bytes), ByteString.EMPTY, ByteString.EMPTY,
          encodeKeyComparison(rt.ordering), 0, 0)
      case rt: LargestKeyLessThanOrEqualTo =>
        (codec.ReadType.READ_TYPE_LARGEST_KEY_LESS_THAN_OR_EQUAL_TO, ByteString.copyFrom(rt.key.bytes), ByteString.EMPTY, ByteString.EMPTY,
          encodeKeyComparison(rt.ordering), 0, 0)
      case rt: KeyRange =>
        (codec.ReadType.READ_TYPE_KEY_RANGE, ByteString.EMPTY, ByteString.copyFrom(rt.minimum.bytes), ByteString.copyFrom(rt.maximum.bytes),
          encodeKeyComparison(rt.ordering), 0, 0)

    codec.Read(
      toStore = Some(encode(o.toStore)),
      fromClient = Some(encodeUUID(o.fromClient.uuid)),
      readUuid = Some(encodeUUID(o.readUUID)),
      objectPointer = Some(encode(o.objectPointer)),
      readType = readType,
      key = key,
      min = min,
      max = max,
      comparison = comparison,
      offset = offset,
      length = length
    )

  def decode(m: codec.Read): Read =
    val toStore = decode(m.toStore.get)
    val from = ClientId(decodeUUID(m.fromClient.get))
    val readUuid = decodeUUID(m.readUuid.get)
    val objPtr = decode(m.objectPointer.get)
    val readType = m.readType match
      case codec.ReadType.READ_TYPE_METADATA_ONLY => MetadataOnly()
      case codec.ReadType.READ_TYPE_FULL_OBJECT => FullObject()
      case codec.ReadType.READ_TYPE_BYTE_RANGE => ByteRange(m.offset, m.length)
      case codec.ReadType.READ_TYPE_SINGLE_KEY => SingleKey(Key(m.key.toByteArray), decodeKeyComparison(m.comparison))
      case codec.ReadType.READ_TYPE_LARGEST_KEY_LESS_THAN => LargestKeyLessThan(Key(m.key.toByteArray), decodeKeyComparison(m.comparison))
      case codec.ReadType.READ_TYPE_LARGEST_KEY_LESS_THAN_OR_EQUAL_TO => LargestKeyLessThanOrEqualTo(Key(m.key.toByteArray), decodeKeyComparison(m.comparison))
      case codec.ReadType.READ_TYPE_KEY_RANGE => KeyRange(Key(m.min.toByteArray), Key(m.max.toByteArray), decodeKeyComparison(m.comparison))
      case codec.ReadType.Unrecognized(v) => throw new EncodingError(f"Invalid Read Type: $v")

    Read(toStore, from, readUuid, objPtr, readType)


  def encode(o: ReadResponse): codec.ReadResponse =
    o.result match
      case Left(err) =>
        codec.ReadResponse(
          fromStore = Some(encode(o.fromStore)),
          toClient = Some(encodeUUID(o.toClient.uuid)),
          readUuid = Some(encodeUUID(o.readUUID)),
          readTime = o.readTime.asLong,
          haveData = false,
          readError = encodeReadError(err)
        )

      case Right(r) =>
        val arr = new Array[Byte](r.lockedWriteTransactions.size * 16)
        val bb = ByteBuffer.wrap(arr)
        bb.order(ByteOrder.BIG_ENDIAN)
        r.lockedWriteTransactions.foreach: id =>
          bb.putLong(id.uuid.getMostSignificantBits)
          bb.putLong(id.uuid.getLeastSignificantBits)

        codec.ReadResponse(
          fromStore = Some(encode(o.fromStore)),
          toClient = Some(encodeUUID(o.toClient.uuid)),
          readUuid = Some(encodeUUID(o.readUUID)),
          readTime = o.readTime.asLong,
          haveData = true,
          revision = Some(encode(r.revision)),
          refcount = Some(encode(r.refcount)),
          sizeOnStore = r.sizeOnStore,
          timestamp = r.timestamp.asLong,
          objectData = r.objectData.map(data => ByteString.copyFrom(data.asReadOnlyBuffer())).getOrElse(ByteString.EMPTY),
          lockedWriteTransactions = ByteString.copyFrom(arr)
        )

  def decode(m: codec.ReadResponse): ReadResponse =
    val fromStore = decode(m.fromStore.get)
    val toClient = ClientId(decodeUUID(m.toClient.get))
    val readUuid = decodeUUID(m.readUuid.get)
    val readTime = HLCTimestamp(m.readTime)
    val result = if ! m.haveData then
      Left(decodeReadError(m.readError))
    else
      val revision = decode(m.revision.get)
      val refcount = decode(m.refcount.get)
      val timestamp = HLCTimestamp(m.timestamp)
      val sizeOnStore = m.sizeOnStore
      val data = if m.objectData.size == 0 then
        None
      else
        Some(DataBuffer(m.objectData.toByteArray))

      var lockedTx = List[TransactionId]()

      val bb = m.lockedWriteTransactions.asReadOnlyByteBuffer()
      bb.order(ByteOrder.BIG_ENDIAN)
      while (bb.remaining() != 0) {
        val msb = bb.getLong()
        val lsb = bb.getLong()
        lockedTx = TransactionId(new UUID(msb, lsb)) :: lockedTx
      }

      Right(ReadResponse.CurrentState(revision, refcount, timestamp, sizeOnStore, data, lockedTx.toSet))

    ReadResponse(toClient, fromStore, readUuid, readTime, result)


  def encode(o: OpportunisticRebuild): codec.OpportunisticRebuild =
    codec.OpportunisticRebuild(
      toStore = Some(encode(o.toStore)),
      fromClient = Some(encodeUUID(o.fromClient.uuid)),
      pointer = Some(encode(o.pointer)),
      revision = Some(encode(o.revision)),
      refcount = Some(encode(o.refcount)),
      timestamp = o.timestamp.asLong,
      data = ByteString.copyFrom(o.data.asReadOnlyBuffer())
    )

  def decode(m: codec.OpportunisticRebuild): OpportunisticRebuild =
    val toStore = decode(m.toStore.get)
    val fromClient = ClientId(decodeUUID(m.fromClient.get))
    val pointer = decode(m.pointer.get)
    val revision = decode(m.revision.get)
    val refcount = decode(m.refcount.get)
    val timestamp = HLCTimestamp(m.timestamp)
    val data = DataBuffer(m.data.toByteArray)
    OpportunisticRebuild(toStore, fromClient, pointer, revision, refcount, timestamp, data)


  def encode(o: TransactionCompletionQuery): codec.TransactionCompletionQuery =
    codec.TransactionCompletionQuery(
      toStore = Some(encode(o.toStore)),
      fromClient = Some(encodeUUID(o.fromClient.uuid)),
      queryUuid = Some(encodeUUID(o.queryUUID)),
      transactionUuid = Some(encodeUUID(o.transactionId.uuid))
    )

  def decode(m: codec.TransactionCompletionQuery): TransactionCompletionQuery =
    val toStore = decode(m.toStore.get)
    val fromClient = ClientId(decodeUUID(m.fromClient.get))
    val queryUuid = decodeUUID(m.queryUuid.get)
    val txid = TransactionId(decodeUUID(m.transactionUuid.get))
    TransactionCompletionQuery(toStore, fromClient, queryUuid, txid)


  def encode(o: TransactionCompletionResponse): codec.TransactionCompletionResponse =
    codec.TransactionCompletionResponse(
      toClient = Some(encodeUUID(o.toClient.uuid)),
      fromStore = Some(encode(o.fromStore)),
      queryUuid = Some(encodeUUID(o.queryUUID)),
      isComplete = o.isComplete
    )

  def decode(m: codec.TransactionCompletionResponse): TransactionCompletionResponse =
    val fromStore = decode(m.fromStore.get)
    val toClient = ClientId(decodeUUID(m.toClient.get))
    val queryUuid = decodeUUID(m.queryUuid.get)
    val isComplete = m.isComplete
    TransactionCompletionResponse(toClient, fromStore, queryUuid, isComplete)


  def encode(o: TransactionResolved): codec.TransactionResolved =
    codec.TransactionResolved(
      toClient = Some(encodeUUID(o.toClient.uuid)),
      fromStore = Some(encode(o.fromStore)),
      transactionUuid = Some(encodeUUID(o.transactionId.uuid)),
      committed = o.committed
    )

  def decode(m: codec.TransactionResolved): TransactionResolved =
    val fromStore = decode(m.fromStore.get)
    val toClient = ClientId(decodeUUID(m.toClient.get))
    val transactionId = TransactionId(decodeUUID(m.transactionUuid.get))
    val committed = m.committed
    TransactionResolved(toClient, fromStore, transactionId, committed)


  def encode(o: TransactionFinalized): codec.TransactionFinalized =
    codec.TransactionFinalized(
      toClient = Some(encodeUUID(o.toClient.uuid)),
      fromStore = Some(encode(o.fromStore)),
      transactionUuid = Some(encodeUUID(o.transactionId.uuid)),
      committed = o.committed
    )

  def decode(m: codec.TransactionFinalized): TransactionFinalized =
    val fromStore = decode(m.fromStore.get)
    val toClient = ClientId(decodeUUID(m.toClient.get))
    val transactionId = TransactionId(decodeUUID(m.transactionUuid.get))
    val committed = m.committed
    TransactionFinalized(toClient, fromStore, transactionId, committed)


  def encode(o: HostHeartbeat): codec.HostHeartbeat =
    codec.HostHeartbeat(fromHostId = Some(encodeUUID(o.hostId.uuid)))

  def decode(m: codec.HostHeartbeat): HostHeartbeat =
    HostHeartbeat(HostId(decodeUUID(m.fromHostId.get)))


  def encode(o: StartStoreTransfer): codec.StartStoreTransfer =
    codec.StartStoreTransfer(
      toHost = Some(encodeUUID(o.toHost.uuid)),
      fromClient = Some(encodeUUID(o.fromClient.uuid)),
      fromDevice = Some(encode(o.fromDevice)),
      storeId = Some(encode(o.storeId)),
      timestamp = o.timestamp.asLong,
      transferUUID = Some(encodeUUID(o.transferUUID))
    )

  def decode(m: codec.StartStoreTransfer): StartStoreTransfer =
    val toHost = HostId(decodeUUID(m.toHost.get))
    val fromClient = ClientId(decodeUUID(m.fromClient.get))
    val fromDevice = decode(m.fromDevice.get)
    val storeId = decode(m.storeId.get)
    val timestamp = HLCTimestamp(m.timestamp)
    val transferUUID = decodeUUID(m.transferUUID.get)
    StartStoreTransfer(toHost, fromClient, fromDevice, storeId, timestamp, transferUUID)


  def encode(o: StoreTransferData): codec.StoreTransferData =
    codec.StoreTransferData(
      toHost = Some(encodeUUID(o.toHost.uuid)),
      fromClient = Some(encodeUUID(o.fromClient.uuid)),
      transferUUID = Some(encodeUUID(o.transferUUID)),
      data = ByteString.copyFrom(o.data.asReadOnlyBuffer())
    )

  def decode(m: codec.StoreTransferData): StoreTransferData =
    val toHost = HostId(decodeUUID(m.toHost.get))
    val fromClient = ClientId(decodeUUID(m.fromClient.get))
    val transferUUID = decodeUUID(m.transferUUID.get)
    val data = DataBuffer(m.data.toByteArray)
    StoreTransferData(toHost, fromClient, transferUUID, data)


  def encode(o: CheckStorageDevice): codec.CheckStorageDevice =
    codec.CheckStorageDevice(
      toHost = Some(encodeUUID(o.toHost.uuid)),
      fromClient = Some(encodeUUID(o.fromClient.uuid)),
      deviceId = Some(encode(o.deviceId))
    )

  def decode(m: codec.CheckStorageDevice): CheckStorageDevice =
    val toHost = HostId(decodeUUID(m.toHost.get))
    val fromClient = ClientId(decodeUUID(m.fromClient.get))
    val deviceId = decode(m.deviceId.get)
    CheckStorageDevice(toHost, fromClient, deviceId)


  // ----------------------- Non Network Messages -----------------------

  def encode(o: ObjectUpdate): codec.ObjectUpdate =
    codec.ObjectUpdate(
      objectId = Some(encodeUUID(o.objectId.uuid)),
      data = ByteString.copyFrom(o.data.asReadOnlyBuffer())
    )

  def decode(m: codec.ObjectUpdate): ObjectUpdate =
    val oid = ObjectId(decodeUUID(m.objectId.get))
    val objectData = DataBuffer(m.data.toByteArray)
    ObjectUpdate(oid, objectData)


  def encode(o: PersistentState): codec.PersistentState =
    codec.PersistentState(
      promised = o.promised.map(encode),
      acceptedProposalId = o.accepted.map((pid, _) => encode(pid)),
      acceptedValue = o.accepted.map((_, value) => value).getOrElse(false)
    )

  def decode(m: codec.PersistentState): PersistentState =
    val promised = m.promised.map(decode)
    val accepted = m.acceptedProposalId.map(pid => (decode(pid), m.acceptedValue))
    PersistentState(promised, accepted)


  def encode(o: TransactionRecoveryState): codec.TransactionRecoveryState =
    codec.TransactionRecoveryState(
      storeId = Some(encode(o.storeId)),
      serializedTxd = ByteString.copyFrom(o.serializedTxd.asReadOnlyBuffer()),
      objectUpdates = o.objectUpdates.map(encode),
      disposition = encodeTransactionDisposition(o.disposition),
      status = encodeTransactionStatus(o.status),
      paxosAcceptorState = Some(encode(o.paxosAcceptorState))
    )

  def decode(m: codec.TransactionRecoveryState): TransactionRecoveryState =
    val storeId = decode(m.storeId.get)
    val serializedTxd = DataBuffer(m.serializedTxd.toByteArray)
    val objectUpdates = m.objectUpdates.map(decode).toList
    val disposition = decodeTransactionDisposition(m.disposition)
    val status = decodeTransactionStatus(m.status)
    val paxosAcceptorState = decode(m.paxosAcceptorState.get)
    TransactionRecoveryState(storeId, serializedTxd, objectUpdates, disposition, status, paxosAcceptorState)


  def encode(o: StoragePoolState.StoreEntry): codec.PoolStoreEntry =
    codec.PoolStoreEntry(
      hostId = Some(encodeUUID(o.hostId.uuid)),
      storageDeviceId = Some(encode(o.storageDeviceId))
    )

  def decode(m: codec.PoolStoreEntry): StoragePoolState.StoreEntry =
    val hostId = HostId(decodeUUID(m.hostId.get))
    val storageDeviceId = decode(m.storageDeviceId.get)
    StoragePoolState.StoreEntry(hostId, storageDeviceId)


  def encodeBackendConfig(o: BackendConfig): codec.BackendConfig = o match
    case RocksDBConfig() => codec.BackendConfig.BACKEND_CONFIG_ROCKS_DB

  def decodeBackendConfig(m: codec.BackendConfig): BackendConfig = m match
    case codec.BackendConfig.BACKEND_CONFIG_ROCKS_DB => RocksDBConfig()
    case codec.BackendConfig.Unrecognized(v) => throw new EncodingError(f"Invalid Backend Config: $v")


  def encode(o: StoragePoolState): codec.StoragePoolState =
    codec.StoragePoolState(
      poolId = Some(encodeUUID(o.poolId.uuid)),
      name = o.name,
      ida = Some(encode(o.ida)),
      maxObjectSize = o.maxObjectSize.getOrElse(0),
      stores = o.stores.map(encode).toSeq,
      backendConfig = encodeBackendConfig(o.backendConfig),
      currentUsage = o.currentUsage,
      maximumStoreSize = o.maximumStoreSize,
      allocationGroups = o.allocationGroups.map(encodeUUID)
    )

  def decode(m: codec.StoragePoolState): StoragePoolState =
    val poolId = PoolId(decodeUUID(m.poolId.get))
    val name = m.name
    val ida = decode(m.ida.get)
    val maxObjectSize = if m.maxObjectSize == 0 then None else Some(m.maxObjectSize)
    val stores = m.stores.map(decode).toArray
    val backendConfig = decodeBackendConfig(m.backendConfig)
    val currentUsage = m.currentUsage
    val maximumStoreSize = m.maximumStoreSize
    val allocationGroups = m.allocationGroups.map(decodeUUID).toList

    StoragePoolState(poolId, name, ida, maxObjectSize, stores, backendConfig,
      currentUsage, maximumStoreSize, allocationGroups)


  def encode(o: HostState): codec.HostState =
    codec.HostState(
      hostId = Some(encodeUUID(o.hostId.uuid)),
      name = o.name,
      address = o.address,
      dataPort = o.dataPort,
      cncPort = o.cncPort,
      storeTransferPort = o.storeTransferPort,
      storageDevices = o.storageDevices.map(encode).toSeq
    )

  def decode(m: codec.HostState): HostState =
    val hostId = HostId(decodeUUID(m.hostId.get))
    val name = m.name
    val address = m.address
    val dataPort = m.dataPort
    val cncPort = m.cncPort
    val storeTransferPort = m.storeTransferPort
    val storageDevices = m.storageDevices.map(decode).toSet

    HostState(hostId, name, address, dataPort, cncPort, storeTransferPort, storageDevices)

  // CnC Messages -----------------------------------------------------------------

  def encode(o: NewStore): codec.NewStore =
    codec.NewStore(
      storeId = Some(encode(o.storeId)),
      backendConfig = encodeBackendConfig(o.backendType)
    )

  def decode(m: codec.NewStore): NewStore =
    val storeId = decode(m.storeId.get)
    val backendType = decodeBackendConfig(m.backendConfig)
    NewStore(storeId, backendType)


  def encode(o: ShutdownStore): codec.ShutdownStore =
    codec.ShutdownStore(storeId = Some(encode(o.storeId)))

  def decode(m: codec.ShutdownStore): ShutdownStore =
    ShutdownStore(decode(m.storeId.get))


  def encode(o: TransferStore): codec.TransferStore =
    codec.TransferStore(
      storeId = Some(encode(o.storeId)),
      toHost = Some(encodeUUID(o.toHost.uuid))
    )

  def decode(m: codec.TransferStore): TransferStore =
    val storeId = decode(m.storeId.get)
    val toHost = HostId(decodeUUID(m.toHost.get))
    TransferStore(storeId, toHost)


  def encode(o: cnc.Error): codec.CnCError =
    codec.CnCError(message = o.message)

  def decode(m: codec.CnCError): cnc.Error =
    cnc.Error(m.message)

  // Storage Device Messages -----------------------------------------------------------------

  def encode(o: StorageDeviceId): codec.StorageDeviceId =
    codec.StorageDeviceId(deviceUuid = Some(encodeUUID(o.uuid)))

  def decode(m: codec.StorageDeviceId): StorageDeviceId =
    StorageDeviceId(decodeUUID(m.deviceUuid.get))


  def encodeStorageDeviceStoreStatus(o: StorageDeviceState.StoreStatus): codec.StorageDeviceStoreStatus = o match
    case StorageDeviceState.StoreStatus.Initializing => codec.StorageDeviceStoreStatus.STORAGE_DEVICE_STORE_STATUS_INITIALIZING
    case StorageDeviceState.StoreStatus.Active => codec.StorageDeviceStoreStatus.STORAGE_DEVICE_STORE_STATUS_ACTIVE
    case StorageDeviceState.StoreStatus.TransferringIn => codec.StorageDeviceStoreStatus.STORAGE_DEVICE_STORE_STATUS_TRANSFERRING_IN
    case StorageDeviceState.StoreStatus.TransferringOut => codec.StorageDeviceStoreStatus.STORAGE_DEVICE_STORE_STATUS_TRANSFERRING_OUT
    case StorageDeviceState.StoreStatus.Rebuilding => codec.StorageDeviceStoreStatus.STORAGE_DEVICE_STORE_STATUS_REBUILDING

  def decodeStorageDeviceStoreStatus(m: codec.StorageDeviceStoreStatus): StorageDeviceState.StoreStatus = m match
    case codec.StorageDeviceStoreStatus.STORAGE_DEVICE_STORE_STATUS_INITIALIZING => StorageDeviceState.StoreStatus.Initializing
    case codec.StorageDeviceStoreStatus.STORAGE_DEVICE_STORE_STATUS_ACTIVE => StorageDeviceState.StoreStatus.Active
    case codec.StorageDeviceStoreStatus.STORAGE_DEVICE_STORE_STATUS_TRANSFERRING_IN => StorageDeviceState.StoreStatus.TransferringIn
    case codec.StorageDeviceStoreStatus.STORAGE_DEVICE_STORE_STATUS_TRANSFERRING_OUT => StorageDeviceState.StoreStatus.TransferringOut
    case codec.StorageDeviceStoreStatus.STORAGE_DEVICE_STORE_STATUS_REBUILDING => StorageDeviceState.StoreStatus.Rebuilding
    case codec.StorageDeviceStoreStatus.Unrecognized(v) => throw new EncodingError(f"Invalid StorageDeviceStoreStatus: $v")


  def encode(o: StorageDeviceState.StoreEntry): codec.StorageDeviceStoreEntry =
    codec.StorageDeviceStoreEntry(
      status = encodeStorageDeviceStoreStatus(o.status),
      transferDevice = o.transferDevice.map(encode)
    )

  def decode(m: codec.StorageDeviceStoreEntry): StorageDeviceState.StoreEntry =
    val status = decodeStorageDeviceStoreStatus(m.status)
    val transferDevice = m.transferDevice.map(decode)
    StorageDeviceState.StoreEntry(status, transferDevice)


  def encode(o: StorageDeviceState): codec.StorageDeviceState =
    codec.StorageDeviceState(
      storageDeviceId = Some(encode(o.storageDeviceId)),
      hostId = Some(encodeUUID(o.hostId.uuid)),
      stores = o.stores.map { (storeId, storeEntry) =>
        codec.StorageDeviceStoreKeyValue(
          storeId = ByteString.copyFrom(storeId.toBytes),
          entry = Some(encode(storeEntry))
        )
      }.toSeq
    )

  def decode(m: codec.StorageDeviceState): StorageDeviceState =
    val storageDeviceId = decode(m.storageDeviceId.get)
    val hostId = HostId(decodeUUID(m.hostId.get))
    val stores = m.stores.map { keyValue =>
      val storeId = StoreId(keyValue.storeId.toByteArray)
      val entry = decode(keyValue.entry.get)
      storeId -> entry
    }.toMap

    new StorageDeviceState(storageDeviceId, hostId, stores)

  def encodeSteppedDurableTaskState(step: Int, state: Map[String, Array[Byte]]): Array[Byte] =
    codec.SteppedDurableTaskState(
      step = step,
      entries = state.map { (key, value) =>
        codec.SteppedDurableTaskEntry(
          key = key,
          value = ByteString.copyFrom(value)
        )
      }.toSeq
    ).toByteArray

  def decodeSteppedDurableTaskState(data: Array[Byte]): (Int, Map[String, Array[Byte]]) =
    val m = codec.SteppedDurableTaskState.parseFrom(data)
    val step = m.step
    val state = m.entries.map { entry =>
      entry.key -> entry.value.toByteArray
    }.toMap
    (step, state)
