package org.aspen_ddp.aspen.common.network

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.aspen_ddp.aspen.common.{DataBuffer, HLCTimestamp}
import org.aspen_ddp.aspen.common.ida.{IDA, ReedSolomon, Replication}
import org.aspen_ddp.aspen.common.allocation_group.AllocationGroupId
import org.aspen_ddp.aspen.common.metadata.{AllocationGroupState, HostId, HostState, StorageDeviceId, StorageDeviceState, StoragePoolState}
import org.aspen_ddp.aspen.common.objects.*
import org.aspen_ddp.aspen.common.paxos.{PersistentState, ProposalId}
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.transaction.*
import org.aspen_ddp.aspen.server.cnc
import org.aspen_ddp.aspen.server.crl.TransactionRecoveryState
import org.aspen_ddp.aspen.server.store.backend.RocksDBConfig

import java.util.UUID

class CodecRoundTripSuite extends AnyFunSuite with Matchers:

  private def uuid(n: Int): UUID = new UUID(n.toLong << 32 | n.toLong, n.toLong)
  private def storeId(n: Int): StoreId = StoreId(PoolId(uuid(n)), n.toByte)
  private def dataPtr(n: Int): DataObjectPointer = DataObjectPointer(ObjectId(uuid(n)), PoolId(uuid(n + 100)), Array[Byte](n.toByte))
  private def kvPtr(n: Int): KeyValueObjectPointer = KeyValueObjectPointer(ObjectId(uuid(n)), PoolId(uuid(n + 100)), Array[Byte](n.toByte))
  private def rev(n: Int): ObjectRevision = ObjectRevision(TransactionId(uuid(n)))

  test("UUID round-trip"):
    val original = uuid(42)
    val decoded = Codec.decodeUUID(Codec.encodeUUID(original))
    decoded shouldBe original

    val zero = new UUID(0, 0)
    Codec.decodeUUID(Codec.encodeUUID(zero)) shouldBe zero

    val max = new UUID(Long.MaxValue, Long.MinValue)
    Codec.decodeUUID(Codec.encodeUUID(max)) shouldBe max

  test("Replication round-trip"):
    val original = Replication(3, 2)
    val decoded = Codec.decode(Codec.encode(original))
    decoded shouldBe original

  test("ReedSolomon round-trip"):
    val original = ReedSolomon(5, 3, 4)
    val decoded = Codec.decode(Codec.encode(original))
    decoded shouldBe original

  test("IDA round-trip"):
    val rep: IDA = Replication(3, 2)
    Codec.decode(Codec.encode(rep)) shouldBe rep

    val rs: IDA = ReedSolomon(5, 3, 4)
    Codec.decode(Codec.encode(rs)) shouldBe rs

  test("ObjectRevision round-trip"):
    val original = rev(42)
    val decoded = Codec.decode(Codec.encode(original))
    decoded.lastUpdateTxUUID shouldBe original.lastUpdateTxUUID

  test("ObjectRefcount round-trip"):
    val original = ObjectRefcount(5, 10)
    val decoded = Codec.decode(Codec.encode(original))
    decoded shouldBe original

  test("ObjectPointer round-trip"):
    val dp = dataPtr(1)
    val decodedDp = Codec.decode(Codec.encode(dp))
    decodedDp shouldBe a[DataObjectPointer]
    decodedDp.id shouldBe dp.id
    decodedDp.poolId shouldBe dp.poolId
    decodedDp.storePointer shouldBe dp.storePointer

    val kp = kvPtr(2)
    val decodedKp = Codec.decode(Codec.encode(kp))
    decodedKp shouldBe a[KeyValueObjectPointer]
    decodedKp.id shouldBe kp.id
    decodedKp.poolId shouldBe kp.poolId
    decodedKp.storePointer shouldBe kp.storePointer

  test("ProposalId round-trip"):
    val original = ProposalId(42, 5.toByte)
    val decoded = Codec.decode(Codec.encode(original))
    decoded shouldBe original

  test("StoreId round-trip"):
    val original = storeId(10)
    val decoded = Codec.decode(Codec.encode(original))
    decoded shouldBe original

  test("DataUpdate round-trip"):
    val original = DataUpdate(dataPtr(1), rev(2), DataUpdateOperation.Overwrite)
    val decoded = Codec.decode(Codec.encode(original))
    decoded.objectPointer.id shouldBe original.objectPointer.id
    decoded.requiredRevision.lastUpdateTxUUID shouldBe original.requiredRevision.lastUpdateTxUUID
    decoded.operation shouldBe original.operation

    val appendCase = DataUpdate(dataPtr(3), rev(4), DataUpdateOperation.Append)
    val decodedAppend = Codec.decode(Codec.encode(appendCase))
    decodedAppend.operation shouldBe DataUpdateOperation.Append

  test("RefcountUpdate round-trip"):
    val original = RefcountUpdate(dataPtr(1), ObjectRefcount(1, 5), ObjectRefcount(2, 6))
    val decoded = Codec.decode(Codec.encode(original))
    decoded.objectPointer.id shouldBe original.objectPointer.id
    decoded.requiredRefcount shouldBe original.requiredRefcount
    decoded.newRefcount shouldBe original.newRefcount

  test("VersionBump round-trip"):
    val original = VersionBump(dataPtr(1), rev(2))
    val decoded = Codec.decode(Codec.encode(original))
    decoded.objectPointer.id shouldBe original.objectPointer.id
    decoded.requiredRevision.lastUpdateTxUUID shouldBe original.requiredRevision.lastUpdateTxUUID

  test("RevisionLock round-trip"):
    val original = RevisionLock(dataPtr(1), rev(2))
    val decoded = Codec.decode(Codec.encode(original))
    decoded.objectPointer.id shouldBe original.objectPointer.id
    decoded.requiredRevision.lastUpdateTxUUID shouldBe original.requiredRevision.lastUpdateTxUUID

  test("KeyRequirement round-trip"):
    val key = Key(Array[Byte](1, 2, 3))
    val ts = HLCTimestamp(1000L)

    val exists = KeyValueUpdate.Exists(key)
    Codec.decode(Codec.encode(exists)).asInstanceOf[KeyValueUpdate.Exists].key.bytes shouldBe key.bytes

    val mayExist = KeyValueUpdate.MayExist(key)
    Codec.decode(Codec.encode(mayExist)) shouldBe a[KeyValueUpdate.MayExist]

    val doesNotExist = KeyValueUpdate.DoesNotExist(key)
    Codec.decode(Codec.encode(doesNotExist)) shouldBe a[KeyValueUpdate.DoesNotExist]

    val tsEquals = KeyValueUpdate.TimestampEquals(key, ts)
    val decodedTsEq = Codec.decode(Codec.encode(tsEquals)).asInstanceOf[KeyValueUpdate.TimestampEquals]
    decodedTsEq.timestamp shouldBe ts

    val tsLt = KeyValueUpdate.TimestampLessThan(key, ts)
    Codec.decode(Codec.encode(tsLt)) shouldBe a[KeyValueUpdate.TimestampLessThan]

    val tsGt = KeyValueUpdate.TimestampGreaterThan(key, ts)
    Codec.decode(Codec.encode(tsGt)) shouldBe a[KeyValueUpdate.TimestampGreaterThan]

    val keyRev = KeyValueUpdate.KeyRevision(key, rev(5))
    val decodedKeyRev = Codec.decode(Codec.encode(keyRev)).asInstanceOf[KeyValueUpdate.KeyRevision]
    decodedKeyRev.revision.lastUpdateTxUUID shouldBe rev(5).lastUpdateTxUUID

    val keyObjRev = KeyValueUpdate.KeyObjectRevision(key, rev(6))
    Codec.decode(Codec.encode(keyObjRev)) shouldBe a[KeyValueUpdate.KeyObjectRevision]

    val withinRange = KeyValueUpdate.WithinRange(key, LexicalKeyOrdering)
    val decodedRange = Codec.decode(Codec.encode(withinRange)).asInstanceOf[KeyValueUpdate.WithinRange]
    decodedRange.ordering shouldBe LexicalKeyOrdering

  test("KeyRevision round-trip"):
    val original = KeyValueUpdate.KeyRevision(Key(Array[Byte](4, 5, 6)), rev(7))
    val decoded = Codec.decode(Codec.encode(original))
    decoded.key.bytes shouldBe original.key.bytes
    decoded.revision.lastUpdateTxUUID shouldBe original.revision.lastUpdateTxUUID

  test("KeyValueUpdate round-trip"):
    val full = KeyValueUpdate(
      kvPtr(1),
      Some(rev(2)),
      Some(KeyValueUpdate.FullContentLock(List(
        KeyValueUpdate.KeyRevision(Key(Array[Byte](1, 2)), rev(3))
      ))),
      List(KeyValueUpdate.Exists(Key(Array[Byte](4, 5))))
    )
    val decodedFull = Codec.decode(Codec.encode(full))
    decodedFull.requiredRevision.isDefined shouldBe true
    decodedFull.contentLock.isDefined shouldBe true
    decodedFull.requirements.size shouldBe 1

    val minimal = KeyValueUpdate(kvPtr(10), None, None, Nil)
    val decodedMin = Codec.decode(Codec.encode(minimal))
    decodedMin.requiredRevision shouldBe None
    decodedMin.contentLock shouldBe None
    decodedMin.requirements shouldBe empty

  test("LocalTimeRequirement round-trip"):
    val lt = LocalTimeRequirement(HLCTimestamp(1000L), LocalTimeRequirement.Requirement.LessThan)
    Codec.decode(Codec.encode(lt)).tsRequirement shouldBe LocalTimeRequirement.Requirement.LessThan

    val gt = LocalTimeRequirement(HLCTimestamp(2000L), LocalTimeRequirement.Requirement.GreaterThan)
    Codec.decode(Codec.encode(gt)).tsRequirement shouldBe LocalTimeRequirement.Requirement.GreaterThan

    val eq = LocalTimeRequirement(HLCTimestamp(3000L), LocalTimeRequirement.Requirement.Equals)
    val decodedEq = Codec.decode(Codec.encode(eq))
    decodedEq.timestamp shouldBe HLCTimestamp(3000L)
    decodedEq.tsRequirement shouldBe LocalTimeRequirement.Requirement.Equals

  test("TransactionRequirement round-trip"):
    val du: TransactionRequirement = DataUpdate(dataPtr(1), rev(2), DataUpdateOperation.Append)
    Codec.decode(Codec.encode(du)) shouldBe a[DataUpdate]

    val ru: TransactionRequirement = RefcountUpdate(dataPtr(3), ObjectRefcount(1, 5), ObjectRefcount(2, 6))
    Codec.decode(Codec.encode(ru)) shouldBe a[RefcountUpdate]

    val vb: TransactionRequirement = VersionBump(dataPtr(4), rev(5))
    Codec.decode(Codec.encode(vb)) shouldBe a[VersionBump]

    val rl: TransactionRequirement = RevisionLock(dataPtr(6), rev(7))
    Codec.decode(Codec.encode(rl)) shouldBe a[RevisionLock]

    val kvu: TransactionRequirement = KeyValueUpdate(kvPtr(8), None, None, Nil)
    Codec.decode(Codec.encode(kvu)) shouldBe a[KeyValueUpdate]

    val ltr: TransactionRequirement = LocalTimeRequirement(HLCTimestamp(1000L), LocalTimeRequirement.Requirement.Equals)
    Codec.decode(Codec.encode(ltr)) shouldBe a[LocalTimeRequirement]

  test("SerializedFinalizationAction round-trip"):
    val original = SerializedFinalizationAction(FinalizationActionId(uuid(1)), Array[Byte](10, 20, 30))
    val decoded = Codec.decode(Codec.encode(original))
    decoded.typeId shouldBe original.typeId
    decoded.data shouldBe original.data

  test("TransactionDescription round-trip"):
    val poolId = PoolId(uuid(3))
    val ida: IDA = Replication(3, 2)
    val full = TransactionDescription(
      transactionId = TransactionId(uuid(1)),
      startTimestamp = HLCTimestamp(1000L),
      primaryObject = dataPtr(2),
      designatedLeaderUID = 5.toByte,
      requirements = List(DataUpdate(dataPtr(4), rev(5), DataUpdateOperation.Overwrite)),
      finalizationActions = List(SerializedFinalizationAction(FinalizationActionId(uuid(6)), Array[Byte](1, 2))),
      originatingClient = Some(ClientId(uuid(7))),
      notifyOnResolution = List(storeId(8)),
      notes = List("test note"),
      primaryObjectIDA = ida,
      poolIDAMap = Map(dataPtr(2).poolId -> ida),
      allocatingObjects = Set(ObjectId(uuid(9)))
    )
    val decoded = Codec.decode(Codec.encode(full))
    decoded.transactionId shouldBe full.transactionId
    decoded.startTimestamp shouldBe full.startTimestamp
    decoded.designatedLeaderUID shouldBe full.designatedLeaderUID
    decoded.requirements.size shouldBe 1
    decoded.finalizationActions.size shouldBe 1
    decoded.originatingClient shouldBe full.originatingClient
    decoded.notifyOnResolution.size shouldBe 1
    decoded.notes shouldBe List("test note")
    decoded.allocatingObjects.size shouldBe 1

    val minimal = TransactionDescription(
      TransactionId(uuid(20)), HLCTimestamp(2000L), dataPtr(21), 1.toByte,
      Nil, Nil, None, Nil, Nil, ida, Map(dataPtr(21).poolId -> ida), Set.empty
    )
    val decodedMin = Codec.decode(Codec.encode(minimal))
    decodedMin.originatingClient shouldBe None
    decodedMin.requirements shouldBe empty
    decodedMin.allocatingObjects shouldBe empty

  test("TxPrepare round-trip"):
    val ida: IDA = Replication(3, 2)
    val txd = TransactionDescription(
      TransactionId(uuid(1)), HLCTimestamp(1000L), dataPtr(2), 1.toByte,
      Nil, Nil, None, Nil, Nil, ida, Map(dataPtr(2).poolId -> ida)
    )
    val original = TxPrepare(storeId(10), storeId(11), txd, ProposalId(1, 1.toByte), Nil, Nil)
    val decoded = Codec.decode(Codec.encode(original), Nil, Nil)
    decoded.to shouldBe original.to
    decoded.from shouldBe original.from
    decoded.proposalId shouldBe original.proposalId

  test("TxPrepareResponse round-trip"):
    val nack = TxPrepareResponse(
      storeId(1), storeId(2), TransactionId(uuid(3)),
      Left(TxPrepareResponse.Nack(ProposalId(5, 2.toByte))),
      ProposalId(3, 1.toByte), TransactionDisposition.VoteAbort,
      List(TransactionId(uuid(10)), TransactionId(uuid(11)))
    )
    val decodedNack = Codec.decode(Codec.encode(nack))
    decodedNack.response.isLeft shouldBe true
    decodedNack.response.left.toOption.get.promisedId shouldBe ProposalId(5, 2.toByte)
    decodedNack.collisions.size shouldBe 2
    decodedNack.disposition shouldBe TransactionDisposition.VoteAbort

    val promise = TxPrepareResponse(
      storeId(1), storeId(2), TransactionId(uuid(3)),
      Right(TxPrepareResponse.Promise(Some((ProposalId(4, 1.toByte), true)))),
      ProposalId(3, 1.toByte), TransactionDisposition.VoteCommit, Nil
    )
    val decodedPromise = Codec.decode(Codec.encode(promise))
    decodedPromise.response.isRight shouldBe true
    val promiseValue = decodedPromise.response.toOption.get
    promiseValue.lastAccepted.get._1 shouldBe ProposalId(4, 1.toByte)
    promiseValue.lastAccepted.get._2 shouldBe true

    val promiseNoLast = TxPrepareResponse(
      storeId(1), storeId(2), TransactionId(uuid(3)),
      Right(TxPrepareResponse.Promise(None)),
      ProposalId(3, 1.toByte), TransactionDisposition.Undetermined, Nil
    )
    val decodedNoLast = Codec.decode(Codec.encode(promiseNoLast))
    decodedNoLast.response.toOption.get.lastAccepted shouldBe None

  test("TxAccept round-trip"):
    val original = TxAccept(storeId(1), storeId(2), TransactionId(uuid(3)), ProposalId(4, 1.toByte), true)
    val decoded = Codec.decode(Codec.encode(original))
    decoded.to shouldBe original.to
    decoded.from shouldBe original.from
    decoded.transactionId shouldBe original.transactionId
    decoded.value shouldBe true

  test("TxAcceptResponse round-trip"):
    val nack = TxAcceptResponse(
      storeId(1), storeId(2), TransactionId(uuid(3)),
      ProposalId(4, 1.toByte), Left(TxAcceptResponse.Nack(ProposalId(5, 2.toByte)))
    )
    val decodedNack = Codec.decode(Codec.encode(nack))
    decodedNack.response.isLeft shouldBe true

    val accepted = TxAcceptResponse(
      storeId(1), storeId(2), TransactionId(uuid(3)),
      ProposalId(4, 1.toByte), Right(TxAcceptResponse.Accepted(true))
    )
    val decodedAccepted = Codec.decode(Codec.encode(accepted))
    decodedAccepted.response.toOption.get.value shouldBe true

  test("TxResolved round-trip"):
    val original = TxResolved(storeId(1), storeId(2), TransactionId(uuid(3)), committed = true)
    val decoded = Codec.decode(Codec.encode(original))
    decoded shouldBe original

  test("TxCommitted round-trip"):
    val original = TxCommitted(storeId(1), storeId(2), TransactionId(uuid(3)),
      List(ObjectId(uuid(4)), ObjectId(uuid(5))))
    val decoded = Codec.decode(Codec.encode(original))
    decoded.to shouldBe original.to
    decoded.objectCommitErrors.size shouldBe 2
    decoded.objectCommitErrors.map(_.uuid).toSet shouldBe original.objectCommitErrors.map(_.uuid).toSet

    val noErrors = TxCommitted(storeId(1), storeId(2), TransactionId(uuid(3)), Nil)
    Codec.decode(Codec.encode(noErrors)).objectCommitErrors shouldBe empty

  test("TxFinalized round-trip"):
    val original = TxFinalized(storeId(1), storeId(2), TransactionId(uuid(3)), committed = true)
    val decoded = Codec.decode(Codec.encode(original))
    decoded shouldBe original

  test("TxHeartbeat round-trip"):
    val original = TxHeartbeat(storeId(1), storeId(2), TransactionId(uuid(3)))
    val decoded = Codec.decode(Codec.encode(original))
    decoded shouldBe original

  test("TxStatusRequest round-trip"):
    val original = TxStatusRequest(storeId(1), storeId(2), TransactionId(uuid(3)), uuid(4))
    val decoded = Codec.decode(Codec.encode(original))
    decoded shouldBe original

  test("TxStatusResponse round-trip"):
    val withStatus = TxStatusResponse(
      storeId(1), storeId(2), TransactionId(uuid(3)), uuid(4),
      Some(TxStatusResponse.TxStatus(TransactionStatus.Committed, finalized = true))
    )
    val decoded = Codec.decode(Codec.encode(withStatus))
    decoded.status.isDefined shouldBe true
    decoded.status.get.status shouldBe TransactionStatus.Committed
    decoded.status.get.finalized shouldBe true

    val noStatus = TxStatusResponse(storeId(1), storeId(2), TransactionId(uuid(3)), uuid(4), None)
    Codec.decode(Codec.encode(noStatus)).status shouldBe None

  test("TxUnknownStore round-trip"):
    val original = TxUnknownStore(storeId(1), storeId(2), TransactionId(uuid(3)))
    val decoded = Codec.decode(Codec.encode(original))
    decoded shouldBe original

  test("Read round-trip"):
    val ptr = dataPtr(1)

    val metaOnly = Read(storeId(10), ClientId(uuid(20)), uuid(30), ptr, MetadataOnly())
    val decodedMeta = Codec.decode(Codec.encode(metaOnly))
    decodedMeta.readType shouldBe a[MetadataOnly]
    decodedMeta.toStore shouldBe storeId(10)

    val fullObj = Read(storeId(10), ClientId(uuid(20)), uuid(30), ptr, FullObject())
    Codec.decode(Codec.encode(fullObj)).readType shouldBe a[FullObject]

    val byteRange = Read(storeId(10), ClientId(uuid(20)), uuid(30), ptr, ByteRange(100, 200))
    val decodedBR = Codec.decode(Codec.encode(byteRange)).readType.asInstanceOf[ByteRange]
    decodedBR.offset shouldBe 100
    decodedBR.length shouldBe 200

    val singleKey = Read(storeId(10), ClientId(uuid(20)), uuid(30), kvPtr(1), SingleKey(Key(Array[Byte](1, 2)), LexicalKeyOrdering))
    val decodedSK = Codec.decode(Codec.encode(singleKey)).readType.asInstanceOf[SingleKey]
    decodedSK.key.bytes shouldBe Array[Byte](1, 2)
    decodedSK.ordering shouldBe LexicalKeyOrdering

    val keyRange = Read(storeId(10), ClientId(uuid(20)), uuid(30), kvPtr(1),
      KeyRange(Key(Array[Byte](1)), Key(Array[Byte](9)), IntegerKeyOrdering))
    val decodedKR = Codec.decode(Codec.encode(keyRange)).readType.asInstanceOf[KeyRange]
    decodedKR.minimum.bytes shouldBe Array[Byte](1)
    decodedKR.maximum.bytes shouldBe Array[Byte](9)
    decodedKR.ordering shouldBe IntegerKeyOrdering

  test("ReadResponse round-trip"):
    val errorResp = ReadResponse(ClientId(uuid(1)), storeId(2), uuid(3), HLCTimestamp(1000L),
      Left(ReadError.ObjectNotFound))
    val decodedErr = Codec.decode(Codec.encode(errorResp))
    decodedErr.result.isLeft shouldBe true
    decodedErr.result.left.toOption.get shouldBe ReadError.ObjectNotFound

    val successResp = ReadResponse(ClientId(uuid(1)), storeId(2), uuid(3), HLCTimestamp(1000L),
      Right(ReadResponse.CurrentState(
        rev(4), ObjectRefcount(1, 5), HLCTimestamp(2000L), 1024,
        Some(DataBuffer(Array[Byte](10, 20, 30))),
        Set(TransactionId(uuid(5)), TransactionId(uuid(6)))
      )))
    val decodedSuccess = Codec.decode(Codec.encode(successResp))
    decodedSuccess.result.isRight shouldBe true
    val state = decodedSuccess.result.toOption.get
    state.sizeOnStore shouldBe 1024
    state.objectData.isDefined shouldBe true
    state.lockedWriteTransactions.size shouldBe 2

    val noDataResp = ReadResponse(ClientId(uuid(1)), storeId(2), uuid(3), HLCTimestamp(1000L),
      Right(ReadResponse.CurrentState(rev(4), ObjectRefcount(1, 5), HLCTimestamp(2000L), 0, None, Set.empty)))
    val decodedNoData = Codec.decode(Codec.encode(noDataResp))
    decodedNoData.result.toOption.get.objectData shouldBe None
    decodedNoData.result.toOption.get.lockedWriteTransactions shouldBe empty

  test("OpportunisticRebuild round-trip"):
    val original = OpportunisticRebuild(
      storeId(1), ClientId(uuid(2)), dataPtr(3), rev(4),
      ObjectRefcount(1, 5), HLCTimestamp(1000L), DataBuffer(Array[Byte](10, 20))
    )
    val decoded = Codec.decode(Codec.encode(original))
    decoded.toStore shouldBe original.toStore
    decoded.fromClient shouldBe original.fromClient
    decoded.pointer.id shouldBe original.pointer.id
    decoded.timestamp shouldBe original.timestamp

  test("TransactionCompletionQuery round-trip"):
    val original = TransactionCompletionQuery(storeId(1), ClientId(uuid(2)), uuid(3), TransactionId(uuid(4)))
    val decoded = Codec.decode(Codec.encode(original))
    decoded shouldBe original

  test("TransactionCompletionResponse round-trip"):
    val original = TransactionCompletionResponse(ClientId(uuid(1)), storeId(2), uuid(3), isComplete = true)
    val decoded = Codec.decode(Codec.encode(original))
    decoded shouldBe original

  test("TransactionResolved round-trip"):
    val original = TransactionResolved(ClientId(uuid(1)), storeId(2), TransactionId(uuid(3)), committed = true)
    val decoded = Codec.decode(Codec.encode(original))
    decoded shouldBe original

  test("TransactionFinalized round-trip"):
    val original = TransactionFinalized(ClientId(uuid(1)), storeId(2), TransactionId(uuid(3)), committed = false)
    val decoded = Codec.decode(Codec.encode(original))
    decoded shouldBe original

  test("HostHeartbeat round-trip"):
    val original = HostHeartbeat(HostId(uuid(1)))
    val decoded = Codec.decode(Codec.encode(original))
    decoded shouldBe original

  test("StartStoreTransfer round-trip"):
    val original = StartStoreTransfer(
      HostId(uuid(1)), ClientId(uuid(2)), StorageDeviceId(uuid(3)),
      storeId(4), HLCTimestamp(1000L), uuid(5)
    )
    val decoded = Codec.decode(Codec.encode(original))
    decoded.toHost shouldBe original.toHost
    decoded.fromClient shouldBe original.fromClient
    decoded.fromDevice shouldBe original.fromDevice
    decoded.storeId shouldBe original.storeId
    decoded.timestamp shouldBe original.timestamp
    decoded.transferUUID shouldBe original.transferUUID

  test("StoreTransferData round-trip"):
    val original = StoreTransferData(HostId(uuid(1)), ClientId(uuid(2)), uuid(3), DataBuffer(Array[Byte](1, 2, 3)))
    val decoded = Codec.decode(Codec.encode(original))
    decoded.toHost shouldBe original.toHost
    decoded.fromClient shouldBe original.fromClient
    decoded.transferUUID shouldBe original.transferUUID

  test("CheckStorageDevice round-trip"):
    val original = CheckStorageDevice(HostId(uuid(1)), ClientId(uuid(2)), StorageDeviceId(uuid(3)))
    val decoded = Codec.decode(Codec.encode(original))
    decoded shouldBe original

  test("ObjectUpdate round-trip"):
    val original = ObjectUpdate(ObjectId(uuid(1)), DataBuffer(Array[Byte](10, 20, 30)))
    val decoded = Codec.decode(Codec.encode(original))
    decoded.objectId shouldBe original.objectId

  test("PersistentState round-trip"):
    val full = PersistentState(Some(ProposalId(1, 2.toByte)), Some((ProposalId(3, 4.toByte), true)))
    val decodedFull = Codec.decode(Codec.encode(full))
    decodedFull.promised.get shouldBe ProposalId(1, 2.toByte)
    decodedFull.accepted.get._1 shouldBe ProposalId(3, 4.toByte)
    decodedFull.accepted.get._2 shouldBe true

    val empty = PersistentState(None, None)
    val decodedEmpty = Codec.decode(Codec.encode(empty))
    decodedEmpty.promised shouldBe None
    decodedEmpty.accepted shouldBe None

  test("TransactionRecoveryState round-trip"):
    val ida: IDA = Replication(3, 2)
    val txd = TransactionDescription(
      TransactionId(uuid(1)), HLCTimestamp(1000L), dataPtr(2), 1.toByte,
      Nil, Nil, None, Nil, Nil, ida, Map(dataPtr(2).poolId -> ida)
    )
    val serializedTxd = DataBuffer(Codec.encode(txd).toByteArray)
    val ou = ObjectUpdate(ObjectId(uuid(3)), DataBuffer(Array[Byte](1, 2, 3)))
    val pax = PersistentState(Some(ProposalId(1, 1.toByte)), None)
    val original = TransactionRecoveryState(
      storeId(10), serializedTxd, List(ou),
      TransactionDisposition.VoteCommit, TransactionStatus.Committed, pax
    )
    val decoded = Codec.decode(Codec.encode(original))
    decoded.storeId shouldBe original.storeId
    decoded.objectUpdates.size shouldBe 1
    decoded.disposition shouldBe TransactionDisposition.VoteCommit
    decoded.status shouldBe TransactionStatus.Committed
    decoded.paxosAcceptorState.promised.get shouldBe ProposalId(1, 1.toByte)

  test("StoragePoolState.StoreEntry round-trip"):
    val original = StoragePoolState.StoreEntry(HostId(uuid(1)), StorageDeviceId(uuid(2)))
    val decoded = Codec.decode(Codec.encode(original))
    decoded shouldBe original

  test("StoragePoolState round-trip"):
    val original = StoragePoolState(
      PoolId(uuid(1)), "test-pool", Replication(3, 2), Some(1024 * 1024),
      Array(
        StoragePoolState.StoreEntry(HostId(uuid(2)), StorageDeviceId(uuid(3))),
        StoragePoolState.StoreEntry(HostId(uuid(4)), StorageDeviceId(uuid(5)))
      ),
      RocksDBConfig(),
      currentUsage = 500L,
      maximumStoreSize = 10000L,
      allocationGroups = List(uuid(6), uuid(7))
    )
    val decoded = Codec.decode(Codec.encode(original))
    decoded.poolId shouldBe original.poolId
    decoded.name shouldBe "test-pool"
    decoded.ida shouldBe Replication(3, 2)
    decoded.maxObjectSize shouldBe Some(1024 * 1024)
    decoded.stores.length shouldBe 2
    decoded.currentUsage shouldBe 500L
    decoded.maximumStoreSize shouldBe 10000L
    decoded.allocationGroups.size shouldBe 2

    val noMaxSize = StoragePoolState(
      PoolId(uuid(10)), "pool2", ReedSolomon(5, 3, 4), None,
      Array.empty, RocksDBConfig()
    )
    val decodedNoMax = Codec.decode(Codec.encode(noMaxSize))
    decodedNoMax.maxObjectSize shouldBe None

  test("AllocationGroupState round-trip"):
    val original = AllocationGroupState(
      AllocationGroupId(uuid(1)),
      level = 2,
      name = "test-group",
      members = List(
        AllocationGroupState.Member(AllocationGroupState.MemberType.Pool, uuid(2), Some(1024), 500L, 10000L),
        AllocationGroupState.Member(AllocationGroupState.MemberType.Group, uuid(3), None, 200L, 5000L)
      ),
      parentGroups = List(AllocationGroupId(uuid(4)), AllocationGroupId(uuid(5)))
    )
    val decoded = Codec.decode(Codec.encode(original))
    decoded.groupId shouldBe original.groupId
    decoded.level shouldBe 2
    decoded.name shouldBe "test-group"
    decoded.members.size shouldBe 2
    decoded.members.head.memberType shouldBe AllocationGroupState.MemberType.Pool
    decoded.members.head.uuid shouldBe uuid(2)
    decoded.members.head.maxObjectSize shouldBe Some(1024)
    decoded.members.head.currentUsage shouldBe 500L
    decoded.members.head.maximumSize shouldBe 10000L
    decoded.members(1).memberType shouldBe AllocationGroupState.MemberType.Group
    decoded.members(1).maxObjectSize shouldBe None
    decoded.parentGroups shouldBe List(AllocationGroupId(uuid(4)), AllocationGroupId(uuid(5)))

    val bytes = original.toBytes
    val fromBytes = AllocationGroupState(bytes)
    fromBytes.groupId shouldBe original.groupId
    fromBytes.level shouldBe original.level
    fromBytes.name shouldBe original.name
    fromBytes.members.size shouldBe original.members.size

    val empty = AllocationGroupState(AllocationGroupId(uuid(10)), level = 0, name = "empty", members = Nil, parentGroups = Nil)
    val decodedEmpty = Codec.decode(Codec.encode(empty))
    decodedEmpty.members shouldBe Nil
    decodedEmpty.parentGroups shouldBe Nil

  test("HostState round-trip"):
    val original = HostState(
      HostId(uuid(1)), "host-1", "192.168.1.1", 5000, 5001, 5002,
      Set(StorageDeviceId(uuid(2)), StorageDeviceId(uuid(3)))
    )
    val decoded = Codec.decode(Codec.encode(original))
    decoded.hostId shouldBe original.hostId
    decoded.name shouldBe "host-1"
    decoded.address shouldBe "192.168.1.1"
    decoded.dataPort shouldBe 5000
    decoded.cncPort shouldBe 5001
    decoded.storeTransferPort shouldBe 5002
    decoded.storageDevices.size shouldBe 2

  test("NewStore round-trip"):
    val original = cnc.NewStore(storeId(1), RocksDBConfig())
    val decoded = Codec.decode(Codec.encode(original))
    decoded.storeId shouldBe original.storeId

  test("ShutdownStore round-trip"):
    val original = cnc.ShutdownStore(storeId(1))
    val decoded = Codec.decode(Codec.encode(original))
    decoded.storeId shouldBe original.storeId

  test("TransferStore round-trip"):
    val original = cnc.TransferStore(storeId(1), HostId(uuid(2)))
    val decoded = Codec.decode(Codec.encode(original))
    decoded.storeId shouldBe original.storeId
    decoded.toHost shouldBe original.toHost

  test("CnCError round-trip"):
    val original = cnc.Error("something went wrong")
    val decoded = Codec.decode(Codec.encode(original))
    decoded.message shouldBe "something went wrong"

  test("StorageDeviceId round-trip"):
    val original = StorageDeviceId(uuid(42))
    val decoded = Codec.decode(Codec.encode(original))
    decoded shouldBe original

  test("StorageDeviceState.StoreEntry round-trip"):
    val withTransfer = StorageDeviceState.StoreEntry(
      StorageDeviceState.StoreStatus.TransferringOut,
      Some(StorageDeviceId(uuid(5)))
    )
    val decodedWithTransfer = Codec.decode(Codec.encode(withTransfer))
    decodedWithTransfer.status shouldBe StorageDeviceState.StoreStatus.TransferringOut
    decodedWithTransfer.transferDevice.get shouldBe StorageDeviceId(uuid(5))

    val noTransfer = StorageDeviceState.StoreEntry(StorageDeviceState.StoreStatus.Active, None)
    val decodedNoTransfer = Codec.decode(Codec.encode(noTransfer))
    decodedNoTransfer.status shouldBe StorageDeviceState.StoreStatus.Active
    decodedNoTransfer.transferDevice shouldBe None

  test("StorageDeviceState round-trip"):
    val original = new StorageDeviceState(
      StorageDeviceId(uuid(1)),
      HostId(uuid(2)),
      1024L * 1024L * 500L,
      1024L * 1024L * 1024L * 10L,
      Map(
        storeId(3) -> StorageDeviceState.StoreEntry(StorageDeviceState.StoreStatus.Active, None),
        storeId(4) -> StorageDeviceState.StoreEntry(StorageDeviceState.StoreStatus.Initializing, Some(StorageDeviceId(uuid(5))))
      )
    )
    val decoded = Codec.decode(Codec.encode(original))
    decoded.storageDeviceId shouldBe original.storageDeviceId
    decoded.hostId shouldBe original.hostId
    decoded.currentUsage shouldBe original.currentUsage
    decoded.totalSize shouldBe original.totalSize
    decoded.stores.size shouldBe 2

  test("SteppedDurableTaskState round-trip"):
    val step = 3
    val state = Map("key1" -> Array[Byte](1, 2, 3), "key2" -> Array[Byte](4, 5))
    val encoded = Codec.encodeSteppedDurableTaskState(step, state)
    val (decodedStep, decodedState) = Codec.decodeSteppedDurableTaskState(encoded)
    decodedStep shouldBe 3
    decodedState.size shouldBe 2
    decodedState("key1") shouldBe Array[Byte](1, 2, 3)
    decodedState("key2") shouldBe Array[Byte](4, 5)

    val emptyState = Map.empty[String, Array[Byte]]
    val (decodedEmptyStep, decodedEmptyState) = Codec.decodeSteppedDurableTaskState(
      Codec.encodeSteppedDurableTaskState(0, emptyState))
    decodedEmptyStep shouldBe 0
    decodedEmptyState shouldBe empty
