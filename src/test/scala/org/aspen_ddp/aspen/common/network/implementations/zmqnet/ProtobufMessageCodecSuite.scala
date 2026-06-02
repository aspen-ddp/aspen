package org.aspen_ddp.aspen.common.network.implementations.zmqnet

import java.util.UUID
import org.aspen_ddp.aspen.common.HLCTimestamp
import org.aspen_ddp.aspen.common.metadata.{HostId, StorageDeviceId}
import org.aspen_ddp.aspen.common.network.*
import org.aspen_ddp.aspen.common.paxos.ProposalId
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.transaction.{TransactionDisposition, TransactionId}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class ProtobufMessageCodecSuite extends AnyFunSuite with Matchers:

  // Helper method to create test UUIDs
  def testUUID(value: Int): UUID = new UUID(0, value)

  // Test data fixtures
  val hostId1 = HostId(testUUID(1))
  val hostId2 = HostId(testUUID(2))
  val poolId1 = PoolId(testUUID(10))
  val storeId1 = StoreId(poolId1, 0)
  val storeId2 = StoreId(poolId1, 1)
  val clientId1 = ClientId(testUUID(100))
  val txId1 = TransactionId(testUUID(200))
  val deviceId1 = StorageDeviceId(testUUID(300))
  val proposalId1 = ProposalId(1, 0)

  test("HostHeartbeat - roundtrip via encodeHeartbeat"):
    val original = HostHeartbeat(hostId1)
    val encoded = ProtobufMessageCodec.encodeHeartbeat(original)
    val decoded = ProtobufMessageCodec.decodeMessage(encoded)

    decoded shouldBe defined
    decoded.get shouldBe a[HostHeartbeat]
    val result = decoded.get.asInstanceOf[HostHeartbeat]
    result.hostId shouldBe original.hostId

  test("HostHeartbeat - roundtrip via generic encodeMessage"):
    val original = HostHeartbeat(hostId2)
    val encoded = ProtobufMessageCodec.encodeMessage(original)
    val decoded = ProtobufMessageCodec.decodeMessage(encoded)

    decoded shouldBe defined
    decoded.get shouldBe a[HostHeartbeat]
    val result = decoded.get.asInstanceOf[HostHeartbeat]
    result.hostId shouldBe original.hostId

  test("TransactionCompletionQuery - roundtrip via encodeClientRequest"):
    val queryUUID = testUUID(123)
    val original = TransactionCompletionQuery(storeId1, clientId1, queryUUID, txId1)
    val encoded = ProtobufMessageCodec.encodeClientRequest(original)
    val decoded = ProtobufMessageCodec.decodeMessage(encoded)

    decoded shouldBe defined
    decoded.get shouldBe a[TransactionCompletionQuery]
    val result = decoded.get.asInstanceOf[TransactionCompletionQuery]
    result.toStore shouldBe original.toStore
    result.fromClient shouldBe original.fromClient
    result.queryUUID shouldBe original.queryUUID
    result.transactionId shouldBe original.transactionId

  test("TransactionCompletionResponse - roundtrip via encodeClientResponse"):
    val queryUUID = testUUID(456)
    val original = TransactionCompletionResponse(clientId1, storeId1, queryUUID, true)
    val encoded = ProtobufMessageCodec.encodeClientResponse(original)
    val decoded = ProtobufMessageCodec.decodeMessage(encoded)

    decoded shouldBe defined
    decoded.get shouldBe a[TransactionCompletionResponse]
    val result = decoded.get.asInstanceOf[TransactionCompletionResponse]
    result.toClient shouldBe original.toClient
    result.fromStore shouldBe original.fromStore
    result.queryUUID shouldBe original.queryUUID
    result.isComplete shouldBe original.isComplete

  test("TransactionResolved - roundtrip via encodeClientResponse"):
    val original = TransactionResolved(clientId1, storeId1, txId1, committed = true)
    val encoded = ProtobufMessageCodec.encodeClientResponse(original)
    val decoded = ProtobufMessageCodec.decodeMessage(encoded)

    decoded shouldBe defined
    decoded.get shouldBe a[TransactionResolved]
    val result = decoded.get.asInstanceOf[TransactionResolved]
    result.toClient shouldBe original.toClient
    result.fromStore shouldBe original.fromStore
    result.transactionId shouldBe original.transactionId
    result.committed shouldBe original.committed

  test("TxHeartbeat - roundtrip via encodeTxMessage"):
    val original = TxHeartbeat(storeId1, storeId2, txId1)
    val encoded = ProtobufMessageCodec.encodeTxMessage(original)
    val decoded = ProtobufMessageCodec.decodeMessage(encoded)

    decoded shouldBe defined
    decoded.get shouldBe a[TxHeartbeat]
    val result = decoded.get.asInstanceOf[TxHeartbeat]
    result.to shouldBe original.to
    result.from shouldBe original.from
    result.transactionId shouldBe original.transactionId

  test("TxResolved - roundtrip via encodeTxMessage"):
    val original = TxResolved(storeId1, storeId2, txId1, committed = true)
    val encoded = ProtobufMessageCodec.encodeTxMessage(original)
    val decoded = ProtobufMessageCodec.decodeMessage(encoded)

    decoded shouldBe defined
    decoded.get shouldBe a[TxResolved]
    val result = decoded.get.asInstanceOf[TxResolved]
    result.to shouldBe original.to
    result.from shouldBe original.from
    result.transactionId shouldBe original.transactionId
    result.committed shouldBe original.committed

  test("TxAccept - roundtrip via encodeTxMessage"):
    val original = TxAccept(storeId1, storeId2, txId1, proposalId1, value = true)
    val encoded = ProtobufMessageCodec.encodeTxMessage(original)
    val decoded = ProtobufMessageCodec.decodeMessage(encoded)

    decoded shouldBe defined
    decoded.get shouldBe a[TxAccept]
    val result = decoded.get.asInstanceOf[TxAccept]
    result.to shouldBe original.to
    result.from shouldBe original.from
    result.transactionId shouldBe original.transactionId
    result.proposalId shouldBe original.proposalId
    result.value shouldBe original.value

  test("TxPrepareResponse - roundtrip via encodeTxMessage"):
    val promise = TxPrepareResponse.Promise(Some((proposalId1, true)))
    val original = TxPrepareResponse(
      storeId1,
      storeId2,
      txId1,
      Right(promise),
      proposalId1,
      TransactionDisposition.VoteCommit,
      Nil
    )
    val encoded = ProtobufMessageCodec.encodeTxMessage(original)
    val decoded = ProtobufMessageCodec.decodeMessage(encoded)

    decoded shouldBe defined
    decoded.get shouldBe a[TxPrepareResponse]
    val result = decoded.get.asInstanceOf[TxPrepareResponse]
    result.to shouldBe original.to
    result.from shouldBe original.from
    result.transactionId shouldBe original.transactionId
    result.proposalId shouldBe original.proposalId
    result.disposition shouldBe original.disposition
    result.collisions shouldBe original.collisions
    result.response shouldBe original.response

  test("CheckStorageDevice - roundtrip via encodeHostMessage"):
    val original = CheckStorageDevice(hostId1, clientId1, deviceId1)
    val encoded = ProtobufMessageCodec.encodeHostMessage(original)
    val decoded = ProtobufMessageCodec.decodeMessage(encoded)

    decoded shouldBe defined
    decoded.get shouldBe a[CheckStorageDevice]
    val result = decoded.get.asInstanceOf[CheckStorageDevice]
    result.toHost shouldBe original.toHost
    result.fromClient shouldBe original.fromClient
    result.deviceId shouldBe original.deviceId

  test("Malformed data - decodeMessage handles garbage input gracefully"):
    val garbage = Array[Byte](1, 2, 3, 4, 5)
    val decoded = ProtobufMessageCodec.decodeMessage(garbage)
    decoded shouldBe None

  test("Empty array - decodeMessage returns None"):
    val empty = Array[Byte]()
    val decoded = ProtobufMessageCodec.decodeMessage(empty)
    decoded shouldBe None

  test("Too short message - decodeMessage returns None"):
    val tooShort = Array[Byte](1, 2)
    val decoded = ProtobufMessageCodec.decodeMessage(tooShort)
    decoded shouldBe None
