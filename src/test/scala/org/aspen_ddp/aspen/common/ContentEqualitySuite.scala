package org.aspen_ddp.aspen.common

import java.nio.ByteBuffer
import java.util.UUID

import org.aspen_ddp.aspen.amoebafs.impl.simple.SimpleSymlink.SetSymLink
import org.aspen_ddp.aspen.codec
import org.aspen_ddp.aspen.common.ida.Replication
import org.aspen_ddp.aspen.common.metadata.{HostId, StorageDeviceId, StorageDeviceSetId, StoragePoolState}
import org.aspen_ddp.aspen.common.network.{ClientId, ReadResponse, ServiceMessage}
import org.aspen_ddp.aspen.common.network.implementations.quic.QuicMessageCodec.Prefix
import org.aspen_ddp.aspen.common.objects.{ObjectRefcount, ObjectRevision, Value, ValueObject}
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.transaction.{FinalizationActionId, SerializedFinalizationAction, TransactionId}
import org.aspen_ddp.aspen.server.crl.simple.{StreamId, WriterThread}
import org.aspen_ddp.aspen.server.store.backend.RocksDBConfig
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** Types holding arrays or ignoring fields in equals must compare and hash by content, consistently */
class ContentEqualitySuite extends AnyFunSuite with Matchers {

  def uuid(n: Int): UUID = new UUID(0, n)
  def bytes(n: Int*): Array[Byte] = n.map(_.toByte).toArray

  /** `make(i)` must build a fresh instance from fresh arrays, with distinct content for distinct `i` */
  def checkContentEquality[T](make: Int => T): Unit =
    make(1) shouldBe make(1)
    make(1).hashCode shouldBe make(1).hashCode
    make(1) should not be make(2)

  test("Value") {
    checkContentEquality(i => Value(bytes(i, 0)))
  }

  test("ValueObject") {
    checkContentEquality(i => ValueObject(Value(bytes(i)), ObjectRevision(TransactionId(uuid(1))), HLCTimestamp(1)))
  }

  test("SerializedFinalizationAction") {
    checkContentEquality(i => SerializedFinalizationAction(FinalizationActionId(uuid(1)), bytes(i, 0)))
  }

  test("ReadResponse.CurrentState ignores timestamp and sizeOnStore") {
    def cs(data: Int, timestamp: Long = 1, sizeOnStore: Int = 2) = ReadResponse.CurrentState(
      ObjectRevision(TransactionId(uuid(1))), ObjectRefcount(0, 1), HLCTimestamp(timestamp), sizeOnStore,
      Some(DataBuffer(bytes(data, 0))), Set(TransactionId(uuid(2))))
    checkContentEquality(i => cs(i))
    cs(1) shouldBe cs(1, timestamp = 5, sizeOnStore = 9)
    cs(1).hashCode shouldBe cs(1, timestamp = 5, sizeOnStore = 9).hashCode
  }

  test("StoragePoolState") {
    checkContentEquality(i => StoragePoolState(PoolId(uuid(1)), "pool", Replication(3, 2), None,
      Array(StoragePoolState.StoreEntry(HostId(uuid(i)), StorageDeviceId(uuid(3)))),
      RocksDBConfig(), StorageDeviceSetId(uuid(4))))
  }

  test("ServiceMessage") {
    checkContentEquality(i => ServiceMessage(HostId(uuid(1)), ClientId(uuid(2)), uuid(3), bytes(i, 0)))
  }

  test("SimpleSymlink.SetSymLink") {
    checkContentEquality(i => SetSymLink(bytes(i, 0)))
  }

  test("QuicMessageCodec.Prefix") {
    checkContentEquality(i => Prefix(codec.Message.defaultInstance, ByteBuffer.wrap(bytes(7)), Array(i, 0)))
  }

  test("WriterThread.Write compares buffers by content") {
    val onComplete = () => ()
    checkContentEquality(i => WriterThread.Write(StreamId(1), 0, Array(ByteBuffer.wrap(bytes(i, 0))), onComplete))
  }
}
