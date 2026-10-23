package org.aspen_ddp.aspen.client.internal.transaction

import org.aspen_ddp.aspen.client.internal.network.Messenger as ClientMessenger
import org.aspen_ddp.aspen.common.ida.Replication
import org.aspen_ddp.aspen.common.network.{ClientRequest, HostMessage, TxMessage}
import org.aspen_ddp.aspen.common.objects.{DataObjectPointer, ObjectId}
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.util.UUID
import scala.util.Random

class DesignatedLeaderSuite extends AnyFunSuite with Matchers:

  private val poolId = PoolId(UUID.fromString("11111111-1111-1111-1111-111111111111"))
  private val pointer = DataObjectPointer(ObjectId(new UUID(0, 1)), poolId, Array[Byte]())
  private val ida = Replication(5, 3)

  private class StubMessenger(offline: Set[Int] = Set(), hostOnline: Set[Int] = (0 until 5).toSet)
    extends ClientMessenger:
    def sendClientRequest(msg: ClientRequest): Unit = ()
    def sendTransactionMessage(msg: TxMessage): Unit = ()
    def sendTransactionMessages(msg: List[TxMessage]): Unit = ()
    def sendHostMessage(msg: HostMessage): Unit = ()
    def dropCacheForStore(storeId: StoreId): Unit = ()
    override def isStoreOffline(storeId: StoreId): Boolean = offline.contains(storeId.poolIndex)
    override def isStoreHostOnline(storeId: StoreId): Boolean = hostOnline.contains(storeId.poolIndex)

  private def chosen(m: ClientMessenger): Set[Int] =
    val random = new Random(42)
    (0 until 500).map(_ => DesignatedLeader.choose(m, pointer, ida, random).toInt).toSet

  test("every store is chosen when all are available"):
    chosen(new StubMessenger()) shouldBe (0 until 5).toSet

  test("offline stores and stores on offline hosts are avoided"):
    chosen(new StubMessenger(offline = Set(0, 1), hostOnline = Set(1, 2, 3))) shouldBe Set(2, 3)

  test("stores not offline are used when no host is known online"):
    chosen(new StubMessenger(offline = Set(0, 4), hostOnline = Set())) shouldBe Set(1, 2, 3)

  test("any store is used when every store is offline"):
    chosen(new StubMessenger(offline = (0 until 5).toSet)) shouldBe (0 until 5).toSet

  test("networks without liveness knowledge spread over every store"):
    chosen(ClientMessenger.None) shouldBe (0 until 5).toSet
