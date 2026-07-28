package org.aspen_ddp.aspen.client.internal

import org.aspen_ddp.aspen.client.internal.network.Messenger as ClientMessenger
import org.aspen_ddp.aspen.common.Radicle
import org.aspen_ddp.aspen.common.ida.{IDA, Replication}
import org.aspen_ddp.aspen.common.network.{ClientId, ClientRequest, HostMessage, Read, TxMessage}
import org.aspen_ddp.aspen.common.objects.{KeyValueObjectPointer, ObjectId, ObjectPointer}
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.{Duration, SECONDS}

/** Covers the bootstrap-pool IDA short circuit in SimpleAspenClient.resolveIda.
  *
  * Resolving the bootstrap pool's IDA by reading its pool object recurses forever, because the
  * storage-pools tree that holds the pointer to that object itself lives in the bootstrap pool.
  * The client must use the IDA supplied offline by the bootstrap config file instead.
  */
object SimpleAspenClientSuite:

  val bootstrapIda: IDA = Replication(3, 2)

  /** Records outbound client requests and drops everything else. Nothing ever replies, so any
    * read started against it stays pending -- which is exactly what the tests below check for.
    */
  class RecordingMessenger extends ClientMessenger:
    var requests: List[ClientRequest] = Nil

    override def sendClientRequest(msg: ClientRequest): Unit = synchronized:
      requests = msg :: requests

    override def sendTransactionMessage(msg: TxMessage): Unit = ()
    override def sendTransactionMessages(msg: List[TxMessage]): Unit = ()
    override def sendHostMessage(msg: HostMessage): Unit = ()
    override def dropCacheForStore(storeId: StoreId): Unit = ()

  /** Exposes the protected resolveIda seam so it can be tested directly. */
  class TestClient(msngr: ClientMessenger, ec: ExecutionContext)
    extends SimpleAspenClient(
      msngr,
      ClientId(new UUID(0, 1)),
      ec,
      Radicle.pointer,
      bootstrapIda,
      Duration(10, SECONDS),
      Duration(10, SECONDS),
      Duration(10, SECONDS),
      Duration(1, SECONDS),
      Duration(5, SECONDS)):

    def testResolveIda(pointer: ObjectPointer): Future[IDA] = resolveIda(pointer)


class SimpleAspenClientSuite extends AnyFunSuite with Matchers:
  import SimpleAspenClientSuite.*

  // Same-thread execution: an unbroken resolveIda cycle recurses on the calling stack rather
  // than bouncing between pool threads, so the pre-fix failure is a prompt StackOverflowError.
  given ExecutionContext = ExecutionContext.parasitic

  private def withClient[T](fn: (TestClient, RecordingMessenger) => T): T =
    val msngr = new RecordingMessenger
    val client = new TestClient(msngr, summon[ExecutionContext])
    try
      fn(client, msngr)
    finally
      client.shutdown()

  test("reading a bootstrap-pool object issues read requests instead of recursing"):
    withClient: (client, msngr) =>
      client.read(Radicle.pointer)

      val reads = msngr.requests.collect { case r: Read => r }

      reads.size shouldBe bootstrapIda.width
      reads.map(_.toStore).toSet shouldBe
        (0 until bootstrapIda.width).map(i => StoreId(PoolId.BootstrapPoolId, i.toByte)).toSet
      reads.foreach(_.objectPointer shouldBe Radicle.pointer)

  test("resolveIda returns the configured IDA for the bootstrap pool"):
    withClient: (client, _) =>
      val f = client.testResolveIda(Radicle.pointer)

      f.value.map(_.get) shouldBe Some(bootstrapIda)

  test("resolveIda still resolves non-bootstrap pools through the storage pool"):
    withClient: (client, msngr) =>
      val otherPool = PoolId(UUID.randomUUID())
      val ptr = KeyValueObjectPointer(ObjectId(UUID.randomUUID()), otherPool)

      val f = client.testResolveIda(ptr)

      // The pool object has to be read before its IDA is known, and nothing ever answers, so
      // this cannot complete. Completing would mean the bootstrap short circuit leaked.
      f.value shouldBe None
      msngr.requests.collect { case r: Read => r } should not be empty
