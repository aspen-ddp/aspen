package org.aspen_ddp.aspen.client.internal.read

import java.util.UUID
import org.aspen_ddp.aspen.client.{CorruptedObject, DataObjectState, InvalidObject, ReadDriverClient}
import org.aspen_ddp.aspen.common.network.{ClientId, ReadResponse}
import org.aspen_ddp.aspen.common.{DataBuffer, HLCTimestamp}
import org.aspen_ddp.aspen.common.ida.Replication
import org.aspen_ddp.aspen.client.{KeyValueObjectState, MetadataObjectState, ObjectCache, ObjectState}
import org.aspen_ddp.aspen.client.internal.OpportunisticRebuildManager
import org.aspen_ddp.aspen.client.internal.network.Messenger
import org.aspen_ddp.aspen.common.network.{ClientRequest, HostMessage, OpportunisticRebuild, Read, TxMessage}
import org.aspen_ddp.aspen.common.objects.{ByteRanges, DataObjectPointer, FullObject, Key, KeySet, KeyValueObjectPointer, MetadataOnly, ObjectId, ObjectPointer, ObjectRefcount, ObjectRevision, ReadError, ReadType, Value}
import org.aspen_ddp.aspen.server.store.{KVObjectState, ValueState => StoreValueState}
import org.aspen_ddp.aspen.common.transaction.PreTransactionOpportunisticRebuild
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.transaction.{TransactionDescription, TransactionId}
import org.scalatest.funsuite.AsyncFunSuite
import org.scalatest.matchers.should.Matchers

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration.*
import scala.language.implicitConversions

object BaseReadDriverSuite {
  val awaitDuration = Duration(100, MILLISECONDS)

  val objId = ObjectId(new UUID(0,1))
  val poolId = PoolId(new UUID(0,2))
  val readUUID = new UUID(0,3)
  val cliUUID = new UUID(0,4)

  val ida = Replication(3,2)

  val ds0 = StoreId(poolId, 0)
  val ds1 = StoreId(poolId, 1)
  val ds2 = StoreId(poolId, 2)

  val ptr = DataObjectPointer(objId, poolId)
  val kvptr = KeyValueObjectPointer(objId, poolId)
  val rev = ObjectRevision.Null
  val ref = ObjectRefcount(1,1)

  val odata = DataBuffer(List[Byte](1,2,3,4).toArray)

  val noLocks = Some(Map[StoreId, List[TransactionDescription]]())

  val client = ClientId(cliUUID)

}

class BaseReadDriverSuite  extends AsyncFunSuite with Matchers {
  import BaseReadDriverSuite._


  def mkReader(client: ReadDriverClient,
               objectPointer: ObjectPointer = ptr,
               readUUID:UUID = readUUID,
               comment: String = "",
               disableOpportunisticRebuild: Boolean = false,
               readType: ReadType = FullObject()) = {
    new BaseReadDriver(client, objectPointer, ida, readUUID, comment, disableOpportunisticRebuild, readType) {
      given ec: ExecutionContext = this.client.clientContext
    }
  }

  test("Fail with invalid object") {
    val m = ReadDriverClient.NoOp(client)
    val r = mkReader(m)
    val nrev = ObjectRevision(TransactionId(new UUID(0,1)))
    val nrev2 = ObjectRevision(TransactionId(new UUID(0,2)))

    val ts = HLCTimestamp.now
    val readTime = HLCTimestamp(ts.asLong - 100)

    r.receiveReadResponse(ReadResponse(client, ds0, readUUID, readTime, Right(ReadResponse.CurrentState(rev, ref, ts, 5, Some(odata), Set()))))
    r.readResult.isCompleted should be (false)
    r.receiveReadResponse(ReadResponse(client, ds1, readUUID, readTime, Left(ReadError.ObjectNotFound)))
    r.readResult.isCompleted should be (false)
    r.receiveReadResponse(ReadResponse(client, ds2, readUUID, readTime, Left(ReadError.ObjectMismatch)))

    r.readResult.isCompleted should be (true)

    recoverToSucceededIf[InvalidObject] {
      r.readResult
    }
  }

  test("Fail with corrupted object") {
    val m = ReadDriverClient.NoOp(client)
    val r = mkReader(m)
    val nrev = ObjectRevision(TransactionId(new UUID(0,1)))
    val nrev2 = ObjectRevision(TransactionId(new UUID(0,2)))

    val ts = HLCTimestamp.now

    r.receiveReadResponse(ReadResponse(client, ds0, readUUID, ts, Right(ReadResponse.CurrentState(rev, ref, ts, 5, Some(odata), Set()))))
    r.readResult.isCompleted should be (false)
    r.receiveReadResponse(ReadResponse(client, ds1, readUUID, ts, Left(ReadError.CorruptedObject)))
    r.readResult.isCompleted should be (false)
    r.receiveReadResponse(ReadResponse(client, ds2, readUUID, ts, Left(ReadError.CorruptedObject)))

    r.readResult.isCompleted should be (true)

    recoverToSucceededIf[CorruptedObject] {
      r.readResult
    }
  }

  test("Succeed with errors") {
    val m = ReadDriverClient.NoOp(client)
    val r = mkReader(m)
    val nrev = ObjectRevision(TransactionId(new UUID(0,1)))
    val nrev2 = ObjectRevision(TransactionId(new UUID(0,2)))
    val ts = HLCTimestamp.now
    val readTime = HLCTimestamp(ts.asLong - 100)

    r.receiveReadResponse(ReadResponse(client, ds0, readUUID, readTime, Right(ReadResponse.CurrentState(rev, ref, ts, 5, Some(odata), Set()))))
    r.readResult.isCompleted should be (false)
    r.receiveReadResponse(ReadResponse(client, ds1, readUUID, readTime, Left(ReadError.ObjectNotFound)))
    r.readResult.isCompleted should be (false)
    r.receiveReadResponse(ReadResponse(client, ds2, readUUID, readTime, Right(ReadResponse.CurrentState(nrev2, ref, ts, 5, Some(odata), Set()))))
    r.readResult.isCompleted should be (false)
    r.receiveReadResponse(ReadResponse(client, ds0, readUUID, readTime, Right(ReadResponse.CurrentState(nrev2, ref, ts, 5, Some(odata), Set()))))
    r.readResult.isCompleted should be (true)
    val o = Await.result(r.readResult, awaitDuration)

    //    o match {
    //      case Left(_) =>
    //      case Right((ds:DataObjectState, o)) =>
    //        println(s"ptr(${ds.pointer}), rev(${ds.revision}), ref(${ds.refcount}), ts(${ds.timestamp}), data(${com.ibm.aspen.util.db2string(ds.data)})")
    //        println(s"ptr(${ptr}), rev(${nrev2}), ref(${ref}), ts(${ts}), data(${com.ibm.aspen.util.db2string(odata)})")
    //    }

    o should be (DataObjectState(ptr, nrev2, ref, ts, readTime, ida, 5, odata))
  }

  test("Ignore old revisions") {
    val m = ReadDriverClient.NoOp(client)
    val r = mkReader(m)
    val nrev = ObjectRevision(TransactionId(new UUID(0,1)))
    val nrev2 = ObjectRevision(TransactionId(new UUID(0,2)))
    val ts = HLCTimestamp.now

    r.receiveReadResponse(ReadResponse(client, ds0, readUUID, ts, Right(ReadResponse.CurrentState(rev,   ref, ts, 5, Some(odata), Set()))))
    r.readResult.isCompleted should be (false)
    r.receiveReadResponse(ReadResponse(client, ds1, readUUID, ts, Right(ReadResponse.CurrentState(nrev,  ref, ts, 5, Some(odata), Set()))))
    r.readResult.isCompleted should be (false)
    r.receiveReadResponse(ReadResponse(client, ds2, readUUID, ts, Right(ReadResponse.CurrentState(nrev2, ref, ts, 5, Some(odata), Set()))))
    r.readResult.isCompleted should be (false)
    r.receiveReadResponse(ReadResponse(client, ds0, readUUID, ts, Right(ReadResponse.CurrentState(nrev2, ref, ts, 5, Some(odata), Set()))))
    r.readResult.isCompleted should be (true)
    val o = Await.result(r.readResult, awaitDuration)

    o should be (DataObjectState(ptr, nrev2, ref, ts, ts, ida, 5, odata))
  }

  test("Use minimum readTime") {
    val m = ReadDriverClient.NoOp(client)
    val r = mkReader(m)
    val nrev = ObjectRevision(TransactionId(new UUID(0,1)))
    val nrev2 = ObjectRevision(TransactionId(new UUID(0,2)))
    val ts = HLCTimestamp.now

    val minTs = HLCTimestamp(ts.asLong-100)

    r.receiveReadResponse(ReadResponse(client, ds0, readUUID, ts, Right(ReadResponse.CurrentState(rev,   ref, ts, 5, Some(odata), Set()))))
    r.readResult.isCompleted should be (false)
    r.receiveReadResponse(ReadResponse(client, ds1, readUUID, ts, Right(ReadResponse.CurrentState(nrev,  ref, ts, 5, Some(odata), Set()))))
    r.readResult.isCompleted should be (false)
    r.receiveReadResponse(ReadResponse(client, ds2, readUUID, minTs, Right(ReadResponse.CurrentState(nrev2, ref, ts, 5, Some(odata), Set()))))
    r.readResult.isCompleted should be (false)
    r.receiveReadResponse(ReadResponse(client, ds0, readUUID, ts, Right(ReadResponse.CurrentState(nrev2, ref, ts, 5, Some(odata), Set()))))
    r.readResult.isCompleted should be (true)
    val o = Await.result(r.readResult, awaitDuration)

    o should be (DataObjectState(ptr, nrev2, ref, ts, minTs, ida, 5, odata))
  }


  test("Successful read with data and locks") {
    val m = ReadDriverClient.NoOp(client)
    val r = mkReader(m)
    val ts = HLCTimestamp.now

    r.receiveReadResponse(ReadResponse(client, ds0, readUUID, ts, Right(ReadResponse.CurrentState(rev, ref, ts, 5, Some(odata), Set()))))
    r.readResult.isCompleted should be (false)
    r.receiveReadResponse(ReadResponse(client, ds1, readUUID, ts, Right(ReadResponse.CurrentState(rev, ref, ts, 5, Some(odata), Set()))))
    r.readResult.isCompleted should be (true)
    val o = Await.result(r.readResult, awaitDuration)

    o should be (DataObjectState(ptr, rev, ref, ts, ts, ida, 5, odata))
  }


  /** Records the cache puts, client requests, and repair marks a read driver makes */
  class RecordingClient extends ReadDriverClient.NoOp(client):
    var puts: List[ObjectState] = Nil
    var requests: List[ClientRequest] = Nil
    var repairMarks: List[Set[Byte]] = Nil

    override private[client] val objectCache: ObjectCache = new ObjectCache:
      def get(pointer: DataObjectPointer): Option[DataObjectState] = None
      def get(pointer: KeyValueObjectPointer): Option[KeyValueObjectState] = None
      private[client] def put(pointer: ObjectPointer, os: ObjectState): Unit = puts = os :: puts

    override private[client] val messenger: Messenger = new Messenger:
      def sendClientRequest(msg: ClientRequest): Unit = requests = msg :: requests
      def sendTransactionMessage(msg: TxMessage): Unit = ()
      def sendTransactionMessages(msg: List[TxMessage]): Unit = ()
      def sendHostMessage(msg: HostMessage): Unit = ()
      def dropCacheForStore(storeId: StoreId): Unit = ()

    override private[client] def opportunisticRebuildManager: OpportunisticRebuildManager = new OpportunisticRebuildManager:
      def markRepairNeeded(os: ObjectState, repairNeeded: Set[Byte]): Unit =
        repairMarks = repairNeeded :: repairMarks
      def getPreTransactionOpportunisticRebuild(pointer: ObjectPointer): Map[Byte, PreTransactionOpportunisticRebuild] = Map()
      def slowReadReplyDuration: Duration = Duration.Zero

  val kvKey = Key(Array[Byte](5))

  def kvData(rev: ObjectRevision, ts: HLCTimestamp): Option[DataBuffer] =
    Some(KVObjectState.encode(None, None, None, None,
      Map(kvKey -> new StoreValueState(Value(Array[Byte](1)), rev, ts, None))))

  /** ds1 replies stale first; ds0 and ds2 then complete the read; ds1 replies stale again.
    * Stores send no object data for metadata-only reads */
  def runStaleStoreReadOf(readType: ReadType, pointer: ObjectPointer,
                          refcount: ObjectRefcount = ref): (RecordingClient, Future[ObjectState]) =
    val m = new RecordingClient
    val r = mkReader(m, objectPointer = pointer, readType = readType)
    val nrev2 = ObjectRevision(TransactionId(new UUID(0,2)))
    val ts = HLCTimestamp.now
    val oldTs = HLCTimestamp(ts.asLong - 1000)

    def data(rv: ObjectRevision, t: HLCTimestamp): Option[DataBuffer] = (readType, pointer) match
      case (_: MetadataOnly, _) => None
      case (_, _: KeyValueObjectPointer) => kvData(rv, t)
      case _ => Some(odata)

    def cs(rv: ObjectRevision, t: HLCTimestamp) = Right(ReadResponse.CurrentState(rv, refcount, t, 5, data(rv, t), Set()))

    r.receiveReadResponse(ReadResponse(client, ds1, readUUID, ts, cs(rev, oldTs)))
    r.receiveReadResponse(ReadResponse(client, ds0, readUUID, ts, cs(nrev2, ts)))
    r.receiveReadResponse(ReadResponse(client, ds2, readUUID, ts, cs(nrev2, ts)))
    r.receiveReadResponse(ReadResponse(client, ds1, readUUID, ts, cs(rev, oldTs)))

    (m, r.readResult)

  def runStaleStoreRead(readType: ReadType): (RecordingClient, KeyValueObjectState) =
    val (m, result) = runStaleStoreReadOf(readType, kvptr)
    (m, Await.result(result, awaitDuration).asInstanceOf[KeyValueObjectState])

  def rebuildsSent(m: RecordingClient): List[OpportunisticRebuild] =
    m.requests.collect { case o: OpportunisticRebuild => o }

  test("full KV read caches and rebuilds stale stores") {
    val (m, kvos) = runStaleStoreRead(FullObject())
    kvos.partial should be (false)
    m.puts.size should be (1)
    m.repairMarks should not be empty
    m.repairMarks.foreach(_ should be (Set[Byte](1)))
    rebuildsSent(m).map(_.toStore) should be (List(ds1))
  }

  test("partial KV read neither caches nor rebuilds") {
    val (m, kvos) = runStaleStoreRead(KeySet(List(kvKey)))
    kvos.partial should be (true)
    kvos.contents.contains(kvKey) should be (true)
    kvos.getRebuildDataForStore(ds1) should be (None)
    m.puts should be (Nil)
    m.repairMarks should be (Nil)
    rebuildsSent(m) should be (Nil)
  }

  test("partial Data ByteRanges read neither caches nor rebuilds") {
    val (m, result) = runStaleStoreReadOf(ByteRanges(List((0, 4)), 0), ptr)
    val dos = Await.result(result, awaitDuration)
    dos.getRebuildDataForStore(ds1) should be (None)
    m.puts should be (Nil)
    m.repairMarks should be (Nil)
    rebuildsSent(m) should be (Nil)
  }

  test("partial KV read sends its read type") {
    val m = new RecordingClient
    val r = mkReader(m, objectPointer = kvptr, readType = KeySet(List(kvKey)))
    r.begin()
    val reads = m.requests.collect { case rd: Read => rd }
    reads.size should be (3)
    reads.foreach(_.readType should be (KeySet(List(kvKey))))
    succeed
  }

  def checkMetadataOnlyRead(pointer: ObjectPointer) =
    val (m, result) = runStaleStoreReadOf(MetadataOnly(), pointer)
    Await.result(result, awaitDuration) match
      case mos: MetadataObjectState =>
        mos.revision should be (ObjectRevision(TransactionId(new UUID(0,2))))
        mos.refcount should be (ref)
      case other => fail(s"Expected a MetadataObjectState, got $other")
    m.puts should be (Nil)
    m.repairMarks should be (Nil)
    rebuildsSent(m) should be (Nil)

  test("metadata-only KV read neither caches nor rebuilds") {
    checkMetadataOnlyRead(kvptr)
  }

  test("metadata-only Data read neither caches nor rebuilds") {
    checkMetadataOnlyRead(ptr)
  }

  test("metadata-only read of a deleted object fails with invalid object") {
    val (m, result) = runStaleStoreReadOf(MetadataOnly(), ptr, refcount = ObjectRefcount(2, 0))
    recoverToSucceededIf[InvalidObject](result)
  }
}
