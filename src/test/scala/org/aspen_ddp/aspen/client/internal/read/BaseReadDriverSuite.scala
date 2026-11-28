package org.aspen_ddp.aspen.client.internal.read

import java.util.UUID
import org.aspen_ddp.aspen.client.{CorruptedObject, DataObjectState, InvalidObject, ReadDriverClient}
import org.aspen_ddp.aspen.common.network.{ClientId, ReadResponse}
import org.aspen_ddp.aspen.common.{DataBuffer, HLCTimestamp}
import org.aspen_ddp.aspen.common.ida.Replication
import org.aspen_ddp.aspen.client.{KeyValueObjectState, MetadataObjectState, ObjectState, TransactionStatusCache}
import org.aspen_ddp.aspen.client.internal.network.Messenger
import org.aspen_ddp.aspen.common.network.{ClientRequest, HostMessage, Read, TxMessage}
import org.aspen_ddp.aspen.common.objects.{ByteRanges, DataObjectPointer, FullObject, Key, KeySet, KeyValueObjectPointer, MetadataOnly, ObjectId, ObjectPointer, ObjectRefcount, ObjectRevision, ReadError, ReadType, Value}
import org.aspen_ddp.aspen.server.store.{KVObjectState, ValueState => StoreValueState}
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
               readType: ReadType = FullObject()) = {
    new BaseReadDriver(client, objectPointer, ida, readUUID, comment, readType) {
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


  /** Records the client requests a read driver makes */
  class RecordingClient extends ReadDriverClient.NoOp(client):
    var requests: List[ClientRequest] = Nil

    override private[client] val messenger: Messenger = new Messenger:
      def sendClientRequest(msg: ClientRequest): Unit = requests = msg :: requests
      def sendTransactionMessage(msg: TxMessage): Unit = ()
      def sendTransactionMessages(msg: List[TxMessage]): Unit = ()
      def sendHostMessage(msg: HostMessage): Unit = ()
      def dropCacheForStore(storeId: StoreId): Unit = ()

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

  test("full KV read sends no rebuild to a stale store") {
    val (m, kvos) = runStaleStoreRead(FullObject())
    kvos.partial should be (false)
    m.requests should be (Nil)
  }

  test("partial KV read completes with partial state") {
    val (m, kvos) = runStaleStoreRead(KeySet(List(kvKey)))
    kvos.partial should be (true)
    kvos.contents.contains(kvKey) should be (true)
    kvos.getRebuildDataForStore(ds1) should be (None)
  }

  test("partial Data ByteRanges read completes") {
    val (m, result) = runStaleStoreReadOf(ByteRanges(List((0, 4)), 0), ptr)
    val dos = Await.result(result, awaitDuration)
    dos.getRebuildDataForStore(ds1) should be (None)
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

  test("metadata-only KV read returns metadata") {
    checkMetadataOnlyRead(kvptr)
  }

  test("metadata-only Data read returns metadata") {
    checkMetadataOnlyRead(ptr)
  }

  test("metadata-only read of a deleted object fails with invalid object") {
    val (m, result) = runStaleStoreReadOf(MetadataOnly(), ptr, refcount = ObjectRefcount(2, 0))
    recoverToSucceededIf[InvalidObject](result)
  }

  test("retrying a read prints nothing and restores nothing while trace logging is off") {
    val m = ReadDriverClient.NoOp(client)
    val r = mkReader(m)
    val cs = ReadResponse.CurrentState(rev, ref, HLCTimestamp.now, 4, Some(odata), Set())
    r.receiveReadResponse(ReadResponse(BaseReadDriverSuite.client, ds0, readUUID, HLCTimestamp.now, Right(cs)))
    r.receiveReadResponse(ReadResponse(BaseReadDriverSuite.client, ds1, readUUID, HLCTimestamp.now, Right(cs)))

    val out = new java.io.ByteArrayOutputStream()
    Console.withOut(out):
      (0 until 6).foreach(_ => r.begin())

    out.toString shouldBe ""
    Future.successful(succeed)
  }

  // A read racing a commit: a store replies with the old revision and a lock of a transaction not yet known to have
  // committed. Without an immediate re-read the read waits for the next full resend (2 s for vdisk-server)
  val lockTx = TransactionId(new UUID(0, 9))
  val lockTxRev = ObjectRevision(lockTx)

  def reads(m: RecordingClient, store: StoreId): Int =
    m.requests.count { case rd: Read => rd.toStore == store; case _ => false }

  def stale(ts: HLCTimestamp) = Right(ReadResponse.CurrentState(rev, ref, ts, 5, Some(odata), Set(lockTx)))
  def current(ts: HLCTimestamp) = Right(ReadResponse.CurrentState(lockTxRev, ref, ts, 5, Some(odata), Set()))

  test("a store holding the lock of the revision another store returned is re-read at once") {
    val m = new RecordingClient
    val r = mkReader(m)
    val ts = HLCTimestamp.now
    val oldTs = HLCTimestamp(ts.asLong - 1000)

    r.receiveReadResponse(ReadResponse(client, ds0, readUUID, ts, stale(oldTs)))
    reads(m, ds0) shouldBe 0
    r.receiveReadResponse(ReadResponse(client, ds1, readUUID, ts, current(ts)))
    // Stores only apply committed transactions, so ds1's revision shows the lock holder committed
    reads(m, ds0) shouldBe 1
    r.readResult.isCompleted shouldBe false

    r.receiveReadResponse(ReadResponse(client, ds2, readUUID, ts, Left(ReadError.ObjectNotFound)))
    reads(m, ds0) shouldBe 1 // One re-read per stale reply

    r.receiveReadResponse(ReadResponse(client, ds0, readUUID, ts, current(ts)))
    Await.result(r.readResult, awaitDuration).revision shouldBe lockTxRev
  }

  test("a store holding a lock is re-read at the next reply once its transaction is known to have committed") {
    val m = new RecordingClient:
      override val txStatusCache: TransactionStatusCache = new TransactionStatusCache()
    val r = mkReader(m)
    val ts = HLCTimestamp.now

    r.receiveReadResponse(ReadResponse(client, ds0, readUUID, ts, stale(ts)))
    m.txStatusCache.transactionCommitted(lockTx)
    reads(m, ds0) shouldBe 0
    r.receiveReadResponse(ReadResponse(client, ds1, readUUID, ts, Left(ReadError.ObjectNotFound)))
    reads(m, ds0) shouldBe 1
  }

  def lastRead(m: RecordingClient, store: StoreId): Read =
    m.requests.collect { case rd: Read if rd.toStore == store => rd }.head

  test("a re-read for a known-committed lock names that transaction") {
    val m = new RecordingClient:
      override val txStatusCache: TransactionStatusCache = new TransactionStatusCache()
    val r = mkReader(m)
    val ts = HLCTimestamp.now

    r.begin()
    m.requests.collect { case rd: Read => rd }.foreach(_.awaitCommit shouldBe Set())

    m.txStatusCache.transactionCommitted(lockTx)
    r.receiveReadResponse(ReadResponse(client, ds0, readUUID, ts, stale(ts)))
    lastRead(m, ds0).awaitCommit shouldBe Set(lockTx)
    lastRead(m, ds1).awaitCommit shouldBe Set()
  }

  test("a re-read of a lock that became known committed after the reply names that transaction") {
    val m = new RecordingClient:
      override val txStatusCache: TransactionStatusCache = new TransactionStatusCache()
    val r = mkReader(m)
    val ts = HLCTimestamp.now

    r.receiveReadResponse(ReadResponse(client, ds0, readUUID, ts, stale(ts)))
    m.txStatusCache.transactionCommitted(lockTx)
    r.receiveReadResponse(ReadResponse(client, ds1, readUUID, ts, Left(ReadError.ObjectNotFound)))
    lastRead(m, ds0).awaitCommit shouldBe Set(lockTx)
  }

  // A commit voter that has not applied the object's allocation answers AllocationPending (transaction-requirements
  // 9.4): the allocation will apply, or abort and the store then answers ObjectNotFound. Neither the reply nor a
  // threshold of them ends the read, which would fail a read of an object the client was just told it allocated
  test("an AllocationPending reply re-reads that store at once and never ends the read") {
    val m = new RecordingClient
    val r = mkReader(m)
    val ts = HLCTimestamp.now

    r.receiveReadResponse(ReadResponse(client, ds0, readUUID, ts, Left(ReadError.AllocationPending)))
    reads(m, ds0) shouldBe 1
    r.receiveReadResponse(ReadResponse(client, ds1, readUUID, ts, Left(ReadError.AllocationPending)))
    reads(m, ds1) shouldBe 1
    r.receiveReadResponse(ReadResponse(client, ds2, readUUID, ts, Left(ReadError.AllocationPending)))
    reads(m, ds2) shouldBe 1
    r.readResult.isCompleted shouldBe false

    r.receiveReadResponse(ReadResponse(client, ds0, readUUID, ts, current(ts)))
    r.receiveReadResponse(ReadResponse(client, ds1, readUUID, ts, current(ts)))
    Await.result(r.readResult, awaitDuration).revision shouldBe lockTxRev
  }

  test("a store holding the lock of an undecided transaction is not re-read") {
    val m = new RecordingClient
    val r = mkReader(m)
    val ts = HLCTimestamp.now

    r.receiveReadResponse(ReadResponse(client, ds0, readUUID, ts, stale(ts)))
    r.receiveReadResponse(ReadResponse(client, ds1, readUUID, ts, Left(ReadError.ObjectNotFound)))
    m.requests shouldBe Nil
  }

  // A blind KV insert leaves the object's revision unchanged: only the key's revision on a store that applied it shows
  // that the insert committed. A store still holding its lock must be re-read at once, not at the next full resend
  val insertTx = TransactionId(new UUID(0, 7))

  def kvReply(withKey: Boolean, locks: Set[TransactionId] = Set()) =
    val ts = HLCTimestamp.now
    Right(ReadResponse.CurrentState(rev, ref, ts, 5,
      if withKey then kvData(ObjectRevision(insertTx), ts) else Some(KVObjectState.encode(None, None, None, None, Map())),
      locks))

  def hasKey(r: BaseReadDriver): Boolean =
    Await.result(r.readResult, awaitDuration).asInstanceOf[KeyValueObjectState].contents.contains(kvKey)

  test("a store holding the lock of a KV insert another store has applied is re-read at once") {
    val m = new RecordingClient
    val r = mkReader(m, objectPointer = kvptr)
    val ts = HLCTimestamp.now

    r.receiveReadResponse(ReadResponse(client, ds0, readUUID, ts, kvReply(withKey = true)))
    r.receiveReadResponse(ReadResponse(client, ds1, readUUID, ts, kvReply(withKey = false, locks = Set(insertTx))))
    reads(m, ds1) shouldBe 1
    r.readResult.isCompleted shouldBe false

    r.receiveReadResponse(ReadResponse(client, ds1, readUUID, ts, kvReply(withKey = true)))
    hasKey(r) shouldBe true
  }

  test("a store holding the lock of a KV insert is re-read once another store's reply shows the insert applied") {
    val m = new RecordingClient
    val r = mkReader(m, objectPointer = kvptr)
    val ts = HLCTimestamp.now

    r.receiveReadResponse(ReadResponse(client, ds1, readUUID, ts, kvReply(withKey = false, locks = Set(insertTx))))
    reads(m, ds1) shouldBe 0
    r.receiveReadResponse(ReadResponse(client, ds0, readUUID, ts, kvReply(withKey = true)))
    reads(m, ds1) shouldBe 1

    r.receiveReadResponse(ReadResponse(client, ds1, readUUID, ts, kvReply(withKey = true)))
    hasKey(r) shouldBe true
  }

  test("a lock summary reports every lock's count but only a few of their ids") {
    val few = Set.fill(BaseReadDriver.MaxLocksReported)(TransactionId(UUID.randomUUID()))
    val many = Set.fill(5000)(TransactionId(UUID.randomUUID()))

    BaseReadDriver.lockSummary(Set()) shouldBe "0[]"
    few.foreach(id => BaseReadDriver.lockSummary(few) should include(id.uuid.toString))

    val summary = BaseReadDriver.lockSummary(many)
    summary should startWith("5000[")
    summary should endWith(",...]")
    summary.length should be < 200
    Future.successful(succeed)
  }

  test("the test harness's read driver re-sends a read whose replies cannot be restored yet") {
    // A KV insert leaves the object's revision unchanged. A read racing it can find the key on one store of three
    // while another store holds the insert's lock, its commit queued behind an earlier one: the key can be neither
    // restored nor taken as deleted, and only a re-read gets replies that can be
    val readsSent = new java.util.concurrent.atomic.AtomicInteger()
    val m = new RecordingClient:
      override private[client] val messenger: Messenger = new Messenger:
        def sendClientRequest(msg: ClientRequest): Unit = msg match
          case _: Read => readsSent.incrementAndGet()
          case _ => ()
        def sendTransactionMessage(msg: TxMessage): Unit = ()
        def sendTransactionMessages(msg: List[TxMessage]): Unit = ()
        def sendHostMessage(msg: HostMessage): Unit = ()
        def dropCacheForStore(storeId: StoreId): Unit = ()
    val r = BaseReadDriver.noErrorRecoveryReadDriver(m, kvptr, ida, readUUID, "",
      FullObject())
    val insertTx = TransactionId(new UUID(0, 7))
    val ts = HLCTimestamp.now
    def kvs(withKey: Boolean, locks: Set[TransactionId] = Set()) = Right(ReadResponse.CurrentState(rev, ref, ts, 5,
      if withKey then kvData(ObjectRevision(insertTx), ts) else Some(KVObjectState.encode(None, None, None, None, Map())),
      locks))

    r.receiveReadResponse(ReadResponse(client, ds0, readUUID, ts, kvs(withKey = true)))
    r.receiveReadResponse(ReadResponse(client, ds1, readUUID, ts, kvs(withKey = false, locks = Set(insertTx))))
    r.receiveReadResponse(ReadResponse(client, ds2, readUUID, ts, kvs(withKey = false)))
    r.readResult.isCompleted shouldBe false

    val deadline = System.nanoTime() + 5.seconds.toNanos
    while readsSent.get < ida.width && System.nanoTime() < deadline do
      Thread.sleep(5)
    readsSent.get should be >= ida.width

    // By the re-read the stores have applied the insert
    r.receiveReadResponse(ReadResponse(client, ds1, readUUID, ts, kvs(withKey = true)))
    Await.result(r.readResult, 1.second).asInstanceOf[KeyValueObjectState].contents.contains(kvKey) shouldBe true
  }

  // Settled reads (BaseReadDriver.settle)

  private def settledReader(m: ReadDriverClient) =
    val r = mkReader(m)
    r.settle()
    r

  private val t = TransactionId(new UUID(0, 7))
  private val later = TransactionId(new UUID(0, 8))
  private val tRev = ObjectRevision(t)

  private def reply(r: BaseReadDriver, store: StoreId, revision: ObjectRevision, locks: Set[TransactionId]): Unit =
    val ts = HLCTimestamp.now
    r.receiveReadResponse(ReadResponse(client, store, readUUID, ts,
      Right(ReadResponse.CurrentState(revision, ref, ts, 4, Some(odata), locks))))

  private def awaitedBy(m: RecordingClient): List[(StoreId, Set[TransactionId])] =
    m.requests.collect { case rd: Read => rd.toStore -> rd.awaitCommit }

  test("a settled read withholds a reply holding locks and re-reads it awaiting them") {
    // T committed (two of three stores voted commit) but nothing applied it, and this client does not know it
    // committed: an ordinary read would return the revision T replaces, from these three replies
    val m = new RecordingClient
    val r = settledReader(m)
    reply(r, ds0, rev, Set(t))
    reply(r, ds1, rev, Set(t))
    reply(r, ds2, rev, Set())
    r.readResult.isCompleted should be (false)
    awaitedBy(m).toSet should be (Set(ds0 -> Set(t), ds1 -> Set(t)))

    // Once the stores apply T, their parked re-reads are answered with it
    reply(r, ds0, tRev, Set())
    r.readResult.isCompleted should be (false)
    reply(r, ds1, tRev, Set())
    r.readResult.value.get.get.revision should be (tRev)
  }

  test("an ordinary read returns the replaced revision from the same replies") {
    val r = mkReader(ReadDriverClient.NoOp(client))
    reply(r, ds0, rev, Set(t))
    reply(r, ds1, rev, Set(t))
    r.readResult.value.get.get.revision should be (rev)
  }

  test("a settled read does not wait for locks a store takes after its first reply") {
    val m = new RecordingClient
    val r = settledReader(m)
    reply(r, ds0, rev, Set(t))
    reply(r, ds1, tRev, Set())
    // ds0 has released t but meanwhile locked `later`, which its first reply did not hold: settled
    reply(r, ds0, tRev, Set(later))
    r.readResult.value.get.get.revision should be (tRev)
    awaitedBy(m) should be (List(ds0 -> Set(t)))
  }
}
