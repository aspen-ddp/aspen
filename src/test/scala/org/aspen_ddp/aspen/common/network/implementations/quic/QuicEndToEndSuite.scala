package org.aspen_ddp.aspen.common.network.implementations.quic

import io.netty.util.ResourceLeakDetector
import org.aspen_ddp.aspen.client.{AspenClient, Transaction}
import org.aspen_ddp.aspen.cmdline.Main
import org.aspen_ddp.aspen.common.{DataBuffer, Radicle}
import org.aspen_ddp.aspen.common.ida.Replication
import org.aspen_ddp.aspen.common.network.NetworkPlugin
import org.aspen_ddp.aspen.common.objects.{Insert, Key}
import org.aspen_ddp.aspen.common.transaction.KeyValueUpdate
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.net.{DatagramSocket, ServerSocket}
import java.util.UUID
import java.util.concurrent.ExecutorService
import scala.concurrent.duration.{Duration, SECONDS}
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.Random

/** A bootstrapped single-host system, started the way the `host` command starts one, with a client built the way
  * every client command builds one. Both use QUIC because it is the CLI's default.
  */
class QuicEndToEndSuite extends AnyFunSuite with Matchers with BeforeAndAfterAll:

  // Generous: a read issued right after commit can find the last store not yet holding the object, and the client
  // then waits out its read retry delay (10 s) before trying again. Observed worst case is under 20 s.
  private val Timeout = Duration(120, SECONDS)

  private var previousLevel: ResourceLeakDetector.Level = scala.compiletime.uninitialized
  private var leakWatch: QuicTestPeer.LeakWatch = scala.compiletime.uninitialized

  override def beforeAll(): Unit =
    previousLevel = ResourceLeakDetector.getLevel
    ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.PARANOID)
    leakWatch = QuicTestPeer.watchLeaks()

  override def afterAll(): Unit =
    try
      leakWatch.leaks() shouldBe Nil
    finally
      leakWatch.stop()
      ResourceLeakDetector.setLevel(previousLevel)

  // ---------------------------------------------------------------------------------------------------------------

  /** Three distinct ports, each free for both UDP and TCP at the time of the call, for data, HTTP and store transfer.
    *
    * Drawn from 20000-31999, below Linux's default ephemeral range (32768-60999) rather than by binding port 0: every
    * QuicNet binds its client channels to 0, from that range and possibly concurrently with this suite. The other
    * QUIC suites draw from 10000-19999 (`QuicNetFixture.freeUdpPort`), so the two never overlap.
    */
  private def freePorts(): (Int, Int, Int) =
    val rng = new Random()
    def isFree(port: Int): Boolean =
      try
        val u = new DatagramSocket(port)
        try
          val t = new ServerSocket(port)
          t.close()
          true
        finally u.close()
      catch case _: java.io.IOException => false
    def pick(): Int =
      var port = 20000 + rng.nextInt(12000)
      while !isFree(port) do
        port = 20000 + rng.nextInt(12000)
      port
    var ports = (pick(), pick(), pick())
    while Set(ports._1, ports._2, ports._3).size != 3 do
      ports = (pick(), pick(), pick())
    ports

  /** Bootstraps a system into a temp directory, starts its host, opens a client, runs `fn`, and tears it all down */
  private def withSystem(fn: AspenClient => Unit): Unit =
    val tmp = os.temp.dir(prefix = "aspen-quic-e2e")
    try
      val (dataPort, httpPort, storeTransferPort) = freePorts()
      Console.withOut(new java.io.ByteArrayOutputStream):
        Main.bootstrap(Replication(3, 2), tmp / "bootstrap-host", "bootstrap-host", "127.0.0.1",
                       dataPort, httpPort, storeTransferPort, tmp / "aspen-client-config.yaml") shouldBe 0

      val host = Main.startHost(tmp / "bootstrap-host")
      try
        host.network shouldBe a [QuicNet]

        val session = Main.openClientSession(tmp / "aspen-client-config.yaml", None)
        val (client, network, _) = Main.createAspenClient(session)
        try
          network shouldBe a [QuicNet]
          network.asInstanceOf[QuicNet].eventLoopsForTests.size shouldBe 1 // a client command's single loop
          network.startIoThread(client)
          fn(client)
        finally
          stopClient(client, network)
      finally
        host.stop()
    finally
      os.remove.all(tmp)

  private def stopClient(client: AspenClient, network: NetworkPlugin): Unit =
    network.shutdown(Duration.Zero)
    client.shutdown()
    client.clientContext match
      case es: ExecutorService => es.shutdownNow()
      case _ => ()

  /** Allocates a DataObject holding `size` bytes, inserting its pointer into the Radicle in the same transaction, then
    * reads both back
    */
  private def roundTrip(size: Int): Unit = withSystem: client =>
    given ExecutionContext = client.clientContext

    val data = new Array[Byte](size)
    new Random(size).nextBytes(data)

    val start = System.nanoTime()
    @volatile var committed = 0L
    // The host updates the Radicle too, so a commit can collide with it and abort. Each attempt allocates the whole
    // object again, so the retries are few, and each uses a key of its own in case an earlier one did commit.
    def attempt(n: Int): Future[(Key, org.aspen_ddp.aspen.common.objects.DataObjectPointer)] =
      val key = Key(UUID.randomUUID())
      client.transact: tx =>
        given Transaction = tx
        for
          pool <- client.getStoragePool(Radicle.poolId)
          dp <- pool.allocator.allocateDataObject(DataBuffer(data))
        yield
          tx.update(Radicle.pointer, None, None,
                    List(KeyValueUpdate.DoesNotExist(key)),
                    List(Insert(key, dp.toArray)))
          (key, dp)
      .map: r =>
        committed = System.nanoTime()
        r
      .recoverWith:
        case _: org.aspen_ddp.aspen.client.TransactionAborted if n < 5 => attempt(n + 1)
    val f = attempt(1)

    val (key, dp) = Await.result(f, Timeout)
    // Commit returns once the outcome is decided, so a read right after it can reach stores that have yet to apply it:
    // the new object is then not found, and the Radicle lacks the key. Read again until both are seen.
    val deadline = System.nanoTime() + Timeout.toNanos
    def readUntil[T](what: String)(read: => Future[T])(done: T => Boolean): T =
      def tryRead(): Either[Throwable, T] =
        try Right(Await.result(read, Timeout)) catch case e: Exception => Left(e)
      var last = tryRead()
      while !last.exists(done) && System.nanoTime() < deadline do
        Thread.sleep(10)
        last = tryRead()
      last.fold(e => throw e, identity)
    val kvos = readUntil("the Radicle")(client.read(Radicle.pointer))(_.contents.contains(key))
    val dos = readUntil("the data object")(client.read(dp))(_ => true)
    val end = System.nanoTime()
    info(s"commit ${(committed - start) / 1000000} ms, reads ${(end - committed) / 1000000} ms")

    kvos.contents.get(key).map(_.value.bytes.toSeq) shouldBe Some(dp.toArray.toSeq)
    dos.data.size shouldBe size
    java.util.Arrays.equals(dos.data.getByteArray, data) shouldBe true

  test("end-to-end transaction over QUIC"):
    roundTrip(1024 * 1024)

  test("end-to-end over QUIC with a 32 MiB object"):
    roundTrip(32 * 1024 * 1024)
