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
import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.{FileVisitResult, Files, Path, SimpleFileVisitor}
import java.util.UUID
import java.util.concurrent.ExecutorService
import scala.concurrent.duration.{Duration, SECONDS}
import scala.concurrent.{Await, ExecutionContext}
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
    * Drawn from below Linux's default ephemeral range (32768-60999) rather than by binding port 0. Every other QUIC
    * suite finds its ports by binding 0, and every QuicNet binds its client channels to 0, all from that range and
    * possibly concurrently with this suite. Probing outside it keeps this suite from taking a port another suite has
    * just probed, and from having its own taken by another suite's ephemeral bind.
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

  private def deleteTree(root: Path): Unit =
    Files.walkFileTree(root, new SimpleFileVisitor[Path]:
      override def visitFile(file: Path, attrs: BasicFileAttributes): FileVisitResult =
        Files.deleteIfExists(file)
        FileVisitResult.CONTINUE

      override def postVisitDirectory(dir: Path, err: java.io.IOException): FileVisitResult =
        Files.deleteIfExists(dir)
        FileVisitResult.CONTINUE
    )

  /** Bootstraps a system into a temp directory, starts its host, opens a client, runs `fn`, and tears it all down */
  private def withSystem(fn: AspenClient => Unit): Unit =
    val tmp = Files.createTempDirectory("aspen-quic-e2e")
    try
      val (dataPort, httpPort, storeTransferPort) = freePorts()
      Console.withOut(new java.io.ByteArrayOutputStream):
        Main.bootstrap(Replication(3, 2), tmp, "127.0.0.1", dataPort, httpPort, storeTransferPort) shouldBe 0

      val host = Main.startHost(tmp.resolve("bootstrap-host"))
      try
        host.network shouldBe a [QuicNet]

        val session = Main.openClientSession(tmp.resolve("aspen-client-config.yaml"), None)
        val (client, network, _) = Main.createAspenClient(session)
        try
          network shouldBe a [QuicNet]
          network.startIoThread(client)
          fn(client)
        finally
          stopClient(client, network)
      finally
        host.stop()
    finally
      deleteTree(tmp)

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
    val key = Key(UUID.randomUUID())

    given tx: Transaction = client.newTransaction()

    val f = for
      pool <- client.getStoragePool(Radicle.poolId)
      dp <- pool.allocator.allocateDataObject(DataBuffer(data))
      _ = tx.update(Radicle.pointer, None, None,
                    List(KeyValueUpdate.DoesNotExist(key)),
                    List(Insert(key, dp.toArray)))
      _ <- tx.commit()
      kvos <- client.read(Radicle.pointer)
      dos <- client.read(dp)
    yield (dp, kvos, dos)

    val (dp, kvos, dos) = Await.result(f, Timeout)

    kvos.contents.get(key).map(_.value.bytes.toSeq) shouldBe Some(dp.toArray.toSeq)
    dos.data.size shouldBe size
    java.util.Arrays.equals(dos.data.getByteArray, data) shouldBe true

  test("end-to-end transaction over QUIC"):
    roundTrip(1024 * 1024)

  test("end-to-end over QUIC with a 32 MiB object"):
    roundTrip(32 * 1024 * 1024)
