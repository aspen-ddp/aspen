package org.aspen_ddp.aspen.client

import org.aspen_ddp.aspen.TestTLS
import org.aspen_ddp.aspen.common.ida.Replication
import org.aspen_ddp.aspen.common.metadata.ClientConfig.BootstrapEndpoint
import org.aspen_ddp.aspen.common.metadata.{BootstrapConfig, ClientConfig, HostId, HostState}
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.server.BootstrapHttpServer
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.net.ServerSocket
import java.util.UUID
import scala.concurrent.Future
import scala.concurrent.duration.*

class BootstrapFetcherSuite extends AnyFunSuite with Matchers:

  private val systemId = UUID.fromString("22222222-2222-2222-2222-222222222222")
  private val otherId = UUID.fromString("33333333-3333-3333-3333-333333333333")

  private def yamlFor(id: UUID, hostName: String): String =
    val poolId = PoolId(UUID.fromString("44444444-4444-4444-4444-444444444444"))
    val state = HostState(HostId(new UUID(0, 1)), "node_0", hostName, 5000, 4751, 5002, Set())
    BootstrapConfig.generateBootstrapConfig(
      id, Replication(1, 1), List(state), List(StoreId(poolId, 0.toByte) -> state.hostId), TestTLS.caCertificatePem)

  private def withServer[T](body: () => Future[String])(f: BootstrapEndpoint => T): T =
    val server = new BootstrapHttpServer(0, body, () => "disk")
    server.start()
    try f(BootstrapEndpoint("127.0.0.1", server.boundPort))
    finally server.stop()

  private def serving(yaml: String) = () => Future.successful(yaml)

  private def deadEndpoint(): BootstrapEndpoint =
    val ss = new ServerSocket(0)
    val port = ss.getLocalPort
    ss.close()
    BootstrapEndpoint("127.0.0.1", port)

  private def cfg(hosts: List[BootstrapEndpoint]) = ClientConfig.Config(systemId, TestTLS.client, hosts)

  test("tries --bootstrap-host before the cached hosts"):
    withServer(serving(yamlFor(systemId, "host-a"))): a =>
      withServer(serving(yamlFor(systemId, "host-b"))): b =>
        val r = BootstrapFetcher.fetch(cfg(List(b)), Some(a))
        r.from should be (a)
        r.config.hosts.head.address should be ("host-a")

  test("falls through an unreachable cached host to the next one"):
    withServer(serving(yamlFor(systemId, "host-b"))): good =>
      val r = BootstrapFetcher.fetch(cfg(List(deadEndpoint(), good)), None)
      r.from should be (good)

  test("treats a non-Aspen HTTP response as a failed attempt"):
    withServer(serving("<html>not aspen</html>")): bad =>
      withServer(serving(yamlFor(systemId, "host-b"))): good =>
        BootstrapFetcher.fetch(cfg(List(bad, good)), None).from should be (good)

  test("a host that accepts but never answers is bounded by the timeout"):
    val silent = new ServerSocket(0)
    try
      val sep = BootstrapEndpoint("127.0.0.1", silent.getLocalPort)
      withServer(serving(yamlFor(systemId, "host-b"))): good =>
        val start = System.nanoTime()
        val r = BootstrapFetcher.fetch(cfg(List(sep, good)), None, 300.millis)
        val elapsed = (System.nanoTime() - start).nanos
        r.from should be (good)
        elapsed should be < 30.seconds // bounded: a silent host would otherwise block forever
    finally silent.close()

  test("reports every attempt when no cached host can be reached"):
    val d1 = deadEndpoint()
    val d2 = deadEndpoint()
    val e = intercept[BootstrapFetcher.NoReachableHosts]:
      BootstrapFetcher.fetch(cfg(List(d1)), Some(d2))
    e.getMessage should startWith ("No cached hosts could be reached")
    e.getMessage should include (d1.toString)
    e.getMessage should include (d2.toString)
    e.attempts.map(_._1) should be (List(d2, d1))
    val lines = e.getMessage.split("\n").toList
    lines.filter(_.contains("(--bootstrap-host)")) should be (List(lines.find(_.contains(d2.toString)).get))
    lines.count(_.contains(d1.toString)) should be (1)
    lines.find(_.contains(d1.toString)).get should not include "(--bootstrap-host)"

  test("rejects a bootstrap config from a different Aspen system"):
    withServer(serving(yamlFor(otherId, "host-a"))): a =>
      val e = intercept[BootstrapFetcher.SystemIdMismatch]:
        BootstrapFetcher.fetch(cfg(List(a)), None)
      e.getMessage should include (systemId.toString)
      e.getMessage should include (otherId.toString)
