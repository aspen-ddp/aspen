package org.aspen_ddp.aspen.client

import org.aspen_ddp.aspen.common.metadata.{BootstrapConfig, ClientConfig}

import java.net.URI
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.time.Duration as JDuration
import java.util.UUID
import scala.concurrent.duration.{Duration, SECONDS}
import scala.util.control.NonFatal

/** Fetches the bootstrap config over HTTP from the cached bootstrap hosts */
object BootstrapFetcher:

  case class Fetched(from: ClientConfig.BootstrapEndpoint, yaml: String, config: BootstrapConfig.Config)

  class NoReachableHosts(val attempts: List[(ClientConfig.BootstrapEndpoint, String)])
    extends Exception(
      ("No cached hosts could be reached:" ::
        attempts.map((ep, reason) => s"  $ep: $reason")).mkString("\n"))

  class SystemIdMismatch(val from: ClientConfig.BootstrapEndpoint, val expected: UUID, val actual: UUID)
    extends Exception(
      s"Host $from serves a bootstrap config for Aspen system $actual but this client is configured for system $expected")

  /** Tries bootstrapHost first, then each cached host. Returns the first config that parses.
    * A config belonging to a different Aspen system is a hard error and is never skipped.
    */
  def fetch(clientConfig: ClientConfig.Config,
            bootstrapHost: Option[ClientConfig.BootstrapEndpoint],
            timeout: Duration = Duration(5, SECONDS)): Fetched =
    val jtimeout = JDuration.ofMillis(timeout.toMillis)
    val http = HttpClient.newBuilder().connectTimeout(jtimeout).build()
    val candidates = (bootstrapHost.toList ++ clientConfig.bootstrapHosts).distinct

    var attempts = List[(ClientConfig.BootstrapEndpoint, String)]()

    for ep <- candidates do
      attempt(http, ep, jtimeout) match
        case Left(reason) => attempts = (ep, reason) :: attempts
        case Right((yaml, config)) =>
          if config.aspenSystemId != clientConfig.aspenSystemId then
            throw SystemIdMismatch(ep, clientConfig.aspenSystemId, config.aspenSystemId)
          return Fetched(ep, yaml, config)

    throw NoReachableHosts(attempts.reverse)

  private def attempt(http: HttpClient,
                      ep: ClientConfig.BootstrapEndpoint,
                      timeout: JDuration): Either[String, (String, BootstrapConfig.Config)] =
    try
      val req = HttpRequest.newBuilder(URI.create(s"http://${ep.address}:${ep.port}/bootstrap"))
        .timeout(timeout)
        .GET()
        .build()
      val resp = http.send(req, HttpResponse.BodyHandlers.ofString())
      if resp.statusCode() != 200 then
        Left(s"HTTP ${resp.statusCode()}")
      else
        try
          Right((resp.body(), BootstrapConfig.parseBootstrapConfig(resp.body())))
        catch
          case NonFatal(e) => Left(s"invalid bootstrap config: ${e.getMessage}")
    catch
      case _: java.net.http.HttpTimeoutException => Left("timed out")
      case e: InterruptedException =>
        Thread.currentThread().interrupt()
        Left("interrupted")
      case NonFatal(e) => Left(s"${e.getClass.getSimpleName}: ${e.getMessage}")
