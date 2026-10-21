package org.aspen_ddp.aspen.server

import com.sun.net.httpserver.{HttpExchange, HttpServer}
import org.aspen_ddp.aspen.common.util.DaemonThreads
import scribe.Logging

import java.net.InetSocketAddress
import java.nio.charset.StandardCharsets
import java.util.concurrent.Executors
import scala.concurrent.duration.*
import scala.concurrent.{Await, Future}
import scala.util.control.NonFatal

/** Serves the current bootstrap config yaml at GET /bootstrap.
 *
 *  The primary source (the Radicle copy) is preferred. If it fails or does not answer within
 *  primaryTimeout the on-disk fallback is used so the endpoint stays available even when the
 *  system is degraded.
 */
class BootstrapHttpServer(port: Int,
                          primary: () => Future[String],
                          fallback: () => String,
                          primaryTimeout: Duration = Duration(2, SECONDS)) extends Logging:

  private val executor = Executors.newFixedThreadPool(4, DaemonThreads.factory("bootstrap-http"))
  private var server: HttpServer = null

  /** Binds 0.0.0.0:port. Throws java.net.BindException if the port is taken. */
  def start(): Unit =
    val s = HttpServer.create(new InetSocketAddress(port), 0)
    s.createContext("/", handle)
    s.setExecutor(executor)
    s.start()
    server = s

  def boundPort: Int = server.getAddress.getPort

  def stop(): Unit =
    if server != null then
      server.stop(0)
    executor.shutdownNow()

  private def respond(ex: HttpExchange, status: Int, body: String, contentType: String): Unit =
    val bytes = body.getBytes(StandardCharsets.UTF_8)
    ex.getResponseHeaders.set("Content-Type", contentType)
    ex.sendResponseHeaders(status, if bytes.isEmpty then -1 else bytes.length)
    if bytes.nonEmpty then
      ex.getResponseBody.write(bytes)
    ex.close()

  private def handle(ex: HttpExchange): Unit =
    try
      if ex.getRequestURI.getPath != "/bootstrap" then
        respond(ex, 404, "", "text/plain; charset=utf-8")
      else if ex.getRequestMethod != "GET" then
        respond(ex, 405, "", "text/plain; charset=utf-8")
      else
        val content: Option[String] =
          try Some(Await.result(primary(), primaryTimeout))
          catch
            case NonFatal(t) =>
              logger.warn(s"Bootstrap config read from the Radicle failed; using the on-disk copy: $t")
              try Some(fallback())
              catch
                case NonFatal(t2) =>
                  logger.error(s"Bootstrap config on-disk fallback failed: $t2")
                  None
        content match
          case Some(yaml) => respond(ex, 200, yaml, "application/yaml; charset=utf-8")
          case None => respond(ex, 503, "Bootstrap config unavailable", "text/plain; charset=utf-8")
    catch
      case NonFatal(t) =>
        logger.warn(s"Error handling bootstrap request: $t")
        ex.close()
