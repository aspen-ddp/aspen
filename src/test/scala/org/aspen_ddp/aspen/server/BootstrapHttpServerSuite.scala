package org.aspen_ddp.aspen.server

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.net.URI
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import scala.concurrent.duration.*
import scala.concurrent.{Future, Promise}

class BootstrapHttpServerSuite extends AnyFunSuite with Matchers:

  private val client = HttpClient.newHttpClient()

  private def send(server: BootstrapHttpServer, path: String, method: String = "GET"): HttpResponse[String] =
    val builder = HttpRequest.newBuilder(URI.create(s"http://127.0.0.1:${server.boundPort}$path"))
    val req = method match
      case "POST" => builder.POST(HttpRequest.BodyPublishers.ofString("x")).build()
      case _ => builder.GET().build()
    client.send(req, HttpResponse.BodyHandlers.ofString())

  private def withServer[T](primary: () => Future[String],
                            fallback: () => String,
                            timeout: Duration = Duration(2, SECONDS))(f: BootstrapHttpServer => T): T =
    val server = new BootstrapHttpServer(0, primary, fallback, timeout)
    server.start()
    try f(server)
    finally server.stop()

  test("GET /bootstrap returns the primary content as yaml"):
    withServer(() => Future.successful("primary"), () => "disk") { s =>
      val r = send(s, "/bootstrap")
      r.statusCode() should be (200)
      r.body() should be ("primary")
      r.headers().firstValue("Content-Type").get() should be ("application/yaml; charset=utf-8")
    }

  test("falls back to the on-disk copy when the primary read fails"):
    withServer(() => Future.failed(Exception("x")), () => "disk") { s =>
      val r = send(s, "/bootstrap")
      r.statusCode() should be (200)
      r.body() should be ("disk")
    }

  test("falls back when the primary read times out"):
    withServer(() => Promise[String]().future, () => "disk", 200.millis) { s =>
      val start = System.nanoTime()
      val r = send(s, "/bootstrap")
      val elapsed = (System.nanoTime() - start).nanos
      r.body() should be ("disk")
      elapsed should be < 2.seconds
    }

  test("returns 503 when both sources fail"):
    withServer(() => Future.failed(Exception("x")), () => throw Exception("y")) { s =>
      send(s, "/bootstrap").statusCode() should be (503)
    }

  test("returns 404 for other paths"):
    withServer(() => Future.successful("primary"), () => "disk") { s =>
      send(s, "/other").statusCode() should be (404)
    }

  test("returns 405 for a POST to /bootstrap"):
    withServer(() => Future.successful("primary"), () => "disk") { s =>
      send(s, "/bootstrap", "POST").statusCode() should be (405)
    }

  test("start fails when the port is already in use"):
    val ss = new java.net.ServerSocket(0)
    try
      val server = new BootstrapHttpServer(ss.getLocalPort, () => Future.successful("p"), () => "d")
      a [java.net.BindException] should be thrownBy server.start()
    finally ss.close()
