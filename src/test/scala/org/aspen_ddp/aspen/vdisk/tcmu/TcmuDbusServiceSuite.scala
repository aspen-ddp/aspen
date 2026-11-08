package org.aspen_ddp.aspen.vdisk.tcmu

import org.aspen_ddp.aspen.vdisk.VDiskError.DiskNotFound
import org.aspen_ddp.aspen.vdisk.tcmu.dbus.{HandlerLogic, TcmuDbusService}
import org.freedesktop.dbus.Marshalling
import org.freedesktop.dbus.messages.ExportedObject
import org.scalatest.funsuite.AnyFunSuite
import org.slf4j.LoggerFactory
import org.slf4j.helpers.{NOPLogger, SubstituteLoggerFactory}
import org.scalatest.matchers.should.Matchers

import scala.concurrent.duration.*
import scala.concurrent.{Await, ExecutionContext, Future, Promise}
import scala.jdk.CollectionConverters.*

class TcmuDbusServiceSuite extends AnyFunSuite with Matchers:

  private def logic(disks: String*): HandlerLogic =
    HandlerLogic(name => Future.successful(disks.contains(name)))

  test("valid existing disk"):
    logic("vd1").checkConfig("aspen/vd1") should be ((true, "success"))

  test("parse error is reported"):
    logic("vd1").checkConfig("file/vd1") should be ((false, "cfgstring must start with aspen/"))

  test("missing disk is reported"):
    logic("vd1").checkConfig("aspen/vd2") should be ((false, "disk vd2 not found"))

  test("a failed lookup reports Aspen unreachable"):
    val l = HandlerLogic(_ => Future.failed(new RuntimeException("boom")))
    l.checkConfig("aspen/vd1") should be ((false, "could not reach Aspen: boom"))

  test("a lookup that never completes times out with a message"):
    val l = HandlerLogic(_ => Promise[Boolean]().future)
    val (ok, msg) = l.checkConfig("aspen/vd1", timeout = 100.millis)
    ok should be (false)
    msg should startWith ("could not reach Aspen: ")

  test("ChangeMedium is refused"):
    logic("vd1").changeMedium("so1", 1024L, "aspen/vd1") should be ((-95, "ChangeMedium is not supported by vdisk-server"))

  test("disk existence maps DiskNotFound to false and passes other failures on"):
    given ExecutionContext = ExecutionContext.parasitic
    val boom = new RuntimeException("boom")
    val exists = HandlerLogic.diskExistsVia:
      case "vd1" => Future.successful(())
      case "gone" => Future.failed(DiskNotFound("gone"))
      case _ => Future.failed(boom)
    Await.result(exists("vd1"), 1.second) should be (true)
    Await.result(exists("gone"), 1.second) should be (false)
    Await.result(exists("other").failed, 1.second) should be (boom)

  test("a failure without a message is still described"):
    val l = HandlerLogic(_ => Future.failed(new RuntimeException()))
    l.checkConfig("aspen/vd1") should be ((false, "could not reach Aspen: java.lang.RuntimeException"))

  test("dbus-java's slf4j logging reaches scribe"):
    // dbus-java brings slf4j-api 2.x, which needs a 2.x provider (scribe-slf4j2), or every slf4j logger is a no-op.
    // While another thread is initializing slf4j (suites run in parallel) it hands out its SubstituteLoggerFactory,
    // so wait for that initialization to end first
    val deadline = System.nanoTime() + 10.seconds.toNanos
    while LoggerFactory.getILoggerFactory.isInstanceOf[SubstituteLoggerFactory] && System.nanoTime() < deadline do
      Thread.sleep(1)
    LoggerFactory.getILoggerFactory.getClass.getName should startWith ("scribe.")
    LoggerFactory.getLogger(classOf[ExportedObject]) should not be a [NOPLogger]

  test("ConfigDesc is the cfgstring description"):
    logic().configDesc should be (Cfgstring.ConfigDesc)

  test("the exported objects match what targetcli expects"):
    val handler = TcmuDbusService.HandlerObject(logic("vd1"))
    handler.getObjectPath should be ("/org/kernel/TCMUService1/aspen")
    val exported = ExportedObject(handler, false)
    val methods = exported.getMethods.keySet.asScala.map(t => (t.getName, t.getSig))
    methods should contain (("CheckConfig", "s"))
    methods should contain (("ChangeMedium", "sts"))
    val xml = exported.getIntrospectiondata
    xml should include ("""<interface name="org.kernel.TCMUService1">""")
    xml should include ("""<property name="ConfigDesc" type="s" access="read" />""")
    // The reply signature as dbus-java computes it when replying. (Its introspection lists tuple outputs twice.)
    def replySig(name: String): String =
      Marshalling.getDBusType(exported.getMethods.asScala.collectFirst {
        case (t, m) if t.getName == name => m
      }.get.getGenericReturnType).mkString
    replySig("CheckConfig") should be ("bs")
    replySig("ChangeMedium") should be ("is")

    val manager = TcmuDbusService.ManagerObject(handler)
    manager.getObjectPath should be ("/org/kernel/TCMUService1")
    ExportedObject(manager, false).getIntrospectiondata should include (
      """<interface name="org.freedesktop.DBus.ObjectManager">""")
    val objects = manager.GetManagedObjects().asScala.map((p, ifs) => (p.getPath, ifs))
    objects.keySet should be (Set("/org/kernel/TCMUService1/aspen"))
    val props = objects("/org/kernel/TCMUService1/aspen").get("org.kernel.TCMUService1")
    props.get("ConfigDesc").getValue should be (Cfgstring.ConfigDesc)
    props.get("ConfigDesc").getSig should be ("s")
