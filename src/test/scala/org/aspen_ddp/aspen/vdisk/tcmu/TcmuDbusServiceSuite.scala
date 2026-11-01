package org.aspen_ddp.aspen.vdisk.tcmu

import org.aspen_ddp.aspen.vdisk.tcmu.dbus.{HandlerLogic, TcmuDbusService}
import org.freedesktop.dbus.Marshalling
import org.freedesktop.dbus.messages.ExportedObject
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.concurrent.duration.*
import scala.concurrent.{Future, Promise}
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
