package org.aspen_ddp.aspen.vdisk.tcmu.dbus

import org.aspen_ddp.aspen.client.AspenClient
import org.aspen_ddp.aspen.vdisk.DiskCatalog
import org.aspen_ddp.aspen.vdisk.VDiskError.DiskNotFound
import org.aspen_ddp.aspen.vdisk.tcmu.Cfgstring
import org.aspen_ddp.aspen.vdisk.tcmu.sys.Linux
import org.freedesktop.dbus.DBusPath
import org.freedesktop.dbus.connections.impl.{DBusConnection, DBusConnectionBuilder}
import org.freedesktop.dbus.interfaces.{DBus, ObjectManager}
import org.freedesktop.dbus.types.{UInt32, UInt64, Variant}

import java.util as ju
import scala.concurrent.duration.*
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.control.NonFatal

/** What the org.kernel.TCMUService1 handler object answers, independent of the bus. */
final class HandlerLogic(diskExists: String => Future[Boolean]):

  val configDesc: String = Cfgstring.ConfigDesc

  /** targetcli passes the full cfgstring, `aspen/<disk>`. Blocks the calling (dbus-java dispatch) thread for up to
    * `timeout` while Aspen is asked whether the disk exists.
    */
  def checkConfig(cfg: String, timeout: FiniteDuration = 10.seconds): (Boolean, String) =
    Cfgstring.parse(cfg) match
      case Left(reason) => (false, reason)
      case Right(disk) =>
        try
          if Await.result(diskExists(disk), timeout) then
            (true, "success")
          else
            (false, s"disk $disk not found")
        catch
          case NonFatal(e) => (false, s"could not reach Aspen: ${Option(e.getMessage).getOrElse(e.toString)}")

  def changeMedium(name: String, size: Long, cfg: String): (Int, String) =
    (-Linux.EOPNOTSUPP, "ChangeMedium is not supported by vdisk-server")

object HandlerLogic:
  /** Disk existence as recorded in the `DiskCatalog`. */
  def diskExistsIn(client: AspenClient): String => Future[Boolean] =
    diskExistsVia(DiskCatalog.load(client, _))(using client.clientContext)

  /** True when `load` succeeds, false when it fails with `DiskNotFound`; any other failure is passed on. */
  def diskExistsVia(load: String => Future[Any])(using ExecutionContext): String => Future[Boolean] = name =>
    load(name).map(_ => true).recover { case _: DiskNotFound => false }

/** The `org.kernel.TCMUService1` service tcmu-runner provides, so that targetcli can create user backstores. */
final class TcmuDbusService private (conn: DBusConnection):
  def close(): Unit = conn.close()

object TcmuDbusService:
  val BusName = "org.kernel.TCMUService1"
  val ManagerPath = "/org/kernel/TCMUService1"
  val HandlerPath = s"$ManagerPath/aspen"

  /** The handler object, as tcmu-runner's dbus_export_handler exports it for the `aspen` subtype. */
  final class HandlerObject(logic: HandlerLogic) extends TcmuService1:
    override def getObjectPath: String = HandlerPath

    // Runs on a dbus-java dispatch thread, which HandlerLogic.checkConfig blocks for up to its timeout.
    override def CheckConfig(configstr: String): TcmuService1.Reply[java.lang.Boolean, String] =
      val (ok, message) = logic.checkConfig(configstr)
      if !ok then
        scribe.info(s"CheckConfig($configstr): $message")
      TcmuService1.Reply(ok, message)

    override def ChangeMedium(name: String, size: UInt64, cfgstring: String): TcmuService1.Reply[Integer, String] =
      val (result, message) = logic.changeMedium(name, size.longValue, cfgstring)
      TcmuService1.Reply(result, message)

    override def getConfigDesc: String = logic.configDesc

  /** The ObjectManager targetcli enumerates handlers with. Its value for the handler is what GetAll returns. */
  final class ManagerObject(handler: HandlerObject) extends ObjectManager:
    override def getObjectPath: String = ManagerPath

    override def GetManagedObjects(): ju.Map[DBusPath, ju.Map[String, ju.Map[String, Variant[?]]]] =
      val props = ju.Map.of[String, Variant[?]]("ConfigDesc", Variant(handler.getConfigDesc))
      ju.Map.of(DBusPath(HandlerPath), ju.Map.of(BusName, props))

  /** Connects to the system bus, exports the objects and takes the name, failing if another process owns it. */
  def start(logic: HandlerLogic): TcmuDbusService =
    // Strong export references: the connection keeps the exported objects alive.
    val conn = DBusConnectionBuilder.forSystemBus().withShared(false).withExportWeakReferences(false).build()
    try
      val handler = HandlerObject(logic)
      val manager = ManagerObject(handler)
      conn.exportObject(manager)
      conn.exportObject(handler)
      val bus = conn.getRemoteObject("org.freedesktop.DBus", "/org/freedesktop/DBus", classOf[DBus])
      // Neither replace an owner nor queue behind one: that would be a running tcmu-runner.
      val reply = bus.RequestName(BusName, UInt32(DBus.DBUS_NAME_FLAG_DO_NOT_QUEUE)).intValue
      if reply != DBus.DBUS_REQUEST_NAME_REPLY_PRIMARY_OWNER && reply != DBus.DBUS_REQUEST_NAME_REPLY_ALREADY_OWNER then
        throw IllegalStateException(s"$BusName is already owned on the system bus (is tcmu-runner running?)")
      scribe.info(s"Serving $BusName on the system bus")
      TcmuDbusService(conn)
    catch
      case NonFatal(e) =>
        try conn.close() catch case NonFatal(_) => ()
        throw e
