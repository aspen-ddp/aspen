package org.aspen_ddp.aspen.vdisk.tcmu

import org.aspen_ddp.aspen.vdisk.tcmu.sys.{Libc, Linux, SysError}

import java.lang.foreign.{Arena, MemorySegment}
import java.lang.foreign.ValueLayout.{JAVA_BYTE, JAVA_INT, JAVA_SHORT}
import java.nio.{ByteBuffer, ByteOrder}
import java.nio.charset.StandardCharsets
import scala.collection.mutable.ListBuffer

/** The TCM-USER generic netlink family, from `/usr/include/linux/target_core_user.h`. */
object TcmuGenl:
  val AddedDevice = 1
  val RemovedDevice = 2
  val ReconfigDevice = 3
  val AddedDeviceDone = 4
  val RemovedDeviceDone = 5
  val ReconfigDeviceDone = 6
  val SetFeatures = 7

  val Device = 1
  val Minor = 2
  val DevCfg = 4
  val DevSize = 5
  val WriteCache = 6
  val CmdStatus = 7
  val DeviceId = 8
  val SuppKernCmdReply = 9

  val Family = "TCM-USER"
  val Group = "config"
  val Version = 2

enum ReconfigChange:
  case Size(bytes: Long)
  case Config(cfg: String)
  case WriteCache(on: Boolean)
  case Unknown

/** A device notification from the kernel. `device` is the configfs `dev_config` string ("aspen/..."). */
sealed trait TcmuEvent:
  def device: String
  def minor: Int
  def deviceId: Int

object TcmuEvent:
  final case class Added(device: String, minor: Int, deviceId: Int) extends TcmuEvent
  final case class Removed(device: String, minor: Int, deviceId: Int) extends TcmuEvent
  final case class Reconfig(device: String, minor: Int, deviceId: Int, change: ReconfigChange) extends TcmuEvent

/** Pure encoding and decoding of the generic netlink messages the TCMU frontend exchanges with the kernel. */
object GenlCodec:
  private val NlmsgHdrLen = 16
  private val GenlHdrLen = 4
  private val NlaHdrLen = 4

  val NLMSG_NOOP = 1
  val NLMSG_ERROR = 2
  val NLMSG_DONE = 3
  val NLMSG_OVERRUN = 4

  val NLM_F_REQUEST = 1
  val NLM_F_ACK = 4

  val GENL_ID_CTRL = 0x10
  private val CTRL_CMD_NEWFAMILY = 1
  private val CTRL_CMD_GETFAMILY = 3
  private val CTRL_ATTR_FAMILY_ID = 1
  private val CTRL_ATTR_FAMILY_NAME = 2
  private val CTRL_ATTR_MCAST_GROUPS = 7
  private val CTRL_ATTR_MCAST_GRP_NAME = 1
  private val CTRL_ATTR_MCAST_GRP_ID = 2

  private val NLA_TYPE_MASK = ~(0x8000 | 0x4000) & 0xffff

  private def align(n: Int): Int = (n + 3) & ~3

  private def order(b: ByteBuffer): ByteBuffer = b.order(ByteOrder.nativeOrder())

  /** An attribute to encode: type and unpadded payload. */
  private final case class Attr(tpe: Int, payload: Array[Byte])

  private def u8Attr(tpe: Int, v: Int) = Attr(tpe, Array(v.toByte))
  private def u32Attr(tpe: Int, v: Int) = Attr(tpe, order(ByteBuffer.allocate(4)).putInt(v).array())
  private def stringAttr(tpe: Int, s: String) = Attr(tpe, s.getBytes(StandardCharsets.UTF_8) :+ 0.toByte)

  private def message(tpe: Int, flags: Int, seq: Int, cmd: Int, version: Int, attrs: Attr*): Array[Byte] =
    val len = NlmsgHdrLen + GenlHdrLen + attrs.map(a => align(NlaHdrLen + a.payload.length)).sum
    val b = order(ByteBuffer.allocate(len))
    b.putInt(len).putShort(tpe.toShort).putShort(flags.toShort).putInt(seq).putInt(0)
    b.put(cmd.toByte).put(version.toByte).putShort(0)
    for a <- attrs do
      val start = b.position()
      b.putShort((NlaHdrLen + a.payload.length).toShort).putShort(a.tpe.toShort).put(a.payload)
      b.position(start + align(NlaHdrLen + a.payload.length))
    b.array()

  def getFamily(name: String, seq: Int): Array[Byte] =
    message(GENL_ID_CTRL, NLM_F_REQUEST, seq, CTRL_CMD_GETFAMILY, 1, stringAttr(CTRL_ATTR_FAMILY_NAME, name))

  def setFeatures(familyId: Int, seq: Int): Array[Byte] =
    message(familyId, NLM_F_REQUEST | NLM_F_ACK, seq, TcmuGenl.SetFeatures, TcmuGenl.Version,
      u8Attr(TcmuGenl.SuppKernCmdReply, 1))

  /** The `*_DONE` reply to an event. As in libtcmu, it asks for no ack: the kernel answers only on error. */
  def reply(familyId: Int, seq: Int, cmd: Int, status: Int, deviceId: Int): Array[Byte] =
    message(familyId, NLM_F_REQUEST, seq, cmd, TcmuGenl.Version,
      u32Attr(TcmuGenl.CmdStatus, status), u32Attr(TcmuGenl.DeviceId, deviceId))

  /** One netlink message: header fields and its payload (everything after the 16-byte header). */
  private final case class Msg(tpe: Int, flags: Int, seq: Int, payload: ByteBuffer)

  /** Splits a received buffer into messages. A malformed header ends the walk with a `Left`. */
  private def split(buf: Array[Byte]): List[Either[String, Msg]] =
    val b = order(ByteBuffer.wrap(buf))
    val out = ListBuffer.empty[Either[String, Msg]]
    var pos = 0
    var stop = false
    while !stop && pos < buf.length do
      if buf.length - pos < NlmsgHdrLen then
        out += Left(s"truncated netlink header: ${buf.length - pos} bytes at offset $pos")
        stop = true
      else
        val len = b.getInt(pos)
        if len < NlmsgHdrLen || len > buf.length - pos then
          out += Left(s"bad netlink message length $len at offset $pos (${buf.length - pos} bytes left)")
          stop = true
        else
          val payload = order(b.slice(pos + NlmsgHdrLen, len - NlmsgHdrLen))
          out += Right(Msg(b.getShort(pos + 4) & 0xffff, b.getShort(pos + 6) & 0xffff, b.getInt(pos + 8), payload))
          pos += align(len)
    out.toList

  /** The error code of an NLMSG_ERROR payload: 0 is an ack, otherwise a negative errno. */
  private def errorCode(m: Msg): Either[String, Int] =
    if m.payload.remaining < 4 then Left("truncated netlink error message")
    else Right(m.payload.getInt(0))

  /** Parses attributes from `b` (position 0 to limit). Later duplicates replace earlier ones. */
  private def attrs(b: ByteBuffer): Either[String, Map[Int, ByteBuffer]] =
    var result = Map.empty[Int, ByteBuffer]
    var pos = 0
    var error: Option[String] = None
    while error.isEmpty && b.limit() - pos >= NlaHdrLen do
      val len = b.getShort(pos) & 0xffff
      val tpe = b.getShort(pos + 2) & NLA_TYPE_MASK
      if len < NlaHdrLen || len > b.limit() - pos then
        error = Some(s"bad attribute length $len for type $tpe at offset $pos (${b.limit() - pos} bytes left)")
      else
        result += tpe -> order(b.slice(pos + NlaHdrLen, len - NlaHdrLen))
        pos += align(len)
    // Fewer than NlaHdrLen trailing bytes are ignored, as the kernel's nla_parse does.
    error.toLeft(result)

  private def genlAttrs(m: Msg): Either[String, (Int, Int, Map[Int, ByteBuffer])] =
    if m.payload.remaining < GenlHdrLen then Left(s"truncated genl header in message type ${m.tpe}")
    else
      val cmd = m.payload.get(0) & 0xff
      val version = m.payload.get(1) & 0xff
      attrs(order(m.payload.slice(GenlHdrLen, m.payload.remaining - GenlHdrLen)))
        .map(a => (cmd, version, a))
        .left.map(e => s"cmd $cmd: $e")

  /** An NLA_STRING payload, up to its NUL terminator if it has one. */
  private def string(b: ByteBuffer): String =
    val bytes = new Array[Byte](b.remaining)
    b.duplicate().get(bytes)
    val end = bytes.indexOf(0.toByte)
    new String(bytes, 0, if end < 0 then bytes.length else end, StandardCharsets.UTF_8)

  private def sized(name: String, b: ByteBuffer, size: Int): Either[String, ByteBuffer] =
    if b.remaining < size then Left(s"attribute $name has ${b.remaining} bytes, needs $size")
    else Right(b)

  private def u8(name: String, b: ByteBuffer) = sized(name, b, 1).map(_.get(0) & 0xff)
  private def u16(name: String, b: ByteBuffer) = sized(name, b, 2).map(_.getShort(0) & 0xffff)
  private def u32(name: String, b: ByteBuffer) = sized(name, b, 4).map(_.getInt(0))
  private def u64(name: String, b: ByteBuffer) = sized(name, b, 8).map(_.getLong(0))

  /** Parses the reply to `getFamily`: the family id and its multicast groups by name. */
  def parseFamily(msg: Array[Byte]): Either[String, (Int, Map[String, Int])] =
    split(msg).headOption match
      case None => Left("empty family reply")
      case Some(Left(e)) => Left(e)
      case Some(Right(m)) if m.tpe == NLMSG_ERROR =>
        errorCode(m).flatMap(c => Left(s"family lookup failed: errno ${-c}"))
      case Some(Right(m)) if m.tpe != GENL_ID_CTRL => Left(s"unexpected family reply type ${m.tpe}")
      case Some(Right(m)) =>
        for
          (cmd, _, a) <- genlAttrs(m)
          _ <- if cmd == CTRL_CMD_NEWFAMILY then Right(()) else Left(s"unexpected family reply cmd $cmd")
          idAttr <- a.get(CTRL_ATTR_FAMILY_ID).toRight("family reply without CTRL_ATTR_FAMILY_ID")
          id <- u16("CTRL_ATTR_FAMILY_ID", idAttr)
          groups <- a.get(CTRL_ATTR_MCAST_GROUPS) match
            case None => Right(Map.empty[String, Int])
            case Some(g) => parseGroups(g)
        yield (id, groups)

  private def parseGroups(b: ByteBuffer): Either[String, Map[String, Int]] =
    attrs(b).flatMap: entries =>
      entries.values.foldLeft[Either[String, Map[String, Int]]](Right(Map.empty)): (acc, entry) =>
        for
          m <- acc
          fields <- attrs(entry)
          nameAttr <- fields.get(CTRL_ATTR_MCAST_GRP_NAME).toRight("multicast group without a name")
          name = string(nameAttr)
          idAttr <- fields.get(CTRL_ATTR_MCAST_GRP_ID).toRight(s"multicast group $name without an id")
          id <- u32("CTRL_ATTR_MCAST_GRP_ID", idAttr)
        yield m + (name -> id)

  /** Parses every message in a received buffer. Acks, NLMSG_DONE and NLMSG_NOOP are skipped; anything that is not
    * a well-formed TCMU event, including a message `libtcmu.c:handle_netlink` would drop, is a `Left`.
    */
  def parseEvents(buf: Array[Byte], familyId: Int): List[Either[String, TcmuEvent]] =
    split(buf).flatMap:
      case Left(e) => List(Left(e))
      case Right(m) if m.tpe == NLMSG_NOOP || m.tpe == NLMSG_DONE => Nil
      case Right(m) if m.tpe == NLMSG_ERROR =>
        errorCode(m) match
          case Right(0) => Nil
          case Right(c) => List(Left(s"netlink error for seq ${m.seq}: errno ${-c}"))
          case Left(e) => List(Left(e))
      case Right(m) if m.tpe == NLMSG_OVERRUN => List(Left("netlink overrun"))
      case Right(m) if m.tpe != familyId => List(Left(s"unexpected netlink message type ${m.tpe}"))
      case Right(m) => List(genlAttrs(m).flatMap(event))

  private def event(cmd: Int, version: Int, a: Map[Int, ByteBuffer]): Either[String, TcmuEvent] =
    import TcmuGenl.*
    for
      _ <- if Set(AddedDevice, RemovedDevice, ReconfigDevice).contains(cmd) then Right(())
           else Left(s"unexpected TCM-USER cmd $cmd")
      (minorAttr, deviceAttr) <- a.get(Minor).zip(a.get(Device))
        .toRight(s"cmd $cmd without TCMU_ATTR_MINOR or TCMU_ATTR_DEVICE")
      minor <- u32("TCMU_ATTR_MINOR", minorAttr)
      device = string(deviceAttr)
      deviceId <- a.get(DeviceId) match
        case Some(d) => u32("TCMU_ATTR_DEVICE_ID", d)
        case None if version > 1 => Left(s"v$version cmd $cmd without TCMU_ATTR_DEVICE_ID")
        case None => Right(0)
      ev <- cmd match
        case AddedDevice => Right(TcmuEvent.Added(device, minor, deviceId))
        case RemovedDevice => Right(TcmuEvent.Removed(device, minor, deviceId))
        case _ => reconfigChange(a).map(TcmuEvent.Reconfig(device, minor, deviceId, _))
    yield ev

  /** As libtcmu's `reconfig_device`: the config string, then the size, then the write cache flag. */
  private def reconfigChange(a: Map[Int, ByteBuffer]): Either[String, ReconfigChange] =
    import TcmuGenl.*
    (a.get(DevCfg), a.get(DevSize), a.get(TcmuGenl.WriteCache)) match
      case (Some(c), _, _) => Right(ReconfigChange.Config(string(c)))
      case (_, Some(s), _) => u64("TCMU_ATTR_DEV_SIZE", s).map(ReconfigChange.Size(_))
      case (_, _, Some(w)) => u8("TCMU_ATTR_WRITECACHE", w).map(v => ReconfigChange.WriteCache(v != 0))
      case _ => Right(ReconfigChange.Unknown)

  /** For `TcmuNetlink.open`: the error code of the NLMSG_ERROR answering `seq`, if `buf` holds one. */
  def ackFor(buf: Array[Byte], seq: Int): Option[Either[String, Int]] =
    split(buf).collectFirst:
      case Right(m) if m.tpe == NLMSG_ERROR && m.seq == seq => errorCode(m)

/** A generic netlink socket subscribed to the TCM-USER "config" group. */
final class TcmuNetlink private (fd: Int, familyId: Int, private var pending: List[Either[String, TcmuEvent]]):
  // Guarded by this. The last sequence number used; `open` used 1 and 2.
  private var seq = 2
  private var closed = false

  /** Blocks until the kernel sends something; returns each message in it. Throws once `close` has been called,
    * but `close` does not wake a `receive` already blocked in `recv`.
    */
  def receive(): List[Either[String, TcmuEvent]] =
    val early = synchronized:
      if closed then throw IllegalStateException("netlink socket closed")
      val p = pending
      pending = Nil
      p
    if early.nonEmpty then early
    else GenlCodec.parseEvents(TcmuNetlink.recv(fd), familyId)

  /** Sends the `*_DONE` reply for `event`. Like libtcmu, it uses the socket's own sequence number: the kernel
    * matches a reply to its pending command by command and device id.
    */
  def reply(event: TcmuEvent, status: Int): Unit = synchronized:
    if !closed then
      val cmd = event match
        case _: TcmuEvent.Added => TcmuGenl.AddedDeviceDone
        case _: TcmuEvent.Removed => TcmuGenl.RemovedDeviceDone
        case _: TcmuEvent.Reconfig => TcmuGenl.ReconfigDeviceDone
      seq += 1
      TcmuNetlink.send(fd, GenlCodec.reply(familyId, seq, cmd, status, event.deviceId))

  def close(): Unit = synchronized:
    if !closed then
      closed = true
      Libc.close(fd)

object TcmuNetlink:
  private val RecvBufferSize = 65536

  private def send(fd: Int, msg: Array[Byte]): Unit =
    val arena = Arena.ofConfined()
    try
      val seg = arena.allocate(msg.length.toLong)
      seg.copyFrom(MemorySegment.ofArray(msg))
      val n = Libc.send(fd, seg)
      if n != msg.length then throw IllegalStateException(s"netlink send: sent $n of ${msg.length} bytes")
    finally arena.close()

  private def recv(fd: Int): Array[Byte] =
    val arena = Arena.ofConfined()
    try
      val seg = arena.allocate(RecvBufferSize.toLong)
      val n = Libc.recv(fd, seg)
      seg.asSlice(0, n).toArray(JAVA_BYTE)
    finally arena.close()

  /** Opens the socket, resolves TCM-USER and its "config" group, joins it and enables kernel command replies. */
  def open(): TcmuNetlink =
    val fd = Libc.socket(Linux.AF_NETLINK, Linux.SOCK_RAW | Linux.SOCK_CLOEXEC, Linux.NETLINK_GENERIC)
    try
      bind(fd)
      send(fd, GenlCodec.getFamily(TcmuGenl.Family, 1))
      val (familyId, groups) = GenlCodec.parseFamily(recv(fd)) match
        case Right(r) => r
        case Left(e) =>
          throw IllegalStateException(s"resolve ${TcmuGenl.Family} family (is target_core_user loaded?): $e")
      val group = groups.getOrElse(TcmuGenl.Group,
        throw IllegalStateException(s"resolve ${TcmuGenl.Family} group ${TcmuGenl.Group}: not in $groups"))
      Libc.setsockoptInt(fd, Linux.SOL_NETLINK, Linux.NETLINK_ADD_MEMBERSHIP, group)
      val pending = setFeatures(fd, familyId)
      TcmuNetlink(fd, familyId, pending)
    catch
      case e: Throwable =>
        try Libc.close(fd)
        catch case c: Exception => scribe.warn(s"netlink: close after failed open: $c")
        throw e

  private def bind(fd: Int): Unit =
    val arena = Arena.ofConfined()
    try
      // struct sockaddr_nl: nl_family u16, nl_pad u16, nl_pid u32 (0: kernel assigns), nl_groups u32
      val addr = arena.allocate(12L, 4)
      addr.set(JAVA_SHORT, 0L, Linux.AF_NETLINK.toShort)
      addr.set(JAVA_SHORT, 2L, 0.toShort)
      addr.set(JAVA_INT, 4L, 0)
      addr.set(JAVA_INT, 8L, 0)
      Libc.bind(fd, addr)
    finally arena.close()

  /** Sends SET_FEATURES and waits for its ack. Events that arrive first are returned for the first `receive`. */
  private def setFeatures(fd: Int, familyId: Int): List[Either[String, TcmuEvent]] =
    val seq = 2
    send(fd, GenlCodec.setFeatures(familyId, seq))
    val early = ListBuffer.empty[Either[String, TcmuEvent]]
    var ack: Option[Either[String, Int]] = None
    while ack.isEmpty do
      val buf = recv(fd)
      ack = GenlCodec.ackFor(buf, seq)
      if ack.isEmpty then early ++= GenlCodec.parseEvents(buf, familyId)
    ack.get match
      case Right(0) => ()
      case Right(c) if -c == Linux.EOPNOTSUPP =>
        scribe.warn("netlink: kernel does not support SET_FEATURES; running without command replies")
      case Right(c) => throw SysError("netlink SET_FEATURES", -c, "rejected by the kernel")
      case Left(e) => throw IllegalStateException(s"netlink SET_FEATURES ack: $e")
    early.toList
