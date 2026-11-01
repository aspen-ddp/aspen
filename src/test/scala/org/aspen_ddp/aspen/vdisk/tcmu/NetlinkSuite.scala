package org.aspen_ddp.aspen.vdisk.tcmu

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.nio.{ByteBuffer, ByteOrder}

class NetlinkSuite extends AnyFunSuite with Matchers:

  // Hand-written wire fields, host byte order.
  private def u8(v: Int): Array[Byte] = Array(v.toByte)
  private def u16(v: Int): Array[Byte] =
    ByteBuffer.allocate(2).order(ByteOrder.nativeOrder()).putShort(v.toShort).array()
  private def u32(v: Int): Array[Byte] =
    ByteBuffer.allocate(4).order(ByteOrder.nativeOrder()).putInt(v).array()
  private def u64(v: Long): Array[Byte] =
    ByteBuffer.allocate(8).order(ByteOrder.nativeOrder()).putLong(v).array()
  private def ascii(s: String): Array[Byte] = s.getBytes("US-ASCII")
  private def pad(n: Int): Array[Byte] = new Array[Byte](n)
  private def bytes(parts: Array[Byte]*): Array[Byte] = parts.toArray.flatten

  private val family = 0x1a

  /** An event from the kernel: "dev" on uio3, device id 7, then `extra` attributes. Lengths are counted by hand. */
  private def event(cmd: Int, version: Int, extra: Array[Byte], extraLen: Int): Array[Byte] =
    bytes(
      u32(16 + 4 + 8 + 8 + 8 + extraLen), // nlmsg_len
      u16(family),                        // nlmsg_type: the TCM-USER family
      u16(0),                             // nlmsg_flags
      u32(0),                             // nlmsg_seq
      u32(0),                             // nlmsg_pid
      u8(cmd), u8(version), u16(0),       // genlmsghdr: cmd, version, reserved
      u16(8), u16(1), ascii("dev"), u8(0), // TCMU_ATTR_DEVICE "dev\0" (len 4 + 4)
      u16(8), u16(2), u32(3),             // TCMU_ATTR_MINOR = 3
      u16(8), u16(8), u32(7),             // TCMU_ATTR_DEVICE_ID = 7
      extra)

  test("GETFAMILY request bytes"):
    val expected = bytes(
      u32(36),                           // nlmsg_len: 16 + 4 + 16
      u16(0x10),                         // nlmsg_type: GENL_ID_CTRL
      u16(1),                            // nlmsg_flags: NLM_F_REQUEST
      u32(1),                            // nlmsg_seq
      u32(0),                            // nlmsg_pid
      u8(3), u8(1), u16(0),              // genlmsghdr: CTRL_CMD_GETFAMILY, version 1, reserved
      u16(13), u16(2),                   // nla_len = 4 + 9, nla_type = CTRL_ATTR_FAMILY_NAME
      ascii("TCM-USER"), u8(0), pad(3))  // NUL-terminated name, padded to 16
    GenlCodec.getFamily("TCM-USER", 1).toList should be (expected.toList)

  test("family reply parses id and groups"):
    val msg = bytes(
      u32(16 + 4 + 8 + 12 + 28),         // nlmsg_len = 68
      u16(0x10),                         // nlmsg_type: GENL_ID_CTRL
      u16(0), u32(1), u32(0),            // flags, seq, pid
      u8(1), u8(2), u16(0),              // CTRL_CMD_NEWFAMILY, version 2
      u16(6), u16(1), u16(family), pad(2),  // CTRL_ATTR_FAMILY_ID (u16), padded to 8
      u16(12), u16(2), ascii("TCM-USER"),   // CTRL_ATTR_FAMILY_NAME without NUL: len 12
      u16(28), u16(0x8000 | 7),          // CTRL_ATTR_MCAST_GROUPS, NLA_F_NESTED, len 4 + 24
      u16(24), u16(0x8000 | 1),          //   group #1, nested, len 4 + 12 + 8
      u16(11), u16(1), ascii("config"), u8(0), pad(1), // CTRL_ATTR_MCAST_GRP_NAME "config\0", padded to 12
      u16(8), u16(2), u32(5))            //     CTRL_ATTR_MCAST_GRP_ID = 5
    msg.length should be (68)
    GenlCodec.parseFamily(msg) should be (Right((family, Map("config" -> 5))))

  test("family lookup error is Left"):
    val msg = bytes(
      u32(16 + 4 + 16),                  // nlmsg_len
      u16(2),                            // NLMSG_ERROR
      u16(0), u32(1), u32(0),            // flags, seq, pid
      u32(-2),                           // error = -ENOENT
      u32(36), u16(0x10), u16(1), u32(1), u32(0)) // the request's nlmsghdr
    val r = GenlCodec.parseFamily(msg)
    r.isLeft should be (true)
    r.left.toOption.get should include ("errno 2")

  test("added event parses"):
    val msg = event(1, 2, Array.empty, 0)
    GenlCodec.parseEvents(msg, family) should be (List(Right(TcmuEvent.Added("dev", 3, 7))))

  test("removed event parses"):
    val msg = event(2, 2, Array.empty, 0)
    GenlCodec.parseEvents(msg, family) should be (List(Right(TcmuEvent.Removed("dev", 3, 7))))

  test("reconfig size parses"):
    val extra = bytes(u16(12), u16(5), u64(1L << 33))   // TCMU_ATTR_DEV_SIZE (u64), len 12
    GenlCodec.parseEvents(event(3, 2, extra, 12), family) should be
      (List(Right(TcmuEvent.Reconfig("dev", 3, 7, ReconfigChange.Size(1L << 33)))))

  test("reconfig write cache parses"):
    val extra = bytes(u16(5), u16(6), u8(1), pad(3))    // TCMU_ATTR_WRITECACHE (u8), len 5, padded to 8
    GenlCodec.parseEvents(event(3, 2, extra, 8), family) should be
      (List(Right(TcmuEvent.Reconfig("dev", 3, 7, ReconfigChange.WriteCache(true)))))

  test("reconfig config parses and takes precedence over size"):
    val extra = bytes(
      u16(12), u16(5), u64(4096),                       // TCMU_ATTR_DEV_SIZE
      u16(8), u16(4), ascii("a/b"), u8(0))              // TCMU_ATTR_DEV_CFG "a/b\0"
    GenlCodec.parseEvents(event(3, 2, extra, 20), family) should be
      (List(Right(TcmuEvent.Reconfig("dev", 3, 7, ReconfigChange.Config("a/b")))))

  test("reconfig without a known attribute is Unknown"):
    GenlCodec.parseEvents(event(3, 2, Array.empty, 0), family) should be
      (List(Right(TcmuEvent.Reconfig("dev", 3, 7, ReconfigChange.Unknown))))

  test("missing minor is Left"):
    val msg = bytes(
      u32(16 + 4 + 8 + 8),               // nlmsg_len
      u16(family), u16(0), u32(0), u32(0),
      u8(1), u8(2), u16(0),              // ADDED_DEVICE v2
      u16(8), u16(1), ascii("dev"), u8(0),  // TCMU_ATTR_DEVICE
      u16(8), u16(8), u32(7))            // TCMU_ATTR_DEVICE_ID
    val r = GenlCodec.parseEvents(msg, family)
    r should have size 1
    r.head.isLeft should be (true)

  test("v2 without device id is Left"):
    val msg = bytes(
      u32(16 + 4 + 8 + 8),               // nlmsg_len
      u16(family), u16(0), u32(0), u32(0),
      u8(1), u8(2), u16(0),              // ADDED_DEVICE v2
      u16(8), u16(1), ascii("dev"), u8(0),  // TCMU_ATTR_DEVICE
      u16(8), u16(2), u32(3))            // TCMU_ATTR_MINOR
    val r = GenlCodec.parseEvents(msg, family)
    r should have size 1
    r.head.isLeft should be (true)

  test("two messages in one buffer parse"):
    val buf = bytes(event(1, 2, Array.empty, 0), event(2, 2, Array.empty, 0))
    GenlCodec.parseEvents(buf, family) should be
      (List(Right(TcmuEvent.Added("dev", 3, 7)), Right(TcmuEvent.Removed("dev", 3, 7))))

  test("ack and done are skipped, an error and another family are Left"):
    val ack = bytes(
      u32(36), u16(2), u16(0x100), u32(4), u32(0), // NLMSG_ERROR, NLM_F_CAPPED
      u32(0),                            // error 0: an ack
      u32(36), u16(family), u16(5), u32(4), u32(0)) // the request's nlmsghdr
    val error = bytes(
      u32(36), u16(2), u16(0), u32(5), u32(0),
      u32(-22),                          // -EINVAL
      u32(36), u16(family), u16(1), u32(5), u32(0))
    val done = bytes(u32(20), u16(3), u16(0), u32(0), u32(0), u32(0)) // NLMSG_DONE with a 4-byte payload
    val other = bytes(u32(20), u16(0x1b), u16(0), u32(0), u32(0), u8(1), u8(2), u16(0))
    val r = GenlCodec.parseEvents(bytes(ack, error, done, other, event(1, 2, Array.empty, 0)), family)
    r should have size 3
    r(0).left.toOption.get should include ("errno 22")
    r(1).isLeft should be (true)
    r(2) should be (Right(TcmuEvent.Added("dev", 3, 7)))

  test("reply bytes carry status and device id"):
    val expected = bytes(
      u32(36),                           // nlmsg_len: 16 + 4 + 8 + 8
      u16(0x1a),                         // nlmsg_type: family id
      u16(1),                            // nlmsg_flags: NLM_F_REQUEST
      u32(9),                            // nlmsg_seq
      u32(0),                            // nlmsg_pid
      u8(4), u8(2), u16(0),              // genlmsghdr: ADDED_DEVICE_DONE, version 2
      u16(8), u16(7), u32(-22),          // TCMU_ATTR_CMD_STATUS = -22
      u16(8), u16(8), u32(3))            // TCMU_ATTR_DEVICE_ID = 3
    GenlCodec.reply(0x1a, 9, 4, -22, 3).toList should be (expected.toList)

  test("set features bytes request an ack"):
    val expected = bytes(
      u32(28),                           // nlmsg_len: 16 + 4 + 8
      u16(0x1a),                         // nlmsg_type: family id
      u16(5),                            // nlmsg_flags: NLM_F_REQUEST | NLM_F_ACK
      u32(2), u32(0),                    // seq, pid
      u8(7), u8(2), u16(0),              // genlmsghdr: SET_FEATURES, version 2
      u16(5), u16(9), u8(1), pad(3))     // TCMU_ATTR_SUPP_KERN_CMD_REPLY = 1, padded to 8
    GenlCodec.setFeatures(0x1a, 2).toList should be (expected.toList)

  test("a truncated attribute is Left, not an exception"):
    val full = event(1, 2, Array.empty, 0)
    // The minor attribute claims 8 bytes but the message ends 2 bytes into its payload.
    val msg = bytes(u32(16 + 4 + 8 + 6), full.slice(4, 16 + 4 + 8 + 6))
    val r = GenlCodec.parseEvents(msg, family)
    r should have size 1
    r.head.isLeft should be (true)

  test("a truncated message header is Left, not an exception"):
    val full = event(1, 2, Array.empty, 0)
    for n <- 1 until full.length do
      val r = GenlCodec.parseEvents(full.take(n), family)
      r should have size 1
      r.head.isLeft should be (true)

  test("an undersized integer attribute is Left"):
    val msg = bytes(
      u32(16 + 4 + 8 + 8 + 8),
      u16(family), u16(0), u32(0), u32(0),
      u8(1), u8(2), u16(0),
      u16(8), u16(1), ascii("dev"), u8(0),
      u16(6), u16(2), u16(3), pad(2),    // TCMU_ATTR_MINOR with a 2-byte payload
      u16(8), u16(8), u32(7))
    GenlCodec.parseEvents(msg, family).head.isLeft should be (true)
