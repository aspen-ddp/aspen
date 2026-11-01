package org.aspen_ddp.aspen.vdisk.tcmu.ring

/** Byte offsets and constants of the TCMU shared-memory ring, from `/usr/include/linux/target_core_user.h`.
  * All multi-byte fields are host byte order.
  */
object RingLayout:
  // struct tcmu_mailbox
  val MbVersion = 0     // u16
  val MbFlags = 2       // u16
  val MbCmdrOff = 4     // u32
  val MbCmdrSize = 8    // u32
  val MbCmdHead = 12    // u32
  val MbCmdTail = 64    // u32, cache-line aligned
  val MailboxSize = MbCmdTail + 4

  // struct tcmu_cmd_entry_hdr
  val EntLenOp = 0      // u32
  val EntCmdId = 4      // u16
  val EntKFlags = 6     // u8
  val EntUFlags = 7     // u8
  val HdrSize = 8

  // struct tcmu_cmd_entry req
  val ReqIovCnt = 8     // u32
  val ReqIovBidiCnt = 12 // u32
  val ReqIovDifCnt = 16 // u32
  val ReqCdbOff = 20    // u64, unaligned
  val ReqIov = 44       // struct iovec[], unaligned
  val IovecSize = 16    // u64 base, u64 len

  // struct tcmu_cmd_entry rsp
  val RspScsiStatus = 8 // u8
  val RspReadLen = 12   // u32
  val RspSense = 16     // char[96]
  /** sizeof(struct tcmu_cmd_entry): every CMD entry is at least this long, so the response fits. */
  val RspSize = RspSense + 96

  // struct tcmu_tmr_entry
  val TmrType = 8       // u8
  val TmrCmdCnt = 12    // u32
  val TmrCmdIds = 32    // u16[]

  val OpPad = 0
  val OpCmd = 1
  val OpTmr = 2
  val OpMask = 7

  // Mailbox capability flags
  val FlagOooc = 1
  val FlagReadLen = 2
  val FlagTmr = 4
  val FlagKeepBuf = 8

  // Entry uflags
  val UFlagUnknownOp = 1
  val UFlagReadLen = 2

  val MailboxVersion = 2
  val SenseSize = 96
  val Align = 8

  def align8(n: Int): Int = (n + 7) & ~7

  /** The CDB length implied by the opcode's group code (SPC-4 4.2.5.1), or None when it is unsupported.
    * `byte7` gives the additional length of a variable-length (0x7F) CDB.
    */
  def cdbLength(opcode: Int, byte7: => Int): Option[Int] = (opcode & 0xff) >> 5 match
    case 0 => Some(6)
    case 1 | 2 => Some(10)
    case 3 if (opcode & 0xff) == 0x7f => Some(8 + (byte7 & 0xff))
    case 4 => Some(16)
    case 5 => Some(12)
    case _ => None
