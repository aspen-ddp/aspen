package org.aspen_ddp.aspen.vdisk.tcmu

import org.aspen_ddp.aspen.vdisk.tcmu.FakeKernel.{read10, write10}
import org.aspen_ddp.aspen.vdisk.tcmu.ring.{Iovec, Ring, RingCorrupt, RingItem}
import org.aspen_ddp.aspen.vdisk.tcmu.ring.RingLayout.*
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.lang.foreign.ValueLayout.{JAVA_BYTE, JAVA_INT, JAVA_SHORT}

class RingSuite extends AnyFunSuite with Matchers:

  private def withRing(fake: FakeKernel = FakeKernel())(f: (FakeKernel, Ring) => Unit): Unit =
    try
      val ring = Ring.open(fake.segment).fold(e => fail(e), identity)
      f(fake, ring)
    finally fake.close()

  private def pattern(n: Int, seed: Int = 0): Array[Byte] = Array.tabulate(n)(i => ((i * 31 + seed) & 0xff).toByte)

  private def onlyCommand(items: List[RingItem]): RingItem.Command = items match
    case List(c: RingItem.Command) => c
    case other => fail(s"expected one Command, got $other")

  test("the layout matches the kernel's target_core_user.h"):
    // offsetof() values from compiling against /usr/include/linux/target_core_user.h. The fake kernel writes entries
    // with these same constants, so only this test ties them to the real ABI.
    List(MbVersion, MbFlags, MbCmdrOff, MbCmdrSize, MbCmdHead, MbCmdTail) should be (List(0, 2, 4, 8, 12, 64))
    List(EntLenOp, EntCmdId, EntKFlags, EntUFlags, HdrSize) should be (List(0, 4, 6, 7, 8))
    List(ReqIovCnt, ReqIovBidiCnt, ReqIovDifCnt, ReqCdbOff, ReqIov) should be (List(8, 12, 16, 24, 48))
    List(RspScsiStatus, RspReadLen, RspSense) should be (List(8, 12, 16))
    List(TmrType, TmrCmdCnt, TmrCmdIds) should be (List(8, 12, 32))

  test("open rejects version 1, a missing OOOC flag, and a command ring past the map"):
    val v1 = FakeKernel()
    try
      v1.segment.set(JAVA_SHORT, MbVersion.toLong, 1.toShort)
      Ring.open(v1.segment).left.toOption.get should include ("version")
    finally v1.close()

    val noOooc = FakeKernel(flags = FlagReadLen | FlagTmr)
    try Ring.open(noOooc.segment).left.toOption.get should include ("OOOC")
    finally noOooc.close()

    val past = FakeKernel()
    try
      past.segment.set(JAVA_INT, MbCmdrSize.toLong, past.cmdrSize + past.dataSize + 8)
      Ring.open(past.segment).left.toOption.get should include ("cmdr")
    finally past.close()

  test("a command round trips"):
    withRing(): (fake, ring) =>
      val cdb = read10(100, 8)
      val id = fake.submit(cdb, readLen = 4096)
      val cmd = onlyCommand(ring.nextItems())
      cmd.cmdId should be (id)
      cmd.cdb.toList should be (cdb.toList)
      cmd.iovecs.map(_.length).sum should be (4096L)
      ring.dataLength(cmd.iovecs) should be (4096L)
      ring.inFlight should be (1)
      val data = pattern(4096)
      ring.scatter(cmd.iovecs, data) should be (4096)
      ring.complete(id, 0, Array.empty, Some(4096)) should be (true)
      ring.inFlight should be (0)
      val List(c) = fake.completions(): @unchecked
      c.cmdId should be (id)
      c.status should be (0)
      c.readLen should be (Some(4096))
      c.data.toList should be (data.toList)

  test("write data is gathered across split iovecs"):
    withRing(): (fake, ring) =>
      val data = pattern(4096, 7)
      fake.submit(write10(0, 8), data = data, iovSplit = 1000)
      val cmd = onlyCommand(ring.nextItems())
      cmd.iovecs.size should be (5)
      ring.gather(cmd.iovecs, 4096).toList should be (data.toList)
      ring.gather(cmd.iovecs, 1500).toList should be (data.take(1500).toList)

  test("PAD at wrap with out-of-order completions"):
    // Each entry is 224 bytes (10 iovecs plus the CDB). Two completed commands move head and tail to 448, so the
    // third of the next three crosses the 1024-byte ring end and needs a PAD.
    withRing(FakeKernel(cmdrSize = 1024)): (fake, ring) =>
      def submitOne(): Int = fake.submit(read10(0, 2), readLen = 1000, iovSplit = 100)
      val warm = List(submitOne(), submitOne())
      ring.nextItems().size should be (2)
      warm.foreach(id => ring.complete(id, 0, Array.empty, None) should be (true))
      fake.completions().map(_.cmdId) should be (warm)
      fake.mailboxTail should be (448)

      val ids = List(submitOne(), submitOne(), submitOne())
      fake.padsWritten should be (1)
      ring.nextItems().collect { case c: RingItem.Command => c.cmdId } should be (ids)
      for id <- List(ids(2), ids(0), ids(1)) do
        ring.complete(id, 0, Array.empty, Some(1000)) should be (true)
      val done = fake.completions()
      done.map(_.cmdId).toSet should be (ids.toSet)
      done.size should be (3)
      fake.mailboxTail should be (fake.mailboxHead)
      ring.inFlight should be (0)
      ring.nextItems() should be (Nil)

  test("TMR entries decode"):
    withRing(): (fake, ring) =>
      fake.submitTmr(5, Seq(7, 9))
      ring.nextItems() should be (List(RingItem.Tmr(5, Vector(7, 9))))
      ring.inFlight should be (0)

  test("an iovec outside the data area is Malformed and completable"):
    withRing(): (fake, ring) =>
      val id = fake.allocId()
      fake.submitRaw(fake.commandEntry(id, read10(0, 1), Seq((0L, 512L))))
      ring.nextItems() match
        case List(RingItem.Malformed(mid, reason, dump, true)) =>
          mid should be (id)
          reason should include ("iovec")
          dump should include (s"cmd_id=$id")
        case other => fail(s"expected Malformed, got $other")
      val sense = Array.fill[Byte](18)(1)
      ring.complete(id, 2, sense, None) should be (true)
      val List(c) = fake.completions(): @unchecked
      c.cmdId should be (id)
      c.status should be (2)
      c.sense.take(18).toList should be (sense.toList)
      c.readLen should be (None)

  test("bidi and dif counts are Malformed"):
    withRing(): (fake, ring) =>
      val iov = Seq((fake.dataOff.toLong, 512L))
      val a = fake.allocId()
      fake.submitRaw(fake.commandEntry(a, read10(0, 1), iov, bidiCnt = 1))
      val b = fake.allocId()
      fake.submitRaw(fake.commandEntry(b, read10(0, 1), iov, difCnt = 1))
      ring.nextItems() match
        case List(RingItem.Malformed(`a`, ra, _, true), RingItem.Malformed(`b`, rb, _, true)) =>
          ra should include ("iov_bidi_cnt")
          rb should include ("iov_dif_cnt")
        case other => fail(s"expected two Malformed, got $other")
      ring.complete(a, 2, Array.empty, None) should be (true)
      ring.complete(b, 2, Array.empty, None) should be (true)
      fake.completions().map(_.cmdId) should be (List(a, b))

  test("an iovec count larger than the entry is Malformed"):
    withRing(): (fake, ring) =>
      val id = fake.allocId()
      val bytes = fake.commandEntry(id, read10(0, 1), Seq((fake.dataOff.toLong, 512L)))
      java.nio.ByteBuffer.wrap(bytes).order(java.nio.ByteOrder.nativeOrder()).putInt(ReqIovCnt, 1000)
      fake.submitRaw(bytes)
      ring.nextItems() match
        case List(RingItem.Malformed(`id`, reason, _, true)) => reason should include ("iov_cnt")
        case other => fail(s"expected Malformed, got $other")

  test("a CDB outside the map is Malformed"):
    withRing(): (fake, ring) =>
      val id = fake.allocId()
      val bytes = fake.commandEntry(id, read10(0, 1), Seq((fake.dataOff.toLong, 512L)))
      java.nio.ByteBuffer.wrap(bytes).order(java.nio.ByteOrder.nativeOrder())
        .putLong(ReqCdbOff, fake.segment.byteSize() - 4)
      fake.submitRaw(bytes)
      ring.nextItems() match
        case List(RingItem.Malformed(`id`, reason, _, true)) => reason should include ("cdb_off")
        case other => fail(s"expected Malformed, got $other")

  test("a CDB in the data area, or running past its entry, is Malformed and its dump reads nothing outside it"):
    // The data area may hold pages the kernel never allocated: only the entry itself may be read. Each ring is
    // fresh, so the entry lands at the start of the command ring.
    def submitWith(fake: FakeKernel)(edit: (java.nio.ByteBuffer, Int) => Long): Int =
      val id = fake.allocId()
      val bytes = fake.commandEntry(id, read10(0, 1), Seq((fake.dataOff.toLong, 512L)))
      val bb = java.nio.ByteBuffer.wrap(bytes).order(java.nio.ByteOrder.nativeOrder())
      bb.putLong(ReqCdbOff, edit(bb, bytes.length))
      fake.submitRaw(bytes)
      id
    def expectMalformed(ring: Ring, id: Int, text: String, dumped: String): Unit =
      ring.nextItems() match
        case List(RingItem.Malformed(`id`, reason, dump, true)) =>
          reason should include (text)
          dump should include (dumped)
        case other => fail(s"expected Malformed, got $other")
    withRing(): (fake, ring) =>
      val id = submitWith(fake)((_, _) => fake.dataOff.toLong)
      expectMalformed(ring, id, "outside the entry", "cdb=(outside the entry)")
    withRing(): (fake, ring) =>
      // Just past the entry's end: inside the command ring, but not this entry
      val id = submitWith(fake)((_, len) => fake.cmdrOff.toLong + len)
      expectMalformed(ring, id, "outside the entry", "cdb=(outside the entry)")
    withRing(): (fake, ring) =>
      // A READ 10 opcode in the entry's last 8 bytes: its 10-byte CDB runs past the entry
      val id = submitWith(fake): (bb, len) =>
        bb.put(len - 8, 0x28.toByte)
        fake.cmdrOff.toLong + len - 8
      expectMalformed(ring, id, "runs past the entry end", "cdb=28")

  test("a TMR count larger than the entry is Malformed"):
    withRing(): (fake, ring) =>
      val buf = java.nio.ByteBuffer.allocate(40).order(java.nio.ByteOrder.nativeOrder())
      buf.putInt(EntLenOp, 40 | OpTmr)
      buf.putInt(TmrCmdCnt, 5)
      fake.submitRaw(buf.array())
      ring.nextItems() match
        case List(RingItem.Malformed(_, reason, _, false)) => reason should include ("cmd_cnt")
        case other => fail(s"expected Malformed, got $other")
      ring.inFlight should be (0)

  test("an entry length running past the ring is RingCorrupt"):
    withRing(): (fake, ring) =>
      val bytes = new Array[Byte](128)
      java.nio.ByteBuffer.wrap(bytes).order(java.nio.ByteOrder.nativeOrder())
        .putInt(EntLenOp, (fake.cmdrSize + 8) | OpCmd)
      fake.submitRaw(bytes)
      val e = intercept[RingCorrupt](ring.nextItems())
      e.reason should include ("cmdr_size")
      e.dump should include ("cmd_head")
      e.dump should include ("cmd_tail")

  test("an entry length too short, past cmd_head, or too short for a CMD response is RingCorrupt"):
    // len_op's low 3 bits are the op, so a length is always 8-aligned.
    for (len, word) <- List((0, "< 8"), (256, "cmd_head"), (64, "response")) do
      withRing(): (fake, ring) =>
        val bytes = new Array[Byte](128)
        java.nio.ByteBuffer.wrap(bytes).order(java.nio.ByteOrder.nativeOrder()).putInt(EntLenOp, len | OpCmd)
        fake.submitRaw(bytes)
        intercept[RingCorrupt](ring.nextItems()).reason should include (word)

  test("an unknown op is RingCorrupt"):
    withRing(): (fake, ring) =>
      val bytes = new Array[Byte](16)
      java.nio.ByteBuffer.wrap(bytes).order(java.nio.ByteOrder.nativeOrder()).putInt(EntLenOp, 16 | 5)
      fake.submitRaw(bytes)
      intercept[RingCorrupt](ring.nextItems()).reason should include ("op")

  test("a head out of range is RingCorrupt"):
    withRing(): (fake, ring) =>
      fake.setCmdHead(fake.cmdrSize)
      intercept[RingCorrupt](ring.nextItems()).reason should include ("cmd_head")
      fake.setCmdHead(4)
      intercept[RingCorrupt](ring.nextItems()).reason should include ("cmd_head")
      fake.setCmdHead(-8)
      intercept[RingCorrupt](ring.nextItems()).reason should include ("cmd_head")

  test("a failed walk consumes nothing"):
    withRing(): (fake, ring) =>
      val id = fake.submit(read10(0, 1), readLen = 512)
      val bytes = new Array[Byte](16)
      java.nio.ByteBuffer.wrap(bytes).order(java.nio.ByteOrder.nativeOrder()).putInt(EntLenOp, 16 | 5)
      fake.submitRaw(bytes)
      intercept[RingCorrupt](ring.nextItems())
      ring.inFlight should be (0)
      ring.complete(id, 0, Array.empty, None) should be (false)

  test("a second completion of the same id returns false and writes nothing"):
    withRing(): (fake, ring) =>
      val id = fake.submit(read10(0, 1), readLen = 512)
      val other = fake.submit(read10(1, 1), readLen = 512)
      ring.nextItems().size should be (2)
      ring.complete(id, 0, Array.empty, None) should be (true)
      val before = fake.segment.toArray(JAVA_BYTE)
      ring.complete(id, 2, Array.fill[Byte](18)(9), Some(1)) should be (false)
      fake.segment.toArray(JAVA_BYTE).sameElements(before) should be (true)
      ring.complete(12345, 0, Array.empty, None) should be (false)
      fake.completions().map(_.cmdId) should be (List(id))
      ring.complete(other, 0, Array.empty, None) should be (true)
      fake.completions().map(_.cmdId) should be (List(other))

  test("resync drops outstanding ids"):
    withRing(): (fake, ring) =>
      val old = List(fake.submit(read10(0, 1), readLen = 512), fake.submit(read10(1, 1), readLen = 512))
      ring.nextItems().size should be (2)
      ring.inFlight should be (2)
      fake.resetRing()
      ring.resync() should be (Right(()))
      ring.inFlight should be (0)
      old.foreach(id => ring.complete(id, 0, Array.empty, None) should be (false))
      fake.mailboxTail should be (0)
      val id = fake.submit(read10(2, 1), readLen = 512)
      val cmd = onlyCommand(ring.nextItems())
      cmd.cmdId should be (id)
      val data = pattern(512, 3)
      ring.scatter(cmd.iovecs, data) should be (512)
      ring.complete(id, 0, Array.empty, Some(512)) should be (true)
      val List(c) = fake.completions(): @unchecked
      c.cmdId should be (id)
      c.data.toList should be (data.toList)

  test("resync rejects a mailbox that became invalid"):
    withRing(): (fake, ring) =>
      fake.segment.set(JAVA_SHORT, MbVersion.toLong, 1.toShort)
      ring.resync().left.toOption.get should include ("version")

  test("mailboxDump names every field"):
    withRing(): (fake, ring) =>
      val d = ring.mailboxDump
      for f <- List("version=2", "flags=", "cmdr_off=128", "cmdr_size=4096", "cmd_head=0", "cmd_tail=0",
                    "consume_tail=0", "inFlight=0") do
        d should include (f)

  test("scatter stops at the iovec capacity"):
    withRing(): (fake, ring) =>
      fake.submit(read10(0, 1), readLen = 512)
      val cmd = onlyCommand(ring.nextItems())
      ring.scatter(cmd.iovecs, pattern(600)) should be (512)
      ring.dataLength(Vector(Iovec(0, 3), Iovec(10, 4))) should be (7L)

  test("concurrent completions are each delivered once"):
    withRing(FakeKernel(cmdrSize = 16 * 1024)): (fake, ring) =>
      val ids = (0 until 64).map(i => fake.submit(read10(i, 1), readLen = 512)).toList
      ring.nextItems().size should be (64)
      val start = java.util.concurrent.CountDownLatch(1)
      val threads = ids.grouped(8).toList.map: group =>
        Thread.ofPlatform().start: () =>
          start.await()
          group.reverse.foreach(id => ring.complete(id, 0, Array.empty, None))
      start.countDown()
      threads.foreach(_.join())
      ring.inFlight should be (0)
      val done = fake.completions().map(_.cmdId)
      done.sorted should be (ids.sorted)
      fake.mailboxTail should be (fake.mailboxHead)

  test("a completion after a kernel reset but before resync writes nothing"):
    withRing(): (fake, ring) =>
      // A completed warm-up command moves cmd_tail off 0, so the kernel's reset to 0 is visible.
      val warm = fake.submit(read10(9, 1), readLen = 512)
      val old = fake.submit(read10(0, 1), readLen = 512)
      ring.nextItems().size should be (2)
      ring.complete(warm, 0, Array.empty, None) should be (true)
      fake.completions().map(_.cmdId) should be (List(warm))
      fake.mailboxTail should not be (0)
      fake.resetRing()
      val id = fake.submit(read10(1, 1), readLen = 512)
      val before = fake.segment.toArray(JAVA_BYTE)
      ring.complete(old, 0, Array.empty, Some(512)) should be (false)
      fake.segment.toArray(JAVA_BYTE).sameElements(before) should be (true)
      ring.resync() should be (Right(()))
      val cmd = onlyCommand(ring.nextItems())
      cmd.cmdId should be (id)
      val data = pattern(512, 5)
      ring.scatter(cmd.iovecs, data) should be (512)
      ring.complete(id, 0, Array.empty, Some(512)) should be (true)
      val List(c) = fake.completions(): @unchecked
      c.cmdId should be (id)
      c.data.toList should be (data.toList)

  test("an empty sense clears the whole sense area"):
    withRing(): (fake, ring) =>
      // 20 iovecs fill the slot's sense area (offsets 16-111) with request bytes.
      val id = fake.submit(read10(0, 1), readLen = 2000, iovSplit = 100)
      val cmd = onlyCommand(ring.nextItems())
      cmd.iovecs.size should be (20)
      val e = fake.cmdrOff.toLong
      fake.segment.asSlice(e + RspSense, SenseSize).toArray(JAVA_BYTE).exists(_ != 0) should be (true)
      ring.complete(id, 2, Array.empty, None) should be (true)
      val List(c) = fake.completions(): @unchecked
      c.status should be (2)
      c.sense.toList should be (List.fill(SenseSize)(0.toByte))

  test("resync with a reset callback holds completions off until the ring is resynced"):
    withRing(): (fake, ring) =>
      // cmd_tail is 0 before and after this reset, so only the lock keeps the old completion out.
      val old = fake.submit(read10(0, 1), readLen = 512)
      ring.nextItems().size should be (1)
      val result = java.util.concurrent.CompletableFuture[Boolean]()
      var completer: Thread = null
      var newId = 0
      ring.resync: () =>
        fake.resetRing()
        newId = fake.submit(read10(1, 1), readLen = 512)
        completer = Thread.ofPlatform().start(() => result.complete(ring.complete(old, 0, Array.empty, None)))
        val deadline = System.nanoTime() + 5_000_000_000L
        while completer.getState != Thread.State.BLOCKED && System.nanoTime() < deadline do Thread.onSpinWait()
        completer.getState should be (Thread.State.BLOCKED)
      .fold(e => fail(e), identity)
      completer.join()
      result.get() should be (false)
      val cmd = onlyCommand(ring.nextItems())
      cmd.cmdId should be (newId)
      ring.complete(newId, 0, Array.empty, None) should be (true)
      fake.completions().map(_.cmdId) should be (List(newId))
