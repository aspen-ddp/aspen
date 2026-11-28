package org.aspen_ddp.aspen.common.eventlog

import java.util.UUID

import org.aspen_ddp.aspen.common.HLCTimestamp
import org.scalatest.funsuite.AnyFunSuite

class EventTypesSuite extends AnyFunSuite:

  test("EventLogEntry round-trips through encode/decode") {
    val ts = HLCTimestamp(0x8123456789abcdefL)
    val typeUUID = UUID.randomUUID()
    for size <- List(0, 1000) do
      val e = EventLogEntry(ts, typeUUID, Array.tabulate[Byte](size)(i => (i * 7).toByte))
      val d = EventLogEntry.decode(e.encode())
      assert(d == e)
      assert(d.timestamp == ts)
      assert(d.typeUUID == typeUUID)
      assert(d.data.length == size)
      assert(d.hashCode == e.hashCode)
  }

  test("DroppedEvent.entryFor carries the dropped type and reason and the dropped timestamp") {
    val ts = HLCTimestamp(12345L << 16)
    val droppedType = UUID.randomUUID()
    val dropped = EventLogEntry(ts, droppedType, new Array[Byte](100))
    val entry = DroppedEvent.entryFor(dropped, DropReason.ExceededMaximumLogEntrySize)
    assert(entry.typeUUID == DroppedEvent.TypeUUID)
    assert(entry.timestamp == ts)
    val ev = DroppedEvent.createEvent(EventLogEntry.decode(entry.encode()))
    assert(ev.timestamp == ts)
    assert(ev.droppedType == droppedType)
    assert(ev.reason == DropReason.ExceededMaximumLogEntrySize)
    assert(ev.typeUUID == DroppedEvent.TypeUUID)
    assert(ev.describe.contains(droppedType.toString))
  }
