package org.aspen_ddp.aspen.common.eventlog

import java.nio.ByteBuffer

import org.aspen_ddp.aspen.common.HLCTimestamp
import org.aspen_ddp.aspen.common.objects.Key

/** Order-preserving key for an HLC timestamp.
  *
  * HLCTimestamp orders as a signed Long but ByteArrayKeyOrdering compares unsigned bytes. Flipping the sign bit
  * makes the unsigned big-endian bytes sort identically to the signed timestamps.
  */
object LogKey:

  def apply(ts: HLCTimestamp): Key =
    Key(ByteBuffer.allocate(8).putLong(ts.asLong ^ Long.MinValue).array())

  def decode(k: Key): HLCTimestamp =
    require(k.bytes.length == 8, s"Event log key must be 8 bytes, got ${k.bytes.length}")
    HLCTimestamp(ByteBuffer.wrap(k.bytes).getLong ^ Long.MinValue)
