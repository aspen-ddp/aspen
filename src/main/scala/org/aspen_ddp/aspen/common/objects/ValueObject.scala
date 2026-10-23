package org.aspen_ddp.aspen.common.objects

import org.aspen_ddp.aspen.common.HLCTimestamp

case class ValueObject(value: Value,
                       revision: ObjectRevision,
                       timestamp: HLCTimestamp):

  override def equals(other: Any): Boolean = other match
    case that: ValueObject =>
      java.util.Arrays.equals(value.bytes, that.value.bytes) && revision == that.revision && timestamp == that.timestamp
    case _ => false

  override def hashCode: Int = (java.util.Arrays.hashCode(value.bytes), revision, timestamp).##
