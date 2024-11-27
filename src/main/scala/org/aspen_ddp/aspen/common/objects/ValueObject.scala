package org.aspen_ddp.aspen.common.objects

import org.aspen_ddp.aspen.common.HLCTimestamp

case class ValueObject(value: Value,
                       revision: ObjectRevision,
                       timestamp: HLCTimestamp)
