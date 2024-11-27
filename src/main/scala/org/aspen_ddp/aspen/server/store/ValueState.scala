package org.aspen_ddp.aspen.server.store

import org.aspen_ddp.aspen.common.HLCTimestamp
import org.aspen_ddp.aspen.common.objects.{ObjectRevision, Value}
import org.aspen_ddp.aspen.common.transaction.TransactionId

class ValueState(var value: Value,
                 var revision: ObjectRevision,
                 var timestamp: HLCTimestamp,
                 var lockedToTransaction: Option[TransactionId]) {

}
