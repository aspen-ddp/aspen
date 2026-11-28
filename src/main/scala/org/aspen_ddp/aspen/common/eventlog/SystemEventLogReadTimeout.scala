package org.aspen_ddp.aspen.common.eventlog

import scala.concurrent.duration.FiniteDuration

/** A system event log read (AspenClient.readSystemEvents) did not finish within `timeout`. The log is not readable
  * right now: typically a store holding the lock of a transaction that cannot yet be decided is unreachable, and a
  * settled read must wait for it (DESIGN.md section 6). Nothing partial is returned */
class SystemEventLogReadTimeout(val timeout: FiniteDuration)
  extends Exception(s"The system event log could not be read within $timeout. It is not readable right now: a store " +
    "holding an undecided transaction may be unreachable. Try again later")
