package org.aspen_ddp.aspen.common.metadata

/** Whether an allocation group restricts the IDAs of its members. Fixed when the group is created. */
enum IDAPolicy:
  /** Every member pool or group must use an IDA equal to the group's own `ida` */
  case ExactMatch
  /** Members may use any IDA; the group has no `ida` of its own */
  case AllowAll
