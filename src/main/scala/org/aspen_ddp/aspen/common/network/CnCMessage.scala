package org.aspen_ddp.aspen.common.network

// Base type for all Command-and-Control request messages. No concrete request
// types are currently defined; future CnC messages are added here as case
// classes extending CnCRequest.
sealed abstract class CnCRequest

// Universal reply for all CnC requests.
sealed abstract class CnCReply

object CnCReply:
  case class Ok() extends CnCReply
  case class Error(message: String) extends CnCReply
