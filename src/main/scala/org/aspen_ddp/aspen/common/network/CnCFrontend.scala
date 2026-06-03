package org.aspen_ddp.aspen.common.network

import scala.concurrent.Future

// Sender side of the Command-and-Control framework. Implementations send a
// request to a single target host and complete the returned Future with the
// host's reply. The Future fails only on a transport/serialization error; an
// application-level failure is returned as a successful Future containing a
// CnCReply.Error.
trait CnCFrontend:
  def send(msg: CnCRequest): Future[CnCReply]
