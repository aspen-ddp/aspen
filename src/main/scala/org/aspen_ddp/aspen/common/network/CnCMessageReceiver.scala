package org.aspen_ddp.aspen.common.network

import scala.concurrent.Future

// Inbound dispatch for the Command-and-Control framework, mirroring
// MessageHandler. A CnCBackend hands each decoded request here and replies with
// the resolved CnCReply. Concrete handlers pattern-match on the sealed
// CnCRequest type once message types exist.
trait CnCMessageReceiver:
  def onCnCRequestReceived(msg: CnCRequest): Future[CnCReply]

object CnCMessageReceiver:
  // Default receiver for nodes that expose the CnC channel but define no
  // handlers yet. Always replies with an error.
  object Unhandled extends CnCMessageReceiver:
    def onCnCRequestReceived(msg: CnCRequest): Future[CnCReply] =
      Future.successful(CnCReply.Error("unhandled CnC request"))
