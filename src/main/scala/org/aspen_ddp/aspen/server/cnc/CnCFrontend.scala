package org.aspen_ddp.aspen.server.cnc

import org.aspen_ddp.aspen.common.metadata.HostState
import scala.concurrent.Future


trait CnCFrontend:
  
  def host: HostState
  
  def send(msg: NewStore): Future[Unit]

  def send(msg: ShutdownStore): Future[Unit]

  def send(msg: TransferStore): Future[Unit]
