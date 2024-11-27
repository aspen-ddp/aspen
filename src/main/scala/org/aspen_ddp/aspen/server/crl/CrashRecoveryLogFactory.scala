package org.aspen_ddp.aspen.server.crl

trait CrashRecoveryLogFactory {
  def createCRL(): CrashRecoveryLog
}
