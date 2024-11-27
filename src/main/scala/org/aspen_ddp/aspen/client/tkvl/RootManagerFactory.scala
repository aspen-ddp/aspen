package org.aspen_ddp.aspen.client.tkvl

import org.aspen_ddp.aspen.client.AspenClient

trait RootManagerFactory {
  def createRootManager(client: AspenClient, data: Array[Byte]): RootManager
}
