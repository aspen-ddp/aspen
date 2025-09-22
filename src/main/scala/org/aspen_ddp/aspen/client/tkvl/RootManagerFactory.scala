package org.aspen_ddp.aspen.client.tkvl

import org.aspen_ddp.aspen.client.{AspenClient, RegisteredTypeFactory}

trait RootManagerFactory extends RegisteredTypeFactory:
  
  def createRootManager(client: AspenClient, data: Array[Byte]): RootManager
