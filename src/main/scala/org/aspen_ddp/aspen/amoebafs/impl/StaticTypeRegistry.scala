package org.aspen_ddp.aspen.amoebafs.impl

import java.util.UUID

import org.aspen_ddp.aspen.client.RegisteredTypeFactory
import org.aspen_ddp.aspen.amoebafs.impl.simple.SimpleDirectoryRootManager

object StaticTypeRegistry {

  private val registry: List[RegisteredTypeFactory] = List(
   SimpleDirectoryRootManager
  )

  val types: List[(UUID, RegisteredTypeFactory)] = registry.map(t => t.typeUUID -> t)
}
