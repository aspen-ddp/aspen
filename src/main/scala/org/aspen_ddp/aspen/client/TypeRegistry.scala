package org.aspen_ddp.aspen.client

import java.util.UUID

class TypeRegistry(private val registry: Map[UUID, RegisteredTypeFactory]) {

  def getType[T](typeUUID: UUID): Option[T] = {
    registry.get(typeUUID).map(_.asInstanceOf[T])
  }

}
