package org.aspen_ddp.aspen.client

import java.util.UUID

object TypeRegistry:

  class DuplicateTypeRegistration(uuid: UUID,
                                  firstClass: String,
                                  secondClass: String) extends Throwable(s"DuplicateRegistration: ${uuid}. $firstClass, $secondClass")

  def apply(lists: List[RegisteredTypeFactory]*): TypeRegistry =
    var map = Map[UUID, RegisteredTypeFactory]()
    lists.flatten.foreach: rtf =>
      map.get(rtf.typeUUID) match
        case None => map += rtf.typeUUID -> rtf
        case Some(prev) =>
          if rtf ne prev then
            throw new DuplicateTypeRegistration(rtf.typeUUID, rtf.getClass.getTypeName, prev.getClass.getTypeName)

    new TypeRegistry(map)

class TypeRegistry(private val registry: Map[UUID, RegisteredTypeFactory]):

  def getType[T](typeUUID: UUID): Option[T] =
    registry.get(typeUUID).map(_.asInstanceOf[T])

