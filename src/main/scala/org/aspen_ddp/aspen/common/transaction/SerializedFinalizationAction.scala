package org.aspen_ddp.aspen.common.transaction

final case class SerializedFinalizationAction(
       /** Identifies the type of the serialized FinalizationAction
         *
         *  UUIDs are used instead of an enumeration in order to support the definition of arbitrary,
         *  application-level FinalizationActions
         */
       typeId: FinalizationActionId,

       /** Serialized FinalizationAction */
       data: Array[Byte]) {

  override def equals(other: Any): Boolean = other match {
    case rhs: SerializedFinalizationAction => typeId == rhs.typeId && java.util.Arrays.equals(data, rhs.data)
    case _ => false
  }
}
