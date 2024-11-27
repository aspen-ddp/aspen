package org.aspen_ddp.aspen.client.internal.read

import java.util.UUID

import org.aspen_ddp.aspen.client.{AspenClient, ObjectState, ReadError}
import org.aspen_ddp.aspen.common.network.ReadResponse
import org.aspen_ddp.aspen.common.objects.ObjectPointer

import scala.concurrent.Future

trait ReadDriver {
  def readResult: Future[ObjectState]

  /** Called to begin the read process. Read messages must not be sent until this method is called */
  def begin(): Unit

  /** Called to abandon the read. This calls should cancel all activity scheduled for the future */
  def shutdown(): Unit

  /** Returns True when all stores have been heard from */
  def receiveReadResponse(response:ReadResponse): Boolean
}

object ReadDriver {

  /**
    * objectPointer: ObjectPointer,
    * readType: ReadType,
    * readUUID:UUID,
    * comment: String Comment describing purpose of the read for debug/trace logs
    * disableOpportunisticRebuild: Boolean
    */
  type Factory = (AspenClient, ObjectPointer, UUID, String, Boolean) => ReadDriver

}
