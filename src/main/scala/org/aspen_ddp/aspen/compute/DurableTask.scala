package org.aspen_ddp.aspen.compute

import java.util.UUID

import org.aspen_ddp.aspen.common.objects.{Key, ObjectRevision}

import scala.concurrent.Future

object DurableTask {
  val TaskTypeKey = Key(0xFF) // Corresponds to TaskType UUID

  val ReservedFromKeyId = 0xF0

  val IdleTaskType = new UUID(0,0)
  // Make a TaskGroup must be able to immediately start task (cause it returns a handle to result)
  // Could make one in terms of remote executor + tcp connection or via insert task and poll for
  // result
  // Ultimately need a UUID based tree similar to object allocators?
}

trait DurableTask {

  val taskPointer: DurableTaskPointer

  def completed: Future[Option[AnyRef]]
}
