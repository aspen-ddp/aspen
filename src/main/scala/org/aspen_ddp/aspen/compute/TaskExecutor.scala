package org.aspen_ddp.aspen.compute

import org.aspen_ddp.aspen.client.Transaction
import org.aspen_ddp.aspen.common.objects.Key

import scala.concurrent.Future

trait TaskExecutor {
  def prepareTask(
                   taskType: DurableTaskType,
                   initialState: List[(Key, Array[Byte])])(implicit tx: Transaction): Future[Future[Option[AnyRef]]]

}
