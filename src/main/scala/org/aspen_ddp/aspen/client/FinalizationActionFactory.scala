package org.aspen_ddp.aspen.client

import org.aspen_ddp.aspen.common.transaction.TransactionDescription

trait FinalizationActionFactory {
  def createFinalizationAction(client: AspenClient,
                               txd: TransactionDescription,
                               data: Array[Byte]): FinalizationAction
}
