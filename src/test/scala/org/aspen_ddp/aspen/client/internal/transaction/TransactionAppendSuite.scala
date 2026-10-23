package org.aspen_ddp.aspen.client.internal.transaction

import org.aspen_ddp.aspen.IntegrationTestSuite
import org.aspen_ddp.aspen.client.AppendPosition
import org.aspen_ddp.aspen.common.DataBuffer
import org.aspen_ddp.aspen.common.objects.{DataObjectPointer, ObjectId, ObjectRevision}
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.transaction.TransactionId

import java.util.UUID

class TransactionAppendSuite extends IntegrationTestSuite {

  private val ptr = DataObjectPointer(ObjectId(new UUID(0, 1)), PoolId(new UUID(0, 0)), Array[Byte]())
  private val rev = ObjectRevision(TransactionId(new UUID(0, 3)))
  private val L = 30
  private val pos = AppendPosition(ptr, rev, L)
  private val data = Array.tabulate[Byte](50)(i => (i * 7 + 1).toByte)

  atest("append returns the post-commit position") {
    val tx = client.newTransaction()

    val result = tx.append(pos, DataBuffer(data))

    result should be (AppendPosition(ptr, tx.revision, L + data.length))
    intercept[IllegalArgumentException]:
      tx.append(pos, DataBuffer.Empty)
    scala.concurrent.Future.successful(succeed)
  }
}
