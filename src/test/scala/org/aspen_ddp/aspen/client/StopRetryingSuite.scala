package org.aspen_ddp.aspen.client

import org.aspen_ddp.aspen.common.HLCTimestamp
import org.aspen_ddp.aspen.common.ida.Replication
import org.aspen_ddp.aspen.common.objects.{DataObjectPointer, ObjectId}
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.transaction.{TransactionDescription, TransactionId}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.util.UUID

class StopRetryingSuite extends AnyFunSuite with Matchers:

  private val poolId = PoolId(new UUID(0, 5))
  private val ida = Replication(3, 2)
  private val txd = TransactionDescription(TransactionId(UUID.randomUUID()), HLCTimestamp(100),
    DataObjectPointer(ObjectId(new UUID(0, 1)), poolId, Array[Byte]()), 0, Nil, Nil, None, Nil, Nil, ida,
    Map(poolId -> ida))

  test("unlessCollision retries TransactionAborted"):
    StopRetrying.unlessCollision(TransactionAborted(txd)).value.get.isSuccess should be(true)

  test("unlessCollision retries TransactionOutcomeUnknown"):
    StopRetrying.unlessCollision(TransactionOutcomeUnknown(txd)).value.get.isSuccess should be(true)

  test("unlessCollision stops on anything else"):
    val e = new Exception("other")
    val thrown = intercept[StopRetrying](StopRetrying.unlessCollision(e))
    thrown.reason should be theSameInstanceAs e

  test("collisionRetry remembers an Unknown and not an abort, and stops on anything else"):
    val r = StopRetrying.collisionRetry()
    r.onFailure(TransactionAborted(txd)).value.get.isSuccess should be(true)
    r.mayHaveCommitted should be(false)
    r.onFailure(TransactionOutcomeUnknown(txd)).value.get.isSuccess should be(true)
    r.mayHaveCommitted should be(true)
    val e = new Exception("other")
    intercept[StopRetrying](r.onFailure(e)).reason should be theSameInstanceAs e
