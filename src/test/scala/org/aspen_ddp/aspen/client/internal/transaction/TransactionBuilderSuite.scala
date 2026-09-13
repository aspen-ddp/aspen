package org.aspen_ddp.aspen.client.internal.transaction

import org.aspen_ddp.aspen.client.AppendNotYetSupported
import org.aspen_ddp.aspen.common.DataBuffer
import org.aspen_ddp.aspen.common.network.ClientId
import org.aspen_ddp.aspen.common.objects.{DataObjectPointer, ObjectId, ObjectRevision}
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.transaction.TransactionId
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.util.UUID

class TransactionBuilderSuite extends AnyFunSuite with Matchers {

  private val oid1 = ObjectId(new UUID(0, 1))
  private val tx1 = TransactionId(new UUID(0, 2))
  private val rev1 = ObjectRevision(TransactionId(new UUID(0, 3)))
  private val p1 = DataObjectPointer(oid1, PoolId(new UUID(0, 0)), Array[Byte]())

  private def mkBuilder(): TransactionBuilder =
    new TransactionBuilder(tx1, _ => 0.toByte, ClientId(new UUID(0, 4)))

  test("append throws AppendNotYetSupported") {
    val bldr = mkBuilder()

    val err = intercept[AppendNotYetSupported] {
      bldr.append(p1, rev1, DataBuffer(Array[Byte](1, 2, 3)))
    }

    err.objectPointer should be(p1)
  }

  test("a rejected append leaves no partial state in the builder") {
    val bldr = mkBuilder()

    intercept[AppendNotYetSupported] {
      bldr.append(p1, rev1, DataBuffer(Array[Byte](1, 2, 3)))
    }

    // The throw must precede every mutation, otherwise the object would be recorded as
    // updated and a later overwrite of it would wrongly raise MultipleDataUpdatesToObject
    assert(bldr.isEmpty)
  }

  test("overwrite records an update") {
    val bldr = mkBuilder()

    assert(bldr.isEmpty)

    bldr.overwrite(p1, rev1, DataBuffer(Array[Byte](1, 2, 3)))

    assert(!bldr.isEmpty)
  }
}
