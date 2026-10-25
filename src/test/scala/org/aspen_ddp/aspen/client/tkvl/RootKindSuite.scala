package org.aspen_ddp.aspen.client.tkvl

import java.nio.{ByteBuffer, ByteOrder}
import java.util.UUID

import org.aspen_ddp.aspen.client.PoolObjectAllocatorId
import org.aspen_ddp.aspen.common.objects.{ByteArrayKeyOrdering, DataObjectPointer, KeyValueObjectPointer, ObjectId, ObjectPointer}
import org.aspen_ddp.aspen.common.pool.PoolId
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class RootKindSuite extends AnyFunSuite with Matchers:

  private val poolId = PoolId(new UUID(0, 5))
  private val allocator = new NodeAllocator(null, List((PoolObjectAllocatorId(poolId), 4096)))

  private def dataPointer = DataObjectPointer(ObjectId(new UUID(1, 2)), poolId, Array[Byte](1, 2, 3))
  private def kvPointer = KeyValueObjectPointer(ObjectId(new UUID(3, 4)), poolId, Array[Byte](4, 5))

  private def roundTrip(root: Root): Root = Root(null, root.encode())

  private def check(kind: TreeKind, ptr: ObjectPointer): Unit =
    val withPtr = roundTrip(Root(2, ByteArrayKeyOrdering, Some(ptr), allocator, kind))
    withPtr.kind should be (kind)
    withPtr.tier should be (2)
    withPtr.ordering should be (ByteArrayKeyOrdering)
    withPtr.orootObject should be (Some(ptr))
    withPtr.orootObject.get.objectType should be (ptr.objectType)

    val without = roundTrip(Root(0, ByteArrayKeyOrdering, None, allocator, kind))
    without.kind should be (kind)
    without.orootObject should be (None)

  test("KeyValueObjects root round-trips with and without a root pointer"):
    check(TreeKind.KeyValueObjects, kvPointer)

  test("DataObjects root round-trips with and without a root pointer"):
    check(TreeKind.DataObjects, dataPointer)

  test("kind defaults to KeyValueObjects"):
    Root(0, ByteArrayKeyOrdering, None, allocator).kind should be (TreeKind.KeyValueObjects)

  test("rootKeyValuePointer is the pointer for key-value roots only"):
    Root(0, ByteArrayKeyOrdering, Some(kvPointer), allocator).rootKeyValuePointer.map(_.id) should be (Some(kvPointer.id))
    Root(0, ByteArrayKeyOrdering, None, allocator).rootKeyValuePointer should be (None)

  test("decoding a DataObjects root holding a KeyValueObjectPointer fails"):
    val bytes = Root(1, ByteArrayKeyOrdering, Some(kvPointer), allocator, TreeKind.DataObjects).encode()
    an [Exception] should be thrownBy Root(null, bytes)
