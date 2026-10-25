package org.aspen_ddp.aspen.server.store.backend

import org.aspen_ddp.aspen.server.store.backend.RocksDBBackend.ResourceConfig
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class RocksDBBackendResourcesSuite extends AnyFunSuite with Matchers:

  test("thread pools keep RocksDB's minimum of one compaction and one flush thread"):
    RocksDBBackend.threadAllocation(0, 16) should be((1, 1))
    RocksDBBackend.threadAllocation(1, 16) should be((1, 1))

  test("thread pools grow by two threads per open database"):
    RocksDBBackend.threadAllocation(2, 16) should be((3, 1))
    RocksDBBackend.threadAllocation(4, 16) should be((6, 2))

  test("thread pools are capped by maxBackgroundThreads"):
    RocksDBBackend.threadAllocation(100, 16) should be((12, 4))
    RocksDBBackend.threadAllocation(100, 2) should be((1, 1))

  test("resolve derives unspecified values from the machine"):
    val rc = ResourceConfig.resolve()
    rc.memoryBytes should be > 0L
    rc.writeBufferBytes should be((rc.memoryBytes * ResourceConfig.DefaultWriteBufferFraction).toLong)
    rc.maxBackgroundThreads should be(math.max(2, Runtime.getRuntime.availableProcessors))

  test("resolve keeps explicitly specified values"):
    ResourceConfig.resolve(Some(1000L), Some(100L), Some(4)) should be(ResourceConfig(1000L, 100L, 4))

  test("invalid limits are rejected"):
    an [IllegalArgumentException] should be thrownBy ResourceConfig(0L, 0L, 4)
    an [IllegalArgumentException] should be thrownBy ResourceConfig(100L, 200L, 4)
    an [IllegalArgumentException] should be thrownBy ResourceConfig(100L, 50L, 1)

  test("decodeDBValue rejects an unknown object type byte"):
    an [RocksDBBackend.CorruptedObjectEncoding] should be thrownBy
      RocksDBBackend.decodeDBValue(Array[Byte](7, 0, 0, 0, 0))
