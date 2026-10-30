package org.aspen_ddp.aspen.server.store.backend.fsif

import java.util.UUID

import scala.concurrent.{Await, ExecutionContext}
import scala.concurrent.duration.*

import org.aspen_ddp.aspen.FileBasedTests
import org.aspen_ddp.aspen.common.ida.{ReedSolomon, Replication}
import org.aspen_ddp.aspen.common.objects.ObjectId
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.server.store.backend.FsifConfig

class FsifInitializationSuite extends FileBasedTests:

  private val layout = FsifLayout(1024, 64)
  private val storeId = StoreId(PoolId(new UUID(7, 7)), 0)
  private val io = ExecutionContext.global
  private val Timeout = 10.seconds
  private val Unmapped = (-1, ObjectId(new UUID(0, 0)))

  private def close(b: FsifBackend): Unit = Await.result(b.close(), Timeout)

  test("create writes the fsif-initialized marker"):
    val dir = tdir / "store"
    val b = FsifBackend.create(storeId, dir, layout, io, _ => ())
    try
      os.exists(dir / FsifBackend.InitializedMarkerFile) shouldBe true
    finally close(b)

  test("a store whose creation was cut off before the marker opens as an empty store"):
    val dir = tdir / "store"
    os.makeDir.all(dir)
    // initialize writes data and journal.a before index.a: a crash in between leaves no valid index
    os.write(dir / FsifRecovery.DataFile, Array.fill[Byte](100)(5))
    os.write(dir / FsifRecovery.JournalFiles(0), Array.fill[Byte](10)(9))
    an[FsifRecovery.StoreLost] should be thrownBy
      new FsifBackend(storeId, dir, layout, new NioFsifFiles(dir), io)

    val b = FsifBackend.open(storeId, dir, layout, io, _ => ())
    try
      os.exists(dir / FsifBackend.InitializedMarkerFile) shouldBe true
      b.slotCount shouldBe 0
      Await.result(b.mapping(0), Timeout) shouldBe Unmapped
      Await.result(b.mapping(63), Timeout) shouldBe Unmapped
    finally close(b)

  test("a fully initialized store without its marker also opens as an empty store"):
    val dir = tdir / "store"
    os.makeDir.all(dir)
    FsifRecovery.initialize(new NioFsifFiles(dir), layout)
    val b = FsifBackend.open(storeId, dir, layout, io, _ => ())
    try
      os.exists(dir / FsifBackend.InitializedMarkerFile) shouldBe true
      b.slotCount shouldBe 0
    finally close(b)

  test("a marked store with no valid index is lost, not re-initialized"):
    val dir = tdir / "store"
    close(FsifBackend.create(storeId, dir, layout, io, _ => ()))
    os.remove(dir / FsifRecovery.IndexFiles(0))
    an[FsifRecovery.StoreLost] should be thrownBy FsifBackend.open(storeId, dir, layout, io, _ => ())
    os.exists(dir / FsifRecovery.IndexFiles(0)) shouldBe false

  test("a marked store reopens through open"):
    val dir = tdir / "store"
    close(FsifBackend.create(storeId, dir, layout, io, _ => ()))
    val b = FsifBackend.open(storeId, dir, layout, io, _ => ())
    close(b)

  test("FsifConfig.forObjectSize sizes segments with ida.minSliceSizeFor"):
    val r = Replication(3, 2)
    r.minSliceSizeFor(10 << 20) shouldBe (10L << 20)
    FsifConfig.forObjectSize(r, 10 << 20, 10) shouldBe FsifConfig(10 << 20, 10)
    FsifConfig.forObjectSize(r, 10 << 20, 10).maxSliceSize shouldBe Some(10 << 20)

    val rs = ReedSolomon(5, 3, 4)
    val cfg = FsifConfig.forObjectSize(rs, 1 << 20, 1000)
    cfg.segmentPayloadSize.toLong shouldBe rs.minSliceSizeFor(1 << 20)
    cfg.segmentCount shouldBe 1000
    rs.maxObjectSize(cfg.segmentPayloadSize) should be >= (1 << 20)
