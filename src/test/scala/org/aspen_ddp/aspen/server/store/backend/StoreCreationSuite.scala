package org.aspen_ddp.aspen.server.store.backend

import java.util.UUID

import org.aspen_ddp.aspen.FileBasedTests
import org.aspen_ddp.aspen.common.ida.Replication
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.server.StoreConfig
import org.aspen_ddp.aspen.server.crl.AdmissionTier

class StoreCreationSuite extends FileBasedTests:

  private val storeId = StoreId(PoolId(new UUID(3, 3)), 1)
  private val cfg = StoreConfig(storeId, StoreConfig.RocksDB(None), None, Replication(3, 2), AdmissionTier.Data)
  private def creating = tdir / StoreCreation.CreatingDirectory
  private def noScratch(): Unit = (!os.exists(creating) || os.list(creating).isEmpty) shouldBe true

  test("a create places the complete directory at the target and leaves no scratch copy"):
    val target = tdir / storeId.directoryName
    StoreCreation.createAtomically(cfg, target, creating): dir =>
      os.exists(target) shouldBe false
      os.write(dir / "data", "x")
    os.read(target / "data") shouldBe "x"
    StoreConfig.loadStoreConfig(target / StoreConfig.configFilename).storeId shouldBe storeId
    noScratch()

  test("a failing init leaves the target absent and removes the scratch copy"):
    val target = tdir / storeId.directoryName
    an[IllegalStateException] should be thrownBy
      StoreCreation.createAtomically(cfg, target, creating): dir =>
        os.write(dir / "data", "x")
        throw new IllegalStateException("init failed")
    os.exists(target) shouldBe false
    noScratch()

  test("an existing target is never replaced"):
    val target = tdir / storeId.directoryName
    os.makeDir.all(target)
    os.write(target / "data", "old")
    an[Exception] should be thrownBy
      StoreCreation.createAtomically(cfg, target, creating)(dir => os.write(dir / "data", "new"))
    os.read(target / "data") shouldBe "old"
    noScratch()
