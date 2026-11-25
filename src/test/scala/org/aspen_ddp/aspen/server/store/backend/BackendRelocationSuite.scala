package org.aspen_ddp.aspen.server.store.backend

import org.aspen_ddp.aspen.FileBasedTests
import org.aspen_ddp.aspen.common.{DataBuffer, HLCTimestamp}
import org.aspen_ddp.aspen.common.ida.Replication
import org.aspen_ddp.aspen.common.objects.{DataObjectPointer, Metadata, ObjectId, ObjectRefcount, ObjectRevision,
  ObjectType}
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.{ReadState, StoreId}
import org.aspen_ddp.aspen.common.objects.ReadError
import org.aspen_ddp.aspen.common.transaction.TransactionId
import org.aspen_ddp.aspen.common.metadata.StorageDeviceId
import org.aspen_ddp.aspen.server.{DurableFiles, StorageDeviceConfig, StoreConfig}

import java.nio.ByteBuffer
import java.util.UUID
import scala.concurrent.{Await, ExecutionContext, Promise}
import scala.concurrent.duration.*

/** The copy layout renames a closed store's directory between phase directories (spec "Device layout"): no backend
 *  may record its own absolute path */
class BackendRelocationSuite extends FileBasedTests:

  private val storeId = StoreId(PoolId(new UUID(9, 1)), 0)
  private val oid = ObjectId(new UUID(9, 2))
  // FSIF locates the object by a 4-byte slot index; RocksDB ignores it
  private val ptr = DataObjectPointer(oid, storeId.poolId, ByteBuffer.allocate(4).putInt(3).array())
  private val txid = TransactionId(new UUID(9, 3))
  private val metadata = Metadata(ObjectRevision(txid), ObjectRefcount(1, 1), HLCTimestamp(1))
  private val Timeout = 20.seconds

  private final class Completions extends CompletionHandler:
    val committed: Promise[Either[Unit, CommitError.Value]] = Promise()
    val read: Promise[Either[ReadState, ReadError.Value]] = Promise()
    def complete(op: Completion): Unit = op match
      case c: Commit => committed.trySuccess(c.result)
      case r: Read => read.trySuccess(r.result)
      case _ => ()

  private def open(cfg: StoreConfig, path: os.Path): (Backend, Completions) =
    val opener = new DefaultBackendOpener(ExecutionContext.global)
    // The device config an FSIF open's lazy journal creation sizes the device's journal by
    os.write.over(path / os.up / StorageDeviceConfig.configFilename,
      StorageDeviceConfig(StorageDeviceId(new UUID(9, 4)), new UUID(9, 5), fsifJournalFiles = Some(3),
        fsifJournalFileSizeMb = Some(1)).yamlConfig)
    val backend = Await.result(opener.open(cfg, path, _ => (), org.aspen_ddp.aspen.server.TestDeviceIo.of(path / os.up)), Timeout)
    val c = new Completions
    backend.setCompletionHandler(c)
    (backend, c)

  test("a closed RocksDB and FSIF store opens again after its directory is renamed"):
    val backends = List(StoreConfig.RocksDB(None), StoreConfig.Fsif(1024, 64))
    backends.foreach: kind =>
      withClue(kind):
        val cfg = StoreConfig(storeId, kind, None, Replication(3, 2))
        val dev = tdir / kind.getClass.getSimpleName
        os.makeDir.all(dev)
        val before = dev / "flat"
        DefaultBackendOpener.createStore(cfg, before, dev / StoreCreation.CreatingDirectory)

        val (b1, c1) = open(cfg, before)
        b1.commit(CommitState(ptr, metadata, ObjectType.Data, DataBuffer(Array[Byte](1, 2, 3)), allocating = true), txid)
        Await.result(c1.committed.future, Timeout) should be(Left(()))
        Await.result(b1.close(), Timeout)

        val after = dev / "phase" / "moved"
        os.makeDir.all(after / os.up)
        DurableFiles.move(before, after)

        val (b2, c2) = open(cfg, after)
        try
          b2.path should be(after)
          b2.read(ptr)
          Await.result(c2.read.future, Timeout) match
            case Left(state) => state.data.getByteArray.toList should be(List[Byte](1, 2, 3))
            case Right(err) => fail(s"The relocated store did not read its object: $err")
        finally Await.result(b2.close(), Timeout)
