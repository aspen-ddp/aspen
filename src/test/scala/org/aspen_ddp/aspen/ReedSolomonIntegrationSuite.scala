package org.aspen_ddp.aspen

import org.aspen_ddp.aspen.client.Transaction
import org.aspen_ddp.aspen.common.Radicle
import org.aspen_ddp.aspen.common.ida.{IDA, ReedSolomon}
import org.aspen_ddp.aspen.common.metadata.BootstrapConfig
import org.aspen_ddp.aspen.common.objects.{Delete, Insert, Key, ObjectId, Value}
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.transaction.KeyValueUpdate

import scala.concurrent.Future
import scala.language.implicitConversions

/** Integration coverage for the ReedSolomon IDA against a live, RS-bootstrapped pool.
  *
  * ReedSolomonSuite proves the codec in isolation. This suite proves the rest of the system
  * cooperates with it: that an RS IDA survives bootstrap and the pool-config round trip, that
  * stores really do hold shards rather than replicas, and that objects restore correctly when
  * fewer than `width` stores answer a read.
  *
  * That last point is why the degraded-read tests matter. With every store healthy a read always
  * collects data shards 0 until k and takes the fast path, so `decodeMissing` -- the entire Galois
  * side of the implementation -- is never reached by any other integration test.
  */
trait ReedSolomonIntegrationTests:
  this: IntegrationTestSuite =>

  import ReedSolomonIntegrationTests.*

  private def ida: IDA = net.ida
  private def k: Int = ida.restoreThreshold

  /** Compares by length and first mismatching index rather than by deep equality. The objects here
    * run to thousands of bytes, and a whole-array diff in the failure output buries the one fact
    * that matters.
    */
  private def bytesShouldMatch(actual: Array[Byte], expected: Array[Byte]): Unit =
    withClue("length: "):
      actual.length should be (expected.length)
    val mismatch = actual.indices.find(i => actual(i) != expected(i))
    withClue(s"bytes differ at index ${mismatch.getOrElse(-1)}: "):
      mismatch should be (None)

  /** StoreIds by pool index, for the read-silencing helper. */
  private def storeIds(indices: Iterable[Int]): Set[StoreId] =
    indices.map(i => net.stores(i).storeId).toSet

  /** Every set of stores that can be silenced while still leaving a readable quorum: all subsets
    * up to `width - consistentRestoreThreshold` in size, including the empty one. For
    * ReedSolomon(5,3,4) that is 16 sets; for ReedSolomon(4,2,3), 11. Enumerating them rather than
    * hand-picking a few is what guarantees the parity-only and mixed data/parity cases are
    * covered for whichever config the concrete suite bootstraps with.
    */
  private def silenceableSubsets: List[Set[Int]] =
    val maxSilenced = ida.width - ida.consistentRestoreThreshold
    (0 to maxSilenced).toList.flatMap: size =>
      (0 until ida.width).toList.combinations(size).map(_.toSet)

  /** Must be awaited before any silenced read.
    *
    * Two things have to have settled first. `commit()` resolves when the transaction is decided,
    * which is earlier than every store having applied it, and a read that reaches an unsilenced
    * store before it applies gets ObjectNotFound -- which, with no spare stores left to ask, drops
    * the read below threshold and hangs it forever under noErrorRecoveryReadDriver. And the
    * server-side finalizers share this client, so their reads of the radicle would otherwise land
    * inside the silencing window too.
    */
  private def drainBeforeSilencing(): Future[Unit] = net.waitForTransactionsToComplete()

  /** Fails the test if any store is missing `objectId` or holds a stale revision of it.
    *
    * The degraded reads silence every store they can and still leave a quorum, which only works if
    * all `width` stores are current. A store that is behind pushes the read below threshold, and
    * noErrorRecoveryReadDriver has no timeout -- the test would hang rather than fail. Checking
    * first converts that into a legible failure. It holds for the objects used here because each
    * is written by exactly one transaction, its allocation; an object updated afterwards would not
    * be safe to assert this about, since the harness routinely leaves one store behind on updates.
    */
  private def requireAllStoresCurrent(objectId: ObjectId): Unit =
    val states = net.stores.map(store => store.storeId -> store.m.get(objectId))

    withClue(s"stores missing the object: ${states.filter(_._2.isEmpty).map(_._1)}: "):
      states.forall(_._2.isDefined) should be (true)

    val revisions = states.flatMap(_._2).map(_.metadata.revision).distinct
    withClue(s"stores disagree on the revision: $revisions: "):
      revisions.length should be (1)

  /** Runs `f` over the items one at a time. Sequencing matters for the degraded reads: each read
    * has to be the only thing inside its withReadSilenced window, which the window restores on
    * completion.
    */
  private def sequentially[A, B](items: List[A])(f: A => Future[B]): Future[List[B]] =
    val reversed = items.foldLeft(Future.successful(List.empty[B])): (acc, item) =>
      acc.flatMap(rs => f(item).map(r => r :: rs))
    reversed.map(_.reverse)

  // -- Configuration ---------------------------------------------------------------------

  atest("the bootstrapped storage pool reports the reed-solomon IDA"):
    client.getStoragePool(Radicle.poolId).map: pool =>
      // Round trips the IDA through serializeIDAType/deserializeIDAType inside the pool config.
      pool.ida should be (testIda)

  atest("the generated bootstrap config parses back to the reed-solomon IDA"):
    // The config lives in the radicle as a KV value, so this restores an IDA-encoded value too.
    client.getBootstrapConfig().map: yaml =>
      val config = BootstrapConfig.parseBootstrapConfig(yaml)
      config.bootstrapIDA should be (testIda)
      config.hosts.map(_.stores.length).sum should be (ida.width)

  // -- Dispersal -------------------------------------------------------------------------

  atest("stores hold erasure-coded shards rather than whole replicas"):
    val n = 1000
    val content = testContent(n)
    val segmentLength = ida.calculateEncodedSegmentLength(n)

    given tx: Transaction = client.newTransaction()

    for
      pool <- client.getStoragePool(Radicle.poolId)
      dp <- pool.allocator.allocateDataObject(content)
      _ <- tx.commit()
      // commit() resolves once the transaction is decided, which is earlier than every store
      // having applied it to its backend. Drain before reaching into the MapBackends.
      _ <- net.waitForTransactionsToComplete()
    yield
      val shards = net.stores.map(store => store.m(dp.id).data.getByteArray)

      shards.length should be (ida.width)
      shards.foreach(shard => shard.length should be (segmentLength))

      // Systematic code: shards 0 until k concatenated are the padded content.
      bytesShouldMatch(shards.take(k).reduce(_ ++ _).take(n), content)

      // And nothing holds the plaintext, which is what would happen under Replication.
      shards.exists(shard => shard.sameElements(content)) should be (false)

  // -- Data objects ----------------------------------------------------------------------

  atest("data objects round trip at sizes spanning the shard boundary"):
    // One transaction for the whole sweep: atest drains transactions once per test, so a
    // transaction per size would cost far more than it proves.
    val sizes = List(0, 1, 2, 3, 4, 5, 7, 8, 100, 1000, 4096, 65536)

    given tx: Transaction = client.newTransaction()

    for
      pool <- client.getStoragePool(Radicle.poolId)
      pointers <- Future.sequence(sizes.map(n => pool.allocator.allocateDataObject(testContent(n))))
      _ <- tx.commit()
      states <- Future.sequence(pointers.map(p => client.read(p)))
    yield
      sizes.zip(states).foreach: (n, dos) =>
        withClue(s"size $n: "):
          dos.data.size should be (n)
          bytesShouldMatch(dos.data.getByteArray, testContent(n))
      succeed

  atest("overwriting a data object resizes its shards in both directions"):
    val initial = testContent(2000)
    val smaller = testContent(7)
    val larger = testContent(5000)

    given tx: Transaction = client.newTransaction()

    for
      pool <- client.getStoragePool(Radicle.poolId)
      dp <- pool.allocator.allocateDataObject(initial)
      _ <- tx.commit()

      first <- client.read(dp)

      shrinkTx = client.newTransaction()
      _ = shrinkTx.overwrite(dp, first.revision, smaller)
      _ <- shrinkTx.commit()

      second <- client.read(dp)

      growTx = client.newTransaction()
      _ = growTx.overwrite(dp, second.revision, larger)
      _ <- growTx.commit()

      third <- client.read(dp)
      _ <- net.waitForTransactionsToComplete()
    yield
      bytesShouldMatch(first.data.getByteArray, initial)
      bytesShouldMatch(second.data.getByteArray, smaller)
      bytesShouldMatch(third.data.getByteArray, larger)

      // Only writeThreshold stores are guaranteed to have applied the overwrite. This harness
      // routinely leaves one store behind on an update -- see TestNetwork's note about missing
      // the third store -- and with no background task manager running, nothing repairs it.
      val sizes = net.stores.map(store => store.m(dp.id).data.size)
      withClue(s"shard sizes $sizes: "):
        sizes.count(_ == ida.calculateEncodedSegmentLength(larger.length)) should
          be >= ida.writeThreshold

  // -- Key value objects -----------------------------------------------------------------

  atest("key value objects round trip, including min, max, left and right"):
    // left/right are SingleEncodedValue and so are IDA-encoded; min/max are
    // SingleReplicatedValue and must come back byte-for-byte from any single store.
    val valueSizes = List(0, 1, k - 1, k, k + 1, 2 * k + 1, 500).filter(_ >= 0).distinct
    val entries = valueSizes.zipWithIndex.map((n, i) => keyOf(i) -> Value(testContent(n))).toMap

    val minimum = Key(Array[Byte](0))
    val maximum = Key(Array[Byte](-1))
    val left = Value(testContent(37))
    val right = Value(testContent(300))

    given tx: Transaction = client.newTransaction()

    for
      pool <- client.getStoragePool(Radicle.poolId)
      kp <- pool.allocator.allocateKeyValueObject(entries, Some(minimum), Some(maximum),
                                                  Some(left), Some(right))
      _ <- tx.commit()
      kvos <- client.read(kp)
    yield
      kvos.minimum should be (Some(minimum))
      kvos.maximum should be (Some(maximum))
      bytesShouldMatch(kvos.left.get.bytes, left.bytes)
      bytesShouldMatch(kvos.right.get.bytes, right.bytes)

      kvos.contents.size should be (entries.size)
      entries.foreach: (key, value) =>
        withClue(s"key ${key.bytes.toList}: "):
          bytesShouldMatch(kvos.contents(key).value.bytes, value.bytes)
      succeed

  atest("key value inserts, updates and deletes survive successive transactions"):
    val key = keyOf(0)
    val other = keyOf(1)
    val original = Value(testContent(101))
    val replacement = Value(testContent(3003))

    given tx: Transaction = client.newTransaction()

    for
      pool <- client.getStoragePool(Radicle.poolId)
      kp <- pool.allocator.allocateKeyValueObject(Map(key -> original))
      _ <- tx.commit()

      afterInsert <- client.read(kp)

      updateTx = client.newTransaction()
      _ = updateTx.update(kp, None, None,
                          List(KeyValueUpdate.KeyRevision(key, afterInsert.contents(key).revision)),
                          List(Insert(key, replacement.bytes), Insert(other, original.bytes)))
      _ <- updateTx.commit()

      afterUpdate <- client.read(kp)

      deleteTx = client.newTransaction()
      _ = deleteTx.update(kp, None, None,
                          List(KeyValueUpdate.Exists(key)),
                          List(Delete(key)))
      _ <- deleteTx.commit()

      afterDelete <- client.read(kp)
    yield
      bytesShouldMatch(afterInsert.contents(key).value.bytes, original.bytes)

      bytesShouldMatch(afterUpdate.contents(key).value.bytes, replacement.bytes)
      bytesShouldMatch(afterUpdate.contents(other).value.bytes, original.bytes)

      afterDelete.contents.contains(key) should be (false)
      bytesShouldMatch(afterDelete.contents(other).value.bytes, original.bytes)
      succeed

  atest("a key value object is deleted when its refcount drops to zero"):
    given tx: Transaction = client.newTransaction()

    for
      pool <- client.getStoragePool(Radicle.poolId)
      kp <- pool.allocator.allocateKeyValueObject(Map(keyOf(0) -> Value(testContent(600))))
      _ <- tx.commit()

      kvos <- client.read(kp)

      deleteTx = client.newTransaction()
      _ = deleteTx.setRefcount(kp, kvos.refcount, kvos.refcount.decrement())
      _ <- deleteTx.commit()

      after <- client.readOptional(kp)
    yield
      after.isEmpty should be (true)

  // -- Degraded reads --------------------------------------------------------------------

  atest("data objects restore from every readable subset of stores"):
    // Silencing ReadResponses leaves the shards on disk but hides them from the reader, so the
    // restore must run decodeMissing over whatever indices did answer -- parity-only subsets
    // included.
    val n = 7777
    val content = testContent(n)

    given tx: Transaction = client.newTransaction()

    for
      pool <- client.getStoragePool(Radicle.poolId)
      dp <- pool.allocator.allocateDataObject(content)
      _ <- tx.commit()
      _ <- drainBeforeSilencing()
      _ = requireAllStoresCurrent(dp.id)

      results <- sequentially(silenceableSubsets): silenced =>
                   val read = net.withReadSilenced(storeIds(silenced))(client.read(dp))
                   read.map(dos => silenced -> dos.data.getByteArray)
    yield
      results.foreach: (silenced, restored) =>
        withClue(s"silenced ${silenced.toList.sorted}: "):
          bytesShouldMatch(restored, content)
      succeed

  atest("key value objects restore from every readable subset of stores"):
    // Covers the encoded left/right attributes as well as the per-key values. If left/right were
    // stored verbatim rather than IDA-encoded, a parity-only subset would restore garbage here
    // even though the all-healthy read above succeeds.
    val key = keyOf(0)
    val value = Value(testContent(2049))
    val left = Value(testContent(130))
    val right = Value(testContent(1))
    val minimum = Key(Array[Byte](0))
    val maximum = Key(Array[Byte](-1))

    given tx: Transaction = client.newTransaction()

    for
      pool <- client.getStoragePool(Radicle.poolId)
      kp <- pool.allocator.allocateKeyValueObject(Map(key -> value), Some(minimum), Some(maximum),
                                                  Some(left), Some(right))
      _ <- tx.commit()
      _ <- drainBeforeSilencing()
      _ = requireAllStoresCurrent(kp.id)

      results <- sequentially(silenceableSubsets): silenced =>
                   val read = net.withReadSilenced(storeIds(silenced))(client.read(kp))
                   read.map(kvos => silenced -> kvos)
    yield
      results.foreach: (silenced, kvos) =>
        withClue(s"silenced ${silenced.toList.sorted}: "):
          bytesShouldMatch(kvos.contents(key).value.bytes, value.bytes)
          bytesShouldMatch(kvos.left.get.bytes, left.bytes)
          bytesShouldMatch(kvos.right.get.bytes, right.bytes)
          kvos.minimum should be (Some(minimum))
          kvos.maximum should be (Some(maximum))
      succeed


object ReedSolomonIntegrationTests:

  /** Deterministic, non-uniform content. Ascending bytes would hide a shard-ordering bug behind a
    * plausible-looking result; this does not.
    */
  def testContent(nbytes: Int): Array[Byte] =
    Array.tabulate(nbytes)(i => ((i * 37 + (i / 251) * 11 + 7) & 0xFF).toByte)

  def keyOf(index: Int): Key = Key(Array[Byte](0x40, index.toByte))


/** The primary configuration: two parity shards, so reads can lose two stores and readable subsets
  * exist that mix data and parity shards.
  */
class ReedSolomonIntegrationSuite extends IntegrationTestSuite with ReedSolomonIntegrationTests:
  override def testIda: IDA = ReedSolomon(5, 3, 4)


/** A second configuration with a different k, and one where a readable subset can consist of
  * nothing but parity shards. ReedSolomon(3,2,3) would be the tighter test of narrowness, but its
  * writeThreshold equals its width, so a single missed store would stall every commit.
  */
class NarrowReedSolomonIntegrationSuite extends IntegrationTestSuite with ReedSolomonIntegrationTests:
  override def testIda: IDA = ReedSolomon(4, 2, 3)
