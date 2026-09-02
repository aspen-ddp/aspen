package org.aspen_ddp.aspen.amoebafs.impl.simple

import org.aspen_ddp.aspen.IntegrationTestSuite
import org.aspen_ddp.aspen.amoebafs.{FileSystem, FileType}
import org.aspen_ddp.aspen.client.ObjectAllocator
import org.aspen_ddp.aspen.client.internal.allocation.PoolObjectAllocator
import org.aspen_ddp.aspen.common.objects.{KeyAlreadyExists, KeyValueObjectPointer}

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

/** Covers the registry path a filesystem is located through, which the pointer-embedded-in-the-
 *  radicle scheme did not need: name -> id -> root object.
 *
 *  Every test waits for transactions to quiesce after a create. TestNetwork.TClient makes a
 *  single attempt with no recovery, so a read issued against an unsettled transaction is flaky.
 */
class FileSystemRegistrationSuite extends IntegrationTestSuite:

  private def allocator(): Future[ObjectAllocator] =
    given ExecutionContext = executionContext
    client.getStoragePool(radicle.poolId).map(pool => new PoolObjectAllocator(client, pool))

  private def create(name: String): Future[(UUID, KeyValueObjectPointer)] =
    given ExecutionContext = executionContext
    for
      alloc <- allocator()
      r     <- SimpleFileSystem.create(client, alloc, name)
      _     <- waitForTransactionsToComplete()
    yield r

  atest("a created filesystem loads back by name"):
    given ExecutionContext = executionContext
    for
      (fsId, _) <- create("myfs")
      // Nothing of the created filesystem is carried over in memory, so this is the same path
      // a second process takes.
      fs        <- SimpleFileSystem.load(client, "myfs", 1)
    yield
      fs.uuid should be(fsId)

  atest("both registry hops resolve to what create returned"):
    given ExecutionContext = executionContext
    for
      (fsId, fsRoot) <- create("myfs")
      byName         <- client.getRegisteredId(FileSystem.RegistryNamespace, "myfs")
      byId           <- client.getRegisteredKeyValueObject(fsId)
    yield
      // Asserted independently of the load path so a failure above is diagnosable to a hop.
      byName should be(fsId)
      byId should be(fsRoot)

  atest("a duplicate name fails with KeyAlreadyExists and changes nothing"):
    given ExecutionContext = executionContext
    for
      (fsId, _) <- create("myfs")
      alloc     <- allocator()
      err       <- SimpleFileSystem.create(client, alloc, "myfs").failed
      _         <- waitForTransactionsToComplete()
      byName    <- client.getRegisteredId(FileSystem.RegistryNamespace, "myfs")
      all       <- client.listRegisteredIds(FileSystem.RegistryNamespace)
    yield
      // Unwrapped rather than a StopRetrying: create makes a single transact() attempt, so
      // KeyAlreadyExists comes straight out of Registry.prepareRegister.
      err shouldBe a[KeyAlreadyExists]
      // The second attempt claims the name before it allocates, and the transaction it
      // invalidated took the whole attempt with it -- so the name still points at the first.
      byName should be(fsId)
      all should be(List("myfs" -> fsId))

  atest("loading an unregistered name fails with NoSuchElementException"):
    given ExecutionContext = executionContext
    for
      err <- SimpleFileSystem.load(client, "nosuchfs", 1).failed
    yield
      // The CLI's "filesystem 'X' not found" message keys off this type.
      err shouldBe a[NoSuchElementException]

  atest("two filesystems coexist under distinct names"):
    given ExecutionContext = executionContext
    for
      (firstId, _)  <- create("myfs")
      (secondId, _) <- create("second")
      first         <- SimpleFileSystem.load(client, "myfs", 1)
      second        <- SimpleFileSystem.load(client, "second", 1)
      all           <- client.listRegisteredIds(FileSystem.RegistryNamespace)
    yield
      firstId should not be secondId
      first.uuid should be(firstId)
      second.uuid should be(secondId)
      all.toSet should be(Set("myfs" -> firstId, "second" -> secondId))

  atest("a filesystem loaded by name is usable"):
    given ExecutionContext = executionContext
    for
      _            <- create("myfs")
      fs           <- SimpleFileSystem.load(client, "myfs", 1)
      (rootInode, _, _) <- fs.readInode(1)
    yield
      // Proves the root object the registry handed back carries the inode table, not just
      // that it decoded as a KeyValueObjectPointer.
      rootInode.fileType should be(FileType.Directory)
