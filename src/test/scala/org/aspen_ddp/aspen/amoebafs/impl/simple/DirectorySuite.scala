package org.aspen_ddp.aspen.amoebafs.impl.simple

import java.nio.charset.StandardCharsets
import org.aspen_ddp.aspen.PinAudit
import org.aspen_ddp.aspen.client.KeyValueObjectState.ValueState
import org.aspen_ddp.aspen.client.Transaction
import org.aspen_ddp.aspen.client.tkvl.{Root, RootAlreadyModified, SinglePoolNodeAllocator, TieredKeyValueList}
import org.aspen_ddp.aspen.common.objects.{DataObjectPointer, Delete, Insert, Key, KeyAlreadyExists, LexicalKeyOrdering, ObjectPointer, ObjectRevision}
import org.aspen_ddp.aspen.common.transaction.KeyValueUpdate
import org.aspen_ddp.aspen.amoebafs.error.{DirectoryEntryDoesNotExist, DirectoryEntryExists, DirectoryNotEmpty}
import org.aspen_ddp.aspen.amoebafs.{Directory, DirectoryInode, DirectoryPointer, FileInode, FileMode, FilePointer, FileSystem, Timespec}
import org.aspen_ddp.aspen.common.DataBuffer

import scala.concurrent.Future

class DirectorySuite extends FilesSystemTestSuite:
  def cdir(dir: Directory, name: String, mode: Int, uid: Int, gid: Int): Future[DirectoryPointer] =
    given tx: Transaction = dir.fs.client.newTransaction()
    val fprep = dir.prepareCreateDirectory(name, mode, uid, gid)
    fprep.foreach(_ => tx.commit())
    fprep.flatMap(fresult => fresult)

  atest("Amoeba Bootstrap"):
    for
      fs <- bootstrap()
      rootDir <- fs.loadRoot()
      (rootInode, _) <- rootDir.getInode()
    yield
      rootInode.uid should be (0)

  atest("Create Directory"):
    for
      fs <- bootstrap()
      rootDir <- fs.loadRoot()
      initialContent <- rootDir.getContents()
      newDirPointer <- cdir(rootDir, "foo", mode=0, uid=1, gid=2)
      newDir <- fs.loadDirectory(newDirPointer)
      (newInode, _) <- newDir.getInode()
      newContent <- rootDir.getContents()
    yield
      initialContent.length should be (0)
      newInode.uid should be (1)
      newInode.gid should be (2)
      newContent.length should be (1)
      newContent.head.name should be ("foo")

  atest("Change Directory UID"):
    for
      fs <- bootstrap()
      rootDir <- fs.loadRoot()
      _ <- rootDir.getContents()
      newDirPointer <- cdir(rootDir,"foo", mode=0, uid=1, gid=2)
      newDir <- fs.loadDirectory(newDirPointer)
      origUID = newDir.uid
      _ <- newDir.setUID(5)
      newDir2 <- fs.loadDirectory(newDirPointer)
    yield
      origUID should be (1)
      newDir.uid should be (5)
      newDir2.uid should be (5)

  atest("Change Directory UID with recovery from revision mismatch"):
    def vbump(ptr: ObjectPointer, revision: ObjectRevision): Future[Unit] =
      given tx: Transaction = client.newTransaction()
      tx.bumpVersion(ptr, revision)
      tx.commit().map(_=>())

    for
      fs <- bootstrap()
      rootDir <- fs.loadRoot()
      _ <- rootDir.getContents()

      newDirPointer <- cdir(rootDir,"foo", mode=0, uid=1, gid=2)
      newDir <- fs.loadDirectory(newDirPointer)
      (_, revision) <- newDir.getInode()
      origUID = newDir.uid

      _ <- vbump(newDir.pointer.pointer, revision)

      _ <- newDir.setUID(5)

      newDir2 <- fs.loadDirectory(newDirPointer)
    yield
      origUID should be (1)
      newDir.uid should be (5)
      newDir2.uid should be (5)

  atest("Change multiple metadata attributes"):
    val u = 6
    val g = 7
    val m = 1
    val ct = Timespec(1,2)
    val mt = Timespec(3,4)
    val at = Timespec(4,5)
    for
      fs <- bootstrap()
      rootDir <- fs.loadRoot()
      _ <- rootDir.getContents()
      newDirPointer <- cdir(rootDir, "foo", mode=0, uid=1, gid=2)
      newDir <- fs.loadDirectory(newDirPointer)
      fu = newDir.setUID(u)
      fg = newDir.setGID(g)
      fm = newDir.setMode(m)
      fc = newDir.setCtime(ct)
      fx = newDir.setMtime(mt)
      fa = newDir.setAtime(at)
      _ <- Future.sequence(List(fu, fg, fm, fc, fx, fa))
      d <- fs.loadDirectory(newDirPointer)
    yield
      d.uid should be (u)
      d.gid should be (g)
      d.mode should be (m | FileMode.S_IFDIR)
      d.ctime should be (ct)
      d.mtime should be (mt)
      d.atime should be (at)

  atest("Delete non-empty Directory"):
    for
      fs <- bootstrap()
      rootDir <- fs.loadRoot()
      initialContent <- rootDir.getContents()
      newDirPointer <- cdir(rootDir, "foo", mode=0, uid=1, gid=2)
      newDir <- fs.loadDirectory(newDirPointer)
      _ <- cdir(newDir, "bar", mode=0, uid=1, gid=2)
      dc <- newDir.getContents()
      if dc.length == 1
      _ <- recoverToSucceededIf[DirectoryNotEmpty](rootDir.delete("foo"))
    yield
      initialContent.length should be (0)

  atest("Create file fails if file already exists"):
    for
      fs <- bootstrap()
      rootDir <- fs.loadRoot()
      initialContent <- rootDir.getContents()
      _ <- cdir(rootDir, "foo", mode=0, uid=1, gid=2)
      _ <- recoverToSucceededIf[DirectoryEntryExists](rootDir.createFile("foo", mode=0, uid=1, gid=2))
    yield
      initialContent.length should be (0)

  atest("Insert file succeeds if file already exists"):
    for
      fs <- bootstrap()
      rootDir <- fs.loadRoot()
      initialContent <- rootDir.getContents()
      _ <- cdir(rootDir, "foo", mode=0, uid=1, gid=2)
      _ <- rootDir.insert("foo", rootDir.pointer)
    yield
      initialContent.length should be (0)

  atest("Insert file fails if file is a non-empty directory"):
    for
      fs <- bootstrap()
      rootDir <- fs.loadRoot()
      initialContent <- rootDir.getContents()
      dp <- cdir(rootDir, "foo", mode = 0, uid = 1, gid = 2)
      d <- fs.loadDirectory(dp)
      _ <- cdir(d, "subdir", mode=0, uid=1, gid=2)
      e <- d.isEmpty()
      _ <- recoverToSucceededIf[DirectoryNotEmpty](rootDir.insert("foo", rootDir.pointer))
    yield
      initialContent.length should be(0)

  atest("Delete file fails if file does not exist"):
    for
      fs <- bootstrap()
      rootDir <- fs.loadRoot()
      initialContent <- rootDir.getContents()
      _ <- cdir(rootDir, "foo", mode=0, uid=1, gid=2)
      _ <- recoverToSucceededIf[DirectoryEntryDoesNotExist](rootDir.delete("INVALID"))
    yield
      initialContent.length should be (0)

  atest("Rename file fails if source file does not exist"):
    for
      fs <- bootstrap()
      rootDir <- fs.loadRoot()
      initialContent <- rootDir.getContents()
      _ <- cdir(rootDir, "foo", mode=0, uid=1, gid=2)
      _ <- recoverToSucceededIf[DirectoryEntryDoesNotExist](rootDir.rename("INVALID", "bar"))
    yield
      initialContent.length should be (0)

  atest("Rename fails if destination file is a non-empty directory"):
    for
      fs <- bootstrap()
      rootDir <- fs.loadRoot()
      initialContent <- rootDir.getContents()
      _ <- cdir(rootDir, "foo", mode=0, uid=1, gid=2)
      barPtr <- cdir(rootDir, "bar", mode=0, uid=1, gid=2)
      barDir <- fs.loadDirectory(barPtr)
      _ <- cdir(barDir, "bar_subdir", mode=0, uid=1, gid=2)
      _ <- recoverToSucceededIf[DirectoryNotEmpty](rootDir.rename("foo", "bar"))
    yield
      initialContent.length should be (0)

  atest("Rename file succeeds if destination file exists"):
    for
      fs <- bootstrap()
      rootDir <- fs.loadRoot()
      initialContent <- rootDir.getContents()
      _ <- cdir(rootDir, "foo", mode = 0, uid = 1, gid = 2)
      _ <- cdir(rootDir, "bar", mode = 0, uid = 1, gid = 2)
      _ <- rootDir.rename("foo", "bar")
    yield
      initialContent.length should be(0)

  atest("HardLink file succeeds if destination file exists"):
    for
      fs <- bootstrap()
      rootDir <- fs.loadRoot()
      initialContent <- rootDir.getContents()
      fooptr <- cdir(rootDir, "foo", mode=0, uid=1, gid=2)
      _ <- cdir(rootDir, "bar", mode=0, uid=1, gid=2)
      foo <- fs.lookup(fooptr)
      _ <- recoverToSucceededIf[DirectoryEntryExists](rootDir.hardLink("bar", foo))
    yield
      initialContent.length should be (0)

  atest("Delete empty Directory"):
    for
      fs <- bootstrap()
      rootDir <- fs.loadRoot()
      _ <- cdir(rootDir, "foo", mode=0, uid=1, gid=2)
      _ <- rootDir.delete("foo")
      oentry <- rootDir.getEntry("foo")
    yield
      oentry should be (None)

  atest("Delete Directory with data tiered list"):
    for
      fs <- bootstrap()
      rootDir <- fs.loadRoot()
      newDirPointer <- cdir(rootDir, "foo", mode=0, uid=1, gid=2)
      newDir <- fs.loadDirectory(newDirPointer)
      _ <- cdir(newDir, "bar", mode=0, uid=1, gid=2)
      dc <- newDir.getContents()
      if dc.length == 1
      _ <- newDir.delete("bar")
      obar <- newDir.getEntry("bar")
      _ <- rootDir.delete("foo")
      ofoo <- rootDir.getEntry("foo")
    yield
      obar should be (None)
      ofoo should be (None)

  atest("Test Symlink"):
    for
      fs <- bootstrap()
      rootDir <- fs.loadRoot()
      _ <- rootDir.getContents()
      sptr <-
        given tx: Transaction = rootDir.fs.client.newTransaction()
        val fprep = rootDir.prepareCreateSymlink("foo", mode=0, uid=1, gid=2, link="bar")
        fprep.foreach(_ => tx.commit())
        fprep.flatMap(fresult => fresult)
      sl1 <- fs.loadSymlink(sptr)
      origSize = sl1.size
      origLink = sl1.symLinkAsString
      _<-sl1.setSymLink("quux".getBytes(StandardCharsets.UTF_8))
      sl2 <- fs.loadSymlink(sptr)
    yield
      origSize should be (3)
      origLink should be ("bar")
      sl1.size should be (4)
      sl1.symLinkAsString should be ("quux")
      sl2.size should be (4)
      sl2.symLinkAsString should be ("quux")

  atest("Test UnixSocket"):
    for
      fs <- bootstrap()
      rootDir <- fs.loadRoot()
      _ <- rootDir.getContents()
      sptr <-
        given tx: Transaction = rootDir.fs.client.newTransaction()
        val fprep = rootDir.prepareCreateUnixSocket("foo", mode=0, uid=1, gid=2)
        fprep.foreach(_ => tx.commit())
        fprep.flatMap(fresult => fresult)
      us <- fs.loadUnixSocket(sptr)
    yield
      us.uid should be (1)

  atest("Test FIFO"):
    for
      fs <- bootstrap()
      rootDir <- fs.loadRoot()
      _ <- rootDir.getContents()
      sptr <-
        given tx: Transaction = rootDir.fs.client.newTransaction()
        val fprep = rootDir.prepareCreateFIFO("foo", mode=0, uid=1, gid=2)
        fprep.foreach(_ => tx.commit())
        fprep.flatMap(fresult => fresult)
      us <- fs.loadFIFO(sptr)
    yield
      us.uid should be (1)

  atest("Test CharacterDevice"):
    for
      fs <- bootstrap()
      rootDir <- fs.loadRoot()
      _ <- rootDir.getContents()
      sptr <-
        given tx: Transaction = rootDir.fs.client.newTransaction()
        val fprep = rootDir.prepareCreateCharacterDevice("foo", mode=0, uid=1, gid=2, rdev=10)
        fprep.foreach(_ => tx.commit())
        fprep.flatMap(fresult => fresult)
      us <- fs.loadCharacterDevice(sptr)
    yield
      us.uid should be (1)
      us.rdev should be (10)

  atest("Test BlockDevice"):
    for
      fs <- bootstrap()
      rootDir <- fs.loadRoot()
      _ <- rootDir.getContents()
      sptr <-
        given tx: Transaction = rootDir.fs.client.newTransaction()
        val fprep = rootDir.prepareCreateBlockDevice("foo", mode=0, uid=1, gid=2, rdev=10)
        fprep.foreach(_ => tx.commit())
        fprep.flatMap(fresult => fresult)
      us <- fs.loadBlockDevice(sptr)
    yield
      us.uid should be (1)
      us.rdev should be (10)

  atest("Test Hardlink"):
    for
      fs <- bootstrap()
      rootDir <- fs.loadRoot()
      _ <- rootDir.getContents()
      sptr <-
        given tx: Transaction = rootDir.fs.client.newTransaction()
        val fprep = rootDir.prepareCreateBlockDevice("foo", mode=0, uid=1, gid=2, rdev=10)
        fprep.foreach(_ => tx.commit())
        fprep.flatMap(fresult => fresult)
      us <- fs.loadBlockDevice(sptr)
      _ <- fs.client.transact { tx =>
        given t: Transaction = tx
        rootDir.prepareHardLink("bar", us)
      }
      us2 <- fs.loadBlockDevice(sptr)
      postLinkContent <- rootDir.getContents()
    yield
      us2.links should be (2)
      postLinkContent.size should be (2)

  // ========== CORNER CASE TESTS ==========

  atest("Create directory with very long name"):
    val longName = "a" * 255 // Maximum typical filename length
    for
      fs <- bootstrap()
      rootDir <- fs.loadRoot()
      newDirPointer <- cdir(rootDir, longName, mode=0, uid=1, gid=2)
      newDir <- fs.loadDirectory(newDirPointer)
      contents <- rootDir.getContents()
    yield
      contents.length should be (1)
      contents.head.name should be (longName)

  atest("Create directory with special characters in name"):
    val specialName = "test-dir_with.special@chars#123"
    for
      fs <- bootstrap()
      rootDir <- fs.loadRoot()
      newDirPointer <- cdir(rootDir, specialName, mode=0, uid=1, gid=2)
      contents <- rootDir.getContents()
    yield
      contents.length should be (1)
      contents.head.name should be (specialName)

  atest("Large directory with many entries"):
    val numEntries = 50
    def rcreate(rootDir: Directory, l: List[Int]): Future[Unit] =
      if l.isEmpty then
        Future.unit
      else
        //println("---------------------------------------------")
        //println(s"Creating directory number ${l.head}")
        cdir(rootDir, s"dir${l.head}", mode=0, uid=1, gid=2).flatMap: _ =>
          rcreate(rootDir, l.tail)

    for
      fs <- bootstrap()
      rootDir <- fs.loadRoot()
      //_ <- Future.sequence((1 to numEntries).map(i => cdir(rootDir, s"dir$i", mode=0, uid=1, gid=2)))
      _ <- rcreate(rootDir, (1 to numEntries).toList)
      contents <- rootDir.getContents()
    yield
      contents.length should be (numEntries)
      contents.map(_.name).toSet should be ((1 to numEntries).map(i => s"dir$i").toSet)

  atest("Deep directory nesting"):
    val depth = 10
    def createNestedDirs(fs: FileSystem, dir: Directory, currentDepth: Int): Future[Directory] =
      if currentDepth == 0 then
        Future.successful(dir)
      else
        val dirName = s"level$currentDepth"
        cdir(dir, dirName, mode=0, uid=1, gid=2).flatMap: newDirPtr =>
          fs.loadDirectory(newDirPtr).flatMap: newDir =>
            createNestedDirs(fs, newDir, currentDepth - 1)

    for
      fs <- bootstrap()
      rootDir <- fs.loadRoot()
      deepestDir <- createNestedDirs(fs, rootDir, depth)
      contents <- deepestDir.getContents()
    yield
      contents.length should be (0) // Deepest directory should be empty

  atest("Concurrent directory modifications"):
    for
      fs <- bootstrap()
      rootDir <- fs.loadRoot()

      // Create multiple directories concurrently
      futures = (1 to 20).map(i => cdir(rootDir, s"concurrent$i", mode=0, uid=1, gid=2))
      _ <- Future.sequence(futures)

      contents <- rootDir.getContents()
    yield
      contents.length should be (20)
      contents.map(_.name).toSet should be ((1 to 20).map(i => s"concurrent$i").toSet)

  atest("Rename with same source and destination name"):
    for
      fs <- bootstrap()
      rootDir <- fs.loadRoot()
      _ <- cdir(rootDir, "samename", mode=0, uid=1, gid=2)
      initialContents <- rootDir.getContents()
      _ <- rootDir.rename("samename", "samename") // Should be no-op
      finalContents <- rootDir.getContents()
    yield
      initialContents.size should be (1)
      finalContents.size should be (1)
      initialContents.head.name should be (finalContents.head.name)

  atest("Multiple renames in sequence"):
    for
      fs <- bootstrap()
      rootDir <- fs.loadRoot()
      _ <- cdir(rootDir, "original", mode=0, uid=1, gid=2)
      _ <- rootDir.rename("original", "renamed1")
      _ <- rootDir.rename("renamed1", "renamed2")
      _ <- rootDir.rename("renamed2", "final")
      contents <- rootDir.getContents()
      oentry1 <- rootDir.getEntry("original")
      oentry2 <- rootDir.getEntry("renamed1")
      oentry3 <- rootDir.getEntry("renamed2")
      ofinal <- rootDir.getEntry("final")
    yield
      contents.length should be (1)
      contents.head.name should be ("final")
      oentry1 should be (None)
      oentry2 should be (None)
      oentry3 should be (None)
      ofinal shouldBe defined

  atest("Directory tree iteration with mixed content"):
    for
      fs <- bootstrap()
      rootDir <- fs.loadRoot()

      // Create mixed content
      dir1Ptr <- cdir(rootDir, "subdir1", mode=0, uid=1, gid=2)
      dir1 <- fs.loadDirectory(dir1Ptr)
      _ <- cdir(dir1, "nested", mode=0, uid=1, gid=2)

      _ <- fs.client.transact { tx =>
        given t: Transaction = tx
        rootDir.prepareCreateFile("file1", mode=0, uid=1, gid=2)
      }.flatMap(identity)

      contents <- rootDir.getContents()
      isEmpty <- rootDir.isEmpty()
    yield
      contents.length should be (2)
      isEmpty should be (false)

  atest("Insert operation with existing directory (overwrite)"):
    for
      fs <- bootstrap()
      rootDir <- fs.loadRoot()

      // Create initial directory
      dir1Ptr <- cdir(rootDir, "testdir", mode=0, uid=1, gid=2)

      // Create another directory to insert
      dir2Ptr <- cdir(rootDir, "otherdir", mode=0, uid=1, gid=2)

      initialContents <- rootDir.getContents()

      // Insert should replace the existing entry
      _ <- rootDir.insert("testdir", dir2Ptr)

      finalContents <- rootDir.getContents()
      entry <- rootDir.getEntry("testdir")
    yield
      initialContents.length should be (2)
      finalContents.length should be (2)
      entry.get.number should be (dir2Ptr.number)

  atest("Hard link reference counting edge cases"):
    for
      fs <- bootstrap()
      rootDir <- fs.loadRoot()

      // Create file and multiple hard links
      filePtr <- fs.client.transact { tx =>
        given t: Transaction = tx
        rootDir.prepareCreateFile("original", mode=0, uid=1, gid=2)
      }.flatMap(identity)

      file <- fs.loadFile(filePtr)
      initialLinks = file.links

      // Create hard links
      _ <- fs.client.transact { tx =>
        given t: Transaction = tx
        rootDir.prepareHardLink("link1", file)
      }

      _ <- fs.client.transact { tx =>
        given t: Transaction = tx
        rootDir.prepareHardLink("link2", file)
      }

      fileAfterLinks <- fs.loadFile(filePtr)

      // Delete one link
      fdelComplete <- rootDir.delete("link1")
      _ <- fdelComplete

      fileAfterDelete <- fs.loadFile(filePtr)


    yield
      initialLinks should be (1)
      fileAfterLinks.links should be (3)
      fileAfterDelete.links should be (2)

  atest("Directory isEmpty with deleted entries"):
    for
      fs <- bootstrap()
      rootDir <- fs.loadRoot()

      // Create and then delete entries
      _ <- cdir(rootDir, "temp1", mode=0, uid=1, gid=2)
      _ <- cdir(rootDir, "temp2", mode=0, uid=1, gid=2)

      contentsAfterCreate <- rootDir.getContents()
      isEmptyAfterCreate <- rootDir.isEmpty()

      _ <- rootDir.delete("temp1")
      _ <- rootDir.delete("temp2")

      contentsAfterDelete <- rootDir.getContents()
      isEmptyAfterDelete <- rootDir.isEmpty()

    yield
      contentsAfterCreate.length should be (2)
      isEmptyAfterCreate should be (false)
      contentsAfterDelete.length should be (0)
      isEmptyAfterDelete should be (true)

  atest("Case sensitivity handling"):
    for
      fs <- bootstrap()
      rootDir <- fs.loadRoot()

      _ <- cdir(rootDir, "CaseTest", mode=0, uid=1, gid=2)
      _ <- cdir(rootDir, "casetest", mode=0, uid=1, gid=2)
      _ <- cdir(rootDir, "CASETEST", mode=0, uid=1, gid=2)

      contents <- rootDir.getContents()
      entry1 <- rootDir.getEntry("CaseTest")
      entry2 <- rootDir.getEntry("casetest")
      entry3 <- rootDir.getEntry("CASETEST")

    yield
      contents.length should be (3) // All should be treated as different
      entry1 shouldBe defined
      entry2 shouldBe defined
      entry3 shouldBe defined

  atest("Batch operations consistency"):
    for
      fs <- bootstrap()
      rootDir <- fs.loadRoot()

      // Create multiple entries in parallel
      createFutures = (1 to 10).map(i => cdir(rootDir, s"batch$i", mode=0, uid=1, gid=2))
      _ <- Future.sequence(createFutures)

      contentsAfterCreate <- rootDir.getContents()

      // Delete half of them in parallel
      deleteFutures = (1 to 5).map(i => rootDir.delete(s"batch$i"))
      _ <- Future.sequence(deleteFutures)

      contentsAfterDelete <- rootDir.getContents()

    yield
      contentsAfterCreate.length should be (10)
      contentsAfterDelete.length should be (5)
      contentsAfterDelete.map(_.name).toSet should be ((6 to 10).map(i => s"batch$i").toSet)

  atest("Resource cleanup after failures"):
    for
      fs <- bootstrap()
      rootDir <- fs.loadRoot()

      // Create directory
      dirPtr <- cdir(rootDir, "cleanup_test", mode=0, uid=1, gid=2)
      dir <- fs.loadDirectory(dirPtr)

      // Add content to make it non-empty
      _ <- cdir(dir, "subcontent", mode=0, uid=1, gid=2)

      // Attempt to delete non-empty directory (should fail)
      _ <- recoverToSucceededIf[DirectoryNotEmpty](rootDir.delete("cleanup_test"))

      // Verify directory still exists and is accessible
      contents <- rootDir.getContents()
      dirAfterFailedDelete <- fs.loadDirectory(dirPtr)
      subContents <- dirAfterFailedDelete.getContents()

    yield
      contents.length should be (1)
      contents.head.name should be ("cleanup_test")
      subContents.length should be (1)
      subContents.head.name should be ("subcontent")

  atest("two initial-node creations in one transaction fail with RootAlreadyModified and abort it"):
    for
      fs <- bootstrap()
      rootDir <- fs.loadRoot()
      dirPointer <- cdir(rootDir, "foo", mode=0, uid=1, gid=2)
      rm = new SimpleDirectoryRootManager(client, dirPointer.pointer)
      tx = client.newTransaction()
      _ <- rm.createInitialNode(Map())(using tx)
      err <- rm.createInitialNode(Map())(using tx).failed
      txErr <- tx.result.failed
      (_, _, oroot) <- rm.getRootNode()
    yield
      assert(err == RootAlreadyModified(dirPointer.pointer))
      assert(txErr == err)
      assert(oroot.isEmpty)

  atest("inserting an absent name requires that it does not exist: a concurrent create of the name is not overwritten"):
    for
      fs <- bootstrap()
      rootDir <- fs.loadRoot()
      // The concurrent create, committed after the inserter's read found the name absent
      targetPtr <- cdir(rootDir, "target", mode=0, uid=1, gid=2)
      otherPtr <- cdir(rootDir, "other", mode=0, uid=1, gid=2)
      (rootInode, rootRevision) <- rootDir.getInode()
      // Its entry read is the stale one: it saw "target" absent
      stale = new SimpleDirectory(rootDir.pointer, rootRevision, rootInode, fs):
        override def tree: TieredKeyValueList =
          val real = super.tree
          new TieredKeyValueList(real.client, real.rootManager):
            override def get(key: Key): Future[Option[ValueState]] =
              if key == Key("target") then Future.successful(None) else super.get(key)
      tx = client.newTransaction()
      outcome <- stale.prepareInsert("target", otherPtr, incref = false)(using tx)
        .flatMap(_ => tx.commit()).map(_ => "committed")
        .recover { case _: KeyAlreadyExists => "refused" }
      entry <- rootDir.getEntry("target")
    yield
      outcome should be ("refused")
      entry.map(_.number) should be (Some(targetPtr.number))

  // The planned delete finds nothing: without the entry's KeyRevision the move's half is unpinned, so it fails as an
  // absent entry rather than with UnpinnedTransaction
  atest("a delete whose entry a concurrent unlink removed after it was read fails as an absent entry"):
    for
      fs <- bootstrap()
      rootDir <- fs.loadRoot()
      targetPtr <- cdir(rootDir, "target", mode=0, uid=1, gid=2)
      (rootInode, rootRevision) <- rootDir.getInode()
      seen <- rootDir.getEntry("target")
      // The concurrent unlink, committed after the deleter's read found the entry
      _ <- rootDir.delete("target")
      stale = new SimpleDirectory(rootDir.pointer, rootRevision, rootInode, fs):
        override def tree: TieredKeyValueList =
          val real = super.tree
          new TieredKeyValueList(real.client, real.rootManager):
            override def get(key: Key): Future[Option[ValueState]] =
              if key == Key("target") then
                Future.successful(Some(ValueState(org.aspen_ddp.aspen.common.objects.Value(targetPtr.toArray),
                  ObjectRevision(org.aspen_ddp.aspen.common.transaction.TransactionId(java.util.UUID.randomUUID())),
                  org.aspen_ddp.aspen.common.HLCTimestamp.now)))
              else super.get(key)
      tx = client.newTransaction()
      outcome <- stale.prepareDelete("target", decref = false)(using tx)
        .map(_ => "staged")
        .recover { case _: DirectoryEntryDoesNotExist => "absent" }
    yield
      seen.map(_.number) should be (Some(targetPtr.number))
      outcome should be ("absent")

  atest("a cross-directory move stages its delete and insert before committing"):
    for
      fs <- bootstrap()
      rootDir <- fs.loadRoot()
      aPtr <- cdir(rootDir, "a", mode=0, uid=1, gid=2)
      bPtr <- cdir(rootDir, "b", mode=0, uid=1, gid=2)
      a <- fs.loadDirectory(aPtr)
      b <- fs.loadDirectory(bPtr)
      subPtr <- cdir(a, "sub", mode=0, uid=1, gid=2)
      sub0 <- fs.loadDirectory(subPtr)
      // A non-empty directory: the move must not treat its source entry as a deletion
      _ <- cdir(sub0, "inner", mode=0, uid=1, gid=2)
      sub <- fs.loadDirectory(subPtr)
      _ <- a.refresh()
      _ <- b.refresh()
      audit = new PinAudit(client)
      _ <- client.transact: tx =>
        Directory.prepareMove(a, "sub", b, "moved")(using tx)
      _ = audit.stop()
      aEntry <- a.getEntry("sub")
      bEntry <- b.getEntry("moved")
      moved <- fs.loadDirectory(subPtr)
      (movedInode, _) <- moved.getInode()
      innerEntry <- moved.getEntry("inner")
    yield
      val ops = audit.builtExceptMarks.map(_.kvOps.values.flatten.toList)
      val moveTxs = ops.filter: o =>
        o.exists { case d: Delete => d.key == Key("sub"); case _ => false } &&
          o.exists { case i: Insert => i.key == Key("moved"); case _ => false }
      moveTxs.size should be (1)
      aEntry should be (None)
      bEntry.map(_.number) should be (Some(subPtr.number))
      movedInode.oparent.map(_.number) should be (Some(bPtr.number))
      innerEntry shouldBe defined

  // The moved inode is the one under the name when the move runs, not one a caller read before
  // POSIX rename: an existing destination is replaced on every attempt, not only on the first
  atest("a cross-directory move replaces an existing destination entry, also after a failed attempt"):
    for
      fs <- bootstrap()
      rootDir <- fs.loadRoot()
      aPtr <- cdir(rootDir, "a", mode=0, uid=1, gid=2)
      bPtr <- cdir(rootDir, "b", mode=0, uid=1, gid=2)
      a <- fs.loadDirectory(aPtr)
      b <- fs.loadDirectory(bPtr)
      movedPtr <- cdir(a, "src", mode=0, uid=1, gid=2)
      _ <- cdir(b, "target", mode=0, uid=1, gid=2)
      racerPtr <- cdir(b, "racer", mode=0, uid=1, gid=2)
      (bInode, bRevision) <- b.getInode()
      // The first attempt's insert is followed by a committed replacement of the destination entry, so that attempt
      // fails at commit with the destination name still present
      attempts = new java.util.concurrent.atomic.AtomicInteger(0)
      racingDst = new SimpleDirectory(bPtr, bRevision, bInode, fs):
        override def prepareInsert(name: String, ptr: org.aspen_ddp.aspen.amoebafs.InodePointer, incref: Boolean)
                                  (using tx: Transaction): Future[Unit] =
          super.prepareInsert(name, ptr, incref).flatMap: _ =>
            if attempts.incrementAndGet() == 1 then b.rename("racer", "target") else Future.unit
      _ <- Directory.move(a, "src", racingDst, "target")
      srcEntry <- a.getEntry("src")
      targetEntry <- b.getEntry("target")
      racerEntry <- b.getEntry("racer")
      moved <- fs.loadDirectory(movedPtr)
      _ <- moved.refresh()
      (movedInode, _) <- moved.getInode()
    yield
      attempts.get should be (2)
      srcEntry should be (None)
      racerEntry should be (None)
      targetEntry.map(_.number) should be (Some(movedPtr.number))
      targetEntry.map(_.number) should not be (Some(racerPtr.number))
      movedInode.oparent.map(_.number) should be (Some(bPtr.number))

  atest("a cross-directory move onto a non-empty directory fails with DirectoryNotEmpty and changes nothing"):
    for
      fs <- bootstrap()
      rootDir <- fs.loadRoot()
      aPtr <- cdir(rootDir, "a", mode=0, uid=1, gid=2)
      bPtr <- cdir(rootDir, "b", mode=0, uid=1, gid=2)
      a <- fs.loadDirectory(aPtr)
      b <- fs.loadDirectory(bPtr)
      movedPtr <- cdir(a, "src", mode=0, uid=1, gid=2)
      targetPtr <- cdir(b, "target", mode=0, uid=1, gid=2)
      target <- fs.loadDirectory(targetPtr)
      _ <- cdir(target, "inner", mode=0, uid=1, gid=2)
      _ <- recoverToSucceededIf[DirectoryNotEmpty](Directory.move(a, "src", b, "target"))
      srcEntry <- a.getEntry("src")
      targetEntry <- b.getEntry("target")
    yield
      srcEntry.map(_.number) should be (Some(movedPtr.number))
      targetEntry.map(_.number) should be (Some(targetPtr.number))

  atest("a cross-directory move moves the entry as it reads it, and a replacement after its read aborts it"):
    for
      fs <- bootstrap()
      rootDir <- fs.loadRoot()
      aPtr <- cdir(rootDir, "a", mode=0, uid=1, gid=2)
      bPtr <- cdir(rootDir, "b", mode=0, uid=1, gid=2)
      a <- fs.loadDirectory(aPtr)
      b <- fs.loadDirectory(bPtr)
      firstPtr <- cdir(a, "sub", mode=0, uid=1, gid=2)
      secondPtr <- cdir(a, "other", mode=0, uid=1, gid=2)
      thirdPtr <- cdir(a, "third", mode=0, uid=1, gid=2)
      // Staged against "sub" as first created, then "sub" is replaced before the commit: the commit must fail
      staged = client.newTransaction()
      _ <- Directory.prepareMove(a, "sub", b, "moved")(using staged)
      _ <- a.rename("other", "sub")
      stagedFailed <- staged.commit().map(_ => false).recover(_ => true)
      afterRace <- b.getEntry("moved")
      // A move run now moves the replacement
      _ <- Directory.move(a, "sub", b, "moved")
      // Replaced again between a caller's read and the move: the move takes what the name holds when it runs
      seen <- a.getEntry("third")
      replacementPtr <- cdir(a, "sub2", mode=0, uid=1, gid=2)
      _ <- a.rename("sub2", "third")
      _ <- Directory.move(a, "third", b, "third")
      aSub <- a.getEntry("sub")
      bMoved <- b.getEntry("moved")
      bThird <- b.getEntry("third")
      moved <- fs.loadDirectory(secondPtr)
      _ <- moved.refresh()
      (movedInode, _) <- moved.getInode()
    yield
      stagedFailed should be (true)
      afterRace should be (None)
      aSub should be (None)
      bMoved.map(_.number) should be (Some(secondPtr.number))
      movedInode.oparent.map(_.number) should be (Some(bPtr.number))
      seen.map(_.number) should be (Some(thirdPtr.number))
      bThird.map(_.number) should be (Some(replacementPtr.number))

  /** A directory whose tree has nodes of `nodeSize` bytes, so that a few long names span several tier-0 nodes */
  private def smallNodeDirectory(parent: Directory, name: String, nodeSize: Int): Future[DirectoryPointer] =
    client.transact { tx =>
      given Transaction = tx
      for
        pool <- client.getStoragePool(parent.pointer.pointer.poolId)
        root = Root(0, LexicalKeyOrdering, None, new SinglePoolNodeAllocator(client, pool.poolId, nodeSize))
        inode = DirectoryInode.init(0, 1, 2, Some(parent.pointer), None, root)
        fcreated <- CreateFileTask.prepareTask(parent.fs, parent.pointer, name, inode)
      yield fcreated
    }.flatMap(identity).map(_.get.asInstanceOf[DirectoryPointer])

  atest("a rename across tier-0 nodes unlinks the existing target entry"):
    val pad = "x" * 200
    val source = s"a$pad"
    val target = s"z$pad"
    def fill(d: Directory, names: List[String]): Future[Unit] = names match
      case Nil => Future.unit
      case n :: rest => cdir(d, n, mode=0, uid=1, gid=2).flatMap(_ => fill(d, rest))
    for
      fs <- bootstrap()
      rootDir <- fs.loadRoot()
      dPtr <- smallNodeDirectory(rootDir, "d", 1024)
      d <- fs.loadDirectory(dPtr)
      sourcePtr <- cdir(d, source, mode=0, uid=1, gid=2)
      targetPtr <- cdir(d, target, mode=0, uid=1, gid=2)
      _ <- fill(d, (1 to 10).map(i => f"m$i%02d$pad").toList)
      _ <- waitForTransactionsToComplete()
      _ <- d.refresh()
      tr = d.asInstanceOf[SimpleDirectory].tree
      osourceNode <- tr.getContainingNode(Key(source))
      otargetNode <- tr.getContainingNode(Key(target))
      _ = osourceNode.get.nodeUUID should not be (otargetNode.get.nodeUUID)
      targetRevision = otargetNode.get.get(Key(target)).get.revision
      audit = new PinAudit(client)
      _ <- d.rename(source, target)
      _ = audit.stop()
      sourceEntry <- d.getEntry(source)
      targetEntry <- d.getEntry(target)
    yield
      sourceEntry should be (None)
      targetEntry.map(_.number) should be (Some(sourcePtr.number))
      audit.unpinned.filterNot(PinAudit.isMark) shouldBe empty
      val built = audit.builtExceptMarks
      // The replaced entry is unlinked: an UnlinkFileTask for the old target is enrolled
      built.exists(_.kvOps.values.flatten.exists:
        case i: Insert => i.value.sameElements(targetPtr.toArray)
        case _ => false) should be (true)
      // The new key is written at the revision the rename read
      built.flatMap(_.description.requirements).exists:
        case kvu: KeyValueUpdate => kvu.requirements.contains(KeyValueUpdate.KeyRevision(Key(target), targetRevision))
        case _ => false
      .should(be(true))

  /** A rename whose old entry is alone in its tier-0 node, so that deleting it joins the node with its right
    * neighbor, and whose new name falls in that neighbor. The join and the write of the new name update the same
    * node: they must be staged as one update of it
    */
  private def renameIntoJoinedNeighbor(replaceTarget: Boolean): Future[org.scalatest.Assertion] =
    val pad = "x" * 200
    val names = (1 to 12).map(i => f"m$i%02d$pad").toList
    def fill(d: Directory, names: List[String]): Future[Unit] = names match
      case Nil => Future.unit
      case n :: rest => cdir(d, n, mode=0, uid=1, gid=2).flatMap(_ => fill(d, rest))
    def deleteAll(d: Directory, names: List[String]): Future[Unit] = names match
      case Nil => Future.unit
      case n :: rest => d.delete(n).flatMap(f => f).flatMap(_ => deleteAll(d, rest))
    for
      fs <- bootstrap()
      rootDir <- fs.loadRoot()
      dPtr <- smallNodeDirectory(rootDir, "d", 1024)
      d <- fs.loadDirectory(dPtr)
      _ <- fill(d, names)
      _ <- waitForTransactionsToComplete()
      tr = d.asInstanceOf[SimpleDirectory].tree
      nodes <- Future.sequence(names.map(n => tr.getContainingNode(Key(n)).map(o => n -> o.get.nodeUUID)))
      // Nodes in key order, each with the names it holds
      groups = nodes.foldLeft(List.empty[(java.util.UUID, List[String])]):
        case ((u, ns) :: rest, (n, nu)) if u == nu => (u, ns :+ n) :: rest
        case (acc, (n, nu)) => (nu, List(n)) :: acc
      .reverse
      _ = groups.size should be > 2
      sourceGroup = groups(1)._2
      neighborGroup = groups(2)._2
      source = sourceGroup.head
      target = if replaceTarget then neighborGroup.last else neighborGroup.head + "q"
      sourcePtr <- d.getEntry(source).map(_.get)
      _ <- deleteAll(d, sourceGroup.tail)
      _ <- waitForTransactionsToComplete()
      _ <- d.refresh()
      osourceNode <- tr.getContainingNode(Key(source))
      otargetNode <- tr.getContainingNode(Key(target))
      _ = osourceNode.get.keys should be (Set(Key(source)))
      _ = osourceNode.get.nodeUUID should not be (otargetNode.get.nodeUUID)
      audit = new PinAudit(client)
      _ <- d.rename(source, target)
      _ = audit.stop()
      _ <- waitForTransactionsToComplete()
      sourceEntry <- d.getEntry(source)
      targetEntry <- d.getEntry(target)
      contents <- d.getContents()
    yield
      audit.unpinned.filterNot(PinAudit.isMark) shouldBe empty
      sourceEntry should be (None)
      targetEntry.map(_.number) should be (Some(sourcePtr.number))
      val expected = (names.toSet -- sourceGroup) + target
      contents.map(_.name).toSet should be (expected)

  atest("a rename whose delete joins the old entry's node into the new name's node commits"):
    renameIntoJoinedNeighbor(replaceTarget = false)

  atest("a rename whose delete joins the old entry's node into the node of the entry it replaces commits"):
    renameIntoJoinedNeighbor(replaceTarget = true)

  private def createFile(dir: Directory, name: String): Future[FilePointer] =
    client.transact { tx =>
      given Transaction = tx
      dir.prepareCreateFile(name, mode=0, uid=1, gid=2)
    }.flatMap(identity)

  // The unlink's completion includes the release of the content it enrolled
  atest("unlinking a file frees its content tree and data segments"):
    for
      fs <- bootstrap()
      rootDir <- fs.loadRoot()
      filePtr <- createFile(rootDir, "f")
      file <- fs.loadFile(filePtr)
      _ <- file.write(0, DataBuffer(Array[Byte](1, 2, 3)))
      _ <- waitForTransactionsToComplete()
      (inode, _, _) <- fs.readInode(filePtr)
      contentRoot = inode.asInstanceOf[FileInode].contents.rootKeyValuePointer.get
      rootKvos <- client.read(contentRoot)
      segments = rootKvos.contents.values.map(vs => DataObjectPointer(vs.value.bytes)).toList
      fdel <- rootDir.delete("f")
      _ <- fdel
      oroot <- client.readOptional(contentRoot)
      osegments <- Future.sequence(segments.map(client.readOptional))
      oinode <- client.readOptional(filePtr.pointer)
    yield
      segments should not be empty
      oroot should be (None)
      osegments.flatten shouldBe empty
      oinode should be (None)

  // The content task must not depend on the inode the same unlink step deletes
  atest("unlinking a directory frees its content tree"):
    for
      fs <- bootstrap()
      rootDir <- fs.loadRoot()
      dPtr <- cdir(rootDir, "d", mode=0, uid=1, gid=2)
      d <- fs.loadDirectory(dPtr)
      _ <- cdir(d, "x", mode=0, uid=1, gid=2)
      fx <- d.delete("x")
      _ <- fx
      (inode, _, _) <- fs.readInode(dPtr)
      contentRoot = inode.asInstanceOf[DirectoryInode].contents.rootKeyValuePointer.get
      fdel <- rootDir.delete("d")
      _ <- fdel
      oroot <- client.readOptional(contentRoot)
      oinode <- client.readOptional(dPtr.pointer)
    yield
      oroot should be (None)
      oinode should be (None)

  // A create racing an rmdir can leave an entry in a directory whose last link is gone. Its content deletion
  // unlinks the entry, exactly once
  atest("deleting a non-empty directory's content unlinks each entry once"):
    for
      fs <- bootstrap()
      rootDir <- fs.loadRoot()
      dPtr <- cdir(rootDir, "d", mode=0, uid=1, gid=2)
      d <- fs.loadDirectory(dPtr)
      filePtr <- createFile(rootDir, "f")
      file <- fs.loadFile(filePtr)
      _ <- client.transact { tx =>
        given Transaction = tx
        d.prepareHardLink("g", file)
      }
      (linked, _, _) <- fs.readInode(filePtr)
      // Unlink d without the emptiness check
      funlink <- client.transact { tx =>
        UnlinkFileTask.prepareTask(fs, dPtr)(using tx)
      }
      _ <- funlink
      (after, _, _) <- fs.readInode(filePtr)
      oinode <- client.readOptional(dPtr.pointer)
    yield
      linked.links should be (2)
      after.links should be (1)
      oinode should be (None)
