package org.aspen_ddp.aspen.amoebafs.impl.simple

import java.nio.charset.StandardCharsets
import org.aspen_ddp.aspen.client.Transaction
import org.aspen_ddp.aspen.common.objects.{ObjectPointer, ObjectRevision}
import org.aspen_ddp.aspen.amoebafs.error.{DirectoryEntryDoesNotExist, DirectoryEntryExists, DirectoryNotEmpty}
import org.aspen_ddp.aspen.amoebafs.{Directory, DirectoryPointer, FileMode, FileSystem, Timespec}

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
