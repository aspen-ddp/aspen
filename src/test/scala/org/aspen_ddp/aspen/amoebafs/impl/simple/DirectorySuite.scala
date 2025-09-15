package org.aspen_ddp.aspen.amoebafs.impl.simple

import java.nio.charset.StandardCharsets

import org.aspen_ddp.aspen.client.Transaction
import org.aspen_ddp.aspen.common.objects.{ObjectPointer, ObjectRevision}
import org.aspen_ddp.aspen.amoebafs.error.{DirectoryEntryDoesNotExist, DirectoryEntryExists, DirectoryNotEmpty}
import org.aspen_ddp.aspen.amoebafs.{Directory, DirectoryPointer, FileMode, Timespec}

import scala.concurrent.Future

class DirectorySuite extends FilesSystemTestSuite:
  def cdir(dir: Directory, name: String, mode: Int, uid: Int, gid: Int): Future[DirectoryPointer] =
    given tx: Transaction = dir.fs.client.newTransaction()
    val fprep = dir.prepareCreateDirectory(name, mode, uid, gid)
    fprep.foreach(_ => tx.commit())
    fprep.flatMap(fresult => fresult)

  test("Amoeba Bootstrap"):
    for
      fs <- bootstrap()
      rootDir <- fs.loadRoot()
      (rootInode, _) <- rootDir.getInode()
    yield
      rootInode.uid should be (0)

  test("Create Directory"):
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

  test("Change Directory UID"):
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

  test("Change Directory UID with recovery from revision mismatch"):
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

  test("Change multiple metadata attributes"):
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

  test("Delete non-empty Directory"):
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

  test("Create file fails if file already exists"):
    for
      fs <- bootstrap()
      rootDir <- fs.loadRoot()
      initialContent <- rootDir.getContents()
      _ <- cdir(rootDir, "foo", mode=0, uid=1, gid=2)
      _ <- recoverToSucceededIf[DirectoryEntryExists](rootDir.createFile("foo", mode=0, uid=1, gid=2))
    yield
      initialContent.length should be (0)

  test("Insert file fails if file already exists"):
    for
      fs <- bootstrap()
      rootDir <- fs.loadRoot()
      initialContent <- rootDir.getContents()
      _ <- cdir(rootDir, "foo", mode=0, uid=1, gid=2)
      _ <- recoverToSucceededIf[DirectoryEntryExists](rootDir.insert("foo",rootDir.pointer))
    yield
      initialContent.length should be (0)

  test("Delete file fails if file does not exist"):
    for
      fs <- bootstrap()
      rootDir <- fs.loadRoot()
      initialContent <- rootDir.getContents()
      _ <- cdir(rootDir, "foo", mode=0, uid=1, gid=2)
      _ <- recoverToSucceededIf[DirectoryEntryDoesNotExist](rootDir.delete("INVALID"))
    yield
      initialContent.length should be (0)

  test("Rename file fails if source file does not exist"):
    for
      fs <- bootstrap()
      rootDir <- fs.loadRoot()
      initialContent <- rootDir.getContents()
      _ <- cdir(rootDir, "foo", mode=0, uid=1, gid=2)
      _ <- recoverToSucceededIf[DirectoryEntryDoesNotExist](rootDir.rename("INVALID", "bar"))
    yield
      initialContent.length should be (0)

  test("Rename file fails if destination file exists"):
    for
      fs <- bootstrap()
      rootDir <- fs.loadRoot()
      initialContent <- rootDir.getContents()
      _ <- cdir(rootDir, "foo", mode=0, uid=1, gid=2)
      _ <- cdir(rootDir, "bar", mode=0, uid=1, gid=2)
      _ <- recoverToSucceededIf[DirectoryEntryExists](rootDir.rename("foo", "bar"))
    yield
      initialContent.length should be (0)

  test("HardLink file fails if destination file exists"):
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

  test("Delete empty Directory"):
    for
      fs <- bootstrap()
      rootDir <- fs.loadRoot()
      _ <- cdir(rootDir, "foo", mode=0, uid=1, gid=2)
      _ <- rootDir.delete("foo")
      oentry <- rootDir.getEntry("foo")
    yield
      oentry should be (None)

  test("Delete Directory with data tiered list"):
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

  test("Test Symlink"):
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

  test("Test UnixSocket"):
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

  test("Test FIFO"):
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

  test("Test CharacterDevice"):
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

  test("Test BlockDevice"):
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

  test("Test Hardlink"):
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
      _ <- fs.client.transact { implicit tx =>
        rootDir.prepareHardLink("bar", us)
      }
      us2 <- fs.loadBlockDevice(sptr)
      postLinkContent <- rootDir.getContents()
    yield
      us2.links should be (2)
      postLinkContent.size should be (2)
