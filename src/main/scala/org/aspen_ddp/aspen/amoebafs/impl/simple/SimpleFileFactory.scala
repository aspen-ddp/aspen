package org.aspen_ddp.aspen.amoebafs.impl.simple

import org.aspen_ddp.aspen.common.objects.ObjectRevision
import org.aspen_ddp.aspen.amoebafs.{BlockDevice, BlockDeviceInode, BlockDevicePointer, CharacterDevice, CharacterDeviceInode, CharacterDevicePointer, Directory, DirectoryInode, DirectoryPointer, FIFO, FIFOInode, FIFOPointer, File, FileFactory, FileHandle, FileInode, FilePointer, FileSystem, Symlink, SymlinkInode, SymlinkPointer, UnixSocket, UnixSocketInode, UnixSocketPointer}

import scala.concurrent.Future

class SimpleFileFactory(val writeBufferSize: Int) extends FileFactory {

  def createFileHandle(fs: FileSystem, file: File): FileHandle = {
    new SimpleFileHandle(file, writeBufferSize)
  }

  def createDirectory(fs: FileSystem,
                      pointer: DirectoryPointer,
                      inode: DirectoryInode,
                      revision: ObjectRevision): Future[Directory] = {
    Future.successful(new SimpleDirectory(pointer, revision, inode, fs))
  }

  def createFile(fs: FileSystem,
                 pointer: FilePointer,
                 inode: FileInode,
                 revision: ObjectRevision): Future[File] = {
    Future.successful(new SimpleFile(pointer, revision, inode, fs))
  }

  def createSymlink(fs: FileSystem,
                    pointer: SymlinkPointer,
                    inode: SymlinkInode,
                    revision: ObjectRevision): Future[Symlink] = {
    Future.successful(new SimpleSymlink(pointer, inode, revision, fs))
  }

  def createUnixSocket(fs: FileSystem,
                       pointer: UnixSocketPointer,
                       inode: UnixSocketInode,
                       revision: ObjectRevision): Future[UnixSocket] = {
    Future.successful(new SimpleUnixSocket(pointer, inode, revision, fs))
  }

  def createFIFO(fs: FileSystem,
                 pointer: FIFOPointer,
                 inode: FIFOInode,
                 revision: ObjectRevision): Future[FIFO] = {
    Future.successful(new SimpleFIFO(pointer, inode, revision, fs))
  }

  def createCharacterDevice(fs: FileSystem,
                            pointer: CharacterDevicePointer,
                            inode: CharacterDeviceInode,
                            revision: ObjectRevision): Future[CharacterDevice] = {
    Future.successful(new SimpleCharacterDevice(pointer, inode, revision, fs))
  }

  def createBlockDevice(fs: FileSystem,
                        pointer: BlockDevicePointer,
                        inode: BlockDeviceInode,
                        revision: ObjectRevision): Future[BlockDevice] = {
    Future.successful(new SimpleBlockDevice(pointer, inode, revision, fs))
  }


}
