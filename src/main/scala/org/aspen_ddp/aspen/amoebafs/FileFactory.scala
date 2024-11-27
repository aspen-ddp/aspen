package org.aspen_ddp.aspen.amoebafs

import org.aspen_ddp.aspen.common.objects.ObjectRevision

import scala.concurrent.Future

trait FileFactory {

  def createFileHandle(fs: FileSystem, file: File): FileHandle

  def createDirectory(fs: FileSystem,
                      pointer: DirectoryPointer,
                      inode: DirectoryInode,
                      revision: ObjectRevision): Future[Directory]

  def createFile(fs: FileSystem,
                 pointer: FilePointer,
                 inode: FileInode,
                 revision: ObjectRevision): Future[File]

  def createSymlink(fs: FileSystem,
                    pointer: SymlinkPointer,
                    inode: SymlinkInode,
                    revision: ObjectRevision): Future[Symlink]

  def createUnixSocket(fs: FileSystem,
                       pointer: UnixSocketPointer,
                       inode: UnixSocketInode,
                       revision: ObjectRevision): Future[UnixSocket]

  def createFIFO(fs: FileSystem,
                 pointer: FIFOPointer,
                 inode: FIFOInode,
                 revision: ObjectRevision): Future[FIFO]

  def createCharacterDevice(fs: FileSystem,
                            pointer: CharacterDevicePointer,
                            inode: CharacterDeviceInode,
                            revision: ObjectRevision): Future[CharacterDevice]

  def createBlockDevice(fs: FileSystem,
                        pointer: BlockDevicePointer,
                        inode: BlockDeviceInode,
                        revision: ObjectRevision): Future[BlockDevice]

}
