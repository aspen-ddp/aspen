package org.aspen_ddp.aspen.amoebafs

object FileMode {
  val S_IFMT    = 0xf000 // bit mask for the file type bit field
  val S_IFSOCK  = 0xc000 // unix socket
  val S_IFLNK   = 0xa000 // symbolic link
  val S_IFREG   = 0x8000 // regular file
  val S_IFBLK   = 0x6000 // block device
  val S_IFDIR   = 0x4000 // directory
  val S_IFCHR   = 0x2000 // character device
  val S_IFFIFO  = 0x1000 // FIFO

  val S_IRWXU   = 0x1c0  // Read, write, execute/search by owner.
  val S_IRUSR   = 0x100  // Read permission, owner.
  val S_IWUSR   = 0x80   // Write permission, owner.
  val S_IXUSR   = 0x40   // Execute/search permission, owner.
  val S_IRWXG   = 0x38   // Read, write, execute/search by group.
  val S_IRGRP   = 0x20   // Read permission, group.
  val S_IWGRP   = 0x10   // Write permission, group.
  val S_IXGRP   = 0x8    // Execute/search permission, group.
  val S_IRWXO   = 0x7    // Read, write, execute/search by others.
  val S_IROTH   = 0x4    // Read permission, others.
  val S_IWOTH   = 0x2    // Write permission, others.
  val S_IXOTH   = 0x1    // Execute/search permission, others.
  val S_ISUID   = 0x800  // Set-user-ID on execution.
  val S_ISGID   = 0x400  // Set-group-ID on execution.
  val S_ISVTX   = 0x200  // On directories, restricted deletion flag.   [Option End]
}
