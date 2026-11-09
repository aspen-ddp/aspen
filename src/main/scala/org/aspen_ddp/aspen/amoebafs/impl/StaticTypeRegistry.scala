package org.aspen_ddp.aspen.amoebafs.impl


import org.aspen_ddp.aspen.client.RegisteredTypeFactory
import org.aspen_ddp.aspen.amoebafs.impl.simple.{SimpleDirectory, CreateFileTask, SimpleDirectoryRootManager, SimpleFileContent, SimpleFileRootManager, UnlinkFileTask}

object StaticTypeRegistry {

  val factories: List[RegisteredTypeFactory] = List(
    SimpleDirectoryRootManager,
    SimpleFileRootManager,
    SimpleDirectory.DeleteDirectoryContentTask,
    SimpleFileContent.DeleteFileContentTask,
    CreateFileTask,
    UnlinkFileTask,
  )
}
