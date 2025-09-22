package org.aspen_ddp.aspen.amoebafs.impl


import java.util.UUID
import org.aspen_ddp.aspen.client.RegisteredTypeFactory
import org.aspen_ddp.aspen.amoebafs.impl.simple.{SimpleDirectory, CreateFileTask, SimpleDirectoryRootManager, SimpleFileContent, SimpleFileRootManager, UnlinkFileTask}

object StaticTypeRegistry {

  private val registry: List[RegisteredTypeFactory] = List(
    SimpleDirectoryRootManager,
    SimpleFileRootManager,
    SimpleDirectory.DeleteDirectoryContentTask,
    SimpleFileContent.DeleteFileContentTask,
    CreateFileTask,
    UnlinkFileTask,
  )

  val types: List[(UUID, RegisteredTypeFactory)] = registry.map(t => t.typeUUID -> t)
}
