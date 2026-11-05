package org.aspen_ddp.aspen.server.host

import org.aspen_ddp.aspen.client.AspenClient
import org.aspen_ddp.aspen.common.metadata.HostId
import org.aspen_ddp.aspen.common.util.BackgroundTaskManager
import org.aspen_ddp.aspen.server.Host
import org.aspen_ddp.aspen.server.network.Messenger

import java.util.UUID
import scala.concurrent.ExecutionContext

/** What the classes extracted from Host share: the host's identity and services, the seams that tests replace, and
 *  the loaded devices. Each component takes the DeviceControl as its only constructor parameter and is added here as
 *  a lazy val, which lets components refer to each other without construction-order cycles.
 *
 *  `lock` is the Host instance and `post` puts an event on its queue; both go away with the event queue. */
final class DeviceControl(val metadata: DeviceMetadata,
                          val scheduler: ControlScheduler,
                          val backends: BackendOpener,
                          val devices: LocalDevices,
                          val client: AspenClient,
                          val hostId: HostId,
                          val aspenSystemId: UUID,
                          val rootDir: os.Path,
                          val net: Messenger,
                          val backgroundTasks: BackgroundTaskManager,
                          val lock: AnyRef,
                          val post: Host.Event => Unit)(using val ec: ExecutionContext)
