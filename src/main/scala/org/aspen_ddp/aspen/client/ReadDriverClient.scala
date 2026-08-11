package org.aspen_ddp.aspen.client

import org.aspen_ddp.aspen.client.internal.OpportunisticRebuildManager
import org.aspen_ddp.aspen.client.internal.network.Messenger
import org.aspen_ddp.aspen.common.network.ClientId
import org.aspen_ddp.aspen.common.util.BackgroundTaskManager

import scala.concurrent.ExecutionContext

/** The slice of a client that a ReadDriver needs.
 *
 *  Declared separately from AspenClient so that read-driver tests need not stub the
 *  full client interface. AspenClient extends this trait and declares none of these
 *  members itself, so every AspenClient implementation satisfies it automatically.
 */
trait ReadDriverClient:

  val clientId: ClientId

  val txStatusCache: TransactionStatusCache

  def clientContext: ExecutionContext

  def backgroundTaskManager: BackgroundTaskManager

  private[client] def opportunisticRebuildManager: OpportunisticRebuildManager

  private[client] val messenger: Messenger

  private[client] val objectCache: ObjectCache

  /** Not a read concern. It is here so that BaseReadDriver.noErrorRecoveryReadDriver can
   *  name the running test in its hung-read logs without threading a label through
   *  ReadDriver.Factory. The matching setter stays on AspenClient.
   */
  private[aspen] def getSystemAttribute(key: String): Option[String]

object ReadDriverClient:

  /** No-op implementation for tests. Follows the convention set by Messenger.None,
   *  ObjectCache.NoCache, and friends: every member is the existing no-op singleton
   *  for its type, so a test that does not care about a dependency need not name it.
   */
  class NoOp(val clientId: ClientId) extends ReadDriverClient:

    val txStatusCache: TransactionStatusCache = TransactionStatusCache.NoCache

    def clientContext: ExecutionContext = ExecutionContext.Implicits.global

    def backgroundTaskManager: BackgroundTaskManager = BackgroundTaskManager.NoBackgroundTaskManager

    private[client] def opportunisticRebuildManager: OpportunisticRebuildManager = OpportunisticRebuildManager.None

    private[client] val messenger: Messenger = Messenger.None

    private[client] val objectCache: ObjectCache = ObjectCache.NoCache

    private[aspen] def getSystemAttribute(key: String): Option[String] = None
