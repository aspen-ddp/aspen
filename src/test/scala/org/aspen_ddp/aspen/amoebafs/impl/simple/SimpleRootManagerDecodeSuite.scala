package org.aspen_ddp.aspen.amoebafs.impl.simple

import org.aspen_ddp.aspen.IntegrationTestSuite
import org.aspen_ddp.aspen.client.Transaction
import org.aspen_ddp.aspen.client.tkvl.{InvalidRoot, Root}
import org.aspen_ddp.aspen.common.Radicle
import org.scalatest.Assertion

import java.util.{Timer, TimerTask}
import java.util.concurrent.TimeoutException
import scala.concurrent.{Future, Promise}
import scala.language.implicitConversions
import scala.util.{Success, Try}

/** Covers the failure path of the root managers' getRData: an inode that cannot be decoded must
  * surface as a failed Future rather than a throw out of the executor that leaves the caller's
  * Future uncompleted forever.
  */
class SimpleRootManagerDecodeSuite extends IntegrationTestSuite {

  /** Converts a hang into a legible failure. Without this, the pre-fix behavior is an uncompleted
    * Future and the suite simply never finishes.
    */
  private def withTimeout[T](what: String, f: Future[T], ms: Long = 15000): Future[T] =
    val p = Promise[T]()
    val timer = new Timer(true)

    timer.schedule(new TimerTask:
      def run(): Unit = p.tryFailure(new TimeoutException(s"$what did not complete within ${ms}ms"))
    , ms)

    f.onComplete: result =>
      timer.cancel()
      p.tryComplete(result)

    p.future

  /** A four byte object is far too short to decode as an inode, so the decode fails with a buffer
    * underflow before any of the root handling is reached.
    */
  private def allocateUndecodableInode(): Future[org.aspen_ddp.aspen.common.objects.DataObjectPointer] =
    given tx: Transaction = client.newTransaction()
    for
      pool <- client.getStoragePool(Radicle.poolId)
      dp <- pool.allocator.allocateDataObject(Array[Byte](0, 0, 0, 0))
      _ <- tx.commit()
    yield
      dp

  /** An InvalidRoot that carries the decode failure as its cause. The cause is what makes the
    * failure diagnosable; InvalidRoot on its own says nothing about what went wrong.
    */
  private def assertInvalidRoot(result: Try[Root]): Assertion =
    withClue(s"expected a failed Future, got $result: "):
      result.isFailure should be (true)

    val err = result.failed.get

    withClue(s"expected an InvalidRoot, got $err: "):
      err shouldBe a [InvalidRoot]

    withClue("InvalidRoot should carry the decode failure as its cause: "):
      err.getCause should not be null

  atest("SimpleFileRootManager.getRoot fails when the inode cannot be decoded") {
    for
      dp <- allocateUndecodableInode()
      result <- withTimeout("getRoot", new SimpleFileRootManager(client, dp).getRoot().transform(Success(_)))
    yield
      assertInvalidRoot(result)
  }

  atest("SimpleDirectoryRootManager.getRoot fails when the inode cannot be decoded") {
    for
      dp <- allocateUndecodableInode()
      result <- withTimeout("getRoot", new SimpleDirectoryRootManager(client, dp).getRoot().transform(Success(_)))
    yield
      assertInvalidRoot(result)
  }
}
