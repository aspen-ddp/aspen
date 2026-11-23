package org.aspen_ddp.aspen.server.crl.simple

import org.aspen_ddp.aspen.server.crl.CrlLocked
import org.scalatest.funsuite.AnyFunSuite

import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.AtomicReference

class CrlLockSuite extends AnyFunSuite:

  test("a lock held on one CRL never delays acquire on another") {
    val a = os.temp.dir(deleteOnExit = true) / "a-crl"
    val b = os.temp.dir(deleteOnExit = true) / "b-crl"
    val entered = new CountDownLatch(1)
    val proceed = new CountDownLatch(1)
    // Only A's acquire stops, inside its lock file's monitor, before its tryLock: as a hung device's would
    CrlLock.beforeTryLock = dir =>
      if dir == a then
        entered.countDown()
        proceed.await()
    try
      val lockA = new AtomicReference[Either[Throwable, CrlLock]]()
      val t = new Thread(() =>
        lockA.set(try Right(CrlLock.acquire(a)) catch case e: Throwable => Left(e)))
      t.setDaemon(true)
      t.start()
      assert(entered.await(30, TimeUnit.SECONDS))

      // B's acquire completes while A's is stopped. The bound only turns a regression into a failure, not a hang
      val bDone = new CountDownLatch(1)
      val lockB = new AtomicReference[Either[Throwable, CrlLock]]()
      val tb = new Thread(() =>
        try lockB.set(try Right(CrlLock.acquire(b)) catch case e: Throwable => Left(e))
        finally bDone.countDown())
      tb.setDaemon(true)
      tb.start()
      assert(bDone.await(30, TimeUnit.SECONDS), "acquire on B waited for A's")
      assert(lockB.get.isRight)
      // B is held: a second in-process opener is refused
      assertThrows[CrlLocked](CrlLock.acquire(b))

      proceed.countDown()
      t.join(30_000)
      assert(!t.isAlive)
      assert(lockA.get.isRight)
      assertThrows[CrlLocked](CrlLock.acquire(a))

      lockA.get.toOption.get.release()
      lockB.get.toOption.get.release()
      // Released: each opens again
      CrlLock.acquire(a).release()
      CrlLock.acquire(b).release()
    finally
      CrlLock.beforeTryLock = _ => ()
      proceed.countDown()
  }

  test("another path to a held lock file is refused") {
    val dir = os.temp.dir(deleteOnExit = true) / "crl"
    val lock = CrlLock.acquire(dir)
    val link = dir / os.up / "link"
    os.symlink(link, dir)
    try assertThrows[CrlLocked](CrlLock.acquire(link))
    finally lock.release()
    CrlLock.acquire(link).release()
  }
