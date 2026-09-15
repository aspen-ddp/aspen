package org.aspen_ddp.aspen.client

import org.aspen_ddp.aspen.IntegrationTestSuite

import scala.concurrent.ExecutionContext

class TransactionFailureSuite extends IntegrationTestSuite:

  atest("abortAndThrow throws the reason it was given"):
    given ExecutionContext = executionContext
    val tx = client.newTransaction()
    val reason = new Exception("boom")

    val thrown = intercept[Exception](tx.abortAndThrow(reason))

    tx.result.failed.map: stored =>
      thrown should be theSameInstanceAs reason
      stored should be theSameInstanceAs reason

  atest("abort records the failure without throwing"):
    given ExecutionContext = executionContext
    val tx = client.newTransaction()
    val reason = new Exception("recorded")

    tx.abort(reason)

    tx.result.failed.map(_ should be theSameInstanceAs reason)

  atest("only the first reason is propagated"):
    given ExecutionContext = executionContext
    val tx = client.newTransaction()
    val first = new Exception("first")

    tx.abort(first)
    tx.abort(new Exception("second"))

    tx.result.failed.map(_ should be theSameInstanceAs first)

  atest("an aborted transaction carrying updates cannot commit"):
    given ExecutionContext = executionContext
    for
      before <- client.read(radicle)
      tx = client.newTransaction()
      _ = tx.setRefcount(radicle, before.refcount, before.refcount.increment())
      _ = tx.abort(new Exception("boom"))
      err <- tx.commit().failed
      after <- client.read(radicle)
    yield
      err.getMessage should be("boom")
      // The guard that matters: the update was staged before the abort and must not land.
      after.refcount should be(before.refcount)

  atest("isEmpty reports staged updates, not abort state"):
    given ExecutionContext = executionContext
    for
      before <- client.read(radicle)
      untouched = client.newTransaction()
      aborted = client.newTransaction()
      _ = aborted.setRefcount(radicle, before.refcount, before.refcount.increment())
      _ = aborted.abort(new Exception("boom"))
      _ <- aborted.commit().failed
    yield
      untouched.isEmpty should be(true)
      // The conflation this replaces: `valid` was false here, which read as "empty".
      aborted.isEmpty should be(false)
