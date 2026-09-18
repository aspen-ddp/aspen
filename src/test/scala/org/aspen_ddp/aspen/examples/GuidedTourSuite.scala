package org.aspen_ddp.aspen.examples

import org.aspen_ddp.aspen.IntegrationTestSuite
import org.aspen_ddp.aspen.client.{Transaction, TransactionAborted}
import org.aspen_ddp.aspen.common.Radicle
import org.aspen_ddp.aspen.common.objects.{DataObjectPointer, Delete, Insert, Key, KeyValueObjectPointer, ObjectRevision, Value}
import org.aspen_ddp.aspen.common.transaction.KeyValueUpdate

import java.nio.charset.StandardCharsets.UTF_8
import scala.concurrent.Future
import scala.language.implicitConversions

/** A guided tour of the Aspen client API.
  *
  * This suite builds a toy "bookmark store" out of two Aspen objects and then exercises it. It is
  * written to be read top-to-bottom as documentation -- it is reproduced in UsageGuide.md -- and it
  * runs as a normal test so the guide can never drift from a working API.
  *
  * The store consists of:
  *
  *   - an *index*, a KeyValueObject mapping a bookmark name to a URL
  *   - a *note*, a DataObject holding a free-form description of the store
  *
  * The index holds the note's ObjectPointer as one of its values. That is the whole trick behind
  * distributed data structures in Aspen: a pointer is just bytes, so any object can hold pointers to
  * other objects, and following them is how you walk the structure.
  *
  * `client`, `radicle`, and `atest` come from IntegrationTestSuite, which stands up a complete
  * in-memory Aspen system (see TestNetwork). A real application gets its `client` from
  * SimpleAspenClient instead -- see "Initializing an AspenClient" in UsageGuide.md. Everything after
  * that point is identical.
  */
class GuidedTourSuite extends IntegrationTestSuite:

  private def bytes(s: String): Array[Byte] = s.getBytes(UTF_8)
  private def string(b: Array[Byte]): String = new String(b, UTF_8)

  // The key under which we hang our index off of a root object.
  private val BookmarkStoreKey = Key("guided-tour-bookmarks")

  // Keys within the index itself.
  private val NotePointerKey = Key("note-pointer")
  private val AspenKey       = Key("aspen")
  private val ScalaKey       = Key("scala")

  atest("guided tour of the Aspen client API") {

    // ---------------------------------------------------------------------------------------
    // Step 1: Allocate two objects and link them into the system, all in one transaction.
    //
    // Objects are allocated from a StoragePool's ObjectAllocator, and allocation always happens
    // inside a transaction. Here we use `client.transact`, which creates a transaction, runs our
    // block against it, and commits when the Future our block returns completes.
    //
    // The critical rule: the same transaction that allocates an object must also store that
    // object's pointer somewhere reachable. Aspen has no directory of "all objects" -- an object
    // whose only pointer was dropped on the floor is unreachable forever, and its storage is
    // leaked. Doing the allocation and the linking atomically makes that impossible: either both
    // happen or neither does.
    // ---------------------------------------------------------------------------------------

    val tourText = "Bookmarks collected during the guided tour."

    def buildStore(): Future[(KeyValueObjectPointer, DataObjectPointer)] =
      client.transact { t =>
        given tx: Transaction = t

        for
          // Every pool defines its own IDA -- its replication or erasure-coding strategy -- so the
          // pool you allocate from determines how durable the object is and where it lives.
          // Radicle.poolId is the bootstrap pool, which always exists.
          pool <- client.getStoragePool(Radicle.poolId)
          alloc = pool.allocator

          // A DataObject is an opaque blob of bytes. The Array[Byte] converts implicitly to
          // DataBuffer, which is why `import scala.language.implicitConversions` is needed above.
          notePtr <- alloc.allocateDataObject(bytes(tourText))

          // A KeyValueObject is a map. We seed it with one real bookmark and with the note's
          // pointer, serialized via `toArray`. This is the embedded-pointer pattern: the index now
          // knows how to find the note.
          indexPtr <- alloc.allocateKeyValueObject(Map(
            NotePointerKey -> Value(notePtr.toArray),
            AspenKey       -> Value(bytes("https://github.com/aspen-ddp/aspen"))))

          // And now we make the index itself reachable, in this same transaction.
          //
          // IMPORTANT -- do not do this in a real application. We are writing our pointer straight
          // into the radicle because it keeps the example to a single concept, but the radicle is
          // Aspen's own root object and application data does not belong in it. Real applications
          // register their roots instead:
          //
          //   _ <- client.prepareRegisterId("my-app", "bookmark-store", storeUUID)
          //   _ <- client.prepareRegisterObject(storeUUID, indexPtr)
          //
          // and recover them later with `client.getRegisteredId` / `client.getRegisteredObject`.
          // Both prepare* forms stage into the transaction you supply, so registration stays atomic
          // with the allocation exactly as the direct write below is. See SimpleFileSystem.create
          // in the AmoebaFS sources for the real-world version of this step.
          //
          // Note what we require and what we do *not*. We require only that our own key is absent
          // -- not that the whole object is unchanged. Transactions touching disjoint keys of a
          // KeyValueObject do not conflict, so leaving `requiredRevision` as None lets unrelated
          // writers proceed concurrently. Passing Some(revision) from a prior read would instead
          // lock the entire object against any concurrent change, which is occasionally what you
          // want and usually not. Note also that we never had to read the radicle to write to it.
          _ = tx.update(
                radicle,
                None,                                                  // requiredRevision
                None,                                                  // contentLock
                List(KeyValueUpdate.DoesNotExist(BookmarkStoreKey)),   // requirements
                List(Insert(BookmarkStoreKey, indexPtr.toArray)))      // operations
        yield
          (indexPtr, notePtr)
      }

    // ---------------------------------------------------------------------------------------
    // Step 2: Find our data again by following pointers.
    //
    // This is what a read path looks like in a real application: start from a known root, decode
    // a pointer out of it, read the object it names, and repeat. Nothing here uses the pointers
    // returned by Step 1 -- we deliberately re-derive them from the radicle to show that the link
    // we stored is genuinely sufficient to recover everything.
    // ---------------------------------------------------------------------------------------

    def loadStore(): Future[(KeyValueObjectPointer, DataObjectPointer, String)] =
      for
        rkvos <- client.read(radicle)

        // `contents` is a Map[Key, ValueState]. A ValueState carries the value's bytes plus the
        // revision and timestamp of the transaction that last wrote *that key* -- per-key metadata
        // is what makes fine-grained requirements possible.
        indexPtr = KeyValueObjectPointer(rkvos.contents(BookmarkStoreKey).value.bytes)

        index <- client.read(indexPtr)
        notePtr = DataObjectPointer(index.contents(NotePointerKey).value.bytes)

        note <- client.read(notePtr)
      yield
        (indexPtr, notePtr, string(note.data.getByteArray))

    // ---------------------------------------------------------------------------------------
    // Step 3: Add a bookmark -- Insert guarded by DoesNotExist.
    //
    // Here we drive the transaction by hand rather than via `transact`, so the commit is visible.
    // `commit()` returns a Future that fails if the transaction aborts.
    // ---------------------------------------------------------------------------------------

    def addBookmark(indexPtr: KeyValueObjectPointer, key: Key, url: String): Future[Unit] =
      val tx = client.newTransaction()
      tx.update(
        indexPtr,
        None,
        None,
        List(KeyValueUpdate.DoesNotExist(key)),
        List(Insert(key, bytes(url))))
      tx.commit().map(_ => ())

    // ---------------------------------------------------------------------------------------
    // Step 4: Update a bookmark -- Insert guarded by KeyRevision.
    //
    // Aspen has no separate "set" operation: Insert overwrites an existing key. What distinguishes
    // a blind overwrite from a safe update is the requirement attached to it. KeyRevision says
    // "this key must still be at the revision I read", making this a compare-and-swap: if anyone
    // else wrote the key since we read it, our transaction aborts instead of clobbering them.
    // ---------------------------------------------------------------------------------------

    def updateBookmark(indexPtr: KeyValueObjectPointer,
                       key: Key,
                       expected: ObjectRevision,
                       url: String): Future[Unit] =
      val tx = client.newTransaction()
      tx.update(
        indexPtr,
        None,
        None,
        List(KeyValueUpdate.KeyRevision(key, expected)),
        List(Insert(key, bytes(url))))
      tx.commit().map(_ => ())

    // A helper for the two steps below that expect an abort. An unmet requirement surfaces as a
    // failed commit Future carrying TransactionAborted -- requirements are checked by the stores,
    // not the client, so you learn the outcome at commit time rather than when building.
    def expectAbort(f: Future[Unit]): Future[Boolean] =
      f.map(_ => false).recover { case _: TransactionAborted => true }

    // ---------------------------------------------------------------------------------------
    // Run the tour.
    // ---------------------------------------------------------------------------------------

    for
      // Steps 1 and 2.
      (allocatedIndex, allocatedNote) <- buildStore()
      (indexPtr, notePtr, noteText) <- loadStore()

      // Step 3: a new key, so DoesNotExist holds and the commit succeeds.
      _ <- addBookmark(indexPtr, ScalaKey, "https://scala-lang.org")

      // The same insert a second time. The key exists now, DoesNotExist fails, and the whole
      // transaction aborts -- no partial application, even though the operation itself is valid.
      duplicateAborted <- expectAbort(addBookmark(indexPtr, ScalaKey, "https://example.com"))

      // Step 4: read the key's current revision, then compare-and-swap against it.
      afterInsert <- client.read(indexPtr)
      scalaRevision = afterInsert.contents(ScalaKey).revision
      _ <- updateBookmark(indexPtr, ScalaKey, scalaRevision, "https://docs.scala-lang.org")

      // Reusing the revision we captured before that update makes it stale, so this aborts. This
      // is exactly the collision a second writer would hit. Aspen can retry through contention --
      // see transactUntilSuccessful -- but the mechanism is inefficient under load, so prefer
      // designs that keep concurrent writers off the same keys in the first place.
      staleAborted <- expectAbort(
                        updateBookmark(indexPtr, ScalaKey, scalaRevision, "https://example.com"))

      // -------------------------------------------------------------------------------------
      // Step 5: One transaction spanning both object flavors.
      //
      // Deleting a bookmark from the KeyValueObject and rewriting the DataObject happen together
      // or not at all. Mixing object types in a single transaction is ordinary -- the only hard
      // rule is that a given object may be modified once per transaction.
      // -------------------------------------------------------------------------------------
      current <- client.read(indexPtr)
      note <- client.read(notePtr)

      _ <- client.transact { t =>
             given tx: Transaction = t

             // Exists is the mirror of DoesNotExist: refuse to delete something already gone.
             tx.update(
               indexPtr,
               None,
               None,
               List(KeyValueUpdate.Exists(ScalaKey)),
               List(Delete(ScalaKey)))

             // DataObjects have no per-key structure, so `overwrite` replaces the whole blob and
             // the required revision applies to the object as a whole.
             tx.overwrite(notePtr, note.revision, bytes("Bookmarks, trimmed during the tour."))

             Future.successful(())
           }

      // -------------------------------------------------------------------------------------
      // Step 6: Delete the note object.
      //
      // Objects are not deleted by a delete call; they are deleted by setting their refcount to
      // zero. Because the index still holds a pointer to the note, we drop that pointer in the
      // same transaction -- otherwise we would leave a dangling pointer that a later reader would
      // follow into an InvalidObject error. Keeping pointer and referent in step is the caller's
      // job, and the transaction is the tool for it.
      // -------------------------------------------------------------------------------------
      noteState <- client.read(notePtr)

      _ <- client.transact { t =>
             given tx: Transaction = t

             tx.update(
               indexPtr,
               None,
               None,
               List(KeyValueUpdate.Exists(NotePointerKey)),
               List(Delete(NotePointerKey)))

             tx.setRefcount(notePtr, noteState.refcount, noteState.refcount.decrement())

             Future.successful(())
           }

      // `readOptional` returns None for an object that no longer exists, where `read` would fail.
      deletedNote <- client.readOptional(notePtr)
      finalIndex <- client.read(indexPtr)
    yield
      // Step 1 handed back the same pointers Step 2 recovered by traversal.
      allocatedIndex.id should be (indexPtr.id)
      allocatedNote.id should be (notePtr.id)

      // The note read back through two pointer hops is the text we allocated it with.
      noteText should be (tourText)

      // Both requirement violations aborted their transactions.
      duplicateAborted should be (true)
      staleAborted should be (true)

      // The compare-and-swap took effect, and the guarded delete removed the key.
      string(current.contents(ScalaKey).value.bytes) should be ("https://docs.scala-lang.org")
      finalIndex.contents.contains(ScalaKey) should be (false)

      // The note object is gone and the index no longer points at it.
      deletedNote should be (None)
      finalIndex.contents.contains(NotePointerKey) should be (false)

      // The original bookmark was never targeted by any of it and is untouched.
      string(finalIndex.contents(AspenKey).value.bytes) should be (
        "https://github.com/aspen-ddp/aspen")
  }
