# Usage Guide
This document will give you a brief introduction to the main Aspen classes and how to use them.
The [Architecture](Architecture.md) document needs to be read in order to fully understand how
to properly use Aspen to build distributed applications. It's rather long and involved though so
for those just looking to get the gist of what Aspen is and how to use it, the next section provides
an extremely condensed overview.

**Note:** Aspen is written in Scala and implemented in terms of the standard Future library. For
those unfamiliar with Scala or asynchronous programming using Scala Futures, the following code
will look a little weird. If you just look at it as oddly structured pseudo code though, you 
should be able to understand the key points.

## Architecture Crash Course
Aspen is all about building and using distributed data structures. Objects allocated in Aspen
result in a small, binary pointer that must be used to read the object's contents and metadata.
All objects are allocated and modified using transactions that can update multiple objects
simultaneously. Either all the objects are updated or none of them are. However, Aspen doesn't
yet provide Isolation support so reading back objects after a transaction commits can show some
as updated and others as not.

Objects are allocated from StoragePools that define the replication/erasure-coding strategy for
objects within the pool. These can vary between StoragePools and is a tunable design parameter.
The DataStores that comprise the pool and store the actual object data are not fixed in location
and can be moved on-the-fly to different geographical locations and backing storage media.

**Key Concept:** DataStore placement is a critical performance tuning nob. For low-latency data
lookups on a distributed B+ Tree, put the upper tiers of the tree in co-located, NVMe backed
media. The bulk data can go on cheaper spinning disks spread across the country.

**Key Concept:** Application design and operational runtime are intertwined yet distinct. Your
application can be designed to put upper tiers on co-located, NVMe media but a deployed instance
of your application can start off on a single host and a single storage device. The application
will function just fine, you just won't have the ideal reliability and performance. Your ops team
can incrementally grow your deployment as performance needs rise until they ultimately implement
the hardware to back your ideal deployment strategy.

**Key Concept:** DurableTasks - These are crash-proof, long-running operations that can provide
exactly-once operational guarantees. An example of such a task is deleting a file in AmoebaFS.
This is a multi-step operation that must delete the file content, which could take a very long time 
for a multi-terabyte file, and must properly deallocate the inode when the operation is complete. Aspen
provides a basic distributed processing service that ensures that if the host running your DurableTask
fails, another will pick it up and resume operation relatively quickly.

One last item is the Radicle. This is the name for the root object in an Aspen system. All objects stored
in an Aspen system are found by navigating the pointers stored in this object. Ultimately, Aspen is a giant
graph and the Radicle is the root node of the graph.

This should be enough to get you going but to really understand what's going on, read through 
the [Architecture](Architecture.md) document.

## Key Classes & Concepts
Despite being a fairly large project, the end-user API for developing applications on top of Aspen is
actually quite small. The following sections describe the most important classes that end-users will
interact with along with some of the important concepts concerning their use.

### AspenClient
The AspenClient class is the primary interface to an Aspen system. It provides several essential
functions.
* Exposes an ExecutionContext for use with the Future API
* read() methods for reading objects stored in the system
* Creates new transactions via the newTransaction() method
* Access to system metadata
* Creates ObjectAllocator instances for allocating new objects from groups of StoragePools
  (Read the [Architecture](Architecture.md) for more information on why this is important)
* Provides registries for storing and retrieving the ObjectPointers to your application's root objects
* Provides the ability to register DurableTasks
* Provides a BackgroundTaskManager for scheduling polling operations and other periodic operations

AspenClient also provides a few convenience mechanisms that continually retry operations in the
event of failure. The retries employ and exponential backoff mechanism to prevent overloading the
system. Two such methods are `transactUntilSuccessful` and `transactUntilSuccessfulWithRecovery`. The 
first just continually retries transactions until they eventually succeed. The "WithRecovery" variant
passes the Throwable that caused the transaction to fail to a handler mechanism that can take corrective
action and/or abort the retry loop if it determines that success is impossible.

### DataObject & KeyValueObject
Objects come in two flavors: Data and KeyValue. Both share the same metadata of ObjectRevision,
ObjectRefcount, and HLCTimestamp. The ObjectRevision is just the UUID of the last transaction to
successfully update the object and HLCTimestamp is the time that transaction started. The refcount is
exactly that. It's just a number for tracking reference counts and when a transaction sets it to zero, 
the object is deleted.

DataObjects are just binary blobs. KeyValueObjects are what the name implies. A collection of key-value
pairs. Each KeyValue pair has an associated ObjectRevision and HLCTimestamp and Transactions may target
them individually. Of critical importance is that Transactions operating on disjoint sets of kv pairs
will not conflict with each other. This makes them extremely useful for the efficient implementation of
certain data structures but they come at a cost. Their storage efficiency is poor and they require
significantly more overhead to use. Prefer using DataObjects and fall back to KeyValueObjects only as
needed.

### Transaction
Transactions are used for all object allocations and updates. DataObjects support simple overwrite
operations while KeyValue objects support more complex operations. Transactions also specify a list 
of requirements that must be met in order for the Transaction to successfully commit. This includes 
things like the ObjectRevision of a DataObject must exactly match a given value, a Key must not exist
within a KeyValue object, or the HLCTimestamp on a key must be greater or lower than a certain value.

The Transaction class records the set of object updates and transaction requirements that should be 
applied and, when everything is ready, the `def commit(): Future[HLCTimestamp]` method is used to 
attempt to commit the transaction.

**Note:** An object can only be updated once per transaction. A second attempt to modify the same
object will abort the Transaction and throw an exception.

**Note:** To reduce visual clutter, Transaction instances are usually passed via implicit arguments
similar to how ExecutionContexts are implicitly passed for Future callback handling. So, in the code
snippets below, note that the transactions are being used even though they aren't being explicitly passed
to the functions that require them.

### RegisteredTypeFactory
DurableTasks and a few other types that can be instantiated within the Aspen implementation use 
RegisteredTypeFactory instances to look up a Factory instance to create class instances. The lookup
is done by a UUID which is the single abstract variable exported by the class. Specifically for a
DurableTask, you register the DurableTaskType UUID along with the initial task state. When Aspen
launches the task, the factory is looked up via the registered UUID. When initializing the AspenSystem
class, you can pass in a list of your custom RegisteredTypeFactory instances to make them available to
the system.

**Note:** All Hosts and other processes attached to the same Aspen system must have access to these
types in order for them to work properly. A jar + class loader solution hasn't been implemented quite
yet so using them is currently a bit inconvenient as everything is hard coded. This should be an easy
problem to solve, it just hasn't made it to the top of the queue yet.

### Tiered Key Value List (TKVL)
This is a distributed B+ Tree provided by Aspen and is the workhorse underlying most of Aspen's internal
implementation. It's a general-purpose data structure and may be used by end-user applications. Its key
characteristic is that set() operations targeting different keys will not result in transaction
contention. Thus it is well suited to concurrent updates.

## Code examples
### A Guided Tour
If you're new to Aspen, start here. This example builds a toy "bookmark store" out of two objects
and then exercises it, covering the things nearly every Aspen application does: allocating objects,
embedding pointers to form a data structure, following those pointers back, guarding updates with
requirements, and deleting an object when you're done with it.

The store consists of an *index* (a KeyValueObject mapping a bookmark name to a URL) and a *note*
(a DataObject holding a free-form description). The index holds the note's ObjectPointer as one of
its values. That's the whole trick behind distributed data structures in Aspen: a pointer is just
bytes, so any object can hold pointers to other objects, and following them is how you walk the
structure.

The code below lives in `src/test/scala/org/aspen_ddp/aspen/examples/GuidedTourSuite.scala` and runs
as part of the test suite, so it can't drift away from a working API. Run it with:

```
sbt 'testOnly *GuidedTourSuite'
```

`client`, `radicle`, and `atest` come from `IntegrationTestSuite`, which stands up a complete
in-memory Aspen system. A real application gets its `client` from `SimpleAspenClient` instead — see
[Initializing an AspenClient](#initializing-an-aspenclient) below — and would reach its own root
object through the registry rather than through `radicle`. Everything after that point is identical.

#### Keys and helpers used throughout
The tour refers to these throughout, so they're defined once at the top of the suite. `Key` has
overloads for `String`, `Long`, and `UUID`, all of which just encode down to the byte array a Key
really is. The two converters exist only to keep the example readable — Aspen deals in bytes and has
no opinion about text encoding.

```scala
private def bytes(s: String): Array[Byte] = s.getBytes(UTF_8)
private def string(b: Array[Byte]): String = new String(b, UTF_8)

// The key under which we hang our index off of a root object.
private val BookmarkStoreKey = Key("guided-tour-bookmarks")

// Keys within the index itself.
private val NotePointerKey = Key("note-pointer")
private val AspenKey       = Key("aspen")
private val ScalaKey       = Key("scala")
```

#### Step 1: Allocate two objects and link them in, atomically
Allocation always happens inside a transaction. Here we use `client.transact`, which creates a
transaction, runs our block against it, and commits when the Future our block returns completes.

The critical rule: **the same transaction that allocates an object must also store that object's
pointer somewhere reachable.** Aspen has no directory of "all objects" — an object whose only
pointer was dropped on the floor is unreachable forever, and its storage is leaked. Doing the
allocation and the linking atomically makes that impossible.

```scala
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
```

#### Step 2: Find the data again by following pointers
This is what a read path looks like in a real application: start from a known root, decode a
pointer out of it, read the object it names, and repeat. Note that this re-derives everything from
the root — it never uses the pointers Step 1 returned — which demonstrates that the link we stored
really is sufficient to recover the whole structure after a restart.

```scala
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
```

#### Steps 3 & 4: Requirements are what make an operation safe
These two helpers issue the same `Insert` operation. The only difference between them is the
requirement attached, and that difference is everything: `DoesNotExist` makes the first an
insert-if-absent, while `KeyRevision` makes the second a compare-and-swap.

Aspen has no separate "set" operation — `Insert` overwrites an existing key. An unguarded `Insert`
is a blind clobber, so the requirement is the part you have to think about.

These also show the transaction being driven by hand, rather than through `transact`, so the commit
is visible. `commit()` returns a Future that fails if the transaction aborts.

```scala
def addBookmark(indexPtr: KeyValueObjectPointer, key: Key, url: String): Future[Unit] =
  val tx = client.newTransaction()
  tx.update(
    indexPtr,
    None,
    None,
    List(KeyValueUpdate.DoesNotExist(key)),
    List(Insert(key, bytes(url))))
  tx.commit().map(_ => ())

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
```

#### Running the tour
The rest of the example drives those pieces and demonstrates both requirement types failing as well
as succeeding. Steps 5 and 6 then show a single transaction spanning both object flavors, and the
refcount-based deletion of an object.

```scala
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
```

#### What to take away
* Allocate and link in the same transaction, always.
* A pointer is just bytes. `toArray` to store one, `KeyValueObjectPointer(bytes)` or
  `DataObjectPointer(bytes)` to get it back.
* The operation says what to change; the **requirement** says when that's allowed. Choosing the
  right requirement is most of what correct Aspen code is.
* Requirements are evaluated by the stores, so a violation surfaces as a failed `commit()` carrying
  `TransactionAborted`, not as an exception while you're building the transaction.
* `TransactionAborted` does not tell you *why* a transaction failed, only that it did.
* Objects die by refcount, and it's on you to remove pointers to them in the same transaction.
* Object refcounts exist for user convenience. It is perfectly acceptable to store multiple pointers
  to an object while leaving its refcount at 1. The distinction is similar to strong vs weak pointers
  in languages like Java & Python. Use whatever scheme best suits your application.

### Initializing an AspenClient
AspenClient accepts several parameters to tweak the timeouts for various internal components. Also,
the networking backend for Aspen is pluggable and in the following example a ZeroMQ based networking
backend is used. Both the networking plugin and AspenClient are implemented in terms of each other so
the initialization process is a touch awkward. The following excerpt is from the Aspen command line
utility.
```scala
  def createNetwork(bootstrapConfigFile: os.Path,
                    ohost: Option[(HostId, Int)],
                    oclientId: Option[ClientId]): (NetworkBridge, ZMQNet) = {
    val b = new NetworkBridge

    val heartbeatPeriod = Duration(10, SECONDS)
    val net = new ZMQNet(bootstrapConfigFile, oclientId, ohost, heartbeatPeriod, b)
    onetwork = Some(net)
    (b, net)
  }

  def createAspenClient(bootstrapConfigFile: os.Path,
                        onnet: Option[(NetworkBridge, ZMQNet)]=None): (AspenClient, ZMQNet, KeyValueObjectPointer) = {

    val (networkBridge, nnet) = onnet.getOrElse(createNetwork(bootstrapConfigFile, None, None))

    val txStatusCacheDuration = Duration(10, SECONDS)
    val initialReadDelay = Duration(10, SECONDS)
    val maxReadDelay = Duration(6, SECONDS)
    val txRetransmitDelay = Duration(1, SECONDS)
    val allocationRetransmitDelay = Duration(5, SECONDS)

    val sched = Executors.newScheduledThreadPool(3, DaemonThreads.factory("aspen-client"))
    val ec: ExecutionContext = ExecutionContext.fromExecutorService(sched)
    val bootstrapIda = BootstrapConfig.loadBootstrapConfig(bootstrapConfigFile.toIO).bootstrapIDA

    val ret = (new SimpleAspenClient(nnet.clientMessenger, nnet.clientId, ec, Radicle.pointer,
      bootstrapIda,
      txStatusCacheDuration,
      initialReadDelay,
      maxReadDelay,
      txRetransmitDelay,
      allocationRetransmitDelay),  nnet, Radicle.pointer)

    networkBridge.oclient = Some(ret._1)

    ret
  }
```

### Initializing AmoebaFS
The next except is pulled from AmoebaFS and shows the steps needed to create and register a new 
filesystem with Aspen.

```scala
object SimpleFileSystem {

  private val FileSystemUUIDKey    = Key(1)
  private val TaskExecutorRootKey  = Key(2)
  private val InodeTableRootKey    = Key(3)

  /** Create a new filesystem and register it under `fsName`.
   *
   *  Returns the new filesystem's UUID and the pointer to its root object. The name is
   *  registered in the namespaced registry under [[FileSystem.RegistryNamespace]] and the id
   *  is registered in the object registry against the root object, so that
   *  `load(client, fsName, _)` finds it again on a later run -- and on a later process.
   *
   *  Fails with KeyAlreadyExists if `fsName` is already taken. 
   */
  def create(client: AspenClient,
             allocator: ObjectAllocator,
             fsName: String): Future[(UUID, KeyValueObjectPointer)] = {

    given ExecutionContext = client.clientContext

    val fileSystemUUID: UUID = UUID.randomUUID()

    val rootDirMode = FileMode.S_IFDIR | FileMode.S_IRWXU
    
    client.transact { t =>
      given tx: Transaction = t

      for
        _ <- client.prepareRegisterId(FileSystem.RegistryNamespace, fsName, fileSystemUUID)
        taskRoot <- allocator.allocateKeyValueObject(Map())
        rootRoot = new Root(0, LexicalKeyOrdering, None, new SinglePoolNodeAllocator(client, taskRoot.poolId))
        rootDirInode = DirectoryInode.init(rootDirMode, 0, 0, None, Some(1), rootRoot)
        rootDirectory <- allocator.allocateDataObject(rootDirInode.toArray)
        rootDirectoryPointer = new DirectoryPointer(1, rootDirectory)
        inodeTableContentRoot <- allocator.allocateKeyValueObject(Map(Key(1) -> Value(rootDirectoryPointer.toArray)))
        inodeTableRoot = new Root(0, IntegerKeyOrdering, Some(inodeTableContentRoot), new SinglePoolNodeAllocator(client, taskRoot.poolId) )
        content = Map( FileSystemUUIDKey -> Value(uuid2byte(fileSystemUUID)),
          TaskExecutorRootKey -> Value(taskRoot.toArray),
          InodeTableRootKey -> Value(inodeTableRoot.encode()))
        fsRootPointer <- allocator.allocateKeyValueObject(content)
        _ <- client.prepareRegisterObject(fileSystemUUID, fsRootPointer)
      yield
        (fileSystemUUID, fsRootPointer)
    }
  }
}
```

### A Multi-Step Durable Task
The next snippet makes use of the SteppedDurableTask class inside Aspen. This class handles the 
details of tracking which step a durable task is on and restarting the last step when recovering from 
a crash. At each step, the state map may be updated to track durable state that should be maintained 
between each step. The steps themselves are defined in the `steps` array. The implementation starts at
index 0 and calls each step in order as the transactions implementing the step successfully complete.

Note that DurableTaskFactory derives from RegisteredTaskFactory. To create an instance of a DurableTask, 
TaskExecutor looks up the UnlinkFileTask object (which is a RegisteredTaskFactory) via its UUID, casts
the object to a DurableTaskFactory and then invokes `createTask`.

Also note that if the transaction to complete a step fails, SteppedDurableTask will continually retry
until the step succeeds. So if, for example, multiple entities are modifying the link count on the 
inode, there may be some contention but eventually the decrement will occur. And it will occur exactly
once. The transaction that updates the inode link count is the same transaction that updates the step of
the task. So if the update to the inode object succeeds, then the update to the state succeeded as well.
So, the update cannot happen more than once for a given task.

After the update to the link succeeds, the count is checked to see if it was set to zero. If so, the 
file content is deleted (a potentially long duration activity for enormous files), the inode is removed
from the inode table, and the task completes.

```scala
object UnlinkFileTask extends DurableTaskFactory:
  val typeUUID: UUID = UUID.fromString("B02539DC-3AE1-4E50-B52B-A5EFA6B5B330")

  private val FileSystemUUIDKey = Key(1)
  private val InodePointerKey   = Key(2)

  def createTask(client: AspenClient,
                 pointer: DurableTaskPointer,
                 revision: ObjectRevision,
                 state: Map[Key, KeyValueObjectState.ValueState],
                 taskExecutor: TaskExecutor): DurableTask =
    val fsUUID = byte2uuid(state(FileSystemUUIDKey).value.bytes)
    val ptr = InodePointer(state(InodePointerKey).value.bytes)
    val fs = FileSystem.getRegisteredFileSystem(fsUUID).get

    new UnlinkFileTask(pointer, fs, ptr)

  def prepareTask(fileSystem: FileSystem,
                  inodePointer: InodePointer)(using tx: Transaction): Future[Future[Option[AnyRef]]] =
    val (stepKey, stepValue) = SteppedDurableTask.getInitialContent(Map.empty)
    val istate = List(
      stepKey -> stepValue.bytes,
      FileSystemUUIDKey -> uuid2byte(fileSystem.uuid),
      InodePointerKey -> inodePointer.toArray
    )
    fileSystem.taskExecutor.prepareTask(this, istate)


class UnlinkFileTask(
  taskPointer: DurableTaskPointer,
  val fs: FileSystem,
  val iptr: InodePointer
) extends SteppedDurableTask(taskPointer, fs.client):

  val steps: Array[(Transaction, Map[String, Array[Byte]], ObjectRevision) => Future[Map[String, Array[Byte]]]] =
    Array(decrementLinkCount, checkForDeletion)

  def decrementLinkCount(tx: Transaction, state: Map[String, Array[Byte]], stepRevision: ObjectRevision): Future[Map[String, Array[Byte]]] =
    given Transaction = tx
    for
      (inode, _, revision) <- fs.readInode(iptr)
    yield
      val newLinks = inode.links - 1
      tx.overwrite(iptr.pointer, revision, inode.update(links = Some(newLinks)).toArray)
      state.updated("updatedLink", int2byte(newLinks))

  def checkForDeletion(tx: Transaction, state: Map[String, Array[Byte]], stepRevision: ObjectRevision): Future[Map[String, Array[Byte]]] =
    given Transaction = tx
    val updatedLink = byte2int(state("updatedLink"))
    updatedLink match
      case 0 =>
        for
          entry <- fs.lookup(iptr)
          _ <- entry.freeResources()
          _ <- fs.inodeTable.delete(iptr)
        yield
          tx.setRefcount(iptr.pointer, ObjectRefcount(0, 1), ObjectRefcount(1, 0))
          state
      case _ => Future.successful(state)
```

## Where To Go From Here
The AmoebaFS implementation is a good source for examples of how to use Aspen as are portions of the
Aspen codebase itself. Aspen is a self-hosting system and is implemented in terms of itself. All the
metadata needed to define and run an Aspen system is stored within the Aspen system as are most 
of the operations it performs. Things like migrating a DataStore from one StorageDevice to another are 
implemented in terms of DurableTasks and Transactions that update metadata state objects. Of particular
note are the AspenClient and Host classes. These make heavy use of metadata objects. And for examples of
how to build distributed data structures, take a look at the implementation of the linked list 
implementation in the aspen.client.tkvl package. The Tiered Key Value List that implements a
distributed B+ Tree on top of it is also a good example.