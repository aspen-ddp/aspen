# Usage Guide
This section will give you a brief introduction to the main Aspen classes and how to use them.
To fully appreciate how this code works and what's going on under the hood, you must read 
the [Architecture](Architecture.md) document. It's rather long and involved though so for those
just looking to get the gist of what Aspen is and how to use it, the next section provides
an extremely condensed overview.

**Note:** Aspen is built on top of Scala's Future library. A solid understanding of asynchronous
programming using that library is essential to understand this code.

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