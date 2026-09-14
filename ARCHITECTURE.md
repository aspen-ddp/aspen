# Architecture Overview

Aspen is a general-purpose distributed data platform that aims to serve as the foundation for building
distributed applications of various types. Given this intent, Aspen’s design focuses on providing highly
flexible tools for application design and also for highly flexible run-time operation.

To achieve this, Aspen provides an object storage system where, similar to C++, object allocation in 
Aspen results in a binary pointer. Those pointers may be stored in other objects to create arbitrary 
distributed data structures. Just like various in-memory data structures are optimized for specific use 
cases, so too may their distributed variants and generally for the exact same reasons. Those data 
structures then serve as the foundation upon which the desired application is built.

All updates to Aspen objects are made by way of transactions. Transactions may simultaneously update
multiple objects and provide ACD guarantees from the traditional ACID model (Isolation support will be
added in the future). Multi-object transactions are often employed to ensure the consistency of 
distributed data structures. For example, splitting a node in a distributed B-Tree would use a 
transaction that allocates a new node with the migrated content, deletes the migrated content from the
old node, and includes a revision guard requirement that ensures no new data has been added to the 
node being split since the last time it was read. Should data have been added in the intervening 
space, the transaction will fail and the split operation would have to be restarted.

# System Foundations

Aspen is a self-hosting system that is largely implemented in terms of itself. At the core are just 
four fundamental components: DataStores, StoragePools, Objects, and ObjectPointers. Everything else
in Aspen is built upon those four key pillars.

As one might expect, DataStores are the logical component that actually stores the data and metadata
associated with objects. There are two important aspects to DataStores from the system perspective. 
First, the backend storage mechanism is pluggable. There is no silver-bullet storage backend that 
provides ideal performance across all possible use cases. Sometimes a RocksDB backend will be best 
suited to an application, sometimes a segmented flat-file will be best, and sometimes a system will
need a mix of both or several other alternatives at the same time. Aspen provides the flexibility to
tailor this to suit the application’s needs and even allows conversion from one backend to another 
if needed.

The second key aspect of DataStores is that they are not fixed in place. They can be migrated between
machines and physical storage devices while the system is running. This means that DataStores can be 
moved to the geo-location and on the backing media most advantageous to the application. For ultra 
low-latency access, placing the stores on machines in the same data center and on NVMe media might be
best. For bulk data that doesn’t have strict access time requirements, spreading it out across 
multiple data centers to prevent downtime should a site go offline might be the better option. Aspen
allows the flexibility to mix and match these options as needed to best suit the application. It can
also change on-the-fly as the system grows. Start out on a single machine, grow to a handful of 
servers, then a data center, and eventually across the globe as needed.

StoragePools contain DataStores. Each StoragePool contains a number of DataStores defined by the 
width of the pool’s Information Dispersal Algorithm or IDA for short. The IDA defines whether the 
pool is using replication or erasure-coding to protect against data loss and the numbers associated 
with that algorithm. For replication, the width is the number of replicas to create and the 
write-threshold is the number of DataStores that must agree to commit a transaction in order for it 
to succeed. At a minimum, the write-threshold must be a simple majority of stores as we determine 
the most recent version of an object by finding the version that the majority agrees upon. However, 
a simple majority for the write-threshold isn’t enough for real-world use since that would mean that
a recently-written object could tolerate zero device failures before becoming unreadable. Practical
systems will set this to a more conservative value of, say 4 out of 5, which would allow a write to
succeed in the presence of a failed drive and still allow one more drive failure to be sustained 
before data was irreparably lost.

Erasure-coding is an alternative to replication and will almost always be the better choice for 
real-world solutions. A quick primer on erasure coding for those unfamiliar with it is that it is a 
mechanism used to recover from data loss with far less overhead than replication. In the previous 
example, only 2 drive failures could be lost and it required 500% more storage to achieve. A good 
erasure-coding approach could tolerate 6 simultaneous failures but with only 60% more storage. A very
brief but accurate description of how this works is that you take your chunk of data and slice it 
into 10 equally sized slices. You then do some complex math to synthesize 6 more slices yielding a 
total of 16 slices of data to store. When it comes time to restore the original chunk of data, you 
just need any 10 slices, original or synthesized. In terms of the IDA numbers for erasure-coding, 
the width is the total number of slices (which could be 16 in the previous example). The 
read-threshold is the number of slices needed to restore the object (10 in the previous example). 
And the write-threshold is the number beyond the read-threshold that must be achieved for a 
successful write operation to succeed. It functions identically to the replication scenario and 
provides a guard against failures for recently-written objects.

Back to the StorgePools, the IDA is the most important value the pool defines but it may also 
optionally define a maximum object size for objects allocated from the pool. This is for certain
backends that have limitations on the maximum size of the data they can store, such as a flat-file
segmented into fixed-sized chunks. An additional point to note is that StoragePools constrain the 
data dependencies. Object data is stored exclusively amongst the DataStores contained within the 
pool and Objects cannot be moved between pools. Likewise, DataStores cannot be moved between pools. 
Their physical location can migrate but not their logical location of belonging to the pool. In fact,
the identity of a DataStore is the UUID of the pool it belongs to plus its index into the array of 
DataStores that comprise the pool.

Objects in Aspen come in two flavors: DataObjects and KeyValueObjects. Common between the two is that, in
addition to their data, they each have an HLCTimestamp indicating the time of their last update, a 
Revision which changes with each update, and a ReferenceCount. Objects are deleted when a transaction 
sets the reference count to zero.

DataObjects are traditional binary blobs whereas KeyValueObjects are significantly more complex. As 
their name implies, KeyValueObjects contain key, value pairs and are implemented such that 
transactions operating on disjoint sets of keys do not conflict with one another. Their storage 
efficiency is poor and they require significantly more overhead to use but the non-conflicting 
updates to different keys can be very useful for effective implementation and use of certain data 
structures. They can also be helpful for reducing the potential for transaction contention when 
designing applications built on top of Aspen. As with everything else though, they’re an effective 
tool when used appropriately but don’t go thinking you can use these to build a solution that will 
compete with a dedicated key-value database like FoundationDB. DataObjects should be the default 
choice with the fallback option of KeyValueObjects when non-conflicting updates are truly needed.

The final core component is ObjectPointers. These are quite simple and, as mentioned before, they are 
logically equivalent to C++ pointers. They’re used to read and write the corresponding object content 
and that content cannot be accessed without them. If you lose the ObjectPointer for an allocated 
object, it is forever leaked. Consequently the same transaction that allocates an object should also 
store the pointer in an appropriate place for future retrieval, such as in the previous node of a 
linked list when adding a new node.

Logically, ObjectPointers consist of three things: the UUID of the object, the UUID of the 
StoragePool the object belongs to, and an optional array of bytes that can be used by the DataStore 
backend to locate the object within the store. A potential use for this byte array would be to store 
the index of the object within a flat-file segmented into fixed-sized chunks. Reading and writing 
such an object would be extremely simple as the content could be found at offset segmentSize * 
objectIndex. Due to the optional and variably-sized array, ObjectPointers do not have a fixed size.

## Transaction Model

In addition to the four key concepts mentioned above, we need to add the concept of a Client in order 
to discuss the transaction model. Most of the details can be deferred to later though. For the moment,
just consider it to be the thing that communicates with DataStores to read objects and initiate 
transactions.

Once a client has read a few objects and wants to make changes, it creates a TransactionDescription 
which defines all the objects to be modified, the requirements that must be met for the transaction 
to commit, a primary object, and a designated leader index. The object and requirement lists are 
straight-forward to understand but the primary object and designated leader index need a bit more 
explaining. The purpose of those two attributes of the TransactionDescription is to define which 
DataStore should drive the transaction to conclusion. Clients initiate transactions but it’s up to 
the DataStores to ensure that the transactions are eventually driven to conclusion. The reason for 
this is that a Client is a single point of failure. If it crashes, there is no obvious candidate to 
take over and finish what it started. The DataStores, on the other hand, always come in groups that 
already exist for the sole purpose of providing redundancy and error recovery. The primary object 
defines which set of DataStores is responsible for driving the transaction to its conclusion and the 
designated leader is the randomly chosen index within that set of stores that will serve as the 
“designated leader” of the transaction. That store will drive the transaction to conclusion. Unless, 
of course, that store is offline. In which case, failure recovery kicks in and another DataStore will 
volunteer to take over.

Transactions are designed to reach consensus on the Commit/Abort decision as fast as possible. The 
original, unmodified, single-synod Paxos algorithm is used to achieve consensus on this decision and 
it does so in a single round-trip in the error free case. To do this, we send some additional data 
along with the Prepare and Promise messages. The TransactionDescription and updated object content 
are sent along with the Prepare message. Each DataStore examines the state of its local objects and 
if all the requirements are met, it writes its “VoteCommit” decision to persistent state along with 
the TransactionDescription, local object data, and the required Paxos minimum proposal id and 
accepted value state. The Promise message is sent to all DataStores for all objects in the 
transaction. Each DataStore tracks the promise messages it receives from all stores and, if the 
VoteCommits exceed a 2/3rds majority for all objects to be modified, it will be impossible for the 
transaction to reach any decision other than Commit even if all the DataStores crash and have to be 
recovered. Because of this, each DataStore can self-generate the Accept message rather than waiting 
for the designated leader to send the message over the network. This is what allows the 
one-round-trip commit resolution. However, it does come at the cost of increased message delays in 
the case of an Abort decision. To allow self-generation of the pro-commit Accept message, it’s the 
only valid Accept message that can be sent in round 1 of the Paxos algorithm. To resolve an abort, we 
have to move on to round 2 and start with another Prepare message for the new round.

It’s worth pointing out that the transaction recovery process is pessimistic and, when enacted, it 
always attempts to drive the transaction to an Abort decision. If sufficient stores have received the 
necessary data to achieve a commit, the recovery process will still result in a commit decision but 
because the DataStore driving the transaction does not have the full data for every object being 
modified, it cannot re-transmit Prepare messages with the local object updates for each DataStore. 
Thus it always tries to Abort.

One reason transactions are intended to commit/abort as fast as possible is that once a DataStore 
votes to commit a transaction, the objects it hosts are locked to that transaction until the 
transaction completes with a commit/abort decision. If two transactions simultaneously try to update 
the same object, only one of them will receive a VoteCommit. When contention is encountered, an 
Optimistic Concurrency Control solution is used with the following set of rules:

1. If success is impossible due to Abort votes by width – write-threshold stores, abort.
2. If a write-threshold number of stores granted permission, commit.
3. If no store granted commit permission, abort.
4. Examine the timestamps of the other transactions for which stores granted commit permission. 
   Continue if this transaction has the lowest timestamp. Otherwise, abort.

Combined with an exponential backoff mechanism for retransmitting messages, this approach ensures 
that transactions eventually conclude. However, transaction contention can be expensive to resolve so 
applications should be designed to avoid it in the common case.

### Finalization Actions
In addition to the content mentioned in the previous section, TransactionDescriptions also carry a 
list of FinalizationActions. These are cleanup activities that must be successfully completed before 
the state for a transaction that resolved in a commit can be forgotten by a DataStore. 
FinalizationActions provide “at least once” guarantees and must be idempotent as the transaction 
recovery process can cause them to be executed multiple times.

FinalizationActions are intended for short-duration activities only as they consume persistent 
storage space and, if too many are left running at once, they could potentially clog up the 
transaction system resulting in slowdowns or potentially even lockups in the extreme case.

Two example use cases that Aspen uses internally is for recording DataStores that are suspected of 
having been offline during a successful transaction commit so the object can be later repaired and 
inserting an ObjectPointer for a newly allocated node in a distributed B-Tree into the tier above it. 
Both are idempotent, short-duration activities that are well suited to this lightweight mechanism.

### Durable Tasks
For long-duration and/or multi-step tasks, Aspen provides a crash-proof DurableTask solution that 
consists of two things: a UUID that identifies the task type and an ObjectPointer to the 
DurableTask’s state. As each step in the task is completed, the state object is updated to indicate 
that the next step should be started. Should the entity driving the task crash, the task can be 
recovered and restarted later, either by the recovered entity or some other volunteer. Upon recovery, 
the state of the task is read and the current step is restarted.

In addition to crash-recovery, DurableTasks can also provide exactly-once guarantees when needed. To 
do this, simply add the objects that need to be modified exactly once to the transaction that updates 
the task state. Either the operation succeeds and the task moves on to the next step or the operation 
must be retried until success is achieved.

### Tiered Key Value Lists (TKVLs)
The Tiered Key Value List is the general-purpose distributed data structure that supports most of 
Aspen’s internal structure. It is essentially a distributed B+ Tree implemented in terms of tiers of 
linked lists with sorted key-value pairs. The bottom tier, Tier-0, stores the data and the linked 
list tiers above it contain pointers to the nodes in the tier below with the key being the minimum 
key allowed within the node, just like a standard B+ Tree. Any element can be found by starting with 
the first node of the tier-0 linked list and simply scanning to the right until the target key is 
found. The upper tiers simply serve as a short cut to reduce the amount of scanning required to find 
the target key.

The wrinkle is that navigating left-to-right within a single tier is guaranteed to be consistent due 
to transactional updates inserting and removing nodes from the linked list. Top-to-bottom navigation, 
however, is not guaranteed to be consistent. To reduce the potential for transaction contention, 
inserting or removing a node from a tier does not also include the insertion/removal of the pointer 
to it in the tier above. Instead, the transaction that does the insertion/removal includes a 
FinalizationAction that will modify the upper tier on its behalf. This is a short-duration operation 
and multiple attempts at performing the same action do not cause problems so it is well suited to 
being implemented as a FinalizationAction.

The potential for downward pointers being missing or pointing to nodes that no longer exist 
complicates the tree navigation logic a bit and may require some additional scanning but the result 
is a scalable tree structure that can hold vast quantities of data and that has minimal potential for 
transaction contention. KeyValueObjects are used for all nodes so individual insertions and removals 
have little chance of running into conflicts.

### Recovering From Missed Transactions
Each StoragePool has a “missed transactions tree”. This is a TKVL used to flag objects that are 
likely in need of repair. When a transaction is finalized, the DataStore driving the transaction to 
resolution looks through the messages received from all of the stores that participated in the 
transaction. If there are missing responses from DataStores or the transaction driver has other 
reasons to suspect that a store may not have updated its local state, the transaction driver makes a 
note of it in the missed transactions tree for that store’s StoragePool.

The key for the entry in the tree is the 1-byte index of the DataStore within the StoragePool 
followed by the UUID of the object and the value is the optional array of bytes from the 
ObjectPointer for the object. The 1-byte prefix is used to ensure that all of the key-value pairs for 
a specific DataStore bunch together and, as the tree size grows, each store effectively gets its own 
sub-tree within the overall TKVL.

To repair the errors in the tree, background tasks constantly scan the missed transaction tree on 
behalf of each online DataStore and, when entries are seen, it reads the current state of the object 
suspected to have fallen behind and updates the DataStore’s state to the current value. The entry is 
then removed from the missed transaction tree and the repair process continues on until every entry 
in the tree is repaired. This process continues on indefinitely as missed transactions can happen at 
any time.

One point to note is that transactions modifying the missed transactions tree disable the usual 
FinalizationAction that updates the missed transaction tree when DataStores may not have successfully 
handled the transaction. This prevents an infinite loop from forming when DataStores hosting the 
missed transaction tree are unavailable. Repairs to the objects comprising the tree rely on 
Opportunistic Rebuilds which are performed during object reads. If, when an object is read, the 
reader sees that the state for an object returned by one of the stores is behind its peers, it will 
send the repair state to the store “opportunisticly” to help it recover. This is an insufficient 
general solution as it wouldn’t be an effective solution for infrequently read objects. In this case, 
however, the missed transaction tree is constantly being traversed so this mechanism is sufficient to 
ensure timely repairs.

### Recovering Lost DataStores
In addition to a missed transaction tree, each StorgePool also has an “allocation tree”. When objects 
are allocated, the allocating transaction has a FinalizationAction to insert a pointer to the newly 
allocated object into the allocation tree. If a store is lost due to the failure of its underlying 
storage media, it can be recovered by simply creating an empty DataStore backend of the appropriate 
type and then by walking the allocation tree from left to right. As each object is read, the local 
store’s state is written to the DataStore. Once the local state for all objects is reconstructed, the 
rebuild process is complete. The local state for some objects may have fallen behind while the 
rebuild process was being conducted but the missed transaction tree will be used to bring everything 
back into sync.

## Self Hosting
Much of Aspen’s functionality is implemented in terms of itself. All the metadata needed to define 
and operate an Aspen system is stored within the system itself. Similarly, background services and 
tasks needed to operate the system and keep it healthy are defined in terms of Aspen objects.

### Metadata
When an aspen system is bootstrapped, it creates a pool named “aspen-bootstrap” that contains the 
metadata needed to define and operate the system. Specifically, that metadata includes:

* Host Machines
* Storage Devices
* Storage Pools
* Storage Device Sets
* Allocation Groups

One point to note is that the primary identifier for all metadata entries is a UUID. Some entries  
also have human readable names for convenience but, internally, everything is implemented in terms of 
UUIDs.

#### Host Machines
Storing the list of hosts that comprise the system within the system itself does lead to a 
bootstrapping issue when bringing the system up. To support this, all applications connecting to an 
Aspen system, including the Aspen command-line utility and the storage hosts themselves, must use a 
bootstrapping configuration file that lists the network connection information for the machines 
hosting the DataStores that belong to the “aspen-bootstrap” StoragePool. Once an application has 
access to this pool, it can look up the connection information for all other host machines.

In addition to network connection information, the host metadata also includes a list of all 
StorageDevices it contains.

#### Storage Devices
The primary intention for StorageDevice metadata entries is to define all of the physical storage 
media available within an Aspen system. Typically this will be hdd, ssd, and NVMe drives but could 
also refer to more exotic setups like RAID arrays or LVM volumes in the (unlikely) event it makes 
sense to do so.

The data tracked for each StorageDevice is:
* Host Machine Id the store belongs to
* Storage Device Set Id the store belongs go
* Total size in bytes
* Approximate current usage in bytes
* The list of Data Store Ids assigned to this store and their associated state which is one of: 
  Initializing, Active, TransferringIn, TransferringOut, Rebuilding

When a StorageDevice fails, it is always considered a total loss and all DataStores it was hosting 
are considered lost as well.

#### Storage Pools
The metadata entries for StoragePools contain the following items:
* IDA
* Optional maximum object size
* Storage Device Set Id the pool is assigned to
* Maximum store size in bytes
* Approximate current usage in bytes
* List of Allocation Group Ids the pool is a member of
* Array of StoreEntries with one entry per DataStore
* Allocation Tree ObjectPointer
* Error Tree ObjectPointer

There are a few other items as well but those are the key elements. The array of StoreEnties requires 
a bit more explanation. The StoreEntry is a tuple of (StorageDeviceId, HostId). The StorageDeviceId 
alone is technically sufficient since we can use that to look up the StorageDevice metadata which 
contains the Host Machine Id. However, that would mean multiple extra reads with no additional 
benefit for an extremely common operation so we cache the Host Machine Id in the StoragePool.

Maximum DataStore size and current usage is a fuzzy issue. The only hard limit is that DataStores 
cannot exceed the size of the physical media backing them. In Aspen, DataStores are either completely 
healthy or completely lost. There is no concept of partial failure, it’s all or nothing. So 
DataStores cannot logically straddle physical media. The “maximum size” in the StoragePool’s metadata 
is a soft limit and it can be changed on the fly.

In practical terms, DataStores should generally be kept small compared to the size of the backing 
media. 10% is a good rule of thumb. The reason for this is to facilitate rebalancing efforts where 
DataStores are shuffled between StorageDevices to keep their usage approximately equal. Adding and 
removing StorageDevices is implemented in terms of rebalancing and is also facilitated by having 
small stores relative to the size of the media.

When it comes to current usage, this value is a best-effort guess that is complicated by a number of 
factors. One is that while all DataStores should, in theory, report back the same value, practical 
complications like missed transactions that add, delta, and change the size of objects can be missed. 
This skews the values between stores so an average is the best we can do. Another complication is 
that not all backends can report an accurate number. RocksDB, for example, doesn’t immediately clean 
up deleted data so it may report much higher usage than necessary.

#### Storage Device Sets
Storage Device Sets aggregate StorageDevices and other StorageDeviceSets into logical groups of 
storage. The sets have a fixed numeric level assigned to them. Level-0 sets consist exclusively of 
StorageDevices and Level-1+ sets consist exclusively of StorageDeviceSets below their level (using 
explicit levels is just a simple way to prevent the accidental formation of cycles).

Sets are defined by the runtime operators and are tailored to the deployment and use case. Using a 
distributed file system as an example, it might make sense to define the Level-0 sets: 
sacramento-5400rpm-hdd, austin-5400rpm-hdd, boston-5400rpm-hdd. Then maybe the Level-1 set 
us-5400rpm-hdd that contains those three Level-0 sets.

StoragePools are assigned to a specific StorageDeviceSet and spread their DataStores uniformly across 
the set members. So, a pool assigned to sacramento-5400-rpm-hdd would be local to Sacramento. A pool 
assigned or migrated to us-5400rpm-hdd would have its DataStores spread across the three regional sites.

Level-0 StorageDeviceSets also define the scope in which rebalancing efforts are made. A background 
task periodically queries the current usage of StorageDevices in Level-0 sets and will migrate 
DataStores between members of the set to keep all members as close to balanced as possible. This 
mechanism is also used to gracefully add and remove StorageDevices from the set.

#### Allocation Groups
AllocationGroups exist to spread object allocations across a group of StoragePools or other 
AllocationGroups. Similar to StorageDeviceSets, AllocationGroups use the same Leveling strategy to 
form groups of groups for allocation purposes.

Allocation groups track the approximate usage of all group members and employs a weighted selection 
method when choosing a pool or group to allocate an object from. Pools and groups that are relatively 
empty will receive proportionately more allocations than those that are more full.

### Durable Services
DurableTasks are good for defining multi-step operations that can recover from crashes. However, 
something must detect that a DurableTask needs to be resumed in the event of a crash. Some tasks are 
local to a persistent entity like a host machine which can know to look for and restart any tasks 
that it was running previously. However, not all entities connecting to an Aspen system will 
necessarily be persistent. The Aspen CLI, for example, can initiate long-duration operations such as 
rebalancing a StorageDeviceSet but it would be completely unreasonable to expect that CLI process to 
drive the potentially multi-hour process to conclusion and reconnect and resume that operation should 
the CLI crash. A more general-purpose mechanism is needed.

On a related note, some persistent services need to be continually run. Rebalancing, for example, 
must periodically scan StorageDevices and shuffle DataStores around to ensure smooth operation. It 
needs to always be running somewhere in the system.

To solve these issues, Aspen includes the concept of DurableServices. These are similar to 
DurableTasks in that they use a UUID to identify the service type and an object to store the service 
state but they are intended to always be running somewhere in the system. To facilitate this, there 
is a global DurableServices TKVL. The key is a UUID identifying the service instance and the value is 
a tuple of (ServiceTypeUUID, leaseExpiryTimestamp, stateObjectPointer).

All host machines periodically scan the durable services tree and examine the lease timestamps. If a 
host sees that the timestamp for a service has expired, it will attempt to claim ownership of the 
service. If it succeeds, it uses the ServiceTypeUUID to look up a factory for creating the 
DurableService instance and invokes it.

Presently there are two DurableServices: the SystemTaskExecutorService and the RebalancingService.

#### SystemTaskExecutorService
This service periodically scans a global TKVL where DurableTasks can be registered that need to be 
executed. To prevent a single machine from being overloaded, the service doesn’t execute the task 
itself. Instead, when a new task is found, the service chooses a random host machine and asks that 
machine to execute the task on its behalf. A heartbeating mechanism is used to ensure the task 
remains running and if the heartbeats stop, the task is reassigned to another machine. Upon 
completion, the entry for that task in the global TKVL is deleted and the service forgets it ever 
existed.

To ensure timely action to newly registered tasks, a nudge message is sent to the service to cause it 
to immediately re-scan the global TKVL. Thus if the CLI creates a DurableTask to transfer a DataStore 
transfer to a new device, it will send the nudge to the service so it picks the task up immediately 
rather than waiting for the next polling cycle to notice it.

#### RebalancingService
As the name implies, this is a service that periodically scans the usage of StorageDevices in level-0 
StorageDeviceSets and initiates DataStore transfers to balance the usage across the set members. By 
default, this is done every 8 hours but the Aspen CLI may be used to trigger an immediate rebalance. 
A motivator for doing so would be after adding a few new StorageDevices to an almost-full set.

### The Radicle
If you’ve ever seen a grove of Aspen trees, you probably didn’t realize you were looking at a single 
organism, not a collection of individual trees. Aspens grow in large clonal clusters, formed from a 
single seedling, and sprout new trees as the clusters spread through the soil. Essentially, they are 
nature’s implementation of a distributed graph that can live for tens of thousands of years and is 
robust against the destruction of individual trees.

In Botany, the radicle is the first part of the seedling to emerge from the seed. Correspondingly in 
Aspen, the Radicle is the root object from which all others can be found. The Radicle stores the root 
node for the HostMachine, StorageDevice, StoragePool, StorageDeviceSet, and AllocationGroup TKVL 
trees along with a TKVLs for a general purpose NameRegistry which maps namespaced names to UUIDs and 
a general purpose ObjectRegistry which maps UUIDs to ObjectPointers. Applications built on top of 
Aspen may use these two registries to obtain references to their distributed data structures without 
needing to modify the Radicle directly.

# Implementation
Several languages were tried for implementing Aspen but Scala ultimately won out in the end. The 
flexibility of the JVM paired with Scala’s excellent support for asynchronous programming hit the 
sweet spot for the initial implementation.

One aspect of the codebase that was hinted at in previous sections is that Aspen clients can inject 
their own implementations for DurableTasks, Services, and FinalizationActions into the library. Aspen 
defines a RegisteredTypeFactory that contains a single abstract UUID variable. When an instance of a 
Service, DurableTask, or FinalizationAction is needed, it looks up the RegisteredTypeFactory for the 
given UUID and dynamically casts it to a factory class that can create an instance of the appropriate 
type. If the cast succeeds, the appropriate subclass can be constructed. Applications built on top of 
Aspen can pass in a list of RegisteredTypeFactory instances at initialization time to allow their own 
subtypes to be used.

One big caveat to note with this approach is that those types must be passed to every host and client 
attached to the Aspen system. FinalizationActions are executed by the machines hosting the DataStores 
so they must have access to the code that implements the actions. Given the flexibility of the JVM, 
there’s probably a good dynamic way to do this with jars and classloaders but, for the moment, it's 
all hardcoded.

Another point to note is that Aspen’s networking subsystem is pluggable. The initial implementation 
was built on top of ZeroMQ for simplicity but the right way to go for a real system is probably to 
build one based on Netty & QUIC.

Most people that have spent much time with real-world, large-scale distributed systems realize that 
you can’t rely on direct communication with network endpoints for correctness. Machines go offline, 
network partitions cut off communication, and all other sorts of things can go wrong that make “do 
this now” communication unreliable. A common pattern in Aspen’s implementation is to write a request 
for action, such as registering a DurableTask with the system service for execution, into a location 
that is periodically polled and sending a “nudge” message that causes the service to poll immediately 
rather than waiting for the next timeout to expire. This has the same effect as a “RunThisTaskNow” 
direct message but it’s guaranteed to be reliable even if the nudge message is lost because the 
polling operation will eventually pick it up.

An important note about polling operations in Aspen though is that, because reads do not time out, 
all polling operations need to be protected against pileups. If a network partition occurs that cuts 
off communication, potentially hundreds or thousands of read operations could be initiated and all of 
them will flood the system when communication is restored. The BackgroundTaskManager class has a 
scheduleNonConcurrentPollingTask() for this purpose that only invokes the supplied function if the 
previous call has completed. Adding a read method that supports timeouts would be another potential 
solution but hasn’t been implemented yet.

In terms of operational safety, Aspen uses a crash-only architecture. There is no clean shutdown 
process and it’s safe to kill -9 a process at any time.

To protect the integrity of transactions in the event of system crashes or kill -9 commands, Aspen 
uses a CrashRecoveryLog which functions much like a WriteAheadLog for a database. The 
TransactionDescription, local object data, and required Paxos state is written to disk when a 
transaction is started and the Paxos state is updated as needed. The current implementation functions 
much like a circular ring buffer split over a few files. When beginning to overwrite a new file, any 
needed state for live transactions in the second-to-last file is first copied over to the new file. 
This process continues indefinitely and the CRL data is never read during normal operation. The only 
time the log is read is during the initialization process to restore into memory the state needed to 
resume the transactions from where they left off.

Given the high-volume of data being sent to the CRL, most deployments will probably want to back the 
CRL files with NVMe or SSD media. Importantly, the data in the CRL is centralized and the state for 
transactions affecting all the DataStores on the host are commingled. Consequently, the in-flight 
transaction data is NOT stored on the StorageDevice hosting the DataStore. So, you can’t just pull a 
drive out of one host and plug it into another and expect everything to work just yet. Physically 
moving drives between hosts is definitely possible but it will require implementing a command that 
first shuts down all the DataStores on a drive and writes their active transaction state to their 
folder on the underlying StorageDevice. Shutting down DataStores in this manner is already 
implemented and is required for transferring DataStores between Hosts/StorageDevices. What remains is 
just the command to shut down all stores on a device and note that it is ready for transfer. Followed 
by a little bit of code in the Host to detect a transferred drive and properly load all the stores.
A good portion of this is already implemented but not quite all of it just yet.

Aspen is currently being co-developed with the distributed file system [AmoebaFS](AMOEBAFS.md) which is 
Aspen's first real-world use case. AmoebaFS and designed both validate and demonstrate most of 
Aspen's capabilities. It's still in the early stages of development but the core functionality is in 
place and it serves as a good demonstration of how to use Aspen's feature set.
