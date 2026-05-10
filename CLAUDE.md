## Project Overview

Aspen is a distributed data platform for building higher-level distributed applications, written in Scala 3. 
It implements a distributed object storage system that uses transactions to allocate, update, and delete
one or more objects at a time with Atomic, Consistent, and Durable transaction guarantees.

## Key Architectural Components
- **Objects**:
  - All data within Aspen is stored within objects
  - Identified by UUID
  - Contain a Revision & Refcount
    - The revision is the UUID of the last transaction to update the object
    - Objects are deleted when their refcount is set to 0
  - There are two types DataObjects and KeyValue objects
    - DataObjects contain binary data
    - KeyValue objects contain Key/Value pairs. Both Keys and Values are binary data.
- **DataStores**:
  - Stores replicas/slices of object data
  - Logical entities with no fixed physical location or on-disk format
  - Identified by numerical index within its enclosing StoragePool
- **StoragePools**:
  - Identified by UUID
  - Contains a fixed number of DataStores
  - Objects allocated within the pool has their data dispersed amongst the enclosing stores
- **Information Dispersal Algorithm (IDA)**:
  - Defines the replication/erasure-coding strategy used to provide robustness against data loss
  - Currently only Replication and ReedSolomon are supported
- **ObjectPointer**:
  - Allocations result in a binary-encoded ObjectPointer.
  - This pointer must be supplied in order to retrieve object data
  - Contains the StoragePool, IDA, and any additional content needed by stores to locate object data
- **Transactions**:
  - Provide the only means to modify object content
  - Provides Atomic, Consistent, and Durable guarantees
  - May update multiple objects at once
    - Any individual object can only be modified once. Multiple updates to the same object is not permitted.
  - At least one TransactionRequirement must be supplied
    - All supplied requirements must be satisfied for the transaction to commit
- **Finalization Actions**:
  - Specify required actions that must be completed before transaction state may be forgotten by the servers
  - Executed after a transaction successfully commits
  - Can be used for arbitrary "fast" operations such as logging which DataStores failed to properly process a
    transaction or insering a newly allocated node into the next tier up in a distributed B-tree
  - Not suitable for long-lived operations as they "clog up" resources on the Servers
- **Durable Tasks**:
  - Provides support for long-lived and/or "crash-proof", multi-step tasks.
    - The key is that each step in the task uses a multi-object transaction to both preform the task's logical
      step and to update the state of the durable task to note that the step has been successfully completed.
    - If a crash occurs, the durable task state is used to simply pickup where it left off. The 
      next as-yet-to-be-completed step in the task is simply restarted.
    - Example use: allocating an inode and inserting it into a directory in a distributed file system. Both 
      operations must complete successfully to avoid a corrupted file system but cannot be done in a single
      transaction. Using a DurableTask ensures that the operations proceed successfully and in the correct order.
  
## Key Design Aspects
- Building and using Distributed Data Structures is the primary use case for Aspen-based systems
  - Linked lists, Tiered Key Value Lists (similar to B-Trees), and other data structures can be used to
    efficiently store and retrieve data in Aspen
- Aspen is designed and implemented largely as a logical system
  - Messages are addressed to logical entities like DataStores rather than physical hosts
  - The last step prior to sending a message is to look up the address of the network endpoint currently
    hosting the logical entity. The message is then sent to that physical host which will in turn deliver
    the message to the logical entity
- Unlike most scalable distributed storage systems, Aspen acheives scale not through consistent hashing
  or sharding, but rather through binary ObjectPointers that encode the location of the object within the
  logical storage system.
- Because DataStores do not have a fixed location, they may be freely migrated between physical machines and 
  storage devices.
  - This allows for optimization of distributed data structures in varios ways. For example, placing the upper
    tiers of a distributed B-Tree on low-latency NVMe storage and the bottom tier on HDD. Allows fast lookups
    of cheaply stored bulk data
- Aspen is a self-hosting system.
  - Transactions, object storage and retrieval, and DataStore implementation is foundational
  - Most other aspects of Aspen are built on top of distributed objects stored in the Aspen system
    - Logging of object update errors
    - System Metadata such as the available StoragePools, DataStores, network endpoints, physical media, etc
  - Creating an Aspen Client requires an offline configuration of the bootstrap StoragePool and network endpoints
    - From this, the "Radicle Object" with a zeroed UUID is loaded
      - This object serves as the ultimate root for locating all data stored within an Aspen system
      - It contains embedded ObjectPointers to the roots of all critical distributed data structures used
        to store system metadata such as physical storage hosts, netowrk configurations, storage pool
        configurations, etc
- Aspen uses a Crash-Only architecture. By intentional design, there is no clean shutdown process.
- The Network Layer is designed to be pluggable. Currently only a simple but inefficient ZeroMQ based network
  is supported.
- DataStores are pluggable. Currently only a RocksDB and in-memory Map are supported.

## Subsystem Details

### Transaction Design
- TransactionRequirements are varied. They can specify simple things like required revision, refcount, key-value revision,
  key doesn't exist, etc. It is intended to be expanded over time and is implemented as a sealed abstract class to ensure
  that new additions will be properly handled in the codebase. 
- Paxos is used to ensure transaction consensus
  - Transactions are initiated by the client but the Paxos participants are the DataStores hosting only one
    of the targeted objects. This is done to reduce the number of participants and does not affect correctness.
  - The Paxos algorithm itself is unmodified but some clever message handling is done to reduce the number of
    round-trips and optimize data flow. The details are only important when working on transaction message
    handling.
  - The CRL is used to store the persistent state required by the Paxos protocol
-  Lifecycle:
  - The client builds a TransactionDescription and sends it plus object data to the stores
  - The stores check all local state against the TransactionRequirements and determine their Commit/Abort decision
  - The CrashRecoveryLog is used to store this decision and Paxos is then used to arrive at an overall commit/abort decision
  - The client and all stores are informed of the final decision
  - If committed, FinalizationActions are then run. Once complete, the CRL content is dropped and the transaction is forgotten.
  - If the refcount of the object is set to zero, it is deleted

### Client Internals
- Largely a collection of Managers and Drivers used to overcome message loss to and from DataStores.
  - Drivers track response state and re-issue messages to slow/offline stores until the individual operation concludes
  - Managers track the drivers and deliver messages to them as they arrive from the network
- There are three key Managers:
  - ReadManager - Ensures reads complete
  - AllocationManager - Ensures object allocations complete
  - TransactionManager - Ensures transactions complete
- Proactively assists with object reconstruction by sending OpportunisticRebuild messages when DataStores return stale
  object state.

### Messaging & Serialization
- Google ProtocolBuffers is used for serializstion/deserialization of both network messages and arbitrary data 
  structures
- Currently, all encoding and decoding is done within the Codec class and a single codec.proto file
- A common pattern and naming scheme is used for all encode/decode functions
- The common.network.Message module defines all non-CnC messages
- Messages are sent between logical entities e.g. Clients and DataStores or DataStores to other DataStores.
- Low-level networking dealing with things like network endpoints, transport protocols, and the like are mostly 
  handled by the networking plugin.
     
### Crash Recovery Log
- Similar to a write-ahead-log in traditional databases
- Stores the state necessary for Paxos as well as the TransactionDescription, commit/abort vote, and object data
- Only read at application startup to recover in-progress transactions
- Takes advantage of the fact that Transactions are typically very short-lived so their state usually does not need to
  be retained for long
- Implemented as a series of fixed-size, write-only files that are continually recycled.
- Functions similar to a circular buffer when the oldest "live" data is copied to the head of the buffer whenever it
  is about to overwrite that live data.

### Tiered Key Value List (TKVL)
- Very simple B-Tree like data structure implemented in terms of a hierarchy of linked lists.
- Trees use a KeyOrdering instance to sort keys. Current orderings include Integer, Lexical, and ByteArray
- Every time a new node is added to a list, an the minimum value of the new node and a pointer to the new object is
  inserted into the tier above it.
- Tier-0 holds the data, the upper tiers consist exclusively of (minimum_key, object-pointer) pairs
- The root of the tree is replaced every time the current root node is split into two nodes
- FinalizationActions are used to insert/remove entries in upper tiers as nodes are split/joined
- Left to right navigation in a tier is guaranteed to be consistent. Navigation between tiers is not

### Command and Control (CnC)
- Mechanism to instruct a store to do something immediately
- Generally avoided as messages can be lost and/or hosts may be unavailable when the command is issued
- Prefer updating state in the system for a later polling by the target entity
- Largely deprecated and may be removed entirely

### Host and StorageDevice Management
- Most administration is done by reading and writing host and device objects containing the desired state
- Hosts will poll these instances and act on changes
  - This approach is preferred over direct Command and Control messaging since those messages could be lost
  - An example is seeing that a DataStore is marked for transfer in or out. When seen, the host will start the process

### Hybrid Logical Clock (HLC) Timestamps
- Provides timestamps that are guaranteed to be later than some previously observed event(s)
- Similar to vector clocks but much simpler and in an NTP-compatible timestamp
- Used extensively in Aspen to facilitate reliable timestamp comparisons that are robust against many traditional
  pitfalls associated with comparing pure NTP-managed timestamps.


## System Usage
- **aspen.client.AspenClient**: 
  - Serves as the primary interface for applications using Aspen object storage
  - Supports object allocation, transaction building, and retrieving object data
- Transaction collisions are to be avoided by system designers
  - There is a built-in collision handling mechanism but it is inefficient for heavy collisions
  - Application design and usage patterns should take this into account and avoid the potential
    for collisions whenever possible

## Code Organization
- **Client** (`org.aspen_ddp.aspen.client`): AspenClient, object readers, allocators, transactions
- **Server** (`org.aspen_ddp.aspen.server`): Store management, transaction processing, CRL (Crash Recovery Log)
- **Common** (`org.aspen_ddp.aspen.common`): Shared data structures, network protocols, IDA (Information Dispersal Algorithms)
- **Compute** (`org.aspen_ddp.aspen.compute`): Implements a durable, crash-and-recover tasking model on top of Aspen objects for long-lived and/or multi-step tasks.
- **AmoebaFS** (`org.aspen_ddp.aspen.amoebafs`): NFS server that exports a distributed filesystem implemented on top of an Aspen client.
- **Demo** (`org.aspen_ddp.aspen.demo`): Command line utilities for launching demo processes with ZeroMQ based networking. Very much a work-in-progress.

## Building and Testing
- `sbt compile` - Compile the project
- `sbt test` - Run all tests
- `sbt 'testOnly *TestName -- -z "test substring"'` - Run a specific test by substring

## Testing and Configuration
- ScalaTest framework with custom timeouts (`Tests.Argument(TestFrameworks.ScalaTest, "-W", "10", "5")`)
- YAML-based configuration for bootstrap, nodes, and logging
- Log4j2 with async logging configuration

## Key Dependencies
- **Scaffeine**: Used for all caching operations
- **nfs4j-core**: Provides the NFSv4 interface to export the AmoebaFS distributed file system
- **os-lib**: Should be used for all filesystem operations. (currently a mix of native java and os-lib)
- **jeromq**: Used for the ZeroMQ network plugin. Simple but inefficient
- **SnakeYAML**: Used for reading configuration files
- **rocksdbjni**: Used for the RocksDB DataStore implementation
- **slf4j-log4j12**: Provides the interface for logging

## Development Rules
- Prefer Scala 3 "quiet mode" syntax. Braces should still be used when they clearly enhance readability
- Prefer indented if/then/else syntax. Avoid single lines e.g. "if a == 1 then value" unless it enhances readability
