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
  - Contains a fixed IDA and the number of DataStores exactly matches the width of the IDA
  - Objects allocated within the pool have their data dispersed amongst the enclosing stores
- **Information Dispersal Algorithm (IDA)**:
  - Defines the replication/erasure-coding strategy used to provide robustness against data loss
  - Currently only Replication and ReedSolomon are supported
- **ObjectPointer**:
  - Allocations result in a binary-encoded ObjectPointer.
  - Intended to be embedded within other objects to form distributed data structures
  - This pointer must be supplied in order to retrieve object data
  - Contains the Object UUID, StoragePool, and optional additional content needed by stores to locate object data
- **Transactions**:
  - Provides the only means to modify object content
  - Provides Atomic, Consistent, and Durable guarantees
  - May update multiple objects at once
    - Any individual object can only be modified once. Multiple updates to the same object is not permitted.
  - At least one TransactionRequirement must be supplied
    - All supplied requirements must be satisfied for the transaction to commit
- **Finalization Actions**:
  - Specify required actions that must be completed before transaction state may be forgotten by the servers
  - Executed after a transaction successfully commits.
  - All FinalizationActions must complete before transaction state may be deleted
  - Can be used for arbitrary "fast" operations such as logging which DataStores failed to properly process a
    transaction or insering a newly allocated node into the next tier up in a distributed B-tree
  - Not suitable for long-lived operations as they "clog up" resources on the Servers
  - Should be idempotent operations as they may be executed multiple times
  - Provides At-Least-Once guarantees
- **Durable Tasks**:
  - Provides support for long-lived and/or "crash-proof", multi-step tasks.
    - The key is that each step in the task uses a multi-object transaction to both preform the task's logical
      step and to update the state of the durable task to note that the step has been successfully completed.
    - If a crash occurs, the durable task state is used to simply pickup where it left off. The 
      next as-yet-to-be-completed step in the task is simply restarted.
    - Example use: allocating an inode and inserting it into a directory in a distributed file system. Both 
      operations must complete successfully to avoid a corrupted file system but cannot be done in a single
      transaction. Using a DurableTask ensures that the operations proceed successfully and in the correct order.
  - Provides Exactly-Once guarantees
  
## Key Design Aspects
- Building and using Distributed Data Structures is the primary use case for Aspen-based systems
  - Linked lists, Tiered Key Value Lists (similar to B-Trees), and other data structures can be used to
    efficiently store and retrieve data in Aspen
  - Every Transaction that allocates an object should also store the resulting ObjectPointer in another object
    to prevent data loss in the event of a crash.
- Aspen is designed and implemented largely as a logical system
  - Messages are addressed to logical entities like DataStores rather than physical hosts
  - The last step prior to sending a message is to look up the address of the network endpoint currently
    hosting the logical entity. The message is then sent to that physical host which will in turn deliver
    the message to the logical entity
- Unlike most scalable distributed storage systems, Aspen achieves scale not through consistent hashing
  or sharding, but rather through binary ObjectPointers that encode the location of the object within the
  logical storage system.
- Because DataStores do not have a fixed location, they may be freely migrated between physical machines and 
  storage devices.
  - This allows for optimization of distributed data structures in various ways. For example, placing the upper
    tiers of a distributed B-Tree on low-latency NVMe storage and the bottom tier on HDD. Allows fast lookups
    of cheaply stored bulk data
- Aspen is a self-hosting system.
  - Transactions, object storage and retrieval, and DataStore implementation are foundational
  - Most other aspects of Aspen are built on top of distributed objects stored in the Aspen system. Such as:
    - Logging of object update errors
    - System Metadata such as the available StoragePools, DataStores, network endpoints, physical media, etc
  - Creating an Aspen Client requires an offline configuration of the bootstrap StoragePool and network endpoints
    - From this, the "Radicle Object", with a zeroed UUID, is loaded
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

### Command and Control (CnC)
- Mechanism to instruct a store to do something immediately
- Generally avoided as messages can be lost and/or hosts may be unavailable when the command is issued
- Prefer updating state in the system for a later polling by the target entity
- Largely deprecated and may be removed entirely

### Host and StorageDevice Management
- Most administration is done by reading and writing host and device objects containing the desired state
- Hosts will poll these instances and act on changes
  - This approach is preferred over direct Command and Control messaging since those messages could be lost
  - An example is seeing that a DataStore is marked for transfer in or out. When seen, the hoste will start the process

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
  - Application design and usage patterns should take this into account and reduce the potential
    for collisions whenever possible

## Building and Testing
- `sbt 'testOnly *TestName -- -z "test substring"'` - Run a specific test by substring

## Key Dependencies
- **Scaffeine**: Used for all caching operations
- **os-lib**: Should be used for all filesystem operations. (currently a mix of native java and os-lib)
- **jeromq**: Used for the ZeroMQ network plugin. Simple but inefficient

## Development Rules
- Prefer Scala 3 "quiet mode" syntax. Braces should still be used when they clearly enhance readability
- Prefer indented if/then/else syntax. Avoid single lines e.g. "if a == 1 then value" unless it enhances readability
- `cmdline` is a work-in-progress; treat its interfaces as unstable
