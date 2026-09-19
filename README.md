<p align="center">
  <picture>
    <!-- Displayed when GitHub user is in Dark Mode -->
    <source media="(prefers-color-scheme: dark)" srcset="assets/aspen-logo-dark.svg">
    <!-- Displayed when GitHub user is in Light Mode -->
    <source media="(prefers-color-scheme: light)" srcset="assets/aspen-logo-light.svg">
    <!-- Fallback for renders that do not support <picture> -->
    <img alt="Project Logo" src="assets/aspen-logo-dark.svg" width="450">
  </picture>
</p>

---

Aspen is a general-purpose distributed data platform for building higher-level distributed
systems. It provides a solid foundation suitable to a broad range of applications and it does
so in a unique way that emphasizes flexibility in both application design and runtime operation.

## Key Features

* **General-Purpose** - Designed to provide good performance across a broad range of
  applications rather than great performance in just one.
* **Unique Architecture** - Uses a new approach to managing data at scale that doesn't rely on consistent
  hashing or sharding.
* **Flexible** - Provides unparalleled flexibility to both distributed system architects and
  runtime operators
* **AmoebaFS** - Provides a distributed file system that demonstrates Aspen's capabilities

## Motivation

Aspen solves a problem you probably didn't realize we have and that's the lack of a
general-purpose distributed data platform for building various kinds of distributed systems. 
To date, if you want to build a distributed system, you essentially have two choices. You either
start from scratch or you use composition. 

In the first case, you build a custom-purpose distributed system from the ground up that solves all
the hard distributed data problems like replication/erasure-coding, consensus, transactions, 
version management, drive failure recovery. etc. The advantage is that you get a system ideally 
suited to your use case and that gives you the best possible performance but the up front cost is
enormous.

The alternative is composition where you build your system on top of an aggregation of existing 
systems. Say CockroachDB for the database, S3 for bulk data, Kafka for event streaming, and Redis
for caching. This is the far more popular option as it is usually faster and cheaper than staring
from the ground up. However, there are a couple drawbacks to this approach. The primary one being 
that these systems aren't designed to integrate with one another. You can't, for example, update 
an object in S3 and record the update in CockroachDB a single, atomic transaction. So if you need 
an atomic operation that spans components or otherwise need guarantees in behavior between the 
boundaries of components, you need to engineer a custom solution for each case and validate that 
it works across all failure modes of each component. For simple applications this usually isn't 
too much of an issue but as application complexity grows, these work-arounds can become a 
significant burden. Another challenge with the composition approach is operational support. Your 
ops team must fully understand the idiosyncrasies of each component, all of which are wildly 
different from one another, in order to plan, operate, and maintain each system individually 
as well as your application as a whole.  

Aspen aims to provide a third option for distributed systems architects. One that provides many 
of the benefits of the ground-up approach while simultaneously reducing some of the burden inherent
to composition. To do this, Aspen focuses on solving the distributed data management problem in a
new, general-purpose manner that emphasizes flexibility in terms of architectural design and 
runtime operation. 

## What it is

Aspen is a general-purpose distributed data management platform that is intended to serve as the
foundation for building a wide range of distributed applications. It solves the difficult distributed
data management problems like transactional updates, recovering from disk failure, and gracefully 
handling growing and shrinking hardware footprints in a way that maximizes flexibility.

Like all systems, the design choices underpinning Aspen make it better suited to some applications
than others. Rather than try to optimize for ideal performance in a narrow range of use cases, 
Aspen instead aims to provide "good" performance across a wide range of application domains. The
primary goal of Aspen is to provide distributed system architects with a new set of tools with
which to address problems.

In addition to opening the door for system designers to go off in new directions, Aspen can also 
be used to build the kinds of systems we already use today. You can use it to build a traditional
database, S3 storage system, distributed file system, message broker, event sourcing application, 
etc. Due to Aspen being designed for general-purpose use rather than tailored for optimal 
performance in a specific domain, these kinds of systems built on top of Aspen won't always match the
performance of their traditional counterparts. The obvious question then becomes "why bother"? 
The answer is threefold.

**First:** Not every deployment needs maximum performance. In fact, the vast majority do not and
"good enough" is usually just that.

**Second:** Operational simplicity. All applications built on top of Aspen share the same underlying
operational model. There would be little difference between deploying and maintaining a 
distributed file system built on Aspen than there would be an event sourcing system or database.

**Third:** (The main one) Systems built on top of Aspen are much more easily integrated as they
share the same operational, data, and transaction models. If you need an all-or-nothing atomic
operation that updates a database entry, deletes a file in a distributed file system, and adds
an event to a stream... you can do that. And you can do it naturally, no complex shenanigans
required.

In short, Aspen aims to fill a role in the distributed data world similar to the role scripting
languages serve in software development. Scripts can be used to quickly and effectively tackle
many problems that would be difficult to solve with a lower level language. There's a reason
there aren't any C/C++ web content platforms competing with Django. Python is just better
suited to solving that problem and its performance is more than adequate. Aspen is aiming at a
similar niche with distributed data arena.

## How it's different

Of course, Aspen isn't a silver bullet. At the end of the day, it's just another tool in the 
toolbox. So, the question is how is it different and what is it good for? To answer that, we
must first describe the basics of how it works.

At it's core, Aspen is an object storage system that runs on top of the JVM. All operations
are carried out through transactions that can update multiple objects simultaneously with 
ACD semantics from the standard ACID model (Isolation is achievable but deferred for the 
moment).

Object data is stored in logical DataStores that support pluggable backend implementations,
such as RocksDB and flat files. DataStores are not fixed in place and may be migrated 
between physical hosts and storage devices while the system is running. 

StoragePools aggregate a collection of DataStores and define the replication or 
erasure-coding strategy that is used for objects allocated within the pool. 

Objects are allocated from StoragePools and use the replication/erasure-coding strategy
defined for the pool they are allocated from. Each object has an ObjectRevision that can be
used for ensuring transaction consistency as well as a ReferenceCount. Objects are
deleted when a transaction sets their ReferenceCount to zero.

Much like C/C++ programs, you get a small, binary pointer when you allocate an object
in Aspen and you embed those pointers within other objects to create distributed data
structures.

Distributed Data Structures are the key concept that serves as the foundation for Aspen's goal of 
being a general-purpose distributed data platform. This is facilitated by the on-the-fly ability
to move DataStores to geographical locations and backing media to optimize the run-time deployment
strategy to the application's needs. The [Architecture](Architecture.md) document goes into much
greater detail but this is the core concept that defines Aspen's niche in the distributed system
arena. It isn't intended to do X better than ever before. It's intended to allow you to approach the
problem from a completely new direction Z, which was never an option before now.

Movable DataStores, StoragePools, Transactional Objects, ObjectPointers, and Distributed
Data Structures are the foundational concepts in Aspen. There's a lot more to the system, 
of course, but those are the core and a few things fall out of it that might not be
immediately obvious.

The first is that, because DataStores may be migrated between machines and physical
storage media, you can create distributed data structures that take advantage of 
heterogeneous storage media and geo-location. If you want a distributed B-Tree for fast 
lookups of bulk data, place the upper tiers of the tree on co-located machines with NVMe
media and the large data on geo-dispersed traditional spinning hard drives. If you have 
a distributed file system where content is organized by region, ensure that the 
/root/region/sacramento directory content and files are physically located there while 
the rest of your data is elsewhere. If the office relocates, rename the folder to 
/root/region/dallas and migrate the underlying DataStores to Texas. The degree of 
flexibility is unparalleled and it opens up options for application designs that aren't
practical in any other major system.

A second major advantage is that Aspen makes it easy to define crash-proof durable
tasks that provide exactly-once guarantees. To do this, we use a UUID that defines the
task type and a state object that contains the crash-proof state needed to carry out
the task. Each time a step in the task is completed, the state is updated to point to
the next step in the process. Should the host running the durable task crash, another
host can resume the task by using the UUID and state object. The resumed task will 
simply pick up where the crash happened and restart te last operation. When an 
exactly-once operation is needed, add the objects being modified to the transaction
that updates the task state to the next step. It's as simple as that. Aspen is a 
self-hosting system and makes use of this strategy to implement many of its internal
features.

For an example of how Aspen is used, the following is a snippet from the AmoebaFS 
implementation. This method atomically decrements an inode's reference count and creates a
durable task to delete the file content if the update to the reference count will set it
to zero:
```scala
def prepareUnlink()(using tx: Transaction): Future[Future[Unit]] = synchronized {
    val updatedInode = inode.update(links=Some(inode.links-1))

    tx.overwrite(pointer.pointer, cachedInodeRevision, updatedInode.toDataBuffer)

    tx.result.foreach(_ => setCachedInode(updatedInode, tx.revision))

    if (inode.links == 1)
      UnlinkFileTask.prepareTask(fs, pointer).map(f => f.map(_=>()))
    else
      Future.successful(Future.unit)
}
```

Another example is from safely deleting an empty node from a distributed linked list:
```scala
def deleteNode(ptr: KeyValueListPointer,
               nodeRevision: ObjectRevision,
               optr: Option[KeyValueListPointer]): Future[Unit] =
  client.read(ptr.pointer).flatMap { kvos =>

    if kvos.contents.nonEmpty then
      throw new Exception("Node is not empty")
      
    val tx = client.newTransaction()

    val op = optr match
      case None => DeleteRight()
      case Some(ptr) => SetRight(ptr.toArray)

    // Lock full content to ensure nothing is inserted while we're trying to
    // delete the node
    tx.update(ptr.pointer, Some(kvos.revision), Some(FullContentLock(List())), Nil, Nil)
    tx.setRefcount(ptr.pointer, kvos.refcount, kvos.refcount.decrement())

    // Update the right pointer of the start node to point to the next node
    // in the chain
    tx.update(nodePointer.pointer, Some(nodeRevision), None, Nil, List(op))
    tx.commit().map(_ => ())
  }.recover:
    case _: InvalidObject => () // node already deleted. Return success
```

# System Architecture
The [Architecture](Architecture.md) document contains full description of how Aspen works and its design 
tradeoffs. Unlike SQL databases which are more or less interchangeable black boxes you can
"just use", a solid understanding of how Aspen works is required to use it effectively. Its 
design and implementation are the foundational structure supporting everything built on top 
of it.

# Usage Guide
The [Usage Guide](UsageGuide.md) provides an overview of how do develop applications on top of Aspen.
It describes the most important classes and displays a few choice snippets from the AmoebaFS and Aspen
codebases that demonstrate the use of Aspen's API.

# Operating an Aspen Deployment
The [Operation](Operation.md) document describes how to bootstrap and operate an Aspen
system. At least, to some degree. Given the early-development stage Aspen is currently in, much
of the practical knowledge and experience to effectively operate the system is lacking. And
the tooling for doing so is still in its infancy. The situation should improve rapidly if a few
early adopters are willing to jump in and experiment.

# AmoebaFS

[AmoebaFS](Amoebafs.md) is a distributed file system built on top of Aspen and is 
currently being co-developed with it. AmoebaFS was created for a couple of reasons.

1. It provides a real-world use case for Aspen to help uncover and fix weaknesses in
   Aspen's design and implementation.
2. It's a strong use-case for Aspen's architecture. 
3. There are a wide variety of potential features that could be added to it.

See the [AmoebaFS](Amoebafs.md) document for more information.

## How to run the AmoebaFS NFS server demo

### Setup
1. Ensure jdk17+ is installed and JAVA_HOME is set correctly
2. Install sbt (the Scala Build Tool)
2. git submodule update --init --recursive # to pull in Backblaze's Reed-Solomon library
4. Run `sbt compile`

### Execution
Run the following command to create a new Aspen system under `/tmp/aspen-test`. The first
argument after the target directory is the address other hosts use to reach this one; the
remaining four are the bootstrap pool's IDA: type, read threshold, write threshold, and
width. This creates `/tmp/aspen-test/bootstrap-host`, containing the host's configuration,
its storage devices, and the three bootstrap data stores.
```
./aspen bootstrap /tmp/aspen-test 127.0.0.1 replication 2 3 3
```

This walkthrough runs everything on one machine, so `127.0.0.1` is the right address here
and the "not reachable from other machines" warning it prints is expected. A system that
will have hosts added to it later needs an address those hosts can route to — an external
IP or a DNS name — since the value given here is what they will dial.

The host directory is self-describing: bootstrap writes both `aspen-host-config.yaml` and
`aspen-bootstrap-config.yaml` into it, so starting the storage host needs nothing more than
the directory itself.
```
./aspen host /tmp/aspen-test/bootstrap-host
```

In a second terminal, create a filesystem. A freshly bootstrapped system has exactly one
storage pool, `aspen-bootstrap`, so that is what to allocate it in. Any number of
filesystems may coexist, each under its own name; `./aspen list-filesystems <config>` shows
them.
```
./aspen create-filesystem /tmp/aspen-test/bootstrap-host/aspen-bootstrap-config.yaml myfs aspen-bootstrap
```

Then launch the AmoebaFS NFS server process to allow clients to attach to the file system via the
normal NFS mounting process for the client machine. Note that on MacOS you need to
first run `sudo launchctl start com.apple.rpcbind` to allow the NFS server to register with
the local RPC daemon. On Linux, ensure you have the rpcbind service running.
```
./aspen amoebafs /tmp/aspen-test/bootstrap-host/aspen-bootstrap-config.yaml myfs
```

To use the AmoebaFS file system, just mount it like you would any other NFS filesystem. On Linux,
execute the following to mount it at /mnt:
```
umount -f /mnt; mount -v -t nfs4 -o "vers=4.1" <Server IP Address>:/ /mnt
```







