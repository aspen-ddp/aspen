# Aspen
[Project Homepage](https://aspen-ddp.org)

## What it is
For the last few decades there have been two general solutions for managing distributed
data at scale, consistent hashing and sharding. Aspen breaks the status quo by
offering a third approach which is based on explicit data pointers. It requires a little 
more overhead in the data lookup process but offers an order of magnitude more flexibility
for both distributed application design and run time operation.

The goal of the Aspen project is to create a general-purpose platform for
building higher-level distributed applications like object stores, distributed file 
systems, distributed indices, databases, and other solutions, particularly those that are
not well suited to current distributed system architectures. It is not designed to "do the
same thing only better" or to replace any existing systems. Rather, the intent is to take a
completely different approach to the distributed data management problem and provide
developers with a new set of tools for taking distributed system design in directions that
were not previously possible.

## Why Aspen?
Aspen emphasizes flexibility for both application design and run time operation. It provides
distributed system designers with a completely different set of capabilities for building
solutions that simply aren't possible with the consistent hashing or sharding models. It's 
also designed to be a common component shared across a wide variety of applications. This 
is to spread out the maintenance burden and allow enhancements made in support of one 
application to benefit a community of others.

## How is it different?
As mentioned before, the key difference in Aspen, as compared to other distributed data 
architectures, is that it uses a model based on explicit object pointers for locating data
in the system rather than the conventional consistent hashing or sharding approaches. The
cost is a little more overhead in the data lookup process and bookkeeping but the benefit
is unparalleled flexibility in data configuration and placement options that may be
leveraged during application design. It also provides run time benefits for on-the-fly 
tuning of operational factors such as availability, reliability, latency, and cost, to 
name a few.

At its core, Aspen is an object store. At least insofar as all data managed by the system
is stored as discreet "objects" which are relatively small, typically on the order of
kilobytes to tens of megabytes. The following list outlines some of the characteristics of
the system. While most of these aspects are not unique to Aspen, the combination of them
results in an architecture significantly different from current mainstream distributed 
system designs.

- **Object allocation results in an Object Pointer** - These are relatively small,
typically 50 - 100 bytes in size and may be stored in other objects to form distributed
data structures like linked-lists, B-trees, graphs, etc.
- **The choice between replication or erasure coding and their factor is made at the
time of allocation** - This allows applications to mix-and-match various replication and
erasure coding schemes to best suit their needs.
- **All object updates are made via transactions** - Transactions may simultaneously
update single or multiple objects with Atomic, Consistent, and Durable guarantees.
- **Transactions require only one round trip in the contention and error free case**
- **Transactions may specify required post-commit actions** - These idempotent actions
are guaranteed to be completed after a transaction successfully commits and are useful
for short-duration cleanup and maintenance activities.
- **Provides a tasking model for durable, long-running operations** - Tasks leverage Aspen
objects to store task state and multi-object transactions to ensure the successful
completion of multi-phase, long-running tasks in the presence of node failure and system 
crashes, e.g, deleting a tree structure comprised of millions of individual objects.
- **Designed to take advantage of heterogeneous storage media** - Data stores may be placed
on storage media to optimize their intended use case. For example, the upper tiers of a
B-tree could be backed by NVME media for fast lookups while the bottom tier could be backed
by spinning disks to store bulk data.
- **Data stores and, consequently, their stored objects may be freely migrated between hosts
and backing media on-the-fly** - Data stores are logical entities that may be freely moved
about to satisfy changing needs of the operational environment.

A full description of how Aspen works and its design tradeoffs may be found in the 
Architecture section of the [Project Homepage](https://aspen-ddp.org)

# AmoebaFS
AmoebaFS is included with Aspen as a proof-of-concept application that demonstrates most
of Aspen's unique features. It leverage's dcache's Java NFS server library to export a
file system built on top of Aspen and currently supports most of the basic file system
operations. It's very much alpha quality at the moment but it does a decent job of
showcasing what Aspen is capable of and could eventually morph into something useful,
should others find it interesting enough to chip in on it's development.

## How to run the AmoebaFS NFS server demo

### Setup
1. Run `sbt compile`
2. Copy t.template to t
3. Run sbt without any options to enter its cli
4. Run `export runtime:fullClasspath` to generate the full classpath needed to run the compiled code
5. exit sbt
5. Update the 't' file with the generated CLASSPATH and set the JAVA_HOME directory to your installed JDK location

### Execution
Run the following command to generate and initialize 3 nodes. This will create and
populate a state folder in the `demo` directory for each node.
```
./t bootstrap demo/bootstrap_config.yaml
```

In three separate terminals, execute each of the node server processes:
```
./t node demo/bootstrap_config.yaml demo/node_a.yaml
./t node demo/bootstrap_config.yaml demo/node_b.yaml
./t node demo/bootstrap_config.yaml demo/node_c.yaml
```

In a fourth terminal, run the following command to launch the AmoebaFS NFS server process.
Note that on MacOS you need to first run `sudo launchctl start com.apple.rpcbind` to allow 
the NFS server to register with the local RPC daemon. On Linux, ensure you have the 
rpcbind service running.
```
./t nfs demo/bootstrap_config.yaml demo/log4j-conf.xml
```

To attach a client to the NFS server, run the following on the client machine:
```
umount -f /mnt; mount -v -t nfs4 -o "vers=4.1" <Server IP Address>:/ /mnt
```







