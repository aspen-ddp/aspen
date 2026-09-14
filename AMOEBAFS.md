# AmoebaFS

AmoebaFS is being co-developed alongside Aspen as its first real-world use case to
keep the design and implementation grounded in reality and to verify that it can be
effectively employed to build a practical solution.

The goal of AmoebaFS isn't to build a better Ceph or Gluster, rather it is intended
to provide a distributed system with maximal flexibility and the ability to adapt,
on-the-fly to a changing runtime environment. This is why it was named after the
mighty Amoeba.

The implementation of AmoebaFS takes maximum advantage of what Aspen is capable
of and serves to both test and demonstrate its capabilities.

Presently, AmoebaFS consists of a very simple and straight-forward implementation
of a traditional POSIX file system that is exported to the outside world via the
dCache NFS library. In the future though, it could be extended to support all
kinds of features. To name a few:

* UnionFS style system. Read-only root w/ CopyOnWrite to writable layers
* Customizing the AllocationGroups used for each directory. Which would enable:
* Geo-location of directory content
* Per-directory control over baking media (NVMe vs 5400rpm hdd)
* All writes to NVMe w/ background migration to slower media
* Track hot/cold data and migrate between storage media
* Geo-dispursed base FS with per-site cache for hot files
* Background compression / deduplication
* Btrfs-style COW implementation with snapshotting support
* Embed a type UUID in the Inode to support multiple File implementations
  * Could have implementation optimized for concurrent write access
  * Optimizations for append-only log files

Currently, AmoebaFS is very much in the experimental stage. The core
functionality is implemented but it has seen little practical testing. It
consists of about 3k lines of code and is a good example of how to use
Aspen. Please refer to the [Readme](README.md) and 
[Architecture](ARCHITECTURE.md) documents for additional information.
