### Messaging & Serialization
- Google ProtocolBuffers is used for serializstion/deserialization of both network messages and arbitrary data 
  structures
- Currently, all encoding and decoding is done within the Codec class and a single codec.proto file
- A common pattern and naming scheme is used for all encode/decode functions
- The common.network.Message module defines all network messages
- Messages are sent between logical entities e.g. Clients and DataStores or DataStores to other DataStores.
- Low-level networking dealing with things like network endpoints, transport protocols, and the like are mostly 
  handled by the networking plugin.
- The Network Layer is pluggable via `common.network.NetworkPlugin`. QUIC is the only plugin
  (`implementations/quic/CLAUDE.md`)

### Network cache and snapshots
- A pluggable `NetworkCache` persists store->host and host->endpoint mappings, scoped to the system id.
  - `NoCache` for the CLI; RocksDB for hosts (`<host-root>/cache/network`) and `amoebafs --network-cache-dir`
  - MetadataManager seeds from it at startup, trusts entries immediately, and writes every change back
- `NetworkCachingService` (`snapshot/`) writes a daily snapshot of every host endpoint and store->host mapping.
  `NetworkSnapshotReader` applies it at startup when the cache was empty (always for `NoCache`) and otherwise at a
  random slot each day. Newer entries replace held ones; entries absent from it and older than its scan start by more
  than a 10 minute drop margin are dropped.
