### Messaging & Serialization
- Google ProtocolBuffers is used for serializstion/deserialization of both network messages and arbitrary data 
  structures
- Currently, all encoding and decoding is done within the Codec class and a single codec.proto file
- A common pattern and naming scheme is used for all encode/decode functions
- The common.network.Message module defines all non-CnC messages
- Messages are sent between logical entities e.g. Clients and DataStores or DataStores to other DataStores.
- Low-level networking dealing with things like network endpoints, transport protocols, and the like are mostly 
  handled by the networking plugin.
