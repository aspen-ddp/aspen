### QUIC network plugin (Netty 4.2)
- Mutual TLS on every connection. Every message goes on a stream, which QUIC retransmits: a lost datagram waited for
  Aspen's own retries, and bursts overflow receive buffers even on loopback. Only 1-byte keepalives are datagrams.
- Messages of 1024 encoded bytes or less share one small-message stream (the pool when it is full); larger ones use a
  round-robin pool of `streamsPerConnection` (4). Messages are dropped (never queued) when over budget.
- UDP sockets request 16 MiB buffers; a smaller OS grant is logged. With epoll, packets go out in UDP GSO batches and
  come in through GRO and `recvmmsg` (`udpOffload`), and path MTU discovery, with don't-fragment set, raises them from
  1200 to `maxUdpPayloadBytes` (1452).
- Payloads are not copied on send and are copied exactly once on receive.
- Send buffering (`QuicConfig`): one host-wide `SendBudget` (a quarter of the heap, at least 256 MiB) shared by all
  connections; a connection may queue below its floor (256 KiB) always, or below its ceiling (a quarter of that
  budget, at least 64 MiB) while the shared budget has room. A stream whose writes wait over 500 ms is logged. The
  pre-handshake queue has its own bound (16 MiB) outside the shared budget
- Stale host endpoint repair: after `peerQueryDelay` (2 min) of failed dials to a host reported online, it asks
  `peerQueryFanout` (3) peers (clients ask their relays). Newer answers replace the entry.
- Envelopes carry the intended HostId. A host receiving one meant for another HostId drops it and replies `WrongHost`
  (rate-limited per connection). A handshake certificate UUID mismatch is treated the same way. Either marks the entry
  known-wrong, drops sends to it, and starts the peer query immediately.
- Large-message tests need the 4 GiB heap set in `.jvmopts`
