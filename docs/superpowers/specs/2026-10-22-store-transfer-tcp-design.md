# Store Transfer over TLS/TCP — Design

Date: 2026-10-22

## Intent

Moving a DataStore between hosts currently ships the store's bytes as a series of
`StoreTransferData` host messages over the pluggable network layer. With the QUIC plugin as the
default, that is the wrong transport: QUIC messages are dropped (never queued) when a
connection's buffers fill, there is no ordering guarantee between messages, and there is no
backpressure. A dropped or reordered chunk silently corrupts the `jar` stream on the receiving
side. Store transfer needs guaranteed, in-order delivery with backpressure — exactly what a
plain blocking TCP stream provides.

This design replaces the data path with a dedicated, mutual-TLS-protected, blocking TCP
connection per transfer attempt. The `jar --create` / `jar --extract` subprocess foundation is
kept, as is the metadata side of the transfer (pool repointing, transfer-out marker,
abandoned-transfer handling).

## Goals

- Store bytes travel over a single TLS/TCP stream per attempt; TCP provides ordering and
  backpressure.
- The receiver can distinguish a complete stream from a truncated one.
- Stale connections from superseded attempts are detected and rejected.
- A dead peer (power loss, partition) never hangs a transfer forever.
- Any failure on the receiving side causes a clean restart of the transfer.

## Non-goals

- Resumable transfers. A failed attempt restarts from zero.
- Changing the archive format (`jar`) or the metadata/state-machine side of transfers.
- Per-host identity checks on the TLS peer beyond "signed by this system's CA".
- Changing the QUIC or ZeroMQ plugins beyond removing `StoreTransferData`.

## Anti-goals

- Any path where a truncated or mixed stream from two attempts is extracted and then loaded as
  a store.
- Routing store bytes through the message-oriented network layer.

## Current state (for orientation)

- `server/transfer/StoreTransfer.scala` — `StoreTransferIn`/`StoreTransferOut` traits and the
  `StoreTransferFactory` test seam.
- `server/transfer/TransferringOut.scala` — writes the `transferring-out` marker file into the
  store directory, spawns `jar --create` in it, reads stdout in 1 MiB pieces and sends each as
  `StoreTransferData`; an empty message signals end of stream.
- `server/transfer/TransferringIn.scala` — extracts into
  `<devicePath>/transferring-in/<storeId.directoryName>` via `jar --extract`, runs a 30 s
  no-data watchdog, and on completion removes the marker file and moves the directory to
  `<devicePath>/<storeId.directoryName>`.
- `server/Host.scala` — `startStoreTransferIn` (creates the in-side, sends `StartStoreTransfer`,
  retransmits every 30 s via `pendingStartTransfers`, restarts on failure, and on success calls
  `updateStateForTransferredStore` then loads or discards the store), `startStoreTransferOut`
  (validates pool/device state, closes the store, creates the out-side, supersedes older
  attempts by timestamp), and `transferDataReceived`.
- `StartStoreTransfer` (`common/network/Message.scala`) already carries `storeId`, `timestamp`
  (HLC) and `transferUUID`.
- `StoreId` encodes to exactly 17 bytes (16-byte pool UUID + 1-byte index).
- `HostState` carries `address` and `storeTransferPort`; `HostConfig` has
  `store-transfer-port` and `tls-certificate`; `common/network/tls/TLSContext.create(ca, cert,
  key)` builds an `SSLContext` whose trust manager accepts any chain signed by the system CA.

## Decisions

### Message

- Keep the name `StartStoreTransfer`. Its existing `transferUUID` field is the per-attempt
  random UUID; no format change is needed.
- Remove `StoreTransferData` entirely: the case class, `Codec` encode/decode, the protobuf
  definition, both plugins' codec cases (`QuicMessageCodec`, `ProtobufMessageCodec`), and
  `Host.transferDataReceived` / `StoreTransferIn.dataReceived`.

### Wire protocol (one TLS connection per attempt)

1. Sender connects with TLS to the destination host's `address:storeTransferPort`.
2. Sender writes a 33-byte header: 17-byte encoded `StoreId`, then 16-byte `TransferUUID`
   (most-significant long, then least-significant long, big-endian).
3. Sender streams `jar --create` stdout, reading and writing in `CHUNK_SIZE` pieces
   (default 1 MiB).
4. At end of the jar stream, sender writes the 17-byte `StoreId` as a trailer, then closes the
   socket.

The trailer is only the StoreId: the header has already bound the connection to the attempt, so
the trailer only needs to mark a clean end of stream.

### `StoreTransferManager`

New class in `server/transfer`. Constructor takes the port to listen on and the TLS material
used for mutual TLS (an `SSLContext` built with `TLSContext.create` from the host's CA,
certificate and private key — the same material the QUIC plugin uses). The server socket
requires client authentication.

`beginTransferIn(storeId, transferUUID, destinationDirectory): Future[Unit]`
- Registers `(storeId, transferUUID) -> destinationDirectory + promise`. Re-registering a
  storeId replaces the previous registration (the previous attempt's future is failed if still
  pending).
- `Host` creates the UUID and passes it in (it also needs it for `StartStoreTransfer`).

`beginTransferOut(storeId, transferUUID, sourceDirectory, destHost, destPort, timestamp): Unit`
- If a transfer-out for this storeId is active:
  - same timestamp → ignore the call;
  - newer timestamp → kill the current attempt (destroy its `jar` process, close its socket)
    and start a new one;
  - older timestamp → ignore.
- Otherwise spawn a TransferOut thread.

Accept loop: a dedicated thread accepts connections. Each accepted connection is handed to a
TransferIn thread, which first reads the 33-byte header (under the same 90 s read timeout). If
`(storeId, transferUUID)` does not match a current registration, log it and close the
connection — this is how stale attempts are rejected. A matched registration is consumed so a
second connection with the same pair is also rejected.

### TransferOut thread

- Connect, write header, spawn `jar --create` in the source directory, loop
  read-chunk/write-chunk, write trailer, close, terminate.
- Send timeout: 90 s. Java sockets have no write timeout, so a watchdog closes the socket if no
  write has completed for 90 s, unblocking the stuck write.
- On any failure: log, destroy the `jar` process, close the socket, drop the tracking entry.
  The sender does not retry on its own; the receiver's retransmitted `StartStoreTransfer`
  drives the next attempt.
- The `transferring-out` marker file continues to be written into the source store directory
  before archiving, as today.

### TransferIn thread

- Recursively delete `<destinationDirectory>/<storeId.directoryName>` if it exists, create it,
  spawn `jar --extract` there.
- Read from the socket with a 90 s read timeout (`setSoTimeout`). Always hold back the most
  recent 17 bytes; forward everything before them to the extractor's stdin.
- On end of stream: if the held-back 17 bytes equal the StoreId, close the extractor's stdin and
  wait for it to exit. Exit code 0 → success. Anything else → failure.
- Failure cases: header mismatch is handled by the accept path (above); read timeout; trailer
  mismatch or stream shorter than the trailer; socket error; extractor non-zero exit or early
  death. On failure: destroy the extractor, delete the partial directory, fail the future.
- Success: complete the future. Moving the directory into its final place stays with `Host`
  (see below).

### Host integration

- `Host` owns one `StoreTransferManager`, listening on its configured `store-transfer-port`
  with its own TLS material.
- `destinationDirectory` passed to `beginTransferIn` is `<devicePath>/transferring-in`, so
  extraction still happens in the staging area.
- Transfer-in (`startStoreTransferIn`): if not already transferring in, capture a fresh HLC
  timestamp and a fresh random UUID, call `beginTransferIn`, send `StartStoreTransfer` to the
  host holding the store, and retransmit periodically until the future completes.
  - Success: stop retransmitting, remove the `transferring-out` marker from the extracted copy
    and move it to `<devicePath>/<storeId.directoryName>` (as `TransferringIn` does today), then
    `updateStateForTransferredStore` and load or discard exactly as today.
  - Failure: stop retransmitting and restart with a new timestamp and new UUID.
  - The retransmit stops on future completion, not on first data received (today's
    `transferDataReceived` behaviour goes away).
- Transfer-out (`startStoreTransferOut`): validation, store closing and timestamp supersession
  logic stay. Instead of `createTransferOut`, look up the destination host's `HostState` for
  `address` and `storeTransferPort` and call `beginTransferOut`.
  - Optimization: if the store named in the message is not yet closed and marked for transfer
    out locally, trigger an immediate scan of network state before handling the message. This
    covers the common case where the receiver notices the transfer before the sender does.
    Correctness does not depend on it; retransmits cover it.
- The existing `StoreTransferIn`/`StoreTransferOut` objects with their own 30 s no-data timer
  are replaced; the timeout now lives on the socket.

### Testing

- Replace the `StoreTransferFactory` seam with an equivalent seam around the transfer manager
  (e.g. a trait `Host` depends on, with an in-memory test implementation) so existing `Host`
  tests keep working without sockets or subprocesses.
- `StoreTransferManager` gets its own suite over real loopback TLS (certificates from
  `CertificateAuthority`): successful round-trip of a directory tree; truncated stream (no or
  wrong trailer) fails and cleans up; stale UUID rejected while the correct one succeeds;
  unknown StoreId rejected; read-timeout failure (timeout made injectable so tests don't wait
  90 s); superseding transfer-out by newer timestamp; duplicate timestamp ignored.

### Clarifications from review

Items marked *(call)* are the designer's judgement calls, not decisions made with the user.

- **Retransmits after an attempt finished** *(call)*. The manager remembers the most recent
  timestamp it has acted on for each storeId, even after that attempt's thread has finished.
  A `beginTransferOut` whose timestamp is equal or older is ignored. A retransmit of the
  same attempt therefore never starts a second `jar --create` that would just be rejected.
  When an attempt fails, the receiver restarts with a newer timestamp, and that newer
  timestamp is what starts the next send.
- **Sender completeness** *(call)*. After stdout reaches EOF, the sender waits for
  `jar --create` to exit and writes the trailer only if the exit code is 0. Otherwise it
  closes the socket without a trailer, and the receiver fails the attempt. An archive that
  is truncated or failed can therefore never carry a valid trailer. The trailer, together
  with the extractor's exit code, is the integrity check. No length or checksum is added.
- **Source-side lifecycle is unchanged.** Pool repointing in
  `updateStateForTransferredStore` and the existing marker and offline handling decide what
  happens to the source copy, as they do today. `StoreTransferOut` completion only ever
  dropped the tracking entry. `Host` uses `transferringOut` in several places as "this
  process is shipping this store": the abandoned-transfer-out guard (around
  `Host.scala:1136`) and the store listings around `Host.scala:1470-1478`. The manager (or
  the test seam) must expose that query, for example `isTransferringOut(storeId)`, so those
  call sites keep their meaning.
- **TLS material.** `cmdline/Main.scala` already builds a `TlsCredentials` (CA certificate,
  host certificate, private key; `common/network/implementations/quic/QuicTls.scala`) for
  the network plugin. The same credentials, or the manager built from them, are passed into
  `Host`. Both the server and client sockets require the peer chain to be signed by the CA,
  using the `AspenTrustManager` semantics. Endpoint or hostname identification is not
  enabled, because peers are reached by address.
- **Destination lookup failures** *(call)*. The destination's `address` and
  `storeTransferPort` are resolved from a fresh `HostState` read each time `Host` handles
  `StartStoreTransfer`. If the read fails, or the connection fails, the failure is logged
  at warn, the attempt is dropped, and the next retransmit tries again. `beginTransferOut`
  never fails synchronously.
- **Unprocessable `StartStoreTransfer`** *(call)*. If the store is still not marked for
  transfer-out after the immediate scan, the message is logged at info and dropped, the
  same as today's "Transfer probably completed" paths. The retransmit covers it.

## Left to the builder

- Exact shape of the test seam and where timeouts/chunk size are parameterized.
- Thread naming, daemon status, and how the accept loop is started.
- Whether `Host` triggers the "immediate scan" through the existing device-check event or a new
  one.
- Cleanup of now-unused code in `StoreTransfer.scala` and related tests.
