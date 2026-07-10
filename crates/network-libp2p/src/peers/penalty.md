# Peer Penalty System — Invariants Reference

This document is the single source of truth for the telcoin-network peer penalty system.
Every penalty severity, score-model invariant, and every site that applies a penalty is listed here with a source citation.
Treat each entry as an assertion to verify against the code.

## 1. Penalty Severity Levels

`Penalty` is defined in `crates/network-libp2p/src/peers/types.rs:69-86`.
Score deltas are applied in `crates/network-libp2p/src/peers/score.rs:84-100`.

| Variant           | Score Delta                          | Effect                                                             | Source                                        |
| ----------------- | ------------------------------------ | ------------------------------------------------------------------ | --------------------------------------------- |
| `Penalty::Mild`   | `-1.0`                               | Clamped to `[min_score, max_score]`                                | `crates/network-libp2p/src/peers/score.rs:90` |
| `Penalty::Medium` | `-5.0`                               | Clamped to `[min_score, max_score]`                                | `crates/network-libp2p/src/peers/score.rs:91` |
| `Penalty::Severe` | `-10.0`                              | Clamped to `[min_score, max_score]`                                | `crates/network-libp2p/src/peers/score.rs:92` |
| `Penalty::Fatal`  | sets score to `min_score` (`-100.0`) | Immediate ban — score jumps below `min_score_before_ban` (`-50.0`) | `crates/network-libp2p/src/peers/score.rs:93` |

## 2. Score Model Invariants

- Score range: `[min_score, max_score]` = `[-100.0, 100.0]`. `crates/config/src/network.rs:416-417`.
- Default starting score: `0.0`. `crates/config/src/network.rs:415`.
- Ban threshold (`min_score_before_ban`): `-50.0`. `crates/config/src/network.rs:428`.
- Disconnect threshold (`min_score_before_disconnect`): `-20.0`. `crates/config/src/network.rs:427`.
- Score halflife: `300.0` seconds; decay factor is `e^(-ln(2)/halflife * dt)`. `crates/config/src/network.rs:421`, `crates/network-libp2p/src/peers/score.rs:130-132`.
- `banned_before_decay_secs`: `30 * 60` (30 min). When a peer crosses the ban threshold, `last_updated` is pushed forward by this duration so the score does not decay during the lockout. `crates/config/src/network.rs:426`, `crates/network-libp2p/src/peers/score.rs:149-154`.
- Exempt peers skip penalty application entirely — `Peer::apply_penalty` short-circuits when the caller passes a `TrustBasis` (operator allowlist or committee validator); validator status is derived from the three tracked committee slots (previous/current/next) in `AllPeers::trust_basis`, not stored on the peer. `crates/network-libp2p/src/peers/peer.rs`, `crates/network-libp2p/src/peers/all_peers.rs`.
- Penalty application has no debouncing: every `process_penalty` call evaluates reputation immediately and may produce a ban on the same call. `crates/network-libp2p/src/peers/all_peers.rs:111-147`.
- Bans surface to the rest of the swarm as `PeerEvent::Banned`, pushed by `process_ban`. `crates/network-libp2p/src/peers/manager.rs:389-401`.
- `Penalty::Fatal` always crosses the ban threshold on the first call because `min_score` (`-100`) is less than `min_score_before_ban` (`-50`). `crates/network-libp2p/src/peers/score.rs:93`, `crates/config/src/network.rs:417,428`.
- Only the `Banned`/`Disconnected`/`Trusted` reputation transitions trigger a `PeerAction`. If the new reputation equals the prior reputation, `process_penalty` returns `PeerAction::NoAction`. `crates/network-libp2p/src/peers/all_peers.rs:117-119`.

## 3. Penalty Application Sites — Network Layer

All sites live in `crates/network-libp2p/src/consensus.rs`. `NetworkCommand::ReportPenalty` (L700-707) is the external entry from the app layer; everything below is internal.

### 3.1 Gossip events (`process_gossip_event`, L781)

| Location                                     | Trigger (immediate guard)                                                     | Severity |
| -------------------------------------------- | ----------------------------------------------------------------------------- | -------- |
| `crates/network-libp2p/src/consensus.rs:940` | `verify_gossip` rejected `TooLarge` (oversized payload) **and** the relaying peer's BLS has resolved (`RejectPenalty::FatalRelayer`) — the size bound is deterministic network-wide, so under `Strict` validation a peer that forwards an oversized payload is itself misbehaving | `Fatal`  |
| `crates/network-libp2p/src/consensus.rs:942` | `verify_gossip` rejected `UnauthorizedAuthor` (author absent / unresolved / unauthorized), or `TooLarge` from an unresolved relayer (`RejectPenalty::Skip`) — author-attributable or unattributable, so the forwarder is not penalized (issues #801/#819) | `None`   |
| `crates/network-libp2p/src/consensus.rs:839` | `GossipEvent::GossipsubNotSupported { peer_id }`                              | `Fatal`  |
| `crates/network-libp2p/src/consensus.rs:843` | `GossipEvent::SlowPeer { peer_id, failed_messages }`                          | `Mild`   |

### 3.2 Req/Res events (`process_reqres_event`, L851)

| Location                                     | Trigger                                                                                            | Severity |
| -------------------------------------------- | -------------------------------------------------------------------------------------------------- | -------- |
| `crates/network-libp2p/src/consensus.rs:943` | `OutboundFailure::DialFailure` / `OutboundFailure::ConnectionClosed`                               | `None`   |
| `crates/network-libp2p/src/consensus.rs:946` | `OutboundFailure::Io(e)` with transport-flap `e.kind()` (Reset/Aborted/Timed/EOF/Pipe/Interrupted) | `None`   |
| `crates/network-libp2p/src/consensus.rs:946` | `OutboundFailure::Io(e)` other kinds (codec violation, e.g. `io::Error::other`)                    | `Medium` |
| `crates/network-libp2p/src/consensus.rs:961` | `OutboundFailure::Timeout`                                                                         | `Mild`   |
| `crates/network-libp2p/src/consensus.rs:1071` | `OutboundFailure::UnsupportedProtocols` (honest version/role skew — warn only) | `None` |
| `crates/network-libp2p/src/consensus.rs:973` | `InboundFailure::Io(e)` with transport-flap `e.kind()`                                             | `None`   |
| `crates/network-libp2p/src/consensus.rs:973` | `InboundFailure::Io(e)` other kinds (codec violation)                                              | `Medium` |
| `crates/network-libp2p/src/consensus.rs:1112` | `InboundFailure::UnsupportedProtocols` (honest version/role skew — warn only) | `None` |
| `crates/network-libp2p/src/consensus.rs:995` | `InboundFailure::Timeout` / `InboundFailure::ConnectionClosed`                                     | `None`   |

`InboundFailure::ResponseOmission` is explicitly a no-op (local error). See restraint invariants below.

### 3.3 Kademlia outbound `GetRecord` query (`process_kad_event`, L1208)

| Location                                      | Trigger                                                                                                         | Severity |
| --------------------------------------------- | --------------------------------------------------------------------------------------------------------------- | -------- |
| `crates/network-libp2p/src/consensus.rs:1267` | `kad::QueryResult::GetRecord(Ok(FoundRecord))` but `peer_record_valid` returned `None` (bad sig / key mismatch) | `Fatal`  |

### 3.4 Kad put-record handler (`process_kad_put_request`, L1362)

| Location                                      | Trigger                                                                               | Severity |
| --------------------------------------------- | ------------------------------------------------------------------------------------- | -------- |
| `crates/network-libp2p/src/consensus.rs:1383` | record was rejected AND `record.publisher.is_none()` (publisher-less record)          | `Fatal`  |
| `crates/network-libp2p/src/consensus.rs:1406` | `peer_record_valid(&record).is_some()` but `!is_newer_record(&record)` (stale record — trace log only; kad routinely replays put-records to refresh TTLs) | `None`   |
| `crates/network-libp2p/src/consensus.rs:1413` | `peer_record_valid(&record).is_none()` (invalid signature / wrong network key)        | `Fatal`  |

A rejected record from a banned source/publisher with a publisher present does
NOT incur an extra penalty here — only the missing-publisher case is fatal. The
ban itself was applied elsewhere.

### 3.5 Kad query-result post-processing (`process_kad_query_result`, L1450)

| Location                                      | Trigger                                                                       | Severity |
| --------------------------------------------- | ----------------------------------------------------------------------------- | -------- |
| `crates/network-libp2p/src/consensus.rs:1479` | record valid but `query.request != key` (returned key does not match request) | `Fatal`  |
| `crates/network-libp2p/src/consensus.rs:1488` | `peer_record_valid(&record).is_none()` on the late-step path                  | `Fatal`  |

## 4. Penalty Application Sites — Worker Layer

### 4.1 `crates/consensus/worker/src/network/mod.rs` call sites

| Location                                         | Handler                                                | Source of penalty                               |
| ------------------------------------------------ | ------------------------------------------------------ | ----------------------------------------------- |
| `crates/consensus/worker/src/network/mod.rs:246` | `process_report_batch`                                 | `WorkerNetworkError::into() -> Option<Penalty>` |
| `crates/consensus/worker/src/network/mod.rs:271` | `process_gossip`                                       | `WorkerNetworkError::penalty()`                 |
| `crates/consensus/worker/src/network/mod.rs:379` | `process_request_batches_stream` response-error branch | `WorkerNetworkError::into() -> Option<Penalty>` |
| `crates/consensus/worker/src/network/mod.rs:435` | `process_inbound_stream`                               | `WorkerNetworkError::penalty()`                 |

### 4.2 `WorkerNetworkError::penalty()` mapping

Source: `crates/consensus/worker/src/network/error.rs:76-138`.

| Error variant                                                                       | Severity | Source                                                 |
| ----------------------------------------------------------------------------------- | -------- | ------------------------------------------------------ |
| `BatchValidation(CanonicalChain { .. })`                                            | `Mild`   | `crates/consensus/worker/src/network/error.rs:86`      |
| `BatchValidation(InvalidEpoch { .. })`                                              | `Medium` | `crates/consensus/worker/src/network/error.rs:88-89`   |
| `BatchValidation(InvalidTx4844(_))`                                                 | `Medium` | `crates/consensus/worker/src/network/error.rs:88-89`   |
| `BatchValidation(RecoverTransaction(..))`                                           | `Severe` | `crates/consensus/worker/src/network/error.rs:91`      |
| `BatchValidation(EmptyBatch)`                                                       | `Fatal`  | `crates/consensus/worker/src/network/error.rs:93-101`  |
| `BatchValidation(InvalidBaseFee { .. })`                                            | `Fatal`  | `crates/consensus/worker/src/network/error.rs:93-101`  |
| `BatchValidation(InvalidWorkerId { .. })`                                           | `Fatal`  | `crates/consensus/worker/src/network/error.rs:93-101`  |
| `BatchValidation(InvalidDigest)`                                                    | `Fatal`  | `crates/consensus/worker/src/network/error.rs:93-101`  |
| `BatchValidation(GasOverflow)`                                                      | `Fatal`  | `crates/consensus/worker/src/network/error.rs:93-101`  |
| `BatchValidation(CalculateMaxPossibleGas)`                                          | `Fatal`  | `crates/consensus/worker/src/network/error.rs:93-101`  |
| `BatchValidation(HeaderMaxGasExceedsGasLimit { .. })`                               | `Fatal`  | `crates/consensus/worker/src/network/error.rs:93-101`  |
| `BatchValidation(HeaderTransactionBytesExceedsMax(_))`                              | `Fatal`  | `crates/consensus/worker/src/network/error.rs:93-101`  |
| `InvalidRequest(_)`                                                                 | `Mild`   | `crates/consensus/worker/src/network/error.rs:105-106` |
| `UnknownStreamRequest(_)`                                                           | `Mild`   | `crates/consensus/worker/src/network/error.rs:105-106` |
| `StdIo(io_err)` kind `ConnectionReset`/`ConnectionAborted`/`TimedOut`/`Interrupted` | `Mild`   | `crates/consensus/worker/src/network/error.rs:108-114` |
| `StdIo(io_err)` other kinds                                                         | `Medium` | `crates/consensus/worker/src/network/error.rs:115`     |
| `NonCommitteeBatch`                                                                 | `Medium` | `crates/consensus/worker/src/network/error.rs:119`     |
| `InvalidTopic`                                                                      | `Fatal`  | `crates/consensus/worker/src/network/error.rs:121-126` |
| `Bcs(_)`                                                                            | `Fatal`  | `crates/consensus/worker/src/network/error.rs:121-126` |
| `TooManyBatches { .. }`                                                             | `Fatal`  | `crates/consensus/worker/src/network/error.rs:121-126` |
| `UnexpectedBatch(_)`                                                                | `Fatal`  | `crates/consensus/worker/src/network/error.rs:121-126` |
| `DuplicateBatch(_)`                                                                 | `Fatal`  | `crates/consensus/worker/src/network/error.rs:121-126` |
| `RequestHashMismatch`                                                               | `Fatal`  | `crates/consensus/worker/src/network/error.rs:121-126` |
| `Timeout(_)`                                                                        | None     | `crates/consensus/worker/src/network/error.rs:128-135` |
| `DBInsert(_)` / `DBCommit(_)` / `DBRead(_)`                                         | None     | `crates/consensus/worker/src/network/error.rs:128-135` |
| `StreamClosed`                                                                      | None     | `crates/consensus/worker/src/network/error.rs:128-135` |
| `Network(_)`                                                                        | None     | `crates/consensus/worker/src/network/error.rs:128-135` |
| `BatchEpochMismatch(_, _)`                                                          | None     | `crates/consensus/worker/src/network/error.rs:128-135` |
| `Internal(_)`                                                                       | None     | `crates/consensus/worker/src/network/error.rs:128-135` |

## 5. Penalty Application Sites — Primary Layer

### 5.1 `crates/consensus/primary/src/network/mod.rs` call sites

| Location                                          | Handler                                    | Source of penalty                                  |
| ------------------------------------------------- | ------------------------------------------ | -------------------------------------------------- |
| `crates/consensus/primary/src/network/mod.rs:421` | `stream_import` (epoch pack download)      | `Self::consensus_chain_error_to_penalty(&err)`     |
| `crates/consensus/primary/src/network/mod.rs:682` | `retrieve_missing_certs`                   | `(&PrimaryNetworkError).into() -> Option<Penalty>` |
| `crates/consensus/primary/src/network/mod.rs:719` | `retrieve_consensus_header`                | `(&PrimaryNetworkError).into()` — only reachable variant `UnknownConsensusHeaderDigest` → `None` |
| `crates/consensus/primary/src/network/mod.rs:751` | `retrieve_epoch_record`                    | `(&PrimaryNetworkError).into()`                    |
| `crates/consensus/primary/src/network/mod.rs:835` | `process_epoch_stream` response-err branch | `(&PrimaryNetworkError).into()`                    |
| `crates/consensus/primary/src/network/mod.rs:863` | `process_gossip`                           | `(&PrimaryNetworkError).into()`                    |
| `crates/consensus/primary/src/network/mod.rs:912` | `process_inbound_stream`                   | `(&PrimaryNetworkError).into()`                    |

### 5.2 `From<&PrimaryNetworkError> for Option<Penalty>`

Source: `crates/consensus/primary/src/error/network.rs:70-132`.

| Variant                                                                                                                                                                                                                                                                                                                                  | Severity                                 | Source                                                  |
| ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------- | ------------------------------------------------------- |
| `InvalidHeader(header_error)`                                                                                                                                                                                                                                                                                                            | delegates to `penalty_from_header_error` | `crates/consensus/primary/src/error/network.rs:76-78`   |
| `Certificate(CertManagerError::Certificate(CertificateError::Header(h)))`                                                                                                                                                                                                                                                                | delegates to `penalty_from_header_error` | `crates/consensus/primary/src/error/network.rs:81-83`   |
| `Certificate(CertManagerError::Certificate(CertificateError::TooOld(..)))`                                                                                                                                                                                                                                                               | `Mild`                                   | `crates/consensus/primary/src/error/network.rs:85`      |
| `Certificate(CertManagerError::Certificate(CertificateError::RecoverBlsAggregateSignatureBytes))`                                                                                                                                                                                                                                        | `Fatal`                                  | `crates/consensus/primary/src/error/network.rs:87-90`   |
| `Certificate(CertManagerError::Certificate(CertificateError::Unsigned))`                                                                                                                                                                                                                                                                 | `Fatal`                                  | `crates/consensus/primary/src/error/network.rs:87-90`   |
| `Certificate(CertManagerError::Certificate(CertificateError::Inquorate { .. }))`                                                                                                                                                                                                                                                         | `Fatal`                                  | `crates/consensus/primary/src/error/network.rs:87-90`   |
| `Certificate(CertManagerError::Certificate(CertificateError::InvalidSignature))`                                                                                                                                                                                                                                                         | `Fatal`                                  | `crates/consensus/primary/src/error/network.rs:87-90`   |
| `Certificate(CertManagerError::Certificate(CertificateError::ResChannelClosed(_)))`                                                                                                                                                                                                                                                      | None                                     | `crates/consensus/primary/src/error/network.rs:92-94`   |
| `Certificate(CertManagerError::Certificate(CertificateError::TooNew(..)))`                                                                                                                                                                                                                                                               | None                                     | `crates/consensus/primary/src/error/network.rs:92-94`   |
| `Certificate(CertManagerError::Certificate(CertificateError::Storage(_)))`                                                                                                                                                                                                                                                               | None                                     | `crates/consensus/primary/src/error/network.rs:92-94`   |
| `Certificate(CertManagerError::UnverifiedSignature(_))`                                                                                                                                                                                                                                                                                  | `Fatal`                                  | `crates/consensus/primary/src/error/network.rs:97`      |
| `Certificate(CertManagerError::*)` operational variants (`PendingCertificateNotFound`, `PendingParentsMismatch`, `CertificateManagerOneshot`, `FatalForwardAcceptedCertificate`, `NoCertificateFetched`, `FatalAppendParent`, `GC`, `JoinError`, `Pending`, `Storage`, `RequestBounds`, `Timeout`, `Network`, `ChannelClosed`, `TNSend`) | None                                     | `crates/consensus/primary/src/error/network.rs:99-113`  |
| `InvalidRequest(_)`                                                                                                                                                                                                                                                                                                                      | `Mild`                                   | `crates/consensus/primary/src/error/network.rs:115-118` |
| `UnknownConsensusHeaderDigest(_)`                                                                                                                                                                                                                                                                                                        | `None`                                   | `crates/consensus/primary/src/error/network.rs:115-117` |
| `UnknownStreamRequest(_)`                                                                                                                                                                                                                                                                                                                | `Mild`                                   | `crates/consensus/primary/src/error/network.rs:115-118` |
| `UnknownConsensusHeaderCert(_)`                                                                                                                                                                                                                                                                                                          | `Mild`                                   | `crates/consensus/primary/src/error/network.rs:115-118` |
| `InvalidEpochRequest`                                                                                                                                                                                                                                                                                                                    | `Medium`                                 | `crates/consensus/primary/src/error/network.rs:119-120` |
| `StdIo(_)`                                                                                                                                                                                                                                                                                                                               | `Medium`                                 | `crates/consensus/primary/src/error/network.rs:119-120` |
| `InvalidTopic`                                                                                                                                                                                                                                                                                                                           | `Fatal`                                  | `crates/consensus/primary/src/error/network.rs:121-122` |
| `Decode(_)`                                                                                                                                                                                                                                                                                                                              | `Fatal`                                  | `crates/consensus/primary/src/error/network.rs:121-122` |
| `UnavailableEpoch(_)`                                                                                                                                                                                                                                                                                                                    | None                                     | `crates/consensus/primary/src/error/network.rs:123-129` |
| `UnavailableEpochDigest(_)`                                                                                                                                                                                                                                                                                                              | None                                     | `crates/consensus/primary/src/error/network.rs:123-129` |
| `PeerNotInCommittee(_)`                                                                                                                                                                                                                                                                                                                  | None                                     | `crates/consensus/primary/src/error/network.rs:123-129` |
| `Storage(_)`                                                                                                                                                                                                                                                                                                                             | None                                     | `crates/consensus/primary/src/error/network.rs:123-129` |
| `Timeout(_)`                                                                                                                                                                                                                                                                                                                             | None                                     | `crates/consensus/primary/src/error/network.rs:123-129` |
| `StreamUnavailable(_)`                                                                                                                                                                                                                                                                                                                   | None                                     | `crates/consensus/primary/src/error/network.rs:123-129` |
| `Internal(_)`                                                                                                                                                                                                                                                                                                                            | None                                     | `crates/consensus/primary/src/error/network.rs:123-129` |

### 5.3 `penalty_from_header_error`

Source: `crates/consensus/primary/src/error/network.rs:137-171`.

| `HeaderError` variant              | Severity | Source                                                  |
| ---------------------------------- | -------- | ------------------------------------------------------- |
| `SyncBatches(_)`                   | `Mild`   | `crates/consensus/primary/src/error/network.rs:140-143` |
| `TooNew { .. }`                    | `Mild`   | `crates/consensus/primary/src/error/network.rs:140-143` |
| `Storage(_)`                       | `Mild`   | `crates/consensus/primary/src/error/network.rs:140-143` |
| `UnknownExecutionResult(_)`        | `Mild`   | `crates/consensus/primary/src/error/network.rs:140-143` |
| `InvalidParents`                   | `Medium` | `crates/consensus/primary/src/error/network.rs:145-147` |
| `WrongNumberOfParents(_, _)`       | `Medium` | `crates/consensus/primary/src/error/network.rs:145-147` |
| `TooOld { .. }`                    | `Medium` | `crates/consensus/primary/src/error/network.rs:145-147` |
| `InvalidTimestamp { .. }`          | `Severe` | `crates/consensus/primary/src/error/network.rs:149-151` |
| `InvalidParentRound`               | `Severe` | `crates/consensus/primary/src/error/network.rs:149-151` |
| `AlreadyVotedForLaterRound { .. }` | `Fatal`  | `crates/consensus/primary/src/error/network.rs:153-164` |
| `AlreadyVoted(_, _)`               | `Fatal`  | `crates/consensus/primary/src/error/network.rs:153-164` |
| `DuplicateParents`                 | `Fatal`  | `crates/consensus/primary/src/error/network.rs:153-164` |
| `TooManyParents(_, _)`             | `Fatal`  | `crates/consensus/primary/src/error/network.rs:153-164` |
| `TooManyBatches(_, _)`             | `Fatal`  | `crates/consensus/primary/src/error/network.rs:153-164` |
| `UnknownNetworkKey(_)`             | `Fatal`  | `crates/consensus/primary/src/error/network.rs:153-164` |
| `PeerNotAuthor`                    | `Fatal`  | `crates/consensus/primary/src/error/network.rs:153-164` |
| `InvalidGenesisParent(_)`          | `Fatal`  | `crates/consensus/primary/src/error/network.rs:153-164` |
| `ParentMissingSignature`           | `Fatal`  | `crates/consensus/primary/src/error/network.rs:153-164` |
| `InvalidParentTimestamp { .. }`    | `Fatal`  | `crates/consensus/primary/src/error/network.rs:153-164` |
| `UnkownWorkerId`                   | `Fatal`  | `crates/consensus/primary/src/error/network.rs:153-164` |
| `InvalidHeaderDigest`              | `Fatal`  | `crates/consensus/primary/src/error/network.rs:153-164` |
| `UnknownAuthority(_)`              | `Fatal`  | `crates/consensus/primary/src/error/network.rs:153-164` |
| `PendingCertificateOneshot`        | None     | `crates/consensus/primary/src/error/network.rs:166-169` |
| `TNSend(_)`                        | None     | `crates/consensus/primary/src/error/network.rs:166-169` |
| `InvalidEpoch { .. }`              | None     | `crates/consensus/primary/src/error/network.rs:166-169` |
| `ClosedWatchChannel`               | None     | `crates/consensus/primary/src/error/network.rs:166-169` |

### 5.4 `consensus_chain_error_to_penalty`

Source: `crates/consensus/primary/src/network/mod.rs:448-489`.

| Variant                                                                                                                                                                                                                                                                                                             | Severity | Source                                                |
| ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------- | ----------------------------------------------------- |
| `PackError(PackError::MissingBatch)`                                                                                                                                                                                                                                                                                | `Medium` | `crates/consensus/primary/src/network/mod.rs:451-454` |
| `PackError(PackError::NotConsensus)`                                                                                                                                                                                                                                                                                | `Medium` | `crates/consensus/primary/src/network/mod.rs:451-454` |
| `PackError(PackError::NotBatch)`                                                                                                                                                                                                                                                                                    | `Medium` | `crates/consensus/primary/src/network/mod.rs:451-454` |
| `PackError(PackError::NotEpoch)`                                                                                                                                                                                                                                                                                    | `Medium` | `crates/consensus/primary/src/network/mod.rs:451-454` |
| `PackError(PackError::InvalidConsensusChain)`                                                                                                                                                                                                                                                                       | `Severe` | `crates/consensus/primary/src/network/mod.rs:455-459` |
| `PackError(PackError::ExtraBatches)`                                                                                                                                                                                                                                                                                | `Severe` | `crates/consensus/primary/src/network/mod.rs:455-459` |
| `PackError(PackError::MissingBatches)`                                                                                                                                                                                                                                                                              | `Severe` | `crates/consensus/primary/src/network/mod.rs:455-459` |
| `PackError(PackError::CorruptPack)`                                                                                                                                                                                                                                                                                 | `Severe` | `crates/consensus/primary/src/network/mod.rs:455-459` |
| `PackError(PackError::InvalidEpoch)`                                                                                                                                                                                                                                                                                | `Severe` | `crates/consensus/primary/src/network/mod.rs:455-459` |
| `PackError` operational variants (`IO`, `BatchLoad`, `EpochLoad`, `Append`, `IndexAppend`, `Fetch`, `Open`, `ReadOnly`, `ReadError`, `MissingAuthority`, `SendFailed`, `ReceiveFailed`, `PersistError`, `InvalidConsensusNumber`, `ConsensusNumberAlreadyAdded`, `ConsensusNumberTooLow`, `ConsensusNumberTooHigh`) | None     | `crates/consensus/primary/src/network/mod.rs:460-476` |
| `EpochMismatch`                                                                                                                                                                                                                                                                                                     | `Mild`   | `crates/consensus/primary/src/network/mod.rs:478-480` |
| `PrevCommitteeEpochMismatch`                                                                                                                                                                                                                                                                                        | `Mild`   | `crates/consensus/primary/src/network/mod.rs:478-480` |
| `CrcError`                                                                                                                                                                                                                                                                                                          | `Mild`   | `crates/consensus/primary/src/network/mod.rs:478-480` |
| `EmptyImport`                                                                                                                                                                                                                                                                                                       | `Severe` | `crates/consensus/primary/src/network/mod.rs:481-483` |
| `InvalidImport`                                                                                                                                                                                                                                                                                                     | `Severe` | `crates/consensus/primary/src/network/mod.rs:481-483` |
| `StreamUnavailable` / `NoCurrentEpoch` / `EpochDbError(_)` / `IO(_)`                                                                                                                                                                                                                                                | None     | `crates/consensus/primary/src/network/mod.rs:484-487` |

## 6. Restraint Invariants (no-penalty cases)

These are deliberate tolerances for benign failures. Changing any of these from
`None` to a real penalty risks banning honest peers during normal operation.

- `InboundFailure::ResponseOmission` — local error, no peer fault. `crates/network-libp2p/src/consensus.rs:981`.
- PX-disconnect outbound failures — `pending_px_disconnects` short-circuit before penalty. `crates/network-libp2p/src/consensus.rs:940-943`.
- `WorkerNetworkError::Timeout` / `DBInsert` / `DBCommit` / `DBRead` / `StreamClosed` / `Network` / `BatchEpochMismatch` / `Internal` — local failures or epoch-boundary races. `crates/consensus/worker/src/network/error.rs:128-135`.
- `PrimaryNetworkError::UnavailableEpoch` / `UnavailableEpochDigest` — a peer "might not have this yet" during sync. `crates/consensus/primary/src/error/network.rs:123-124`.
- `PrimaryNetworkError::PeerNotInCommittee` — left to `None` to avoid penalizing during epoch transitions. `crates/consensus/primary/src/error/network.rs:125`.
- `PrimaryNetworkError::Storage` / `Timeout` / `StreamUnavailable` / `Internal` — local failures. `crates/consensus/primary/src/error/network.rs:126-129`.
- `CertManagerError::Certificate(CertificateError::TooNew(..))` — request races ahead of local state. `crates/consensus/primary/src/error/network.rs:92-94`.
- All `CertManagerError` operational variants (pending lookups, GC, oneshot drops, channel closures, internal storage / network errors). `crates/consensus/primary/src/error/network.rs:99-113`.
- `HeaderError::InvalidEpoch { .. }` — explicitly `None`; epoch boundary mismatch is not penalized. `crates/consensus/primary/src/error/network.rs:166-169`.
- `HeaderError::PendingCertificateOneshot` / `TNSend` / `ClosedWatchChannel` — local channel/task failures. `crates/consensus/primary/src/error/network.rs:166-169`.
- All `PackError` IO/load/persist/internal variants — local failures. `crates/consensus/primary/src/network/mod.rs:460-476`.
- `ConsensusChainError::StreamUnavailable` / `NoCurrentEpoch` / `EpochDbError` / `IO` — local failures during epoch pack streaming. `crates/consensus/primary/src/network/mod.rs:484-487`.
- A rejected kad put record from a banned source/publisher with `record.publisher.is_some()` — no extra penalty is stacked on top of the existing ban. `crates/network-libp2p/src/consensus.rs:1375-1388`.
- `OutboundFailure::UnsupportedProtocols` / `InboundFailure::UnsupportedProtocols` — honest version/role skew (multistream-select found no common protocol), not misbehavior; warn only, no penalty. Precondition for the #765 chain-id protocol split, which intentionally makes old/new nodes fail to peer. `crates/network-libp2p/src/consensus.rs:1071,1112`. The stream-path mirror (`StreamFailure::UnsupportedProtocol`) is likewise `None`. `crates/network-libp2p/src/stream/upgrade.rs`.
- The node's own peer id — `PeerManager::process_penalty` short-circuits before the score model when the target is the local identity, so a self-connection (e.g. a learned hairpin address routed back to our own id) can never ban the node's own worker. `crates/network-libp2p/src/peers/manager.rs` (`is_local_peer`). Self-connections are also denied earlier on the dial/discovery/connection paths so they do not reach the penalty path at all.

## 7. Ban Lifecycle

What happens when a peer's score crosses `min_score_before_ban` (`-50.0`):

1. `Score::apply_penalty` writes the new `telcoin_score` and calls `Score::update_score`. `crates/network-libp2p/src/peers/score.rs:84-100`.
2. `update_score` observes `!already_banned && self.is_banned()` and pushes `last_updated` forward by `banned_before_decay` so the score does not decay during the lockout. `crates/network-libp2p/src/peers/score.rs:141-154`.
3. `AllPeers::process_penalty` sees `new_reputation == Reputation::Banned` and calls `update_connection_status(peer_id, NewConnectionStatus::Banned)`, returning `PeerAction::Ban(_)`. `crates/network-libp2p/src/peers/all_peers.rs:121-127`.
4. `PeerManager::apply_peer_action` matches `PeerAction::Ban` and invokes `process_ban`, which pushes `PeerEvent::Banned(peer_id)`. `crates/network-libp2p/src/peers/manager.rs:287-292,389-401`.
5. `ConsensusNetwork::process_peer_manager_event` consumes `PeerEvent::Banned`, blacklists the peer in gossipsub, and removes it from the kad routing table. `crates/network-libp2p/src/consensus.rs:1152-1158`.
6. Future inbound/outbound connection attempts are denied by `handle_established_inbound_connection` / `handle_established_outbound_connection` via `peer_banned`. `crates/network-libp2p/src/peers/behavior.rs:73-107`.
7. The score does not decay during the `banned_before_decay` window (30 min by default). After expiry, `Score::update_at` resumes exponential decay using the halflife constant. `crates/network-libp2p/src/peers/score.rs:115-135`.

## 8. Open Questions / Notes

- `NetworkCommand::ReportPenalty` (`crates/network-libp2p/src/consensus.rs:700-707`) is the external entry point from the application layer. Worker and primary call sites all route through this command via `report_penalty` on the network handle (`crates/network-libp2p/src/types.rs:548`).
- `Penalty::Severe` never appears as a literal in `crates/network-libp2p/src/consensus.rs`. It only reaches `process_penalty` via app-layer error mappings: `BatchValidationError::RecoverTransaction`, `HeaderError::{InvalidTimestamp, InvalidParentRound}`, `PackError::{InvalidConsensusChain, ExtraBatches, MissingBatches, CorruptPack, InvalidEpoch}`, and `ConsensusChainError::{EmptyImport, InvalidImport}`.
- `PeerAction::DisconnectWithPX` adds the peer to `temporarily_banned` and emits `DisconnectPeerX`. This is a soft ban that bypasses the score model and does not produce a `PeerEvent::Banned`. `crates/network-libp2p/src/peers/manager.rs:295-303`.
- The exemption short-circuit in `Peer::apply_penalty` (it bypasses the score model when the caller passes a `TrustBasis`) is the only mechanism preventing scored bans of exempt peers — operator-allowlisted peers OR current-committee validators, the basis computed by `AllPeers` from the committee set. There is no allowlist applied at the network-layer call sites.
- `retrieve_consensus_header` (`crates/consensus/primary/src/network/mod.rs:711-725`) now routes through `(&PrimaryNetworkError).into()` like the other primary call sites. The only reachable variant from this code path is `UnknownConsensusHeaderDigest`, which the central mapping now maps to `None` (observers legitimately request not-yet-served headers).
- Penalty application is not debounced. A peer that produces many `Mild` errors in quick succession (e.g. during a sync flap) can still cross the ban threshold (`-50.0`) in fewer than ~50 events if its score has already drifted negative.
