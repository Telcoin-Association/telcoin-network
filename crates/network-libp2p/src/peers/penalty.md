# Peer Penalty System — Invariants Reference

This document is the single source of truth for the telcoin-network peer penalty system.
Every penalty severity, score-model invariant, and every site that applies a penalty is listed here with a source citation.
Treat each entry as an assertion to verify against the code.

## 1. Penalty Severity Levels

`Penalty` is defined in `crates/network-libp2p/src/peers/types.rs:109-126`.
Score deltas are applied in `crates/network-libp2p/src/peers/score.rs:84-100`.

| Variant           | Score Delta                          | Effect                                                             | Source                                        |
| ----------------- | ------------------------------------ | ------------------------------------------------------------------ | --------------------------------------------- |
| `Penalty::Mild`   | `-1.0`                               | Clamped to `[min_score, max_score]`                                | `crates/network-libp2p/src/peers/score.rs:90` |
| `Penalty::Medium` | `-5.0`                               | Clamped to `[min_score, max_score]`                                | `crates/network-libp2p/src/peers/score.rs:91` |
| `Penalty::Severe` | `-10.0`                              | Clamped to `[min_score, max_score]`                                | `crates/network-libp2p/src/peers/score.rs:92` |
| `Penalty::Fatal`  | sets score to `min_score` (`-100.0`) | Immediate ban — score jumps below `min_score_before_ban` (`-50.0`) | `crates/network-libp2p/src/peers/score.rs:93` |

## 2. Score Model Invariants

- Score range: `[min_score, max_score]` = `[-100.0, 100.0]`. `crates/config/src/network.rs:483-484`.
- Default starting score: `0.0`. `crates/config/src/network.rs:482`.
- Ban threshold (`min_score_before_ban`): `-50.0`. `crates/config/src/network.rs:495`.
- Disconnect threshold (`min_score_before_disconnect`): `-20.0`. `crates/config/src/network.rs:494`.
- Score halflife: `300.0` seconds; decay factor is `e^(-ln(2)/halflife * dt)`. `crates/config/src/network.rs:488`, `crates/network-libp2p/src/peers/score.rs:130-132`.
- `banned_before_decay_secs`: `30 * 60` (30 min). When a peer crosses the ban threshold, `last_updated` is pushed forward by this duration so the score does not decay during the lockout. `crates/config/src/network.rs:493`, `crates/network-libp2p/src/peers/score.rs:149-154`.
- Exempt peers skip penalty application entirely — `Peer::apply_penalty` short-circuits when the caller passes a `TrustBasis` (operator allowlist or committee validator); validator status is derived from the three tracked committee slots (previous/current/next) in `AllPeers::trust_basis`, not stored on the peer. `crates/network-libp2p/src/peers/peer.rs`, `crates/network-libp2p/src/peers/all_peers.rs`.
- Penalty application has no debouncing: every `process_penalty` call evaluates reputation immediately and may produce a ban on the same call. `crates/network-libp2p/src/peers/all_peers.rs:264-306`.
- Bans surface to the rest of the swarm as `PeerEvent::Banned`, pushed by `process_ban`. `crates/network-libp2p/src/peers/manager.rs:609-622`.
- `Penalty::Fatal` always crosses the ban threshold on the first call because `min_score` (`-100`) is less than `min_score_before_ban` (`-50`). `crates/network-libp2p/src/peers/score.rs:93`, `crates/config/src/network.rs:484,495`.
- Only the `Banned`/`Disconnected`/`Trusted` reputation transitions trigger a `PeerAction`. If the new reputation equals the prior reputation, `process_penalty` returns `PeerAction::NoAction`. `crates/network-libp2p/src/peers/all_peers.rs:272-274`.

## 3. Penalty Application Sites — Network Layer

All sites live in `crates/network-libp2p/src/consensus.rs`. `NetworkCommand::ReportPenalty` (L1012-1019) is the external entry from the app layer; everything below is internal.

### 3.1 Gossip events (`process_gossip_event`, L1104)

| Location                                     | Trigger (immediate guard)                                                     | Severity |
| -------------------------------------------- | ----------------------------------------------------------------------------- | -------- |
| `crates/network-libp2p/src/consensus.rs:1191` | `verify_gossip` rejected `TooLarge` (oversized payload) **and** the relaying peer's BLS has resolved (`RejectPenalty::FatalRelayer`) — the size bound is a compile-time protocol constant (`MAX_GOSSIP_MESSAGE_SIZE`) identical on every honest node, so under `Strict` validation a peer that forwards an oversized payload is itself misbehaving | `Fatal`  |
| `crates/network-libp2p/src/consensus.rs:1208` | `verify_gossip` rejected `UnauthorizedAuthor` (author absent / unresolved / unauthorized), or `TooLarge` from an unresolved relayer (`RejectPenalty::Skip`) — author-attributable or unattributable, so the forwarder is not penalized (issues #801/#819) | `None`   |
| `crates/network-libp2p/src/consensus.rs:1229` | `GossipEvent::GossipsubNotSupported { peer_id }`                              | `Fatal`  |
| `crates/network-libp2p/src/consensus.rs:1233` | `GossipEvent::SlowPeer { peer_id, failed_messages }`                          | `Mild`   |

### 3.2 Req/Res events (`process_reqres_event`, L1241)

| Location                                     | Trigger                                                                                            | Severity |
| -------------------------------------------- | -------------------------------------------------------------------------------------------------- | -------- |
| `crates/network-libp2p/src/consensus.rs:1346` | `OutboundFailure::DialFailure` / `OutboundFailure::ConnectionClosed`                               | `None`   |
| `crates/network-libp2p/src/consensus.rs:1351` | `OutboundFailure::Io(e)` with transport-flap `e.kind()` (Reset/Aborted/Timed/EOF/Pipe/Interrupted) | `None`   |
| `crates/network-libp2p/src/consensus.rs:1368` | `OutboundFailure::Io(e)` other kinds (codec violation, e.g. `io::Error::other`)                    | `Medium` |
| `crates/network-libp2p/src/consensus.rs:1375` | `OutboundFailure::Timeout`                                                                         | `Mild`   |
| `crates/network-libp2p/src/consensus.rs:1384` | `OutboundFailure::UnsupportedProtocols` (honest version/role skew — warn only) | `None` |
| `crates/network-libp2p/src/consensus.rs:1399` | `InboundFailure::Io(e)` with transport-flap `e.kind()`                                             | `None`   |
| `crates/network-libp2p/src/consensus.rs:1416` | `InboundFailure::Io(e)` other kinds (codec violation)                                              | `Medium` |
| `crates/network-libp2p/src/consensus.rs:1425` | `InboundFailure::UnsupportedProtocols` (honest version/role skew — warn only) | `None` |
| `crates/network-libp2p/src/consensus.rs:1428` | `InboundFailure::Timeout` / `InboundFailure::ConnectionClosed`                                     | `None`   |

`InboundFailure::ResponseOmission` is explicitly a no-op (local error). See restraint invariants below.

### 3.3 Kademlia outbound `GetRecord` query (`process_kad_event`, L1831)

| Location                                      | Trigger                                                                                                         | Severity |
| --------------------------------------------- | --------------------------------------------------------------------------------------------------------------- | -------- |
| `crates/network-libp2p/src/consensus.rs:1889` | `kad::QueryResult::GetRecord(Ok(FoundRecord))` but `peer_record_valid` returned `None` (bad sig / key mismatch) | `Fatal`  |

### 3.4 Kad put-record handler (`process_kad_put_request`, L2000)

| Location                                      | Trigger                                                                               | Severity |
| --------------------------------------------- | ------------------------------------------------------------------------------------- | -------- |
| `crates/network-libp2p/src/consensus.rs:2030` | record was rejected AND `record.publisher.is_none()` (publisher-less record)          | `Fatal`  |
| `crates/network-libp2p/src/consensus.rs:2068` | `peer_record_valid(&record).is_some()` but `!is_newer_record(&record)` (stale record — trace log only; kad routinely replays put-records to refresh TTLs) | `None`   |
| `crates/network-libp2p/src/consensus.rs:2079` | `peer_record_valid(&record).is_none()` (invalid signature / wrong network key)        | `Fatal`  |

A rejected record from a banned source/publisher with a publisher present does
NOT incur an extra penalty here — only the missing-publisher case is fatal. The
ban itself was applied elsewhere.

### 3.5 Kad query-result post-processing (`process_kad_query_result`, L2170)

| Location                                      | Trigger                                                                       | Severity |
| --------------------------------------------- | ----------------------------------------------------------------------------- | -------- |
| `crates/network-libp2p/src/consensus.rs:2194` | record valid but `query.request != key` (returned key does not match request) | `Fatal`  |
| `crates/network-libp2p/src/consensus.rs:1488` | `peer_record_valid(&record).is_none()` on the late-step path                  | `Fatal`  |

## 4. Penalty Application Sites — Worker Layer

### 4.1 `crates/consensus/worker/src/network/mod.rs` call sites

| Location                                         | Handler                                                | Source of penalty                               |
| ------------------------------------------------ | ------------------------------------------------------ | ----------------------------------------------- |
| `crates/consensus/worker/src/network/mod.rs:334` | `process_report_batch`                                 | `WorkerNetworkError::into() -> Option<Penalty>` |
| `crates/consensus/worker/src/network/mod.rs:371` | `process_gossip`                                       | `WorkerNetworkError::penalty()`                 |
| `crates/consensus/worker/src/network/mod.rs:435` | `process_inbound_stream`                               | `WorkerNetworkError::penalty()`                 |

### 4.2 `WorkerNetworkError::penalty()` mapping

Source: `crates/consensus/worker/src/network/error.rs:78-138`.

| Error variant                                                                       | Severity | Source                                                 |
| ----------------------------------------------------------------------------------- | -------- | ------------------------------------------------------ |
| `BatchValidation(CanonicalChain { .. })`                                            | `Mild`   | `crates/consensus/worker/src/network/error.rs:86`      |
| `BatchValidation(InvalidEpoch { .. })`                                              | `Medium` | `crates/consensus/worker/src/network/error.rs:88-90`   |
| `BatchValidation(InvalidTx4844(_))`                                                 | `Medium` | `crates/consensus/worker/src/network/error.rs:88-90`   |
| `BatchValidation(UnsupportedTxType { .. })`                                         | `Medium` | `crates/consensus/worker/src/network/error.rs:88-90`   |
| `BatchValidation(RecoverTransaction(..))`                                           | `Severe` | `crates/consensus/worker/src/network/error.rs:92`      |
| `BatchValidation(EmptyBatch)`                                                       | `Fatal`  | `crates/consensus/worker/src/network/error.rs:94-102`  |
| `BatchValidation(InvalidBaseFee { .. })`                                            | `Fatal`  | `crates/consensus/worker/src/network/error.rs:94-102`  |
| `BatchValidation(InvalidWorkerId { .. })`                                           | `Fatal`  | `crates/consensus/worker/src/network/error.rs:94-102`  |
| `BatchValidation(InvalidDigest)`                                                    | `Fatal`  | `crates/consensus/worker/src/network/error.rs:94-102`  |
| `BatchValidation(GasOverflow)`                                                      | `Fatal`  | `crates/consensus/worker/src/network/error.rs:94-102`  |
| `BatchValidation(CalculateMaxPossibleGas)`                                          | `Fatal`  | `crates/consensus/worker/src/network/error.rs:94-102`  |
| `BatchValidation(HeaderMaxGasExceedsGasLimit { .. })`                               | `Fatal`  | `crates/consensus/worker/src/network/error.rs:94-102`  |
| `BatchValidation(HeaderTransactionBytesExceedsMax(_))`                              | `Fatal`  | `crates/consensus/worker/src/network/error.rs:94-102`  |
| `InvalidRequest(_)`                                                                 | `Mild`   | `crates/consensus/worker/src/network/error.rs:106-107` |
| `UnknownStreamRequest(_)`                                                           | `Mild`   | `crates/consensus/worker/src/network/error.rs:106-107` |
| `StdIo(io_err)` kind `ConnectionReset`/`ConnectionAborted`/`TimedOut`/`Interrupted` | `Mild`   | `crates/consensus/worker/src/network/error.rs:109-115` |
| `StdIo(io_err)` other kinds                                                         | `Medium` | `crates/consensus/worker/src/network/error.rs:116`     |
| `NonCommitteeBatch`                                                                 | `Medium` | `crates/consensus/worker/src/network/error.rs:120`     |
| `InvalidTopic`                                                                      | `Fatal`  | `crates/consensus/worker/src/network/error.rs:122-127` |
| `Bcs(_)`                                                                            | `Fatal`  | `crates/consensus/worker/src/network/error.rs:122-127` |
| `TooManyBatches { .. }`                                                             | `Fatal`  | `crates/consensus/worker/src/network/error.rs:122-127` |
| `UnexpectedBatch(_)`                                                                | `Fatal`  | `crates/consensus/worker/src/network/error.rs:122-127` |
| `DuplicateBatch(_)`                                                                 | `Fatal`  | `crates/consensus/worker/src/network/error.rs:122-127` |
| `RequestHashMismatch`                                                               | `Fatal`  | `crates/consensus/worker/src/network/error.rs:122-127` |
| `Timeout(_)`                                                                        | None     | `crates/consensus/worker/src/network/error.rs:129-136` |
| `DBInsert(_)` / `DBCommit(_)` / `DBRead(_)`                                         | None     | `crates/consensus/worker/src/network/error.rs:129-136` |
| `StreamClosed`                                                                      | None     | `crates/consensus/worker/src/network/error.rs:129-136` |
| `Network(_)`                                                                        | None     | `crates/consensus/worker/src/network/error.rs:129-136` |
| `BatchEpochMismatch(_, _)`                                                          | None     | `crates/consensus/worker/src/network/error.rs:129-136` |
| `Internal(_)`                                                                       | None     | `crates/consensus/worker/src/network/error.rs:129-136` |

## 5. Penalty Application Sites — Primary Layer

### 5.1 `crates/consensus/primary/src/network/mod.rs` call sites

| Location                                          | Handler                                    | Source of penalty                                  |
| ------------------------------------------------- | ------------------------------------------ | -------------------------------------------------- |
| `crates/consensus/primary/src/network/mod.rs:421` | `stream_import` (epoch pack download)      | `Self::consensus_chain_error_to_penalty(&err)`     |
| `crates/consensus/primary/src/network/mod.rs:682` | `retrieve_missing_certs`                   | `(&PrimaryNetworkError).into() -> Option<Penalty>` |
| `crates/consensus/primary/src/network/mod.rs:719` | `retrieve_consensus_header`                | `(&PrimaryNetworkError).into()` — only reachable variant `UnknownConsensusHeaderDigest` → `None` |
| `crates/consensus/primary/src/network/mod.rs:1401` | `retrieve_epoch_record`                    | `(&PrimaryNetworkError).into()`                    |
| `crates/consensus/primary/src/network/mod.rs:1440` | `process_gossip`                           | `(&PrimaryNetworkError).into()`                    |
| `crates/consensus/primary/src/network/mod.rs:912` | `process_inbound_stream`                   | `(&PrimaryNetworkError).into()`                    |

### 5.2 `From<&PrimaryNetworkError> for Option<Penalty>`

Source: `crates/consensus/primary/src/error/network.rs:142-204`.

| Variant                                                                                                                                                                                                                                                                                                                                  | Severity                                 | Source                                                  |
| ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------- | ------------------------------------------------------- |
| `InvalidHeader(header_error)`                                                                                                                                                                                                                                                                                                            | delegates to `penalty_from_header_error` | `crates/consensus/primary/src/error/network.rs:147-149`   |
| `Certificate(CertManagerError::Certificate(CertificateError::Header(h)))`                                                                                                                                                                                                                                                                | delegates to `penalty_from_header_error` | `crates/consensus/primary/src/error/network.rs:152-154`   |
| `Certificate(CertManagerError::Certificate(CertificateError::TooOld(..)))`                                                                                                                                                                                                                                                               | `Mild`                                   | `crates/consensus/primary/src/error/network.rs:156`      |
| `Certificate(CertManagerError::Certificate(CertificateError::RecoverBlsAggregateSignatureBytes))`                                                                                                                                                                                                                                        | `Fatal`                                  | `crates/consensus/primary/src/error/network.rs:158-161`   |
| `Certificate(CertManagerError::Certificate(CertificateError::Unsigned))`                                                                                                                                                                                                                                                                 | `Fatal`                                  | `crates/consensus/primary/src/error/network.rs:158-161`   |
| `Certificate(CertManagerError::Certificate(CertificateError::Inquorate { .. }))`                                                                                                                                                                                                                                                         | `Fatal`                                  | `crates/consensus/primary/src/error/network.rs:158-161`   |
| `Certificate(CertManagerError::Certificate(CertificateError::InvalidSignature))`                                                                                                                                                                                                                                                         | `Fatal`                                  | `crates/consensus/primary/src/error/network.rs:158-161`   |
| `Certificate(CertManagerError::Certificate(CertificateError::ResChannelClosed(_)))`                                                                                                                                                                                                                                                      | None                                     | `crates/consensus/primary/src/error/network.rs:163-165`   |
| `Certificate(CertManagerError::Certificate(CertificateError::TooNew(..)))`                                                                                                                                                                                                                                                               | None                                     | `crates/consensus/primary/src/error/network.rs:163-165`   |
| `Certificate(CertManagerError::Certificate(CertificateError::Storage(_)))`                                                                                                                                                                                                                                                               | None                                     | `crates/consensus/primary/src/error/network.rs:163-165`   |
| `Certificate(CertManagerError::UnverifiedSignature(_))`                                                                                                                                                                                                                                                                                  | `Fatal`                                  | `crates/consensus/primary/src/error/network.rs:168`      |
| `Certificate(CertManagerError::*)` operational variants (`PendingCertificateNotFound`, `PendingParentsMismatch`, `CertificateManagerOneshot`, `FatalForwardAcceptedCertificate`, `NoCertificateFetched`, `FatalAppendParent`, `GC`, `JoinError`, `Pending`, `Storage`, `RequestBounds`, `Timeout`, `Network`, `ChannelClosed`, `TNSend`) | None                                     | `crates/consensus/primary/src/error/network.rs:170-184`  |
| `InvalidRequest(_)`                                                                                                                                                                                                                                                                                                                      | `Mild`                                   | `crates/consensus/primary/src/error/network.rs:189-191` |
| `UnknownConsensusHeaderDigest(_)`                                                                                                                                                                                                                                                                                                        | `None`                                   | `crates/consensus/primary/src/error/network.rs:115-117` |
| `UnknownStreamRequest(_)`                                                                                                                                                                                                                                                                                                                | `Mild`                                   | `crates/consensus/primary/src/error/network.rs:115-118` |
| `UnknownConsensusHeaderCert(_)`                                                                                                                                                                                                                                                                                                          | `Mild`                                   | `crates/consensus/primary/src/error/network.rs:189-191` |
| `InvalidEpochRequest`                                                                                                                                                                                                                                                                                                                    | `Medium`                                 | `crates/consensus/primary/src/error/network.rs:192-193` |
| `StdIo(_)`                                                                                                                                                                                                                                                                                                                               | `Medium`                                 | `crates/consensus/primary/src/error/network.rs:192-193` |
| `InvalidTopic`                                                                                                                                                                                                                                                                                                                           | `Fatal`                                  | `crates/consensus/primary/src/error/network.rs:194-195` |
| `Decode(_)`                                                                                                                                                                                                                                                                                                                              | `Fatal`                                  | `crates/consensus/primary/src/error/network.rs:194-195` |
| `UnavailableEpoch(_)`                                                                                                                                                                                                                                                                                                                    | None                                     | `crates/consensus/primary/src/error/network.rs:196-202` |
| `UnavailableEpochDigest(_)`                                                                                                                                                                                                                                                                                                              | None                                     | `crates/consensus/primary/src/error/network.rs:196-202` |
| `PeerNotInCommittee(_)`                                                                                                                                                                                                                                                                                                                  | None                                     | `crates/consensus/primary/src/error/network.rs:196-202` |
| `Storage(_)`                                                                                                                                                                                                                                                                                                                             | None                                     | `crates/consensus/primary/src/error/network.rs:196-202` |
| `Timeout(_)`                                                                                                                                                                                                                                                                                                                             | None                                     | `crates/consensus/primary/src/error/network.rs:196-202` |
| `StreamUnavailable(_)`                                                                                                                                                                                                                                                                                                                   | None                                     | `crates/consensus/primary/src/error/network.rs:123-129` |
| `Internal(_)`                                                                                                                                                                                                                                                                                                                            | None                                     | `crates/consensus/primary/src/error/network.rs:196-202` |

### 5.3 `penalty_from_header_error`

Source: `crates/consensus/primary/src/error/network.rs:210-259`.

| `HeaderError` variant              | Severity | Source                                                  |
| ---------------------------------- | -------- | ------------------------------------------------------- |
| `SyncBatches(_)`                   | `Mild`   | `crates/consensus/primary/src/error/network.rs:213` |
| `TooNew { .. }`                    | `Mild`   | `crates/consensus/primary/src/error/network.rs:213` |
| `Storage(_)`                       | None     | `crates/consensus/primary/src/error/network.rs:244-257` |
| `UnknownExecutionResult(_)`        | None     | `crates/consensus/primary/src/error/network.rs:244-257` |
| `InvalidParents`                   | `Medium` | `crates/consensus/primary/src/error/network.rs:215-217` |
| `WrongNumberOfParents(_, _)`       | `Medium` | `crates/consensus/primary/src/error/network.rs:215-217` |
| `TooOld { .. }`                    | `Medium` | `crates/consensus/primary/src/error/network.rs:215-217` |
| `InvalidTimestamp { .. }`          | `Severe` | `crates/consensus/primary/src/error/network.rs:227-229` |
| `InvalidParentRound`               | `Severe` | `crates/consensus/primary/src/error/network.rs:227-229` |
| `AlreadyVotedForLaterRound { .. }` | `Fatal`  | `crates/consensus/primary/src/error/network.rs:231-243` |
| `AlreadyVoted(_, _)`               | `Fatal`  | `crates/consensus/primary/src/error/network.rs:231-243` |
| `DuplicateParents`                 | `Fatal`  | `crates/consensus/primary/src/error/network.rs:231-243` |
| `TooManyParents(_, _)`             | `Fatal`  | `crates/consensus/primary/src/error/network.rs:231-243` |
| `TooManyBatches(_, _)`             | `Fatal`  | `crates/consensus/primary/src/error/network.rs:231-243` |
| `UnknownNetworkKey(_)`             | `Fatal`  | `crates/consensus/primary/src/error/network.rs:231-243` |
| `PeerNotAuthor`                    | `Fatal`  | `crates/consensus/primary/src/error/network.rs:231-243` |
| `InvalidGenesisParent(_)`          | `Fatal`  | `crates/consensus/primary/src/error/network.rs:231-243` |
| `ParentMissingSignature`           | `Fatal`  | `crates/consensus/primary/src/error/network.rs:231-243` |
| `InvalidParentTimestamp { .. }`    | `Fatal`  | `crates/consensus/primary/src/error/network.rs:231-243` |
| `UnkownWorkerId`                   | `Fatal`  | `crates/consensus/primary/src/error/network.rs:231-243` |
| `InvalidHeaderDigest`              | `Fatal`  | `crates/consensus/primary/src/error/network.rs:153-164` |
| `UnknownAuthority(_)`              | `Fatal`  | `crates/consensus/primary/src/error/network.rs:231-243` |
| `PendingCertificateOneshot`        | None     | `crates/consensus/primary/src/error/network.rs:251-257` |
| `TNSend(_)`                        | None     | `crates/consensus/primary/src/error/network.rs:251-257` |
| `InvalidEpoch { .. }`              | None     | `crates/consensus/primary/src/error/network.rs:251-257` |
| `ClosedWatchChannel`               | None     | `crates/consensus/primary/src/error/network.rs:251-257` |

### 5.4 `consensus_chain_error_to_penalty`

Source: `crates/consensus/primary/src/network/mod.rs:1131-1180`.

| Variant                                                                                                                                                                                                                                                                                                             | Severity | Source                                                |
| ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------- | ----------------------------------------------------- |
| `PackError(PackError::MissingBatch)`                                                                                                                                                                                                                                                                                | `Medium` | `crates/consensus/primary/src/network/mod.rs:1134-1137` |
| `PackError(PackError::NotConsensus)`                                                                                                                                                                                                                                                                                | `Medium` | `crates/consensus/primary/src/network/mod.rs:1134-1137` |
| `PackError(PackError::NotBatch)`                                                                                                                                                                                                                                                                                    | `Medium` | `crates/consensus/primary/src/network/mod.rs:1134-1137` |
| `PackError(PackError::NotEpoch)`                                                                                                                                                                                                                                                                                    | `Medium` | `crates/consensus/primary/src/network/mod.rs:1134-1137` |
| `PackError(PackError::InvalidConsensusChain)`                                                                                                                                                                                                                                                                       | `Severe` | `crates/consensus/primary/src/network/mod.rs:1138-1144` |
| `PackError(PackError::ExtraBatches)`                                                                                                                                                                                                                                                                                | `Severe` | `crates/consensus/primary/src/network/mod.rs:1138-1144` |
| `PackError(PackError::MissingBatches)`                                                                                                                                                                                                                                                                              | `Severe` | `crates/consensus/primary/src/network/mod.rs:1138-1144` |
| `PackError(PackError::CorruptPack)`                                                                                                                                                                                                                                                                                 | `Severe` | `crates/consensus/primary/src/network/mod.rs:1138-1144` |
| `PackError(PackError::InvalidEpoch)`                                                                                                                                                                                                                                                                                | `Severe` | `crates/consensus/primary/src/network/mod.rs:1138-1144` |
| `PackError` operational variants (`IO`, `BatchLoad`, `EpochLoad`, `Append`, `IndexAppend`, `Fetch`, `Open`, `ReadOnly`, `ReadError`, `MissingAuthority`, `SendFailed`, `ReceiveFailed`, `PersistError`, `InvalidConsensusNumber`, `ConsensusNumberAlreadyAdded`, `ConsensusNumberTooLow`, `ConsensusNumberTooHigh`) | None     | `crates/consensus/primary/src/network/mod.rs:1145-1162` |
| `EpochMismatch`                                                                                                                                                                                                                                                                                                     | `Mild`   | `crates/consensus/primary/src/network/mod.rs:1164-1166` |
| `PrevCommitteeEpochMismatch`                                                                                                                                                                                                                                                                                        | `Mild`   | `crates/consensus/primary/src/network/mod.rs:1164-1166` |
| `CrcError`                                                                                                                                                                                                                                                                                                          | `Mild`   | `crates/consensus/primary/src/network/mod.rs:1164-1166` |
| `EmptyImport`                                                                                                                                                                                                                                                                                                       | `Severe` | `crates/consensus/primary/src/network/mod.rs:1167-1169` |
| `InvalidImport`                                                                                                                                                                                                                                                                                                     | `Severe` | `crates/consensus/primary/src/network/mod.rs:1167-1169` |
| `StreamUnavailable` / `NoCurrentEpoch` / `EpochDbError(_)` / `IO(_)`                                                                                                                                                                                                                                                | None     | `crates/consensus/primary/src/network/mod.rs:1170-1178` |

## 6. Restraint Invariants (no-penalty cases)

These are deliberate tolerances for benign failures. Changing any of these from
`None` to a real penalty risks banning honest peers during normal operation.

- `InboundFailure::ResponseOmission` — local error, no peer fault. `crates/network-libp2p/src/consensus.rs:1431`.
- PX-disconnect outbound failures — `pending_px_disconnects` short-circuit before penalty. `crates/network-libp2p/src/consensus.rs:1327-1330`.
- `WorkerNetworkError::Timeout` / `DBInsert` / `DBCommit` / `DBRead` / `StreamClosed` / `Network` / `BatchEpochMismatch` / `Internal` — local failures or epoch-boundary races. `crates/consensus/worker/src/network/error.rs:129-136`.
- `PrimaryNetworkError::UnavailableEpoch` / `UnavailableEpochDigest` — a peer "might not have this yet" during sync. `crates/consensus/primary/src/error/network.rs:196-197`.
- `PrimaryNetworkError::PeerNotInCommittee` — left to `None` to avoid penalizing during epoch transitions. `crates/consensus/primary/src/error/network.rs:198`.
- `PrimaryNetworkError::Storage` / `Timeout` / `StreamUnavailable` / `Internal` — local failures. `crates/consensus/primary/src/error/network.rs:199-202`.
- `CertManagerError::Certificate(CertificateError::TooNew(..))` — request races ahead of local state. `crates/consensus/primary/src/error/network.rs:163-165`.
- All `CertManagerError` operational variants (pending lookups, GC, oneshot drops, channel closures, internal storage / network errors). `crates/consensus/primary/src/error/network.rs:170-184`.
- `HeaderError::InvalidEpoch { .. }` — explicitly `None`; epoch boundary mismatch is not penalized. `crates/consensus/primary/src/error/network.rs:251-257`.
- `HeaderError::PendingCertificateOneshot` / `TNSend` / `ClosedWatchChannel` — local channel/task failures. `crates/consensus/primary/src/error/network.rs:251-257`.
- All `PackError` IO/load/persist/internal variants — local failures. `crates/consensus/primary/src/network/mod.rs:1145-1162`.
- `ConsensusChainError::StreamUnavailable` / `NoCurrentEpoch` / `EpochDbError` / `IO` — local failures during epoch pack streaming. `crates/consensus/primary/src/network/mod.rs:1170-1178`.
- A rejected kad put record from a banned source/publisher with `record.publisher.is_some()` — no extra penalty is stacked on top of the existing ban. `crates/network-libp2p/src/consensus.rs:2013-2035`.
- `OutboundFailure::UnsupportedProtocols` / `InboundFailure::UnsupportedProtocols` — honest version/role skew (multistream-select found no common protocol), not misbehavior; warn only, no penalty. Precondition for the #765 chain-id protocol split, which intentionally makes old/new nodes fail to peer. `crates/network-libp2p/src/consensus.rs:1384,1425`. The stream-path mirror (`StreamFailure::UnsupportedProtocol`) is likewise `None`. `crates/network-libp2p/src/stream/upgrade.rs`.
- The node's own peer id — `PeerManager::process_penalty` short-circuits before the score model when the target is the local identity, so a self-connection (e.g. a learned hairpin address routed back to our own id) can never ban the node's own worker. `crates/network-libp2p/src/peers/manager.rs` (`is_local_peer`). Self-connections are also denied earlier on the dial/discovery/connection paths so they do not reach the penalty path at all.

## 7. Ban Lifecycle

What happens when a peer's score crosses `min_score_before_ban` (`-50.0`):

1. `Score::apply_penalty` writes the new `telcoin_score` and calls `Score::update_score`. `crates/network-libp2p/src/peers/score.rs:84-100`.
2. `update_score` observes `!already_banned && self.is_banned()` and pushes `last_updated` forward by `banned_before_decay` so the score does not decay during the lockout. `crates/network-libp2p/src/peers/score.rs:141-154`.
3. `AllPeers::process_penalty` sees `new_reputation == Reputation::Banned` and calls `update_connection_status(peer_id, NewConnectionStatus::Banned)`, returning `PeerAction::Ban(_)`. `crates/network-libp2p/src/peers/all_peers.rs:276-282`.
4. `PeerManager::apply_peer_action` matches `PeerAction::Ban` and invokes `process_ban`, which pushes `PeerEvent::Banned(peer_id)`. `crates/network-libp2p/src/peers/manager.rs:458-462,609-622`.
5. `ConsensusNetwork::process_peer_manager_event` consumes `PeerEvent::Banned`, blacklists the peer in gossipsub, and removes it from the kad routing table. `crates/network-libp2p/src/consensus.rs:1743-1749`.
6. Future inbound/outbound connection attempts are denied by `handle_established_inbound_connection` / `handle_established_outbound_connection` via `peer_banned`. `crates/network-libp2p/src/peers/behavior.rs:87-134`.
7. The score does not decay during the `banned_before_decay` window (30 min by default). After expiry, `Score::update_at` resumes exponential decay using the halflife constant. `crates/network-libp2p/src/peers/score.rs:115-135`.

## 8. Open Questions / Notes

- `NetworkCommand::ReportPenalty` (`crates/network-libp2p/src/consensus.rs:1012-1019`) is the external entry point from the application layer. Worker and primary call sites all route through this command via `report_penalty` on the network handle (`crates/network-libp2p/src/types.rs:755`).
- `Penalty::Severe` never appears as a literal in `crates/network-libp2p/src/consensus.rs`. It only reaches `process_penalty` via app-layer error mappings: `BatchValidationError::RecoverTransaction`, `HeaderError::{InvalidTimestamp, InvalidParentRound}`, `PackError::{InvalidConsensusChain, ExtraBatches, MissingBatches, CorruptPack, InvalidEpoch}`, and `ConsensusChainError::{EmptyImport, InvalidImport}`.
- `PeerAction::DisconnectWithPX` adds the peer to `temporarily_banned` and emits `DisconnectPeerX`. This is a soft ban that bypasses the score model and does not produce a `PeerEvent::Banned`. `crates/network-libp2p/src/peers/manager.rs:468-474`.
- The exemption short-circuit in `Peer::apply_penalty` (it bypasses the score model when the caller passes a `TrustBasis`) is the only mechanism preventing scored bans of exempt peers — operator-allowlisted peers OR current-committee validators, the basis computed by `AllPeers` from the committee set. There is no allowlist applied at the network-layer call sites.
- `retrieve_consensus_header` (`crates/consensus/primary/src/network/mod.rs:711-725`) now routes through `(&PrimaryNetworkError).into()` like the other primary call sites. The only reachable variant from this code path is `UnknownConsensusHeaderDigest`, which the central mapping now maps to `None` (observers legitimately request not-yet-served headers).
- Penalty application is not debounced. A peer that produces many `Mild` errors in quick succession (e.g. during a sync flap) can still cross the ban threshold (`-50.0`) in fewer than ~50 events if its score has already drifted negative.
