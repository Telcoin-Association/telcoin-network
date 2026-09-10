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
| `crates/network-libp2p/src/consensus.rs:1205` | `verify_gossip` rejected `UnauthorizedAuthor` (author is not an authorized publisher for the topic) **and** the author's BLS has resolved (`RejectPenalty::FatalAuthor`) — the fault is the author's content, so the penalty lands on `GossipMessage::source`, never on the forwarding relayer (issues #801/#819) | `Fatal`  |
| `crates/network-libp2p/src/consensus.rs:1208` | `TooLarge` from an unresolved relayer, or `UnauthorizedAuthor` with an absent / unresolved author (`RejectPenalty::Skip`) — the accountable peer's identity has not resolved, so the reject is unattributable and the forwarder is never charged for an author fault (issues #801/#819) | `None`   |
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
| `crates/network-libp2p/src/consensus.rs:2047` | `put_record_rate_limited(source)` — inbound puts exceed the per-source budget; dropped before the BLS verify and kad store write (GHSA-f6rq-62rr-4h9g) | `Medium` |
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

The gossip site charges one of two peers: the author for a content-determined fault
(`is_author_content_fault` — `Bcs`, `InvalidTopic`, `TooManyBatches`, `UnexpectedBatch`,
`DuplicateBatch`, `RequestHashMismatch`), the relaying peer for every other fault
(issues #801/#819). Either way the `zip` drops the penalty when that peer's BLS identity
has not resolved. `crates/consensus/worker/src/network/mod.rs:369-370`, `crates/consensus/worker/src/network/error.rs:156-187`.

### 4.2 `WorkerNetworkError::penalty()` mapping

Source: `crates/consensus/worker/src/network/error.rs:78-138`.

`TooManyBatches`, `UnexpectedBatch`, `DuplicateBatch`, `RequestHashMismatch`,
`UnknownStreamRequest` and `StreamClosed` are classified for forward-safety: no production
code constructs them today, only tests
(`crates/consensus/worker/tests/it/network_tests.rs:344-347,399,403`).

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
| `crates/consensus/primary/src/network/mod.rs:749` | `try_sync_consensus_output_exchange`       | `Self::consensus_chain_error_to_penalty(&e)`       |
| `crates/consensus/primary/src/network/mod.rs:1343` | `process_vote_request`                     | `(&PrimaryNetworkError).into() -> Option<Penalty>` |
| `crates/consensus/primary/src/network/mod.rs:1401` | `process_epoch_record_request`             | `(&PrimaryNetworkError).into()`                    |
| `crates/consensus/primary/src/network/mod.rs:1440` | `process_gossip`                           | `(&PrimaryNetworkError).into()`                    |

Three peer-facing primary paths deliberately apply no penalty and so have no row above:
the certificate fetch (`fetch_certificates`, `crates/consensus/primary/src/network/mod.rs:457`),
the epoch-pack sync import (`crates/consensus/primary/src/network/mod.rs:1080-1111`),
and the inbound sync-stream serve (`process_inbound_sync_stream`, `crates/consensus/primary/src/network/mod.rs:1462-1584`).
See the restraint invariants in section 6.

### 5.2 `From<&PrimaryNetworkError> for Option<Penalty>`

Source: `crates/consensus/primary/src/error/network.rs:142-204`.

On the gossip path these severities do not always land on the peer that forwarded the
message. `is_author_content_fault` (`crates/consensus/primary/src/error/network.rs:115-138`) redirects content-determined faults to
the gossip author instead of the relayer, so a relayer is never banned for forwarding a
malformed certificate it did not write.

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
| `UnknownConsensusOutput(_)`                                                                                                                                                                                                                                                                                                              | None                                     | `crates/consensus/primary/src/error/network.rs:188`     |
| `InvalidRequest(_)`                                                                                                                                                                                                                                                                                                                      | `Mild`                                   | `crates/consensus/primary/src/error/network.rs:189-191` |
| `InvalidEpochVote(_, _, _)`                                                                                                                                                                                                                                                                                                              | `Mild`                                   | `crates/consensus/primary/src/error/network.rs:189-191` |
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
| `ConsensusChainError(_)`                                                                                                                                                                                                                                                                                                                 | None                                     | `crates/consensus/primary/src/error/network.rs:196-202` |
| `Internal(_)`                                                                                                                                                                                                                                                                                                                            | None                                     | `crates/consensus/primary/src/error/network.rs:196-202` |

### 5.3 `penalty_from_header_error`

Source: `crates/consensus/primary/src/error/network.rs:210-259`.

| `HeaderError` variant              | Severity | Source                                                  |
| ---------------------------------- | -------- | ------------------------------------------------------- |
| `SyncBatches(_)`                   | `Mild`   | `crates/consensus/primary/src/error/network.rs:213` |
| `TooNew { .. }`                    | `Mild`   | `crates/consensus/primary/src/error/network.rs:213` |
| `InvalidParents`                   | `Medium` | `crates/consensus/primary/src/error/network.rs:215-217` |
| `WrongNumberOfParents(_, _)`       | `Medium` | `crates/consensus/primary/src/error/network.rs:215-217` |
| `TooOld { .. }`                    | `Medium` | `crates/consensus/primary/src/error/network.rs:215-217` |
| `InvalidTimestamp { .. }`          | `Severe` | `crates/consensus/primary/src/error/network.rs:227-229` |
| `InvalidParentRound`               | `Severe` | `crates/consensus/primary/src/error/network.rs:227-229` |
| `InvalidSeedSignature`             | `Severe` | `crates/consensus/primary/src/error/network.rs:227-229` |
| `AlreadyVotedForLaterRound { .. }` | `Fatal`  | `crates/consensus/primary/src/error/network.rs:231-243` |
| `AlreadyVoted(_, _)`               | `Fatal`  | `crates/consensus/primary/src/error/network.rs:231-243` |
| `DuplicateParents`                 | `Fatal`  | `crates/consensus/primary/src/error/network.rs:231-243` |
| `TooManyParents(_, _)`             | `Fatal`  | `crates/consensus/primary/src/error/network.rs:231-243` |
| `TooManyBatches(_, _)`             | `Fatal`  | `crates/consensus/primary/src/error/network.rs:231-243` |
| `UnknownNetworkKey(_)`             | `Fatal`  | `crates/consensus/primary/src/error/network.rs:231-243` |
| `PeerNotAuthor`                    | `Fatal`  | `crates/consensus/primary/src/error/network.rs:231-243` |
| `InvalidGenesisParent(_)`          | `Fatal`  | `crates/consensus/primary/src/error/network.rs:231-243` |
| `InvalidRound(_)`                  | `Fatal`  | `crates/consensus/primary/src/error/network.rs:231-243` |
| `ParentMissingSignature`           | `Fatal`  | `crates/consensus/primary/src/error/network.rs:231-243` |
| `InvalidParentTimestamp { .. }`    | `Fatal`  | `crates/consensus/primary/src/error/network.rs:231-243` |
| `UnkownWorkerId`                   | `Fatal`  | `crates/consensus/primary/src/error/network.rs:231-243` |
| `UnknownAuthority(_)`              | `Fatal`  | `crates/consensus/primary/src/error/network.rs:231-243` |
| `PendingCertificateOneshot`        | None     | `crates/consensus/primary/src/error/network.rs:251-257` |
| `Storage(_)`                       | None     | `crates/consensus/primary/src/error/network.rs:244-257` |
| `UnknownExecutionResult(_)`        | None     | `crates/consensus/primary/src/error/network.rs:244-257` |
| `TNSend(_)`                        | None     | `crates/consensus/primary/src/error/network.rs:251-257` |
| `InvalidEpoch { .. }`              | None     | `crates/consensus/primary/src/error/network.rs:251-257` |
| `NotCommitteeMember`               | None     | `crates/consensus/primary/src/error/network.rs:251-257` |
| `ClosedWatchChannel`               | None     | `crates/consensus/primary/src/error/network.rs:251-257` |

`InvalidSeedSignature` is `Severe` rather than `Fatal` on purpose.
The seed message is anchored to the verifier's local `prior_epoch_record`, so a node whose record diverged
signs an anchor no peer accepts and rejects every honest peer's header.
`Fatal` would make that ban mutual, total and non-self-healing — neither side could ever repair its record
from the other once both have banned. `Severe` still suppresses a genuinely bad signer while leaving a
divergent node a path back. `crates/consensus/primary/src/error/network.rs:220-226`.

### 5.4 `consensus_chain_error_to_penalty`

Source: `crates/consensus/primary/src/network/mod.rs:1131-1180`.

| Variant                                                                                                                                                                                                                                                                                                                                 | Severity | Source                                                  |
| --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------- | ------------------------------------------------------- |
| `PackError(PackError::MissingBatch)`                                                                                                                                                                                                                                                                                                    | `Medium` | `crates/consensus/primary/src/network/mod.rs:1134-1137` |
| `PackError(PackError::NotConsensus)`                                                                                                                                                                                                                                                                                                    | `Medium` | `crates/consensus/primary/src/network/mod.rs:1134-1137` |
| `PackError(PackError::NotBatch)`                                                                                                                                                                                                                                                                                                        | `Medium` | `crates/consensus/primary/src/network/mod.rs:1134-1137` |
| `PackError(PackError::NotEpoch)`                                                                                                                                                                                                                                                                                                        | `Medium` | `crates/consensus/primary/src/network/mod.rs:1134-1137` |
| `PackError(PackError::InvalidConsensusChain)`                                                                                                                                                                                                                                                                                           | `Severe` | `crates/consensus/primary/src/network/mod.rs:1138-1144` |
| `PackError(PackError::ExtraBatches)`                                                                                                                                                                                                                                                                                                    | `Severe` | `crates/consensus/primary/src/network/mod.rs:1138-1144` |
| `PackError(PackError::MissingBatches)`                                                                                                                                                                                                                                                                                                  | `Severe` | `crates/consensus/primary/src/network/mod.rs:1138-1144` |
| `PackError(PackError::TooManyBatches)`                                                                                                                                                                                                                                                                                                  | `Severe` | `crates/consensus/primary/src/network/mod.rs:1138-1144` |
| `PackError(PackError::CorruptPack)`                                                                                                                                                                                                                                                                                                     | `Severe` | `crates/consensus/primary/src/network/mod.rs:1138-1144` |
| `PackError(PackError::UnexpectedConsensusDigest)`                                                                                                                                                                                                                                                                                       | `Severe` | `crates/consensus/primary/src/network/mod.rs:1138-1144` |
| `PackError(PackError::InvalidEpoch)`                                                                                                                                                                                                                                                                                                    | `Severe` | `crates/consensus/primary/src/network/mod.rs:1138-1144` |
| `PackError` non-penalized variants (`IO`, `BatchLoad`, `EpochLoad`, `Append`, `IndexAppend`, `Fetch`, `Open`, `ReadOnly`, `ReadError`, `MissingAuthority`, `SendFailed`, `ReceiveFailed`, `PersistError`, `InvalidConsensusNumber`, `ConsensusNumberAlreadyAdded`, `ConsensusNumberTooLow`, `InvalidVersion`, `ConsensusNumberTooHigh`) | None     | `crates/consensus/primary/src/network/mod.rs:1145-1162` |
| `EpochMismatch`                                                                                                                                                                                                                                                                                                                         | `Mild`   | `crates/consensus/primary/src/network/mod.rs:1164-1166` |
| `PrevCommitteeEpochMismatch`                                                                                                                                                                                                                                                                                                            | `Mild`   | `crates/consensus/primary/src/network/mod.rs:1164-1166` |
| `CrcError`                                                                                                                                                                                                                                                                                                                              | `Mild`   | `crates/consensus/primary/src/network/mod.rs:1164-1166` |
| `EmptyImport`                                                                                                                                                                                                                                                                                                                           | `Severe` | `crates/consensus/primary/src/network/mod.rs:1167-1169` |
| `InvalidImport`                                                                                                                                                                                                                                                                                                                         | `Severe` | `crates/consensus/primary/src/network/mod.rs:1167-1169` |
| `StreamUnavailable` / `NoCurrentEpoch` / `EpochDbError(_)` / `InvalidPackEpoch(_, _)` / `CantSaveAndNotAvailable(_)` / `NonMonotonicConsensusNumber { .. }` / `IO(_)`                                                                                                                                                                   | None     | `crates/consensus/primary/src/network/mod.rs:1170-1178` |

## 6. Restraint Invariants (no-penalty cases)

These are deliberate tolerances for benign failures. Changing any of these from
`None` to a real penalty risks banning honest peers during normal operation.

- `InboundFailure::ResponseOmission` — local error, no peer fault. `crates/network-libp2p/src/consensus.rs:1431`.
- PX-disconnect outbound failures — `pending_px_disconnects` short-circuit before penalty. `crates/network-libp2p/src/consensus.rs:1327-1330`.
- `WorkerNetworkError::Timeout` / `DBInsert` / `DBCommit` / `DBRead` / `StreamClosed` / `Network` / `BatchEpochMismatch` / `Internal` — local failures or epoch-boundary races. `crates/consensus/worker/src/network/error.rs:129-136`.
- The worker sync-batch stream applies no penalty on either side: a requester that opens with a non-`Req` frame or never sends a readable request is warned and dropped after a bounded error write (`crates/consensus/worker/src/network/mod.rs:430-437,467-471`), and the responder signals its own failures with `SyncFrame::Err` rather than charging them (`crates/consensus/worker/src/network/handler.rs:317-320`).
- `fetch_certificates` — the primary's certificate fetch is penalty-exempt end to end; a failed exchange only advances the staggered fan-out to the next peer. `crates/consensus/primary/src/network/mod.rs:454,457`.
- Epoch-pack sync import — a bad or undecodable pack is classified `EpochPackAttempt::Failed` (try the next peer) with no penalty. `crates/consensus/primary/src/network/mod.rs:1078-1079,1080-1111`.
- `process_inbound_sync_stream` — an unexpected opening sync frame is signalled with `SyncFrame::Err(Malformed)` and dropped, metrics-only. `crates/consensus/primary/src/network/mod.rs:1558-1559`, `crates/consensus/primary/src/network/handler.rs:1403-1404`.
- `PrimaryNetworkError::UnavailableEpoch` / `UnavailableEpochDigest` — a peer "might not have this yet" during sync. `crates/consensus/primary/src/error/network.rs:196-197`.
- `PrimaryNetworkError::UnknownConsensusOutput` — a benign miss: observers legitimately request outputs this node has not served yet, so honest catch-up sync is not banned. `crates/consensus/primary/src/error/network.rs:188`.
- `PrimaryNetworkError::PeerNotInCommittee` — the benign, non-penalizing outcome of the pre-crypto committee check on gossiped epoch votes / consensus results, deliberately chosen over the `Fatal` `PeerNotAuthor` so a fatal penalty cannot land on the wrong peer on the gossip path (GHSA-j2g4-553f-875r). `crates/consensus/primary/src/error/network.rs:198`; rationale at `crates/consensus/primary/src/network/handler.rs:510-517`.
- `PrimaryNetworkError::Storage` / `Timeout` / `ConsensusChainError` / `Internal` — local failures. `crates/consensus/primary/src/error/network.rs:199-202`. `ConsensusChainError` is built at exactly one site, a failed read of this node's own chain while serving a peer (`crates/consensus/primary/src/network/handler.rs:1158`); peer-attributable chain faults are scored separately at the sync call site (`crates/consensus/primary/src/network/mod.rs:748-750`).
- `CertManagerError::Certificate(CertificateError::TooNew(..))` — request races ahead of local state. `crates/consensus/primary/src/error/network.rs:163-165`.
- All `CertManagerError` operational variants (pending lookups, GC, oneshot drops, channel closures, internal storage / network errors). `crates/consensus/primary/src/error/network.rs:170-184`.
- `HeaderError::InvalidEpoch { .. }` — explicitly `None`; epoch boundary mismatch is not penalized. `crates/consensus/primary/src/error/network.rs:251-257`.
- `HeaderError::PendingCertificateOneshot` / `TNSend` / `ClosedWatchChannel` — local channel/task failures. `crates/consensus/primary/src/error/network.rs:251-257`.
- All `PackError` IO/load/persist/internal variants, plus `InvalidVersion` (pack written by a newer node — version skew, not a fault) and the `ConsensusNumber*` sequencing variants — local or skew failures. `crates/consensus/primary/src/network/mod.rs:1145-1162`.
- `ConsensusChainError::StreamUnavailable` / `NoCurrentEpoch` / `EpochDbError` / `InvalidPackEpoch` / `CantSaveAndNotAvailable` / `NonMonotonicConsensusNumber` / `IO` — local failures during epoch pack streaming; the arm documents `NonMonotonicConsensusNumber` as local resume state rather than peer misbehavior. `crates/consensus/primary/src/network/mod.rs:1170-1178`.
- A rejected kad put record from a banned source/publisher with `record.publisher.is_some()` — no extra penalty is stacked on top of the existing ban. `crates/network-libp2p/src/consensus.rs:2028-2034`.
- `OutboundFailure::UnsupportedProtocols` / `InboundFailure::UnsupportedProtocols` — honest version/role skew (multistream-select found no common protocol), not misbehavior; warn only, no penalty. Precondition for the #765 chain-id protocol split, which intentionally makes old/new nodes fail to peer. `crates/network-libp2p/src/consensus.rs:1384,1425`. The stream-path mirror (`StreamFailure::UnsupportedProtocol`) is likewise `None`. `crates/network-libp2p/src/stream/upgrade.rs:133`. The whole stream taxonomy is classification-only today: stream failures are reported metrics-only and never applied, so no `StreamFailure` arm penalizes anyone. `crates/network-libp2p/src/consensus.rs:1809-1815`.
- The node's own peer id — `PeerManager::process_penalty` short-circuits before the score model when the target is the local identity, so a self-connection (e.g. a learned hairpin address routed back to our own id) can never ban the node's own worker. `crates/network-libp2p/src/peers/manager.rs:595-598` (`is_local_peer`). Self-connections are also denied earlier on the dial/discovery/connection paths so they do not reach the penalty path at all.

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
- `Penalty::Severe` never appears as a literal in `crates/network-libp2p/src/consensus.rs`. It only reaches `process_penalty` via app-layer error mappings: `BatchValidationError::RecoverTransaction` (`crates/consensus/worker/src/network/error.rs:92`), `HeaderError::{InvalidTimestamp, InvalidParentRound, InvalidSeedSignature}` (`crates/consensus/primary/src/error/network.rs:229`), `PackError::{InvalidConsensusChain, ExtraBatches, MissingBatches, TooManyBatches, CorruptPack, UnexpectedConsensusDigest, InvalidEpoch}` (`crates/consensus/primary/src/network/mod.rs:1144`), and `ConsensusChainError::{EmptyImport, InvalidImport}` (`crates/consensus/primary/src/network/mod.rs:1168`).
- `PeerAction::DisconnectWithPX` adds the peer to `temporarily_banned` and emits `DisconnectPeerX`. This is a soft ban that bypasses the score model and does not produce a `PeerEvent::Banned`. `crates/network-libp2p/src/peers/manager.rs:468-474`.
- The exemption short-circuit in `Peer::apply_penalty` (it bypasses the score model when the caller passes a `TrustBasis`) is the only mechanism preventing scored bans of exempt peers — operator-allowlisted peers OR validators in any of the three tracked committee slots (previous/current/next), the basis computed live by `AllPeers::exemption` rather than stored on the peer. `crates/network-libp2p/src/peers/peer.rs:297-312`, `crates/network-libp2p/src/peers/all_peers.rs:856-876`. There is no allowlist applied at the network-layer call sites.
- `PrimaryNetworkError::PeerNotInCommittee` carries two contradictory rationales in the source. The variant's own doc comment reads "Temparily disabled, will be back soon" (`crates/consensus/primary/src/error/network.rs:44-47`), implying a suppressed penalty pending re-enablement, while the handler documents it as the deliberately benign alternative to a misattributed `Fatal` (`crates/consensus/primary/src/network/handler.rs:510-517`). Section 6 follows the handler comment as the more recent and more specific of the two. Which one is authoritative is unresolved.
- Penalty application is not debounced. A peer that produces many `Mild` errors in quick succession (e.g. during a sync flap) can still cross the ban threshold (`-50.0`) in fewer than ~50 events if its score has already drifted negative.
