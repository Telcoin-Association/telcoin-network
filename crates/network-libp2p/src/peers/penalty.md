# Peer Penalty System — Invariants Reference

This document is the single source of truth for the telcoin-network peer penalty system.
Every penalty severity, score-model invariant, and every site that applies a penalty is listed here with a source citation.
Treat each entry as an assertion to verify against the code.

## 1. Penalty Severity Levels

`Penalty` and `LoadPenalty` are defined in `crates/network-libp2p/src/peers/penalty.rs`.
Score deltas are applied in `crates/network-libp2p/src/peers/score.rs:84-100`.

| Variant           | Score Delta                          | Effect                                                             | Source                                        |
| ----------------- | ------------------------------------ | ------------------------------------------------------------------ | --------------------------------------------- |
| `Penalty::Mild`   | `-1.0`                               | Clamped to `[min_score, max_score]`                                | `crates/network-libp2p/src/peers/score.rs:90` |
| `Penalty::Medium` | `-5.0`                               | Clamped to `[min_score, max_score]`                                | `crates/network-libp2p/src/peers/score.rs:91` |
| `Penalty::Severe` | `-10.0`                              | Clamped to `[min_score, max_score]`                                | `crates/network-libp2p/src/peers/score.rs:92` |
| `Penalty::Fatal`  | sets score to `min_score` (`-100.0`) | Immediate ban — score jumps below `min_score_before_ban` (`-50.0`) | `crates/network-libp2p/src/peers/score.rs:93` |

| `Penalty::Load(cause)` | Mild, Medium, or Severe weight | Subject to load-scoring policy | `Penalty::severity()` in `penalty.rs` |

## 2. Score Model Invariants

- Score range: `[min_score, max_score]` = `[-100.0, 100.0]`. `crates/config/src/network.rs:483-484`.
- Default starting score: `0.0`. `crates/config/src/network.rs:482`.
- Ban threshold (`min_score_before_ban`): `-50.0`. `crates/config/src/network.rs:495`.
- Disconnect threshold (`min_score_before_disconnect`): `-20.0`. `crates/config/src/network.rs:494`.
- Score halflife: `300.0` seconds; decay factor is `e^(-ln(2)/halflife * dt)`. `crates/config/src/network.rs:488`, `crates/network-libp2p/src/peers/score.rs:130-132`.
- `banned_before_decay_secs`: `30 * 60` (30 min). When a peer crosses the ban threshold, `last_updated` is pushed forward by this duration so the score does not decay during the lockout. `crates/config/src/network.rs:493`, `crates/network-libp2p/src/peers/score.rs:149-154`.
- Operator trust suppresses only `Penalty::Load`. Previous, current, and next committee members remain exempt from all score penalties for liveness. `AllPeers::peer_policy` composes these privileges from live trust bases; see `README.md` for the matrix.
- Penalty application has no debouncing: every `process_penalty` call evaluates reputation immediately and may produce a ban on the same call. `crates/network-libp2p/src/peers/all_peers.rs:264-306`.
- Bans surface to the rest of the swarm as `PeerEvent::Banned`, pushed by `process_ban`. `crates/network-libp2p/src/peers/manager.rs:609-622`.
- When scoring applies, `Penalty::Fatal` crosses the ban threshold on its first application. Committee membership suppresses that score change, while message validation still rejects the offending content.
- Only the `Banned`/`Disconnected`/`Trusted` reputation transitions trigger a `PeerAction`. If the new reputation equals the prior reputation, `process_penalty` returns `PeerAction::NoAction`. `crates/network-libp2p/src/peers/all_peers.rs:272-274`.

## 3. Penalty Application Sites — Network Layer

All sites below are in `crates/network-libp2p/src/consensus.rs`. Symbol and event names
identify the call sites without relying on line numbers. `NetworkCommand::ReportPenalty` is
the external application entry point. Class controls exemptions; severity controls score weight.

### 3.1 Gossip events (`process_gossip_event`)

| Trigger | Class | Severity |
| --- | --- | --- |
| `verify_gossip`: `TooLarge` with a resolved relayer (`FatalRelayer`) | Protocol, charged to relayer | Fatal |
| `verify_gossip`: `UnauthorizedAuthor` with a resolved author (`FatalAuthor`) | Protocol, charged to author | Fatal |
| Unresolved accountable identity (`RejectPenalty::Skip`) | None | None |
| `GossipsubNotSupported` | Protocol | Fatal |
| `SlowPeer` | `Load(SlowPeer)` | Mild |

### 3.2 Request/response events (`process_reqres_event`)

| Trigger | Class | Severity |
| --- | --- | --- |
| Outbound `DialFailure` / `ConnectionClosed` | None | None |
| Inbound or outbound `Io` with a transport-flap error kind | None | None |
| Inbound or outbound `Io` with other kinds (codec violation) | Protocol | Medium |
| Outbound `Timeout` | `Load(Timeout)` | Mild |
| Inbound or outbound `UnsupportedProtocols` (version or role skew) | None | None |
| Inbound `Timeout` / `ConnectionClosed` / `ResponseOmission` | None | None |

### 3.3 Kademlia `GetRecord` result (`process_kad_event`)

| Trigger | Class | Severity |
| --- | --- | --- |
| `FoundRecord` fails `peer_record_valid` (signature or key mismatch) | Protocol | Fatal |

### 3.4 Kademlia put-record handler (`process_kad_put_request`)

| Trigger | Class | Severity |
| --- | --- | --- |
| Rejected record with no publisher | Protocol | Fatal |
| Put-record flood threshold exceeded, before BLS verification or storage | `Load(KademliaFlood)` | Severe |
| Valid but stale record | None | None |
| Invalid signature or network key | Protocol | Fatal |

A record rejected because its source or publisher is already banned incurs no extra penalty
when a publisher is present. Over-budget work is dropped even for score-exempt peers.

### 3.5 Kademlia result post-processing (`process_kad_query_result`)

| Trigger | Class | Severity |
| --- | --- | --- |
| Returned record key differs from the requested key | Protocol | Fatal |

### 3.6 Kademlia provider handler (`process_kad_add_provider`)

| Trigger | Class | Severity |
| --- | --- | --- |
| `add_provider_rate_limited` exceeds the independent provider budget | `Load(KademliaRateLimit)` | Medium |

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
| `try_sync_epoch_pack_exchange` (`SyncFrame::Ack` import arm) | epoch-pack sync import | `consensus_chain_error_to_penalty(&e)`, gated by `import_fault_is_peer_caused(&e)` |
| `crates/consensus/primary/src/network/mod.rs:1343` | `process_vote_request`                     | `(&PrimaryNetworkError).into() -> Option<Penalty>` |
| `crates/consensus/primary/src/network/mod.rs:1401` | `process_epoch_record_request`             | `(&PrimaryNetworkError).into()`                    |
| `crates/consensus/primary/src/network/mod.rs:1440` | `process_gossip`                           | `(&PrimaryNetworkError).into()`                    |

The epoch-pack sync import charges a penalty ONLY for a fault attributable solely to the peer's
streamed bytes — a structural/framing/over-a-resource-bound `PackError`, or
`ConsensusChainError::{EmptyImport, InvalidImport}` — as decided by `import_fault_is_peer_caused`. It
deliberately does NOT penalise a local/ambiguous error raised while importing (a full disk, a failed
mmap/index write, `CorruptPack` — which local recovery also produces — or a local chain-state
mismatch), so this node's own storage failure is not charged to the peer. Committee peers
remain score-exempt, so this import path cannot reputation-ban a validator. Operator-trusted
peers outside the committee remain subject to protocol penalties. A pack damaged at rest on
the serving peer may earn Medium per failed probe; the failed candidate moves to the back of
the probe order for that epoch regardless of its scoring exemption.

Two peer-facing primary paths deliberately apply no penalty and so have no row above:
the certificate fetch (`fetch_certificates`, `crates/consensus/primary/src/network/mod.rs:457`),
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

Source: `crates/consensus/primary/src/error/network.rs:210-273`.

| `HeaderError` variant              | Severity | Source                                                  |
| ---------------------------------- | -------- | ------------------------------------------------------- |
| `SyncBatches(_)`                   | `Mild`   | `crates/consensus/primary/src/error/network.rs:216-218` |
| `TooNew { .. }` | None | `penalty_from_header_error` in `crates/consensus/primary/src/error/network.rs` |
| `InvalidParents`                   | `Medium` | `crates/consensus/primary/src/error/network.rs:220-222` |
| `WrongNumberOfParents(_, _)`       | `Medium` | `crates/consensus/primary/src/error/network.rs:220-222` |
| `TooOld { .. }` | None | `penalty_from_header_error` in `crates/consensus/primary/src/error/network.rs` |
| `InvalidTimestamp { .. }` | None | `penalty_from_header_error` in `crates/consensus/primary/src/error/network.rs` |
| `InvalidParentRound`               | `Severe` | `crates/consensus/primary/src/error/network.rs:232-234` |
| `InvalidSeedSignature` | None | `penalty_from_header_error` in `crates/consensus/primary/src/error/network.rs` |
| `AlreadyVoted(_, _)`               | `Fatal`  | `crates/consensus/primary/src/error/network.rs:236-248` |
| `DuplicateParents`                 | `Fatal`  | `crates/consensus/primary/src/error/network.rs:236-248` |
| `TooManyParents(_, _)`             | `Fatal`  | `crates/consensus/primary/src/error/network.rs:236-248` |
| `TooManyBatches(_, _)`             | `Fatal`  | `crates/consensus/primary/src/error/network.rs:236-248` |
| `UnknownNetworkKey(_)`             | `Fatal`  | `crates/consensus/primary/src/error/network.rs:236-248` |
| `PeerNotAuthor`                    | `Fatal`  | `crates/consensus/primary/src/error/network.rs:236-248` |
| `InvalidGenesisParent(_)`          | `Fatal`  | `crates/consensus/primary/src/error/network.rs:236-248` |
| `InvalidRound(_)`                  | `Fatal`  | `crates/consensus/primary/src/error/network.rs:236-248` |
| `ParentMissingSignature`           | `Fatal`  | `crates/consensus/primary/src/error/network.rs:236-248` |
| `InvalidParentTimestamp { .. }`    | `Fatal`  | `crates/consensus/primary/src/error/network.rs:236-248` |
| `InvalidTimestampMillis(_)`        | `Fatal`  | `crates/consensus/primary/src/error/network.rs:236-248` |
| `UnkownWorkerId`                   | `Fatal`  | `crates/consensus/primary/src/error/network.rs:236-248` |
| `UnknownAuthority(_)`              | `Fatal`  | `crates/consensus/primary/src/error/network.rs:236-248` |
| `PendingCertificateOneshot`        | None     | `crates/consensus/primary/src/error/network.rs:256-262` |
| `Storage(_)`                       | None     | `crates/consensus/primary/src/error/network.rs:249-262` |
| `UnknownExecutionResult(_)`        | None     | `crates/consensus/primary/src/error/network.rs:249-262` |
| `TNSend(_)`                        | None     | `crates/consensus/primary/src/error/network.rs:256-262` |
| `InvalidEpoch { .. }`              | None     | `crates/consensus/primary/src/error/network.rs:256-262` |
| `NotCommitteeMember`               | None     | `crates/consensus/primary/src/error/network.rs:256-262` |
| `ClosedWatchChannel`               | None     | `crates/consensus/primary/src/error/network.rs:256-262` |
| `AlreadyVotedForLaterRound { .. }` | None     | `crates/consensus/primary/src/error/network.rs:263-271` |

`TooNew` and `TooOld` compare the header against local round state, so restart or catch-up
skew rejects a header without penalizing its author. `InvalidTimestamp` likewise rejects and
caches a header beyond the clock window without scoring either peer for relative clock skew.

`InvalidSeedSignature` carries no score penalty. Verification depends on the local
`prior_epoch_record`; a mismatch alone does not establish which side has the wrong anchor.
The header still receives no vote. This preserves connectivity for recovery and diagnosis;
it does not claim a divergent record will automatically repair itself.

`InvalidTimestampMillis(_)` is `Fatal` because no honest node can produce it.
An honest `created_at_millis` is derived from a millisecond timestamp and is always below 1000,
and header decode already rejects an out-of-range value before `Header::validate` runs, so the
validate check only catches a bug in the header type itself. `crates/consensus/primary/src/error/network.rs:231-244`.

### 5.4 `consensus_chain_error_to_penalty`

Source: `crates/consensus/primary/src/network/mod.rs:1152-1211`.

| Variant                                                                                                                                                                                                                                                                                                                                 | Severity | Source                                                  |
| --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------- | ------------------------------------------------------- |
| `PackError(PackError::MissingBatch)`                                                                                                                                                                                                                                                                                                    | `Medium` | `crates/consensus/primary/src/network/mod.rs:1155-1158` |
| `PackError(PackError::NotConsensus)`                                                                                                                                                                                                                                                                                                    | `Medium` | `crates/consensus/primary/src/network/mod.rs:1155-1158` |
| `PackError(PackError::NotBatch)`                                                                                                                                                                                                                                                                                                        | `Medium` | `crates/consensus/primary/src/network/mod.rs:1155-1158` |
| `PackError(PackError::NotEpoch)`                                                                                                                                                                                                                                                                                                        | `Medium` | `crates/consensus/primary/src/network/mod.rs:1155-1158` |
| `PackError(PackError::UnexpectedRecord)`                                                                                                                                                                                                                                                                                                | `Medium` | `crates/consensus/primary/src/network/mod.rs:1155-1158` |
| `PackError(PackError::UndecodableRecord)`                                                                                                                                                                                                                                                                                               | `Medium` | `crates/consensus/primary/src/network/mod.rs:1155-1158` |
| `PackError(PackError::InvalidConsensusChain)`                                                                                                                                                                                                                                                                                           | `Severe` | `crates/consensus/primary/src/network/mod.rs:1159-1176` |
| `PackError(PackError::ExtraBatches)`                                                                                                                                                                                                                                                                                                    | `Severe` | `crates/consensus/primary/src/network/mod.rs:1159-1176` |
| `PackError(PackError::MissingBatches)`                                                                                                                                                                                                                                                                                                  | `Severe` | `crates/consensus/primary/src/network/mod.rs:1159-1176` |
| `PackError(PackError::TooManyBatches)`                                                                                                                                                                                                                                                                                                  | `Severe` | `crates/consensus/primary/src/network/mod.rs:1159-1176` |
| `PackError(PackError::CorruptPack)`                                                                                                                                                                                                                                                                                                     | `Severe` | `crates/consensus/primary/src/network/mod.rs:1159-1176` |
| `PackError(PackError::UnexpectedConsensusDigest)`                                                                                                                                                                                                                                                                                       | `Severe` | `crates/consensus/primary/src/network/mod.rs:1159-1176` |
| `PackError(PackError::EmptySubDag)`                                                                                                                                                                                                                                                                                                     | `Severe` | `crates/consensus/primary/src/network/mod.rs:1159-1176` |
| `PackError(PackError::InvalidEpoch)`                                                                                                                                                                                                                                                                                                    | `Severe` | `crates/consensus/primary/src/network/mod.rs:1159-1176` |
| `PackError(PackError::BatchTooLarge)`                                                                                                                                                                                                                                                                                                   | `Severe` | `crates/consensus/primary/src/network/mod.rs:1159-1176` |
| `PackError(PackError::OutputTooLarge)`                                                                                                                                                                                                                                                                                                  | `Severe` | `crates/consensus/primary/src/network/mod.rs:1159-1176` |
| `PackError(PackError::InvalidConsensusNumber)`                                                                                                                                                                                                                                                                                          | `Severe` | `crates/consensus/primary/src/network/mod.rs:1159-1176` |
| `PackError` non-penalized variants (`IO`, `BatchLoad`, `EpochLoad`, `Append`, `IndexAppend`, `Fetch`, `Open`, `ReadOnly`, `ReadError`, `MissingAuthority`, `SendFailed`, `ReceiveFailed`, `PersistError`, `ConsensusNumberAlreadyAdded`, `ConsensusNumberTooLow`, `InvalidVersion`, `ConsensusNumberTooHigh`) | None     | `crates/consensus/primary/src/network/mod.rs:1177-1193` |
| `EpochMismatch`                                                                                                                                                                                                                                                                                                                         | `Mild`   | `crates/consensus/primary/src/network/mod.rs:1195-1197` |
| `PrevCommitteeEpochMismatch`                                                                                                                                                                                                                                                                                                            | `Mild`   | `crates/consensus/primary/src/network/mod.rs:1195-1197` |
| `CrcError`                                                                                                                                                                                                                                                                                                                              | `Mild`   | `crates/consensus/primary/src/network/mod.rs:1195-1197` |
| `EmptyImport`                                                                                                                                                                                                                                                                                                                           | `Severe` | `crates/consensus/primary/src/network/mod.rs:1198-1200` |
| `InvalidImport`                                                                                                                                                                                                                                                                                                                         | `Severe` | `crates/consensus/primary/src/network/mod.rs:1198-1200` |
| `StreamUnavailable` / `NoCurrentEpoch` / `EpochDbError(_)` / `InvalidPackEpoch(_, _)` / `CantSaveAndNotAvailable(_)` / `NonMonotonicConsensusNumber { .. }` / `IO(_)`                                                                                                                                                                   | None     | `crates/consensus/primary/src/network/mod.rs:1201-1209` |

## 6. Restraint Invariants (no-penalty cases)

These are deliberate tolerances for benign failures. Changing any of these from
`None` to a real penalty risks banning honest peers during normal operation.

- `InboundFailure::ResponseOmission` — local error, no peer fault. `crates/network-libp2p/src/consensus.rs:1431`.
- PX-disconnect outbound failures — `pending_px_disconnects` short-circuit before penalty. `crates/network-libp2p/src/consensus.rs:1327-1330`.
- `WorkerNetworkError::Timeout` / `DBInsert` / `DBCommit` / `DBRead` / `StreamClosed` / `Network` / `BatchEpochMismatch` / `Internal` — local failures or epoch-boundary races. `crates/consensus/worker/src/network/error.rs:129-136`.
- The worker sync-batch stream applies no penalty on either side: a requester that opens with a non-`Req` frame or never sends a readable request is warned and dropped after a bounded error write (`crates/consensus/worker/src/network/mod.rs:430-437,467-471`), and the responder signals its own failures with `SyncFrame::Err` rather than charging them (`crates/consensus/worker/src/network/handler.rs:317-320`).
- `fetch_certificates` — the primary's certificate fetch is penalty-exempt end to end; a failed exchange only advances the staggered fan-out to the next peer. `crates/consensus/primary/src/network/mod.rs:454,457`.
- Epoch-pack sync import — a peer-caused stream fault (a structural/framing/over-a-resource-bound `PackError`, or `ConsensusChainError::{EmptyImport, InvalidImport}`) IS penalised via `consensus_chain_error_to_penalty`, gated by `import_fault_is_peer_caused`; a local/ambiguous error raised while importing (IO/Append/Persist/Open/EpochDb, `CorruptPack`, or a local chain-state mismatch) is NOT (it would ban an honest peer for this node's own storage failure). Either way the pack is still classified `EpochPackAttempt::Failed` so the probe tries the next peer.
- `process_inbound_sync_stream` — an unexpected opening sync frame is signalled with `SyncFrame::Err(Malformed)` and dropped, metrics-only. `crates/consensus/primary/src/network/mod.rs:1558-1559`, `crates/consensus/primary/src/network/handler.rs:1403-1404`.
- `PrimaryNetworkError::UnavailableEpoch` / `UnavailableEpochDigest` — a peer "might not have this yet" during sync. `crates/consensus/primary/src/error/network.rs:196-197`.
- `PrimaryNetworkError::UnknownConsensusOutput` — a benign miss: observers legitimately request outputs this node has not served yet, so honest catch-up sync is not banned. `crates/consensus/primary/src/error/network.rs:188`.
- `PrimaryNetworkError::PeerNotInCommittee` — the benign, non-penalizing outcome of the pre-crypto committee check on gossiped epoch votes / consensus results, deliberately chosen over the `Fatal` `PeerNotAuthor` so a fatal penalty cannot land on the wrong peer on the gossip path (GHSA-j2g4-553f-875r). `crates/consensus/primary/src/error/network.rs:198`; rationale at `crates/consensus/primary/src/network/handler.rs:510-517`.
- `PrimaryNetworkError::Storage` / `Timeout` / `ConsensusChainError` / `Internal` — local failures. `crates/consensus/primary/src/error/network.rs:199-202`. `ConsensusChainError` is built at exactly one site, a failed read of this node's own chain while serving a peer (`crates/consensus/primary/src/network/handler.rs:1158`); peer-attributable chain faults are scored separately at the sync call site (`crates/consensus/primary/src/network/mod.rs:748-750`).
- `CertManagerError::Certificate(CertificateError::TooNew(..))` — request races ahead of local state. `crates/consensus/primary/src/error/network.rs:163-165`.
- All `CertManagerError` operational variants (pending lookups, GC, oneshot drops, channel closures, internal storage / network errors). `crates/consensus/primary/src/error/network.rs:170-184`.
- `HeaderError::InvalidEpoch { .. }` — explicitly `None`; epoch boundary mismatch is not penalized. `crates/consensus/primary/src/error/network.rs:252-258`.
- `HeaderError::PendingCertificateOneshot` / `TNSend` / `ClosedWatchChannel` — local channel/task failures. `crates/consensus/primary/src/error/network.rs:252-258`.
- All `PackError` IO/load/persist/internal variants, plus `InvalidVersion` (a pack version this build does not read: written by a newer node, or a legacy v0 source, which an upgraded peer serves migrated — version skew, not a fault) and the `ConsensusNumber*` sequencing variants — local or skew failures. `crates/consensus/primary/src/network/mod.rs:1244-1261`.
- `ConsensusChainError::StreamUnavailable` / `NoCurrentEpoch` / `EpochDbError` / `InvalidPackEpoch` / `CantSaveAndNotAvailable` / `NonMonotonicConsensusNumber` / `IO` — local failures during epoch pack streaming; the arm documents `NonMonotonicConsensusNumber` as local resume state rather than peer misbehavior. `crates/consensus/primary/src/network/mod.rs:1269-1277`.
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
- Severe weight reaches `process_penalty` through both protocol error mappings and `Penalty::Load(LoadPenalty::KademliaFlood)`. The latter is load-classified even though its score delta is -10; see `Penalty::severity()`.
- `PeerAction::DisconnectWithPX` adds the peer to `temporarily_banned` and emits `DisconnectPeerX`. This is a soft ban that bypasses the score model and does not produce a `PeerEvent::Banned`. `crates/network-libp2p/src/peers/manager.rs:468-474`.
- `PeerPolicy::applies` exempts committee members from all scoring and operator-trusted peers outside the committee from load scoring only. Operator trust reload and identity merges preserve protocol history; committee promotion forgives reputation bans. Validation and resource limits remain before the scoring decision.
- `PrimaryNetworkError::PeerNotInCommittee` carries two contradictory rationales in the source. The variant's own doc comment reads "Temparily disabled, will be back soon" (`crates/consensus/primary/src/error/network.rs:44-47`), implying a suppressed penalty pending re-enablement, while the handler documents it as the deliberately benign alternative to a misattributed `Fatal` (`crates/consensus/primary/src/network/handler.rs:510-517`). Section 6 follows the handler comment as the more recent and more specific of the two. Which one is authoritative is unresolved.
- Penalty application is not debounced. A peer that produces many `Mild` errors in quick succession (e.g. during a sync flap) can still cross the ban threshold (`-50.0`) in fewer than ~50 events if its score has already drifted negative.
